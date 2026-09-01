//! In-memory mirror of the Postgres-backed `issues` table, plus the
//! `WalRuntime` adapter Palimpsest reads through.
//!
//! Writes do **not** flow through here — HTTP handlers go straight
//! to Postgres via tokio-postgres. The logical-replication consumer
//! (see `db.rs`) tails the slot, decodes pgoutput frames into
//! `DecodedEvent::Row { op, old, new }`, and pipes those into
//! `IssueStore::apply_txn`. The mirror keeps the "current snapshot" in
//! lock-step with the database, and the journal of `RawDiff`s drives
//! the live-diff cursor the dataflow consumes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_server::cursor::RawDiff;
use palimpsest_server::snapshot::{SnapshotBatch, SnapshotTableRows};
use palimpsest_server::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_server::TraceCursor;
use palimpsest_sql::ColumnType;
use palimpsest_wal::{DatumType, TableId};
use serde::Serialize;

use crate::db::issue_to_row;

/// One tracked issue. Everything the tracker needs is kept in
/// Int/Text columns so rows flow through the dataflow's scalar schema
/// unchanged:
///
/// * `status` — `backlog | todo | in_progress | in_review | done | cancelled`
/// * `priority` — `0` none … `4` urgent
/// * `assignee` — persona id, `""` when unassigned
/// * `created_day` / `completed_day` — days since the Unix epoch
///   (`completed_day = 0` while the issue is open), denormalized so
///   throughput and cycle-time analytics are plain `GROUP BY`s
/// * `cycle_days` — `completed_day - created_day` once done, else `0`
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Issue {
    pub id: i64,
    pub title: String,
    pub status: String,
    pub priority: i64,
    pub assignee: String,
    pub project: String,
    pub estimate: i64,
    pub created_day: i64,
    pub completed_day: i64,
    pub cycle_days: i64,
}

/// One row change to apply at a transaction boundary.
#[derive(Debug, Clone)]
pub enum IssueChange {
    Insert(Issue),
    Update { prev: Issue, curr: Issue },
    Delete(Issue),
}

pub struct IssueStore {
    rows: Mutex<Vec<Issue>>,
    /// Monotonic clock the journal anchors on. Bumped once per
    /// applied Postgres transaction (one Begin/Commit pair), so a
    /// bulk simulator batch that touches many rows produces one
    /// cursor-pump wakeup, not one per row. Deliberately not derived
    /// from Postgres LSN (sparse, not contiguous).
    lsn: AtomicU64,
    journal: Arc<Mutex<Vec<RawDiff>>>,
}

impl IssueStore {
    pub fn from_snapshot(snapshot: Vec<Issue>) -> Self {
        Self {
            rows: Mutex::new(snapshot),
            lsn: AtomicU64::new(1),
            journal: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn snapshot(&self) -> Vec<Issue> {
        self.rows.lock().expect("issues poisoned").clone()
    }

    pub fn current_lsn(&self) -> Lsn {
        Lsn::new(self.lsn.load(Ordering::SeqCst))
    }

    fn bump_lsn(&self) -> Lsn {
        Lsn::new(self.lsn.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn journal_handle(&self) -> Arc<Mutex<Vec<RawDiff>>> {
        Arc::clone(&self.journal)
    }

    /// Apply every row change in a Postgres transaction at one LSN.
    /// No-op if `changes` is empty so Begin/Commit pairs that don't
    /// touch this table don't bump the journal.
    pub fn apply_txn(&self, changes: &[IssueChange]) {
        if changes.is_empty() {
            return;
        }
        {
            let mut rows = self.rows.lock().expect("issues poisoned");
            for change in changes {
                match change {
                    IssueChange::Insert(issue) => rows.push(issue.clone()),
                    IssueChange::Update { curr, .. } => {
                        if let Some(slot) = rows.iter_mut().find(|i| i.id == curr.id) {
                            *slot = curr.clone();
                        }
                    }
                    IssueChange::Delete(prev) => {
                        if let Some(idx) = rows.iter().position(|i| i.id == prev.id) {
                            rows.remove(idx);
                        }
                    }
                }
            }
        }
        let lsn = self.bump_lsn();
        let mut journal = self.journal.lock().expect("issues journal poisoned");
        for change in changes {
            match change {
                IssueChange::Insert(issue) => journal.push(RawDiff {
                    table: None,
                    row: issue_to_row(issue),
                    lsn,
                    diff: 1,
                }),
                IssueChange::Update { prev, curr } => {
                    journal.push(RawDiff {
                        table: None,
                        row: issue_to_row(prev),
                        lsn,
                        diff: -1,
                    });
                    journal.push(RawDiff {
                        table: None,
                        row: issue_to_row(curr),
                        lsn,
                        diff: 1,
                    });
                }
                IssueChange::Delete(prev) => journal.push(RawDiff {
                    table: None,
                    row: issue_to_row(prev),
                    lsn,
                    diff: -1,
                }),
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Adapter that exposes the issues mirror as a `WalRuntime`.
// -----------------------------------------------------------------------------

pub struct DemoWalRuntime {
    issues: Arc<IssueStore>,
    issues_table_id: TableId,
}

impl DemoWalRuntime {
    pub fn new(issues: Arc<IssueStore>, issues_table_id: TableId) -> Self {
        Self {
            issues,
            issues_table_id,
        }
    }
}

/// `(name, wire type, sql type)` for every `issues` column, in table
/// order. Single source of truth for the three schema surfaces below.
const ISSUE_COLUMNS: &[(&str, DatumType, ColumnType)] = &[
    ("id", DatumType::I64, ColumnType::Int),
    ("title", DatumType::Text, ColumnType::Text),
    ("status", DatumType::Text, ColumnType::Text),
    ("priority", DatumType::I64, ColumnType::Int),
    ("assignee", DatumType::Text, ColumnType::Text),
    ("project", DatumType::Text, ColumnType::Text),
    ("estimate", DatumType::I64, ColumnType::Int),
    ("created_day", DatumType::I64, ColumnType::Int),
    ("completed_day", DatumType::I64, ColumnType::Int),
    ("cycle_days", DatumType::I64, ColumnType::Int),
];

impl WalRuntime for DemoWalRuntime {
    fn fetch_snapshot(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        let rows = self.issues.snapshot().iter().map(issue_to_row).collect();
        Ok(SnapshotBatch {
            snapshot_lsn: self.issues.current_lsn(),
            rows: vec![SnapshotTableRows {
                table: self.issues_table_id,
                rows,
            }],
        })
    }

    fn query_schema(&self, _query: &QueryId) -> Result<SchemaDefinition, String> {
        // For compiled queries (everything the tracker runs) the
        // dataflow's `CompiledPlan::output_schema` overrides this in
        // `palimpsest_server::handle_subscribe`; this base-table shape
        // covers the fallback path.
        Ok(SchemaDefinition {
            id: SchemaId::new(0),
            columns: ISSUE_COLUMNS
                .iter()
                .map(|(name, wire, _)| ColumnSpec {
                    name: (*name).to_owned(),
                    datum_type: wire.clone(),
                    nullable: false,
                })
                .collect(),
            primary_key_columns: vec![0],
        })
    }

    fn open_cursor(
        &self,
        _query: &QueryId,
        from_lsn: Lsn,
    ) -> Result<Box<dyn TraceCursor + Send>, String> {
        Ok(Box::new(JournalCursor {
            journal: self.issues.journal_handle(),
            next_index: 0,
            from_lsn,
        }))
    }

    fn table_schema(&self, table: &str) -> Option<(TableId, ScalarSchema)> {
        if table != "issues" {
            return None;
        }
        Some((
            self.issues_table_id,
            ScalarSchema::from_pairs(
                ISSUE_COLUMNS
                    .iter()
                    .map(|(name, _, sql)| ((*name).to_owned(), *sql)),
            ),
        ))
    }
}

/// Cursor over the diff journal. Anchored by LSN so a write that
/// races `fetch_snapshot` re-surfaces as a live diff.
struct JournalCursor {
    journal: Arc<Mutex<Vec<RawDiff>>>,
    next_index: usize,
    from_lsn: Lsn,
}

impl TraceCursor for JournalCursor {
    fn next_diff(&mut self) -> Option<RawDiff> {
        let journal = self.journal.lock().expect("journal poisoned");
        while let Some(entry) = journal.get(self.next_index) {
            self.next_index += 1;
            if entry.lsn > self.from_lsn {
                return Some(entry.clone());
            }
        }
        None
    }

    fn peek_lsn(&self) -> Option<Lsn> {
        let journal = self.journal.lock().expect("journal poisoned");
        let mut idx = self.next_index;
        while let Some(entry) = journal.get(idx) {
            if entry.lsn > self.from_lsn {
                return Some(entry.lsn);
            }
            idx += 1;
        }
        None
    }
}

/// The SQL-frontend catalog for the tracker: one `issues` table whose
/// columns line up with `ISSUE_COLUMNS`. Permission rules compile
/// against this.
pub fn issues_catalog() -> palimpsest_sql::Catalog {
    palimpsest_sql::Catalog::new([palimpsest_sql::TableSchema::new(
        "issues",
        ISSUE_COLUMNS
            .iter()
            .map(|(name, _, sql)| palimpsest_sql::ColumnSchema::new(*name, *sql))
            .collect::<Vec<_>>(),
    )])
}
