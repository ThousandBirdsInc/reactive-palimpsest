// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The [`PostgresWalRuntime`]: an in-memory mirror + bounded diff
//! journal fed by logical replication, exposed to the server through
//! the [`WalRuntime`] trait.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_dataflow::palimpsest::{Lsn, Row};
use palimpsest_server::cursor::RawDiff;
use palimpsest_server::snapshot::{SnapshotBatch, SnapshotTableRows};
use palimpsest_server::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_server::TraceCursor;
use palimpsest_sql::prepared::QueryRegistry;
use palimpsest_sql::ColumnType;
use palimpsest_wal::{Datum, DatumType, RowOp, TableId, Tuple};

use crate::introspect::IntrospectedTable;

/// Configuration for [`PostgresWalRuntime::connect`].
#[derive(Debug, Clone)]
pub struct PostgresRuntimeConfig {
    /// Postgres DSN (`postgres://user:pass@host:port/db`). Never
    /// logged.
    pub dsn: String,
    /// Tables to stream. Prefer deriving this from the query registry
    /// via [`Self::from_registry`] — a hand-maintained list is exactly
    /// the kind of second source of truth this crate exists to remove.
    pub tables: Vec<String>,
    /// Logical replication slot name.
    pub slot: String,
    /// Publication name.
    pub publication: String,
    /// Poll interval for slot changes.
    pub poll_interval: Duration,
    /// Bound on the in-memory diff journal (in row diffs). When the
    /// journal overflows, the oldest diffs are dropped and any cursor
    /// that would need them refuses to resume instead of serving a
    /// gap.
    pub journal_capacity: usize,
    /// Whether the runtime may issue `ALTER TABLE ... REPLICA IDENTITY
    /// FULL` itself where the current identity is insufficient. When
    /// `false` (or when the role lacks permission) startup fails with
    /// the exact remedial DDL instead.
    pub manage_replica_identity: bool,
}

impl PostgresRuntimeConfig {
    /// Config with defaults for everything but the DSN. Populate
    /// `tables` explicitly or via [`Self::from_registry`].
    #[must_use]
    pub fn new(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            tables: Vec::new(),
            slot: "palimpsest".to_owned(),
            publication: "palimpsest".to_owned(),
            poll_interval: Duration::from_millis(100),
            journal_capacity: 262_144,
            manage_replica_identity: true,
        }
    }

    /// Derives the streamed table set from the query registry: the
    /// union of base tables referenced by every registered query. No
    /// `tables = [...]` config, and a registered query against a table
    /// missing from the database fails at
    /// [`PostgresWalRuntime::connect`], not at subscribe time.
    #[must_use]
    pub fn from_registry(dsn: impl Into<String>, registry: &QueryRegistry) -> Self {
        let mut config = Self::new(dsn);
        config.tables = registry.referenced_tables();
        config
    }
}

// ---------------------------------------------------------------------
// Journal
// ---------------------------------------------------------------------

struct Journal {
    entries: VecDeque<RawDiff>,
    /// Absolute sequence number of `entries[0]`.
    base_seq: u64,
    /// Highest LSN that has been dropped from the front. A cursor
    /// asked to resume from at-or-before this LSN is refused.
    truncated_through: u64,
    capacity: usize,
}

impl Journal {
    fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            base_seq: 0,
            truncated_through: 0,
            capacity: capacity.max(1),
        }
    }

    fn push(&mut self, diff: RawDiff) {
        if self.entries.len() == self.capacity {
            if let Some(dropped) = self.entries.pop_front() {
                self.base_seq += 1;
                self.truncated_through = self.truncated_through.max(dropped.lsn.get());
            }
        }
        self.entries.push_back(diff);
    }
}

// ---------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------

#[allow(clippy::redundant_pub_crate)]
pub(crate) struct SharedState {
    /// Streamed tables keyed by every name a query may use for them
    /// (bare relation name, and `namespace.relation`).
    tables_by_name: BTreeMap<String, Arc<IntrospectedTable>>,
    names_by_id: BTreeMap<TableId, Arc<IntrospectedTable>>,
    mirror: Mutex<BTreeMap<TableId, Vec<Row>>>,
    journal: Mutex<Journal>,
    /// Last applied commit LSN (also the snapshot clock).
    clock: AtomicU64,
    /// Latched fatal error (schema drift). Once set, snapshots and
    /// cursors refuse rather than risk serving mis-decoded rows.
    failed: Mutex<Option<String>>,
}

/// Production Postgres-backed [`WalRuntime`].
///
/// Cheap to clone; all clones share the mirror, journal, and clock.
#[derive(Clone)]
pub struct PostgresWalRuntime {
    pub(crate) state: Arc<SharedState>,
}

impl PostgresWalRuntime {
    pub(crate) fn from_tables(tables: Vec<IntrospectedTable>, journal_capacity: usize) -> Self {
        let mut tables_by_name = BTreeMap::new();
        let mut names_by_id = BTreeMap::new();
        for table in tables {
            let table = Arc::new(table);
            names_by_id.insert(table.id, Arc::clone(&table));
            tables_by_name.insert(
                format!("{}.{}", table.namespace, table.name),
                Arc::clone(&table),
            );
            // The bare name resolves too unless it is ambiguous across
            // namespaces (first registration wins; queries against an
            // ambiguous bare name should qualify it).
            tables_by_name
                .entry(table.name.clone())
                .or_insert_with(|| Arc::clone(&table));
        }
        Self {
            state: Arc::new(SharedState {
                tables_by_name,
                names_by_id,
                mirror: Mutex::new(BTreeMap::new()),
                journal: Mutex::new(Journal::new(journal_capacity)),
                clock: AtomicU64::new(0),
                failed: Mutex::new(None),
            }),
        }
    }

    /// The introspected tables, keyed by id.
    pub(crate) fn tables(&self) -> Vec<Arc<IntrospectedTable>> {
        self.state.names_by_id.values().cloned().collect()
    }

    pub(crate) fn table_by_id(&self, id: TableId) -> Option<Arc<IntrospectedTable>> {
        self.state.names_by_id.get(&id).cloned()
    }

    /// Latches a fatal error; every subsequent snapshot/cursor call
    /// fails with it.
    pub(crate) fn fail(&self, message: String) {
        tracing::error!(%message, "postgres runtime entering failed state");
        let mut failed = self.state.failed.lock().expect("failed latch");
        failed.get_or_insert(message);
    }

    fn check_failed(&self) -> Result<(), String> {
        self.state
            .failed
            .lock()
            .expect("failed latch")
            .clone()
            .map_or(Ok(()), Err)
    }

    /// Current logical clock (last applied commit LSN).
    #[must_use]
    pub fn current_lsn(&self) -> Lsn {
        Lsn::new(self.state.clock.load(Ordering::SeqCst))
    }

    /// Seeds the mirror from the initial snapshot. No journal entries:
    /// subscribers see snapshot rows through `fetch_snapshot`.
    pub(crate) fn seed_snapshot(&self, lsn: u64, rows: BTreeMap<TableId, Vec<Row>>) {
        *self.state.mirror.lock().expect("mirror") = rows;
        self.state.clock.store(lsn, Ordering::SeqCst);
    }

    /// Applies one committed transaction: updates the mirror and
    /// appends the row diffs to the journal at `commit_lsn`.
    pub(crate) fn apply_transaction(
        &self,
        commit_lsn: u64,
        changes: Vec<(TableId, RowOp, Option<Tuple>, Option<Tuple>)>,
    ) {
        if changes.is_empty() {
            return;
        }
        let lsn = Lsn::new(commit_lsn);
        let mut mirror = self.state.mirror.lock().expect("mirror");
        let mut journal = self.state.journal.lock().expect("journal");
        for (table, op, old, new) in changes {
            let rows = mirror.entry(table).or_default();
            match op {
                RowOp::Insert => {
                    if let Some(new) = new {
                        let row = tuple_to_row(new, None);
                        rows.push(row.clone());
                        journal.push(RawDiff {
                            table: Some(table),
                            row,
                            lsn,
                            diff: 1,
                        });
                    }
                }
                RowOp::Update => {
                    let (Some(old), Some(new)) = (old, new) else {
                        continue;
                    };
                    let old_row = tuple_to_row(old, None);
                    // TOAST placeholders in the new image resolve
                    // against the old row (REPLICA IDENTITY FULL
                    // guarantees we have it).
                    let new_row = tuple_to_row(new, Some(&old_row));
                    if let Some(position) = rows.iter().position(|row| *row == old_row) {
                        rows[position] = new_row.clone();
                    } else {
                        rows.push(new_row.clone());
                    }
                    journal.push(RawDiff {
                        table: Some(table),
                        row: old_row,
                        lsn,
                        diff: -1,
                    });
                    journal.push(RawDiff {
                        table: Some(table),
                        row: new_row,
                        lsn,
                        diff: 1,
                    });
                }
                RowOp::Delete => {
                    if let Some(old) = old {
                        let old_row = tuple_to_row(old, None);
                        if let Some(position) = rows.iter().position(|row| *row == old_row) {
                            rows.swap_remove(position);
                        }
                        journal.push(RawDiff {
                            table: Some(table),
                            row: old_row,
                            lsn,
                            diff: -1,
                        });
                    }
                }
            }
        }
        drop(journal);
        drop(mirror);
        self.state.clock.store(commit_lsn, Ordering::SeqCst);
    }

    /// Applies a `TRUNCATE` of `tables` at `commit_lsn`: retracts
    /// every mirrored row.
    pub(crate) fn apply_truncate(&self, commit_lsn: u64, tables: &[TableId]) {
        let lsn = Lsn::new(commit_lsn);
        let mut mirror = self.state.mirror.lock().expect("mirror");
        let mut journal = self.state.journal.lock().expect("journal");
        for table in tables {
            let rows = mirror.entry(*table).or_default();
            for row in rows.drain(..) {
                journal.push(RawDiff {
                    table: Some(*table),
                    row,
                    lsn,
                    diff: -1,
                });
            }
        }
        drop(journal);
        drop(mirror);
        self.state.clock.store(commit_lsn, Ordering::SeqCst);
    }

    /// Reconciles a fresh snapshot against the mirror after a
    /// reconnect: computes the bag difference per table and emits it
    /// as one transaction at `lsn`, then adopts the snapshot as the
    /// new mirror.
    pub(crate) fn reconcile_snapshot(&self, lsn: u64, fresh: BTreeMap<TableId, Vec<Row>>) {
        let mut mirror = self.state.mirror.lock().expect("mirror");
        let mut journal = self.state.journal.lock().expect("journal");
        let at = Lsn::new(lsn);
        for &table in self.state.names_by_id.keys() {
            let old_rows = mirror.remove(&table).unwrap_or_default();
            let new_rows = fresh.get(&table).cloned().unwrap_or_default();

            let mut counts: HashMap<Row, i64> = HashMap::new();
            for row in &new_rows {
                *counts.entry(row.clone()).or_insert(0) += 1;
            }
            for row in old_rows {
                *counts.entry(row).or_insert(0) -= 1;
            }
            for (row, count) in counts {
                let diff = if count > 0 { 1 } else { -1 };
                for _ in 0..count.abs() {
                    journal.push(RawDiff {
                        table: Some(table),
                        row: row.clone(),
                        lsn: at,
                        diff,
                    });
                }
            }
            mirror.insert(table, new_rows);
        }
        drop(journal);
        drop(mirror);
        self.state.clock.store(lsn, Ordering::SeqCst);
    }

    /// Base tables referenced by `query`, resolved against the
    /// streamed set.
    fn query_tables(&self, query: &QueryId) -> Result<Vec<Arc<IntrospectedTable>>, String> {
        let graph = palimpsest_sql::parse_and_lower(query.as_str())
            .map_err(|err| format!("query does not parse: {err}"))?;
        let mut tables = Vec::new();
        let mut seen = BTreeSet::new();
        for index in graph.base_table_indices() {
            if let palimpsest_sql::mir::MirNodeKind::BaseTable { table, .. } =
                graph.node_kind(index)
            {
                if !seen.insert(table.clone()) {
                    continue;
                }
                let resolved = self.state.tables_by_name.get(table).ok_or_else(|| {
                    format!(
                        "table '{table}' is not in the streamed set (derived from the query registry)"
                    )
                })?;
                tables.push(Arc::clone(resolved));
            }
        }
        Ok(tables)
    }
}

/// Converts a decoded pgoutput tuple into a dataflow row, resolving
/// `Datum::Unchanged` TOAST placeholders against `old` when available.
fn tuple_to_row(tuple: Tuple, old: Option<&Row>) -> Row {
    tuple
        .into_iter()
        .enumerate()
        .map(|(index, datum)| match datum {
            Datum::Unchanged => old
                .and_then(|row| row.get(index).cloned())
                .unwrap_or(Datum::Null),
            other => other,
        })
        .collect()
}

impl WalRuntime for PostgresWalRuntime {
    fn fetch_snapshot(&self, query: &QueryId) -> Result<SnapshotBatch, String> {
        self.check_failed()?;
        let tables = self.query_tables(query)?;
        let mirror = self.state.mirror.lock().expect("mirror");
        let rows = tables
            .iter()
            .map(|table| SnapshotTableRows {
                table: table.id,
                rows: mirror.get(&table.id).cloned().unwrap_or_default(),
            })
            .collect();
        drop(mirror);
        Ok(SnapshotBatch {
            snapshot_lsn: self.current_lsn(),
            rows,
        })
    }

    fn query_schema(&self, query: &QueryId) -> Result<SchemaDefinition, String> {
        self.check_failed()?;
        // Derive the result shape from the compiled plan — the same
        // catalog-driven source the dataflow itself uses, so clients
        // can trust it for row-type generation.
        let graph = palimpsest_sql::parse_and_lower(query.as_str())
            .map_err(|err| format!("query does not parse: {err}"))?;
        let lookup = |table: &str| self.table_schema(table);
        let plan = palimpsest_dataflow::palimpsest::compile_mir::compile_mir(&graph, &lookup)
            .map_err(|err| format!("query does not compile: {err}"))?;
        let columns = plan
            .output_schema
            .columns()
            .iter()
            .map(|(name, column_type)| ColumnSpec {
                name: name.clone(),
                datum_type: wire_datum_type(*column_type),
                nullable: true,
            })
            .collect();
        Ok(SchemaDefinition {
            id: SchemaId::new(0),
            columns,
            primary_key_columns: vec![0],
        })
    }

    fn open_cursor(
        &self,
        query: &QueryId,
        from_lsn: Lsn,
    ) -> Result<Box<dyn TraceCursor + Send>, String> {
        self.check_failed()?;
        let tables: BTreeSet<TableId> = self
            .query_tables(query)?
            .iter()
            .map(|table| table.id)
            .collect();
        let journal = self.state.journal.lock().expect("journal");
        if journal.truncated_through > from_lsn.get() {
            return Err(format!(
                "diff journal no longer covers LSN {} (truncated through {}); resubscribe for a fresh snapshot",
                from_lsn.get(),
                journal.truncated_through
            ));
        }
        let next_seq = journal.base_seq;
        drop(journal);
        Ok(Box::new(JournalCursor {
            state: Arc::clone(&self.state),
            next_seq,
            from_lsn,
            tables,
            poisoned: false,
        }))
    }

    fn table_schema(&self, table: &str) -> Option<(TableId, ScalarSchema)> {
        let table = self.state.tables_by_name.get(table)?;
        Some((table.id, table.scalar_schema()))
    }
}

/// Wire datum type advertised for a compiled output column.
fn wire_datum_type(column_type: ColumnType) -> DatumType {
    match column_type {
        ColumnType::Bool => DatumType::Bool,
        ColumnType::Int => DatumType::I64,
        ColumnType::Float => DatumType::F64,
        ColumnType::Numeric => DatumType::Numeric,
        ColumnType::Timestamp => DatumType::Timestamp,
        ColumnType::TimestampTz => DatumType::TimestampTz,
        ColumnType::Date => DatumType::Date,
        ColumnType::Time => DatumType::Time,
        ColumnType::Interval => DatumType::Interval,
        ColumnType::Uuid => DatumType::Uuid,
        ColumnType::Jsonb => DatumType::Jsonb,
        ColumnType::Bytea => DatumType::Bytea,
        ColumnType::Array => DatumType::Array(Box::new(DatumType::Text)),
        ColumnType::Text | ColumnType::Enum | ColumnType::Unknown => DatumType::Text,
    }
}

/// Cursor over the shared diff journal, filtered to one query's
/// tables and anchored at the subscriber's snapshot LSN.
struct JournalCursor {
    state: Arc<SharedState>,
    next_seq: u64,
    from_lsn: Lsn,
    tables: BTreeSet<TableId>,
    poisoned: bool,
}

impl JournalCursor {
    /// True when `entry` is one this cursor should surface.
    fn relevant(&self, entry: &RawDiff) -> bool {
        entry.lsn > self.from_lsn
            && entry
                .table
                .is_some_and(|table| self.tables.contains(&table))
    }
}

impl TraceCursor for JournalCursor {
    fn next_diff(&mut self) -> Option<RawDiff> {
        if self.poisoned {
            return None;
        }
        let journal = self.state.journal.lock().expect("journal");
        if self.next_seq < journal.base_seq {
            // The journal dropped entries this cursor never saw.
            // Never serve a gap: park permanently and let the
            // subscriber's staleness handling resubscribe.
            self.poisoned = true;
            tracing::error!(
                next_seq = self.next_seq,
                base_seq = journal.base_seq,
                "journal truncated under a live cursor; parking it"
            );
            return None;
        }
        while let Some(entry) = journal
            .entries
            .get(usize::try_from(self.next_seq - journal.base_seq).ok()?)
        {
            self.next_seq += 1;
            if self.relevant(entry) {
                return Some(entry.clone());
            }
        }
        None
    }

    fn peek_lsn(&self) -> Option<Lsn> {
        if self.poisoned {
            return None;
        }
        let journal = self.state.journal.lock().expect("journal");
        if self.next_seq < journal.base_seq {
            return None;
        }
        let mut index = usize::try_from(self.next_seq - journal.base_seq).ok()?;
        while let Some(entry) = journal.entries.get(index) {
            if self.relevant(entry) {
                return Some(entry.lsn);
            }
            index += 1;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::introspect::IntrospectedColumn;
    use palimpsest_wal::ReplicaIdentity;
    use smallvec::smallvec;

    fn tickets_table() -> IntrospectedTable {
        IntrospectedTable {
            id: TableId::new(101),
            namespace: "public".to_owned(),
            name: "tickets".to_owned(),
            replica_identity: ReplicaIdentity::Full,
            columns: vec![
                IntrospectedColumn {
                    name: "id".to_owned(),
                    type_oid: palimpsest_wal::INT8_OID,
                    datum_type: DatumType::I64,
                    column_type: ColumnType::Int,
                    nullable: false,
                    primary_key: true,
                },
                IntrospectedColumn {
                    name: "title".to_owned(),
                    type_oid: palimpsest_wal::TEXT_OID,
                    datum_type: DatumType::Text,
                    column_type: ColumnType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
        }
    }

    fn runtime() -> PostgresWalRuntime {
        PostgresWalRuntime::from_tables(vec![tickets_table()], 8)
    }

    fn row(id: i64, title: &str) -> Row {
        smallvec![
            Datum::I64(id),
            Datum::Text(bytes::Bytes::copy_from_slice(title.as_bytes()))
        ]
    }

    #[test]
    fn snapshot_serves_mirror_rows_for_query_tables() {
        let runtime = runtime();
        let mut seed = BTreeMap::new();
        seed.insert(TableId::new(101), vec![row(1, "a"), row(2, "b")]);
        runtime.seed_snapshot(7, seed);

        let batch = runtime
            .fetch_snapshot(&QueryId::new("SELECT id, title FROM tickets"))
            .expect("snapshot");
        assert_eq!(batch.snapshot_lsn, Lsn::new(7));
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].table, TableId::new(101));
        assert_eq!(batch.rows[0].rows.len(), 2);

        let err = runtime
            .fetch_snapshot(&QueryId::new("SELECT id FROM unstreamed"))
            .expect_err("unstreamed table refused");
        assert!(err.contains("unstreamed"), "{err}");
    }

    #[test]
    fn update_resolves_toast_placeholder_from_old_row() {
        let runtime = runtime();
        let mut seed = BTreeMap::new();
        seed.insert(TableId::new(101), vec![row(1, "big-toasted-title")]);
        runtime.seed_snapshot(1, seed);

        // New image carries Unchanged for the untouched toasted column.
        runtime.apply_transaction(
            2,
            vec![(
                TableId::new(101),
                RowOp::Update,
                Some(smallvec![
                    Datum::I64(1),
                    Datum::Text(bytes::Bytes::from_static(b"big-toasted-title"))
                ]),
                Some(smallvec![Datum::I64(1), Datum::Unchanged]),
            )],
        );

        let mut cursor = runtime
            .open_cursor(&QueryId::new("SELECT id, title FROM tickets"), Lsn::new(1))
            .expect("cursor");
        let retract = cursor.next_diff().expect("retraction");
        assert_eq!(retract.diff, -1);
        let assert_diff = cursor.next_diff().expect("assertion");
        assert_eq!(assert_diff.diff, 1);
        assert_eq!(
            assert_diff.row,
            row(1, "big-toasted-title"),
            "TOAST placeholder must resolve to the old value"
        );
    }

    #[test]
    fn cursor_is_anchored_and_filters_by_table() {
        let runtime = runtime();
        runtime.seed_snapshot(5, BTreeMap::new());
        runtime.apply_transaction(
            6,
            vec![(
                TableId::new(101),
                RowOp::Insert,
                None,
                Some(smallvec![
                    Datum::I64(1),
                    Datum::Text(bytes::Bytes::from_static(b"x"))
                ]),
            )],
        );

        // Anchored at 6: the LSN-6 insert is already in the snapshot.
        let mut cursor = runtime
            .open_cursor(&QueryId::new("SELECT id FROM tickets"), Lsn::new(6))
            .expect("cursor");
        assert!(cursor.next_diff().is_none());

        // Anchored at 5: the insert surfaces as a live diff.
        let mut cursor = runtime
            .open_cursor(&QueryId::new("SELECT id FROM tickets"), Lsn::new(5))
            .expect("cursor");
        let diff = cursor.next_diff().expect("diff after anchor");
        assert_eq!(diff.lsn, Lsn::new(6));
        assert_eq!(diff.diff, 1);
    }

    #[test]
    fn truncated_resume_is_refused_not_served_with_a_gap() {
        let runtime = runtime(); // journal capacity 8
        runtime.seed_snapshot(1, BTreeMap::new());
        for i in 0..12_i64 {
            runtime.apply_transaction(
                2 + u64::try_from(i).expect("small"),
                vec![(
                    TableId::new(101),
                    RowOp::Insert,
                    None,
                    Some(smallvec![
                        Datum::I64(i),
                        Datum::Text(bytes::Bytes::from_static(b"x"))
                    ]),
                )],
            );
        }

        // 12 diffs through a capacity-8 journal: the first 4 dropped.
        let Err(err) = runtime.open_cursor(&QueryId::new("SELECT id FROM tickets"), Lsn::new(2))
        else {
            panic!("resume into the truncated window must be refused");
        };
        assert!(err.contains("truncated"), "{err}");

        // Resuming after the truncation point still works.
        let cursor = runtime.open_cursor(
            &QueryId::new("SELECT id FROM tickets"),
            runtime.current_lsn(),
        );
        assert!(cursor.is_ok());
    }

    #[test]
    fn reconcile_emits_only_the_difference() {
        let runtime = runtime();
        let mut seed = BTreeMap::new();
        seed.insert(TableId::new(101), vec![row(1, "keep"), row(2, "gone")]);
        runtime.seed_snapshot(3, seed);

        let mut fresh = BTreeMap::new();
        fresh.insert(TableId::new(101), vec![row(1, "keep"), row(3, "new")]);
        runtime.reconcile_snapshot(9, fresh);

        let mut cursor = runtime
            .open_cursor(&QueryId::new("SELECT id, title FROM tickets"), Lsn::new(3))
            .expect("cursor");
        let mut diffs = Vec::new();
        while let Some(diff) = cursor.next_diff() {
            diffs.push(diff);
        }
        assert_eq!(diffs.len(), 2, "unchanged row must not be re-emitted");
        assert!(diffs
            .iter()
            .any(|d| d.diff == -1 && d.row == row(2, "gone")));
        assert!(diffs.iter().any(|d| d.diff == 1 && d.row == row(3, "new")));
        assert!(diffs.iter().all(|d| d.lsn == Lsn::new(9)));

        let batch = runtime
            .fetch_snapshot(&QueryId::new("SELECT id, title FROM tickets"))
            .expect("snapshot");
        assert_eq!(batch.rows[0].rows.len(), 2);
    }

    #[test]
    fn failed_latch_refuses_snapshots_and_cursors() {
        let runtime = runtime();
        runtime.fail("schema drift on tickets".to_owned());
        let err = runtime
            .fetch_snapshot(&QueryId::new("SELECT id FROM tickets"))
            .expect_err("failed runtime refuses snapshots");
        assert!(err.contains("drift"), "{err}");
        assert!(runtime
            .open_cursor(&QueryId::new("SELECT id FROM tickets"), Lsn::new(0))
            .is_err());
    }

    #[test]
    fn query_schema_describes_compiled_output_shape() {
        let runtime = runtime();
        let schema = runtime
            .query_schema(&QueryId::new(
                "SELECT title, COUNT(*) AS n FROM tickets GROUP BY title",
            ))
            .expect("schema");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "title");
        assert_eq!(schema.columns[0].datum_type, DatumType::Text);
        assert_eq!(schema.columns[1].name, "n");
        assert_eq!(schema.columns[1].datum_type, DatumType::I64);
    }
}
