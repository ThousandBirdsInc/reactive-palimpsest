//! In-memory mirror of the Postgres-backed `posts` + `events` tables,
//! plus the `WalRuntime` adapter Palimpsest reads through.
//!
//! Writes do **not** flow through here — HTTP handlers go straight
//! to Postgres via tokio-postgres. The logical-replication consumer
//! (see `db.rs`) tails the slot, decodes pgoutput frames into
//! `DecodedEvent::Row { op, old, new }`, and pipes those into
//! `Store::apply_*` / `EventStore::apply_*`. The mirror keeps the
//! "current snapshot" in lock-step with the database, and the
//! journal of `RawDiff`s drives the live-diff cursor the dataflow
//! consumes.
//!
//! Two `Store` shapes (posts vs events) instead of one generic
//! type so the row constructors stay tightly typed — the demo only
//! needs these two surfaces and a generic event-keyed store would
//! cost more in plumbing than it pays back in flexibility.

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

use crate::db::{event_to_row, post_to_row};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Post {
    pub id: i64,
    pub title: String,
    pub published: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub id: i64,
    pub category_id: i64,
    pub value: i64,
}

// -----------------------------------------------------------------------------
// Posts mirror — populated from `SELECT * FROM posts` on boot, then
// updated by the replication consumer.
// -----------------------------------------------------------------------------

/// One row change to apply at a transaction boundary.
#[derive(Debug, Clone)]
pub enum PostChange {
    Insert(Post),
    Update { prev: Post, curr: Post },
    Delete(Post),
}

pub struct Store {
    rows: Mutex<Vec<Post>>,
    /// Monotonic clock the journal anchors on. Bumped once per
    /// applied Postgres transaction (one Begin/Commit pair), so a
    /// bulk INSERT that touches 1000 rows produces one cursor-pump
    /// wakeup, not 1000. Deliberately not derived from Postgres LSN
    /// (sparse, not contiguous).
    lsn: AtomicU64,
    journal: Arc<Mutex<Vec<RawDiff>>>,
}

impl Store {
    pub fn from_snapshot(snapshot: Vec<Post>) -> Self {
        Self {
            rows: Mutex::new(snapshot),
            lsn: AtomicU64::new(1),
            journal: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn snapshot(&self) -> Vec<Post> {
        self.rows.lock().expect("store poisoned").clone()
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
    pub fn apply_txn(&self, changes: &[PostChange]) {
        if changes.is_empty() {
            return;
        }
        {
            let mut rows = self.rows.lock().expect("store poisoned");
            for change in changes {
                match change {
                    PostChange::Insert(post) => rows.push(post.clone()),
                    PostChange::Update { curr, .. } => {
                        if let Some(slot) = rows.iter_mut().find(|p| p.id == curr.id) {
                            *slot = curr.clone();
                        }
                    }
                    PostChange::Delete(prev) => {
                        if let Some(idx) = rows.iter().position(|p| p.id == prev.id) {
                            rows.remove(idx);
                        }
                    }
                }
            }
        }
        let lsn = self.bump_lsn();
        let mut journal = self.journal.lock().expect("journal poisoned");
        for change in changes {
            match change {
                PostChange::Insert(post) => journal.push(RawDiff {
                    row: post_to_row(post),
                    lsn,
                    diff: 1,
                }),
                PostChange::Update { prev, curr } => {
                    journal.push(RawDiff {
                        row: post_to_row(prev),
                        lsn,
                        diff: -1,
                    });
                    journal.push(RawDiff {
                        row: post_to_row(curr),
                        lsn,
                        diff: 1,
                    });
                }
                PostChange::Delete(prev) => journal.push(RawDiff {
                    row: post_to_row(prev),
                    lsn,
                    diff: -1,
                }),
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Events mirror — same shape as Store, different row type.
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum EventChange {
    Insert(Event),
    Update { prev: Event, curr: Event },
    Delete(Event),
}

pub struct EventStore {
    rows: Mutex<Vec<Event>>,
    lsn: AtomicU64,
    journal: Arc<Mutex<Vec<RawDiff>>>,
}

impl EventStore {
    pub fn from_snapshot(snapshot: Vec<Event>) -> Self {
        Self {
            rows: Mutex::new(snapshot),
            lsn: AtomicU64::new(1),
            journal: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn snapshot(&self) -> Vec<Event> {
        self.rows.lock().expect("events poisoned").clone()
    }

    pub fn current_lsn(&self) -> Lsn {
        Lsn::new(self.lsn.load(Ordering::SeqCst))
    }

    pub fn row_count(&self) -> usize {
        self.rows.lock().expect("events poisoned").len()
    }

    fn bump_lsn(&self) -> Lsn {
        Lsn::new(self.lsn.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn journal_handle(&self) -> Arc<Mutex<Vec<RawDiff>>> {
        Arc::clone(&self.journal)
    }

    /// Apply all event-row changes in one Postgres transaction at a
    /// single LSN. See `Store::apply_txn` for why batching matters.
    pub fn apply_txn(&self, changes: &[EventChange]) {
        if changes.is_empty() {
            return;
        }
        {
            let mut rows = self.rows.lock().expect("events poisoned");
            for change in changes {
                match change {
                    EventChange::Insert(ev) => rows.push(ev.clone()),
                    EventChange::Update { curr, .. } => {
                        if let Some(slot) = rows.iter_mut().find(|e| e.id == curr.id) {
                            *slot = curr.clone();
                        }
                    }
                    EventChange::Delete(prev) => {
                        if let Some(idx) = rows.iter().position(|e| e.id == prev.id) {
                            rows.remove(idx);
                        }
                    }
                }
            }
        }
        let lsn = self.bump_lsn();
        let mut journal = self.journal.lock().expect("events journal poisoned");
        for change in changes {
            match change {
                EventChange::Insert(ev) => journal.push(RawDiff {
                    row: event_to_row(ev),
                    lsn,
                    diff: 1,
                }),
                EventChange::Update { prev, curr } => {
                    journal.push(RawDiff {
                        row: event_to_row(prev),
                        lsn,
                        diff: -1,
                    });
                    journal.push(RawDiff {
                        row: event_to_row(curr),
                        lsn,
                        diff: 1,
                    });
                }
                EventChange::Delete(prev) => journal.push(RawDiff {
                    row: event_to_row(prev),
                    lsn,
                    diff: -1,
                }),
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Adapter that exposes both mirrors as a single `WalRuntime`.
// -----------------------------------------------------------------------------

pub struct DemoWalRuntime {
    posts: Arc<Store>,
    events: Arc<EventStore>,
    posts_table_id: TableId,
    events_table_id: TableId,
}

impl DemoWalRuntime {
    pub fn new(
        posts: Arc<Store>,
        events: Arc<EventStore>,
        posts_table_id: TableId,
        events_table_id: TableId,
    ) -> Self {
        Self {
            posts,
            events,
            posts_table_id,
            events_table_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Posts,
    EventsAggregate,
}

/// Pick a target table from the SQL we packed into the QueryId.
/// Anything referencing `events` lands on the events mirror; the
/// rest go to posts. Case-insensitive.
fn classify(query: &QueryId) -> Target {
    let sql = query.as_str().to_ascii_lowercase();
    if sql.contains("from events") {
        Target::EventsAggregate
    } else {
        Target::Posts
    }
}

impl WalRuntime for DemoWalRuntime {
    fn fetch_snapshot(&self, query: &QueryId) -> Result<SnapshotBatch, String> {
        match classify(query) {
            Target::Posts => {
                let rows = self.posts.snapshot().iter().map(post_to_row).collect();
                Ok(SnapshotBatch {
                    snapshot_lsn: self.posts.current_lsn(),
                    rows: vec![SnapshotTableRows {
                        table: self.posts_table_id,
                        rows,
                    }],
                })
            }
            Target::EventsAggregate => {
                let rows = self.events.snapshot().iter().map(event_to_row).collect();
                Ok(SnapshotBatch {
                    snapshot_lsn: self.events.current_lsn(),
                    rows: vec![SnapshotTableRows {
                        table: self.events_table_id,
                        rows,
                    }],
                })
            }
        }
    }

    fn query_schema(&self, query: &QueryId) -> Result<SchemaDefinition, String> {
        Ok(SchemaDefinition {
            id: SchemaId::new(0),
            columns: schema_for(query),
            primary_key_columns: vec![0],
        })
    }

    fn open_cursor(
        &self,
        query: &QueryId,
        from_lsn: Lsn,
    ) -> Result<Box<dyn TraceCursor + Send>, String> {
        let journal = match classify(query) {
            Target::Posts => self.posts.journal_handle(),
            Target::EventsAggregate => self.events.journal_handle(),
        };
        Ok(Box::new(JournalCursor {
            journal,
            next_index: 0,
            from_lsn,
        }))
    }

    fn table_schema(&self, table: &str) -> Option<(TableId, ScalarSchema)> {
        match table {
            "posts" => Some((
                self.posts_table_id,
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("title".to_owned(), ColumnType::Text),
                    ("published".to_owned(), ColumnType::Bool),
                ]),
            )),
            "events" => Some((
                self.events_table_id,
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("category_id".to_owned(), ColumnType::Int),
                    ("value".to_owned(), ColumnType::Int),
                ]),
            )),
            _ => None,
        }
    }
}

/// Cursor over a per-table diff journal. Anchored by LSN so a
/// write that races `fetch_snapshot` re-surfaces as a live diff.
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

/// Per-target output column schema. Matches what
/// `WalRuntime::query_schema` advertises in the gRPC `Accepted`
/// payload — though for compiled aggregate queries the dataflow's
/// `CompiledPlan::output_schema` overrides this in
/// `palimpsest_server::handle_subscribe`.
fn schema_for(query: &QueryId) -> Vec<ColumnSpec> {
    match classify(query) {
        Target::Posts => vec![
            ColumnSpec {
                name: "id".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "title".to_owned(),
                datum_type: DatumType::Text,
                nullable: false,
            },
            ColumnSpec {
                name: "published".to_owned(),
                datum_type: DatumType::Bool,
                nullable: false,
            },
        ],
        Target::EventsAggregate => vec![
            ColumnSpec {
                name: "id".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "category_id".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "value".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            },
        ],
    }
}
