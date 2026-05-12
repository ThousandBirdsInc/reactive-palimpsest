//! In-memory `posts` table plus a `WalRuntime` adapter that exposes
//! that state to Palimpsest's snapshot path *and* a per-mutation
//! journal that drives live diffs.
//!
//! In a real deployment this is replaced by a Postgres logical-decoding
//! runtime; for the demo we just hold rows in a `Mutex<Vec>`, bump a
//! monotonic LSN counter on every write, and append `RawDiff` entries
//! to a shared journal. The Palimpsest server spawns a per-subscription
//! task that drains the journal via `open_cursor` and pumps batches
//! into the router so every active subscription sees the change.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use palimpsest_dataflow::palimpsest::{Lsn, Row};
use palimpsest_server::cursor::RawDiff;
use palimpsest_server::snapshot::{SnapshotBatch, SnapshotTableRows};
use palimpsest_server::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_server::TraceCursor;
use palimpsest_wal::{Datum, DatumType, TableId};
use serde::Serialize;
use smallvec::smallvec;

/// Postgres-style relation id we report for the synthetic `posts` table.
/// The actual value is irrelevant — palimpsest doesn't dereference it,
/// it's just an opaque key on the snapshot side.
const POSTS_TABLE_ID: u32 = 16384;

#[derive(Debug, Clone, Serialize)]
pub struct Post {
    pub id: i64,
    pub title: String,
    pub published: bool,
}

impl Post {
    /// Build the row vector palimpsest sees for this post. Column
    /// order must match [`DemoWalRuntime::query_schema`].
    fn to_row(&self) -> Row {
        smallvec![
            Datum::I64(self.id),
            Datum::Text(self.title.clone().into_bytes().into()),
            Datum::Bool(self.published),
        ]
    }
}

pub struct Store {
    rows: Mutex<Vec<Post>>,
    next_id: AtomicU64,
    lsn: AtomicU64,
    /// Append-only diff journal. Every mutation pushes one entry per
    /// row delta (an update emits a -1 + +1 pair at the same LSN).
    /// Grows unbounded for the demo — real deployments compact behind
    /// the global ack watermark.
    journal: Arc<Mutex<Vec<RawDiff>>>,
}

impl Store {
    pub fn with_seed(seed: Vec<Post>) -> Self {
        let next_id = seed.iter().map(|p| p.id).max().unwrap_or(0) + 1;
        Self {
            rows: Mutex::new(seed),
            next_id: AtomicU64::new(u64::try_from(next_id).unwrap_or(1)),
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

    fn record(&self, lsn: Lsn, row: Row, diff: i64) {
        self.journal
            .lock()
            .expect("journal poisoned")
            .push(RawDiff { row, lsn, diff });
    }

    pub fn create(&self, title: String, published: bool) -> Post {
        let id = i64::try_from(self.next_id.fetch_add(1, Ordering::SeqCst))
            .expect("id within i64 range");
        let post = Post {
            id,
            title,
            published,
        };
        self.rows.lock().expect("store poisoned").push(post.clone());
        let lsn = self.bump_lsn();
        self.record(lsn, post.to_row(), 1);
        post
    }

    pub fn set_published(&self, id: i64, published: bool) -> Option<Post> {
        let mut rows = self.rows.lock().expect("store poisoned");
        let row = rows.iter_mut().find(|p| p.id == id)?;
        let old = row.clone();
        row.published = published;
        let updated = row.clone();
        drop(rows);
        let lsn = self.bump_lsn();
        // Update = retract old + assert new at the same LSN; the router
        // pairs them into a `RowChange::Update` keyed by the primary
        // key column(s).
        self.record(lsn, old.to_row(), -1);
        self.record(lsn, updated.to_row(), 1);
        Some(updated)
    }

    pub fn delete(&self, id: i64) -> bool {
        let mut rows = self.rows.lock().expect("store poisoned");
        let removed = rows
            .iter()
            .position(|p| p.id == id)
            .map(|idx| rows.remove(idx));
        drop(rows);
        match removed {
            Some(post) => {
                let lsn = self.bump_lsn();
                self.record(lsn, post.to_row(), -1);
                true
            }
            None => false,
        }
    }
}

/// `WalRuntime` adapter over [`Store`]. Returns the current rows as the
/// snapshot for every query (the demo wires a single `SELECT … FROM
/// posts` subscription on the frontend); reports a fixed three-column
/// schema; and exposes the Store's journal as a [`JournalCursor`] so
/// the server can stream live diffs.
pub struct DemoWalRuntime {
    store: Arc<Store>,
}

impl DemoWalRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

impl WalRuntime for DemoWalRuntime {
    fn fetch_snapshot(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        let posts = self.store.snapshot();
        let rows = posts
            .into_iter()
            .map(|post| post.to_row())
            .collect();
        Ok(SnapshotBatch {
            snapshot_lsn: self.store.current_lsn(),
            rows: vec![SnapshotTableRows {
                table: TableId::new(POSTS_TABLE_ID),
                rows,
            }],
        })
    }

    fn query_schema(&self, _query: &QueryId) -> Result<SchemaDefinition, String> {
        Ok(SchemaDefinition {
            id: SchemaId::new(0), // overwritten by the router
            columns: vec![
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
            primary_key_columns: vec![0],
        })
    }

    fn open_cursor(
        &self,
        _query: &QueryId,
        from_lsn: Lsn,
    ) -> Result<Box<dyn TraceCursor + Send>, String> {
        // The cursor is anchored by LSN, not by index, so a write that
        // landed between `fetch_snapshot` returning and us reading the
        // journal is handled correctly — its entry has lsn > from_lsn
        // and gets re-emitted as a live diff. Entries already covered
        // by the snapshot (lsn ≤ from_lsn) are skipped.
        Ok(Box::new(JournalCursor {
            journal: self.store.journal_handle(),
            next_index: 0,
            from_lsn,
        }))
    }
}

/// Live cursor over [`Store`]'s diff journal.
///
/// Holds a shared handle to the journal vector plus a per-cursor read
/// index. `next_diff` skips entries with `lsn <= from_lsn` so the
/// initial snapshot isn't re-emitted as diffs.
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
