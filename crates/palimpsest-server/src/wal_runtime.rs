// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pluggable WAL runtime that the embeddable server depends on.
//!
//! The router itself is transport- and source-agnostic: it consumes a
//! [`SnapshotProvider`] and a [`TraceCursor`] per subscription. The
//! gRPC adapter wires both halves through this trait, which a future
//! `palimpsest-wal-runtime` module implements on top of the real
//! Postgres replication slot.
//!
//! For v1 we ship a stub implementation ([`EmptyWalRuntime`]) that lets
//! the embedded server boot and serve subscribe / ack / unsubscribe
//! traffic without an upstream Postgres. The CLI uses it for dev mode
//! and the integration tests use it to drive the gRPC bidi loop.

use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_wal::{DatumType, TableId};

use crate::cursor::{TraceCursor, VecCursor};
use crate::snapshot::{SnapshotBatch, SnapshotProvider};
use crate::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};

/// Combined snapshot + cursor source for one query.
///
/// The router calls [`Self::fetch_snapshot`] once at subscribe time and
/// uses [`Self::open_cursor`] to drain trace updates while the
/// subscription is live.
pub trait WalRuntime: Send + Sync + 'static {
    /// Fetches the initial snapshot for `query`. Returns the rows visible
    /// at `snapshot_lsn`.
    ///
    /// # Errors
    /// Implementation-defined; see [`SnapshotProvider::fetch`].
    fn fetch_snapshot(&self, query: &QueryId) -> Result<SnapshotBatch, String>;

    /// Returns the result-row schema for `query`, attached to the
    /// `Accepted` message so the client can decode subsequent diffs.
    ///
    /// # Errors
    /// Implementation-defined; signals an unknown query.
    fn query_schema(&self, query: &QueryId) -> Result<SchemaDefinition, String>;

    /// Opens a fresh trace cursor for `query`, replaying from
    /// `from_lsn`.
    ///
    /// # Errors
    /// Implementation-defined.
    fn open_cursor(
        &self,
        query: &QueryId,
        from_lsn: Lsn,
    ) -> Result<Box<dyn TraceCursor + Send>, String>;

    /// Resolve `table` to its `(TableId, ScalarSchema)` pair. Used by
    /// the MIR compiler to bind `BaseTable` nodes to typed inputs.
    /// Default returns `None`, which preserves the legacy raw-row
    /// path for runtimes that don't yet expose typed table schemas.
    fn table_schema(&self, _table: &str) -> Option<(TableId, ScalarSchema)> {
        None
    }
}

/// Bridge that lets a [`WalRuntime`] satisfy the
/// [`SnapshotProvider`] contract without needing a separate adapter.
impl<T: WalRuntime + ?Sized> SnapshotProvider for T {
    fn fetch(&self, query: &QueryId) -> Result<SnapshotBatch, String> {
        self.fetch_snapshot(query)
    }
}

/// Stub runtime that returns empty snapshots and never produces diffs.
///
/// Used in dev/CI builds where no Postgres is attached. The server
/// still accepts subscribes and routes acks; it just never emits
/// `Update` events.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyWalRuntime {
    /// LSN reported as the snapshot's clock. Defaults to 0.
    snapshot_lsn: Lsn,
}

impl EmptyWalRuntime {
    /// Builds an empty runtime that reports `snapshot_lsn` for every
    /// query.
    #[must_use]
    pub const fn at(snapshot_lsn: Lsn) -> Self {
        Self { snapshot_lsn }
    }
}

impl WalRuntime for EmptyWalRuntime {
    fn fetch_snapshot(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        Ok(SnapshotBatch {
            snapshot_lsn: self.snapshot_lsn,
            rows: Vec::new(),
        })
    }

    fn query_schema(&self, _query: &QueryId) -> Result<SchemaDefinition, String> {
        // Stub schema: a single `id` column. Real runtimes derive this
        // from their catalog.
        Ok(SchemaDefinition {
            id: SchemaId::new(0),
            columns: vec![ColumnSpec {
                name: "id".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            }],
            primary_key_columns: vec![0],
        })
    }

    fn open_cursor(
        &self,
        _query: &QueryId,
        _from_lsn: Lsn,
    ) -> Result<Box<dyn TraceCursor + Send>, String> {
        Ok(Box::new(VecCursor::new(std::iter::empty())))
    }
}

#[cfg(test)]
mod tests {
    use palimpsest_dataflow::palimpsest::Lsn;

    use super::{EmptyWalRuntime, WalRuntime};
    use crate::subscription::QueryId;

    #[test]
    fn empty_runtime_returns_empty_snapshot_with_configured_lsn() {
        let runtime = EmptyWalRuntime::at(Lsn::new(42));
        let batch = runtime.fetch_snapshot(&QueryId::new("q")).unwrap();
        assert_eq!(batch.snapshot_lsn, Lsn::new(42));
        assert!(batch.rows.is_empty());
    }

    #[test]
    fn empty_runtime_open_cursor_yields_no_batches() {
        let runtime = EmptyWalRuntime::default();
        let mut cursor = runtime
            .open_cursor(&QueryId::new("q"), Lsn::new(0))
            .unwrap();
        assert!(cursor.next_batch().is_none());
    }

    #[test]
    fn empty_runtime_returns_stub_schema() {
        let schema = EmptyWalRuntime::default()
            .query_schema(&QueryId::new("q"))
            .unwrap();
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.primary_key_columns, vec![0]);
    }
}
