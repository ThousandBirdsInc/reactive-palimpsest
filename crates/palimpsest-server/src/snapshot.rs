// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Initial-snapshot support.
//!
//! When a subscription accepts, the router has to seed the dataflow
//! input with the rows that exist as of `snapshot_lsn` *before*
//! switching the cursor into streaming mode. This module owns:
//!
//! * The [`SnapshotProvider`] trait — implemented by Postgres-aware
//!   code in the host application; the router itself never opens a
//!   network connection.
//! * [`snapshot_to_seed_updates`] — the conversion from a
//!   [`SnapshotBatch`] into the differential updates the dataflow
//!   wants (`+1` weight at `snapshot_lsn`).
//!
//! Splitting the provider behind a trait keeps the router unit-testable
//! against in-memory fixtures and lets us swap in the real Postgres
//! pull path later (Phase 3 §18.8).

use palimpsest_dataflow::palimpsest::{Lsn, Row, WalUpdate};
use palimpsest_wal::TableId;

use crate::error::RouterError;
use crate::subscription::QueryId;

/// One snapshot-row batch returned by a [`SnapshotProvider`].
///
/// The router seeds *all* rows for the query at the same `snapshot_lsn`
/// so the dataflow's input frontier advances past the seed in one
/// timely tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBatch {
    /// Logical clock at which the snapshot was taken (the value the
    /// `Accepted` message will report).
    pub snapshot_lsn: Lsn,
    /// One entry per base table contributing to the query.
    pub rows: Vec<SnapshotTableRows>,
}

/// Rows for a single base table at `snapshot_lsn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTableRows {
    /// Postgres relation identifier the rows came from.
    pub table: TableId,
    /// Row values, in projected-column order.
    pub rows: Vec<Row>,
}

/// Pulls an initial snapshot for a registered query.
///
/// Implementations issue `SELECT … FROM <table>` against Postgres at
/// `snapshot_lsn`, then return the rows + the LSN. The router sequences
/// the result into the dataflow before switching to streaming.
pub trait SnapshotProvider {
    /// Materializes the initial snapshot for the canonical query
    /// identified by `query`.
    ///
    /// # Errors
    /// Implementation-specific. The router wraps any failure in
    /// [`RouterError::Snapshot`].
    fn fetch(&self, query: &QueryId) -> Result<SnapshotBatch, String>;
}

/// Converts a snapshot batch into a flat list of dataflow updates.
///
/// Each row becomes a `WalUpdate` with `diff = +1` at `snapshot_lsn`,
/// matching the ordering used by [`palimpsest_dataflow::WalSourceState`]
/// for fresh inserts after a transaction commit.
#[must_use]
pub fn snapshot_to_seed_updates(batch: &SnapshotBatch) -> Vec<WalUpdate> {
    let total: usize = batch.rows.iter().map(|t| t.rows.len()).sum();
    let mut updates = Vec::with_capacity(total);
    for table_rows in &batch.rows {
        for row in &table_rows.rows {
            let row: Row = row.clone();
            updates.push(WalUpdate::new(table_rows.table, row, batch.snapshot_lsn, 1));
        }
    }
    updates
}

/// Convenience wrapper: pulls a snapshot from `provider` and converts
/// the rows into seed updates in one step.
///
/// # Errors
/// Returns [`RouterError::Snapshot`] when the provider surfaces an
/// error.
pub fn snapshot_seed<P: SnapshotProvider + ?Sized>(
    provider: &P,
    query: &QueryId,
) -> Result<(SnapshotBatch, Vec<WalUpdate>), RouterError> {
    let batch = provider.fetch(query).map_err(RouterError::Snapshot)?;
    let updates = snapshot_to_seed_updates(&batch);
    Ok((batch, updates))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use palimpsest_dataflow::palimpsest::Lsn;
    use palimpsest_wal::{Datum, TableId};
    use smallvec::smallvec;

    use super::{
        snapshot_seed, snapshot_to_seed_updates, SnapshotBatch, SnapshotProvider, SnapshotTableRows,
    };
    use crate::{error::RouterError, subscription::QueryId};

    fn batch() -> SnapshotBatch {
        SnapshotBatch {
            snapshot_lsn: Lsn::new(42),
            rows: vec![
                SnapshotTableRows {
                    table: TableId::new(1),
                    rows: vec![
                        smallvec![Datum::I64(1), Datum::I64(10)],
                        smallvec![Datum::I64(2), Datum::I64(11)],
                    ],
                },
                SnapshotTableRows {
                    table: TableId::new(2),
                    rows: vec![smallvec![Datum::I64(7)]],
                },
            ],
        }
    }

    #[test]
    fn seed_updates_carry_snapshot_lsn_and_unit_diff() {
        let updates = snapshot_to_seed_updates(&batch());
        assert_eq!(updates.len(), 3);
        for update in &updates {
            assert_eq!(update.time, Lsn::new(42));
            assert_eq!(update.diff, 1);
        }
    }

    #[test]
    fn seed_updates_preserve_table_origin() {
        let updates = snapshot_to_seed_updates(&batch());
        let tables: Vec<TableId> = updates.iter().map(|u| u.table).collect();
        assert_eq!(
            tables,
            vec![TableId::new(1), TableId::new(1), TableId::new(2)],
        );
    }

    struct StubProvider {
        responses: RefCell<Vec<Result<SnapshotBatch, String>>>,
    }

    impl SnapshotProvider for StubProvider {
        fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
            self.responses
                .borrow_mut()
                .pop()
                .expect("no stubbed response left")
        }
    }

    #[test]
    fn snapshot_seed_returns_batch_and_seed_updates() {
        let provider = StubProvider {
            responses: RefCell::new(vec![Ok(batch())]),
        };
        let (returned, updates) = snapshot_seed(&provider, &QueryId::new("posts.recent")).unwrap();
        assert_eq!(returned.snapshot_lsn, Lsn::new(42));
        assert_eq!(updates.len(), 3);
    }

    #[test]
    fn snapshot_seed_propagates_provider_error() {
        let provider = StubProvider {
            responses: RefCell::new(vec![Err("postgres lost connection".to_owned())]),
        };
        let err = snapshot_seed(&provider, &QueryId::new("q")).unwrap_err();
        assert!(matches!(err, RouterError::Snapshot(message) if message.contains("postgres")));
    }
}
