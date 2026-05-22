// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server-side diff types that flow through the per-subscription
//! channel and out to the gRPC transport.

use palimpsest_dataflow::palimpsest::{Lsn, Row};
use serde::{Deserialize, Serialize};

use crate::subscription::{SchemaId, SubscriptionId};

/// Operation kind used in [`DiffPayload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffOp {
    /// First batch of rows produced by the snapshot path.
    Initial,
    /// New row materialized into the result set.
    Insert,
    /// Row already in the set whose value changed.
    Update,
    /// Row leaving the result set.
    Delete,
}

/// One row-level change carried inside a router-level [`DiffEvent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowChange {
    /// Operation kind.
    pub op: DiffOp,
    /// Pre-image (`None` for `Initial` / `Insert`).
    pub old: Option<Row>,
    /// Post-image (`None` for `Delete`).
    pub new: Option<Row>,
}

/// Diff event surfaced to the gRPC transport adapter (§18.7 #2,
/// step 1-2 in §10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffEvent {
    /// One-shot bootstrap: rows present at `snapshot_lsn`.
    Initial {
        /// Snapshot LSN.
        lsn: Lsn,
        /// Rows present at the snapshot.
        rows: Vec<Row>,
    },
    /// Row-level changes since the previous emission.
    Update {
        /// LSN this batch is associated with.
        lsn: Lsn,
        /// Per-row changes.
        changes: Vec<RowChange>,
    },
    /// Complete live update for one upstream transaction.
    TransactionUpdate {
        /// PostgreSQL transaction id, when known.
        transaction_id: Option<u32>,
        /// Begin marker LSN, when known.
        begin_lsn: Option<Lsn>,
        /// Commit LSN for this complete transaction.
        commit_lsn: Lsn,
        /// End marker LSN, when known.
        end_lsn: Option<Lsn>,
        /// Per-row changes caused by the transaction.
        changes: Vec<RowChange>,
    },
    /// The router cannot deliver in-order diffs anymore; client must
    /// reconcile.
    Resync {
        /// Reason the router gave up.
        reason: ResyncReason,
    },
}

/// Why the router emitted a `Resync` instead of a normal diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResyncReason {
    /// Resume LSN is older than the current trace compaction frontier.
    LsnCompacted,
    /// The query schema changed; old payload bytes are no longer valid.
    SchemaChanged,
    /// Bounded channel saturated (`§18.7 #5`).
    Backpressure,
    /// Postgres slot was lost and recreated; everything has to be
    /// resnapshotted.
    SlotRecreated,
    /// Permission rules changed underneath this subscription.
    PermissionsChanged,
}

/// Wire-ready diff payload (cf. `palimpsest-proto::Diff`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffPayload {
    /// Owning subscription.
    pub subscription_id: SubscriptionId,
    /// LSN this payload is associated with.
    pub lsn: Lsn,
    /// Operation kind.
    pub op: DiffOp,
    /// Schema id every encoded row references.
    pub schema_id: SchemaId,
    /// Bincode-encoded `Vec<WireRow>`.
    pub rows: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::{DiffEvent, DiffOp, ResyncReason, RowChange};
    use palimpsest_dataflow::palimpsest::Lsn;
    use palimpsest_wal::Datum;
    use smallvec::smallvec;

    #[test]
    fn diff_event_round_trips_initial() {
        let event = DiffEvent::Initial {
            lsn: Lsn::new(10),
            rows: vec![smallvec![Datum::I64(1)]],
        };
        match event {
            DiffEvent::Initial { lsn, rows } => {
                assert_eq!(lsn, Lsn::new(10));
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected Initial"),
        }
    }

    #[test]
    fn diff_event_resync_carries_reason() {
        let event = DiffEvent::Resync {
            reason: ResyncReason::Backpressure,
        };
        let DiffEvent::Resync { reason } = event else {
            panic!()
        };
        assert_eq!(reason, ResyncReason::Backpressure);
    }

    #[test]
    fn row_change_round_trips() {
        let change = RowChange {
            op: DiffOp::Update,
            old: Some(smallvec![Datum::I64(1)]),
            new: Some(smallvec![Datum::I64(2)]),
        };
        assert_eq!(change.op, DiffOp::Update);
        assert!(change.old.is_some());
        assert!(change.new.is_some());
    }
}
