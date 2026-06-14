// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Trace-cursor abstraction.
//!
//! The router consumes diffs through the [`TraceCursor`] trait rather
//! than a concrete `palimpsest_dataflow::TraceHandle`. This keeps the
//! crate free of `timely` plumbing (the embed shim in §18.8 wires the
//! actual `TraceAgent`) and lets unit tests drive the router with
//! deterministic in-memory fixtures.
//!
//! Diffs surfaced by a cursor must be **non-decreasing in `lsn`** so
//! [`batch_by_lsn`] can group them per logical clock tick.

use palimpsest_dataflow::palimpsest::{Lsn, Row};

use crate::error::RouterError;

/// One raw, untyped diff record observed by a trace cursor.
///
/// Inserts and deletes carry the same shape, distinguished by `diff`
/// (`+1` / `-1`) — matching the `(D, T, R)` triple that
/// `differential-dataflow` exposes through `Cursor::map_times`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawDiff {
    /// Differential row payload.
    pub row: Row,
    /// Logical clock at which the diff occurred.
    pub lsn: Lsn,
    /// `+1` for inserts, `-1` for deletes.
    pub diff: i64,
}

/// Iterator-style adapter over diffs surfaced by a trace agent.
pub trait TraceCursor {
    /// Returns the next diff, or `None` when no diff is available right
    /// now (the router will park the task until activated again).
    fn next_diff(&mut self) -> Option<RawDiff>;

    /// Returns the next batch of diffs whose `lsn` are equal. The
    /// default implementation drains [`next_diff`](Self::next_diff)
    /// until the LSN changes.
    fn next_batch(&mut self) -> Option<LsnBatch> {
        let first = self.next_diff()?;
        let mut diffs = vec![first.clone()];
        while let Some(next) = self.peek_lsn() {
            if next != first.lsn {
                break;
            }
            let Some(item) = self.next_diff() else {
                break;
            };
            diffs.push(item);
        }
        Some(LsnBatch {
            lsn: first.lsn,
            diffs,
        })
    }

    /// Returns the next complete transaction delta. Legacy cursors
    /// infer the transaction boundary from one complete LSN batch.
    fn next_transaction(&mut self) -> Option<QueryTransactionDelta> {
        self.next_batch().map(QueryTransactionDelta::from)
    }

    /// Returns the LSN of the diff `next_diff` would yield, without
    /// consuming it. Implementations that cannot peek may return
    /// `None`, in which case [`next_batch`](Self::next_batch) emits
    /// one diff per batch.
    fn peek_lsn(&self) -> Option<Lsn> {
        None
    }
}

/// Diffs grouped under a single logical clock tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsnBatch {
    /// LSN every diff in this batch belongs to.
    pub lsn: Lsn,
    /// Raw diffs at `lsn`.
    pub diffs: Vec<RawDiff>,
}

/// Complete dataflow output for one upstream transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTransactionDelta {
    /// `PostgreSQL` transaction id, when known.
    pub transaction_id: Option<u32>,
    /// Begin marker LSN, when known.
    pub begin_lsn: Option<Lsn>,
    /// Commit LSN shared by every raw diff in this output.
    pub commit_lsn: Lsn,
    /// End marker LSN, when known.
    pub end_lsn: Option<Lsn>,
    /// Raw dataflow diffs caused by this transaction.
    pub diffs: Vec<RawDiff>,
}

impl QueryTransactionDelta {
    /// Builds a transaction delta from already-complete raw output.
    #[must_use]
    pub const fn new(
        transaction_id: Option<u32>,
        begin_lsn: Option<Lsn>,
        commit_lsn: Lsn,
        end_lsn: Option<Lsn>,
        diffs: Vec<RawDiff>,
    ) -> Self {
        Self {
            transaction_id,
            begin_lsn,
            commit_lsn,
            end_lsn,
            diffs,
        }
    }

    /// Total cardinality of the transaction output (sum of `|diff|`).
    #[must_use]
    pub fn cardinality(&self) -> usize {
        self.diffs
            .iter()
            .map(|d| usize::try_from(d.diff.unsigned_abs()).unwrap_or(usize::MAX))
            .sum()
    }

    /// Returns true when the transaction produced no visible output.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }
}

impl From<LsnBatch> for QueryTransactionDelta {
    fn from(batch: LsnBatch) -> Self {
        Self {
            transaction_id: None,
            begin_lsn: None,
            commit_lsn: batch.lsn,
            end_lsn: None,
            diffs: batch.diffs,
        }
    }
}

impl LsnBatch {
    /// Total cardinality of the batch (sum of `|diff|`).
    #[must_use]
    pub fn cardinality(&self) -> usize {
        self.diffs
            .iter()
            .map(|d| usize::try_from(d.diff.unsigned_abs()).unwrap_or(usize::MAX))
            .sum()
    }
}

/// Drains `cursor` into LSN-grouped batches in order.
///
/// Stops at the first non-monotonic LSN — that condition signals a
/// trace inversion and is reported as
/// [`RouterError::ChannelSaturated`] so callers convert to `Resync`.
pub fn batch_by_lsn<C: TraceCursor>(cursor: &mut C) -> Result<Vec<LsnBatch>, RouterError> {
    let mut batches = Vec::new();
    let mut last_lsn: Option<Lsn> = None;
    while let Some(batch) = cursor.next_batch() {
        if let Some(prev) = last_lsn {
            if batch.lsn < prev {
                return Err(RouterError::ChannelSaturated);
            }
        }
        last_lsn = Some(batch.lsn);
        batches.push(batch);
    }
    Ok(batches)
}

/// In-memory cursor over a vector of diffs.
///
/// Used by tests and the in-process server harness.
#[derive(Debug, Clone)]
pub struct VecCursor {
    diffs: std::collections::VecDeque<RawDiff>,
}

impl VecCursor {
    /// Wraps an ordered diff sequence.
    #[must_use]
    pub fn new(diffs: impl IntoIterator<Item = RawDiff>) -> Self {
        Self {
            diffs: diffs.into_iter().collect(),
        }
    }
}

impl TraceCursor for VecCursor {
    fn next_diff(&mut self) -> Option<RawDiff> {
        self.diffs.pop_front()
    }

    fn peek_lsn(&self) -> Option<Lsn> {
        self.diffs.front().map(|diff| diff.lsn)
    }
}

#[cfg(test)]
mod tests {
    use palimpsest_dataflow::palimpsest::Lsn;
    use palimpsest_wal::Datum;
    use smallvec::smallvec;

    use super::{batch_by_lsn, RawDiff, TraceCursor, VecCursor};
    use crate::error::RouterError;

    fn diff(lsn: u64, value: i64, weight: i64) -> RawDiff {
        RawDiff {
            row: smallvec![Datum::I64(value)],
            lsn: Lsn::new(lsn),
            diff: weight,
        }
    }

    #[test]
    fn batch_groups_by_lsn() {
        let mut cursor = VecCursor::new([
            diff(10, 1, 1),
            diff(10, 2, 1),
            diff(11, 3, 1),
            diff(12, 4, -1),
        ]);
        let batches = batch_by_lsn(&mut cursor).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].lsn, Lsn::new(10));
        assert_eq!(batches[0].diffs.len(), 2);
        assert_eq!(batches[1].lsn, Lsn::new(11));
        assert_eq!(batches[2].diffs[0].diff, -1);
    }

    #[test]
    fn batch_rejects_non_monotonic_lsn() {
        let mut cursor = VecCursor::new([diff(10, 1, 1), diff(9, 2, 1)]);
        let err = batch_by_lsn(&mut cursor).unwrap_err();
        assert!(matches!(err, RouterError::ChannelSaturated));
    }

    #[test]
    fn cardinality_counts_absolute_diff() {
        let mut cursor = VecCursor::new([diff(10, 1, 1), diff(10, 2, -1), diff(10, 3, 2)]);
        let batch = cursor.next_batch().unwrap();
        assert_eq!(batch.cardinality(), 4);
    }

    #[test]
    fn empty_cursor_yields_no_batches() {
        let mut cursor = VecCursor::new([]);
        assert!(cursor.next_batch().is_none());
        assert_eq!(batch_by_lsn(&mut cursor).unwrap().len(), 0);
    }
}
