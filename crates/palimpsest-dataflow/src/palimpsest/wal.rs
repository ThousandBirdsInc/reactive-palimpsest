//! WAL event conversion into differential updates.

use palimpsest_wal::{Datum, DecodedEvent, RowOp, TableId, Tuple};
use smallvec::SmallVec;
use timely::Accountable;

use crate::palimpsest::Lsn;

/// Inline-storage row representation used by Palimpsest dataflows.
pub type Row = SmallVec<[Datum; 8]>;

/// Timely container for batches of inline-storage rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowContainer {
    rows: Vec<Row>,
}

impl RowContainer {
    /// Creates an empty row container.
    #[must_use]
    pub const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Creates an empty row container with space for `capacity` rows.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    /// Pushes one row into the container.
    pub fn push(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// Returns the rows as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[Row] {
        &self.rows
    }

    /// Consumes the container and returns its rows.
    #[must_use]
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }
}

impl Accountable for RowContainer {
    fn record_count(&self) -> i64 {
        i64::try_from(self.rows.len()).unwrap_or(i64::MAX)
    }
}

/// One WAL-derived differential update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalUpdate {
    /// Source table identifier.
    pub table: TableId,
    /// Updated row.
    pub row: Row,
    /// Logical update time.
    pub time: Lsn,
    /// Differential multiplicity, normally `+1` or `-1`.
    pub diff: isize,
}

impl WalUpdate {
    /// Creates a WAL-derived update.
    #[must_use]
    pub const fn new(table: TableId, row: Row, time: Lsn, diff: isize) -> Self {
        Self {
            table,
            row,
            time,
            diff,
        }
    }
}

/// Transaction-aware converter from decoded WAL events to dataflow updates.
#[derive(Debug, Clone, Default)]
pub struct WalSourceState {
    transaction_lsn: Option<Lsn>,
    pending: Vec<PendingRow>,
}

impl WalSourceState {
    /// Creates an empty WAL source state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            transaction_lsn: None,
            pending: Vec::new(),
        }
    }

    /// Applies a decoded WAL event and returns updates ready to enter dataflow.
    pub fn apply(&mut self, event: DecodedEvent) -> Vec<WalUpdate> {
        match event {
            DecodedEvent::Begin { commit_lsn, .. } => {
                self.transaction_lsn = Some(Lsn::from(commit_lsn));
                Vec::new()
            }
            DecodedEvent::Row {
                table,
                op,
                old,
                new,
            } => {
                self.pending.push(PendingRow {
                    table,
                    op,
                    old: old.map(tuple_to_row),
                    new: new.map(tuple_to_row),
                });
                Vec::new()
            }
            DecodedEvent::Commit { commit_lsn, .. } => {
                let time = Lsn::from(commit_lsn);
                self.transaction_lsn = None;
                self.drain_pending(time)
            }
            DecodedEvent::Stream(palimpsest_wal::StreamAction::Commit { commit_lsn, .. }) => {
                self.drain_pending(Lsn::from(commit_lsn))
            }
            DecodedEvent::Heartbeat { .. }
            | DecodedEvent::Schema { .. }
            | DecodedEvent::Reconnect { .. }
            | DecodedEvent::Resync { .. }
            | DecodedEvent::Truncate(_)
            | DecodedEvent::Origin(_)
            | DecodedEvent::Stream(_)
            | DecodedEvent::TwoPhase(_) => Vec::new(),
        }
    }

    fn drain_pending(&mut self, time: Lsn) -> Vec<WalUpdate> {
        let mut updates = Vec::new();
        for row in self.pending.drain(..) {
            match row.op {
                RowOp::Insert => {
                    if let Some(new) = row.new {
                        updates.push(WalUpdate::new(row.table, new, time, 1));
                    }
                }
                RowOp::Update => {
                    if let Some(old) = row.old {
                        updates.push(WalUpdate::new(row.table, old, time, -1));
                    }
                    if let Some(new) = row.new {
                        updates.push(WalUpdate::new(row.table, new, time, 1));
                    }
                }
                RowOp::Delete => {
                    if let Some(old) = row.old {
                        updates.push(WalUpdate::new(row.table, old, time, -1));
                    }
                }
            }
        }
        updates
    }
}

#[derive(Debug, Clone)]
struct PendingRow {
    table: TableId,
    op: RowOp,
    old: Option<Row>,
    new: Option<Row>,
}

fn tuple_to_row(tuple: Tuple) -> Row {
    tuple
}

#[cfg(test)]
mod tests {
    use palimpsest_wal::{Datum, DecodedEvent, Lsn as WalLsn, RowOp, TableId};
    use timely::Accountable;

    use super::{Row, RowContainer, WalSourceState, WalUpdate};
    use crate::palimpsest::Lsn;

    #[test]
    fn row_uses_inline_capacity_for_common_widths() {
        let mut row = Row::new();
        for value in 0..8 {
            row.push(Datum::I32(value));
        }

        assert!(!row.spilled());
    }

    #[test]
    fn row_container_accounts_for_rows() {
        let mut container = RowContainer::with_capacity(2);
        container.push(smallvec::smallvec![Datum::I64(1)]);
        container.push(smallvec::smallvec![Datum::I64(2)]);

        assert_eq!(container.record_count(), 2);
        assert_eq!(container.as_slice().len(), 2);
    }

    #[test]
    fn wal_source_emits_insert_at_commit_lsn() {
        let mut source = WalSourceState::new();
        source.apply(DecodedEvent::Begin {
            xid: 1,
            commit_lsn: WalLsn::new(10),
        });
        source.apply(DecodedEvent::Row {
            table: TableId::new(7),
            op: RowOp::Insert,
            old: None,
            new: Some(smallvec::smallvec![Datum::I32(1)]),
        });

        let updates = source.apply(DecodedEvent::Commit {
            commit_lsn: WalLsn::new(12),
            end_lsn: WalLsn::new(13),
        });

        assert_eq!(
            updates,
            vec![WalUpdate::new(
                TableId::new(7),
                smallvec::smallvec![Datum::I32(1)],
                Lsn::new(12),
                1
            )]
        );
    }

    #[test]
    fn wal_source_emits_update_retraction_and_insertion() {
        let mut source = WalSourceState::new();
        source.apply(DecodedEvent::Row {
            table: TableId::new(7),
            op: RowOp::Update,
            old: Some(smallvec::smallvec![
                Datum::I32(1),
                Datum::Text("old".into())
            ]),
            new: Some(smallvec::smallvec![
                Datum::I32(1),
                Datum::Text("new".into())
            ]),
        });

        let updates = source.apply(DecodedEvent::Commit {
            commit_lsn: WalLsn::new(12),
            end_lsn: WalLsn::new(13),
        });

        assert_eq!(
            updates,
            vec![
                WalUpdate::new(
                    TableId::new(7),
                    smallvec::smallvec![Datum::I32(1), Datum::Text("old".into())],
                    Lsn::new(12),
                    -1
                ),
                WalUpdate::new(
                    TableId::new(7),
                    smallvec::smallvec![Datum::I32(1), Datum::Text("new".into())],
                    Lsn::new(12),
                    1
                )
            ]
        );
    }
}
