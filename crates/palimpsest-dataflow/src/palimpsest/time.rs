//! LSN-backed timely timestamp types.

use std::fmt;

use serde::{Deserialize, Serialize};
use timely::{
    order::{PartialOrder, TotalOrder},
    progress::timestamp::{PathSummary, Refines, Timestamp},
};

use crate::lattice::{Lattice, Maximum};

/// PostgreSQL WAL location used as a timely timestamp.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(u64);

impl Lsn {
    /// Creates a timestamp from a raw PostgreSQL LSN integer.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw PostgreSQL LSN integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Lsn").field(&self.0).finish()
    }
}

impl From<palimpsest_wal::Lsn> for Lsn {
    fn from(value: palimpsest_wal::Lsn) -> Self {
        Self(value.get())
    }
}

impl From<Lsn> for palimpsest_wal::Lsn {
    fn from(value: Lsn) -> Self {
        Self::new(value.0)
    }
}

impl PartialOrder for Lsn {
    fn less_than(&self, other: &Self) -> bool {
        self < other
    }

    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl TotalOrder for Lsn {}

impl Timestamp for Lsn {
    type Summary = LsnSummary;

    fn minimum() -> Self {
        Self(0)
    }
}

impl Refines<()> for Lsn {
    fn to_inner(_other: ()) -> Self {
        Self::minimum()
    }

    fn to_outer(self) {}

    fn summarize(_path: Self::Summary) {}
}

impl Lattice for Lsn {
    fn join(&self, other: &Self) -> Self {
        Self(self.0.max(other.0))
    }

    fn meet(&self, other: &Self) -> Self {
        Self(self.0.min(other.0))
    }
}

impl Maximum for Lsn {
    fn maximum() -> Self {
        Self(u64::MAX)
    }
}

/// A path summary that advances an [`Lsn`] by a `u64` amount.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LsnSummary(u64);

impl LsnSummary {
    /// Creates an LSN advancement summary.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw advancement amount.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for LsnSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LsnSummary").field(&self.0).finish()
    }
}

impl PartialOrder for LsnSummary {
    fn less_than(&self, other: &Self) -> bool {
        self < other
    }

    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl PathSummary<Lsn> for LsnSummary {
    fn results_in(&self, src: &Lsn) -> Option<Lsn> {
        src.0.checked_add(self.0).map(Lsn)
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use timely::progress::timestamp::{PathSummary, Refines, Timestamp};

    use super::{Lsn, LsnSummary};
    use crate::lattice::{Lattice, Maximum};

    #[test]
    fn lsn_is_timely_timestamp_with_zero_minimum() {
        assert_eq!(Lsn::minimum(), Lsn::new(0));
        assert!(timely::order::PartialOrder::less_equal(
            &Lsn::new(7),
            &Lsn::new(9)
        ));
        assert_eq!(<Lsn as Refines<()>>::to_inner(()), Lsn::minimum());
        assert_eq!(Lsn::maximum(), Lsn::new(u64::MAX));
        assert_eq!(Lsn::new(7).join(&Lsn::new(9)), Lsn::new(9));
        assert_eq!(Lsn::new(7).meet(&Lsn::new(9)), Lsn::new(7));
    }

    #[test]
    fn lsn_summary_advances_with_overflow_detection() {
        assert_eq!(
            LsnSummary::new(5).results_in(&Lsn::new(10)),
            Some(Lsn::new(15))
        );
        assert_eq!(LsnSummary::new(1).results_in(&Lsn::new(u64::MAX)), None);
        assert_eq!(
            LsnSummary::new(2).followed_by(&LsnSummary::new(3)),
            Some(LsnSummary::new(5))
        );
    }

    #[test]
    fn lsn_round_trips_wal_lsn() {
        let wal = palimpsest_wal::Lsn::new(123);
        let dataflow = Lsn::from(wal);

        assert_eq!(dataflow.get(), 123);
        assert_eq!(palimpsest_wal::Lsn::from(dataflow), wal);
    }
}
