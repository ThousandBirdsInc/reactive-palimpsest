//! LSN watermark tracking for trace compaction.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::{palimpsest::Lsn, trace::TraceReader};

/// Stable identifier for a dataflow subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubscriberId(u64);

impl SubscriberId {
    /// Creates a subscriber identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw subscriber identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Tracks subscriber LSN watermarks and applies the minimum as trace compaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LsnWatermarks {
    subscribers: BTreeMap<SubscriberId, Lsn>,
    applied: Option<Lsn>,
}

impl LsnWatermarks {
    /// Creates an empty watermark set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            subscribers: BTreeMap::new(),
            applied: None,
        }
    }

    /// Adds or advances a subscriber's minimum retained LSN.
    ///
    /// Regressions are ignored so the resulting compaction frontier remains monotonic.
    pub fn update(&mut self, subscriber: SubscriberId, min_lsn: Lsn) {
        self.subscribers
            .entry(subscriber)
            .and_modify(|current| *current = (*current).max(min_lsn))
            .or_insert(min_lsn);
    }

    /// Removes a subscriber from watermark accounting.
    pub fn remove(&mut self, subscriber: SubscriberId) -> Option<Lsn> {
        self.subscribers.remove(&subscriber)
    }

    /// Returns the current minimum subscriber LSN, if any subscribers remain.
    #[must_use]
    pub fn min_subscriber_lsn(&self) -> Option<Lsn> {
        self.subscribers.values().copied().min()
    }

    /// Returns the last LSN applied to a trace compaction frontier.
    #[must_use]
    pub const fn applied_lsn(&self) -> Option<Lsn> {
        self.applied
    }

    /// Applies the minimum subscriber LSN to a trace's logical compaction frontier.
    ///
    /// Returns `Some(lsn)` when the trace frontier was advanced.
    pub fn apply_logical_compaction<Tr>(&mut self, trace: &mut Tr) -> Option<Lsn>
    where
        Tr: TraceReader<Time = Lsn>,
    {
        let min_lsn = self.min_subscriber_lsn()?;
        if self.applied.is_some_and(|applied| applied >= min_lsn) {
            return None;
        }

        let frontier = Antichain::from_elem(min_lsn);
        trace.set_logical_compaction(frontier.borrow());
        self.applied = Some(min_lsn);
        Some(min_lsn)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        input::Input,
        operators::arrange::ArrangeBySelf,
        palimpsest::{Lsn, LsnWatermarks, SubscriberId},
        trace::TraceReader,
    };

    #[test]
    fn watermarks_track_minimum_and_removal() {
        let mut watermarks = LsnWatermarks::new();
        watermarks.update(SubscriberId::new(1), Lsn::new(10));
        watermarks.update(SubscriberId::new(2), Lsn::new(7));

        assert_eq!(watermarks.min_subscriber_lsn(), Some(Lsn::new(7)));

        watermarks.update(SubscriberId::new(2), Lsn::new(12));
        assert_eq!(watermarks.min_subscriber_lsn(), Some(Lsn::new(10)));

        watermarks.remove(SubscriberId::new(1));
        assert_eq!(watermarks.min_subscriber_lsn(), Some(Lsn::new(12)));
    }

    #[test]
    fn watermarks_ignore_regressions() {
        let mut watermarks = LsnWatermarks::new();
        let subscriber = SubscriberId::new(1);
        watermarks.update(subscriber, Lsn::new(10));
        watermarks.update(subscriber, Lsn::new(8));

        assert_eq!(watermarks.min_subscriber_lsn(), Some(Lsn::new(10)));
    }

    #[test]
    fn logical_compaction_uses_minimum_subscriber_lsn() {
        timely::execute_directly(|worker| {
            let mut trace = worker.dataflow::<Lsn, _, _>(|scope| {
                scope
                    .new_collection_from(vec![1_u64])
                    .1
                    .arrange_by_self()
                    .trace
            });
            let mut watermarks = LsnWatermarks::new();
            watermarks.update(SubscriberId::new(1), Lsn::new(11));
            watermarks.update(SubscriberId::new(2), Lsn::new(5));

            assert_eq!(
                watermarks.apply_logical_compaction(&mut trace),
                Some(Lsn::new(5))
            );
            assert_eq!(
                trace.get_logical_compaction().to_owned(),
                timely::progress::Antichain::from_elem(Lsn::new(5))
            );

            watermarks.update(SubscriberId::new(2), Lsn::new(3));
            assert_eq!(watermarks.apply_logical_compaction(&mut trace), None);
            assert_eq!(watermarks.applied_lsn(), Some(Lsn::new(5)));
        });
    }
}
