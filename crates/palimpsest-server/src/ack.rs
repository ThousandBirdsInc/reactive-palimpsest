// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-subscription LSN ack tracking.
//!
//! Acks have two effects:
//!
//! 1. They advance `Subscription::cursor_lsn`, which is what the
//!    resume path checks against the compaction window (§18.7 #11).
//! 2. They feed [`palimpsest_dataflow::LsnWatermarks`] which controls
//!    the global trace-compaction frontier (§9.4 + §18.7 #10).
//!
//! Acks are required to be monotonic: an ack with `lsn` ≤ the current
//! cursor is silently ignored — the client may have retransmitted a
//! stale ack after reconnect.

use palimpsest_dataflow::palimpsest::{Lsn, LsnWatermarks, SubscriberId};

use crate::subscription::SubscriptionId;

/// Ack outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// Ack advanced the cursor and the global watermark.
    Advanced(Lsn),
    /// Ack was a no-op (stale or equal to the current cursor).
    Stale,
}

/// Wraps `LsnWatermarks` with subscription-id translation.
#[derive(Debug, Default)]
pub struct AckTracker {
    watermarks: LsnWatermarks,
}

impl AckTracker {
    /// Creates an empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the seed LSN for a freshly-accepted subscription.
    ///
    /// The router calls this exactly once when the subscription
    /// transitions out of `Seeding`, so the global frontier reflects
    /// the snapshot LSN until the first client ack arrives.
    pub fn install(&mut self, sub: SubscriptionId, snapshot_lsn: Lsn) {
        self.watermarks
            .update(SubscriberId::new(sub.get()), snapshot_lsn);
    }

    /// Removes a subscription from watermark accounting (teardown
    /// path).
    pub fn remove(&mut self, sub: SubscriptionId) {
        self.watermarks.remove(SubscriberId::new(sub.get()));
    }

    /// Records a client ack. Returns whether the cursor advanced.
    ///
    /// Stale acks (ones whose `lsn` does not exceed the current
    /// cursor) are reported as [`AckOutcome::Stale`] without changing
    /// any state.
    pub fn ack(&mut self, sub: SubscriptionId, previous_cursor: Lsn, ack_lsn: Lsn) -> AckOutcome {
        if ack_lsn <= previous_cursor {
            return AckOutcome::Stale;
        }
        self.watermarks
            .update(SubscriberId::new(sub.get()), ack_lsn);
        AckOutcome::Advanced(ack_lsn)
    }

    /// Mutable access to the underlying watermark tracker so the
    /// router can apply logical compaction during its tick.
    pub fn watermarks_mut(&mut self) -> &mut LsnWatermarks {
        &mut self.watermarks
    }

    /// Read-only access to the underlying watermark tracker.
    #[must_use]
    pub const fn watermarks(&self) -> &LsnWatermarks {
        &self.watermarks
    }

    /// Current minimum subscriber LSN, useful for deciding whether the
    /// trace can be compacted.
    #[must_use]
    pub fn min_subscriber_lsn(&self) -> Option<Lsn> {
        self.watermarks.min_subscriber_lsn()
    }
}

#[cfg(test)]
mod tests {
    use palimpsest_dataflow::palimpsest::Lsn;

    use super::{AckOutcome, AckTracker};
    use crate::subscription::SubscriptionId;

    #[test]
    fn install_then_advance_lifts_min() {
        let mut tracker = AckTracker::new();
        tracker.install(SubscriptionId::new(1), Lsn::new(10));
        assert_eq!(tracker.min_subscriber_lsn(), Some(Lsn::new(10)));

        let outcome = tracker.ack(SubscriptionId::new(1), Lsn::new(10), Lsn::new(15));
        assert_eq!(outcome, AckOutcome::Advanced(Lsn::new(15)));
        assert_eq!(tracker.min_subscriber_lsn(), Some(Lsn::new(15)));
    }

    #[test]
    fn stale_ack_is_ignored() {
        let mut tracker = AckTracker::new();
        tracker.install(SubscriptionId::new(1), Lsn::new(10));
        let outcome = tracker.ack(SubscriptionId::new(1), Lsn::new(15), Lsn::new(15));
        assert_eq!(outcome, AckOutcome::Stale);
        let outcome = tracker.ack(SubscriptionId::new(1), Lsn::new(15), Lsn::new(12));
        assert_eq!(outcome, AckOutcome::Stale);
    }

    #[test]
    fn remove_drops_watermark() {
        let mut tracker = AckTracker::new();
        tracker.install(SubscriptionId::new(1), Lsn::new(10));
        tracker.install(SubscriptionId::new(2), Lsn::new(11));
        tracker.remove(SubscriptionId::new(1));
        assert_eq!(tracker.min_subscriber_lsn(), Some(Lsn::new(11)));
    }

    #[test]
    fn min_is_minimum_across_subscriptions() {
        let mut tracker = AckTracker::new();
        tracker.install(SubscriptionId::new(1), Lsn::new(10));
        tracker.install(SubscriptionId::new(2), Lsn::new(20));
        tracker.install(SubscriptionId::new(3), Lsn::new(15));
        assert_eq!(tracker.min_subscriber_lsn(), Some(Lsn::new(10)));
    }
}
