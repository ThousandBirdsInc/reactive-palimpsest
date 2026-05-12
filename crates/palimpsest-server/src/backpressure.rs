// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-subscription bounded channel and backpressure policy.
//!
//! The router opens a bounded `tokio::sync::mpsc` channel per
//! subscription (default depth 256, see [`BoundedDiffChannel::DEFAULT_CAPACITY`]).
//! On saturation the configured [`BackpressurePolicy`] decides whether
//! to:
//!
//! * **`DropAndResync`** — drop the offending diff and emit a `Resync`
//!   so the client refetches; this matches §18.7 #5 and §14.5.
//! * **`Coalesce`** — fold pending updates into a single payload (used
//!   for small result sets where re-snapshotting is more expensive
//!   than coalescing).
//!
//! The receiver-side stream type is [`tokio_stream::wrappers::ReceiverStream`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::diff::{DiffEvent, ResyncReason};

/// Policy for dealing with a saturated per-subscription channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// Drop the offending diff and emit a `Resync` (default).
    #[default]
    DropAndResync,
    /// Coalesce queued updates into the most recent value.
    ///
    /// Only safe for queries whose result set is small enough that
    /// re-emitting a fresh `Initial` is cheaper than per-LSN diffs.
    Coalesce,
}

/// Outcome of pushing one diff into the channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureOutcome {
    /// Diff was queued.
    Sent,
    /// Channel was saturated; router must emit `Resync` and tear down
    /// the subscription.
    SaturatedDropResync,
    /// Channel was saturated; queued updates should be merged into a
    /// single payload before retrying.
    SaturatedCoalesce,
    /// Receiver disconnected; the subscription is gone.
    Closed,
}

/// Sender + receiver pair for a subscription's diff stream.
pub struct BoundedDiffChannel {
    sender: mpsc::Sender<DiffEvent>,
    receiver: Option<mpsc::Receiver<DiffEvent>>,
    policy: BackpressurePolicy,
    capacity: usize,
    full_events: Arc<AtomicU64>,
}

impl std::fmt::Debug for BoundedDiffChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedDiffChannel")
            .field("capacity", &self.capacity)
            .field("policy", &self.policy)
            .field("full_events", &self.full_events.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl BoundedDiffChannel {
    /// Default channel depth, matching §18.7 #5.
    pub const DEFAULT_CAPACITY: usize = 256;

    /// Builds a channel of the requested depth.
    #[must_use]
    pub fn new(capacity: usize, policy: BackpressurePolicy) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            sender,
            receiver: Some(receiver),
            policy,
            capacity,
            full_events: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Builds a channel with [`Self::DEFAULT_CAPACITY`].
    #[must_use]
    pub fn default_with_policy(policy: BackpressurePolicy) -> Self {
        Self::new(Self::DEFAULT_CAPACITY, policy)
    }

    /// Configured channel depth.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Configured backpressure policy.
    #[must_use]
    pub const fn policy(&self) -> BackpressurePolicy {
        self.policy
    }

    /// Detaches the receiver. Subsequent calls return `None`.
    ///
    /// The detached receiver is normally wrapped in
    /// `ReceiverStream::new(rx)` and handed to the gRPC transport.
    pub fn take_receiver(&mut self) -> Option<mpsc::Receiver<DiffEvent>> {
        self.receiver.take()
    }

    /// Detaches the receiver as a `tokio_stream::Stream`.
    pub fn take_stream(&mut self) -> Option<ReceiverStream<DiffEvent>> {
        self.receiver.take().map(ReceiverStream::new)
    }

    /// Clones a sender handle. Used when the cursor task lives on a
    /// different scheduler than the router.
    #[must_use]
    pub fn sender(&self) -> mpsc::Sender<DiffEvent> {
        self.sender.clone()
    }

    /// Counter of how many times the channel reported full since
    /// creation; surfaced through [`crate::metrics::RouterMetrics`].
    #[must_use]
    pub fn full_events(&self) -> u64 {
        self.full_events.load(Ordering::Relaxed)
    }

    /// Tries to enqueue `event`. Returns the policy-specific outcome.
    #[must_use]
    pub fn try_send(&self, event: DiffEvent) -> BackpressureOutcome {
        match self.sender.try_send(event) {
            Ok(()) => BackpressureOutcome::Sent,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.full_events.fetch_add(1, Ordering::Relaxed);
                match self.policy {
                    BackpressurePolicy::DropAndResync => BackpressureOutcome::SaturatedDropResync,
                    BackpressurePolicy::Coalesce => BackpressureOutcome::SaturatedCoalesce,
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => BackpressureOutcome::Closed,
        }
    }

    /// Forces a `Resync` event onto the channel; if the channel is
    /// saturated, the resync replaces nothing — the gRPC transport
    /// breaks the stream when it sees the eventual `Closed` so
    /// dropping is acceptable.
    pub fn force_resync(&self, reason: ResyncReason) {
        let _ = self.sender.try_send(DiffEvent::Resync { reason });
    }
}

#[cfg(test)]
mod tests {
    use palimpsest_dataflow::palimpsest::Lsn;
    use tokio_stream::StreamExt;

    use super::{BackpressureOutcome, BackpressurePolicy, BoundedDiffChannel};
    use crate::diff::{DiffEvent, ResyncReason};

    fn initial_event(lsn: u64) -> DiffEvent {
        DiffEvent::Initial {
            lsn: Lsn::new(lsn),
            rows: Vec::new(),
        }
    }

    #[tokio::test]
    async fn try_send_returns_sent_until_capacity_reached() {
        let mut channel = BoundedDiffChannel::new(2, BackpressurePolicy::DropAndResync);
        assert_eq!(
            channel.try_send(initial_event(1)),
            BackpressureOutcome::Sent
        );
        assert_eq!(
            channel.try_send(initial_event(2)),
            BackpressureOutcome::Sent
        );
        assert_eq!(
            channel.try_send(initial_event(3)),
            BackpressureOutcome::SaturatedDropResync
        );
        assert_eq!(channel.full_events(), 1);
        let mut stream = channel.take_stream().unwrap();
        assert!(matches!(
            stream.next().await,
            Some(DiffEvent::Initial { .. })
        ));
        assert!(matches!(
            stream.next().await,
            Some(DiffEvent::Initial { .. })
        ));
    }

    #[tokio::test]
    async fn coalesce_policy_returns_saturated_coalesce() {
        let channel = BoundedDiffChannel::new(1, BackpressurePolicy::Coalesce);
        let _ = channel.try_send(initial_event(1));
        let outcome = channel.try_send(initial_event(2));
        assert_eq!(outcome, BackpressureOutcome::SaturatedCoalesce);
    }

    #[tokio::test]
    async fn try_send_returns_closed_when_receiver_dropped() {
        let mut channel = BoundedDiffChannel::new(2, BackpressurePolicy::DropAndResync);
        drop(channel.take_receiver());
        assert_eq!(
            channel.try_send(initial_event(1)),
            BackpressureOutcome::Closed
        );
    }

    #[tokio::test]
    async fn force_resync_pushes_resync_event() {
        let mut channel = BoundedDiffChannel::new(2, BackpressurePolicy::DropAndResync);
        channel.force_resync(ResyncReason::SchemaChanged);
        let mut stream = channel.take_stream().unwrap();
        let event = stream.next().await.expect("event present");
        assert!(
            matches!(event, DiffEvent::Resync { reason } if reason == ResyncReason::SchemaChanged)
        );
    }
}
