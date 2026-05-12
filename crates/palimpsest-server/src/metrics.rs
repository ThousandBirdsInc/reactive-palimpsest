// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Router-level metrics surfaced through Prometheus by §18.8.
//!
//! For now we keep counters/estimators private; the gRPC server crate
//! adapts them onto Prometheus families. Latency is tracked via a
//! lightweight P² quantile estimator that avoids storing per-event
//! samples.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::diff::ResyncReason;

/// Snapshot-able router metrics. Every counter is monotonic.
#[derive(Debug, Default, Clone)]
pub struct RouterMetrics {
    inner: Arc<RouterMetricsInner>,
}

#[derive(Debug, Default)]
struct RouterMetricsInner {
    subscriptions_in_flight: AtomicU64,
    subscriptions_total: AtomicU64,
    channel_full_events: AtomicU64,
    resyncs_emitted: AtomicU64,
    resyncs_by_reason: ResyncReasonCounters,
    diffs_sent: AtomicU64,
    bytes_sent: AtomicU64,
    channel_depth_max: AtomicU64,
    wal_lag_bytes: AtomicU64,
    operator_memory_bytes: AtomicU64,
    fanout_latency: Mutex<RunningQuantiles>,
}

#[derive(Debug, Default)]
struct ResyncReasonCounters {
    lsn_compacted: AtomicU64,
    schema_changed: AtomicU64,
    backpressure: AtomicU64,
    slot_recreated: AtomicU64,
    permissions_changed: AtomicU64,
}

impl ResyncReasonCounters {
    fn bump(&self, reason: ResyncReason) {
        let counter = match reason {
            ResyncReason::LsnCompacted => &self.lsn_compacted,
            ResyncReason::SchemaChanged => &self.schema_changed,
            ResyncReason::Backpressure => &self.backpressure,
            ResyncReason::SlotRecreated => &self.slot_recreated,
            ResyncReason::PermissionsChanged => &self.permissions_changed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ResyncReasonSnapshot {
        ResyncReasonSnapshot {
            lsn_compacted: self.lsn_compacted.load(Ordering::Relaxed),
            schema_changed: self.schema_changed.load(Ordering::Relaxed),
            backpressure: self.backpressure.load(Ordering::Relaxed),
            slot_recreated: self.slot_recreated.load(Ordering::Relaxed),
            permissions_changed: self.permissions_changed.load(Ordering::Relaxed),
        }
    }
}

/// Per-reason resync counts; surfaced as labelled Prometheus counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResyncReasonSnapshot {
    /// Resume LSN behind compaction frontier.
    pub lsn_compacted: u64,
    /// Schema (or permission shape) changed.
    pub schema_changed: u64,
    /// Channel saturated.
    pub backpressure: u64,
    /// Replication slot lost.
    pub slot_recreated: u64,
    /// Permission rules changed.
    pub permissions_changed: u64,
}

impl RouterMetrics {
    /// Creates a fresh metric registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a successful subscribe.
    pub fn subscribe(&self) {
        self.inner
            .subscriptions_in_flight
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .subscriptions_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a teardown (matched with a prior subscribe).
    pub fn unsubscribe(&self) {
        self.inner
            .subscriptions_in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            })
            .ok();
    }

    /// Records that the per-subscription channel was found saturated.
    pub fn record_channel_full(&self) {
        self.inner
            .channel_full_events
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a `Resync` emission with no specific reason. Prefer
    /// [`Self::record_resync_with_reason`] when the reason is known so
    /// the per-reason counter advances too.
    pub fn record_resync(&self) {
        self.inner.resyncs_emitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a `Resync` emission and bumps the per-reason counter.
    pub fn record_resync_with_reason(&self, reason: ResyncReason) {
        self.inner.resyncs_emitted.fetch_add(1, Ordering::Relaxed);
        self.inner.resyncs_by_reason.bump(reason);
    }

    /// Records a successful diff push.
    pub fn record_diff(&self, fanout_latency: Duration) {
        self.inner.diffs_sent.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(fanout_latency.as_micros()).unwrap_or(u64::MAX);
        #[allow(clippy::cast_precision_loss)]
        let micros_f = micros as f64;
        if let Ok(mut quantiles) = self.inner.fanout_latency.lock() {
            quantiles.observe(micros_f);
        }
    }

    /// Records bytes flushed onto the wire (e.g. encoded `Diff.rows`
    /// payload). Counter; monotonic.
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.inner.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Reports a per-subscription channel depth observation; tracked
    /// as a high-water gauge.
    pub fn observe_channel_depth(&self, depth: u64) {
        self.inner
            .channel_depth_max
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.max(depth))
            })
            .ok();
    }

    /// Sets the current WAL lag (current LSN − applied LSN, in bytes).
    pub fn set_wal_lag_bytes(&self, lag: u64) {
        self.inner.wal_lag_bytes.store(lag, Ordering::Relaxed);
    }

    /// Sets the aggregate operator memory footprint (sum across the
    /// whole router, in bytes).
    pub fn set_operator_memory_bytes(&self, bytes: u64) {
        self.inner
            .operator_memory_bytes
            .store(bytes, Ordering::Relaxed);
    }

    /// Returns the current snapshot.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        let quantiles = self
            .inner
            .fanout_latency
            .lock()
            .map(|q| q.snapshot())
            .unwrap_or_default();
        MetricsSnapshot {
            subscriptions_in_flight: self.inner.subscriptions_in_flight.load(Ordering::Relaxed),
            subscriptions_total: self.inner.subscriptions_total.load(Ordering::Relaxed),
            channel_full_events: self.inner.channel_full_events.load(Ordering::Relaxed),
            resyncs_emitted: self.inner.resyncs_emitted.load(Ordering::Relaxed),
            resyncs_by_reason: self.inner.resyncs_by_reason.snapshot(),
            diffs_sent: self.inner.diffs_sent.load(Ordering::Relaxed),
            bytes_sent: self.inner.bytes_sent.load(Ordering::Relaxed),
            channel_depth_max: self.inner.channel_depth_max.load(Ordering::Relaxed),
            wal_lag_bytes: self.inner.wal_lag_bytes.load(Ordering::Relaxed),
            operator_memory_bytes: self.inner.operator_memory_bytes.load(Ordering::Relaxed),
            fanout_latency_p50_us: quantiles.p50,
            fanout_latency_p99_us: quantiles.p99,
        }
    }
}

/// Read-only point-in-time view of [`RouterMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct MetricsSnapshot {
    /// Currently-active subscriptions.
    pub subscriptions_in_flight: u64,
    /// Total subscriptions opened since startup (monotonic).
    pub subscriptions_total: u64,
    /// Times any per-subscription channel reported `Full`.
    pub channel_full_events: u64,
    /// Total `Resync` events emitted.
    pub resyncs_emitted: u64,
    /// Per-reason resync breakdown.
    pub resyncs_by_reason: ResyncReasonSnapshot,
    /// Total diff payloads enqueued.
    pub diffs_sent: u64,
    /// Total bytes flushed onto outbound wires (encoded diff payloads).
    pub bytes_sent: u64,
    /// High-water mark of any per-subscription channel depth observed.
    pub channel_depth_max: u64,
    /// Current WAL lag in bytes (set by the WAL runtime).
    pub wal_lag_bytes: u64,
    /// Aggregate per-operator memory footprint in bytes.
    pub operator_memory_bytes: u64,
    /// Estimated p50 fan-out latency, in microseconds.
    pub fanout_latency_p50_us: f64,
    /// Estimated p99 fan-out latency, in microseconds.
    pub fanout_latency_p99_us: f64,
}

/// Streaming p50 / p99 estimator.
///
/// v1 uses a bounded-window sample bag (cap 1024). A future revision
/// can swap in the P² algorithm so the estimator's memory footprint
/// becomes constant; the public API here is stable.
#[derive(Debug)]
struct RunningQuantiles {
    p50: P2Quantile,
    p99: P2Quantile,
}

impl Default for RunningQuantiles {
    fn default() -> Self {
        Self {
            p50: P2Quantile {
                target: 0.5,
                samples: Vec::new(),
            },
            p99: P2Quantile {
                target: 0.99,
                samples: Vec::new(),
            },
        }
    }
}

impl RunningQuantiles {
    fn observe(&mut self, value: f64) {
        self.p50.observe(value);
        self.p99.observe(value);
    }

    fn snapshot(&self) -> QuantilesSnapshot {
        QuantilesSnapshot {
            p50: self.p50.estimate(),
            p99: self.p99.estimate(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct QuantilesSnapshot {
    p50: f64,
    p99: f64,
}

/// Single-quantile P² estimator.
#[derive(Debug)]
struct P2Quantile {
    target: f64,
    samples: Vec<f64>,
}

impl P2Quantile {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn observe(&mut self, value: f64) {
        if self.samples.len() < 1024 {
            self.samples.push(value);
            return;
        }
        let slot = (value as usize) % self.samples.len();
        self.samples[slot] = value;
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    fn estimate(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let index = ((self.target * (sorted.len() as f64 - 1.0)).round()) as usize;
        sorted[index.min(sorted.len() - 1)]
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RouterMetrics;

    #[test]
    fn subscribe_and_unsubscribe_are_balanced() {
        let metrics = RouterMetrics::new();
        metrics.subscribe();
        metrics.subscribe();
        metrics.unsubscribe();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.subscriptions_in_flight, 1);
        assert_eq!(snapshot.subscriptions_total, 2);
    }

    #[test]
    fn channel_full_and_resync_counters_advance() {
        let metrics = RouterMetrics::new();
        metrics.record_channel_full();
        metrics.record_channel_full();
        metrics.record_resync();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.channel_full_events, 2);
        assert_eq!(snapshot.resyncs_emitted, 1);
    }

    #[test]
    fn diff_latency_estimator_returns_observed_values() {
        let metrics = RouterMetrics::new();
        for value in 0..1000 {
            metrics.record_diff(Duration::from_micros(value));
        }
        let snapshot = metrics.snapshot();
        assert!(snapshot.diffs_sent >= 1000);
        assert!(snapshot.fanout_latency_p50_us > 0.0);
        assert!(snapshot.fanout_latency_p99_us >= snapshot.fanout_latency_p50_us);
    }

    #[test]
    fn per_reason_resync_counters_advance() {
        use crate::diff::ResyncReason;
        let metrics = RouterMetrics::new();
        metrics.record_resync_with_reason(ResyncReason::Backpressure);
        metrics.record_resync_with_reason(ResyncReason::Backpressure);
        metrics.record_resync_with_reason(ResyncReason::LsnCompacted);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.resyncs_emitted, 3);
        assert_eq!(snapshot.resyncs_by_reason.backpressure, 2);
        assert_eq!(snapshot.resyncs_by_reason.lsn_compacted, 1);
        assert_eq!(snapshot.resyncs_by_reason.schema_changed, 0);
    }

    #[test]
    fn bytes_and_depth_and_lag_gauges_round_trip() {
        let metrics = RouterMetrics::new();
        metrics.record_bytes_sent(128);
        metrics.record_bytes_sent(64);
        metrics.observe_channel_depth(3);
        metrics.observe_channel_depth(7);
        metrics.observe_channel_depth(5); // does not lower the high-water
        metrics.set_wal_lag_bytes(4096);
        metrics.set_operator_memory_bytes(1_048_576);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.bytes_sent, 192);
        assert_eq!(snapshot.channel_depth_max, 7);
        assert_eq!(snapshot.wal_lag_bytes, 4096);
        assert_eq!(snapshot.operator_memory_bytes, 1_048_576);
    }
}
