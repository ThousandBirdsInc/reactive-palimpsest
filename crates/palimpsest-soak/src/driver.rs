// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared producer/consumer engine for router-path load scenarios.
//!
//! One paced producer generates Zipf-skewed transactions and fans
//! them out to matching subscribers; one consumer task per
//! subscriber drains its stream concurrently, recording
//! commit-to-client latency from timestamps embedded in the rows.
//! Unlike a lock-step pump/drain loop, this exposes real queueing:
//! channel saturation, slow-consumer isolation, and burst drain
//! behavior all become measurable.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_server::{
    ConnectionId, DiffEvent, MetricsSnapshot, QueryTransactionDelta, RouterConfig, RouterError,
    SubscriptionId, SubscriptionRouter,
};
use palimpsest_wal::Datum;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::fixture::{now_ns, subscribe_pass_through, SENT_AT_COLUMN, SNAPSHOT_LSN};
use crate::rng::{Rng, Zipf};
use crate::stats::{rss_bytes, LatencyRecorder, LatencySummary, RssWatcher};
use crate::workload::{Workload, WorkloadMix};

/// Slow-consumer knobs: the first `count` subscribers are placed on
/// the global feed and drain each event with `delay` of think time.
#[derive(Debug, Clone, Copy)]
pub struct SlowConsumers {
    /// How many subscribers are slow.
    pub count: usize,
    /// Per-event drain delay for slow subscribers.
    pub delay: Duration,
}

/// Burst knobs: every `every` paced transactions, `size` extra
/// transactions are fired back-to-back at the hottest shard.
#[derive(Debug, Clone, Copy)]
pub struct Bursts {
    /// Paced-transaction interval between bursts.
    pub every: u64,
    /// Transactions per burst.
    pub size: u64,
}

/// Bulk-backfill knobs: every `every` paced transactions, one
/// transaction of `rows` rows lands on a cold shard — modeling a
/// migration or import running next to interactive traffic.
#[derive(Debug, Clone, Copy)]
pub struct BulkBackfill {
    /// Paced-transaction interval between bulk transactions.
    pub every: u64,
    /// Rows per bulk transaction.
    pub rows: usize,
}

/// Full driver configuration.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// Scenario name stamped on the report.
    pub name: &'static str,
    /// Total concurrent subscribers.
    pub subscribers: usize,
    /// Number of write shards (documents / channels).
    pub shards: usize,
    /// Percent of subscribers on the global firehose feed; the rest
    /// follow a single Zipf-chosen shard.
    pub global_subscriber_pct: u8,
    /// Zipf exponent for write and subscriber skew.
    pub zipf_exponent: f64,
    /// Paced transactions to generate (bursts/bulk are extra).
    pub total_txns: u64,
    /// Target transactions per second; `0` runs unpaced.
    pub target_tps: u64,
    /// Rows in each subscriber's bootstrap snapshot.
    pub snapshot_rows: usize,
    /// Per-subscription channel depth.
    pub channel_capacity: usize,
    /// Consumers ack every N transaction events (`0` = never).
    pub ack_every: u64,
    /// Master seed; every random stream derives from it.
    pub seed: u64,
    /// Operation / transaction-size mixture.
    pub mix: WorkloadMix,
    /// Optional slow-consumer population.
    pub slow: Option<SlowConsumers>,
    /// Optional burst schedule.
    pub bursts: Option<Bursts>,
    /// Optional bulk-backfill schedule.
    pub bulk: Option<BulkBackfill>,
    /// After saturating a subscriber, skip it for this many
    /// transactions (models the client's resync/refetch window).
    pub saturation_cooloff_txns: u64,
}

impl DriverConfig {
    /// Baseline steady-state configuration at full scale.
    #[must_use]
    pub fn steady_state() -> Self {
        Self {
            name: "steady-state",
            subscribers: 1_000,
            shards: 64,
            global_subscriber_pct: 10,
            zipf_exponent: 1.05,
            total_txns: 100_000,
            target_tps: 10_000,
            snapshot_rows: 64,
            channel_capacity: 256,
            ack_every: 16,
            seed: 0x00c0_ffee,
            mix: WorkloadMix::default(),
            slow: None,
            bursts: None,
            bulk: None,
            saturation_cooloff_txns: 64,
        }
    }

    /// Tiny deterministic configuration for CI smoke tests.
    #[must_use]
    pub fn smoke() -> Self {
        Self {
            subscribers: 24,
            shards: 8,
            total_txns: 200,
            target_tps: 0,
            snapshot_rows: 8,
            channel_capacity: 1_024,
            ..Self::steady_state()
        }
    }
}

/// Selected router counters copied out of [`MetricsSnapshot`].
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct RouterCounters {
    /// Total subscriptions opened.
    pub subscriptions_total: u64,
    /// Diff payloads enqueued.
    pub diffs_sent: u64,
    /// Channel-full (saturation) events.
    pub channel_full_events: u64,
    /// Resync events emitted.
    pub resyncs_emitted: u64,
    /// High-water channel depth.
    pub channel_depth_max: u64,
    /// Router-side fan-out latency p50 (µs).
    pub fanout_latency_p50_us: f64,
    /// Router-side fan-out latency p99 (µs).
    pub fanout_latency_p99_us: f64,
}

impl From<&MetricsSnapshot> for RouterCounters {
    fn from(m: &MetricsSnapshot) -> Self {
        Self {
            subscriptions_total: m.subscriptions_total,
            diffs_sent: m.diffs_sent,
            channel_full_events: m.channel_full_events,
            resyncs_emitted: m.resyncs_emitted,
            channel_depth_max: m.channel_depth_max,
            fanout_latency_p50_us: m.fanout_latency_p50_us,
            fanout_latency_p99_us: m.fanout_latency_p99_us,
        }
    }
}

/// Aggregated outcome of one driver run.
#[derive(Debug, Clone, Serialize)]
pub struct DriverReport {
    /// Scenario name.
    pub scenario: String,
    /// Subscriber count.
    pub subscribers: usize,
    /// Shard count.
    pub shards: usize,
    /// Transactions generated (paced + bursts + bulk).
    pub txns_generated: u64,
    /// Wall-clock time for the write phase, seconds.
    pub wall_secs: f64,
    /// Achieved transactions per second.
    pub achieved_tps: f64,
    /// Successful per-subscriber deliveries (fan-out events).
    pub deliveries: u64,
    /// Deliveries per second.
    pub deliveries_per_sec: f64,
    /// Transaction events received by consumers.
    pub events_received: u64,
    /// Commit-to-client latency across healthy subscribers.
    pub update_latency: LatencySummary,
    /// Latency across slow subscribers, when configured.
    pub slow_update_latency: Option<LatencySummary>,
    /// Subscribe-to-`Initial` latency across all subscribers.
    pub subscribe_to_initial: LatencySummary,
    /// Pump attempts dropped due to channel saturation.
    pub saturation_drops: u64,
    /// Resync events received by healthy subscribers.
    pub resyncs_received_healthy: u64,
    /// Resync events received by slow subscribers.
    pub resyncs_received_slow: u64,
    /// RSS before any subscriptions, bytes.
    pub rss_before_bytes: u64,
    /// RSS after all subscriptions bootstrapped, bytes.
    pub rss_after_subscribe_bytes: u64,
    /// Peak RSS observed during the run, bytes.
    pub rss_peak_bytes: u64,
    /// Approximate incremental RSS per subscriber, bytes.
    pub per_subscriber_rss_bytes: u64,
    /// Router-side counters.
    pub router: RouterCounters,
}

impl fmt::Display for DriverReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{}] subs={} shards={} txns={} wall={:.2}s tps={:.0} deliveries={} ({:.0}/s)",
            self.scenario,
            self.subscribers,
            self.shards,
            self.txns_generated,
            self.wall_secs,
            self.achieved_tps,
            self.deliveries,
            self.deliveries_per_sec,
        )?;
        writeln!(f, "  update latency   : {}", self.update_latency)?;
        if let Some(slow) = &self.slow_update_latency {
            writeln!(f, "  slow-sub latency : {slow}")?;
        }
        writeln!(f, "  subscribe latency: {}", self.subscribe_to_initial)?;
        writeln!(
            f,
            "  saturation drops={} resyncs(healthy)={} resyncs(slow)={} channel_full={} depth_max={}",
            self.saturation_drops,
            self.resyncs_received_healthy,
            self.resyncs_received_slow,
            self.router.channel_full_events,
            self.router.channel_depth_max,
        )?;
        write!(
            f,
            "  rss before={}KiB after-subscribe={}KiB peak={}KiB (~{}KiB/sub)",
            self.rss_before_bytes / 1024,
            self.rss_after_subscribe_bytes / 1024,
            self.rss_peak_bytes / 1024,
            self.per_subscriber_rss_bytes / 1024,
        )
    }
}

/// What one consumer task saw over its lifetime.
pub(crate) struct ConsumerStats {
    pub(crate) latency: LatencyRecorder,
    pub(crate) events: u64,
    pub(crate) resyncs: u64,
    pub(crate) initial_us: u64,
}

/// Inputs for one consumer task.
pub(crate) struct Consumer {
    pub(crate) stream: ReceiverStream<DiffEvent>,
    pub(crate) router: Arc<SubscriptionRouter>,
    pub(crate) sub: SubscriptionId,
    pub(crate) delay: Option<Duration>,
    pub(crate) ack_every: u64,
    pub(crate) epoch: Instant,
    pub(crate) subscribed_at: Instant,
    /// Fired by the producer once it knows how many transaction
    /// events were successfully delivered to this subscriber; the
    /// consumer drains until it has seen that many.
    pub(crate) expected: oneshot::Receiver<u64>,
    /// End the task on the first `Resync` (permission-storm mode).
    pub(crate) stop_on_resync: bool,
}

pub(crate) fn spawn_consumer(mut c: Consumer) -> JoinHandle<ConsumerStats> {
    tokio::spawn(async move {
        let mut stats = ConsumerStats {
            latency: LatencyRecorder::default(),
            events: 0,
            resyncs: 0,
            initial_us: 0,
        };
        let mut expected: Option<u64> = None;
        loop {
            if let Some(target) = expected {
                if stats.events >= target {
                    break;
                }
            }
            tokio::select! {
                event = c.stream.next() => match event {
                    Some(DiffEvent::Initial { .. }) => {
                        stats.initial_us =
                            u64::try_from(c.subscribed_at.elapsed().as_micros())
                                .unwrap_or(u64::MAX);
                    }
                    Some(DiffEvent::TransactionUpdate { commit_lsn, changes, .. }) => {
                        stats.events += 1;
                        let recv_ns = now_ns(c.epoch);
                        for change in &changes {
                            if let Some(new) = &change.new {
                                if let Some(Datum::I64(sent)) = new.get(SENT_AT_COLUMN) {
                                    let us = recv_ns.saturating_sub(*sent).max(0) / 1_000;
                                    stats.latency.record_us(us.unsigned_abs());
                                }
                            }
                        }
                        if let Some(delay) = c.delay {
                            tokio::time::sleep(delay).await;
                        }
                        if c.ack_every > 0 && stats.events % c.ack_every == 0 {
                            let _ = c.router.ack(c.sub, commit_lsn);
                        }
                    }
                    Some(DiffEvent::Update { .. }) => {}
                    Some(DiffEvent::Resync { .. }) => {
                        stats.resyncs += 1;
                        if c.stop_on_resync {
                            break;
                        }
                    }
                    None => break,
                },
                target = &mut c.expected, if expected.is_none() => {
                    expected = Some(target.unwrap_or(stats.events));
                }
            }
        }
        stats
    })
}

struct SubSlot {
    sub: SubscriptionId,
    delivered: u64,
    skip_until: u64,
    slow: bool,
    expected_tx: Option<oneshot::Sender<u64>>,
}

/// Runs the configured workload to completion and reports.
///
/// # Errors
///
/// Returns a description of the first unexpected router error (the
/// only *expected* error is `ChannelSaturated`, which is counted and
/// handled via the cooloff window).
pub async fn run(cfg: DriverConfig) -> Result<DriverReport, String> {
    let epoch = Instant::now();
    let rss_before = rss_bytes().unwrap_or(0);
    let rss_watch = RssWatcher::spawn(Duration::from_millis(100));

    let router = Arc::new(SubscriptionRouter::new(RouterConfig {
        channel_capacity: cfg.channel_capacity,
        ..RouterConfig::default()
    }));

    let mut rng = Rng::new(cfg.seed);
    let placement_zipf = Zipf::new(cfg.shards, cfg.zipf_exponent);
    let write_zipf = Zipf::new(cfg.shards, cfg.zipf_exponent);

    // --- Subscribe phase -------------------------------------------------
    let slow_count = cfg.slow.map_or(0, |s| s.count.min(cfg.subscribers));
    let mut slots: Vec<SubSlot> = Vec::with_capacity(cfg.subscribers);
    let mut handles: Vec<JoinHandle<ConsumerStats>> = Vec::with_capacity(cfg.subscribers);
    let mut shard_members: Vec<Vec<usize>> = vec![Vec::new(); cfg.shards];
    let mut global_members: Vec<usize> = Vec::new();
    let mut subscribe_latency = LatencyRecorder::default();

    for idx in 0..cfg.subscribers {
        let slow = idx < slow_count;
        let global = slow || rng.chance_pct(cfg.global_subscriber_pct);
        if global {
            global_members.push(idx);
        } else {
            shard_members[placement_zipf.sample(&mut rng)].push(idx);
        }

        let subscribed_at = Instant::now();
        let response = subscribe_pass_through(
            &router,
            ConnectionId::new(idx as u64 + 1),
            &format!("suite-{idx}"),
            cfg.snapshot_rows,
        )?;
        subscribe_latency
            .record_us(u64::try_from(subscribed_at.elapsed().as_micros()).unwrap_or(u64::MAX));

        let (expected_tx, expected_rx) = oneshot::channel();
        handles.push(spawn_consumer(Consumer {
            stream: response.stream,
            router: Arc::clone(&router),
            sub: response.subscription_id,
            delay: if slow {
                cfg.slow.map(|s| s.delay)
            } else {
                None
            },
            ack_every: cfg.ack_every,
            epoch,
            subscribed_at,
            expected: expected_rx,
            stop_on_resync: false,
        }));
        slots.push(SubSlot {
            sub: response.subscription_id,
            delivered: 0,
            skip_until: 0,
            slow,
            expected_tx: Some(expected_tx),
        });
    }
    let rss_after_subscribe = rss_bytes().unwrap_or(0);

    // --- Write phase -----------------------------------------------------
    let mut workload = Workload::new(cfg.shards, cfg.mix);
    let mut txn_seq: u64 = 0;
    let mut deliveries: u64 = 0;
    let mut saturation_drops: u64 = 0;
    let write_started = Instant::now();
    let interval = (cfg.target_tps > 0)
        .then(|| Duration::from_nanos(1_000_000_000_u64.checked_div(cfg.target_tps).unwrap_or(1)));
    let mut next_tick = write_started;

    let pump = |txn_seq: u64,
                shard: usize,
                rows: usize,
                rng: &mut Rng,
                workload: &mut Workload,
                slots: &mut [SubSlot],
                deliveries: &mut u64,
                saturation_drops: &mut u64|
     -> Result<(), String> {
        let lsn = Lsn::new(SNAPSHOT_LSN + 1 + txn_seq);
        let diffs = workload.transaction(rng, shard, rows, lsn, epoch);
        if diffs.is_empty() {
            return Ok(());
        }
        for &member in global_members.iter().chain(&shard_members[shard]) {
            let slot = &mut slots[member];
            if slot.skip_until > txn_seq {
                continue;
            }
            let delta =
                QueryTransactionDelta::new(Some(txn_seq as u32), None, lsn, None, diffs.clone());
            match router.pump_transaction(slot.sub, delta, &[0]) {
                Ok(()) => {
                    slot.delivered += 1;
                    *deliveries += 1;
                }
                Err(RouterError::ChannelSaturated) => {
                    *saturation_drops += 1;
                    slot.skip_until = txn_seq + cfg.saturation_cooloff_txns;
                }
                Err(other) => return Err(format!("pump failed at txn {txn_seq}: {other}")),
            }
        }
        Ok(())
    };

    for paced_idx in 0..cfg.total_txns {
        if let Some(interval) = interval {
            next_tick += interval;
            let now = Instant::now();
            if next_tick > now {
                tokio::time::sleep(next_tick - now).await;
            }
        } else if paced_idx % 8 == 0 {
            // Unpaced runs still yield so consumers make progress on
            // small runtimes.
            tokio::task::yield_now().await;
        }

        let shard = write_zipf.sample(&mut rng);
        let rows = if let Some(bulk) = cfg
            .bulk
            .filter(|b| paced_idx > 0 && paced_idx % b.every == 0)
        {
            bulk.rows
        } else {
            workload.sample_txn_rows(&mut rng)
        };
        pump(
            txn_seq,
            shard,
            rows,
            &mut rng,
            &mut workload,
            &mut slots,
            &mut deliveries,
            &mut saturation_drops,
        )?;
        txn_seq += 1;

        if let Some(bursts) = cfg
            .bursts
            .filter(|b| paced_idx > 0 && paced_idx % b.every == 0)
        {
            for _ in 0..bursts.size {
                let rows = workload.sample_txn_rows(&mut rng);
                pump(
                    txn_seq,
                    0,
                    rows,
                    &mut rng,
                    &mut workload,
                    &mut slots,
                    &mut deliveries,
                    &mut saturation_drops,
                )?;
                txn_seq += 1;
            }
            tokio::task::yield_now().await;
        }
    }
    let wall_secs = write_started.elapsed().as_secs_f64();

    // --- Drain phase -----------------------------------------------------
    for slot in &mut slots {
        if let Some(tx) = slot.expected_tx.take() {
            let _ = tx.send(slot.delivered);
        }
    }
    let mut healthy_latency = LatencyRecorder::default();
    let mut slow_latency = LatencyRecorder::default();
    let mut events_received = 0_u64;
    let mut resyncs_healthy = 0_u64;
    let mut resyncs_slow = 0_u64;
    let mut initial_latency = LatencyRecorder::default();
    for (slot, handle) in slots.iter().zip(handles) {
        let stats = handle
            .await
            .map_err(|e| format!("consumer panicked: {e}"))?;
        events_received += stats.events;
        initial_latency.record_us(stats.initial_us);
        if slot.slow {
            resyncs_slow += stats.resyncs;
            slow_latency.merge(&stats.latency);
        } else {
            resyncs_healthy += stats.resyncs;
            healthy_latency.merge(&stats.latency);
        }
    }

    let rss_peak = rss_watch.finish().await;
    let metrics = router.metrics().snapshot();
    let per_sub = rss_after_subscribe
        .saturating_sub(rss_before)
        .checked_div(cfg.subscribers as u64)
        .unwrap_or(0);

    Ok(DriverReport {
        scenario: cfg.name.to_owned(),
        subscribers: cfg.subscribers,
        shards: cfg.shards,
        txns_generated: txn_seq,
        wall_secs,
        achieved_tps: txn_seq as f64 / wall_secs.max(f64::EPSILON),
        deliveries,
        deliveries_per_sec: deliveries as f64 / wall_secs.max(f64::EPSILON),
        events_received,
        update_latency: healthy_latency.summary(),
        slow_update_latency: cfg.slow.map(|_| slow_latency.summary()),
        subscribe_to_initial: initial_latency.summary(),
        saturation_drops,
        resyncs_received_healthy: resyncs_healthy,
        resyncs_received_slow: resyncs_slow,
        rss_before_bytes: rss_before,
        rss_after_subscribe_bytes: rss_after_subscribe,
        rss_peak_bytes: rss_peak,
        per_subscriber_rss_bytes: per_sub,
        router: RouterCounters::from(&metrics),
    })
}
