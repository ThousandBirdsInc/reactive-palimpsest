// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Subscriber-churn scenario.
//!
//! Real fleets are never static: tabs open and close, mobile
//! clients roam, deploys restart pods. This scenario keeps a write
//! workload flowing while a churner concurrently subscribes new
//! clients (with realistically-sized bootstrap snapshots) and tears
//! others down, exercising `subscribe`, `unsubscribe`, snapshot
//! fetch, and the registry bookkeeping under contention.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_server::{
    ConnectionId, QueryTransactionDelta, RouterConfig, RouterError, SubscriptionId,
    SubscriptionRouter,
};
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::driver::{spawn_consumer, Consumer, ConsumerStats, RouterCounters};
use crate::fixture::{subscribe_pass_through, SNAPSHOT_LSN};
use crate::rng::{Rng, Zipf};
use crate::stats::{LatencyRecorder, LatencySummary};
use crate::workload::{Workload, WorkloadMix};

/// Churn scenario configuration.
#[derive(Debug, Clone)]
pub struct ChurnConfig {
    /// Population at start (and the target the churner hovers around).
    pub target_population: usize,
    /// Total churn operations (subscribe or unsubscribe).
    pub churn_ops: u64,
    /// Delay between churn operations.
    pub churn_interval: Duration,
    /// Write shards.
    pub shards: usize,
    /// Percent of subscribers on the global feed.
    pub global_subscriber_pct: u8,
    /// Zipf exponent for placement and writes.
    pub zipf_exponent: f64,
    /// Write pacing (transactions per second); `0` = unpaced.
    pub target_tps: u64,
    /// Bootstrap snapshot size for each subscribe.
    pub snapshot_rows: usize,
    /// Per-subscription channel depth.
    pub channel_capacity: usize,
    /// Master seed.
    pub seed: u64,
    /// Write mixture.
    pub mix: WorkloadMix,
}

impl ChurnConfig {
    /// Full-scale defaults: 1k live subscribers, 2k churn ops.
    #[must_use]
    pub fn full() -> Self {
        Self {
            target_population: 1_000,
            churn_ops: 2_000,
            churn_interval: Duration::from_millis(2),
            shards: 64,
            global_subscriber_pct: 10,
            zipf_exponent: 1.05,
            target_tps: 5_000,
            snapshot_rows: 256,
            channel_capacity: 256,
            seed: 0x0c0a_57e5,
            mix: WorkloadMix::default(),
        }
    }

    /// Tiny CI-friendly configuration.
    #[must_use]
    pub fn smoke() -> Self {
        Self {
            target_population: 16,
            churn_ops: 60,
            churn_interval: Duration::from_millis(1),
            target_tps: 2_000,
            snapshot_rows: 32,
            channel_capacity: 1_024,
            ..Self::full()
        }
    }
}

/// Churn scenario outcome.
#[derive(Debug, Clone, Serialize)]
pub struct ChurnReport {
    /// Scenario name (`churn`).
    pub scenario: String,
    /// Churn operations performed.
    pub churn_ops: u64,
    /// Subscribes performed (including the initial population).
    pub subscribes: u64,
    /// Unsubscribes performed.
    pub unsubscribes: u64,
    /// Router-reported active subscriptions at steady end.
    pub active_at_end: usize,
    /// Locally-tracked population at steady end (must match).
    pub tracked_at_end: usize,
    /// Transactions generated while churning.
    pub txns_generated: u64,
    /// Successful per-subscriber deliveries.
    pub deliveries: u64,
    /// Pumps that raced an unsubscribe (expected, counted).
    pub stale_pumps: u64,
    /// Pumps dropped by channel saturation.
    pub saturation_drops: u64,
    /// Wall-clock seconds.
    pub wall_secs: f64,
    /// Latency of the `subscribe` call itself (rewrite + snapshot).
    pub subscribe_call_latency: LatencySummary,
    /// Subscribe-to-`Initial` latency seen by consumers.
    pub subscribe_to_initial: LatencySummary,
    /// Commit-to-client update latency.
    pub update_latency: LatencySummary,
    /// Resyncs consumers observed.
    pub resyncs_received: u64,
    /// Router counters.
    pub router: RouterCounters,
}

impl fmt::Display for ChurnReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{}] ops={} subs={} unsubs={} population(end)={}({} tracked) txns={} deliveries={} wall={:.2}s",
            self.scenario,
            self.churn_ops,
            self.subscribes,
            self.unsubscribes,
            self.active_at_end,
            self.tracked_at_end,
            self.txns_generated,
            self.deliveries,
            self.wall_secs,
        )?;
        writeln!(f, "  subscribe call   : {}", self.subscribe_call_latency)?;
        writeln!(f, "  subscribe→initial: {}", self.subscribe_to_initial)?;
        writeln!(f, "  update latency   : {}", self.update_latency)?;
        write!(
            f,
            "  stale_pumps={} saturation_drops={} resyncs={}",
            self.stale_pumps, self.saturation_drops, self.resyncs_received
        )
    }
}

struct Entry {
    sub: SubscriptionId,
    shard: Option<usize>,
    delivered: u64,
    expected_tx: Option<oneshot::Sender<u64>>,
}

#[derive(Default)]
struct Population {
    /// `BTreeMap` keeps victim selection deterministic under a seed.
    entries: BTreeMap<u64, Entry>,
    retired: Vec<JoinHandle<ConsumerStats>>,
    live_handles: HashMap<u64, JoinHandle<ConsumerStats>>,
}

/// Runs the churn scenario.
///
/// # Errors
///
/// Returns a description of the first unexpected router error.
pub async fn run(cfg: ChurnConfig) -> Result<ChurnReport, String> {
    let epoch = Instant::now();
    let router = Arc::new(SubscriptionRouter::new(RouterConfig {
        channel_capacity: cfg.channel_capacity,
        ..RouterConfig::default()
    }));
    let population = Arc::new(Mutex::new(Population::default()));
    let stop = Arc::new(AtomicBool::new(false));

    let mut rng = Rng::new(cfg.seed);
    let placement_zipf = Zipf::new(cfg.shards, cfg.zipf_exponent);
    let mut next_entry_id: u64 = 0;
    let mut subscribe_latency = LatencyRecorder::default();
    let mut subscribes: u64 = 0;
    let mut unsubscribes: u64 = 0;

    let do_subscribe = |rng: &mut Rng,
                        next_entry_id: &mut u64,
                        subscribe_latency: &mut LatencyRecorder|
     -> Result<(), String> {
        let entry_id = *next_entry_id;
        *next_entry_id += 1;
        let shard = if rng.chance_pct(cfg.global_subscriber_pct) {
            None
        } else {
            Some(placement_zipf.sample(rng))
        };
        let subscribed_at = Instant::now();
        let response = subscribe_pass_through(
            &router,
            ConnectionId::new(entry_id + 1),
            &format!("churn-{entry_id}"),
            cfg.snapshot_rows,
        )?;
        subscribe_latency
            .record_us(u64::try_from(subscribed_at.elapsed().as_micros()).unwrap_or(u64::MAX));
        let (expected_tx, expected_rx) = oneshot::channel();
        let handle = spawn_consumer(Consumer {
            stream: response.stream,
            router: Arc::clone(&router),
            sub: response.subscription_id,
            delay: None,
            ack_every: 16,
            epoch,
            subscribed_at,
            expected: expected_rx,
            stop_on_resync: false,
        });
        let mut pop = population.lock().expect("population lock");
        pop.entries.insert(
            entry_id,
            Entry {
                sub: response.subscription_id,
                shard,
                delivered: 0,
                expected_tx: Some(expected_tx),
            },
        );
        pop.live_handles.insert(entry_id, handle);
        Ok(())
    };

    // Initial population.
    for _ in 0..cfg.target_population {
        do_subscribe(&mut rng, &mut next_entry_id, &mut subscribe_latency)?;
        subscribes += 1;
    }

    // Producer task: paced writes against the churning population.
    let producer = {
        let router = Arc::clone(&router);
        let population = Arc::clone(&population);
        let stop = Arc::clone(&stop);
        let mut rng = rng.fork();
        let write_zipf = Zipf::new(cfg.shards, cfg.zipf_exponent);
        let mix = cfg.mix;
        let shards = cfg.shards;
        let target_tps = cfg.target_tps;
        tokio::spawn(async move {
            let mut workload = Workload::new(shards, mix);
            let mut txn_seq: u64 = 0;
            let mut deliveries: u64 = 0;
            let mut stale_pumps: u64 = 0;
            let mut saturation_drops: u64 = 0;
            let interval = (target_tps > 0).then(|| {
                Duration::from_nanos(1_000_000_000_u64.checked_div(target_tps).unwrap_or(1))
            });
            let mut next_tick = Instant::now();
            let mut targets: Vec<(u64, SubscriptionId)> = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                if let Some(interval) = interval {
                    next_tick += interval;
                    let now = Instant::now();
                    if next_tick > now {
                        tokio::time::sleep(next_tick - now).await;
                    }
                } else if txn_seq % 8 == 0 {
                    tokio::task::yield_now().await;
                }
                let shard = write_zipf.sample(&mut rng);
                let rows = workload.sample_txn_rows(&mut rng);
                let lsn = Lsn::new(SNAPSHOT_LSN + 1 + txn_seq);
                let diffs = workload.transaction(&mut rng, shard, rows, lsn, epoch);
                txn_seq += 1;
                if diffs.is_empty() {
                    continue;
                }
                targets.clear();
                {
                    let pop = population.lock().expect("population lock");
                    for (id, entry) in &pop.entries {
                        if entry.shard.is_none() || entry.shard == Some(shard) {
                            targets.push((*id, entry.sub));
                        }
                    }
                }
                for &(entry_id, sub) in &targets {
                    let delta = QueryTransactionDelta::new(
                        Some(txn_seq as u32),
                        None,
                        lsn,
                        None,
                        diffs.clone(),
                    );
                    match router.pump_transaction(sub, delta, &[0]) {
                        Ok(()) => {
                            deliveries += 1;
                            if let Some(entry) = population
                                .lock()
                                .expect("population lock")
                                .entries
                                .get_mut(&entry_id)
                            {
                                entry.delivered += 1;
                            }
                        }
                        Err(RouterError::UnknownSubscription(_)) => stale_pumps += 1,
                        Err(RouterError::ChannelSaturated) => saturation_drops += 1,
                        Err(other) => {
                            return Err(format!("pump failed at txn {txn_seq}: {other}"));
                        }
                    }
                }
            }
            Ok::<_, String>((txn_seq, deliveries, stale_pumps, saturation_drops))
        })
    };

    // Churner: runs on the main task.
    let churn_started = Instant::now();
    for _ in 0..cfg.churn_ops {
        tokio::time::sleep(cfg.churn_interval).await;
        let current = population.lock().expect("population lock").entries.len();
        let add = if current < cfg.target_population {
            rng.chance_pct(70)
        } else {
            rng.chance_pct(30)
        };
        if add || current == 0 {
            do_subscribe(&mut rng, &mut next_entry_id, &mut subscribe_latency)?;
            subscribes += 1;
        } else {
            let (entry_id, sub, expected_tx, delivered, handle) = {
                let mut pop = population.lock().expect("population lock");
                let victim_idx = rng.below(pop.entries.len());
                let entry_id = *pop
                    .entries
                    .keys()
                    .nth(victim_idx)
                    .expect("victim index in range");
                let mut entry = pop.entries.remove(&entry_id).expect("victim present");
                let handle = pop
                    .live_handles
                    .remove(&entry_id)
                    .expect("handle tracked for entry");
                (
                    entry_id,
                    entry.sub,
                    entry.expected_tx.take(),
                    entry.delivered,
                    handle,
                )
            };
            // Tell the consumer how much it should have seen, then tear
            // the subscription down; either signal ends the task.
            if let Some(tx) = expected_tx {
                let _ = tx.send(delivered);
            }
            router
                .unsubscribe(sub)
                .map_err(|e| format!("unsubscribe {entry_id}: {e}"))?;
            unsubscribes += 1;
            population
                .lock()
                .expect("population lock")
                .retired
                .push(handle);
        }
    }
    let wall_secs = churn_started.elapsed().as_secs_f64();

    // Steady-state accounting before teardown.
    let tracked_at_end = population.lock().expect("population lock").entries.len();
    let active_at_end = router.active_subscriptions();

    // Stop the producer, then tear everything down.
    stop.store(true, Ordering::Relaxed);
    let (txns_generated, deliveries, stale_pumps, saturation_drops) = producer
        .await
        .map_err(|e| format!("producer panicked: {e}"))??;

    let mut handles = Vec::new();
    {
        let mut pop = population.lock().expect("population lock");
        let ids: Vec<u64> = pop.entries.keys().copied().collect();
        for entry_id in ids {
            let mut entry = pop.entries.remove(&entry_id).expect("entry present");
            if let Some(tx) = entry.expected_tx.take() {
                let _ = tx.send(entry.delivered);
            }
            router
                .unsubscribe(entry.sub)
                .map_err(|e| format!("teardown unsubscribe: {e}"))?;
            if let Some(handle) = pop.live_handles.remove(&entry_id) {
                handles.push(handle);
            }
        }
        handles.append(&mut pop.retired);
    }

    let mut update_latency = LatencyRecorder::default();
    let mut initial_latency = LatencyRecorder::default();
    let mut resyncs: u64 = 0;
    for handle in handles {
        let stats = handle
            .await
            .map_err(|e| format!("consumer panicked: {e}"))?;
        update_latency.merge(&stats.latency);
        initial_latency.record_us(stats.initial_us);
        resyncs += stats.resyncs;
    }

    let metrics = router.metrics().snapshot();
    Ok(ChurnReport {
        scenario: "churn".to_owned(),
        churn_ops: cfg.churn_ops,
        subscribes,
        unsubscribes,
        active_at_end,
        tracked_at_end,
        txns_generated,
        deliveries,
        stale_pumps,
        saturation_drops,
        wall_secs,
        subscribe_call_latency: subscribe_latency.summary(),
        subscribe_to_initial: initial_latency.summary(),
        update_latency: update_latency.summary(),
        resyncs_received: resyncs,
        router: RouterCounters::from(&metrics),
    })
}
