// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Permission-revocation storm scenario.
//!
//! Every active subscription carries a compiled, permission-rewritten
//! plan. Periodically the operator swaps the global rule set; the
//! router must force `Resync(PermissionsChanged)` onto every active
//! subscription, and every client then re-subscribes — recompiling
//! its rewritten plan and refetching its snapshot. At fleet scale
//! this is one of the most expensive control-plane events a sync
//! engine faces; this scenario measures both the server-side
//! revocation lag and the cost of the resubscribe storm.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_dataflow::palimpsest::{compile_mir, Lsn};
use palimpsest_permissions::{
    compile_rules, rewrite, PermissionRule, UserContext, UserContextSchema, UserValue,
};
use palimpsest_server::{
    ClientSubscriptionId, ConnectionId, QueryId, QueryTransactionDelta, RouterConfig, RouterError,
    SubscribeRequest, SubscriptionRouter,
};
use palimpsest_sql::{lower::parse_and_lower, Catalog, ColumnType};
use palimpsest_wal::TableId;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::driver::{spawn_consumer, Consumer, ConsumerStats, RouterCounters};
use crate::fixture::{posts_schema, posts_snapshot, OneShotProvider, SNAPSHOT_LSN};
use crate::rng::Rng;
use crate::stats::{LatencyRecorder, LatencySummary};
use crate::workload::{Workload, WorkloadMix};

/// Permission-storm configuration.
#[derive(Debug, Clone)]
pub struct PermissionStormConfig {
    /// Concurrent rule-guarded subscribers.
    pub subscribers: usize,
    /// Rule-set swaps (each forces a full-fleet resync).
    pub rounds: u64,
    /// Paced transactions pumped between swaps.
    pub txns_per_round: u64,
    /// Pacing (transactions per second); `0` = unpaced.
    pub target_tps: u64,
    /// Bootstrap snapshot rows per subscriber.
    pub snapshot_rows: usize,
    /// Per-subscription channel depth.
    pub channel_capacity: usize,
    /// Master seed.
    pub seed: u64,
}

impl PermissionStormConfig {
    /// Full-scale defaults: 500 compiled-plan subscribers, 5 storms.
    #[must_use]
    pub const fn full() -> Self {
        Self {
            subscribers: 500,
            rounds: 5,
            txns_per_round: 5_000,
            target_tps: 5_000,
            snapshot_rows: 64,
            channel_capacity: 256,
            seed: 0x0051_0e15,
        }
    }

    /// Tiny CI-friendly configuration.
    #[must_use]
    pub const fn smoke() -> Self {
        Self {
            subscribers: 12,
            rounds: 3,
            txns_per_round: 60,
            target_tps: 0,
            snapshot_rows: 8,
            channel_capacity: 1_024,
            ..Self::full()
        }
    }
}

/// Permission-storm outcome.
#[derive(Debug, Clone, Serialize)]
pub struct PermissionStormReport {
    /// Scenario name (`permission-storm`).
    pub scenario: String,
    /// Subscribers per round.
    pub subscribers: usize,
    /// Rule swaps performed.
    pub rounds: u64,
    /// `Resync(PermissionsChanged)` events consumers observed.
    pub resyncs_observed: u64,
    /// Expected resyncs (`subscribers × (rounds − 1)` swaps hit an
    /// active fleet; the first rule set is installed pre-subscribe).
    pub resyncs_expected: u64,
    /// Successful per-subscriber deliveries.
    pub deliveries: u64,
    /// Transaction events consumers received.
    pub events_received: u64,
    /// Mean wall-clock to drain a revocation (swap → every consumer
    /// observed its resync), seconds.
    pub revocation_drain_secs_avg: f64,
    /// Worst revocation drain, seconds.
    pub revocation_drain_secs_max: f64,
    /// Mean resubscribe-storm wall clock (rewrite + compile +
    /// subscribe for the whole fleet), seconds.
    pub resubscribe_storm_secs_avg: f64,
    /// Worst resubscribe storm, seconds.
    pub resubscribe_storm_secs_max: f64,
    /// Per-subscription subscribe latency (includes plan compile).
    pub subscribe_latency: LatencySummary,
    /// Commit-to-client latency between storms.
    pub update_latency: LatencySummary,
    /// Server-side revocation lag p50 (µs), from router metrics.
    pub revocation_lag_p50_us: f64,
    /// Server-side revocation lag p99 (µs), from router metrics.
    pub revocation_lag_p99_us: f64,
    /// Rule-set updates the router recorded.
    pub rule_updates: u64,
    /// Router counters.
    pub router: RouterCounters,
}

impl fmt::Display for PermissionStormReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{}] subs={} rounds={} resyncs={}/{} deliveries={} events={}",
            self.scenario,
            self.subscribers,
            self.rounds,
            self.resyncs_observed,
            self.resyncs_expected,
            self.deliveries,
            self.events_received,
        )?;
        writeln!(
            f,
            "  revocation drain: avg={:.3}s max={:.3}s (server lag p50={:.0}us p99={:.0}us)",
            self.revocation_drain_secs_avg,
            self.revocation_drain_secs_max,
            self.revocation_lag_p50_us,
            self.revocation_lag_p99_us,
        )?;
        writeln!(
            f,
            "  resubscribe storm: avg={:.3}s max={:.3}s",
            self.resubscribe_storm_secs_avg, self.resubscribe_storm_secs_max,
        )?;
        writeln!(f, "  subscribe latency: {}", self.subscribe_latency)?;
        write!(f, "  update latency   : {}", self.update_latency)
    }
}

fn rules_for_round(round: u64) -> Vec<palimpsest_permissions::CompiledRule> {
    let author = round % 2;
    let user_schema = UserContextSchema::new([("id".to_owned(), ColumnType::Int)]);
    compile_rules(
        &[PermissionRule::new(
            "posts_owner",
            "posts",
            format!("author_id = {author}"),
        )],
        &Catalog::demo(),
        &user_schema,
    )
    .expect("storm rule compiles")
}

/// Runs the permission-storm scenario.
///
/// # Errors
///
/// Returns a description of the first unexpected router, rewrite, or
/// plan-compilation error.
pub async fn run(cfg: PermissionStormConfig) -> Result<PermissionStormReport, String> {
    let epoch = Instant::now();
    let router = Arc::new(SubscriptionRouter::new(RouterConfig {
        channel_capacity: cfg.channel_capacity,
        ..RouterConfig::default()
    }));
    let graph = parse_and_lower("SELECT id, author_id, sent_at_ns FROM posts")
        .map_err(|e| format!("lower: {e}"))?;
    let posts_lookup = |table: &str| {
        (table == "posts").then(|| {
            (
                TableId::new(1),
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("author_id".to_owned(), ColumnType::Int),
                    ("sent_at_ns".to_owned(), ColumnType::Int),
                ]),
            )
        })
    };

    let mut rng = Rng::new(cfg.seed);
    let mut subscribe_latency = LatencyRecorder::default();
    let mut update_latency = LatencyRecorder::default();
    let mut resyncs_observed: u64 = 0;
    let mut deliveries: u64 = 0;
    let mut events_received: u64 = 0;
    let mut revocation_drains: Vec<f64> = Vec::new();
    let mut resubscribe_storms: Vec<f64> = Vec::new();

    let mut txn_seq: u64 = 0;
    let interval = (cfg.target_tps > 0)
        .then(|| Duration::from_nanos(1_000_000_000_u64.checked_div(cfg.target_tps).unwrap_or(1)));

    for round in 0..cfg.rounds {
        let compiled = rules_for_round(round);
        router.set_rules(compiled.clone());

        // --- Resubscribe storm: the whole fleet rebuilds its plans ---
        let storm_started = Instant::now();
        let mut handles: Vec<JoinHandle<ConsumerStats>> = Vec::with_capacity(cfg.subscribers);
        let mut expected_txs: Vec<oneshot::Sender<u64>> = Vec::with_capacity(cfg.subscribers);
        let mut subs = Vec::with_capacity(cfg.subscribers);
        for idx in 0..cfg.subscribers {
            let user_ctx = UserContext::new([("id".to_owned(), UserValue::Int(idx as i64))]);
            let subscribed_at = Instant::now();
            let rewritten = rewrite(&graph, &compiled, &user_ctx)
                .map_err(|e| format!("rewrite: {e}"))?
                .graph;
            let plan =
                compile_mir(&rewritten, &posts_lookup).map_err(|e| format!("compile: {e}"))?;
            let provider = OneShotProvider::new(posts_snapshot(cfg.snapshot_rows, 0));
            let response = router
                .subscribe(
                    SubscribeRequest {
                        connection: ConnectionId::new(round * 1_000_000 + idx as u64 + 1),
                        subscription_id: router.allocate_subscription_id(),
                        client_id: ClientSubscriptionId::new(format!("storm-{round}-{idx}")),
                        query: QueryId::new("posts.recent"),
                        query_graph: &graph,
                        user_ctx,
                        schema: posts_schema(),
                        resume_lsn: None,
                        compiled_plan: Some(plan),
                        prerun_initial: None,
                    },
                    &provider,
                )
                .map_err(|e| format!("subscribe: {e}"))?;
            subscribe_latency
                .record_us(u64::try_from(subscribed_at.elapsed().as_micros()).unwrap_or(u64::MAX));
            let (tx, rx) = oneshot::channel();
            handles.push(spawn_consumer(Consumer {
                stream: response.stream,
                router: Arc::clone(&router),
                sub: response.subscription_id,
                delay: None,
                ack_every: 16,
                epoch,
                subscribed_at,
                expected: rx,
                stop_on_resync: true,
            }));
            expected_txs.push(tx);
            subs.push(response.subscription_id);
        }
        resubscribe_storms.push(storm_started.elapsed().as_secs_f64());

        // --- Steady traffic between storms ---------------------------
        // Authors match the active rule so rows stay in-policy.
        let author_shard = (round % 2) as usize;
        let mut workload = Workload::new(2, WorkloadMix::default());
        let mut delivered = vec![0_u64; cfg.subscribers];
        let mut next_tick = Instant::now();
        for _ in 0..cfg.txns_per_round {
            if let Some(interval) = interval {
                next_tick += interval;
                let now = Instant::now();
                if next_tick > now {
                    tokio::time::sleep(next_tick - now).await;
                }
            } else if txn_seq % 8 == 0 {
                tokio::task::yield_now().await;
            }
            let rows = workload.sample_txn_rows(&mut rng);
            let lsn = Lsn::new(SNAPSHOT_LSN + 1 + txn_seq);
            let diffs = workload.transaction(&mut rng, author_shard, rows, lsn, epoch);
            txn_seq += 1;
            if diffs.is_empty() {
                continue;
            }
            for (idx, sub) in subs.iter().enumerate() {
                let delta = QueryTransactionDelta::new(
                    Some(txn_seq as u32),
                    None,
                    lsn,
                    None,
                    diffs.clone(),
                );
                match router.pump_transaction(*sub, delta, &[0]) {
                    Ok(()) => {
                        delivered[idx] += 1;
                        deliveries += 1;
                    }
                    Err(RouterError::ChannelSaturated) => {}
                    Err(other) => return Err(format!("pump failed at txn {txn_seq}: {other}")),
                }
            }
        }

        // --- End the round ------------------------------------------
        let last_round = round + 1 == cfg.rounds;
        let drain_started = Instant::now();
        if last_round {
            // No further swap: let consumers finish on delivered counts.
            for (tx, count) in expected_txs.into_iter().zip(&delivered) {
                let _ = tx.send(*count);
            }
        } else {
            // Swap now; consumers exit on Resync(PermissionsChanged).
            router.set_rules(rules_for_round(round + 1));
        }
        for handle in handles {
            let stats = handle
                .await
                .map_err(|e| format!("consumer panicked: {e}"))?;
            events_received += stats.events;
            resyncs_observed += stats.resyncs;
            update_latency.merge(&stats.latency);
        }
        if !last_round {
            revocation_drains.push(drain_started.elapsed().as_secs_f64());
        }
        for sub in subs {
            router
                .unsubscribe(sub)
                .map_err(|e| format!("teardown unsubscribe: {e}"))?;
        }
    }

    let metrics = router.metrics().snapshot();
    let avg = |v: &[f64]| {
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    };
    let max = |v: &[f64]| v.iter().copied().fold(0.0_f64, f64::max);
    Ok(PermissionStormReport {
        scenario: "permission-storm".to_owned(),
        subscribers: cfg.subscribers,
        rounds: cfg.rounds,
        resyncs_observed,
        resyncs_expected: cfg.subscribers as u64 * cfg.rounds.saturating_sub(1),
        deliveries,
        events_received,
        revocation_drain_secs_avg: avg(&revocation_drains),
        revocation_drain_secs_max: max(&revocation_drains),
        resubscribe_storm_secs_avg: avg(&resubscribe_storms),
        resubscribe_storm_secs_max: max(&resubscribe_storms),
        subscribe_latency: subscribe_latency.summary(),
        update_latency: update_latency.summary(),
        revocation_lag_p50_us: metrics.permission_revocation_lag_p50_us,
        revocation_lag_p99_us: metrics.permission_revocation_lag_p99_us,
        rule_updates: metrics.permission_rule_updates,
        router: RouterCounters::from(&metrics),
    })
}
