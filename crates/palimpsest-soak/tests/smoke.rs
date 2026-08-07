// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CI smoke coverage for the load-test suite.
//!
//! Each scenario runs at tiny scale on a multi-thread runtime and is
//! checked for *structural* invariants — delivery accounting,
//! isolation, resync bookkeeping — never for absolute latency
//! numbers, which would flake on shared CI hosts.

use std::time::Duration;

use palimpsest_soak::driver;
use palimpsest_soak::scenarios::{
    self, churn::ChurnConfig, permission_storm::PermissionStormConfig,
    wal_pipeline::WalPipelineConfig,
};

const SMOKE_TIMEOUT: Duration = Duration::from_secs(120);

async fn run_driver(cfg: driver::DriverConfig) -> driver::DriverReport {
    tokio::time::timeout(SMOKE_TIMEOUT, driver::run(cfg))
        .await
        .expect("scenario timed out")
        .expect("scenario failed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn steady_state_delivers_everything_without_resyncs() {
    let report = run_driver(scenarios::steady_state(true)).await;

    assert_eq!(report.txns_generated, 200);
    // Every successful pump must be received by its consumer.
    assert_eq!(report.events_received, report.deliveries);
    assert!(report.deliveries > 0, "workload produced no fan-out");
    // Healthy steady state: no saturation, no resyncs.
    assert_eq!(report.saturation_drops, 0);
    assert_eq!(report.resyncs_received_healthy, 0);
    assert!(report.update_latency.count > 0);
    assert_eq!(report.router.subscriptions_total, 24);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_burst_accounting_stays_consistent() {
    let report = run_driver(scenarios::fanout_burst(true)).await;

    // Bursts ran: paced 200 plus 32-per-burst extras.
    assert!(report.txns_generated > 200);
    assert_eq!(report.events_received, report.deliveries);
    // Saturation (if any) must be matched by router-side accounting.
    assert!(report.saturation_drops <= report.router.channel_full_events);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_consumers_saturate_without_hurting_healthy_subs() {
    let report = run_driver(scenarios::slow_consumer(true)).await;

    assert_eq!(report.events_received, report.deliveries);
    // The slow population must actually have hit backpressure...
    assert!(
        report.saturation_drops > 0,
        "slow consumers never saturated; scenario lost its point"
    );
    // ...while healthy subscribers stayed clean.
    assert_eq!(report.resyncs_received_healthy, 0);
    assert!(report.update_latency.count > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_backfill_keeps_delivery_accounting() {
    let report = run_driver(scenarios::bulk_backfill(true)).await;

    assert_eq!(report.events_received, report.deliveries);
    assert!(report.deliveries > 0);
    assert_eq!(report.resyncs_received_healthy, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn churn_keeps_registry_consistent() {
    let report = tokio::time::timeout(SMOKE_TIMEOUT, scenarios::churn::run(ChurnConfig::smoke()))
        .await
        .expect("scenario timed out")
        .expect("scenario failed");

    assert_eq!(report.churn_ops, 60);
    assert_eq!(
        report.subscribes - report.unsubscribes,
        report.tracked_at_end as u64
    );
    // The router's registry must agree with our own bookkeeping.
    assert_eq!(report.active_at_end, report.tracked_at_end);
    assert!(report.subscribes as usize >= 16);
    assert!(report.subscribe_call_latency.count > 0);
    assert!(report.txns_generated > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_pipeline_decodes_and_routes() {
    let report = tokio::time::timeout(
        SMOKE_TIMEOUT,
        scenarios::wal_pipeline::run(WalPipelineConfig::smoke()),
    )
    .await
    .expect("scenario timed out")
    .expect("scenario failed");

    assert_eq!(report.txns_generated, 300);
    assert!(report.frames_decoded > 0);
    assert!(report.bytes_decoded > 0);
    // Multi-table stream: some rows must be off-query.
    assert!(report.offquery_rows > 0);
    assert!(report.deliveries > 0);
    assert!(report.end_to_end_latency.count > 0);
    assert_eq!(report.saturation_drops, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permission_storm_revokes_the_whole_fleet() {
    let report = tokio::time::timeout(
        SMOKE_TIMEOUT,
        scenarios::permission_storm::run(PermissionStormConfig::smoke()),
    )
    .await
    .expect("scenario timed out")
    .expect("scenario failed");

    // Every swap must resync every active subscription.
    assert_eq!(report.resyncs_observed, report.resyncs_expected);
    assert!(report.rule_updates >= report.rounds);
    assert!(report.deliveries > 0);
    assert_eq!(report.events_received, report.deliveries);
    assert!(report.subscribe_latency.count > 0);
}
