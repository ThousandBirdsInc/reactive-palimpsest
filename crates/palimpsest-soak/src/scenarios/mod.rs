// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The load-test scenario catalog.
//!
//! Four scenarios reuse the shared [`crate::driver`] engine with
//! different knobs; three (`churn`, `wal-pipeline`,
//! `permission-storm`) have bespoke drivers because they exercise
//! lifecycle, decode, and permission paths the engine does not.

pub mod churn;
pub mod permission_storm;
pub mod wal_pipeline;

use std::time::Duration;

use crate::driver::{BulkBackfill, Bursts, DriverConfig, SlowConsumers};

/// Steady-state fan-out: the §15.8 "Figma envelope".
///
/// 1k subscribers (10% on a global feed, the rest following
/// Zipf-popular shards), 10k paced writes/s of mixed
/// insert/update/delete transactions with realistic size mixture.
#[must_use]
pub fn steady_state(smoke: bool) -> DriverConfig {
    if smoke {
        DriverConfig::smoke()
    } else {
        DriverConfig::steady_state()
    }
}

/// Thundering-herd bursts against the hottest shard.
///
/// A moderate baseline load, plus periodic back-to-back bursts on
/// shard 0 (the most-subscribed shard). Measures burst drain
/// latency and whether saturation stays contained to the burst.
#[must_use]
pub fn fanout_burst(smoke: bool) -> DriverConfig {
    let mut cfg = if smoke {
        DriverConfig {
            bursts: Some(Bursts {
                every: 50,
                size: 32,
            }),
            channel_capacity: 16,
            ..DriverConfig::smoke()
        }
    } else {
        DriverConfig {
            total_txns: 20_000,
            target_tps: 2_000,
            bursts: Some(Bursts {
                every: 2_000,
                size: 512,
            }),
            ..DriverConfig::steady_state()
        }
    };
    cfg.name = "fanout-burst";
    cfg
}

/// Slow-consumer isolation.
///
/// A small population of slow clients (mobile radios, throttled
/// tabs) drains with per-event think time while the rest keep up.
/// The interesting outcome: slow channels saturate and resync
/// **without** degrading healthy subscribers.
#[must_use]
pub fn slow_consumer(smoke: bool) -> DriverConfig {
    let mut cfg = if smoke {
        DriverConfig {
            slow: Some(SlowConsumers {
                count: 4,
                delay: Duration::from_millis(5),
            }),
            channel_capacity: 8,
            total_txns: 300,
            ..DriverConfig::smoke()
        }
    } else {
        DriverConfig {
            slow: Some(SlowConsumers {
                count: 50,
                delay: Duration::from_millis(2),
            }),
            total_txns: 50_000,
            target_tps: 5_000,
            ..DriverConfig::steady_state()
        }
    };
    cfg.name = "slow-consumer";
    cfg
}

/// Bulk backfill interleaved with interactive traffic.
///
/// Periodic multi-thousand-row transactions (a migration or import)
/// land while normal OLTP-shaped writes flow; tail latency of the
/// interactive traffic is the number to watch.
#[must_use]
pub fn bulk_backfill(smoke: bool) -> DriverConfig {
    let mut cfg = if smoke {
        DriverConfig {
            bulk: Some(BulkBackfill {
                every: 40,
                rows: 256,
            }),
            ..DriverConfig::smoke()
        }
    } else {
        DriverConfig {
            total_txns: 30_000,
            target_tps: 3_000,
            bulk: Some(BulkBackfill {
                every: 3_000,
                rows: 8_192,
            }),
            ..DriverConfig::steady_state()
        }
    };
    cfg.name = "bulk-backfill";
    cfg
}

/// Names of every scenario in the catalog, in run order.
pub const ALL: &[&str] = &[
    "steady-state",
    "fanout-burst",
    "slow-consumer",
    "bulk-backfill",
    "churn",
    "wal-pipeline",
    "permission-storm",
];
