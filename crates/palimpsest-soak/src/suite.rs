// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Load-test suite binary (§15.8 / §18.12).
//!
//! Runs one or more named scenarios that model realistic large-scale
//! workloads against an in-process `SubscriptionRouter` and prints a
//! report per scenario. Usage:
//!
//! ```text
//! palimpsest-loadsuite [scenario ...]     # default: all
//! palimpsest-loadsuite steady-state churn
//! ```
//!
//! Environment knobs:
//!
//! * `PALIMPSEST_SUITE_SMOKE=1` — tiny CI-scale configurations.
//! * `PALIMPSEST_SUITE_SEED` — master seed override.
//! * `PALIMPSEST_SUITE_SUBSCRIBERS` — subscriber-count override.
//! * `PALIMPSEST_SUITE_TXNS` — transaction-count override.
//! * `PALIMPSEST_SUITE_TPS` — pacing override (0 = unpaced).
//! * `PALIMPSEST_SUITE_JSON=1` — emit one JSON object per scenario.
//! * `PALIMPSEST_SUITE_MAX_P99_US` — fail the run if any scenario's
//!   headline p99 update latency exceeds this budget.

use std::env;
use std::process::ExitCode;

use palimpsest_soak::scenarios::{
    self, churn::ChurnConfig, permission_storm::PermissionStormConfig,
    wal_pipeline::WalPipelineConfig,
};
use palimpsest_soak::{driver, stats::LatencySummary};

fn env_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|s| s.parse().ok())
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

struct Outcome {
    headline_p99_us: u64,
    text: String,
    json: String,
}

async fn run_scenario(name: &str, smoke: bool) -> Result<Outcome, String> {
    let subscribers = env_u64("PALIMPSEST_SUITE_SUBSCRIBERS").map(|v| v as usize);
    let txns = env_u64("PALIMPSEST_SUITE_TXNS");
    let tps = env_u64("PALIMPSEST_SUITE_TPS");
    let seed = env_u64("PALIMPSEST_SUITE_SEED");

    let driver_cfg = |mut cfg: driver::DriverConfig| {
        if let Some(v) = subscribers {
            cfg.subscribers = v;
        }
        if let Some(v) = txns {
            cfg.total_txns = v;
        }
        if let Some(v) = tps {
            cfg.target_tps = v;
        }
        if let Some(v) = seed {
            cfg.seed = v;
        }
        cfg
    };

    let outcome = |p99: u64, text: String, json: String| Outcome {
        headline_p99_us: p99,
        text,
        json,
    };
    let latency_p99 = |l: &LatencySummary| l.p99_us;

    match name {
        "steady-state" | "fanout-burst" | "slow-consumer" | "bulk-backfill" => {
            let cfg = driver_cfg(match name {
                "fanout-burst" => scenarios::fanout_burst(smoke),
                "slow-consumer" => scenarios::slow_consumer(smoke),
                "bulk-backfill" => scenarios::bulk_backfill(smoke),
                _ => scenarios::steady_state(smoke),
            });
            let report = driver::run(cfg).await?;
            Ok(outcome(
                latency_p99(&report.update_latency),
                report.to_string(),
                serde_json::to_string(&report).unwrap_or_default(),
            ))
        }
        "churn" => {
            let mut cfg = if smoke {
                ChurnConfig::smoke()
            } else {
                ChurnConfig::full()
            };
            if let Some(v) = subscribers {
                cfg.target_population = v;
            }
            if let Some(v) = txns {
                cfg.churn_ops = v;
            }
            if let Some(v) = tps {
                cfg.target_tps = v;
            }
            if let Some(v) = seed {
                cfg.seed = v;
            }
            let report = scenarios::churn::run(cfg).await?;
            Ok(outcome(
                latency_p99(&report.update_latency),
                report.to_string(),
                serde_json::to_string(&report).unwrap_or_default(),
            ))
        }
        "wal-pipeline" => {
            let mut cfg = if smoke {
                WalPipelineConfig::smoke()
            } else {
                WalPipelineConfig::full()
            };
            if let Some(v) = subscribers {
                cfg.subscribers = v;
            }
            if let Some(v) = txns {
                cfg.total_txns = v;
            }
            if let Some(v) = tps {
                cfg.target_tps = v;
            }
            if let Some(v) = seed {
                cfg.seed = v;
            }
            let report = scenarios::wal_pipeline::run(cfg).await?;
            Ok(outcome(
                latency_p99(&report.end_to_end_latency),
                report.to_string(),
                serde_json::to_string(&report).unwrap_or_default(),
            ))
        }
        "permission-storm" => {
            let mut cfg = if smoke {
                PermissionStormConfig::smoke()
            } else {
                PermissionStormConfig::full()
            };
            if let Some(v) = subscribers {
                cfg.subscribers = v;
            }
            if let Some(v) = txns {
                cfg.txns_per_round = v;
            }
            if let Some(v) = tps {
                cfg.target_tps = v;
            }
            if let Some(v) = seed {
                cfg.seed = v;
            }
            let report = scenarios::permission_storm::run(cfg).await?;
            Ok(outcome(
                latency_p99(&report.update_latency),
                report.to_string(),
                serde_json::to_string(&report).unwrap_or_default(),
            ))
        }
        other => Err(format!(
            "unknown scenario `{other}`; available: {}",
            scenarios::ALL.join(", ")
        )),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let selected: Vec<&str> = if args.is_empty() || args.iter().any(|a| a == "all") {
        scenarios::ALL.to_vec()
    } else {
        args.iter().map(String::as_str).collect()
    };
    let smoke = env_flag("PALIMPSEST_SUITE_SMOKE");
    let json = env_flag("PALIMPSEST_SUITE_JSON");
    let budget_p99_us = env_u64("PALIMPSEST_SUITE_MAX_P99_US");

    let mut failed = false;
    for name in selected {
        eprintln!(
            "palimpsest-loadsuite: running `{name}`{}",
            if smoke { " (smoke)" } else { "" }
        );
        match run_scenario(name, smoke).await {
            Ok(outcome) => {
                if json {
                    println!("{}", outcome.json);
                } else {
                    println!("{}", outcome.text);
                }
                if let Some(budget) = budget_p99_us {
                    if outcome.headline_p99_us > budget {
                        eprintln!(
                            "loadsuite: `{name}` p99 {}us exceeds budget {budget}us",
                            outcome.headline_p99_us
                        );
                        failed = true;
                    }
                }
            }
            Err(err) => {
                eprintln!("loadsuite: `{name}` failed: {err}");
                failed = true;
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
