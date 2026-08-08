// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "Load-test suite for Palimpsest (§15.8 / §18.12).\n\nModels realistic large-scale workloads against an in-process\n`SubscriptionRouter`: Zipf-skewed multi-shard write traffic, mixed\ntransaction sizes, subscriber churn, slow consumers, bulk backfills,\nthe full `pgoutput` encode→decode pipeline, and permission-rule\nrevocation storms. Every scenario is deterministic under a fixed\nseed so latency numbers are comparable across runs.\n\nScenarios are exposed both as a standalone binary\n(`palimpsest-loadsuite`) for full-scale runs and as tiny \"smoke\"\nconfigurations exercised by `tests/smoke.rs` in CI."]
#![warn(missing_docs)]
#![allow(clippy::cast_precision_loss)]

pub mod driver;
pub mod fixture;
pub mod rng;
pub mod scenarios;
pub mod stats;
pub mod workload;
