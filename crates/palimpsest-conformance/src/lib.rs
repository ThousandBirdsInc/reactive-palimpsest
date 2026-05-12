// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Real-Postgres conformance harness for Palimpsest (§18.13).
//!
//! This crate is **nightly-only and opt-in**. The default build is
//! empty so the main workspace `cargo build` / `cargo test` does not
//! require a Postgres process. Conformance work is gated behind the
//! `real-postgres` cargo feature and the `PALIMPSEST_PG_URL`
//! environment variable.
//!
//! ```text
//! # CI nightly:
//! PALIMPSEST_PG_URL=postgres://palimpsest:palimpsest@localhost:5432/palimpsest \
//!     cargo test -p palimpsest-conformance --features real-postgres
//! ```
//!
//! Layout (each as its own integration test file under `tests/`):
//!
//! - `wire_bytes.rs` — real-PG pgoutput vs. `WalGenerator::encode_pgoutput`.
//! - `catalog_responses.rs` — real-PG catalog SELECTs vs. checked-in fixtures.
//! - `oracle.rs` — property-test corpus replayed on real-PG, compared to
//!   `ReferenceExecutor`.
//! - `replication_smoke.rs` — subset of §15.6 scenarios end-to-end on
//!   real-PG.
//!
//! When the `real-postgres` feature is **off**, every conformance test
//! file becomes a no-op (compiles, prints a skip message, exits 0).
//! That is what local PR runs and the default `cargo test --workspace`
//! see — only the nightly conformance workflow turns the feature on.

#![cfg_attr(not(feature = "real-postgres"), allow(dead_code))]

#[cfg(feature = "real-postgres")]
pub mod harness;

/// Env var holding the conformance Postgres URL.
///
/// Tests look this up; if it is missing, they print a skip message and
/// pass — even with the feature on. That makes local
/// `cargo test -p palimpsest-conformance --features real-postgres`
/// a useful "dry-run" without a database.
pub const PG_URL_ENV: &str = "PALIMPSEST_PG_URL";

/// Logical replication slot name used by every conformance test
/// (drop+recreate on entry to keep runs idempotent).
pub const REPL_SLOT: &str = "palimpsest_conformance";

/// Logical publication name. Created if missing.
pub const REPL_PUBLICATION: &str = "palimpsest_conformance_pub";
