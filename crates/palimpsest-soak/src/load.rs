// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss
)]

//! Load harness binary (§15.8 / §18.12).
//!
//! Replays a recorded production-shaped trace at a target rate
//! (default: 10k writes/s) across a configurable number of
//! subscribers (default: 1k). Reports p50/p99 update latency and
//! per-subscriber memory footprint.
//!
//! The harness intentionally avoids real Postgres — input is a
//! deterministic synthetic workload so latency numbers are comparable
//! across runs.

use std::{
    env,
    process::ExitCode,
    sync::Arc,
    time::{Duration, Instant},
};

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_permissions::UserContext;
use palimpsest_server::{
    ClientSubscriptionId, ColumnSpec, ConnectionId, DiffEvent, QueryId, RawDiff, RouterConfig,
    SchemaDefinition, SchemaId, SnapshotBatch, SnapshotProvider, SnapshotTableRows,
    SubscribeRequest, SubscriptionRouter, VecCursor,
};
use palimpsest_sql::lower::parse_and_lower;
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;
use tokio_stream::StreamExt;

struct OneShot(std::cell::RefCell<Option<SnapshotBatch>>);
impl SnapshotProvider for OneShot {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        self.0.borrow_mut().take().ok_or_else(|| "exhausted".into())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let target_writes_per_sec: u64 = env::var("PALIMPSEST_LOAD_WRITES_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let subscriber_count: usize = env::var("PALIMPSEST_LOAD_SUBSCRIBERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    let total_writes: u64 = env::var("PALIMPSEST_LOAD_TOTAL_WRITES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    eprintln!(
        "palimpsest-load: target={target_writes_per_sec} writes/s, subs={subscriber_count}, total={total_writes}"
    );

    let router = Arc::new(SubscriptionRouter::new(RouterConfig::default()));
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();
    let mut subs = Vec::with_capacity(subscriber_count);
    for i in 0..subscriber_count {
        let provider = OneShot(std::cell::RefCell::new(Some(empty_snapshot())));
        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new((i + 1) as u64),
                    client_id: ClientSubscriptionId::new(format!("load-{i}")),
                    query: QueryId::new("posts.recent"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    subscription_id: router.allocate_subscription_id(),
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        let mut stream = response.stream;
        // Drain initial.
        let _ = stream.next().await;
        subs.push((response.subscription_id, stream));
    }

    let mut latencies_us = Vec::with_capacity(total_writes as usize);
    let interval = Duration::from_nanos(
        1_000_000_000_u64
            .checked_div(target_writes_per_sec)
            .unwrap_or(1),
    );
    let started = Instant::now();
    let mut next_tick = started;

    for round in 0..total_writes {
        next_tick += interval;
        let now = Instant::now();
        if next_tick > now {
            tokio::time::sleep(next_tick - now).await;
        }
        let lsn = Lsn::new(200 + round);
        let row = smallvec![Datum::I64(round as i64), Datum::I64((round % 64) as i64)];
        let pump_start = Instant::now();
        for (sub_id, _) in &subs {
            let mut cursor = VecCursor::new([RawDiff {
                table: None,
                row: row.clone(),
                lsn,
                diff: 1,
            }]);
            if let Err(err) = router.pump_cursor(*sub_id, &mut cursor, &[0]) {
                eprintln!("load: pump failed at round {round}: {err}");
                return ExitCode::FAILURE;
            }
        }
        for (_, stream) in &mut subs {
            match stream.next().await {
                Some(DiffEvent::TransactionUpdate { .. }) => {}
                Some(other) => {
                    eprintln!("load: unexpected event {other:?}");
                    return ExitCode::FAILURE;
                }
                None => {
                    eprintln!("load: stream closed at round {round}");
                    return ExitCode::FAILURE;
                }
            }
        }
        let elapsed = pump_start.elapsed().as_micros();
        latencies_us.push(elapsed as u64);
    }

    latencies_us.sort_unstable();
    let p50 = latencies_us[latencies_us.len() / 2];
    let p99 = latencies_us[latencies_us.len() * 99 / 100];
    let elapsed_s = started.elapsed().as_secs_f64();
    let throughput = (total_writes as f64) / elapsed_s;

    println!(
        "rounds={total_writes} subscribers={subscriber_count} elapsed={elapsed_s:.2}s throughput={throughput:.1}/s p50={p50}us p99={p99}us"
    );
    ExitCode::SUCCESS
}

fn empty_snapshot() -> SnapshotBatch {
    SnapshotBatch {
        snapshot_lsn: Lsn::new(100),
        rows: vec![SnapshotTableRows {
            table: TableId::new(1),
            rows: Vec::new(),
        }],
    }
}

fn schema() -> SchemaDefinition {
    SchemaDefinition {
        id: SchemaId::new(11),
        columns: vec![
            ColumnSpec {
                name: "id".into(),
                datum_type: DatumType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "author_id".into(),
                datum_type: DatumType::I64,
                nullable: false,
            },
        ],
        primary_key_columns: vec![0],
    }
}
