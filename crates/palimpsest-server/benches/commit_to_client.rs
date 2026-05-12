// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(
    clippy::cast_possible_wrap,
    clippy::missing_const_for_fn,
    clippy::doc_markdown
)]

//! End-to-end criterion bench: commit (`RawDiff` at LSN L) → client
//! receives the matching `DiffEvent::Update` (§18.12, §15.9).
//!
//! Builds the router in-process, opens a subscription, and measures
//! the time from `pump_cursor` returning to the test stream yielding
//! the diff event. Snapshot is delivered once before the loop so we're
//! only timing steady-state pump → deliver.

use std::cell::RefCell;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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

struct ScriptedProvider {
    response: RefCell<Option<SnapshotBatch>>,
}
impl ScriptedProvider {
    fn new(batch: SnapshotBatch) -> Self {
        Self {
            response: RefCell::new(Some(batch)),
        }
    }
}
impl SnapshotProvider for ScriptedProvider {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        Ok(self
            .response
            .borrow_mut()
            .take()
            .unwrap_or_else(|| SnapshotBatch {
                snapshot_lsn: Lsn::new(100),
                rows: Vec::new(),
            }))
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

fn snapshot(rows: usize) -> SnapshotBatch {
    SnapshotBatch {
        snapshot_lsn: Lsn::new(100),
        rows: vec![SnapshotTableRows {
            table: TableId::new(1),
            rows: (0..rows as i64)
                .map(|i| smallvec![Datum::I64(i), Datum::I64(i % 8)])
                .collect(),
        }],
    }
}

fn bench_pump_to_deliver(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let mut group = c.benchmark_group("server/commit_to_client");
    for batch in [1_usize, 16, 256] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter_custom(|iters| {
                runtime.block_on(async move {
                    let router = SubscriptionRouter::new(RouterConfig::default());
                    let provider = ScriptedProvider::new(snapshot(0));
                    let graph = parse_and_lower("SELECT id FROM posts").unwrap();
                    let response = router
                        .subscribe(
                            SubscribeRequest {
                                connection: ConnectionId::new(1),
                                client_id: ClientSubscriptionId::new("posts.recent"),
                                query: QueryId::new("posts.recent"),
                                query_graph: &graph,
                                user_ctx: UserContext::new(std::iter::empty()),
                                schema: schema(),
                                resume_lsn: None,
                compiled_plan: None,
                            },
                            &provider,
                        )
                        .unwrap();
                    let mut stream = response.stream;
                    // Drain the initial snapshot event before timing.
                    let _ = stream.next().await;

                    let start = std::time::Instant::now();
                    for round in 0..iters {
                        let lsn = Lsn::new(200 + round);
                        let diffs: Vec<RawDiff> = (0..batch)
                            .map(|i| RawDiff {
                                row: smallvec![
                                    Datum::I64((round * batch as u64 + i as u64) as i64),
                                    Datum::I64((i % 8) as i64),
                                ],
                                lsn,
                                diff: 1,
                            })
                            .collect();
                        let mut cursor = VecCursor::new(diffs);
                        router
                            .pump_cursor(response.subscription_id, &mut cursor, &[0])
                            .unwrap();
                        let event = stream.next().await.expect("event");
                        let _ = black_box(event);
                    }
                    start.elapsed()
                })
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_pump_to_deliver);
criterion_main!(benches);

// Quiet the unused-import warning when the bench is compiled but not
// fully expanded (e.g., `cargo check --benches`).
#[allow(dead_code)]
fn _silence_unused() {
    let _ = DiffEvent::Initial {
        rows: Vec::new(),
        lsn: Lsn::new(0),
    };
}
