// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Criterion bench for the pgoutput decoder (§18.12, §15.9).
//!
//! Generates a synthetic transaction with the test harness's
//! `WalGenerator`, then drives every frame through
//! `decode_pgoutput_message` in a tight loop. Reports events/sec.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use palimpsest_test_harness::{
    Catalog as HarnessCatalog, ColumnDef as HarnessColumn, LogicalEvent, TableDef,
    TableId as HarnessTableId, WalGenerator,
};
use palimpsest_wal::{decode_pgoutput_message, Catalog};

fn synthetic_frames(events_per_txn: usize) -> Vec<Bytes> {
    let table = HarnessTableId::new(7);
    let catalog = HarnessCatalog::with_tables([TableDef::new(
        table,
        "orders",
        vec![
            HarnessColumn {
                name: "id".into(),
                type_oid: 23,
                nullable: false,
            },
            HarnessColumn {
                name: "tenant_id".into(),
                type_oid: 23,
                nullable: false,
            },
            HarnessColumn {
                name: "amount".into(),
                type_oid: 23,
                nullable: false,
            },
        ],
    )]);
    let mut generator = WalGenerator::with_catalog(catalog);

    let mut events = Vec::with_capacity(events_per_txn + 2);
    events.push(LogicalEvent::Begin { xid: 42 });
    for i in 0..events_per_txn {
        events.push(LogicalEvent::Insert {
            table,
            new: vec![format!("{i}"), "1".into(), format!("{}", i * 3)],
        });
    }
    events.push(LogicalEvent::Commit);
    generator.encode_pgoutput(&events)
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal/decode");
    for size in [128_usize, 1024, 8192] {
        let frames = synthetic_frames(size);
        group.throughput(Throughput::Elements(frames.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &frames, |b, frames| {
            b.iter(|| {
                let mut catalog = Catalog::default();
                for frame in frames {
                    let event = decode_pgoutput_message(&mut catalog, black_box(frame.clone()));
                    let _ = black_box(event);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
