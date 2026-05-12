// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Criterion variant of `operator_throughput` (§18.12, §15.9).
//!
//! Runs the same per-operator scenarios but emits criterion output so
//! the regression-gate workflow can compare PR vs. main baselines.
//! The non-criterion version is kept around for ad-hoc local runs.

#![allow(clippy::all, clippy::nursery, clippy::pedantic)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use palimpsest_dataflow::input::Input;
use palimpsest_dataflow::palimpsest::{
    aggregate_i64, distinct, equi_join, filter, project, topk, union, AggregateFunc, SortDirection,
};

const SCALES: &[usize] = &[1_024, 16_384];

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/filter");
    for scale in SCALES {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), scale, |b, &scale| {
            b.iter(|| {
                let input: Vec<i64> = (0..scale as i64).collect();
                timely::example(move |scope| {
                    let collection = scope.new_collection_from(input).1;
                    let _ = filter(&collection, |value| black_box(value) % 2 == 0);
                });
            });
        });
    }
    group.finish();
}

fn bench_project(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/project");
    for scale in SCALES {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), scale, |b, &scale| {
            b.iter(|| {
                let input: Vec<i64> = (0..scale as i64).collect();
                timely::example(move |scope| {
                    let collection = scope.new_collection_from(input).1;
                    let _ = project(&collection, |value| black_box(value).wrapping_mul(7));
                });
            });
        });
    }
    group.finish();
}

fn bench_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/distinct");
    for scale in SCALES {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), scale, |b, &scale| {
            b.iter(|| {
                let input: Vec<u64> = (0..scale).map(|i| (i as u64) % 64).collect();
                timely::example(move |scope| {
                    let collection = scope.new_collection_from(input).1;
                    let _ = distinct(&collection);
                });
            });
        });
    }
    group.finish();
}

fn bench_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/union");
    for scale in SCALES {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), scale, |b, &scale| {
            b.iter(|| {
                let half = scale / 2;
                let left: Vec<u64> = (0..half as u64).collect();
                let right: Vec<u64> = (half as u64..scale as u64).collect();
                timely::example(move |scope| {
                    let l = scope.new_collection_from(left).1;
                    let r = scope.new_collection_from(right).1;
                    let _ = union(&l, &r);
                });
            });
        });
    }
    group.finish();
}

fn bench_equi_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/equi_join");
    for scale in SCALES {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), scale, |b, &scale| {
            b.iter(|| {
                let left: Vec<(u64, u64)> = (0..scale as u64).map(|i| (i % 64, i)).collect();
                let right: Vec<(u64, u64)> = (0..64_u64).map(|i| (i, i * 10)).collect();
                timely::example(move |scope| {
                    let l = scope.new_collection_from(left).1;
                    let r = scope.new_collection_from(right).1;
                    let _ = equi_join(&l, &r, |k, lv, rv| (*k, *lv, *rv));
                });
            });
        });
    }
    group.finish();
}

fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/aggregate");
    for scale in SCALES {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), scale, |b, &scale| {
            b.iter(|| {
                let input: Vec<(u64, i64)> =
                    (0..scale).map(|i| ((i as u64) % 32, i as i64)).collect();
                timely::example(move |scope| {
                    let collection = scope.new_collection_from(input).1;
                    let _ =
                        aggregate_i64(&collection, vec![AggregateFunc::Count, AggregateFunc::Sum]);
                });
            });
        });
    }
    group.finish();
}

fn bench_topk(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/topk");
    for scale in SCALES {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), scale, |b, &scale| {
            b.iter(|| {
                let input: Vec<i64> = (0..scale as i64).map(|i| i % 1024).collect();
                timely::example(move |scope| {
                    let collection = scope.new_collection_from(input).1;
                    let _ = topk(&collection, SortDirection::Descending, 16, 0);
                });
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_filter,
    bench_project,
    bench_distinct,
    bench_union,
    bench_equi_join,
    bench_aggregate,
    bench_topk,
);
criterion_main!(benches);
