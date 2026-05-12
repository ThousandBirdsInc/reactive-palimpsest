//! Per-operator throughput micro-benchmarks (§18.5).
//!
//! Each scenario builds a single relational operator inside a one-shot
//! timely scope, drives a fixed-size input through it, and prints rows
//! per second. The bench harness is intentionally simple — it has no
//! criterion dependency, so `cargo bench -p palimpsest-dataflow` runs
//! quickly on CI as a smoke signal.

#![allow(clippy::all, clippy::nursery, clippy::pedantic)]

use std::time::Instant;

use palimpsest_dataflow::input::Input;
use palimpsest_dataflow::palimpsest::{
    aggregate_i64, distinct, equi_join, filter, project, topk, union, AggregateFunc, SortDirection,
};

fn main() {
    let scale: usize = std::env::var("PALIMPSEST_BENCH_SCALE")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(20_000);

    println!("# palimpsest-dataflow per-operator throughput");
    println!("scale={scale}");

    bench_filter(scale);
    bench_project(scale);
    bench_distinct(scale);
    bench_union(scale);
    bench_equi_join(scale);
    bench_aggregate(scale);
    bench_topk(scale);
}

fn report(operator: &str, rows: usize, elapsed: std::time::Duration) {
    let throughput = rows as f64 / elapsed.as_secs_f64();
    println!(
        "{operator:<14} rows={rows:<8} elapsed={:.3}ms throughput={:.1} rows/s",
        elapsed.as_secs_f64() * 1_000.0,
        throughput,
    );
}

fn bench_filter(scale: usize) {
    let input: Vec<i64> = (0..scale as i64).collect();
    let start = Instant::now();
    timely::example(move |scope| {
        let collection = scope.new_collection_from(input).1;
        let _ = filter(&collection, |value| value % 2 == 0);
    });
    report("filter", scale, start.elapsed());
}

fn bench_project(scale: usize) {
    let input: Vec<i64> = (0..scale as i64).collect();
    let start = Instant::now();
    timely::example(move |scope| {
        let collection = scope.new_collection_from(input).1;
        let _ = project(&collection, |value| value.wrapping_mul(7));
    });
    report("project", scale, start.elapsed());
}

fn bench_distinct(scale: usize) {
    let input: Vec<u64> = (0..scale).map(|index| (index as u64) % 64).collect();
    let start = Instant::now();
    timely::example(move |scope| {
        let collection = scope.new_collection_from(input).1;
        let _ = distinct(&collection);
    });
    report("distinct", scale, start.elapsed());
}

fn bench_union(scale: usize) {
    let half = scale / 2;
    let left: Vec<u64> = (0..half as u64).collect();
    let right: Vec<u64> = (half as u64..scale as u64).collect();
    let start = Instant::now();
    timely::example(move |scope| {
        let left_collection = scope.new_collection_from(left).1;
        let right_collection = scope.new_collection_from(right).1;
        let _ = union(&left_collection, &right_collection);
    });
    report("union", scale, start.elapsed());
}

fn bench_equi_join(scale: usize) {
    let left: Vec<(u64, u64)> = (0..scale as u64).map(|index| (index % 64, index)).collect();
    let right: Vec<(u64, u64)> = (0..64_u64).map(|index| (index, index * 10)).collect();
    let start = Instant::now();
    timely::example(move |scope| {
        let left_collection = scope.new_collection_from(left).1;
        let right_collection = scope.new_collection_from(right).1;
        let _ = equi_join(
            &left_collection,
            &right_collection,
            |key, left_value, right_value| (*key, *left_value, *right_value),
        );
    });
    report("equi_join", scale, start.elapsed());
}

fn bench_aggregate(scale: usize) {
    let input: Vec<(u64, i64)> = (0..scale)
        .map(|index| ((index as u64) % 32, index as i64))
        .collect();
    let start = Instant::now();
    timely::example(move |scope| {
        let collection = scope.new_collection_from(input).1;
        let _ = aggregate_i64(&collection, vec![AggregateFunc::Count, AggregateFunc::Sum]);
    });
    report("aggregate", scale, start.elapsed());
}

fn bench_topk(scale: usize) {
    let input: Vec<i64> = (0..scale as i64).map(|index| index % 1024).collect();
    let start = Instant::now();
    timely::example(move |scope| {
        let collection = scope.new_collection_from(input).1;
        let _ = topk(&collection, SortDirection::Descending, 16, 0);
    });
    report("topk", scale, start.elapsed());
}
