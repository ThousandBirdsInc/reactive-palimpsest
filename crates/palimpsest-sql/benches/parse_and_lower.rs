// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Criterion benches for SQL parse + MIR build (§18.12, §15.9).
//!
//! Tracked via the bench-regression CI gate at 10%. Add new query
//! classes here when MIR support lands for them.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use palimpsest_sql::{parse_and_lower, parse_select};

const QUERIES: &[(&str, &str)] = &[
    ("trivial_select", "SELECT id FROM users"),
    (
        "filter_project",
        "SELECT id, name FROM users WHERE active = TRUE AND tenant_id = 7",
    ),
    (
        "two_table_join",
        "SELECT u.id, p.title FROM users u JOIN posts p ON p.author_id = u.id WHERE u.active",
    ),
    (
        "group_by_aggregate",
        "SELECT tenant_id, COUNT(*) AS n, SUM(amount) AS total FROM invoices GROUP BY tenant_id",
    ),
    (
        "cte_with_topk",
        "WITH recent AS (SELECT id, created_at FROM events ORDER BY created_at DESC LIMIT 100) \
         SELECT * FROM recent WHERE id > 1000",
    ),
];

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql/parse");
    for (name, sql) in QUERIES {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let stmt = parse_select(black_box(sql)).expect("parse");
                black_box(stmt);
            });
        });
    }
    group.finish();
}

fn bench_parse_and_lower(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql/parse_and_lower");
    for (name, sql) in QUERIES {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let mir = parse_and_lower(black_box(sql)).expect("lower");
                black_box(mir);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parse, bench_parse_and_lower);
criterion_main!(benches);
