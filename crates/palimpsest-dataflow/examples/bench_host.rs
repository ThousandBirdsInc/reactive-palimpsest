//! Microbench for PersistentHost::push_table_batch over varying input sizes.

use std::collections::HashMap;
use std::time::Instant;

use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_dataflow::palimpsest::{compile_mir, PersistentHost, Lsn, Row};
use palimpsest_sql::catalog::ColumnType;
use palimpsest_sql::lower::parse_and_lower;
use palimpsest_wal::{Datum, TableId};
use smallvec::smallvec;

const SQL: &str = "WITH per_category AS (
    SELECT category_id, COUNT(*) AS n, SUM(value) AS total
    FROM events
    GROUP BY category_id
)
SELECT category_id, n, total
FROM per_category
ORDER BY total DESC
LIMIT 8";

fn events_schema() -> ScalarSchema {
    ScalarSchema::from_pairs([
        ("id".to_owned(), ColumnType::Int),
        ("category_id".to_owned(), ColumnType::Int),
        ("value".to_owned(), ColumnType::Int),
    ])
}

fn lookup(table: &str) -> Option<(TableId, ScalarSchema)> {
    if table == "events" {
        Some((TableId::new(2), events_schema()))
    } else {
        None
    }
}

fn row(id: i64, cat: i64, value: i64) -> Row {
    smallvec![Datum::I64(id), Datum::I64(cat), Datum::I64(value)]
}

fn run(n_seed: usize, n_categories: i64, n_diff: usize) {
    let graph = parse_and_lower(SQL).unwrap();
    let plan = compile_mir(&graph, &lookup).unwrap();
    let host = PersistentHost::new();
    let canonical = "events.top_categories";

    let mut seed = Vec::with_capacity(n_seed);
    let mut state: u64 = 1;
    for i in 0..n_seed {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let cat = (state % n_categories as u64) as i64 + 1;
        let value = (state >> 32) as i64 % 1000;
        seed.push(row(i as i64 + 1, cat, value));
    }

    let mut inputs = HashMap::new();
    inputs.insert(TableId::new(2), seed);

    let start = Instant::now();
    let _ = host.register_or_seed(canonical, &plan, inputs);
    let seed_time = start.elapsed();
    println!("seed({n_seed} rows): {seed_time:?}");

    // Now push a batch of N diffs.
    let mut batch = Vec::with_capacity(n_diff);
    for i in 0..n_diff {
        batch.push((TableId::new(2), row((n_seed + i + 1) as i64, 7, 100), 1));
    }
    let start = Instant::now();
    let deltas = host.push_table_batch(canonical, batch, Lsn::new(2));
    let push_time = start.elapsed();
    println!("push_batch({n_diff} rows over {n_seed}-row state): {push_time:?} → {} deltas", deltas.len());
}

fn main() {
    run(1_000, 50, 100);
    run(10_000, 50, 100);
    run(100_000, 50, 1_000);
    run(300_000, 50, 1_000);
}
