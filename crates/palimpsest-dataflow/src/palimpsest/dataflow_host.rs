// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Embed-shim that hosts dataflows for the router.
//!
//! Two public entry points:
//!
//! * [`snapshot_run`] — stateless: build a fresh dataflow, feed it
//!   the supplied snapshot, capture output rows, tear it down.
//!   The router uses this on the snapshot path
//!   ([`crate::palimpsest::compile_mir::install_plan`] does the
//!   real work).
//!
//! * [`PersistentHost`] — long-lived per-server-process state that
//!   tracks each subscription's cumulative inputs + last-emitted
//!   aggregate. On every mutation, the host re-runs `snapshot_run`
//!   over the cumulative state, diffs the new aggregate against
//!   the previous one (by row), and emits `(row, lsn, ±1)` deltas
//!   the cursor pump forwards to the router.
//!
//! The persistent host is intentionally NOT a long-running timely
//! worker. Wiring a single worker that holds `!Send`
//! `InputSession`s across many subscriptions and bridges them back
//! to async tokio code through the existing `WorkerHandle::Build`
//! channel is its own substantial milestone; the comparison-based
//! diff approach below is correct for the demo (and any aggregate
//! whose output fits comfortably in memory) without that complexity.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use palimpsest_wal::TableId;
use timely::dataflow::operators::Inspect;

use crate::input::Input;
use crate::palimpsest::compile_mir::{install_plan, CompiledPlan};
use crate::palimpsest::time::Lsn;
use crate::palimpsest::wal::Row;

// -----------------------------------------------------------------------------
// Stateless snapshot run (used on the FreshInitial path)
// -----------------------------------------------------------------------------

/// Compile and run `plan` against `inputs`, returning the output
/// rows the dataflow produces at the final frontier.
#[must_use]
pub fn snapshot_run(plan: &CompiledPlan, inputs: HashMap<TableId, Vec<Row>>) -> Vec<Row> {
    let captured: Arc<Mutex<Vec<Row>>> = Arc::new(Mutex::new(Vec::new()));
    let cap = Arc::clone(&captured);
    let plan = plan.clone();

    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let mut input_collections = HashMap::new();
            for table in &plan.inputs {
                let rows = inputs.get(table).cloned().unwrap_or_default();
                let (_, collection) = scope.new_collection_from(rows);
                input_collections.insert(*table, collection);
            }
            let output = install_plan(&plan, scope, &input_collections);
            let cap_inner = Arc::clone(&cap);
            output.inner.inspect(move |entry: &(Row, u64, isize)| {
                let (row, _time, diff) = entry;
                if *diff > 0 {
                    cap_inner
                        .lock()
                        .expect("capture mutex")
                        .push(row.clone());
                }
            });
        });
    });

    let mut rows = captured.lock().expect("capture mutex");
    std::mem::take(&mut *rows)
}

// -----------------------------------------------------------------------------
// Persistent host (cumulative-replay diff streaming)
// -----------------------------------------------------------------------------

/// State kept per registered plan.
struct PlanState {
    plan: CompiledPlan,
    inputs: HashMap<TableId, Vec<Row>>,
    last_output: Vec<Row>,
    refcount: usize,
}

#[derive(Default)]
struct HostInner {
    plans: HashMap<String, PlanState>,
}

/// Long-lived host that drives compiled plans incrementally. Mutex-
/// guarded `HostInner` is the single point of synchronization — the
/// host is `Send + Sync` so the router can hold one `Arc` and call
/// methods from any task.
pub struct PersistentHost {
    inner: Arc<Mutex<HostInner>>,
}

/// A single change in the aggregate output, produced by
/// [`PersistentHost::push_table_diff`] / [`PersistentHost::register_or_seed`].
/// The cursor pump forwards each entry to the router as a `RawDiff`.
#[derive(Debug, Clone)]
pub struct AggregateDelta {
    /// The row that changed.
    pub row: Row,
    /// LSN of the originating WAL write.
    pub lsn: Lsn,
    /// +1 for an asserted row, -1 for a retracted one.
    pub diff: isize,
}

impl PersistentHost {
    /// Construct an empty host.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HostInner::default())),
        }
    }

    /// Register `plan` under `canonical` with the supplied initial
    /// input snapshot. Returns the current aggregate output rows
    /// the router emits as `DiffEvent::Initial`. Subsequent calls
    /// with the same `canonical` increment a refcount and return
    /// the existing materialized output.
    pub fn register_or_seed(
        &self,
        canonical: &str,
        plan: &CompiledPlan,
        inputs: HashMap<TableId, Vec<Row>>,
    ) -> Vec<Row> {
        let mut inner = self.inner.lock().expect("host inner");
        if let Some(state) = inner.plans.get_mut(canonical) {
            state.refcount += 1;
            return state.last_output.clone();
        }
        let output = snapshot_run(plan, inputs.clone());
        let state = PlanState {
            plan: plan.clone(),
            inputs,
            last_output: output.clone(),
            refcount: 1,
        };
        inner.plans.insert(canonical.to_owned(), state);
        output
    }

    /// Apply a single WAL diff to the cumulative input set for one
    /// plan + table, recompute the aggregate, and return the deltas
    /// against the previously-emitted output. Each delta is
    /// stamped with `lsn` so the cursor pump can re-emit it as a
    /// `RawDiff` at that LSN.
    pub fn push_table_diff(
        &self,
        canonical: &str,
        table_id: TableId,
        row: Row,
        diff: isize,
        lsn: Lsn,
    ) -> Vec<AggregateDelta> {
        let mut inner = self.inner.lock().expect("host inner");
        let Some(state) = inner.plans.get_mut(canonical) else {
            return Vec::new();
        };

        // Apply the diff to the cumulative input set.
        let bucket = state.inputs.entry(table_id).or_default();
        if diff > 0 {
            for _ in 0..diff {
                bucket.push(row.clone());
            }
        } else if diff < 0 {
            for _ in 0..(-diff) {
                if let Some(pos) = bucket.iter().position(|r| r == &row) {
                    bucket.swap_remove(pos);
                }
            }
        }

        let new_output = snapshot_run(&state.plan, state.inputs.clone());
        let deltas = diff_outputs(&state.last_output, &new_output, lsn);
        state.last_output = new_output;
        deltas
    }

    /// Apply a batch of diffs at one LSN. Cheaper than calling
    /// [`Self::push_table_diff`] N times: only one snapshot rerun.
    pub fn push_table_batch(
        &self,
        canonical: &str,
        diffs: Vec<(TableId, Row, isize)>,
        lsn: Lsn,
    ) -> Vec<AggregateDelta> {
        let mut inner = self.inner.lock().expect("host inner");
        let Some(state) = inner.plans.get_mut(canonical) else {
            return Vec::new();
        };

        for (table_id, row, diff) in diffs {
            let bucket = state.inputs.entry(table_id).or_default();
            if diff > 0 {
                for _ in 0..diff {
                    bucket.push(row.clone());
                }
            } else if diff < 0 {
                for _ in 0..(-diff) {
                    if let Some(pos) = bucket.iter().position(|r| r == &row) {
                        bucket.swap_remove(pos);
                    }
                }
            }
        }

        let new_output = snapshot_run(&state.plan, state.inputs.clone());
        let deltas = diff_outputs(&state.last_output, &new_output, lsn);
        state.last_output = new_output;
        deltas
    }

    /// Release one refcount on `canonical`; drop the plan state once
    /// the count reaches zero.
    pub fn release(&self, canonical: &str) {
        let mut inner = self.inner.lock().expect("host inner");
        if let Some(state) = inner.plans.get_mut(canonical) {
            if state.refcount > 0 {
                state.refcount -= 1;
            }
            if state.refcount == 0 {
                inner.plans.remove(canonical);
            }
        }
    }
}

impl Default for PersistentHost {
    fn default() -> Self {
        Self::new()
    }
}

/// Compare two materialized aggregate row sets and emit the
/// retract / assert diffs that transform the first into the second.
fn diff_outputs(prev: &[Row], next: &[Row], lsn: Lsn) -> Vec<AggregateDelta> {
    use std::collections::BTreeMap;

    let mut counts: BTreeMap<Row, isize> = BTreeMap::new();
    for row in prev {
        *counts.entry(row.clone()).or_insert(0) -= 1;
    }
    for row in next {
        *counts.entry(row.clone()).or_insert(0) += 1;
    }

    let mut deltas = Vec::new();
    for (row, change) in counts {
        if change > 0 {
            for _ in 0..change {
                deltas.push(AggregateDelta {
                    row: row.clone(),
                    lsn,
                    diff: 1,
                });
            }
        } else if change < 0 {
            for _ in 0..(-change) {
                deltas.push(AggregateDelta {
                    row: row.clone(),
                    lsn,
                    diff: -1,
                });
            }
        }
    }
    deltas
}

#[cfg(test)]
mod tests {
    use super::*;
    use palimpsest_sql::catalog::ColumnType;
    use palimpsest_sql::lower::parse_and_lower;
    use palimpsest_wal::Datum;

    use crate::palimpsest::compile_mir::compile_mir;
    use crate::palimpsest::eval::ScalarSchema;

    fn events_schema() -> ScalarSchema {
        ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("category_id".to_owned(), ColumnType::Int),
            ("value".to_owned(), ColumnType::Int),
        ])
    }

    fn lookup(table: &str) -> Option<(TableId, ScalarSchema)> {
        match table {
            "events" => Some((TableId::new(2), events_schema())),
            _ => None,
        }
    }

    fn row(values: Vec<Datum>) -> Row {
        values.into_iter().collect()
    }

    #[test]
    fn snapshot_run_emits_aggregate_rows() {
        let sql = "WITH per_category AS (
            SELECT category_id, COUNT(*) AS n, SUM(value) AS total
            FROM events
            GROUP BY category_id
        )
        SELECT category_id, n, total
        FROM per_category
        ORDER BY total DESC
        LIMIT 8";
        let graph = parse_and_lower(sql).unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();

        let mut inputs = HashMap::new();
        inputs.insert(
            TableId::new(2),
            vec![
                row(vec![Datum::I64(1), Datum::I64(7), Datum::I64(100)]),
                row(vec![Datum::I64(2), Datum::I64(7), Datum::I64(50)]),
                row(vec![Datum::I64(3), Datum::I64(9), Datum::I64(20)]),
                row(vec![Datum::I64(4), Datum::I64(9), Datum::I64(20)]),
                row(vec![Datum::I64(5), Datum::I64(11), Datum::I64(5)]),
            ],
        );

        let mut output = snapshot_run(&plan, inputs);
        output.sort();

        assert_eq!(output.len(), 3, "three categories");
    }

    #[test]
    fn persistent_host_emits_initial_and_diffs() {
        let sql = "WITH per_category AS (
            SELECT category_id, COUNT(*) AS n, SUM(value) AS total
            FROM events
            GROUP BY category_id
        )
        SELECT category_id, n, total
        FROM per_category
        ORDER BY total DESC
        LIMIT 8";
        let graph = parse_and_lower(sql).unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();

        let host = PersistentHost::new();
        let canonical = "events.top_categories";

        let mut seed = HashMap::new();
        seed.insert(
            TableId::new(2),
            vec![
                row(vec![Datum::I64(1), Datum::I64(7), Datum::I64(100)]),
                row(vec![Datum::I64(2), Datum::I64(7), Datum::I64(50)]),
                row(vec![Datum::I64(3), Datum::I64(9), Datum::I64(20)]),
            ],
        );
        let mut initial = host.register_or_seed(canonical, &plan, seed);
        initial.sort();
        assert_eq!(initial.len(), 2, "initial has cat 7 + cat 9");

        // Push a new event into cat 9. Expect: retract of old cat 9
        // aggregate row, assert of new cat 9 aggregate row.
        let next_lsn = Lsn::new(2);
        let deltas = host.push_table_diff(
            canonical,
            TableId::new(2),
            row(vec![Datum::I64(4), Datum::I64(9), Datum::I64(100)]),
            1,
            next_lsn,
        );

        let retracts: Vec<_> = deltas.iter().filter(|d| d.diff < 0).collect();
        let asserts: Vec<_> = deltas.iter().filter(|d| d.diff > 0).collect();
        assert_eq!(retracts.len(), 1, "one retract — old cat 9 row");
        assert_eq!(asserts.len(), 1, "one assert — new cat 9 row");

        // Old cat 9 row: (9, 1, 20). New: (9, 2, 120).
        let retracted = &retracts[0].row;
        assert_eq!(retracted.get(0), Some(&Datum::I64(9)));
        assert_eq!(retracted.get(1), Some(&Datum::I64(1)));
        assert_eq!(retracted.get(2), Some(&Datum::I64(20)));
        let asserted = &asserts[0].row;
        assert_eq!(asserted.get(0), Some(&Datum::I64(9)));
        assert_eq!(asserted.get(1), Some(&Datum::I64(2)));
        assert_eq!(asserted.get(2), Some(&Datum::I64(120)));
    }

    #[test]
    fn persistent_host_batch_coalesces() {
        let sql = "WITH per_category AS (
            SELECT category_id, COUNT(*) AS n, SUM(value) AS total
            FROM events
            GROUP BY category_id
        )
        SELECT category_id, n, total
        FROM per_category
        ORDER BY total DESC
        LIMIT 8";
        let plan = compile_mir(&parse_and_lower(sql).unwrap(), &lookup).unwrap();
        let host = PersistentHost::new();
        let canonical = "events.batch";

        let mut seed = HashMap::new();
        seed.insert(
            TableId::new(2),
            vec![row(vec![Datum::I64(1), Datum::I64(7), Datum::I64(10)])],
        );
        host.register_or_seed(canonical, &plan, seed);

        let batch = vec![
            (
                TableId::new(2),
                row(vec![Datum::I64(2), Datum::I64(7), Datum::I64(20)]),
                1,
            ),
            (
                TableId::new(2),
                row(vec![Datum::I64(3), Datum::I64(7), Datum::I64(30)]),
                1,
            ),
        ];
        let deltas = host.push_table_batch(canonical, batch, Lsn::new(2));
        // Two diffs at one LSN: retract (7, 1, 10) + assert (7, 3, 60).
        assert_eq!(deltas.len(), 2);
        assert!(deltas.iter().all(|d| d.lsn == Lsn::new(2)));
    }
}
