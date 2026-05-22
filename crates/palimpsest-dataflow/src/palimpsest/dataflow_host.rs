// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Embed-shim that hosts dataflows for the router.
//!
//! Two public entry points:
//!
//! * [`snapshot_run`] — stateless: build a fresh dataflow, feed it the
//!   supplied snapshot, capture output rows, tear it down. The router
//!   uses this on the snapshot path (see `palimpsest-server`).
//!
//! * [`PersistentHost`] — long-lived per-server-process state. Each
//!   registered plan owns a dedicated timely worker thread that holds
//!   the dataflow's [`InputSession`]s and a [`ProbeHandle`]. On every
//!   cursor batch we push diffs into the inputs at the batch's commit
//!   LSN, advance the frontier, step the worker until the probe passes,
//!   then capture and return the output deltas emitted at that LSN.
//!   Per-batch cost is O(diff), not O(state) — a 1000-row insert into
//!   a 600k-row aggregate computes only the affected groups.

use std::collections::HashMap;
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use palimpsest_wal::TableId;
use timely::communication::allocator::thread::Thread;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::Inspect;
use timely::dataflow::ProbeHandle;
use timely::worker::Worker as TimelyWorker;
use timely::WorkerConfig;

use crate::input::{Input, InputSession};
use crate::palimpsest::compile_mir::{install_plan, CompiledPlan};
use crate::palimpsest::time::Lsn;
use crate::palimpsest::wal::{Row, WalTransaction};

// -----------------------------------------------------------------------------
// Stateless snapshot run (used on the FreshInitial path)
// -----------------------------------------------------------------------------

/// Compile and run `plan` against `inputs`, returning the output rows
/// the dataflow produces at the final frontier.
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
                    cap_inner.lock().expect("capture mutex").push(row.clone());
                }
            });
        });
    });

    let mut rows = captured.lock().expect("capture mutex");
    std::mem::take(&mut *rows)
}

// -----------------------------------------------------------------------------
// Persistent host (per-plan incremental dataflow)
// -----------------------------------------------------------------------------

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

/// Commands sent from `PersistentHost` callers into a plan's worker
/// thread. The worker replies through the embedded sync_channel.
enum DataflowCommand {
    /// Seed the inputs at LSN 0 with the supplied rows, advance to
    /// LSN 1, step to quiescence, return the initial aggregate output.
    Seed {
        inputs: HashMap<TableId, Vec<Row>>,
        reply: std_mpsc::SyncSender<Vec<Row>>,
    },
    /// Apply a batch of row diffs at `lsn`, advance to `lsn + 1`, step
    /// until the probe passes, return the output deltas emitted at
    /// `lsn`.
    Apply {
        diffs: Vec<(TableId, Row, isize)>,
        lsn: Lsn,
        reply: std_mpsc::SyncSender<Vec<AggregateDelta>>,
    },
    /// Stop the worker thread.
    Stop,
}

/// Per-plan worker thread + command channel. Dropping the handle sends
/// `Stop` and joins.
struct IncrementalDataflow {
    cmd_tx: std_mpsc::Sender<DataflowCommand>,
    join: Option<JoinHandle<()>>,
}

impl IncrementalDataflow {
    fn spawn(plan: CompiledPlan) -> Self {
        let (cmd_tx, cmd_rx) = std_mpsc::channel::<DataflowCommand>();
        let join = std::thread::Builder::new()
            .name("palimpsest-dataflow".into())
            .spawn(move || run_worker(plan, cmd_rx))
            .expect("spawn dataflow worker thread");
        Self {
            cmd_tx,
            join: Some(join),
        }
    }

    fn seed(&self, inputs: HashMap<TableId, Vec<Row>>) -> Vec<Row> {
        let (tx, rx) = std_mpsc::sync_channel(0);
        if self
            .cmd_tx
            .send(DataflowCommand::Seed { inputs, reply: tx })
            .is_err()
        {
            return Vec::new();
        }
        rx.recv().unwrap_or_default()
    }

    fn apply(&self, diffs: Vec<(TableId, Row, isize)>, lsn: Lsn) -> Vec<AggregateDelta> {
        let (tx, rx) = std_mpsc::sync_channel(0);
        if self
            .cmd_tx
            .send(DataflowCommand::Apply {
                diffs,
                lsn,
                reply: tx,
            })
            .is_err()
        {
            return Vec::new();
        }
        rx.recv().unwrap_or_default()
    }
}

impl Drop for IncrementalDataflow {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(DataflowCommand::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Run the per-plan worker thread. Builds the dataflow once, then
/// services `Seed` / `Apply` / `Stop` commands until the channel
/// closes or `Stop` arrives.
fn run_worker(plan: CompiledPlan, cmd_rx: std_mpsc::Receiver<DataflowCommand>) {
    let mut worker = TimelyWorker::new(WorkerConfig::default(), Thread::default(), None);

    // Outputs land here from the inspect operator attached to the
    // dataflow's terminal collection. The worker thread drains this
    // after every step.
    let captured: Arc<Mutex<Vec<(Row, Lsn, isize)>>> = Arc::new(Mutex::new(Vec::new()));
    let cap_for_dataflow = Arc::clone(&captured);

    let mut inputs: HashMap<TableId, InputSession<Lsn, Row, isize>> = HashMap::new();
    let mut probe: ProbeHandle<Lsn> = ProbeHandle::new();

    // Build once. The InputSessions live in `inputs`, owned by this
    // thread; the inspect closure captures `cap_for_dataflow` so every
    // output `(Row, time, diff)` tuple lands in `captured`.
    worker.dataflow::<Lsn, _, _>(|scope| {
        let mut input_collections = HashMap::new();
        for table in &plan.inputs {
            let mut input = InputSession::<Lsn, Row, isize>::new();
            let collection = input.to_collection(scope);
            input_collections.insert(*table, collection);
            inputs.insert(*table, input);
        }
        let output = install_plan(&plan, scope, &input_collections);
        let cap_for_inspect = Arc::clone(&cap_for_dataflow);
        output
            .inner
            .probe_with(&mut probe)
            .inspect(move |entry: &(Row, Lsn, isize)| {
                cap_for_inspect.lock().expect("capture").push(entry.clone());
            });
    });

    while let Ok(cmd) = cmd_rx.recv() {
        match cmd {
            DataflowCommand::Seed {
                inputs: seed_rows,
                reply,
            } => {
                // Insert all seed rows at LSN 0 and step the dataflow
                // to the LSN-1 frontier so timely emits the initial
                // aggregate. After this point, callers must supply
                // commit LSNs ≥ 1.
                for (table, rows) in seed_rows {
                    if let Some(session) = inputs.get_mut(&table) {
                        for row in rows {
                            session.update_at(row, Lsn::new(0), 1);
                        }
                    }
                }
                advance_and_step(&mut worker, &mut inputs, &probe, Lsn::new(1));
                let drained = drain_captures(&captured);
                let initial: Vec<Row> = drained
                    .into_iter()
                    .filter(|(_, _, diff)| *diff > 0)
                    .map(|(row, _, _)| row)
                    .collect();
                let _ = reply.send(initial);
            }
            DataflowCommand::Apply { diffs, lsn, reply } => {
                for (table, row, diff) in diffs {
                    if let Some(session) = inputs.get_mut(&table) {
                        session.update_at(row, lsn, diff);
                    }
                }
                let next = Lsn::new(lsn.get().saturating_add(1));
                advance_and_step(&mut worker, &mut inputs, &probe, next);
                let drained = drain_captures(&captured);
                let deltas: Vec<AggregateDelta> = drained
                    .into_iter()
                    .map(|(row, t, diff)| AggregateDelta { row, lsn: t, diff })
                    .collect();
                let _ = reply.send(deltas);
            }
            DataflowCommand::Stop => break,
        }
    }
}

/// Flush every InputSession to `target`, then step the worker until
/// the output probe has passed `target`. After this returns, all
/// output for times `< target` has been captured.
fn advance_and_step(
    worker: &mut TimelyWorker<Thread>,
    inputs: &mut HashMap<TableId, InputSession<Lsn, Row, isize>>,
    probe: &ProbeHandle<Lsn>,
    target: Lsn,
) {
    for session in inputs.values_mut() {
        session.advance_to(target);
        session.flush();
    }
    while probe.less_than(&target) {
        worker.step();
    }
}

fn drain_captures(cap: &Arc<Mutex<Vec<(Row, Lsn, isize)>>>) -> Vec<(Row, Lsn, isize)> {
    let mut guard = cap.lock().expect("capture");
    std::mem::take(&mut *guard)
}

/// State kept per registered plan.
struct PlanState {
    dataflow: IncrementalDataflow,
    /// Cached materialized aggregate output. Updated incrementally
    /// from each apply's deltas. Used by `cached_view` so a second
    /// subscriber sharing the same canonical key can skip the snapshot
    /// pipeline entirely and start from the current materialized view.
    last_output: Vec<Row>,
    /// Logical clock of the most recent state captured in `last_output`.
    /// Set to the snapshot LSN at register time; advanced on every
    /// `apply_and_fanout`. New subscribers report this as their
    /// `Accepted.snapshot_lsn` so the cursor stream picks up
    /// immediately after.
    last_lsn: Lsn,
    /// Active subscribers attached to this plan. Replaces the old
    /// `refcount` — we track ids (not just a count) so the cursor pump
    /// can fan deltas out to each subscriber's channel.
    subscribers: Vec<u64>,
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

impl PersistentHost {
    /// Construct an empty host.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HostInner::default())),
        }
    }

    /// Try to attach `subscriber` to an already-registered plan under
    /// `canonical`. On hit, returns the current cached materialized
    /// view + the LSN that view reflects, and the subscriber is added
    /// to the plan's fan-out list under the same lock. On miss, returns
    /// `None` so the caller can do the full snapshot pull and call
    /// [`Self::register_or_seed`].
    ///
    /// The atomic add-and-snapshot is the key to coherent live diffs
    /// for a late joiner: even if the cursor pump's next `apply_and_fanout`
    /// races this call, exactly one of two things happens:
    ///   * pump wins lock → applies, advances `last_lsn`, fans out
    ///     deltas to current subscribers; our `cached_view` returns
    ///     the *post-apply* view and adds us to the list, so we get
    ///     the next delta.
    ///   * cached_view wins lock → returns the *pre-apply* view and
    ///     registers us; pump applies, advances `last_lsn`, fans out
    ///     including us.
    /// Either way the new subscriber sees a consistent (Initial @ L,
    /// then live diffs > L) timeline.
    pub fn cached_view(&self, canonical: &str, subscriber: u64) -> Option<(Vec<Row>, Lsn)> {
        let mut inner = self.inner.lock().expect("host inner");
        let state = inner.plans.get_mut(canonical)?;
        state.subscribers.push(subscriber);
        Some((state.last_output.clone(), state.last_lsn))
    }

    /// Register `plan` under `canonical` with the supplied initial
    /// input snapshot and attach `subscriber`. Spawns a per-plan worker
    /// thread on first registration. Returns the materialized initial
    /// rows the caller should ship as `Initial`.
    ///
    /// Callers should usually [`Self::cached_view`] first; if that
    /// returns `None`, run the snapshot pipeline and call this fn.
    /// If two threads race the snapshot pipeline for the same
    /// `canonical`, the loser's seed work is discarded and the winning
    /// cached view is returned.
    pub fn register_or_seed(
        &self,
        canonical: &str,
        plan: &CompiledPlan,
        inputs: HashMap<TableId, Vec<Row>>,
        snapshot_lsn: Lsn,
        subscriber: u64,
    ) -> Vec<Row> {
        // Fast path: an entry already exists. Attach the subscriber to
        // the shared plan.
        {
            let mut inner = self.inner.lock().expect("host inner");
            if let Some(state) = inner.plans.get_mut(canonical) {
                state.subscribers.push(subscriber);
                return state.last_output.clone();
            }
        }

        // Spawn the worker outside the lock so other plans aren't
        // serialized behind this one's initial-snapshot computation.
        let dataflow = IncrementalDataflow::spawn(plan.clone());
        let initial = dataflow.seed(inputs);

        let mut inner = self.inner.lock().expect("host inner");
        // Race: another thread may have registered the same canonical
        // while we were seeding. If so, drop our work and fall back to
        // the existing entry.
        if let Some(state) = inner.plans.get_mut(canonical) {
            state.subscribers.push(subscriber);
            return state.last_output.clone();
        }
        inner.plans.insert(
            canonical.to_owned(),
            PlanState {
                dataflow,
                last_output: initial.clone(),
                last_lsn: snapshot_lsn,
                subscribers: vec![subscriber],
            },
        );
        initial
    }

    /// Returns the current subscribers attached to `canonical`, or
    /// `None` if no plan is registered. Used by the cursor pump to
    /// learn its fan-out targets without applying anything.
    #[must_use]
    pub fn subscribers(&self, canonical: &str) -> Option<Vec<u64>> {
        let inner = self.inner.lock().expect("host inner");
        inner.plans.get(canonical).map(|s| s.subscribers.clone())
    }

    /// Apply a single WAL diff. Convenience wrapper around
    /// [`Self::apply_and_fanout`].
    pub fn push_table_diff(
        &self,
        canonical: &str,
        table_id: TableId,
        row: Row,
        diff: isize,
        lsn: Lsn,
    ) -> Vec<AggregateDelta> {
        self.apply_and_fanout(canonical, vec![(table_id, row, diff)], lsn)
            .map_or_else(Vec::new, |(deltas, _subs)| deltas)
    }

    /// Apply a batch of diffs at one LSN. Returns the resulting
    /// aggregate deltas *and* the current subscribers list — bundled
    /// so the cursor pump can fan out atomically without re-locking
    /// the host between apply and lookup.
    ///
    /// Returns `None` if no plan is registered under `canonical` (the
    /// last subscriber released and the plan was torn down). Callers
    /// should treat that as their cue to exit.
    pub fn apply_and_fanout(
        &self,
        canonical: &str,
        diffs: Vec<(TableId, Row, isize)>,
        lsn: Lsn,
    ) -> Option<(Vec<AggregateDelta>, Vec<u64>)> {
        let mut inner = self.inner.lock().expect("host inner");
        let state = inner.plans.get_mut(canonical)?;
        let deltas = state.dataflow.apply(diffs, lsn);
        apply_deltas_to_cache(&mut state.last_output, &deltas);
        state.last_lsn = lsn;
        Some((deltas, state.subscribers.clone()))
    }

    /// Legacy single-pump variant — applies and returns deltas only.
    /// Used by tests and the old per-subscription pump path.
    pub fn push_table_batch(
        &self,
        canonical: &str,
        diffs: Vec<(TableId, Row, isize)>,
        lsn: Lsn,
    ) -> Vec<AggregateDelta> {
        self.apply_and_fanout(canonical, diffs, lsn)
            .map_or_else(Vec::new, |(deltas, _subs)| deltas)
    }

    /// Apply every row update from one committed WAL transaction.
    pub fn push_transaction(
        &self,
        canonical: &str,
        transaction: &WalTransaction,
    ) -> Vec<AggregateDelta> {
        let diffs = transaction
            .updates
            .iter()
            .map(|update| (update.table, update.row.clone(), update.diff))
            .collect();
        self.push_table_batch(canonical, diffs, transaction.commit_lsn)
    }

    /// Detach `subscriber` from `canonical`. Returns the count of
    /// subscribers that remain; once zero, the plan is torn down (the
    /// `IncrementalDataflow` is dropped, which sends `Stop` and joins
    /// the worker thread).
    pub fn release(&self, canonical: &str, subscriber: u64) -> usize {
        let mut inner = self.inner.lock().expect("host inner");
        let Some(state) = inner.plans.get_mut(canonical) else {
            return 0;
        };
        if let Some(pos) = state.subscribers.iter().position(|s| *s == subscriber) {
            state.subscribers.swap_remove(pos);
        }
        let remaining = state.subscribers.len();
        if remaining == 0 {
            inner.plans.remove(canonical);
        }
        remaining
    }
}

impl Default for PersistentHost {
    fn default() -> Self {
        Self::new()
    }
}

/// Fold a batch of deltas into the cached `last_output` so a second
/// subscriber to the same canonical key gets the current materialized
/// view, not the stale seed snapshot.
fn apply_deltas_to_cache(rows: &mut Vec<Row>, deltas: &[AggregateDelta]) {
    for delta in deltas {
        if delta.diff > 0 {
            for _ in 0..delta.diff {
                rows.push(delta.row.clone());
            }
        } else if delta.diff < 0 {
            for _ in 0..-delta.diff {
                if let Some(pos) = rows.iter().position(|r| r == &delta.row) {
                    rows.swap_remove(pos);
                }
            }
        }
    }
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
        let mut initial = host.register_or_seed(canonical, &plan, seed, Lsn::new(1), 42);
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

        host.release(canonical, 42);
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
        host.register_or_seed(canonical, &plan, seed, Lsn::new(1), 7);

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

        host.release(canonical, 7);
    }

    #[test]
    fn cached_view_attaches_late_subscriber_to_current_state() {
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
        let canonical = "events.shared";

        let mut seed = HashMap::new();
        seed.insert(
            TableId::new(2),
            vec![row(vec![Datum::I64(1), Datum::I64(7), Datum::I64(10)])],
        );
        // First subscriber seeds the plan.
        host.register_or_seed(canonical, &plan, seed, Lsn::new(5), 1);

        // Live diff lands.
        let apply_lsn = Lsn::new(6);
        let (_deltas, subs_after_apply) = host
            .apply_and_fanout(
                canonical,
                vec![(
                    TableId::new(2),
                    row(vec![Datum::I64(2), Datum::I64(7), Datum::I64(20)]),
                    1,
                )],
                apply_lsn,
            )
            .expect("plan still registered");
        assert_eq!(subs_after_apply, vec![1]);

        // Second subscriber attaches via cached_view — should see the
        // post-apply view at the apply LSN, and be on the subscribers
        // list for future fan-outs.
        let (cached, cached_lsn) = host
            .cached_view(canonical, 2)
            .expect("cache hit on registered plan");
        assert_eq!(cached_lsn, apply_lsn);
        // post-apply cat 7 row carries n=2, total=30
        assert!(cached.iter().any(|r| r.get(2) == Some(&Datum::I64(30))));

        let subs_view = host.subscribers(canonical).expect("plan registered");
        assert_eq!(subs_view, vec![1, 2]);

        // Releasing one leaves the plan registered for the other.
        assert_eq!(host.release(canonical, 1), 1);
        assert!(host.subscribers(canonical).is_some());
        // Releasing the last drops the plan.
        assert_eq!(host.release(canonical, 2), 0);
        assert!(host.subscribers(canonical).is_none());
    }
}
