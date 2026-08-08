// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Subscription router orchestration.
//!
//! Ties [`SubscriptionRegistry`], [`SnapshotProvider`], [`AckTracker`],
//! [`SharedSubgraphRegistry`] and per-subscription
//! [`BoundedDiffChannel`]s into the public API the §18.8 gRPC layer
//! adapts onto a `tonic::Stream`.
//!
//! The router is transport- and runtime-agnostic: timely lives in the
//! caller's worker task, tokio drives the gRPC stream, and the bridge
//! is a [`TraceCursor`] supplied via [`SubscriptionRouter::pump_cursor`].

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

use palimpsest_dataflow::palimpsest::{
    Lsn, Row, SharedSubgraphAcquire, SharedSubgraphId, SharedSubgraphRegistry,
    SharedSubgraphRelease, WalUpdate,
};
use palimpsest_permissions::{CompiledRule, RewriteStats, UserContext};
use palimpsest_sql::mir::{MirGraph, MirNodeKind};
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use crate::{
    ack::{AckOutcome, AckTracker},
    backpressure::{BackpressureOutcome, BackpressurePolicy, BoundedDiffChannel},
    cursor::{LsnBatch, QueryTransactionDelta, RawDiff, TraceCursor},
    diff::{DiffEvent, DiffOp, ResyncReason, RowChange},
    error::RouterError,
    metrics::RouterMetrics,
    permissions::install_permission_filters,
    registry::SubscriptionRegistry,
    resume::{resolve as resolve_resume, CompactionWindow, ResumeDecision},
    snapshot::{snapshot_seed, SnapshotProvider},
    subscription::{
        ClientSubscriptionId, ConnectionId, QueryId, SchemaDefinition, SchemaId, Subscription,
        SubscriptionId, SubscriptionIdAllocator, SubscriptionState,
    },
};

/// Tunables for the subscription router.
#[derive(Debug, Clone, Copy)]
pub struct RouterConfig {
    /// Per-subscription channel depth (§18.7 #5; default 256).
    pub channel_capacity: usize,
    /// Default backpressure policy.
    pub backpressure: BackpressurePolicy,
    /// Compaction-window upper bound used for resume decisions in
    /// tests; the embed shim usually overrides this with the trace's
    /// frontier.
    pub default_latest_known_lsn: Lsn,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            channel_capacity: BoundedDiffChannel::DEFAULT_CAPACITY,
            backpressure: BackpressurePolicy::DropAndResync,
            default_latest_known_lsn: Lsn::new(u64::MAX / 2),
        }
    }
}

/// Inputs for [`SubscriptionRouter::subscribe`].
pub struct SubscribeRequest<'a> {
    /// Connection id (server-assigned per gRPC stream).
    pub connection: ConnectionId,
    /// Pre-allocated subscription id (see
    /// [`SubscriptionRouter::allocate_subscription_id`]). The caller
    /// allocates this before `subscribe` so the same id can be used as
    /// the `PersistentHost` subscriber tag for shared-plan attachment.
    pub subscription_id: SubscriptionId,
    /// Client-supplied subscription label.
    pub client_id: ClientSubscriptionId,
    /// Canonical query name (matches the build-plan registry key).
    pub query: QueryId,
    /// Lowered MIR for the query; the router applies permission
    /// rewriting in place.
    pub query_graph: &'a MirGraph,
    /// Connection's user context — feeds permission predicates.
    pub user_ctx: UserContext,
    /// Schema descriptor surfaced to the client in `Accepted`.
    pub schema: SchemaDefinition,
    /// Optional resume LSN supplied by the client.
    pub resume_lsn: Option<Lsn>,
    /// Compiled dataflow plan for the (permission-rewritten) query
    /// graph. When present, the router runs the snapshot through the
    /// dataflow and emits the query result rather than the raw table
    /// data. Absent for the few query shapes the compiler still can't
    /// lower (`DISTINCT ON`, `EXCEPT` / `INTERSECT`, `WITH RECURSIVE`)
    /// — those still ship via the v1 pass-through path **unless
    /// row-visibility rules apply to the query's tables**, in which
    /// case `subscribe` fails closed with
    /// [`RouterError::PermissionUnenforceable`] (pass-through cannot
    /// enforce the filters).
    ///
    /// Caller contract: the plan must be compiled from the
    /// permission-rewritten graph (as the gRPC adapter does), so the
    /// spliced visibility filters are actually part of the dataflow.
    pub compiled_plan: Option<palimpsest_dataflow::palimpsest::CompiledPlan>,
    /// Pre-computed `Initial` payload. When `Some`, the router uses
    /// these rows + LSN for the snapshot event and **skips** the
    /// `snapshot_seed`/`snapshot_run` pipeline entirely. Used by the
    /// gRPC adapter when a `PersistentHost` `cached_view` hits: the
    /// materialized view is already current and can be shipped
    /// verbatim without re-pulling the underlying table snapshot or
    /// re-running the dataflow. `seed_updates` in the response is
    /// empty on this path — the host plan is already seeded.
    pub prerun_initial: Option<(Vec<Row>, Lsn)>,
}

/// Optional resume request supplied alongside a `subscribe` call.
#[derive(Debug, Clone, Copy)]
pub struct ResumeRequest {
    /// LSN the client claims to have applied.
    pub last_acked_lsn: Lsn,
}

/// Output of [`SubscriptionRouter::subscribe`].
pub struct SubscribeResponse {
    /// Server-assigned id; sent to the client in `Accepted`.
    pub subscription_id: SubscriptionId,
    /// Schema id (from the supplied [`SchemaDefinition`]).
    pub schema_id: SchemaId,
    /// LSN at which the snapshot was taken.
    pub snapshot_lsn: Lsn,
    /// Resume decision the router took for this `subscribe`.
    pub resume: ResumeDecision,
    /// Permission rewrite statistics.
    pub permission_stats: RewriteStats,
    /// Shared-subgraph acquire outcome (so the embed shim can build /
    /// reuse the dataflow).
    pub subgraph: SharedSubgraphAcquire,
    /// Seed updates the caller must feed into the dataflow input
    /// before switching the cursor to streaming.
    pub seed_updates: Vec<WalUpdate>,
    /// Receiver-side stream the gRPC adapter forwards onto its
    /// `ServerMessage` flow.
    pub stream: ReceiverStream<DiffEvent>,
}

/// Subscription router.
pub struct SubscriptionRouter {
    config: RouterConfig,
    inner: Mutex<RouterInner>,
    metrics: RouterMetrics,
    allocator: SubscriptionIdAllocator,
}

struct RouterInner {
    registry: SubscriptionRegistry,
    ack: AckTracker,
    subgraphs: SharedSubgraphRegistry,
    channels: BTreeMap<SubscriptionId, BoundedDiffChannel>,
    subgraph_holders: BTreeMap<SubscriptionId, SharedSubgraphId>,
    schemas: BTreeMap<SchemaId, SchemaDefinition>,
    rules: Vec<CompiledRule>,
}

#[allow(clippy::significant_drop_tightening)]
impl SubscriptionRouter {
    /// Creates a router with the provided configuration and the empty
    /// rule set. Permission rules can be installed later via
    /// [`Self::set_rules`].
    #[must_use]
    pub fn new(config: RouterConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(RouterInner {
                registry: SubscriptionRegistry::new(),
                ack: AckTracker::new(),
                subgraphs: SharedSubgraphRegistry::new(),
                channels: BTreeMap::new(),
                subgraph_holders: BTreeMap::new(),
                schemas: BTreeMap::new(),
                rules: Vec::new(),
            }),
            metrics: RouterMetrics::new(),
            allocator: SubscriptionIdAllocator::new(1),
        }
    }

    /// Returns the active metric registry.
    #[must_use]
    pub const fn metrics(&self) -> &RouterMetrics {
        &self.metrics
    }

    /// Replaces the compiled rule set the router uses for permission
    /// rewriting, and forces `Resync(PermissionsChanged)` onto every
    /// active subscription so revoked grants take effect on running
    /// streams, not just future subscribes.
    ///
    /// The revocation bound this gives is: rules are swapped and every
    /// open channel has a `Resync` enqueued before this method returns;
    /// a well-behaved client resubscribes on `Resync`, and the fresh
    /// subscribe runs under the new rules (rows a revoked grant covered
    /// are absent from the new `Initial`). The server-side half of that
    /// lag is published as
    /// `palimpsest_permission_revocation_lag_p{50,99}_microseconds`,
    /// alongside `palimpsest_permission_rule_updates_total` and the
    /// `permissions_changed` label of
    /// `palimpsest_resyncs_by_reason_total`.
    pub fn set_rules(&self, rules: Vec<CompiledRule>) {
        let started = Instant::now();
        let resynced = {
            let mut inner = self.inner.lock().expect("router lock");
            inner.rules = rules;
            for channel in inner.channels.values() {
                channel.force_resync(ResyncReason::PermissionsChanged);
            }
            inner.channels.len()
        };
        self.metrics
            .record_permission_rules_update(started.elapsed(), resynced);
    }

    /// Returns a snapshot of the router's compiled permission rules.
    /// The gRPC adapter uses this to apply rewriting *before*
    /// `compile_mir` so the resulting dataflow plan already includes
    /// the row-visibility filters.
    #[must_use]
    pub fn permission_rules(&self) -> Vec<CompiledRule> {
        self.inner.lock().expect("router lock").rules.clone()
    }

    /// Allocate a fresh subscription id without registering anything.
    ///
    /// The gRPC adapter needs the id *before* calling [`Self::subscribe`]
    /// so it can use the same id as the `PersistentHost` subscriber tag
    /// when attaching to a shared canonical plan via
    /// `PersistentHost::cached_view`. [`Self::subscribe`] then takes the
    /// pre-allocated id via [`SubscribeRequest::subscription_id`].
    #[must_use]
    pub fn allocate_subscription_id(&self) -> SubscriptionId {
        self.allocator.allocate()
    }

    /// Total active subscriptions.
    #[must_use]
    pub fn active_subscriptions(&self) -> usize {
        self.inner.lock().expect("router lock").registry.len()
    }

    /// Accepts a subscribe request: rewrites the MIR, allocates IDs,
    /// fetches the snapshot, opens the channel, and emits an initial
    /// `Initial` (or `Resync` if the resume LSN fell out of window).
    ///
    /// # Errors
    /// Surfaces permission failures and snapshot-provider errors.
    pub fn subscribe<P: SnapshotProvider + ?Sized>(
        &self,
        request: SubscribeRequest<'_>,
        provider: &P,
    ) -> Result<SubscribeResponse, RouterError> {
        let SubscribeRequest {
            connection,
            subscription_id: id,
            client_id,
            query,
            query_graph,
            user_ctx,
            schema,
            resume_lsn,
            compiled_plan,
            prerun_initial,
        } = request;

        let rules = self.inner.lock().expect("router lock").rules.clone();
        let outcome = install_permission_filters(query_graph, &rules, &user_ctx)?;

        // Fail closed: when the rewrite spliced at least one
        // row-visibility filter, the only execution path that enforces
        // it is the compiled dataflow. Without a compiled plan the
        // subscription would serve raw pass-through rows — unfiltered —
        // so reject the subscribe instead. (Tautology-only and
        // subscribe-mode rules splice no filter and stay eligible for
        // pass-through.)
        if outcome.stats.filters_inserted > 0 && compiled_plan.is_none() {
            let table = first_rule_guarded_table(query_graph, &rules)
                .unwrap_or_else(|| "<unknown>".to_owned());
            return Err(RouterError::PermissionUnenforceable { table });
        }

        let canonical = canonical_subgraph_key(&query, &user_ctx);

        // Cached fast-path: caller (gRPC adapter) already has the
        // materialized view from a `PersistentHost::cached_view` hit.
        // Skip the snapshot pull + dataflow snapshot_run entirely.
        let (snapshot_lsn, initial_rows, seed_updates) = if let Some((rows, lsn)) = prerun_initial {
            (lsn, rows, Vec::new())
        } else {
            let (snapshot_batch, seed_updates) = snapshot_seed(provider, &query)?;
            let snapshot_lsn = snapshot_batch.snapshot_lsn;
            let rows: Vec<Row> = if let Some(plan) = &compiled_plan {
                let inputs: std::collections::HashMap<palimpsest_wal::TableId, Vec<Row>> =
                    snapshot_batch
                        .rows
                        .into_iter()
                        .map(|table| (table.table, table.rows))
                        .collect();
                palimpsest_dataflow::palimpsest::snapshot_run(plan, inputs)
            } else {
                snapshot_batch
                    .rows
                    .into_iter()
                    .flat_map(|table| table.rows)
                    .collect()
            };
            (snapshot_lsn, rows, seed_updates)
        };

        let resume = resolve_resume(
            resume_lsn,
            CompactionWindow::new(snapshot_lsn, self.config.default_latest_known_lsn),
        );

        let mut channel =
            BoundedDiffChannel::new(self.config.channel_capacity, self.config.backpressure);
        let stream = channel
            .take_stream()
            .expect("freshly built channel must hold a receiver");

        match resume {
            ResumeDecision::FreshInitial { .. } => {
                let initial = DiffEvent::Initial {
                    lsn: snapshot_lsn,
                    rows: initial_rows,
                };
                let outcome = channel.try_send(initial);
                match outcome {
                    BackpressureOutcome::Sent => {}
                    BackpressureOutcome::SaturatedDropResync
                    | BackpressureOutcome::SaturatedCoalesce
                    | BackpressureOutcome::Closed => return Err(RouterError::ChannelSaturated),
                }
            }
            ResumeDecision::Replay { .. } => {
                // No initial event; the cursor task replays from
                // `from_lsn` forward.
            }
        }

        let subscription = Subscription {
            id,
            client_id,
            connection,
            query,
            user_ctx,
            schema_id: schema.id,
            snapshot_lsn,
            cursor_lsn: snapshot_lsn,
            state: SubscriptionState::Seeding,
        };

        let schema_id = schema.id;
        let acquire = {
            let mut inner = self.inner.lock().expect("router lock");
            inner.registry.insert(subscription)?;
            let acquire = inner.subgraphs.acquire(canonical);
            inner.subgraph_holders.insert(id, acquire.id());
            inner.ack.install(id, snapshot_lsn);
            inner.channels.insert(id, channel);
            inner.schemas.insert(schema_id, schema);
            acquire
        };

        self.metrics.subscribe();

        Ok(SubscribeResponse {
            subscription_id: id,
            schema_id,
            snapshot_lsn,
            resume,
            permission_stats: outcome.stats,
            subgraph: acquire,
            seed_updates,
            stream,
        })
    }

    /// Pushes a single LSN batch into the subscription's channel after
    /// pairing inserts and deletes into row-level changes.
    ///
    /// Returns whether the push succeeded — saturation is reported via
    /// [`RouterError::ChannelSaturated`] so the caller can issue
    /// `Resync` and tear the subscription down.
    pub fn pump_batch(
        &self,
        sub: SubscriptionId,
        batch: LsnBatch,
        primary_key: &[usize],
    ) -> Result<(), RouterError> {
        self.pump_transaction(sub, QueryTransactionDelta::from(batch), primary_key)
    }

    /// Pushes a complete transaction delta into the subscription's
    /// channel after pairing inserts and deletes into row-level changes.
    pub fn pump_transaction(
        &self,
        sub: SubscriptionId,
        delta: QueryTransactionDelta,
        primary_key: &[usize],
    ) -> Result<(), RouterError> {
        let started = Instant::now();
        let event = pair_transaction_into_event(delta, primary_key);
        let mut inner = self.inner.lock().expect("router lock");

        let (outcome, channel_capacity, channel_full_events) = {
            let Some(channel) = inner.channels.get(&sub) else {
                return Err(RouterError::UnknownSubscription(sub));
            };
            let outcome = channel.try_send(event);
            if matches!(
                outcome,
                BackpressureOutcome::SaturatedDropResync | BackpressureOutcome::SaturatedCoalesce
            ) {
                channel.force_resync(ResyncReason::Backpressure);
            }
            (outcome, channel.capacity(), channel.full_events())
        };

        match outcome {
            BackpressureOutcome::Sent => {
                self.metrics.record_diff(started.elapsed());
                if let Some(record) = inner.registry.get_mut(sub) {
                    record.state = SubscriptionState::Streaming;
                }
                Ok(())
            }
            BackpressureOutcome::SaturatedDropResync | BackpressureOutcome::SaturatedCoalesce => {
                self.metrics.record_channel_full();
                self.metrics
                    .record_resync_with_reason(ResyncReason::Backpressure);
                // Surface saturation in logs so operators can correlate
                // "client stopped updating" reports with the actual
                // backpressure event. `full_events` is a running count
                // of saturation hits since process start — a rapidly
                // climbing value means the channel is small relative to
                // either the producer rate or consumer drain rate.
                warn!(
                    sub = sub.get(),
                    capacity = channel_capacity,
                    full_events = channel_full_events,
                    policy = ?self.config.backpressure,
                    "per-sub channel saturated; Resync(Backpressure) force-sent",
                );
                if let Some(record) = inner.registry.get_mut(sub) {
                    record.state = SubscriptionState::Draining;
                }
                Err(RouterError::ChannelSaturated)
            }
            BackpressureOutcome::Closed => Err(RouterError::UnknownSubscription(sub)),
        }
    }

    /// Drains `cursor` into the subscription's channel. This is the
    /// serving-path pump for non-host-routed subscriptions (the gRPC
    /// adapter's per-subscription cursor task calls it every poll
    /// tick); tests drive the router with [`crate::cursor::VecCursor`].
    ///
    /// Empty transactions (Begin/Commit pairs that produced no diffs
    /// for this query) are skipped rather than surfaced as no-op
    /// events. On error the drain stops immediately: the failed
    /// transaction is dropped (for `ChannelSaturated` the router has
    /// already forced a `Resync`, so the client refetches), and any
    /// transactions still queued in the cursor are picked up by the
    /// caller's next drain.
    pub fn pump_cursor<C: TraceCursor + ?Sized>(
        &self,
        sub: SubscriptionId,
        cursor: &mut C,
        primary_key: &[usize],
    ) -> Result<usize, RouterError> {
        let mut events = 0;
        while let Some(delta) = cursor.next_transaction() {
            if delta.is_empty() {
                continue;
            }
            self.pump_transaction(sub, delta, primary_key)?;
            events += 1;
        }
        Ok(events)
    }

    /// Records a client ack and advances the global watermark.
    pub fn ack(&self, sub: SubscriptionId, lsn: Lsn) -> Result<AckOutcome, RouterError> {
        let mut inner = self.inner.lock().expect("router lock");
        let Some(record) = inner.registry.get_mut(sub) else {
            return Err(RouterError::UnknownSubscription(sub));
        };
        let previous = record.cursor_lsn;
        let outcome = inner.ack.ack(sub, previous, lsn);
        if let AckOutcome::Advanced(new) = outcome {
            if let Some(record) = inner.registry.get_mut(sub) {
                record.cursor_lsn = new;
            }
        }
        Ok(outcome)
    }

    /// Tears a subscription down. Returns the shared-subgraph release
    /// outcome so the embed shim knows whether to evict the dataflow.
    pub fn unsubscribe(
        &self,
        sub: SubscriptionId,
    ) -> Result<Option<SharedSubgraphRelease>, RouterError> {
        let mut inner = self.inner.lock().expect("router lock");
        if inner.registry.remove(sub).is_none() {
            return Err(RouterError::UnknownSubscription(sub));
        }
        inner.ack.remove(sub);
        let release = inner
            .subgraph_holders
            .remove(&sub)
            .and_then(|id| inner.subgraphs.release(id));
        inner.channels.remove(&sub);
        self.metrics.unsubscribe();
        Ok(release)
    }

    /// Drops every subscription owned by `connection`. Returns the
    /// list of shared-subgraph releases the embed shim must process.
    pub fn disconnect(&self, connection: ConnectionId) -> Vec<SharedSubgraphRelease> {
        let ids = {
            let inner = self.inner.lock().expect("router lock");
            inner.registry.connection_subscriptions(connection)
        };
        let mut releases = Vec::new();
        for sub in ids {
            if let Ok(Some(release)) = self.unsubscribe(sub) {
                releases.push(release);
            }
        }
        releases
    }

    /// Returns the resume decision the router would take right now for
    /// `(sub, resume_lsn)`. Useful for clients submitting an `Update`
    /// or migrating between worker shards.
    #[must_use]
    pub fn resume_decision(
        &self,
        sub: SubscriptionId,
        resume_lsn: Option<Lsn>,
    ) -> Option<ResumeDecision> {
        let inner = self.inner.lock().expect("router lock");
        let record = inner.registry.get(sub)?;
        let window =
            CompactionWindow::new(record.snapshot_lsn, self.config.default_latest_known_lsn);
        Some(resolve_resume(resume_lsn, window))
    }
}

/// First base table in `graph` guarded by a row-visibility rule.
/// Used for the fail-closed error message when a rule-guarded query
/// has no compiled dataflow plan.
fn first_rule_guarded_table(graph: &MirGraph, rules: &[CompiledRule]) -> Option<String> {
    graph.node_kinds().find_map(|node| match node {
        MirNodeKind::BaseTable { table, .. } => rules
            .iter()
            .any(|rule| {
                rule.mode.affects_visibility()
                    && !rule.predicate.is_tautology()
                    && rule.table == *table
            })
            .then(|| table.clone()),
        _ => None,
    })
}

/// Builds the canonical key for a `(query, user_ctx)` pair.
///
/// This is what the [`SharedSubgraphRegistry`] uses to dedupe across
/// subscriptions; identical keys reuse the same dataflow. Re-exported
/// (via `pub`) so the gRPC adapter can use the same key as the
/// `PersistentHost` `host_key` — sharing a plan across subscribers only
/// works if both sides agree on what "the same query" means.
#[must_use]
pub fn canonical_subgraph_key(query: &QueryId, user_ctx: &UserContext) -> String {
    use std::fmt::Write;
    let mut key = String::new();
    write!(&mut key, "{}|", query.as_str()).expect("write into String");
    let mut entries: Vec<_> = user_ctx
        .iter()
        .map(|(field, value)| (field.to_owned(), value.canonical_repr()))
        .collect();
    entries.sort();
    for (field, value) in entries {
        // Length-prefixed so a value containing `;field=` cannot make
        // two different contexts share one canonical key (and thereby
        // one permission-filtered dataflow).
        write!(&mut key, "{field}={}:{value};", value.len()).expect("write into String");
    }
    key
}

/// Pairs `+1` and `-1` diffs at the same LSN by primary key.
fn pair_transaction_into_event(delta: QueryTransactionDelta, primary_key: &[usize]) -> DiffEvent {
    let changes = pair_changes(delta.diffs, primary_key);
    DiffEvent::TransactionUpdate {
        transaction_id: delta.transaction_id,
        begin_lsn: delta.begin_lsn,
        commit_lsn: delta.commit_lsn,
        end_lsn: delta.end_lsn,
        changes,
    }
}

fn pair_changes(diffs: Vec<RawDiff>, primary_key: &[usize]) -> Vec<RowChange> {
    let mut inserts: BTreeMap<Vec<Vec<u8>>, Vec<RawDiff>> = BTreeMap::new();
    let mut deletes: BTreeMap<Vec<Vec<u8>>, Vec<RawDiff>> = BTreeMap::new();

    for diff in diffs {
        let key = primary_key_bytes(&diff.row, primary_key);
        if diff.diff > 0 {
            inserts.entry(key).or_default().push(diff);
        } else {
            deletes.entry(key).or_default().push(diff);
        }
    }

    let mut changes = Vec::new();
    for (key, mut ins) in inserts {
        if let Some(mut outs) = deletes.remove(&key) {
            // Same primary key on both sides ⇒ Update.
            let new = ins.pop().expect("non-empty bin").row;
            let old = outs.pop().expect("non-empty bin").row;
            changes.push(RowChange {
                op: DiffOp::Update,
                old: Some(old),
                new: Some(new),
            });
            // Any remaining duplicates of the same PK at the same LSN
            // are emitted as separate ops.
            for extra in ins {
                changes.push(RowChange {
                    op: DiffOp::Insert,
                    old: None,
                    new: Some(extra.row),
                });
            }
            for extra in outs {
                changes.push(RowChange {
                    op: DiffOp::Delete,
                    old: Some(extra.row),
                    new: None,
                });
            }
        } else {
            for diff in ins {
                changes.push(RowChange {
                    op: DiffOp::Insert,
                    old: None,
                    new: Some(diff.row),
                });
            }
        }
    }
    for diff in deletes.into_values().flatten() {
        changes.push(RowChange {
            op: DiffOp::Delete,
            old: Some(diff.row),
            new: None,
        });
    }

    changes
}

fn primary_key_bytes(row: &Row, primary_key: &[usize]) -> Vec<Vec<u8>> {
    primary_key
        .iter()
        .map(|index| row.get(*index).map(datum_to_key_bytes).unwrap_or_default())
        .collect()
}

fn datum_to_key_bytes(datum: &palimpsest_wal::Datum) -> Vec<u8> {
    use palimpsest_wal::Datum;
    match datum {
        Datum::I64(value) => value.to_be_bytes().to_vec(),
        Datum::I32(value) => value.to_be_bytes().to_vec(),
        Datum::I16(value) => value.to_be_bytes().to_vec(),
        Datum::Bool(value) => vec![u8::from(*value)],
        Datum::Text(bytes) | Datum::Bytea(bytes) => bytes.to_vec(),
        Datum::Uuid(uuid) => uuid.as_bytes().to_vec(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use palimpsest_dataflow::palimpsest::Lsn;
    use palimpsest_permissions::{
        compile_rules, PermissionRule, UserContext, UserContextSchema, UserValue,
    };
    use palimpsest_sql::{lower::parse_and_lower, Catalog, ColumnType};
    use palimpsest_wal::{Datum, DatumType, TableId};
    use smallvec::smallvec;
    use tokio_stream::StreamExt;

    use super::{
        canonical_subgraph_key, AckOutcome, RouterConfig, SubscribeRequest, SubscriptionRouter,
    };
    use crate::{
        cursor::{RawDiff, VecCursor},
        diff::{DiffEvent, DiffOp},
        snapshot::{SnapshotBatch, SnapshotProvider, SnapshotTableRows},
        subscription::{
            ClientSubscriptionId, ColumnSpec, ConnectionId, QueryId, SchemaDefinition, SchemaId,
        },
    };

    struct StubProvider {
        responses: RefCell<Vec<SnapshotBatch>>,
    }
    impl SnapshotProvider for StubProvider {
        fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
            Ok(self
                .responses
                .borrow_mut()
                .pop()
                .expect("stubbed snapshot batch"))
        }
    }

    fn schema() -> SchemaDefinition {
        SchemaDefinition {
            id: SchemaId::new(1),
            columns: vec![
                ColumnSpec {
                    name: "id".to_owned(),
                    datum_type: DatumType::I64,
                    nullable: false,
                },
                ColumnSpec {
                    name: "author_id".to_owned(),
                    datum_type: DatumType::I64,
                    nullable: false,
                },
            ],
            primary_key_columns: vec![0],
        }
    }

    fn snapshot_with(rows: Vec<i64>) -> SnapshotBatch {
        SnapshotBatch {
            snapshot_lsn: Lsn::new(50),
            rows: vec![SnapshotTableRows {
                table: TableId::new(1),
                rows: rows
                    .into_iter()
                    .map(|n| smallvec![Datum::I64(n), Datum::I64(n + 100)])
                    .collect(),
            }],
        }
    }

    #[tokio::test]
    async fn subscribe_emits_initial_event_with_snapshot_rows() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1, 2, 3])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("posts"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        assert_eq!(response.snapshot_lsn, Lsn::new(50));
        let mut stream = response.stream;
        let event = stream.next().await.expect("initial event");
        let DiffEvent::Initial { rows, .. } = event else {
            panic!("expected initial");
        };
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test]
    async fn subscribe_skips_initial_when_resume_lsn_within_window() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("posts"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: Some(Lsn::new(60)),
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        // Channel is empty; a `next` would await indefinitely so we
        // assert the Resume decision instead.
        match response.resume {
            crate::resume::ResumeDecision::Replay { from_lsn } => {
                assert_eq!(from_lsn, Lsn::new(60));
            }
            crate::resume::ResumeDecision::FreshInitial { reason } => {
                panic!("unexpected fresh initial: {reason:?}");
            }
        }
    }

    #[tokio::test]
    async fn pump_batch_pairs_insert_and_delete_into_update() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("posts"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();

        let mut cursor = VecCursor::new([
            RawDiff {
                table: None,
                row: smallvec![Datum::I64(1), Datum::I64(99)],
                lsn: Lsn::new(60),
                diff: -1,
            },
            RawDiff {
                table: None,
                row: smallvec![Datum::I64(1), Datum::I64(100)],
                lsn: Lsn::new(60),
                diff: 1,
            },
        ]);

        let pumped = router
            .pump_cursor(response.subscription_id, &mut cursor, &[0])
            .unwrap();
        assert_eq!(pumped, 1);

        let mut stream = response.stream;
        // Drain Initial first.
        let _ = stream.next().await;
        let event = stream.next().await.expect("update");
        let DiffEvent::TransactionUpdate { changes, .. } = event else {
            panic!("expected transaction update");
        };
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].op, DiffOp::Update);
    }

    #[tokio::test]
    async fn ack_advances_cursor_and_unsubscribe_releases_subgraph() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("posts"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        let id = response.subscription_id;

        let outcome = router.ack(id, Lsn::new(60)).unwrap();
        assert!(matches!(outcome, AckOutcome::Advanced(lsn) if lsn == Lsn::new(60)));

        let release = router.unsubscribe(id).unwrap().expect("release");
        assert!(matches!(
            release,
            palimpsest_dataflow::palimpsest::SharedSubgraphRelease::Teardown { .. }
        ));
        assert_eq!(router.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn duplicate_subscriptions_share_subgraph() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1]), snapshot_with(vec![1])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let user_ctx = UserContext::new([("id".to_owned(), UserValue::Int(7))]);

        let first = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("a"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: user_ctx.clone(),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        let second = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(2),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("a"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx,
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();

        assert_eq!(first.subgraph.id(), second.subgraph.id());
        assert!(matches!(
            second.subgraph,
            palimpsest_dataflow::palimpsest::SharedSubgraphAcquire::Reused { ref_count: 2, .. }
        ));
    }

    /// Schema lookup for the demo `posts` table, used to compile
    /// permission-rewritten plans in tests.
    fn posts_lookup(
        table: &str,
    ) -> Option<(TableId, palimpsest_dataflow::palimpsest::eval::ScalarSchema)> {
        (table == "posts").then(|| {
            (
                TableId::new(1),
                palimpsest_dataflow::palimpsest::eval::ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("author_id".to_owned(), ColumnType::Int),
                ]),
            )
        })
    }

    fn owner_rules() -> Vec<palimpsest_permissions::CompiledRule> {
        let user_schema = UserContextSchema::new([("id".to_owned(), ColumnType::Int)]);
        compile_rules(
            &[PermissionRule::new(
                "posts_owner",
                "posts",
                "author_id = $user.id",
            )],
            &Catalog::demo(),
            &user_schema,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn permission_filter_is_applied_during_subscribe() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules = owner_rules();
        router.set_rules(rules.clone());

        // Compile the permission-rewritten graph, as the gRPC adapter
        // does — the plan is what enforces the filter.
        let user_ctx = UserContext::new([("id".to_owned(), UserValue::Int(7))]);
        let rewritten = palimpsest_permissions::rewrite(&graph, &rules, &user_ctx)
            .unwrap()
            .graph;
        let plan = palimpsest_dataflow::palimpsest::compile_mir(&rewritten, &posts_lookup).unwrap();

        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("posts"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx,
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: Some(plan),
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        assert_eq!(response.permission_stats.filters_inserted, 1);
    }

    /// Schema lookup covering the demo `posts` + `authors` tables, for
    /// compiling permission-rewritten join plans in tests.
    fn join_lookup(
        table: &str,
    ) -> Option<(TableId, palimpsest_dataflow::palimpsest::eval::ScalarSchema)> {
        match table {
            "posts" => posts_lookup(table),
            "authors" => Some((
                TableId::new(2),
                palimpsest_dataflow::palimpsest::eval::ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("name".to_owned(), ColumnType::Text),
                ]),
            )),
            _ => None,
        }
    }

    #[tokio::test]
    async fn join_subscribe_with_visibility_rules_serves_filtered_rows() {
        // The board-shaped case: a join query on a rule-guarded table.
        // With the dataflow able to compile joins, the subscribe must
        // be accepted (no `permission_unenforceable`) and the Initial
        // must carry only the joined rows the rule admits.
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![SnapshotBatch {
                snapshot_lsn: Lsn::new(50),
                rows: vec![
                    SnapshotTableRows {
                        table: TableId::new(1),
                        rows: vec![
                            // (id, author_id): author 7 is ours, 8 is not.
                            smallvec![Datum::I64(1), Datum::I64(7)],
                            smallvec![Datum::I64(2), Datum::I64(8)],
                        ],
                    },
                    SnapshotTableRows {
                        table: TableId::new(2),
                        rows: vec![
                            smallvec![Datum::I64(7), Datum::Text("Ada".into())],
                            smallvec![Datum::I64(8), Datum::Text("Bo".into())],
                        ],
                    },
                ],
            }]),
        };
        let graph = parse_and_lower(
            "SELECT posts.id, authors.name
             FROM posts JOIN authors ON posts.author_id = authors.id",
        )
        .unwrap();
        let rules = owner_rules();
        router.set_rules(rules.clone());

        let user_ctx = UserContext::new([("id".to_owned(), UserValue::Int(7))]);
        let rewritten = palimpsest_permissions::rewrite(&graph, &rules, &user_ctx)
            .unwrap()
            .graph;
        let plan = palimpsest_dataflow::palimpsest::compile_mir(&rewritten, &join_lookup)
            .expect("join queries must compile onto the dataflow");

        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("board"),
                    query: QueryId::new("board"),
                    query_graph: &graph,
                    user_ctx,
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: Some(plan),
                    prerun_initial: None,
                },
                &provider,
            )
            .expect("join subscribe with compiled plan must not fail closed");
        assert_eq!(response.permission_stats.filters_inserted, 1);

        let mut stream = response.stream;
        let event = stream.next().await.expect("initial event");
        let DiffEvent::Initial { rows, .. } = event else {
            panic!("expected initial");
        };
        let expected: Vec<palimpsest_dataflow::palimpsest::Row> =
            vec![smallvec![Datum::I64(1), Datum::Text("Ada".into())]];
        assert_eq!(rows, expected, "only the rule-admitted joined row ships");
    }

    #[tokio::test]
    async fn pass_through_subscribe_with_visibility_rules_fails_closed() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1, 2])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        router.set_rules(owner_rules());

        // No compiled plan: the pass-through path cannot enforce the
        // spliced visibility filter, so the subscribe must be rejected
        // rather than serving unfiltered rows.
        let Err(err) = router.subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts"),
                query: QueryId::new("posts"),
                query_graph: &graph,
                user_ctx: UserContext::new([("id".to_owned(), UserValue::Int(7))]),
                schema: schema(),
                resume_lsn: None,
                compiled_plan: None,
                prerun_initial: None,
            },
            &provider,
        ) else {
            panic!("expected fail-closed rejection");
        };
        assert!(
            matches!(
                &err,
                crate::error::RouterError::PermissionUnenforceable { table } if table == "posts"
            ),
            "got {err:?}"
        );
        assert_eq!(router.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn pass_through_subscribe_with_tautology_rule_is_allowed() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let user_schema = UserContextSchema::new([("id".to_owned(), ColumnType::Int)]);
        let rules = compile_rules(
            &[PermissionRule::new("everyone", "posts", "true")],
            &Catalog::demo(),
            &user_schema,
        )
        .unwrap();
        router.set_rules(rules);

        // A tautology admits every row, so pass-through leaks nothing
        // and stays permitted.
        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("posts"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        assert_eq!(response.permission_stats.filters_inserted, 0);
    }

    #[test]
    fn canonical_key_orders_user_context_fields() {
        let ctx_a = UserContext::new([
            ("id".to_owned(), UserValue::Int(1)),
            ("org".to_owned(), UserValue::Int(2)),
        ]);
        let ctx_b = UserContext::new([
            ("org".to_owned(), UserValue::Int(2)),
            ("id".to_owned(), UserValue::Int(1)),
        ]);
        assert_eq!(
            canonical_subgraph_key(&QueryId::new("q"), &ctx_a),
            canonical_subgraph_key(&QueryId::new("q"), &ctx_b),
        );
    }

    #[tokio::test]
    async fn set_rules_forces_permissions_resync_on_active_subscriptions() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let response = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("posts"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();

        let user_schema = UserContextSchema::new([("id".to_owned(), ColumnType::Int)]);
        let rules = compile_rules(
            &[PermissionRule::new(
                "posts_owner",
                "posts",
                "author_id = $user.id",
            )],
            &Catalog::demo(),
            &user_schema,
        )
        .unwrap();
        router.set_rules(rules);

        let mut stream = response.stream;
        // Drain the Initial, then the forced resync must follow.
        let _ = stream.next().await;
        let event = stream.next().await.expect("resync event");
        assert!(
            matches!(
                event,
                DiffEvent::Resync {
                    reason: crate::diff::ResyncReason::PermissionsChanged
                }
            ),
            "got {event:?}"
        );

        let snapshot = router.metrics().snapshot();
        assert_eq!(snapshot.permission_rule_updates, 1);
        assert_eq!(snapshot.permission_resyncs_forced, 1);
    }

    #[tokio::test]
    async fn disconnect_releases_all_subscriptions_for_connection() {
        let router = SubscriptionRouter::new(RouterConfig::default());
        let provider = StubProvider {
            responses: RefCell::new(vec![snapshot_with(vec![1]), snapshot_with(vec![2])]),
        };
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();

        let _ = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("a"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        let _ = router
            .subscribe(
                SubscribeRequest {
                    connection: ConnectionId::new(1),
                    subscription_id: router.allocate_subscription_id(),
                    client_id: ClientSubscriptionId::new("b"),
                    query: QueryId::new("posts"),
                    query_graph: &graph,
                    user_ctx: UserContext::new(std::iter::empty()),
                    schema: schema(),
                    resume_lsn: None,
                    compiled_plan: None,
                    prerun_initial: None,
                },
                &provider,
            )
            .unwrap();
        assert_eq!(router.active_subscriptions(), 2);
        let releases = router.disconnect(ConnectionId::new(1));
        assert_eq!(releases.len(), 2);
        assert_eq!(router.active_subscriptions(), 0);
    }
}
