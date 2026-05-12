// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! gRPC adaptor that lifts [`SubscriptionRouter`] onto the
//! `palimpsest.sync.v1.SyncEngine` service (§13, §18.8).
//!
//! Each inbound `Subscribe` RPC is one bidirectional stream that
//! multiplexes every subscription owned by the client. The adaptor
//! authenticates the connection once, then splits the conversation into
//! two halves:
//!
//! * **inbound dispatcher** — reads `ClientMessage` from the stream and
//!   routes `Subscribe`/`Update`/`Unsubscribe`/`Ack` onto the
//!   [`SubscriptionRouter`].
//! * **outbound forwarder** — every successful subscribe spawns a task
//!   that reads `DiffEvent`s from the per-subscription
//!   [`tokio_stream::wrappers::ReceiverStream`] and serialises them as
//!   `ServerMessage::Diff` / `Resync` onto the shared response channel.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_proto::palimpsest::sync::v1 as proto;
use palimpsest_sql::{lower::parse_and_lower_with_limits, QueryLimits, SqlError};
use palimpsest_wal::DatumType;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::auth::{AuthError, DynAuthenticator};
use crate::codec::{encode_rows, CodecError};
use crate::diff::{
    DiffEvent, DiffOp as RouterDiffOp, ResyncReason as RouterResyncReason, RowChange,
};
use crate::error::RouterError;
use crate::metrics::RouterMetrics;
use crate::router::{SubscribeRequest, SubscriptionRouter};
use crate::security::{ConnectionLimiter, LimitDecision, ReconnectTracker, SecurityLimits};
use crate::subscription::{
    ClientSubscriptionId, ColumnSpec, ConnectionId, QueryId, SchemaDefinition, SchemaId,
    SubscriptionId,
};
use crate::cursor::{LsnBatch, RawDiff};
use crate::wal_runtime::WalRuntime;
use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_sql::ColumnType;

/// Bounded depth for each gRPC connection's outbound mpsc.
///
/// Larger than the per-subscription channel because every subscription
/// in the same connection multiplexes its events here.
const OUTBOUND_CAPACITY: usize = 1024;

/// Service that implements `palimpsest.sync.v1.SyncEngine`.
pub struct SyncEngineService {
    router: Arc<SubscriptionRouter>,
    auth: DynAuthenticator,
    wal: Arc<dyn WalRuntime>,
    connection_allocator: AtomicU64,
    schema_allocator: Arc<AtomicU64>,
    security: SecurityLimits,
    reconnect_tracker: Arc<ReconnectTracker>,
    /// Long-lived host that drives compiled plans incrementally.
    /// Shared by every subscription so refcounted plan reuse works
    /// across connections; each subscription register_or_seed's its
    /// canonical key and the cursor pump pushes WAL diffs through.
    dataflow_host: Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
}

impl SyncEngineService {
    /// Builds a new service from its three external dependencies. Uses
    /// [`SecurityLimits::DEFAULT`].
    #[must_use]
    pub fn new(
        router: Arc<SubscriptionRouter>,
        auth: DynAuthenticator,
        wal: Arc<dyn WalRuntime>,
    ) -> Self {
        Self::with_security(router, auth, wal, SecurityLimits::DEFAULT)
    }

    /// Builds a service with explicit [`SecurityLimits`].
    #[must_use]
    pub fn with_security(
        router: Arc<SubscriptionRouter>,
        auth: DynAuthenticator,
        wal: Arc<dyn WalRuntime>,
        security: SecurityLimits,
    ) -> Self {
        Self {
            router,
            auth,
            wal,
            connection_allocator: AtomicU64::new(1),
            schema_allocator: Arc::new(AtomicU64::new(1)),
            security,
            reconnect_tracker: Arc::new(ReconnectTracker::new(security.reconnect_rate)),
            dataflow_host: Arc::new(palimpsest_dataflow::palimpsest::PersistentHost::new()),
        }
    }

    fn next_connection(&self) -> ConnectionId {
        ConnectionId::new(self.connection_allocator.fetch_add(1, Ordering::Relaxed))
    }
}

#[async_trait]
impl proto::sync_engine_server::SyncEngine for SyncEngineService {
    type SubscribeStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<proto::ServerMessage, Status>> + Send + 'static>,
    >;

    async fn subscribe(
        &self,
        request: Request<Streaming<proto::ClientMessage>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        if let Some(addr) = request.remote_addr() {
            if matches!(
                self.reconnect_tracker.admit(addr.ip()),
                LimitDecision::ReconnectRateExceeded
            ) {
                warn!(ip = %addr.ip(), "reconnect rate limit exceeded");
                return Err(Status::resource_exhausted(
                    "reconnect rate limit exceeded for source IP",
                ));
            }
        }

        let user_ctx = self
            .auth
            .authenticate(request.metadata())
            .await
            .map_err(auth_error_to_status)?;

        let connection = self.next_connection();
        info!(
            connection = connection.get(),
            "gRPC subscribe stream opened"
        );

        let inbound = request.into_inner();
        let (outbound_tx, outbound_rx) =
            mpsc::channel::<Result<proto::ServerMessage, Status>>(OUTBOUND_CAPACITY);

        let router = Arc::clone(&self.router);
        let wal = Arc::clone(&self.wal);
        let schemas = Arc::clone(&self.schema_allocator);
        let limiter = Arc::new(ConnectionLimiter::new(self.security));
        let host = Arc::clone(&self.dataflow_host);

        tokio::spawn(connection_loop(
            connection,
            user_ctx,
            inbound,
            outbound_tx,
            router,
            wal,
            schemas,
            limiter,
            host,
        ));

        let stream = ReceiverStream::new(outbound_rx);
        Ok(Response::new(Box::pin(stream)))
    }
}

fn allocate_schema_id(allocator: &AtomicU64) -> SchemaId {
    SchemaId::new(allocator.fetch_add(1, Ordering::Relaxed))
}

#[allow(clippy::too_many_arguments)]
async fn connection_loop(
    connection: ConnectionId,
    user_ctx: palimpsest_permissions::UserContext,
    mut inbound: Streaming<proto::ClientMessage>,
    outbound: mpsc::Sender<Result<proto::ServerMessage, Status>>,
    router: Arc<SubscriptionRouter>,
    wal: Arc<dyn WalRuntime>,
    schemas: Arc<AtomicU64>,
    limiter: Arc<ConnectionLimiter>,
    host: Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
) {
    let mut state = ConnectionState::default();

    while let Some(message) = inbound.next().await {
        match message {
            Ok(proto::ClientMessage { kind: Some(kind) }) => {
                if let Err(closed) = handle_client_message(
                    kind,
                    connection,
                    &user_ctx,
                    &outbound,
                    &router,
                    &wal,
                    schemas.as_ref(),
                    &mut state,
                    limiter.as_ref(),
                    &host,
                )
                .await
                {
                    warn!(?closed, "outbound channel closed; ending connection");
                    break;
                }
            }
            Ok(proto::ClientMessage { kind: None }) => {
                debug!("ignored empty ClientMessage");
            }
            Err(status) => {
                warn!(?status, "inbound stream error");
                break;
            }
        }
    }

    info!(
        connection = connection.get(),
        "gRPC subscribe stream closed"
    );
    for handle in state.forwarders.drain() {
        handle.abort();
    }
    let _ = router.disconnect(connection);
}

#[derive(Default)]
struct ConnectionState {
    /// Map server-assigned id → outbound forwarder task handle so
    /// teardown can abort them.
    forwarders: ForwarderHandles,
    /// Map client-supplied label → server-assigned id (so Update/Ack
    /// referenced by `client_subscription_id` resolve).
    by_client_id: HashMap<String, SubscriptionId>,
}

/// Owns `JoinHandle`s so the connection loop can abort outstanding
/// forwarders on disconnect.
#[derive(Default)]
struct ForwarderHandles {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl ForwarderHandles {
    fn push(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.handles.push(handle);
    }

    fn drain(&mut self) -> impl Iterator<Item = tokio::task::JoinHandle<()>> + '_ {
        self.handles.drain(..)
    }
}

#[derive(Debug)]
struct ChannelClosed;

/// Releases a limiter admission unless explicitly `commit()`-ted, so
/// any early return out of [`handle_subscribe`] after `admit_subscribe`
/// gives the slot back instead of leaking capacity.
struct AdmitGuard<'limiter> {
    limiter: &'limiter ConnectionLimiter,
    committed: bool,
}

impl<'limiter> AdmitGuard<'limiter> {
    const fn new(limiter: &'limiter ConnectionLimiter) -> Self {
        Self {
            limiter,
            committed: false,
        }
    }

    const fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for AdmitGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.limiter.release();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_message(
    kind: proto::client_message::Kind,
    connection: ConnectionId,
    user_ctx: &palimpsest_permissions::UserContext,
    outbound: &mpsc::Sender<Result<proto::ServerMessage, Status>>,
    router: &Arc<SubscriptionRouter>,
    wal: &Arc<dyn WalRuntime>,
    schemas: &AtomicU64,
    state: &mut ConnectionState,
    limiter: &ConnectionLimiter,
    host: &Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
) -> Result<(), ChannelClosed> {
    match kind {
        proto::client_message::Kind::Subscribe(request) => {
            handle_subscribe(
                connection, user_ctx, request, outbound, router, wal, host, schemas, state,
                limiter,
            )
            .await
        }
        proto::client_message::Kind::Update(_) => {
            send_error(
                outbound,
                String::new(),
                "unimplemented",
                "Update is not yet implemented",
            )
            .await
        }
        proto::client_message::Kind::Unsubscribe(req) => {
            handle_unsubscribe(req, outbound, router, state, limiter).await
        }
        proto::client_message::Kind::Ack(req) => handle_ack(req, outbound, router, state).await,
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn handle_subscribe(
    connection: ConnectionId,
    user_ctx: &palimpsest_permissions::UserContext,
    request: proto::SubscribeRequest,
    outbound: &mpsc::Sender<Result<proto::ServerMessage, Status>>,
    router: &Arc<SubscriptionRouter>,
    wal: &Arc<dyn WalRuntime>,
    host: &Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
    schemas: &AtomicU64,
    state: &mut ConnectionState,
    limiter: &ConnectionLimiter,
) -> Result<(), ChannelClosed> {
    if let Some((code, detail)) = check_limiter(limiter) {
        return send_error(outbound, request.client_subscription_id, code, detail).await;
    }
    let mut admit_guard = AdmitGuard::new(limiter);

    let client_id = ClientSubscriptionId::new(request.client_subscription_id.clone());
    // Use the SQL text as the `QueryId`. Two subscribers running the
    // same query share a canonical subgraph key (subgraph reuse §11.4),
    // and the WAL runtime can pattern-match the QueryId to route a
    // snapshot/cursor to the right underlying table. Until the trait
    // is extended to receive the lowered MIR directly, this is the
    // sharpest signal a runtime gets about *what* the subscriber asked
    // for.
    let query = QueryId::new(request.sql.clone());
    let resume_lsn = request.resume_lsn.map(Lsn::new);

    let graph = match parse_and_lower_with_limits(&request.sql, QueryLimits::DEFAULT) {
        Ok(graph) => graph,
        Err(SqlError::QueryTooLarge { .. }) => {
            return send_error(
                outbound,
                request.client_subscription_id,
                "query_too_large",
                "SQL input exceeds the configured byte limit",
            )
            .await;
        }
        Err(SqlError::QueryTooComplex { .. }) => {
            return send_error(
                outbound,
                request.client_subscription_id,
                "query_too_complex",
                "lowered MIR exceeds the configured node-count limit",
            )
            .await;
        }
        Err(err) => {
            return send_error(
                outbound,
                request.client_subscription_id,
                "invalid_sql",
                &err.to_string(),
            )
            .await;
        }
    };

    // Try to compile the MIR into a dataflow plan. If the WAL
    // runtime exposes typed table schemas (the post-v1 contract) and
    // the query lowers cleanly, we run the snapshot through the
    // dataflow so the client sees the query's *result* rows. If
    // either is missing, fall back to the v1 pass-through path that
    // ships raw table rows verbatim.
    let table_lookup = WalTableLookup { wal: wal.as_ref() };
    // Compile against the permission-rewritten graph so the dataflow
    // honours row-visibility rules end-to-end. `router.subscribe`
    // will re-run `install_permission_filters` on the original
    // graph; the redundant work is cheap and the outputs match.
    let rules_snapshot = router.permission_rules();
    let rewritten_for_compile = palimpsest_permissions::rewrite(
        &graph,
        &rules_snapshot,
        user_ctx,
    )
    .ok()
    .map(|outcome| outcome.graph);
    let compile_input = rewritten_for_compile.as_ref().unwrap_or(&graph);
    let compiled_plan =
        palimpsest_dataflow::palimpsest::compile_mir(compile_input, &table_lookup).ok();
    let compiled_plan_for_host = compiled_plan.clone();

    let mut schema = match (
        compiled_plan.as_ref(),
        wal.query_schema(&query),
    ) {
        (Some(plan), _) => schema_definition_from_plan(plan),
        (None, Ok(schema)) => schema,
        (None, Err(err)) => {
            return send_error(
                outbound,
                request.client_subscription_id,
                "schema_lookup_failed",
                &err,
            )
            .await;
        }
    };
    schema.id = allocate_schema_id(schemas);

    let response = router.subscribe(
        SubscribeRequest {
            connection,
            client_id: client_id.clone(),
            query,
            query_graph: &graph,
            user_ctx: user_ctx.clone(),
            schema: schema.clone(),
            resume_lsn,
            compiled_plan,
        },
        wal.as_ref(),
    );

    let response = match response {
        Ok(response) => response,
        Err(RouterError::DuplicateClientSubscriptionId) => {
            return send_error(
                outbound,
                request.client_subscription_id,
                "duplicate_subscription_id",
                "client_subscription_id already in use on this connection",
            )
            .await;
        }
        Err(RouterError::Permission(err)) => {
            return send_error(
                outbound,
                request.client_subscription_id,
                "permission_denied",
                &err.to_string(),
            )
            .await;
        }
        Err(err) => {
            return send_error(
                outbound,
                request.client_subscription_id,
                "subscribe_failed",
                &err.to_string(),
            )
            .await;
        }
    };

    let server_id = response.subscription_id;
    admit_guard.commit();
    state
        .by_client_id
        .insert(request.client_subscription_id.clone(), server_id);

    let accepted = proto::ServerMessage {
        kind: Some(proto::server_message::Kind::Accepted(proto::Accepted {
            subscription_id: request.client_subscription_id.clone(),
            schema_id: response.schema_id.get(),
            snapshot_lsn: response.snapshot_lsn.get(),
            schema: Some(schema_to_proto(&schema)),
        })),
    };
    if outbound.send(Ok(accepted)).await.is_err() {
        return Err(ChannelClosed);
    }

    // Seed the persistent host with the same raw rows the router
    // just shipped as `Initial` so its `last_output` matches the
    // client's current view. Subsequent WAL diffs flow through the
    // host's `push_table_batch` to produce *aggregate* deltas.
    let host_key = host_canonical_key(server_id);
    if let Some(plan) = compiled_plan_for_host.as_ref() {
        let mut grouped: std::collections::HashMap<palimpsest_wal::TableId, Vec<palimpsest_dataflow::palimpsest::Row>> =
            std::collections::HashMap::new();
        for update in &response.seed_updates {
            if update.diff > 0 {
                grouped
                    .entry(update.table)
                    .or_default()
                    .push(update.row.clone());
            }
        }
        host.register_or_seed(&host_key, plan, grouped);
    }

    let primary_key = schema.primary_key_columns.clone();
    // Cursor pump uses the SQL-derived QueryId so it lands in the
    // same WAL-runtime dispatch bucket as the snapshot fetched above.
    let cursor_query = QueryId::new(request.sql.clone());
    let cursor_handle = spawn_cursor_pump(
        server_id,
        cursor_query,
        response.snapshot_lsn,
        primary_key,
        Arc::clone(router),
        Arc::clone(wal),
        Arc::clone(host),
        host_key,
        compiled_plan_for_host,
    );
    state.forwarders.push(cursor_handle);

    let forwarder = spawn_forwarder(
        request.client_subscription_id,
        response.schema_id,
        response.stream,
        outbound.clone(),
        router.metrics().clone(),
    );
    state.forwarders.push(forwarder);
    Ok(())
}

/// Drains the WAL runtime's per-subscription cursor into the router.
///
/// Polls every 50ms; rapid bursts are coalesced via `pump_cursor`'s
/// inner loop (which drains the cursor before yielding). The task
/// exits when the cursor stops yielding *and* the subscription has
/// been torn down — surfaced as `RouterError::UnknownSubscription`
/// from [`SubscriptionRouter::pump_batch`].
///
/// Wakeup signalling: the `TraceCursor` trait is sync and returns
/// `None` between bursts. A proper notify-based wakeup belongs to a
/// future cursor protocol (DESIGN.md §18.5); 50ms is a workable
/// trade-off between latency and idle CPU for an in-process WAL.
#[allow(clippy::too_many_arguments)]
fn spawn_cursor_pump(
    sub_id: SubscriptionId,
    query: QueryId,
    from_lsn: Lsn,
    primary_key: Vec<usize>,
    router: Arc<SubscriptionRouter>,
    wal: Arc<dyn WalRuntime>,
    host: Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
    host_key: String,
    plan: Option<palimpsest_dataflow::palimpsest::CompiledPlan>,
) -> tokio::task::JoinHandle<()> {
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

    tokio::spawn(async move {
        let mut cursor = match wal.open_cursor(&query, from_lsn) {
            Ok(c) => c,
            Err(err) => {
                warn!(sub = sub_id.get(), %err, "open_cursor failed; live diffs disabled for this subscription");
                return;
            }
        };
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Single-table plans route raw WAL diffs through the
        // persistent host so the dataflow re-aggregates and the
        // client sees aggregate deltas (retract + assert of changed
        // bucket rows) rather than raw rows from the underlying
        // table. Multi-table plans aren't supported by the host
        // yet, so they fall through to the v1 raw-row pump.
        let route_through_host = plan
            .as_ref()
            .map_or(false, |p| p.inputs.len() == 1);
        let table_id = plan
            .as_ref()
            .and_then(|p| p.inputs.first().copied());

        loop {
            tick.tick().await;
            while let Some(batch) = cursor.next_batch() {
                let to_pump = if route_through_host {
                    let table_id = table_id.expect("checked above");
                    let diffs: Vec<(
                        palimpsest_wal::TableId,
                        palimpsest_dataflow::palimpsest::Row,
                        isize,
                    )> = batch
                        .diffs
                        .iter()
                        .map(|d| (table_id, d.row.clone(), d.diff as isize))
                        .collect();
                    let deltas = host.push_table_batch(&host_key, diffs, batch.lsn);
                    LsnBatch {
                        lsn: batch.lsn,
                        diffs: deltas
                            .into_iter()
                            .map(|d| RawDiff {
                                row: d.row,
                                lsn: d.lsn,
                                diff: i64::from(d.diff as i32),
                            })
                            .collect(),
                    }
                } else {
                    batch
                };
                if to_pump.diffs.is_empty() {
                    continue;
                }
                match router.pump_batch(sub_id, to_pump, &primary_key) {
                    Ok(()) => {}
                    Err(RouterError::UnknownSubscription(_)) => {
                        debug!(sub = sub_id.get(), "cursor pump: subscription gone, exiting");
                        host.release(&host_key);
                        return;
                    }
                    Err(err) => {
                        warn!(sub = sub_id.get(), ?err, "cursor pump: pump_batch failed");
                        host.release(&host_key);
                        return;
                    }
                }
            }
        }
    })
}

/// Stable string key the persistent host uses to namespace each
/// subscription's cumulative state.
fn host_canonical_key(sub_id: SubscriptionId) -> String {
    format!("sub-{}", sub_id.get())
}

/// Bridges the WAL runtime's `table_schema` lookup into the trait the
/// MIR compiler consumes. Avoids leaking `WalRuntime` as a generic
/// bound on the compiler's public surface.
struct WalTableLookup<'a> {
    wal: &'a dyn WalRuntime,
}

impl<'a> palimpsest_dataflow::palimpsest::TableSchemaLookup for WalTableLookup<'a> {
    fn lookup(&self, table: &str) -> Option<(palimpsest_wal::TableId, ScalarSchema)> {
        self.wal.table_schema(table)
    }
}

/// Convert a `ScalarSchema` (the dataflow's typed schema) into a
/// `SchemaDefinition` (the gRPC `Accepted` payload). PKs collapse to
/// the first column — fine for aggregate output where `category_id`
/// (or whatever the group key is) leads the row.
fn schema_definition_from_plan(
    plan: &palimpsest_dataflow::palimpsest::CompiledPlan,
) -> SchemaDefinition {
    let columns = plan
        .output_schema
        .columns()
        .iter()
        .map(|(name, ty)| ColumnSpec {
            name: name.clone(),
            datum_type: column_type_to_datum_type(*ty),
            nullable: false,
        })
        .collect();
    SchemaDefinition {
        id: SchemaId::new(0),
        columns,
        primary_key_columns: vec![0],
    }
}

fn column_type_to_datum_type(ty: ColumnType) -> DatumType {
    match ty {
        ColumnType::Bool => DatumType::Bool,
        ColumnType::Int => DatumType::I64,
        ColumnType::Float => DatumType::F64,
        ColumnType::Text => DatumType::Text,
        ColumnType::Timestamp => DatumType::Timestamp,
        // `Unknown` is the catalog's "couldn't infer" type — picking
        // Text is a defensive default that won't crash decoders that
        // probe the schema.
        ColumnType::Unknown => DatumType::Text,
    }
}

fn check_limiter(limiter: &ConnectionLimiter) -> Option<(&'static str, &'static str)> {
    match limiter.admit_subscribe() {
        LimitDecision::Allowed => None,
        LimitDecision::ConnectionSaturated => Some((
            "connection_saturated",
            "this connection has reached its subscription cap",
        )),
        LimitDecision::SubscribeRateExceeded => Some((
            "rate_limited",
            "subscribe rate limit exceeded for this connection",
        )),
        // Per-IP gate is applied at stream open; matched here for
        // completeness so the enum stays exhaustive.
        LimitDecision::ReconnectRateExceeded => Some(("rate_limited", "rate limit exceeded")),
    }
}

async fn handle_unsubscribe(
    request: proto::UnsubscribeRequest,
    outbound: &mpsc::Sender<Result<proto::ServerMessage, Status>>,
    router: &Arc<SubscriptionRouter>,
    state: &mut ConnectionState,
    limiter: &ConnectionLimiter,
) -> Result<(), ChannelClosed> {
    let Some(server_id) = state.by_client_id.remove(&request.subscription_id) else {
        return send_error(
            outbound,
            request.subscription_id,
            "unknown_subscription",
            "no subscription with that id",
        )
        .await;
    };
    if let Err(err) = router.unsubscribe(server_id) {
        return send_error(
            outbound,
            request.subscription_id,
            "unsubscribe_failed",
            &err.to_string(),
        )
        .await;
    }
    limiter.release();
    Ok(())
}

async fn handle_ack(
    request: proto::AckRequest,
    outbound: &mpsc::Sender<Result<proto::ServerMessage, Status>>,
    router: &Arc<SubscriptionRouter>,
    state: &ConnectionState,
) -> Result<(), ChannelClosed> {
    let Some(server_id) = state.by_client_id.get(&request.subscription_id).copied() else {
        return send_error(
            outbound,
            request.subscription_id,
            "unknown_subscription",
            "no subscription with that id",
        )
        .await;
    };
    if let Err(err) = router.ack(server_id, Lsn::new(request.lsn)) {
        return send_error(
            outbound,
            request.subscription_id,
            "ack_failed",
            &err.to_string(),
        )
        .await;
    }
    Ok(())
}

fn spawn_forwarder(
    client_subscription_id: String,
    schema_id: SchemaId,
    mut stream: ReceiverStream<DiffEvent>,
    outbound: mpsc::Sender<Result<proto::ServerMessage, Status>>,
    metrics: RouterMetrics,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = stream.next().await {
            if let DiffEvent::Resync { reason } = &event {
                metrics.record_resync_with_reason(*reason);
            }
            let Some(message) = diff_event_to_message(&client_subscription_id, schema_id, &event)
            else {
                error!(client_subscription_id, "failed to encode diff event");
                continue;
            };
            if let Some(proto::server_message::Kind::Diff(diff)) = &message.kind {
                metrics.record_bytes_sent(diff.rows.len() as u64);
            }
            if outbound.send(Ok(message)).await.is_err() {
                break;
            }
        }
    })
}

async fn send_error(
    outbound: &mpsc::Sender<Result<proto::ServerMessage, Status>>,
    subscription_id: String,
    code: &str,
    message: &str,
) -> Result<(), ChannelClosed> {
    let body = proto::ServerMessage {
        kind: Some(proto::server_message::Kind::Error(proto::Error {
            subscription_id,
            code: code.to_owned(),
            message: message.to_owned(),
        })),
    };
    outbound.send(Ok(body)).await.map_err(|_| ChannelClosed)
}

fn auth_error_to_status(err: AuthError) -> Status {
    match err {
        AuthError::MissingAuthorization => Status::unauthenticated("missing authorization"),
        AuthError::InvalidToken(detail) => {
            Status::unauthenticated(format!("invalid token: {detail}"))
        }
        AuthError::MissingClaim(name) => Status::unauthenticated(format!("missing claim: {name}")),
        AuthError::InvalidClaimShape(name) => {
            Status::unauthenticated(format!("invalid claim shape: {name}"))
        }
    }
}

fn diff_event_to_message(
    client_subscription_id: &str,
    schema_id: SchemaId,
    event: &DiffEvent,
) -> Option<proto::ServerMessage> {
    let kind = match event {
        DiffEvent::Initial { lsn, rows } => {
            let bytes = match encode_rows(rows) {
                Ok(bytes) => bytes,
                Err(err) => {
                    log_codec_error(&err);
                    return None;
                }
            };
            proto::server_message::Kind::Diff(proto::Diff {
                subscription_id: client_subscription_id.to_owned(),
                lsn: lsn.get(),
                op: proto::DiffOp::Initial.into(),
                schema_id: schema_id.get(),
                rows: bytes,
            })
        }
        DiffEvent::Update { lsn, changes } => {
            let (op, rows) = encode_changes(changes)?;
            proto::server_message::Kind::Diff(proto::Diff {
                subscription_id: client_subscription_id.to_owned(),
                lsn: lsn.get(),
                op: op.into(),
                schema_id: schema_id.get(),
                rows,
            })
        }
        DiffEvent::Resync { reason } => proto::server_message::Kind::Resync(proto::Resync {
            subscription_id: client_subscription_id.to_owned(),
            reason: resync_reason_to_proto(*reason).into(),
            message: format!("{reason:?}"),
        }),
    };
    Some(proto::ServerMessage { kind: Some(kind) })
}

fn encode_changes(changes: &[RowChange]) -> Option<(proto::DiffOp, Vec<u8>)> {
    let dominant_op = dominant_op(changes);
    let rows: Vec<_> = changes
        .iter()
        .filter_map(|change| change.new.clone().or_else(|| change.old.clone()))
        .collect();
    let bytes = match encode_rows(&rows) {
        Ok(bytes) => bytes,
        Err(err) => {
            log_codec_error(&err);
            return None;
        }
    };
    Some((diff_op_to_proto(dominant_op), bytes))
}

fn dominant_op(changes: &[RowChange]) -> RouterDiffOp {
    // The router's `pair_into_event` only ever yields a homogeneous-op
    // batch in the simple case. When mixed (Insert + Delete that did
    // not pair), we summarize as Update so the client knows it must
    // reconcile both `old` and `new`.
    let mut seen_insert = false;
    let mut seen_update = false;
    let mut seen_delete = false;
    for change in changes {
        match change.op {
            RouterDiffOp::Insert => seen_insert = true,
            RouterDiffOp::Update => seen_update = true,
            RouterDiffOp::Delete => seen_delete = true,
            RouterDiffOp::Initial => return RouterDiffOp::Initial,
        }
    }
    match (seen_insert, seen_update, seen_delete) {
        (true, false, false) => RouterDiffOp::Insert,
        (false, false, true) => RouterDiffOp::Delete,
        _ => RouterDiffOp::Update,
    }
}

const fn diff_op_to_proto(op: RouterDiffOp) -> proto::DiffOp {
    match op {
        RouterDiffOp::Initial => proto::DiffOp::Initial,
        RouterDiffOp::Insert => proto::DiffOp::Insert,
        RouterDiffOp::Update => proto::DiffOp::Update,
        RouterDiffOp::Delete => proto::DiffOp::Delete,
    }
}

const fn resync_reason_to_proto(reason: RouterResyncReason) -> proto::ResyncReason {
    match reason {
        RouterResyncReason::LsnCompacted => proto::ResyncReason::LsnCompacted,
        RouterResyncReason::SchemaChanged | RouterResyncReason::PermissionsChanged => {
            proto::ResyncReason::SchemaChanged
        }
        RouterResyncReason::Backpressure => proto::ResyncReason::Backpressure,
        RouterResyncReason::SlotRecreated => proto::ResyncReason::SlotRecreated,
    }
}

fn schema_to_proto(schema: &SchemaDefinition) -> proto::Schema {
    proto::Schema {
        columns: schema.columns.iter().map(column_spec_to_proto).collect(),
        primary_key_columns: schema
            .primary_key_columns
            .iter()
            .map(|index| u32::try_from(*index).unwrap_or(u32::MAX))
            .collect(),
    }
}

fn column_spec_to_proto(column: &ColumnSpec) -> proto::Column {
    proto::Column {
        name: column.name.clone(),
        r#type: datum_type_to_proto(&column.datum_type).into(),
        nullable: column.nullable,
    }
}

const fn datum_type_to_proto(datum_type: &DatumType) -> proto::DatumType {
    match datum_type {
        DatumType::Bool => proto::DatumType::Bool,
        DatumType::I16 => proto::DatumType::I16,
        DatumType::I32 => proto::DatumType::I32,
        DatumType::I64 => proto::DatumType::I64,
        DatumType::F32 => proto::DatumType::F32,
        DatumType::F64 => proto::DatumType::F64,
        DatumType::Numeric => proto::DatumType::Numeric,
        DatumType::Text => proto::DatumType::Text,
        DatumType::Bytea => proto::DatumType::Bytea,
        DatumType::Date => proto::DatumType::Date,
        DatumType::Time => proto::DatumType::Time,
        DatumType::Timestamp => proto::DatumType::Timestamp,
        DatumType::TimestampTz => proto::DatumType::TimestampTz,
        DatumType::Interval => proto::DatumType::Interval,
        DatumType::Uuid => proto::DatumType::Uuid,
        DatumType::Json => proto::DatumType::Json,
        DatumType::Jsonb => proto::DatumType::Jsonb,
        DatumType::Array(_) => proto::DatumType::Array,
    }
}

fn log_codec_error(err: &CodecError) {
    error!(?err, "codec failure while encoding diff payload");
}
