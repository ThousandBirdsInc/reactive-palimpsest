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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::StreamExt;
use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_proto::palimpsest::sync::v1 as proto;
use palimpsest_sql::{lower::parse_and_lower_with_limits, QueryLimits, SqlError};
use palimpsest_wal::DatumType;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::auth::{AuthError, DynAuthenticator};
use crate::codec::{encode_row, encode_rows, CodecError};
use crate::cursor::{QueryTransactionDelta, RawDiff};
use crate::diff::{
    DiffEvent, DiffOp as RouterDiffOp, ResyncReason as RouterResyncReason, RowChange,
};
use crate::error::RouterError;
use crate::metrics::RouterMetrics;
use crate::router::{canonical_subgraph_key, SubscribeRequest, SubscriptionRouter};
use crate::security::{ConnectionLimiter, LimitDecision, ReconnectTracker, SecurityLimits};
use crate::subscription::{
    ClientSubscriptionId, ColumnSpec, ConnectionId, QueryId, SchemaDefinition, SchemaId,
    SubscriptionId,
};
use crate::wal_runtime::WalRuntime;
use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_sql::ColumnType;

/// Bounded depth for each gRPC connection's outbound mpsc.
///
/// Larger than the per-subscription channel because every subscription
/// in the same connection multiplexes its events here.
const OUTBOUND_CAPACITY: usize = 1024;

/// Process-wide cap on how many `Subscribe` requests may run their
/// blocking compile + snapshot pipeline concurrently. The pipeline
/// holds several copies of the snapshot Vec in memory simultaneously
/// (router input map, dataflow inputs, persistent-host seed buffer);
/// for a 600k-row aggregate that's ~100MB+ per in-flight subscribe.
/// Capping concurrency bounds peak memory when several browsers
/// connect at once and each fires N subscribes in parallel. Cheap
/// admission decisions (rate limit, parse error) still run unbounded.
const SUBSCRIBE_BLOCKING_CONCURRENCY: usize = 2;

/// Index of cursor pumps by canonical query key.
///
/// One pump per `canonical_subgraph_key(query, user_ctx)` — same query
/// + same user context = same dataflow plan in the `PersistentHost`,
/// so we should have one cursor pulling diffs and fanning out to every
/// attached subscriber, not N independent pumps double-applying the
/// same WAL.
///
/// The pump exits on its own when the host reports zero subscribers
/// for its canonical key; the next subscribe to that key (which finds
/// either a missing or already-finished entry here) spawns a fresh
/// pump anchored at the new plan's snapshot LSN.
struct CanonicalPumpRegistry {
    inner: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

impl CanonicalPumpRegistry {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

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
    /// Process-wide cap on concurrent heavy subscribe work — see
    /// `SUBSCRIBE_BLOCKING_CONCURRENCY`.
    subscribe_blocking_slots: Arc<Semaphore>,
    /// One cursor pump per canonical query key. See
    /// [`CanonicalPumpRegistry`].
    pump_registry: Arc<CanonicalPumpRegistry>,
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
            subscribe_blocking_slots: Arc::new(Semaphore::new(SUBSCRIBE_BLOCKING_CONCURRENCY)),
            pump_registry: Arc::new(CanonicalPumpRegistry::new()),
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
        let blocking_slots = Arc::clone(&self.subscribe_blocking_slots);
        let pump_registry = Arc::clone(&self.pump_registry);

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
            blocking_slots,
            pump_registry,
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
    blocking_slots: Arc<Semaphore>,
    pump_registry: Arc<CanonicalPumpRegistry>,
) {
    let state = Arc::new(Mutex::new(ConnectionState::default()));

    while let Some(message) = inbound.next().await {
        match message {
            Ok(proto::ClientMessage { kind: Some(kind) }) => match kind {
                // Subscribe is the hot path: the SQL compile + snapshot
                // pipeline can take 100s of ms for large tables. Run
                // admission inline (so cap/rate-limit decisions are
                // taken in arrival order) but spawn the heavy work so
                // the inbound stream keeps draining and other subscribes
                // on this connection don't queue behind one slow one.
                // The forwarder + cursor-pump tasks the handler creates
                // register themselves on `state` for disconnect-time
                // teardown.
                proto::client_message::Kind::Subscribe(request) => {
                    if let Some((code, detail)) = check_limiter(limiter.as_ref()) {
                        if let Err(closed) =
                            send_error(&outbound, request.client_subscription_id, code, detail)
                                .await
                        {
                            warn!(?closed, "outbound channel closed; ending connection");
                            break;
                        }
                    } else {
                        let admit_guard = AdmitGuard::new(Arc::clone(&limiter));
                        tokio::spawn(handle_subscribe(
                            connection,
                            user_ctx.clone(),
                            request,
                            outbound.clone(),
                            Arc::clone(&router),
                            Arc::clone(&wal),
                            Arc::clone(&host),
                            Arc::clone(&schemas),
                            Arc::clone(&state),
                            admit_guard,
                            Arc::clone(&blocking_slots),
                            Arc::clone(&pump_registry),
                        ));
                    }
                }
                proto::client_message::Kind::Update(_) => {
                    if let Err(closed) = send_error(
                        &outbound,
                        String::new(),
                        "unimplemented",
                        "Update is not yet implemented",
                    )
                    .await
                    {
                        warn!(?closed, "outbound channel closed; ending connection");
                        break;
                    }
                }
                proto::client_message::Kind::Unsubscribe(req) => {
                    if let Err(closed) =
                        handle_unsubscribe(req, &outbound, &router, &state, &limiter, host.as_ref())
                            .await
                    {
                        warn!(?closed, "outbound channel closed; ending connection");
                        break;
                    }
                }
                proto::client_message::Kind::Ack(req) => {
                    if let Err(closed) = handle_ack(req, &outbound, &router, &state).await {
                        warn!(?closed, "outbound channel closed; ending connection");
                        break;
                    }
                }
            },
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
    // Drain everything we'll need under one lock.
    let (forwarders, host_attachments) = {
        let mut guard = state.lock().expect("connection state lock");
        let forwarders: Vec<_> = guard.forwarders.handles.drain(..).collect();
        let host_attachments: Vec<(SubscriptionId, String)> =
            guard.host_canonicals.drain().collect();
        (forwarders, host_attachments)
    };
    for handle in forwarders {
        handle.abort();
    }
    // Detach this connection's subscribers from any shared host plans
    // they were attached to. The pumps for any canonical keys whose
    // last subscriber just left will see `host.subscribers(...)` return
    // `None` on their next tick and exit.
    for (sub_id, canonical) in host_attachments {
        host.release(&canonical, sub_id.get());
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
    /// For each subscription that attached to a `PersistentHost`
    /// plan, the canonical key it's attached to. Used by unsubscribe
    /// + disconnect to call `host.release(canonical, sub_id)` and let
    /// the shared cursor pump exit when the last subscriber leaves.
    host_canonicals: HashMap<SubscriptionId, String>,
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
}

#[derive(Debug)]
struct ChannelClosed;

/// Releases a limiter admission unless explicitly `commit()`-ted, so
/// any early return out of [`handle_subscribe`] after `admit_subscribe`
/// gives the slot back instead of leaking capacity. Owns an
/// `Arc<ConnectionLimiter>` so the guard can travel with a spawned
/// task — its drop runs on whichever thread the task ends on, releasing
/// the slot back to the per-connection limiter.
struct AdmitGuard {
    limiter: Arc<ConnectionLimiter>,
    committed: bool,
}

impl AdmitGuard {
    fn new(limiter: Arc<ConnectionLimiter>) -> Self {
        Self {
            limiter,
            committed: false,
        }
    }

    const fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for AdmitGuard {
    fn drop(&mut self) {
        if !self.committed {
            self.limiter.release();
        }
    }
}

/// Handle one `Subscribe` RPC. Spawned by `connection_loop` so multiple
/// subscribes on the same connection can run concurrently.
///
/// `admit_guard` is constructed by the caller (so cap/rate-limit
/// decisions happen in arrival order). This handler is responsible for
/// `commit()`-ing it on success — on any early return, its `Drop` puts
/// the slot back.
///
/// The CPU-heavy section — MIR compile, snapshot fetch, dataflow seed —
/// is wrapped in `tokio::task::spawn_blocking` so it runs on the
/// blocking pool instead of pinning a tokio worker thread for the
/// duration of a 600k-row dataflow pass.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn handle_subscribe(
    connection: ConnectionId,
    user_ctx: palimpsest_permissions::UserContext,
    request: proto::SubscribeRequest,
    outbound: mpsc::Sender<Result<proto::ServerMessage, Status>>,
    router: Arc<SubscriptionRouter>,
    wal: Arc<dyn WalRuntime>,
    host: Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
    schemas: Arc<AtomicU64>,
    state: Arc<Mutex<ConnectionState>>,
    mut admit_guard: AdmitGuard,
    blocking_slots: Arc<Semaphore>,
    pump_registry: Arc<CanonicalPumpRegistry>,
) {
    let client_subscription_id = request.client_subscription_id.clone();
    let client_id = ClientSubscriptionId::new(client_subscription_id.clone());
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
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "query_too_large",
                "SQL input exceeds the configured byte limit",
            )
            .await;
            return;
        }
        Err(SqlError::QueryTooComplex { .. }) => {
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "query_too_complex",
                "lowered MIR exceeds the configured node-count limit",
            )
            .await;
            return;
        }
        Err(err) => {
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "invalid_sql",
                &err.to_string(),
            )
            .await;
            return;
        }
    };

    // The MIR compile, snapshot pull, and dataflow seed are
    // CPU-bound and synchronous (timely's `execute_directly` runs to
    // quiescence inline). Move the bundle to `spawn_blocking` so the
    // tokio runtime keeps making progress on the WS frame pump and
    // every other subscribe on this connection.
    //
    // Acquire a process-wide permit first so a burst of large
    // subscribes (e.g. several browsers connecting at once, each
    // firing the orders aggregate) can't pile up enough concurrent
    // 600k-row snapshot pipelines to OOM the box. Permit is held
    // across the `spawn_blocking` await and dropped right after.
    let _blocking_permit = match blocking_slots.acquire_owned().await {
        Ok(permit) => permit,
        Err(_closed) => {
            // Semaphore is only closed at process shutdown; treat as
            // a transient failure and bail out cleanly.
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "subscribe_failed",
                "server shutting down",
            )
            .await;
            return;
        }
    };
    let blocking = tokio::task::spawn_blocking({
        let router = Arc::clone(&router);
        let wal = Arc::clone(&wal);
        let host = Arc::clone(&host);
        let schemas = Arc::clone(&schemas);
        let user_ctx = user_ctx.clone();
        let query = query.clone();
        let client_id = client_id.clone();
        let graph = graph;
        move || {
            blocking_subscribe(
                connection,
                client_id,
                query,
                graph,
                user_ctx,
                resume_lsn,
                router.as_ref(),
                wal.as_ref(),
                host.as_ref(),
                schemas.as_ref(),
            )
        }
    })
    .await;

    let outcome = match blocking {
        Ok(outcome) => outcome,
        Err(join_err) => {
            error!(?join_err, "subscribe blocking task panicked");
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "subscribe_failed",
                "internal error",
            )
            .await;
            return;
        }
    };

    let SubscribeOutcome {
        response,
        schema,
        compiled_plan_for_host,
        host_canonical,
    } = match outcome {
        Ok(outcome) => outcome,
        Err(SubscribeBlockingError::SchemaLookup(detail)) => {
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "schema_lookup_failed",
                &detail,
            )
            .await;
            return;
        }
        Err(SubscribeBlockingError::Router(RouterError::DuplicateClientSubscriptionId)) => {
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "duplicate_subscription_id",
                "client_subscription_id already in use on this connection",
            )
            .await;
            return;
        }
        Err(SubscribeBlockingError::Router(RouterError::Permission(err))) => {
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "permission_denied",
                &err.to_string(),
            )
            .await;
            return;
        }
        Err(SubscribeBlockingError::Router(err)) => {
            let _ = send_error(
                &outbound,
                client_subscription_id,
                "subscribe_failed",
                &err.to_string(),
            )
            .await;
            return;
        }
    };

    let server_id = response.subscription_id;
    admit_guard.commit();
    {
        let mut guard = state.lock().expect("connection state lock");
        guard
            .by_client_id
            .insert(client_subscription_id.clone(), server_id);
        if let Some(canonical) = host_canonical.as_ref() {
            // Remember which canonical plan this sub is attached to,
            // so disconnect/unsubscribe can call host.release with the
            // right key.
            guard.host_canonicals.insert(server_id, canonical.clone());
        }
    }

    let accepted = proto::ServerMessage {
        kind: Some(proto::server_message::Kind::Accepted(proto::Accepted {
            subscription_id: client_subscription_id.clone(),
            schema_id: response.schema_id.get(),
            snapshot_lsn: response.snapshot_lsn.get(),
            schema: Some(schema_to_proto(&schema)),
        })),
    };
    if outbound.send(Ok(accepted)).await.is_err() {
        return;
    }

    let primary_key = schema.primary_key_columns.clone();
    // Cursor pump topology:
    //
    // * Host-routed plans (single-table aggregate that the compiler
    //   lowered): the WAL diffs are routed *through* the persistent
    //   host so the dataflow re-aggregates, and there is one pump per
    //   canonical query key. All subscribers sharing the canonical key
    //   (e.g. two browsers watching the same orders aggregate) attach
    //   to the same host plan and the same pump, which fans deltas
    //   out via `router.pump_transaction` to each subscriber's
    //   channel.
    //
    // * Non-host-routed plans (multi-table joins, queries the compiler
    //   can't lower yet): one pump per subscription, raw WAL diffs
    //   straight to the router. No state sharing.
    let cursor_query = QueryId::new(request.sql.clone());
    if let (Some(canonical), Some(plan)) =
        (host_canonical.as_ref(), compiled_plan_for_host.as_ref())
    {
        ensure_canonical_pump(
            &pump_registry,
            canonical.clone(),
            cursor_query,
            response.snapshot_lsn,
            primary_key,
            plan.clone(),
            Arc::clone(&router),
            Arc::clone(&wal),
            Arc::clone(&host),
        );
    } else {
        let cursor_handle = spawn_legacy_pump(
            server_id,
            cursor_query,
            response.snapshot_lsn,
            primary_key,
            Arc::clone(&router),
            Arc::clone(&wal),
        );
        state
            .lock()
            .expect("connection state lock")
            .forwarders
            .push(cursor_handle);
    }

    let forwarder = spawn_forwarder(
        client_subscription_id,
        response.schema_id,
        response.stream,
        outbound,
        router.metrics().clone(),
    );

    state
        .lock()
        .expect("connection state lock")
        .forwarders
        .push(forwarder);
}

/// Bundle of values the blocking subscribe section needs to hand back
/// to the async caller.
struct SubscribeOutcome {
    response: crate::router::SubscribeResponse,
    schema: SchemaDefinition,
    compiled_plan_for_host: Option<palimpsest_dataflow::palimpsest::CompiledPlan>,
    /// `Some(canonical_key)` when this subscribe attached to a shared
    /// `PersistentHost` plan. `None` for pass-through queries the
    /// compiler couldn't lower onto the host.
    host_canonical: Option<String>,
}

enum SubscribeBlockingError {
    SchemaLookup(String),
    Router(RouterError),
}

/// CPU-bound half of `handle_subscribe`. Runs on the blocking pool.
///
/// Two paths through this function:
///
/// 1. **Cached path** — the `PersistentHost` already has a plan
///    registered under this `(query, user_ctx)`'s canonical key. We
///    pre-allocate a `SubscriptionId`, attach to the existing plan via
///    `cached_view`, and pass the materialized view to `router.subscribe`
///    as `prerun_initial`. No snapshot pull, no `snapshot_run`, no
///    `register_or_seed`. This is what makes a second browser watching
///    the same query effectively free.
///
/// 2. **Cold path** — first subscriber on this canonical. We do the
///    full snapshot pull + `snapshot_run` + `register_or_seed`. The
///    host plan's worker thread + cursor pump come up as part of this
///    path.
///
/// Lock discipline notes (still valid):
/// * `router.subscribe` releases its internal mutex around the heavy
///   `snapshot_seed`/`snapshot_run` (see `router.rs:243..272`).
/// * `PersistentHost::register_or_seed` spawns the per-plan timely
///   worker outside its own inner lock (`dataflow_host.rs:412..417`).
#[allow(clippy::too_many_arguments)]
fn blocking_subscribe(
    connection: ConnectionId,
    client_id: ClientSubscriptionId,
    query: QueryId,
    graph: palimpsest_sql::mir::MirGraph,
    user_ctx: palimpsest_permissions::UserContext,
    resume_lsn: Option<Lsn>,
    router: &SubscriptionRouter,
    wal: &dyn WalRuntime,
    host: &palimpsest_dataflow::palimpsest::PersistentHost,
    schemas: &AtomicU64,
) -> Result<SubscribeOutcome, SubscribeBlockingError> {
    let table_lookup = WalTableLookup { wal };
    // Compile against the permission-rewritten graph so the dataflow
    // honours row-visibility rules end-to-end. `router.subscribe`
    // will re-run `install_permission_filters` on the original
    // graph; the redundant work is cheap and the outputs match.
    let rules_snapshot = router.permission_rules();
    let rewritten_for_compile = palimpsest_permissions::rewrite(&graph, &rules_snapshot, &user_ctx)
        .ok()
        .map(|outcome| outcome.graph);
    let compile_input = rewritten_for_compile.as_ref().unwrap_or(&graph);
    let compiled_plan =
        palimpsest_dataflow::palimpsest::compile_mir(compile_input, &table_lookup).ok();
    let compiled_plan_for_host = compiled_plan.clone();

    let mut schema = match (compiled_plan.as_ref(), wal.query_schema(&query)) {
        (Some(plan), _) => schema_definition_from_plan(plan),
        (None, Ok(schema)) => schema,
        (None, Err(err)) => return Err(SubscribeBlockingError::SchemaLookup(err)),
    };
    schema.id = allocate_schema_id(schemas);

    // Pre-allocate the SubscriptionId so we can use it as the host's
    // subscriber tag (cached_view / register_or_seed attach under this
    // id, and the cursor pump fans out diffs by calling
    // `router.pump_transaction(SubscriptionId, ...)`).
    let subscription_id = router.allocate_subscription_id();

    // Single-table host-routed plans use the canonical subgraph key
    // to share state across subscribers. Multi-table or unlowered
    // plans don't go through the host.
    let canonical_key: Option<String> = match compiled_plan_for_host.as_ref() {
        Some(plan) if plan.inputs.len() == 1 => Some(canonical_subgraph_key(&query, &user_ctx)),
        _ => None,
    };

    // Try the cached fast-path: if the host already has this canonical
    // plan registered, attach to it and use its materialized view as
    // the Initial. Otherwise we fall through to the cold path below
    // and run the full snapshot pipeline.
    let prerun_initial: Option<(Vec<palimpsest_dataflow::palimpsest::Row>, Lsn)> = canonical_key
        .as_ref()
        .and_then(|key| host.cached_view(key, subscription_id.get()));

    let mut response = router
        .subscribe(
            SubscribeRequest {
                connection,
                subscription_id,
                client_id,
                query,
                query_graph: &graph,
                user_ctx,
                schema: schema.clone(),
                resume_lsn,
                compiled_plan,
                prerun_initial,
            },
            wal,
        )
        .map_err(|err| {
            // If we attached to the host via `cached_view` but
            // `router.subscribe` then failed, detach so we don't leak
            // a phantom subscriber on the shared plan (which would
            // keep the cursor pump alive forever).
            if let Some(key) = canonical_key.as_ref() {
                if response_attached_via_cache(&err) {
                    host.release(key, subscription_id.get());
                }
            }
            SubscribeBlockingError::Router(err)
        })?;

    // Cold path: if there's no cached view but we have a host-routable
    // plan, do the seed now. `seed_updates` carries the rows we just
    // shipped as `Initial`; consume them into the host so its
    // `last_output` matches the client's view, then subsequent WAL
    // diffs flow through `apply_and_fanout` to produce aggregate
    // deltas.
    if let (Some(plan), Some(key)) = (compiled_plan_for_host.as_ref(), canonical_key.as_ref()) {
        // The cached path set `seed_updates` to empty in router.subscribe,
        // so this only runs on the cold path.
        if !response.seed_updates.is_empty() {
            let mut grouped: std::collections::HashMap<
                palimpsest_wal::TableId,
                Vec<palimpsest_dataflow::palimpsest::Row>,
            > = std::collections::HashMap::new();
            let seed_updates = std::mem::take(&mut response.seed_updates);
            for update in seed_updates {
                if update.diff > 0 {
                    grouped.entry(update.table).or_default().push(update.row);
                }
            }
            host.register_or_seed(
                key,
                plan,
                grouped,
                response.snapshot_lsn,
                subscription_id.get(),
            );
        }
    }

    Ok(SubscribeOutcome {
        response,
        schema,
        compiled_plan_for_host,
        host_canonical: canonical_key,
    })
}

/// Whether the router error happened *after* we'd already attached to
/// the host via `cached_view`. Today the only post-attach failure mode
/// is `ChannelSaturated` from `router.subscribe` (everything before
/// the channel send is cheap/synchronous); detect that and detach.
const fn response_attached_via_cache(err: &RouterError) -> bool {
    matches!(err, RouterError::ChannelSaturated)
}

/// Idempotently spawn a cursor pump for the canonical key.
///
/// If a pump for `canonical` already exists in the registry (and is
/// still running), do nothing — the new subscriber is already in the
/// host's subscribers list and will be picked up by the next
/// `apply_and_fanout`. Otherwise spawn a fresh pump anchored at
/// `from_lsn` (the snapshot LSN we just shipped as the Initial) and
/// register its handle.
///
/// A "finished" handle in the registry (pump exited because subscribers
/// went to zero) is treated as missing — we abort + replace it.
#[allow(clippy::too_many_arguments)]
fn ensure_canonical_pump(
    pump_registry: &CanonicalPumpRegistry,
    canonical: String,
    query: QueryId,
    from_lsn: Lsn,
    primary_key: Vec<usize>,
    plan: palimpsest_dataflow::palimpsest::CompiledPlan,
    router: Arc<SubscriptionRouter>,
    wal: Arc<dyn WalRuntime>,
    host: Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
) {
    let mut inner = pump_registry.inner.lock().expect("pump registry");
    let needs_spawn = match inner.get(&canonical) {
        None => true,
        Some(handle) => handle.is_finished(),
    };
    if !needs_spawn {
        return;
    }
    if let Some(old) = inner.remove(&canonical) {
        old.abort();
    }
    let handle = spawn_canonical_pump(
        canonical.clone(),
        query,
        from_lsn,
        primary_key,
        plan,
        router,
        wal,
        host,
    );
    inner.insert(canonical, handle);
}

/// One cursor pump per canonical key, fanning aggregate deltas out to
/// every subscriber currently attached to the corresponding
/// `PersistentHost` plan.
///
/// Exit conditions:
///   * `host.subscribers(canonical)` returns `None` — the plan was
///     released by the last subscriber. Remove ourselves from the
///     registry and return.
///   * `apply_and_fanout` returns `None` for the same reason mid-tick.
///     Same exit logic.
///
/// Backpressure: per-subscriber `BoundedDiffChannel` is unchanged.
/// A `ChannelSaturated` error from `router.pump_transaction` means the
/// router already emitted a `Resync` for that subscriber; we drop the
/// delta for it and continue serving the others.
#[allow(clippy::too_many_arguments)]
fn spawn_canonical_pump(
    canonical: String,
    query: QueryId,
    from_lsn: Lsn,
    primary_key: Vec<usize>,
    plan: palimpsest_dataflow::palimpsest::CompiledPlan,
    router: Arc<SubscriptionRouter>,
    wal: Arc<dyn WalRuntime>,
    host: Arc<palimpsest_dataflow::palimpsest::PersistentHost>,
) -> tokio::task::JoinHandle<()> {
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

    let table_id = plan.inputs.first().copied().expect("single-table plan");

    tokio::spawn(async move {
        let mut cursor = match wal.open_cursor(&query, from_lsn) {
            Ok(c) => c,
            Err(err) => {
                warn!(%err, canonical, "open_cursor failed; live diffs disabled for canonical plan");
                return;
            }
        };
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tick.tick().await;
            if host.subscribers(&canonical).is_none() {
                debug!(canonical, "canonical pump: plan released, exiting");
                return;
            }
            while let Some(transaction) = cursor.next_transaction() {
                let diffs: Vec<(
                    palimpsest_wal::TableId,
                    palimpsest_dataflow::palimpsest::Row,
                    isize,
                )> = transaction
                    .diffs
                    .iter()
                    .map(|d| (table_id, d.row.clone(), d.diff as isize))
                    .collect();
                let (deltas, subscribers) =
                    match host.apply_and_fanout(&canonical, diffs, transaction.commit_lsn) {
                        Some(out) => out,
                        None => {
                            debug!(canonical, "canonical pump: plan released mid-tick, exiting");
                            return;
                        }
                    };
                if deltas.is_empty() {
                    continue;
                }
                let to_pump = QueryTransactionDelta {
                    transaction_id: transaction.transaction_id,
                    begin_lsn: transaction.begin_lsn,
                    commit_lsn: transaction.commit_lsn,
                    end_lsn: transaction.end_lsn,
                    diffs: deltas
                        .into_iter()
                        .map(|d| RawDiff {
                            row: d.row,
                            lsn: d.lsn,
                            diff: i64::from(d.diff as i32),
                        })
                        .collect(),
                };
                // Fan out to every currently attached subscriber.
                // Clone the delta per subscriber (the aggregate is
                // small — top-K rows — so this is cheap).
                for sub_raw in subscribers {
                    let sub_id = SubscriptionId::new(sub_raw);
                    match router.pump_transaction(sub_id, to_pump.clone(), &primary_key) {
                        Ok(()) => {}
                        Err(RouterError::UnknownSubscription(_)) => {
                            // Subscriber unsubscribed between
                            // `apply_and_fanout` returning the list
                            // and this point. Skip.
                        }
                        Err(RouterError::ChannelSaturated) => {
                            // Router emitted Resync for this sub;
                            // continue serving the others.
                        }
                        Err(err) => {
                            warn!(
                                sub = sub_id.get(),
                                ?err,
                                "canonical pump: pump_transaction failed"
                            );
                        }
                    }
                }
            }
        }
    })
}

/// Per-subscription pump for plans the persistent host doesn't handle
/// (multi-table, unlowered). Pushes raw WAL diffs straight to the
/// router. Same lifecycle and backpressure semantics as the pre-Option-C
/// pump.
fn spawn_legacy_pump(
    sub_id: SubscriptionId,
    query: QueryId,
    from_lsn: Lsn,
    primary_key: Vec<usize>,
    router: Arc<SubscriptionRouter>,
    wal: Arc<dyn WalRuntime>,
) -> tokio::task::JoinHandle<()> {
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

    tokio::spawn(async move {
        let mut cursor = match wal.open_cursor(&query, from_lsn) {
            Ok(c) => c,
            Err(err) => {
                warn!(sub = sub_id.get(), %err, "open_cursor failed; live diffs disabled");
                return;
            }
        };
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tick.tick().await;
            while let Some(transaction) = cursor.next_transaction() {
                if transaction.diffs.is_empty() {
                    continue;
                }
                match router.pump_transaction(sub_id, transaction, &primary_key) {
                    Ok(()) => {}
                    Err(RouterError::UnknownSubscription(_)) => {
                        debug!(
                            sub = sub_id.get(),
                            "legacy pump: subscription gone, exiting"
                        );
                        return;
                    }
                    Err(RouterError::ChannelSaturated) => {
                        debug!(
                            sub = sub_id.get(),
                            "legacy pump: channel saturated, batch dropped after resync",
                        );
                    }
                    Err(err) => {
                        warn!(sub = sub_id.get(), ?err, "legacy pump: pump_batch failed");
                        return;
                    }
                }
            }
        }
    })
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
    state: &Arc<Mutex<ConnectionState>>,
    limiter: &ConnectionLimiter,
    host: &palimpsest_dataflow::palimpsest::PersistentHost,
) -> Result<(), ChannelClosed> {
    let lookup: Option<(SubscriptionId, Option<String>)> = {
        let mut guard = state.lock().expect("connection state lock");
        let server_id = guard.by_client_id.remove(&request.subscription_id);
        let canonical = server_id.and_then(|sid| guard.host_canonicals.remove(&sid));
        server_id.map(|sid| (sid, canonical))
    };
    let Some((server_id, host_canonical)) = lookup else {
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
    // Detach from the shared host plan (if any). When the last
    // subscriber on a canonical key leaves, the host drops the plan;
    // the canonical pump notices `host.subscribers(...) == None` on its
    // next tick and exits.
    if let Some(canonical) = host_canonical {
        host.release(&canonical, server_id.get());
    }
    limiter.release();
    Ok(())
}

async fn handle_ack(
    request: proto::AckRequest,
    outbound: &mpsc::Sender<Result<proto::ServerMessage, Status>>,
    router: &Arc<SubscriptionRouter>,
    state: &Arc<Mutex<ConnectionState>>,
) -> Result<(), ChannelClosed> {
    let server_id = state
        .lock()
        .expect("connection state lock")
        .by_client_id
        .get(&request.subscription_id)
        .copied();
    let Some(server_id) = server_id else {
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
            if let Some(proto::server_message::Kind::TransactionUpdate(update)) = &message.kind {
                let bytes = update
                    .changes
                    .iter()
                    .map(|change| {
                        change.old_row.as_ref().map_or(0, Vec::len)
                            + change.new_row.as_ref().map_or(0, Vec::len)
                    })
                    .sum::<usize>();
                metrics.record_bytes_sent(bytes as u64);
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
        DiffEvent::TransactionUpdate {
            transaction_id,
            begin_lsn,
            commit_lsn,
            end_lsn,
            changes,
        } => {
            let changes = encode_transaction_changes(changes)?;
            proto::server_message::Kind::TransactionUpdate(proto::TransactionUpdate {
                subscription_id: client_subscription_id.to_owned(),
                commit_lsn: commit_lsn.get(),
                begin_lsn: begin_lsn.map(Lsn::get),
                end_lsn: end_lsn.map(Lsn::get),
                transaction_id: *transaction_id,
                schema_id: schema_id.get(),
                chunk_index: 0,
                chunk_count: 1,
                changes,
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

fn encode_transaction_changes(changes: &[RowChange]) -> Option<Vec<proto::RowChange>> {
    changes
        .iter()
        .map(|change| {
            let old_row = match change.old.as_ref().map(encode_row).transpose() {
                Ok(row) => row,
                Err(err) => {
                    log_codec_error(&err);
                    return None;
                }
            };
            let new_row = match change.new.as_ref().map(encode_row).transpose() {
                Ok(row) => row,
                Err(err) => {
                    log_codec_error(&err);
                    return None;
                }
            };
            Some(proto::RowChange {
                op: diff_op_to_proto(change.op).into(),
                old_row,
                new_row,
            })
        })
        .collect()
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
