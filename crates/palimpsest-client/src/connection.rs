// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Connection manager for the bidi `Subscribe` RPC.
//!
//! There is exactly one [`ConnectionTask`] per [`crate::Client`]. It
//! owns the gRPC stream, multiplexes every subscription onto it, and
//! handles reconnect-with-backoff transparently to the user-facing
//! [`crate::Subscription`] handles.
//!
//! Why a single shared stream rather than one per subscription?
//! The server expects exactly that shape — `subscribe(stream
//! ClientMessage) -> stream ServerMessage` — and binding subscription
//! lifetimes to a single connection lets us reason about reconnect and
//! `resume_lsn` in one place.

#![allow(
    clippy::redundant_pub_crate,
    // The wasm transport uses `js-sys` types that aren't `Send`; the
    // `run`/`run_once` futures inherit that, but they're scheduled
    // via `wasm_bindgen_futures::spawn_local` which doesn't need it.
    clippy::future_not_send,
)]

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use palimpsest_proto::palimpsest::sync::v1::{self as proto, ClientMessage, ServerMessage};
use palimpsest_proto::wire::SchemaRegistry;

use crate::auth::Auth;
use crate::cache::LocalCache;
use crate::error::ClientError;
use crate::reconnect::{Backoff, BackoffConfig};
use crate::runtime::{self, TaskHandle};
use crate::subscription::DiffEvent;
use crate::transport::{self, Endpoint, OpenError};

/// Bounded depth for the user → manager command channel.
const COMMAND_CAPACITY: usize = 256;
/// Bounded depth for the manager → wire outbound channel (one per
/// active connection attempt).
const OUTBOUND_CAPACITY: usize = 256;

/// Commands accepted by the connection manager.
pub(crate) enum Command {
    Subscribe {
        subscription_id: String,
        sql: String,
        vars: HashMap<String, proto::VarValue>,
        events_tx: mpsc::Sender<Result<DiffEvent, ClientError>>,
        cache: Option<Arc<Mutex<LocalCache>>>,
    },
    Update {
        subscription_id: String,
        vars: HashMap<String, proto::VarValue>,
    },
    Unsubscribe {
        subscription_id: String,
    },
    Ack {
        subscription_id: String,
        lsn: u64,
    },
    Shutdown,
}

/// Cheap-to-clone handle that pushes [`Command`]s into the manager.
#[derive(Clone)]
pub(crate) struct ConnectionInbox {
    tx: mpsc::Sender<Command>,
}

impl ConnectionInbox {
    pub(crate) async fn send(&self, cmd: Command) -> Result<(), ClientError> {
        self.tx
            .send(cmd)
            .await
            .map_err(|_| ClientError::ConnectionClosed)
    }
}

/// State the manager keeps per active subscription. Survives reconnects.
struct SubState {
    sql: String,
    vars: HashMap<String, proto::VarValue>,
    events_tx: mpsc::Sender<Result<DiffEvent, ClientError>>,
    cache: Option<Arc<Mutex<LocalCache>>>,
    /// Schema id and Schema, learned from the most recent `Accepted`.
    /// `None` until the first acceptance.
    schema_id: Option<u64>,
    /// Highest LSN yielded to the user — used for dedupe.
    last_seen_lsn: u64,
    /// Highest LSN explicitly acked by the user — used as `resume_lsn`
    /// after reconnect.
    last_acked_lsn: Option<u64>,
}

/// Owns the bidi stream and reconnect loop.
pub(crate) struct ConnectionTask {
    endpoint: Endpoint,
    auth: Auth,
    backoff: BackoffConfig,
    commands_rx: mpsc::Receiver<Command>,
    state: HashMap<String, SubState>,
    registry: SchemaRegistry,
}

impl ConnectionTask {
    /// Start the manager. Returns `(inbox, join_handle)`.
    pub(crate) fn spawn(
        endpoint: Endpoint,
        auth: Auth,
        backoff: BackoffConfig,
    ) -> (ConnectionInbox, TaskHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CAPACITY);
        let task = Self {
            endpoint,
            auth,
            backoff,
            commands_rx: cmd_rx,
            state: HashMap::new(),
            registry: SchemaRegistry::new(),
        };
        let join = runtime::spawn(task.run());
        (ConnectionInbox { tx: cmd_tx }, join)
    }

    async fn run(mut self) {
        let mut backoff = self.backoff.schedule();
        loop {
            match self.run_once(&mut backoff).await {
                RunOutcome::Shutdown => {
                    self.fail_all(&ClientError::ConnectionClosed);
                    self.drain_pending_subscribes().await;
                    break;
                }
                RunOutcome::Reconnect => {
                    let delay = backoff.next_delay();
                    debug!(delay_ms = delay.as_millis(), "reconnecting");
                    runtime::sleep(delay).await;
                }
            }
        }
    }

    /// Reply to any `Subscribe` commands still queued in `commands_rx`
    /// at shutdown time. Without this, callers whose subscribe ran
    /// against a doomed handshake (e.g. `Unauthenticated`) would never
    /// learn the connection died — their `events_rx` would just return
    /// `None`.
    async fn drain_pending_subscribes(&mut self) {
        while let Ok(cmd) = self.commands_rx.try_recv() {
            if let Command::Subscribe { events_tx, .. } = cmd {
                let _ = events_tx.send(Err(ClientError::ConnectionClosed)).await;
            }
        }
    }

    /// One connection attempt: open the transport, drive the bidi
    /// stream until either the user shuts down or transport drops.
    /// Returns whether the outer loop should reconnect or exit.
    #[allow(clippy::significant_drop_tightening)]
    async fn run_once(&mut self, backoff: &mut Backoff) -> RunOutcome {
        let (outbound_tx, outbound_rx) = mpsc::channel::<ClientMessage>(OUTBOUND_CAPACITY);
        let mut inbound = match transport::open_subscribe(
            &self.endpoint,
            &self.auth,
            outbound_rx,
        )
        .await
        {
            Ok(rx) => rx,
            Err(OpenError::Auth(status)) => {
                self.fail_all(&ClientError::Grpc(status));
                return RunOutcome::Shutdown;
            }
            Err(OpenError::Transient) => {
                return RunOutcome::Reconnect;
            }
        };
        backoff.reset(self.backoff.initial);
        info!("connected; resubscribing {} subs", self.state.len());

        for (sub_id, sub) in &self.state {
            let resume_lsn = sub.last_acked_lsn;
            if outbound_tx
                .send(client_subscribe(sub_id, sub, resume_lsn))
                .await
                .is_err()
            {
                return RunOutcome::Reconnect;
            }
        }

        loop {
            tokio::select! {
                cmd = self.commands_rx.recv() => {
                    let Some(cmd) = cmd else {
                        return RunOutcome::Shutdown;
                    };
                    if matches!(cmd, Command::Shutdown) {
                        return RunOutcome::Shutdown;
                    }
                    if !self.handle_command(cmd, &outbound_tx).await {
                        return RunOutcome::Reconnect;
                    }
                }
                msg = inbound.recv() => {
                    match msg {
                        Some(Ok(server_msg)) => self.dispatch_server(server_msg).await,
                        Some(Err(status)) => {
                            // Auth failures should NOT trigger
                            // exponential-backoff reconnects — the
                            // token isn't going to become valid by
                            // itself, and the wasm transport would
                            // otherwise hammer the bridge forever.
                            // Tear all subscriptions down with the
                            // auth status so callers see the cause.
                            if matches!(
                                status.code(),
                                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
                            ) {
                                warn!(?status, "stream auth failure; shutting down");
                                self.fail_all(&ClientError::Grpc(status));
                                return RunOutcome::Shutdown;
                            }
                            warn!(?status, "stream error; reconnecting");
                            return RunOutcome::Reconnect;
                        }
                        None => {
                            warn!("server closed the stream; reconnecting");
                            return RunOutcome::Reconnect;
                        }
                    }
                }
            }
        }
    }

    /// Apply a user command. Returns `false` if the outbound stream is
    /// dead (caller will reconnect).
    async fn handle_command(
        &mut self,
        cmd: Command,
        outbound_tx: &mpsc::Sender<ClientMessage>,
    ) -> bool {
        match cmd {
            Command::Shutdown => true,
            Command::Subscribe {
                subscription_id,
                sql,
                vars,
                events_tx,
                cache,
            } => {
                let sub = SubState {
                    sql: sql.clone(),
                    vars: vars.clone(),
                    events_tx,
                    cache,
                    schema_id: None,
                    last_seen_lsn: 0,
                    last_acked_lsn: None,
                };
                let request = client_subscribe(&subscription_id, &sub, None);
                self.state.insert(subscription_id, sub);
                outbound_tx.send(request).await.is_ok()
            }
            Command::Update {
                subscription_id,
                vars,
            } => {
                if let Some(sub) = self.state.get_mut(&subscription_id) {
                    sub.vars.clone_from(&vars);
                }
                let msg = ClientMessage {
                    kind: Some(proto::client_message::Kind::Update(proto::UpdateRequest {
                        subscription_id,
                        vars,
                    })),
                };
                outbound_tx.send(msg).await.is_ok()
            }
            Command::Unsubscribe { subscription_id } => {
                if let Some(sub) = self.state.remove(&subscription_id) {
                    if let Some(schema_id) = sub.schema_id {
                        self.registry.remove(schema_id);
                    }
                }
                let msg = ClientMessage {
                    kind: Some(proto::client_message::Kind::Unsubscribe(
                        proto::UnsubscribeRequest {
                            subscription_id: subscription_id.clone(),
                        },
                    )),
                };
                outbound_tx.send(msg).await.is_ok()
            }
            Command::Ack {
                subscription_id,
                lsn,
            } => {
                if let Some(sub) = self.state.get_mut(&subscription_id) {
                    sub.last_acked_lsn = Some(sub.last_acked_lsn.map_or(lsn, |prev| prev.max(lsn)));
                }
                let msg = ClientMessage {
                    kind: Some(proto::client_message::Kind::Ack(proto::AckRequest {
                        subscription_id,
                        lsn,
                    })),
                };
                outbound_tx.send(msg).await.is_ok()
            }
        }
    }

    /// Translate a `ServerMessage` into a `DiffEvent` and forward it to
    /// the matching subscription's fanout channel, applying LSN dedupe
    /// and cache as we go.
    async fn dispatch_server(&mut self, msg: ServerMessage) {
        let Some(kind) = msg.kind else {
            return;
        };
        match kind {
            proto::server_message::Kind::Accepted(accepted) => self.handle_accepted(accepted).await,
            proto::server_message::Kind::Diff(diff) => self.handle_diff(diff).await,
            proto::server_message::Kind::Resync(resync) => self.handle_resync(resync).await,
            proto::server_message::Kind::Error(err) => self.handle_error(err).await,
        }
    }

    async fn handle_accepted(&mut self, accepted: proto::Accepted) {
        let proto::Accepted {
            subscription_id,
            schema_id,
            snapshot_lsn,
            schema,
        } = accepted;
        let Some(schema) = schema else {
            self.fail(&subscription_id, ClientError::EmptyServerMessage)
                .await;
            return;
        };
        self.registry.register(schema_id, schema.clone());
        let Some(sub) = self.state.get_mut(&subscription_id) else {
            return;
        };
        sub.schema_id = Some(schema_id);
        if let Some(cache) = sub.cache.as_ref() {
            // Fresh schema → reset cache to a clean view.
            *cache.lock().await = LocalCache::for_schema(&schema);
        }
        let _ = sub
            .events_tx
            .send(Ok(DiffEvent::Accepted {
                schema_id,
                snapshot_lsn,
                schema,
            }))
            .await;
    }

    async fn handle_diff(&mut self, diff: proto::Diff) {
        let subscription_id = diff.subscription_id.clone();
        let Some(sub) = self.state.get_mut(&subscription_id) else {
            return;
        };
        if diff.lsn <= sub.last_seen_lsn && sub.last_seen_lsn > 0 {
            debug!(
                sub = %subscription_id,
                lsn = diff.lsn,
                last_seen = sub.last_seen_lsn,
                "dropping replayed diff"
            );
            return;
        }
        let rows = match self.registry.decode(&diff) {
            Ok(rows) => rows,
            Err(err) => {
                let _ = sub.events_tx.send(Err(ClientError::Codec(err))).await;
                return;
            }
        };
        let op = proto::DiffOp::try_from(diff.op).unwrap_or(proto::DiffOp::Unspecified);
        if let Some(cache) = sub.cache.as_ref() {
            cache.lock().await.apply(op, &rows);
        }
        sub.last_seen_lsn = diff.lsn;
        let _ = sub
            .events_tx
            .send(Ok(DiffEvent::Diff {
                lsn: diff.lsn,
                op,
                rows,
            }))
            .await;
    }

    async fn handle_resync(&mut self, resync: proto::Resync) {
        let proto::Resync {
            subscription_id,
            reason,
            message,
        } = resync;
        let Some(sub) = self.state.get_mut(&subscription_id) else {
            return;
        };
        let reason =
            proto::ResyncReason::try_from(reason).unwrap_or(proto::ResyncReason::Unspecified);
        sub.last_seen_lsn = 0;
        if let Some(cache) = sub.cache.as_ref() {
            *cache.lock().await = LocalCache::default();
        }
        let _ = sub
            .events_tx
            .send(Ok(DiffEvent::Resync { reason, message }))
            .await;
    }

    async fn handle_error(&self, err: proto::Error) {
        let proto::Error {
            subscription_id,
            code,
            message,
        } = err;
        if let Some(sub) = self.state.get(&subscription_id) {
            let _ = sub
                .events_tx
                .send(Ok(DiffEvent::Error { code, message }))
                .await;
        }
    }

    async fn fail(&self, subscription_id: &str, err: ClientError) {
        if let Some(sub) = self.state.get(subscription_id) {
            let _ = sub.events_tx.send(Err(err)).await;
        }
    }

    fn fail_all(&mut self, err: &ClientError) {
        for sub in self.state.values() {
            let tx = sub.events_tx.clone();
            let _ = tx.try_send(Err(err_clone(err)));
        }
        self.state.clear();
    }
}

/// Best-effort clone of a `ClientError`. `Transport`, `Grpc`, and
/// `Codec` aren't trivially `Clone`, so they collapse to
/// `ConnectionClosed` — the user just needs *some* signal that the
/// stream is dead.
fn err_clone(err: &ClientError) -> ClientError {
    match err {
        ClientError::Endpoint(s) => ClientError::Endpoint(s.clone()),
        ClientError::EmptyServerMessage => ClientError::EmptyServerMessage,
        ClientError::UnaccceptedSubscription(s) => ClientError::UnaccceptedSubscription(s.clone()),
        ClientError::InvalidRequest(s) => ClientError::InvalidRequest(s.clone()),
        _ => ClientError::ConnectionClosed,
    }
}

fn client_subscribe(
    subscription_id: &str,
    sub: &SubState,
    resume_lsn: Option<u64>,
) -> ClientMessage {
    ClientMessage {
        kind: Some(proto::client_message::Kind::Subscribe(
            proto::SubscribeRequest {
                client_subscription_id: subscription_id.to_owned(),
                sql: sub.sql.clone(),
                vars: sub.vars.clone(),
                resume_lsn,
            },
        )),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RunOutcome {
    Reconnect,
    Shutdown,
}

/// Submit a `Shutdown` to the inbox, ignoring delivery failure (the
/// task may already be shutting down).
pub(crate) async fn request_shutdown(inbox: &ConnectionInbox) {
    let _ = inbox.send(Command::Shutdown).await;
}
