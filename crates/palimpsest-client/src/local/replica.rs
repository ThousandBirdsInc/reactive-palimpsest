// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! [`LocalReplica`] — an optimistic, local-first clone of the remote
//! database's permissioned subset.
//!
//! Each mirror is an ordinary Palimpsest subscription (raw SQL, a
//! whole table, or a server-registered named query), so the server's
//! permission rewriter decides exactly which rows stream down: the
//! local database only ever holds the subset the authenticated user is
//! allowed to see, and revocations arrive as retractions like any
//! other diff.
//!
//! Per mirror, the replica:
//! 1. creates the local table from the wire schema on `Accepted`,
//! 2. installs the snapshot on the first `Initial` diff,
//! 3. applies every subsequent remote commit as **one atomic local
//!    batch** (readers never observe a torn transaction),
//! 4. acks each commit LSN so reconnects resume from the last applied
//!    position instead of re-snapshotting.
//!
//! Optimistic writes go through [`LocalReplica::mutate`]: the change is
//! applied to the local database immediately, forwarded to the
//! application's [`RemoteWriter`], and reconciled when the authoritative
//! change comes back through the WAL (see [`super::reconcile`]).

// `MirrorConfig`/`MirrorQuery` are crate-internal wiring shared with
// the in-crate tests; the module itself is private.
#![allow(clippy::redundant_pub_crate)]
// On wasm the local-database futures (JS driver calls) are not `Send`;
// they run on the single-threaded JS event loop via `spawn_local`, the
// same split `connection.rs` documents.
#![allow(clippy::future_not_send)]
// On wasm `Arc<Shared>` wraps JS-driver trait objects that are neither
// `Send` nor `Sync`; the replica never leaves the event loop there.
#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{mpsc, watch, Mutex};
use tracing::{debug, warn};

use palimpsest_proto::palimpsest::sync::v1::{DiffOp, VarValue};
use palimpsest_proto::wire::{WireDatum, WireRow, WireRowChange};

use crate::cache::PrimaryKey;
use crate::connection::QuerySpec;
use crate::error::ClientError;
use crate::runtime::{self, TaskHandle};
use crate::subscription::{DiffEvent, Subscription};
use crate::Client;

use super::db::{DbFuture, LocalDatabase, LocalDbError, MaybeSendSync, TableSpec};
use super::reconcile::{Reconciler, ResolvedKind};

pub use super::reconcile::MutationToken;

/// Bounded depth of the replica → user event channel. Events are
/// advisory notifications (the authoritative state lives in the local
/// database); when the consumer lags, the oldest notifications are
/// dropped rather than stalling sync.
const EVENT_CAPACITY: usize = 1024;

/// An optimistic mutation, expressed against named columns of a
/// mirrored table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// Insert a new row. Columns not listed are staged as SQL `NULL`
    /// locally; the authoritative row (with server-side defaults and
    /// trigger effects) replaces the optimistic one when the write
    /// round-trips through the WAL.
    Insert {
        /// Mirrored table name.
        table: String,
        /// Provided column values.
        values: Vec<(String, WireDatum)>,
    },
    /// Update columns of an existing row identified by its primary key.
    Update {
        /// Mirrored table name.
        table: String,
        /// Primary-key column values.
        key: Vec<(String, WireDatum)>,
        /// Columns to change.
        set: Vec<(String, WireDatum)>,
    },
    /// Delete the row identified by its primary key.
    Delete {
        /// Mirrored table name.
        table: String,
        /// Primary-key column values.
        key: Vec<(String, WireDatum)>,
    },
}

impl Mutation {
    /// Build an insert.
    pub fn insert<T, K>(table: T, values: impl IntoIterator<Item = (K, WireDatum)>) -> Self
    where
        T: Into<String>,
        K: Into<String>,
    {
        Self::Insert {
            table: table.into(),
            values: values.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    /// Build an update.
    pub fn update<T, K, S>(
        table: T,
        key: impl IntoIterator<Item = (K, WireDatum)>,
        set: impl IntoIterator<Item = (S, WireDatum)>,
    ) -> Self
    where
        T: Into<String>,
        K: Into<String>,
        S: Into<String>,
    {
        Self::Update {
            table: table.into(),
            key: key.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            set: set.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    /// Build a delete.
    pub fn delete<T, K>(table: T, key: impl IntoIterator<Item = (K, WireDatum)>) -> Self
    where
        T: Into<String>,
        K: Into<String>,
    {
        Self::Delete {
            table: table.into(),
            key: key.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    /// The table this mutation targets.
    #[must_use]
    pub fn table(&self) -> &str {
        match self {
            Self::Insert { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }
}

/// One optimistic write handed to the application's [`RemoteWriter`].
#[derive(Debug, Clone)]
pub struct WriteRequest {
    /// Token identifying the mutation in subsequent [`ReplicaEvent`]s.
    pub token: MutationToken,
    /// The user-level mutation, exactly as passed to
    /// [`LocalReplica::mutate`].
    pub mutation: Mutation,
}

/// The application's write path.
///
/// Palimpsest never writes to Postgres itself — the writer forwards
/// the mutation through whatever channel the app already trusts (an
/// HTTP API, a direct SQL connection, an RPC). The engine observes the
/// resulting WAL change and settles the optimistic overlay when it
/// streams back.
pub trait RemoteWriter: MaybeSendSync {
    /// Perform the write remotely. `Err` rolls the optimistic change
    /// back locally.
    fn write(&self, request: WriteRequest) -> DbFuture<'_, Result<(), String>>;
}

/// Failures from [`LocalReplica::mutate`].
#[derive(Debug, Error)]
pub enum MutateError {
    /// The replica was built without a [`RemoteWriter`].
    #[error("no remote writer configured; local-first mutations need a write path")]
    NoWriter,
    /// The table isn't mirrored, or its schema hasn't arrived yet.
    #[error("table `{0}` is not mirrored or not yet synced")]
    TableNotReady(String),
    /// A referenced column doesn't exist in the mirror's schema.
    #[error("unknown column `{0}`")]
    UnknownColumn(String),
    /// A primary-key column wasn't provided.
    #[error("mutation must provide primary-key column `{0}`")]
    MissingKeyColumn(String),
    /// Updates may not change primary-key columns.
    #[error("cannot update primary-key column `{0}`; delete and re-insert instead")]
    KeyColumnUpdate(String),
    /// The local database rejected the optimistic write.
    #[error("local database: {0}")]
    Db(#[from] LocalDbError),
}

/// Sync lifecycle of one mirrored table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableSyncState {
    /// Waiting for the server to accept the mirror subscription.
    Connecting,
    /// Accepted; waiting for (or installing) the initial snapshot.
    Snapshotting,
    /// Snapshot installed; streaming incremental commits.
    Live {
        /// Last applied (and acked) LSN.
        lsn: u64,
    },
    /// The server forced a resync; a fresh snapshot is on its way.
    Resyncing,
    /// The mirror hit an error and is not applying updates.
    Errored {
        /// Human-readable cause.
        message: String,
    },
}

/// Notifications emitted by the replica. Advisory — the local database
/// is the authoritative state; a lagging consumer loses notifications,
/// not data.
#[derive(Debug, Clone)]
pub enum ReplicaEvent {
    /// A mirror changed lifecycle state.
    TableState {
        /// Mirrored table.
        table: String,
        /// New state.
        state: TableSyncState,
    },
    /// A batch of remote changes (or a snapshot) was applied locally.
    /// UIs typically re-run local queries on this signal.
    Applied {
        /// Mirrored table.
        table: String,
        /// Commit LSN of the applied batch.
        lsn: u64,
    },
    /// An optimistic mutation was confirmed by the WAL round-trip.
    MutationSettled {
        /// Token from [`LocalReplica::mutate`].
        token: MutationToken,
        /// Mirrored table.
        table: String,
    },
    /// The server's authoritative change disagreed with an already
    /// acknowledged optimistic mutation; the server version won.
    MutationConflicted {
        /// Token from [`LocalReplica::mutate`].
        token: MutationToken,
        /// Mirrored table.
        table: String,
    },
    /// The remote write failed; the optimistic change was rolled back.
    MutationFailed {
        /// Token from [`LocalReplica::mutate`].
        token: MutationToken,
        /// Mirrored table.
        table: String,
        /// Writer-supplied error.
        error: String,
    },
    /// The server rejected or errored the mirror subscription.
    MirrorError {
        /// Mirrored table.
        table: String,
        /// Server error code.
        code: String,
        /// Server error message.
        message: String,
    },
}

/// What one mirror subscribes to.
#[derive(Debug, Clone)]
pub(crate) struct MirrorConfig {
    pub(crate) local_table: String,
    pub(crate) query: MirrorQuery,
    pub(crate) vars: HashMap<String, VarValue>,
}

#[derive(Debug, Clone)]
pub(crate) enum MirrorQuery {
    Sql(String),
    Named(String),
}

/// Builder for [`LocalReplica`].
pub struct LocalReplicaBuilder {
    client: Client,
    db: Arc<dyn LocalDatabase>,
    writer: Option<Arc<dyn RemoteWriter>>,
    mirrors: Vec<MirrorConfig>,
}

impl LocalReplicaBuilder {
    /// Mirror the full permissioned subset of a base table
    /// (`SELECT * FROM <table>`, filtered by the server's permission
    /// rules before transmission).
    #[must_use]
    pub fn mirror_table(mut self, table: impl Into<String>) -> Self {
        let table = table.into();
        self.mirrors.push(MirrorConfig {
            query: MirrorQuery::Sql(format!("SELECT * FROM {table}")),
            local_table: table,
            vars: HashMap::new(),
        });
        self
    }

    /// Mirror an arbitrary SQL query's live result set into
    /// `local_table`.
    #[must_use]
    pub fn mirror_sql(mut self, local_table: impl Into<String>, sql: impl Into<String>) -> Self {
        self.mirrors.push(MirrorConfig {
            local_table: local_table.into(),
            query: MirrorQuery::Sql(sql.into()),
            vars: HashMap::new(),
        });
        self
    }

    /// Mirror a server-registered named prepared query. The local table
    /// takes the query's name; the client never holds SQL on this path.
    #[must_use]
    pub fn mirror_named(self, name: impl Into<String>) -> Self {
        self.mirror_named_with(name, HashMap::new())
    }

    /// Mirror a named prepared query with parameter bindings.
    #[must_use]
    pub fn mirror_named_with(
        mut self,
        name: impl Into<String>,
        params: HashMap<String, VarValue>,
    ) -> Self {
        let name = name.into();
        self.mirrors.push(MirrorConfig {
            local_table: name.clone(),
            query: MirrorQuery::Named(name),
            vars: params,
        });
        self
    }

    /// Attach the application's write path, enabling
    /// [`LocalReplica::mutate`].
    #[must_use]
    pub fn with_writer(mut self, writer: Arc<dyn RemoteWriter>) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Subscribe every mirror and start the sync pumps.
    ///
    /// # Errors
    /// [`ClientError::ConnectionClosed`] if the client has shut down.
    pub async fn start(self) -> Result<LocalReplica, ClientError> {
        let mut subs = Vec::with_capacity(self.mirrors.len());
        for mirror in self.mirrors {
            let query = match &mirror.query {
                MirrorQuery::Sql(sql) => QuerySpec::Sql(sql.clone()),
                MirrorQuery::Named(name) => QuerySpec::Named(name.clone()),
            };
            let sub = self
                .client
                .subscribe_spec_uncached(query, mirror.vars.clone())
                .await?;
            subs.push((mirror, sub));
        }
        Ok(LocalReplica::assemble(self.db, self.writer, subs))
    }
}

impl Client {
    /// Start building a [`LocalReplica`] backed by `db`.
    #[must_use]
    pub fn local_replica(&self, db: Arc<dyn LocalDatabase>) -> LocalReplicaBuilder {
        LocalReplicaBuilder {
            client: self.clone(),
            db,
            writer: None,
            mirrors: Vec::new(),
        }
    }
}

/// Map of mirrored table → sync state.
pub type TableStates = HashMap<String, TableSyncState>;

struct Inner {
    reconciler: Reconciler,
    specs: HashMap<String, Arc<TableSpec>>,
    next_token: MutationToken,
}

struct Shared {
    db: Arc<dyn LocalDatabase>,
    writer: Option<Arc<dyn RemoteWriter>>,
    inner: Mutex<Inner>,
    events_tx: mpsc::Sender<ReplicaEvent>,
    state_tx: watch::Sender<TableStates>,
}

impl Shared {
    /// Advisory event delivery — never blocks the sync path.
    fn emit(&self, event: ReplicaEvent) {
        if let Err(err) = self.events_tx.try_send(event) {
            debug!("dropping replica event: {err}");
        }
    }

    fn set_state(&self, table: &str, state: TableSyncState) {
        self.state_tx.send_modify(|states| {
            states.insert(table.to_owned(), state.clone());
        });
        self.emit(ReplicaEvent::TableState {
            table: table.to_owned(),
            state,
        });
    }
}

/// Handle to a running local-first replica. Cheap to clone.
#[derive(Clone)]
pub struct LocalReplica {
    shared: Arc<Shared>,
    events_rx: Arc<std::sync::Mutex<Option<mpsc::Receiver<ReplicaEvent>>>>,
    state_rx: watch::Receiver<TableStates>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    handles: Arc<Mutex<Vec<TaskHandle>>>,
}

impl LocalReplica {
    pub(crate) fn assemble(
        db: Arc<dyn LocalDatabase>,
        writer: Option<Arc<dyn RemoteWriter>>,
        subs: Vec<(MirrorConfig, Subscription)>,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::channel(EVENT_CAPACITY);
        let initial_states: TableStates = subs
            .iter()
            .map(|(m, _)| (m.local_table.clone(), TableSyncState::Connecting))
            .collect();
        let (state_tx, state_rx) = watch::channel(initial_states);
        let (shutdown_tx, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            db,
            writer,
            inner: Mutex::new(Inner {
                reconciler: Reconciler::default(),
                specs: HashMap::new(),
                next_token: 1,
            }),
            events_tx,
            state_tx,
        });
        let mut handles = Vec::with_capacity(subs.len());
        for (mirror, sub) in subs {
            let shared = shared.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            handles.push(runtime::spawn(pump(shared, sub, mirror, shutdown_rx)));
        }
        Self {
            shared,
            events_rx: Arc::new(std::sync::Mutex::new(Some(events_rx))),
            state_rx,
            shutdown_tx: Arc::new(shutdown_tx),
            handles: Arc::new(Mutex::new(handles)),
        }
    }

    /// Take the replica's event stream. Yields `None` after the first
    /// call — there is a single consumer.
    #[must_use]
    pub fn take_events(&self) -> Option<mpsc::Receiver<ReplicaEvent>> {
        self.events_rx
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }

    /// Watch per-table sync state transitions.
    #[must_use]
    pub fn watch_tables(&self) -> watch::Receiver<TableStates> {
        self.state_rx.clone()
    }

    /// Snapshot the current per-table sync state.
    #[must_use]
    pub fn table_states(&self) -> TableStates {
        self.state_rx.borrow().clone()
    }

    /// The local database backing this replica.
    #[must_use]
    pub fn db(&self) -> &Arc<dyn LocalDatabase> {
        &self.shared.db
    }

    /// Run a read-only SQL query against the local database — the same
    /// Postgres-dialect SQL the server accepts runs unchanged against
    /// the mirrored (permissioned) subset, with optimistic mutations
    /// visible.
    ///
    /// # Errors
    /// Whatever the backing [`LocalDatabase`] surfaces.
    pub async fn query(
        &self,
        sql: &str,
        params: Vec<WireDatum>,
    ) -> Result<Vec<WireRow>, LocalDbError> {
        self.shared.db.query(sql, params).await
    }

    /// Schema of a mirrored table, available once the server has
    /// accepted its mirror subscription.
    pub async fn table_spec(&self, table: &str) -> Option<TableSpec> {
        self.shared
            .inner
            .lock()
            .await
            .specs
            .get(table)
            .map(|spec| (**spec).clone())
    }

    /// Number of optimistic mutations not yet confirmed by the WAL
    /// round-trip for `table`.
    pub async fn pending_mutations(&self, table: &str) -> usize {
        self.shared
            .inner
            .lock()
            .await
            .reconciler
            .pending_count(table)
    }

    /// Apply an optimistic mutation: write locally now, forward to the
    /// [`RemoteWriter`], reconcile when the authoritative change comes
    /// back through the stream. The returned token identifies the
    /// mutation in [`ReplicaEvent`]s.
    ///
    /// # Errors
    /// See [`MutateError`].
    pub async fn mutate(&self, mutation: Mutation) -> Result<MutationToken, MutateError> {
        let writer = self.shared.writer.clone().ok_or(MutateError::NoWriter)?;
        let table = mutation.table().to_owned();
        let token;
        {
            let mut inner = self.shared.inner.lock().await;
            let spec = inner
                .specs
                .get(&table)
                .cloned()
                .ok_or_else(|| MutateError::TableNotReady(table.clone()))?;
            let (key, kind) = resolve_mutation(&spec, &mutation)?;
            // The visible row doubles as the authoritative base image
            // when this key has no other pending mutations; the lock is
            // held so no server batch can interleave.
            let current = self.shared.db.get_row(&spec, &key).await?;
            token = inner.next_token;
            inner.next_token += 1;
            let writes = inner.reconciler.stage(&table, token, key, kind, current);
            if let Err(err) = self.shared.db.apply_writes(&spec, writes).await {
                // Keep ledger and database consistent: un-stage.
                let _ = inner.reconciler.writer_finished(token, false);
                return Err(err.into());
            }
        }
        let shared = self.shared.clone();
        let request = WriteRequest { token, mutation };
        let _handle = runtime::spawn(async move {
            let result = writer.write(request).await;
            let (ok, error) = match result {
                Ok(()) => (true, String::new()),
                Err(err) => (false, err),
            };
            let mut inner = shared.inner.lock().await;
            let apply = inner.reconciler.writer_finished(token, ok);
            let Some(table) = apply.table else { return };
            if !apply.writes.is_empty() {
                if let Some(spec) = inner.specs.get(&table).cloned() {
                    if let Err(err) = shared.db.apply_writes(&spec, apply.writes).await {
                        warn!("rollback write failed for `{table}`: {err}");
                    }
                }
            }
            drop(inner);
            if !ok {
                shared.emit(ReplicaEvent::MutationFailed {
                    token,
                    table,
                    error,
                });
            }
        });
        Ok(token)
    }

    /// Stop every mirror pump (unsubscribing server-side) and wait for
    /// them to wind down. The underlying [`Client`] stays connected.
    pub async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        let handles: Vec<TaskHandle> = std::mem::take(&mut *self.handles.lock().await);
        for handle in handles {
            handle.join().await;
        }
    }
}

/// Resolve a named-column [`Mutation`] against the mirror's schema.
fn resolve_mutation(
    spec: &TableSpec,
    mutation: &Mutation,
) -> Result<(PrimaryKey, ResolvedKind), MutateError> {
    match mutation {
        Mutation::Insert { values, .. } => {
            let mut row: WireRow = vec![WireDatum::Null; spec.schema.columns.len()];
            let mut set_columns = Vec::with_capacity(values.len());
            for (name, value) in values {
                let idx = spec
                    .column_index(name)
                    .ok_or_else(|| MutateError::UnknownColumn(name.clone()))?;
                row[idx] = value.clone();
                set_columns.push(idx);
            }
            for pk_idx in spec.pk_indices() {
                if !set_columns.contains(&pk_idx) {
                    let name = spec
                        .schema
                        .columns
                        .get(pk_idx)
                        .map_or_else(String::new, |c| c.name.clone());
                    return Err(MutateError::MissingKeyColumn(name));
                }
            }
            let key = spec.key_of(&row);
            Ok((key, ResolvedKind::Insert { row, set_columns }))
        }
        Mutation::Update { key, set, .. } => {
            let pk = resolve_key(spec, key)?;
            let mut resolved_set = Vec::with_capacity(set.len());
            let pk_indices = spec.pk_indices();
            for (name, value) in set {
                let idx = spec
                    .column_index(name)
                    .ok_or_else(|| MutateError::UnknownColumn(name.clone()))?;
                if pk_indices.contains(&idx) {
                    return Err(MutateError::KeyColumnUpdate(name.clone()));
                }
                resolved_set.push((idx, value.clone()));
            }
            Ok((pk, ResolvedKind::Update { set: resolved_set }))
        }
        Mutation::Delete { key, .. } => {
            let pk = resolve_key(spec, key)?;
            Ok((pk, ResolvedKind::Delete))
        }
    }
}

/// Build the identity key from named key columns, in schema key order.
fn resolve_key(spec: &TableSpec, key: &[(String, WireDatum)]) -> Result<PrimaryKey, MutateError> {
    for (name, _) in key {
        if spec.column_index(name).is_none() {
            return Err(MutateError::UnknownColumn(name.clone()));
        }
    }
    spec.pk_indices()
        .into_iter()
        .map(|pk_idx| {
            let col_name = spec
                .schema
                .columns
                .get(pk_idx)
                .map_or("", |c| c.name.as_str());
            key.iter()
                .find(|(name, _)| name == col_name)
                .map(|(_, value)| value.clone())
                .ok_or_else(|| MutateError::MissingKeyColumn(col_name.to_owned()))
        })
        .collect()
}

/// Convert a legacy per-op diff into transaction-shaped row changes.
fn diff_to_changes(op: DiffOp, rows: Vec<WireRow>) -> Vec<WireRowChange> {
    rows.into_iter()
        .filter_map(|row| match op {
            DiffOp::Initial | DiffOp::Insert => Some(WireRowChange {
                op: DiffOp::Insert,
                old: None,
                new: Some(row),
            }),
            DiffOp::Update => Some(WireRowChange {
                op: DiffOp::Update,
                old: None,
                new: Some(row),
            }),
            DiffOp::Delete => Some(WireRowChange {
                op: DiffOp::Delete,
                old: Some(row),
                new: None,
            }),
            DiffOp::Unspecified => None,
        })
        .collect()
}

/// Per-mirror sync pump: consumes subscription events and applies them
/// to the local database through the reconciler.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
async fn pump(
    shared: Arc<Shared>,
    mut sub: Subscription,
    mirror: MirrorConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let table = mirror.local_table.clone();
    // A fresh snapshot is expected after every acceptance/resync; the
    // first `Initial` diff replaces the table, subsequent consecutive
    // `Initial` chunks append.
    let mut replace_on_initial = true;
    loop {
        let event = tokio::select! {
            event = sub.next_event() => event,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = sub.unsubscribe().await;
                    return;
                }
                continue;
            }
        };
        let Some(event) = event else {
            shared.set_state(
                &table,
                TableSyncState::Errored {
                    message: "connection closed".to_owned(),
                },
            );
            return;
        };
        match event {
            Ok(DiffEvent::Accepted { schema, .. }) => {
                let spec = Arc::new(TableSpec::new(table.clone(), schema));
                if let Err(err) = shared.db.ensure_table(&spec).await {
                    shared.set_state(
                        &table,
                        TableSyncState::Errored {
                            message: format!("creating local table: {err}"),
                        },
                    );
                    continue;
                }
                shared.inner.lock().await.specs.insert(table.clone(), spec);
                replace_on_initial = true;
                shared.set_state(&table, TableSyncState::Snapshotting);
            }
            Ok(DiffEvent::Diff { lsn, op, rows }) => {
                if op == DiffOp::Initial && replace_on_initial {
                    replace_on_initial = false;
                    let mut inner = shared.inner.lock().await;
                    let Some(spec) = inner.specs.get(&table).cloned() else {
                        continue;
                    };
                    let visible = inner
                        .reconciler
                        .apply_initial(&table, rows, |row| spec.key_of(row));
                    let applied = shared.db.replace_rows(&spec, visible).await;
                    drop(inner);
                    finish_batch(
                        &shared,
                        &sub,
                        &mirror,
                        &table,
                        lsn,
                        applied,
                        &mut replace_on_initial,
                    )
                    .await;
                    continue;
                }
                let changes = diff_to_changes(op, rows);
                apply_changes(
                    &shared,
                    &sub,
                    &mirror,
                    &table,
                    lsn,
                    changes,
                    &mut replace_on_initial,
                )
                .await;
            }
            Ok(DiffEvent::Transaction {
                commit_lsn,
                changes,
                ..
            }) => {
                apply_changes(
                    &shared,
                    &sub,
                    &mirror,
                    &table,
                    commit_lsn,
                    changes,
                    &mut replace_on_initial,
                )
                .await;
            }
            Ok(DiffEvent::Resync { reason, message }) => {
                debug!(
                    table = %table,
                    ?reason,
                    %message,
                    "mirror resync; requesting fresh snapshot"
                );
                replace_on_initial = true;
                shared.set_state(&table, TableSyncState::Resyncing);
                // Re-issuing the query (same vars) makes the server
                // replay a fresh Initial under the current rules —
                // including RESYNC_REASON_PERMISSIONS_CHANGED, where
                // rows a revoked grant covered get retracted by the
                // replacement snapshot.
                if sub.update(mirror.vars.clone()).await.is_err() {
                    shared.set_state(
                        &table,
                        TableSyncState::Errored {
                            message: "connection closed during resync".to_owned(),
                        },
                    );
                    return;
                }
            }
            Ok(DiffEvent::Error { code, message }) => {
                shared.set_state(
                    &table,
                    TableSyncState::Errored {
                        message: format!("{code}: {message}"),
                    },
                );
                shared.emit(ReplicaEvent::MirrorError {
                    table: table.clone(),
                    code,
                    message,
                });
            }
            Err(err) => {
                warn!(table = %table, "mirror stream error: {err}");
                shared.set_state(
                    &table,
                    TableSyncState::Errored {
                        message: err.to_string(),
                    },
                );
            }
        }
    }
}

/// Apply one batch of server row changes through the reconciler.
#[allow(clippy::future_not_send)]
async fn apply_changes(
    shared: &Arc<Shared>,
    sub: &Subscription,
    mirror: &MirrorConfig,
    table: &str,
    lsn: u64,
    changes: Vec<WireRowChange>,
    replace_on_initial: &mut bool,
) {
    let mut inner = shared.inner.lock().await;
    let Some(spec) = inner.specs.get(table).cloned() else {
        return;
    };
    let outcome = inner
        .reconciler
        .apply_server_changes(table, &changes, |row| spec.key_of(row));
    let applied = shared.db.apply_writes(&spec, outcome.writes).await;
    drop(inner);
    for token in outcome.settled {
        shared.emit(ReplicaEvent::MutationSettled {
            token,
            table: table.to_owned(),
        });
    }
    for token in outcome.conflicted {
        shared.emit(ReplicaEvent::MutationConflicted {
            token,
            table: table.to_owned(),
        });
    }
    finish_batch(shared, sub, mirror, table, lsn, applied, replace_on_initial).await;
}

/// Common tail of a batch application: ack on success, force a fresh
/// snapshot on local failure (the mirror can no longer be trusted to
/// be exactly the server subset).
#[allow(clippy::future_not_send)]
async fn finish_batch(
    shared: &Arc<Shared>,
    sub: &Subscription,
    mirror: &MirrorConfig,
    table: &str,
    lsn: u64,
    applied: Result<(), LocalDbError>,
    replace_on_initial: &mut bool,
) {
    match applied {
        Ok(()) => {
            shared.set_state(table, TableSyncState::Live { lsn });
            shared.emit(ReplicaEvent::Applied {
                table: table.to_owned(),
                lsn,
            });
            let _ = sub.ack(lsn).await;
        }
        Err(err) => {
            warn!(table = %table, "local apply failed, requesting resync: {err}");
            *replace_on_initial = true;
            shared.set_state(
                table,
                TableSyncState::Errored {
                    message: format!("local apply failed: {err}"),
                },
            );
            let _ = sub.update(mirror.vars.clone()).await;
        }
    }
}
