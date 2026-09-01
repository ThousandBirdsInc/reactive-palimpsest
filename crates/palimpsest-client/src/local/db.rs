// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Local-database abstraction the replica writes through.
//!
//! The replica itself never generates SQL or touches storage — it talks
//! to a [`LocalDatabase`]. Two implementations ship with the crate:
//!
//! * [`SqlLocalDatabase`](super::sql::SqlLocalDatabase) — adapts any
//!   Postgres-dialect embedded engine (a pgrust/pglite-style WASM build
//!   in the browser, or an embedded Postgres natively) via the
//!   [`SqlExecutor`](super::sql::SqlExecutor) bridge.
//! * [`MemoryDatabase`](super::memory::MemoryDatabase) — a dependency-free
//!   in-memory store for native apps and tests.
//!
//! All methods are async and return boxed futures so the trait stays
//! object-safe; on native the futures must be `Send` (the replica's
//! pump tasks run on Tokio), on wasm they run on the single-threaded
//! JS event loop and don't need to be.

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;

use palimpsest_proto::palimpsest::sync::v1::Schema;
use palimpsest_proto::wire::{WireDatum, WireRow};

use crate::cache::PrimaryKey;

/// `Send` on native targets, unconstrained on `wasm32` — mirrors the
/// split in `runtime::spawn`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` on native targets, unconstrained on `wasm32` — mirrors the
/// split in `runtime::spawn`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Send + Sync` on native targets, unconstrained on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> MaybeSendSync for T {}

/// `Send + Sync` on native targets, unconstrained on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSendSync for T {}

/// Boxed future returned by [`LocalDatabase`] methods. `Send` on
/// native, plain on wasm.
#[cfg(not(target_arch = "wasm32"))]
pub type DbFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed future returned by [`LocalDatabase`] methods. `Send` on
/// native, plain on wasm.
#[cfg(target_arch = "wasm32")]
pub type DbFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Failures surfaced by a [`LocalDatabase`] implementation.
#[derive(Debug, Error)]
pub enum LocalDbError {
    /// The implementation cannot run this operation (e.g. arbitrary SQL
    /// against [`MemoryDatabase`](super::memory::MemoryDatabase)).
    #[error("unsupported by this local database: {0}")]
    Unsupported(String),
    /// The underlying engine rejected a statement.
    #[error("local execution failed: {0}")]
    Execution(String),
    /// A table the replica expected to exist is missing, or returned
    /// rows didn't match the expected schema.
    #[error("local state corrupt: {0}")]
    Corrupt(String),
}

/// Description of one mirrored local table: its name plus the wire
/// schema the server attached to the mirror subscription.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSpec {
    /// Local (and usually remote) table name.
    pub name: String,
    /// Column schema from the server's `Accepted` message.
    pub schema: Schema,
}

impl TableSpec {
    /// Build a spec from a subscription's accepted schema.
    #[must_use]
    pub fn new(name: impl Into<String>, schema: Schema) -> Self {
        Self {
            name: name.into(),
            schema,
        }
    }

    /// Primary-key column indices. When the server reported no primary
    /// key, every column participates in row identity (the same
    /// fallback a `DISTINCT` result set would need).
    #[must_use]
    pub fn pk_indices(&self) -> Vec<usize> {
        if self.schema.primary_key_columns.is_empty() {
            (0..self.schema.columns.len()).collect()
        } else {
            self.schema
                .primary_key_columns
                .iter()
                .map(|c| usize::try_from(*c).unwrap_or(0))
                .collect()
        }
    }

    /// Extract the identity key from a full row.
    #[must_use]
    pub fn key_of(&self, row: &WireRow) -> PrimaryKey {
        self.pk_indices()
            .iter()
            .filter_map(|idx| row.get(*idx).cloned())
            .collect()
    }

    /// Column names in schema order.
    #[must_use]
    pub fn column_names(&self) -> Vec<&str> {
        self.schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Index of a column by name.
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.schema.columns.iter().position(|c| c.name == name)
    }
}

/// One write the replica asks the local database to make. Batches of
/// these are applied atomically so observers never see a torn remote
/// transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalWrite {
    /// Insert-or-replace the row with this identity key.
    Upsert {
        /// Full row in schema column order.
        row: WireRow,
    },
    /// Delete the row with this identity key, if present.
    Delete {
        /// Identity key (see [`TableSpec::pk_indices`]).
        key: PrimaryKey,
    },
}

/// Storage backend for the local replica.
///
/// Implementations must apply each `apply_writes` / `replace_rows`
/// call atomically: a reader (including [`LocalDatabase::query`])
/// observes either none or all of the batch.
pub trait LocalDatabase: MaybeSendSync {
    /// Create the mirrored table if it doesn't exist yet. Called once
    /// per mirror on every (re)subscription acceptance; must be
    /// idempotent.
    fn ensure_table<'a>(&'a self, spec: &'a TableSpec) -> DbFuture<'a, Result<(), LocalDbError>>;

    /// Atomically replace the table's entire contents (fresh snapshot).
    fn replace_rows<'a>(
        &'a self,
        spec: &'a TableSpec,
        rows: Vec<WireRow>,
    ) -> DbFuture<'a, Result<(), LocalDbError>>;

    /// Atomically apply a batch of upserts/deletes (one remote commit,
    /// or one optimistic-mutation rebase).
    fn apply_writes<'a>(
        &'a self,
        spec: &'a TableSpec,
        writes: Vec<LocalWrite>,
    ) -> DbFuture<'a, Result<(), LocalDbError>>;

    /// Read one row back by identity key (used to capture pre-images
    /// before an optimistic mutation).
    fn get_row<'a>(
        &'a self,
        spec: &'a TableSpec,
        key: &'a PrimaryKey,
    ) -> DbFuture<'a, Result<Option<WireRow>, LocalDbError>>;

    /// Run an arbitrary read-only SQL query against the local store.
    ///
    /// This is the "same queries against both" entry point: the SQL
    /// dialect is Postgres, so a query registered with the server runs
    /// unchanged against the mirrored subset.
    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: Vec<WireDatum>,
    ) -> DbFuture<'a, Result<Vec<WireRow>, LocalDbError>>;
}
