// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Local-first replica for Palimpsest clients.
//!
//! Keeps a client-side Postgres-compatible database (a pgrust-style
//! WASM build in the browser, an embedded engine or the bundled
//! in-memory store natively) in sync with the remote database's
//! **permissioned subset**, runs the same SQL against both, and applies
//! optimistic mutations that Palimpsest reconciles when the
//! authoritative change streams back through the WAL.
//!
//! ```no_run
//! use std::sync::Arc;
//! use palimpsest_client::{Auth, Client};
//! use palimpsest_client::local::{MemoryDatabase, Mutation};
//! use palimpsest_client::WireDatum;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::connect("http://127.0.0.1:50051", Auth::Anonymous).await?;
//! let replica = client
//!     .local_replica(Arc::new(MemoryDatabase::new()))
//!     .mirror_table("posts")
//!     .start()
//!     .await?;
//!
//! // Same query, local latency: served from the mirrored subset.
//! let rows = replica.query("SELECT * FROM posts", Vec::new()).await?;
//!
//! // Optimistic write (requires `.with_writer(...)` on the builder):
//! let _token = replica
//!     .mutate(Mutation::update(
//!         "posts",
//!         [("id", WireDatum::I64(1))],
//!         [("title", WireDatum::Text(b"draft".to_vec()))],
//!     ))
//!     .await;
//! # Ok(()) }
//! ```
//!
//! See `docs/LOCAL-FIRST.md` for the full architecture.

mod db;
mod memory;
mod reconcile;
mod replica;
mod sql;

pub use db::{
    DbFuture, LocalDatabase, LocalDbError, LocalWrite, MaybeSend, MaybeSendSync, TableSpec,
};
pub use memory::MemoryDatabase;
pub use replica::{
    LocalReplica, LocalReplicaBuilder, MutateError, Mutation, MutationToken, RemoteWriter,
    ReplicaEvent, TableStates, TableSyncState, WriteRequest,
};
pub use sql::{
    column_type_sql, create_table_sql, delete_statement, quote_ident, select_by_key_statement,
    upsert_statement, SqlExecutor, SqlLocalDatabase, Statement,
};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
