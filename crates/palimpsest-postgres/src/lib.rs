// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Production Postgres WAL runtime.
//!
//! An adopter supplies a DSN (and, usually, a query registry). This
//! crate owns everything else the integration used to require:
//!
//! * **Catalog introspection** — tables, columns, types, nullability,
//!   primary keys, enums, domains, and arrays are read from
//!   `pg_catalog` at startup. There is no `table_schema` to implement
//!   and no column list to transcribe; the database is the single
//!   source of truth.
//! * **Streamed-set derivation** — the table set comes from the
//!   registered queries ([`PostgresRuntimeConfig::from_registry`]); a
//!   query against an unknown table is a startup error.
//! * **Publication ownership** — the publication is created, or
//!   verified and reconciled, from the derived set.
//! * **`REPLICA IDENTITY FULL` ownership** — verified per table, set
//!   where the connection role permits, and refused at startup with
//!   the exact remedial `ALTER TABLE` where not.
//! * **Slot lifecycle** — the logical replication slot is created on
//!   first run and resumed afterwards; the initial snapshot is fenced
//!   against the slot's change stream by transaction-id visibility so
//!   no change is missed or double-applied.
//! * **Streaming ingest** — a real walsender session
//!   (`START_REPLICATION` over `COPY_BOTH`) delivers changes as they
//!   commit. `tokio-postgres` cannot open one, so the session speaks
//!   the frontend/backend protocol directly (see `stream`);
//!   the slot's confirmed position advances only for what has been
//!   applied, so a crash replays rather than loses. pgoutput frames
//!   are decoded, TOAST placeholders resolved against the in-memory
//!   mirror, and complete transactions applied to a bounded diff
//!   journal that refuses a truncated resume rather than serving a
//!   gap.
//! * **TLS** — DSNs asking for TLS negotiate it on *both* the
//!   management connection and the replication stream. Without a root
//!   CA the session is encrypted but the server is unauthenticated
//!   (libpq `sslmode=require`); supplying
//!   [`PostgresRuntimeConfig::tls_root_ca_pem`] upgrades to full
//!   chain + hostname verification.
//! * **Reconnect with reconciliation** — on connection loss the
//!   runtime re-snapshots, diffs against the mirror, and emits only
//!   the difference as one transaction. Credentials never appear in
//!   logs.
//! * **Drift detection** — a `Relation` frame that no longer matches
//!   the introspected shape stops ingest with a named error instead of
//!   silently mis-decoding rows.
//! * **Client types** — [`typescript_module`] emits row and parameter
//!   types for every registered query from the same catalog, so
//!   clients generate their row types instead of transcribing them.

#![warn(missing_docs)]

mod error;
mod introspect;
mod replication;
mod runtime;
mod stream;
mod tls;
mod typegen;

pub use error::PostgresRuntimeError;
pub use introspect::{
    introspect_all_tables, introspect_tables, sql_catalog, IntrospectedColumn, IntrospectedTable,
};
pub use replication::introspect_database;
pub use replication::ReplicationHandle;
pub use runtime::{PostgresRuntimeConfig, PostgresWalRuntime};
pub use typegen::typescript_module;
