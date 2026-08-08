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
//! * **Ingest** — pgoutput frames are decoded, TOAST placeholders are
//!   resolved against the in-memory mirror, and complete transactions
//!   are applied to a bounded diff journal that refuses a truncated
//!   resume rather than serving a gap.
//! * **Reconnect with reconciliation** — on connection loss the
//!   runtime re-snapshots, diffs against the mirror, and emits only
//!   the difference as one transaction. Credentials never appear in
//!   logs.
//! * **Drift detection** — a `Relation` frame that no longer matches
//!   the introspected shape stops ingest with a named error instead of
//!   silently mis-decoding rows.
//!
//! # Limitations
//!
//! Change ingest currently polls
//! `pg_logical_slot_get_binary_changes()` (default every 100 ms):
//! `tokio-postgres` has no `COPY_BOTH` support, so `START_REPLICATION`
//! streaming is unreachable through it. Polling consumes the slot
//! destructively, but because the mirror and journal live in process
//! memory, a crash simply re-snapshots on restart — the reconnect
//! reconciliation path makes the polling loss-window invisible to
//! subscribers. TLS DSNs are not yet supported (`sslmode=disable`).

#![warn(missing_docs)]

mod error;
mod introspect;
mod replication;
mod runtime;

pub use error::PostgresRuntimeError;
pub use introspect::{introspect_tables, sql_catalog, IntrospectedColumn, IntrospectedTable};
pub use replication::ReplicationHandle;
pub use runtime::{PostgresRuntimeConfig, PostgresWalRuntime};
