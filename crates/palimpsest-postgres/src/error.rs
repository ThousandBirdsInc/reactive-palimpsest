// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Runtime errors, each carrying the exact remedial action where one
//! exists. Credentials are never embedded in error text — connection
//! failures name the failure, not the DSN.

use thiserror::Error;

/// Failures starting or running the Postgres WAL runtime.
#[derive(Debug, Error)]
pub enum PostgresRuntimeError {
    /// Connecting to Postgres failed. The DSN is deliberately absent.
    #[error("postgres connection failed: {0}")]
    Connect(String),

    /// A catalog / management query failed.
    #[error("postgres query failed during {phase}: {source}")]
    Query {
        /// What the runtime was doing.
        phase: &'static str,
        /// Driver error.
        source: tokio_postgres::Error,
    },

    /// A streamed table does not exist in the database.
    #[error("table '{table}' is registered for streaming but does not exist in the database")]
    MissingTable {
        /// The missing table (as registered).
        table: String,
    },

    /// A streamed table lacks `REPLICA IDENTITY FULL` and the
    /// connection role may not fix it.
    #[error(
        "table '{table}' has REPLICA IDENTITY {current} and the connection role cannot change it; \
         retractions need the full old row — run: ALTER TABLE {table} REPLICA IDENTITY FULL;"
    )]
    ReplicaIdentity {
        /// The offending table.
        table: String,
        /// The identity currently configured.
        current: &'static str,
    },

    /// The role lacks the REPLICATION attribute (or equivalent).
    #[error(
        "the connection role lacks replication privileges: {detail}; \
         grant them with: ALTER ROLE <role> REPLICATION; \
         (on managed Postgres, grant the provider's replication role instead, \
         e.g. GRANT rds_replication TO <role>)"
    )]
    MissingReplicationPrivilege {
        /// Server-reported detail.
        detail: String,
    },

    /// Publication management failed.
    #[error("publication '{publication}' could not be created or reconciled: {detail}")]
    Publication {
        /// Publication name.
        publication: String,
        /// Server-reported detail.
        detail: String,
    },

    /// Slot management failed.
    #[error("replication slot '{slot}' could not be created or resumed: {detail}")]
    Slot {
        /// Slot name.
        slot: String,
        /// Server-reported detail.
        detail: String,
    },

    /// Snapshot decode failed.
    #[error("snapshot of '{table}' failed: {detail}")]
    Snapshot {
        /// Table being snapshotted.
        table: String,
        /// Failure detail.
        detail: String,
    },

    /// pgoutput frame decode failed.
    #[error("logical decoding failed: {0}")]
    Decode(#[from] palimpsest_wal::WalError),

    /// A `Relation` frame no longer matches the introspected shape.
    /// The runtime refuses to continue rather than mis-decode rows.
    #[error(
        "schema drift detected on table '{table}': {detail}; \
         restart the runtime to re-introspect the catalog"
    )]
    SchemaDrift {
        /// Drifted table.
        table: String,
        /// What changed.
        detail: String,
    },
}

impl PostgresRuntimeError {
    pub(crate) const fn query(phase: &'static str, source: tokio_postgres::Error) -> Self {
        Self::Query { phase, source }
    }
}
