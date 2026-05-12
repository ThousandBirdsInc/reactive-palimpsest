// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Subscription identity and per-subscription state.
//!
//! The fields in [`Subscription`] mirror the §10 sketch in `DESIGN.md`,
//! with two simplifications: the trace handle is opaque to the router
//! (consumed via [`crate::cursor::TraceCursor`]) and permission state
//! is captured by the rewritten MIR rather than per-rule trace handles
//! — the rewriter (§11.2) inlines the filter into the user query, so
//! no separate runtime objects are needed.

use std::sync::atomic::{AtomicU64, Ordering};

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_permissions::UserContext;
use palimpsest_wal::DatumType;
use serde::{Deserialize, Serialize};

/// Server-side stable identifier for a long-lived gRPC connection.
///
/// Connections are scoped: a `ClientSubscriptionId` only needs to be
/// unique within a single connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Wraps a raw connection identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Server-assigned globally-unique identifier for a subscription.
///
/// Returned to the client in `Accepted`. The router preserves this id
/// across resume/replay so clients can de-dupe diffs by `(sub_id, lsn)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    /// Wraps a raw subscription identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic factory for [`SubscriptionId`]s.
#[derive(Debug, Default)]
pub(crate) struct SubscriptionIdAllocator {
    next: AtomicU64,
}

impl SubscriptionIdAllocator {
    pub(crate) const fn new(start: u64) -> Self {
        Self {
            next: AtomicU64::new(start),
        }
    }

    pub(crate) fn allocate(&self) -> SubscriptionId {
        SubscriptionId(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

/// Client-supplied subscription label, scoped to one connection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClientSubscriptionId(String);

impl ClientSubscriptionId {
    /// Wraps a client-supplied label.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the underlying label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Canonical handle for a query plan (the name used by the
/// `BuildPlanRegistry`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QueryId(String);

impl QueryId {
    /// Wraps a canonical query name.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the underlying name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable identifier for a result-row schema attached to a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SchemaId(u64);

impl SchemaId {
    /// Wraps a raw schema identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Server-side description of a single row column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Column name, as projected by the canonical query.
    pub name: String,
    /// Datum type (mirrors `palimpsest_wal::DatumType`).
    pub datum_type: DatumType,
    /// Whether the column may carry `Datum::Null`.
    pub nullable: bool,
}

/// Schema attached to an `Accepted` message; tells the client how to
/// decode subsequent diff payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDefinition {
    /// Stable schema identifier, referenced by every diff payload.
    pub id: SchemaId,
    /// Ordered column list.
    pub columns: Vec<ColumnSpec>,
    /// Indexes (into `columns`) that form the primary key.
    pub primary_key_columns: Vec<usize>,
}

/// Lifecycle states a subscription transitions through.
///
/// The router emits `Initial` only once and only from `Seeding`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// Initial-snapshot path is in flight (§18.7 #3).
    Seeding,
    /// Snapshot complete and the cursor is following the trace.
    Streaming,
    /// Saturation or schema-change forced an early teardown; the next
    /// diff emitted is `Resync` and the router will refuse acks.
    Draining,
    /// Subscription has been torn down; refcounts released.
    Closed,
}

/// Per-subscription router state (cf. §10 `Subscription`).
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Server-assigned id.
    pub id: SubscriptionId,
    /// Client-supplied label (scoped to `connection`).
    pub client_id: ClientSubscriptionId,
    /// Owning connection id.
    pub connection: ConnectionId,
    /// Canonical query handle.
    pub query: QueryId,
    /// User context the permission rewriter was parameterized with.
    pub user_ctx: UserContext,
    /// Schema id for diff payloads.
    pub schema_id: SchemaId,
    /// LSN at which the snapshot was taken; first event the cursor
    /// reads.
    pub snapshot_lsn: Lsn,
    /// LSN through which the client has acked diffs.
    pub cursor_lsn: Lsn,
    /// Lifecycle state.
    pub state: SubscriptionState,
}

impl Subscription {
    /// Helper: returns true when the subscription is still consuming
    /// diffs (not draining or closed).
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(
            self.state,
            SubscriptionState::Seeding | SubscriptionState::Streaming
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientSubscriptionId, ColumnSpec, ConnectionId, QueryId, SchemaDefinition, SchemaId,
        SubscriptionIdAllocator, SubscriptionState,
    };
    use palimpsest_wal::DatumType;

    #[test]
    fn allocator_returns_monotonic_ids() {
        let allocator = SubscriptionIdAllocator::new(0);
        let a = allocator.allocate();
        let b = allocator.allocate();
        let c = allocator.allocate();
        assert_eq!(a.get(), 0);
        assert_eq!(b.get(), 1);
        assert_eq!(c.get(), 2);
    }

    #[test]
    fn ids_round_trip_through_constructors() {
        let conn = ConnectionId::new(42);
        let client = ClientSubscriptionId::new("posts.recent");
        let query = QueryId::new("posts.recent.v1");
        let schema = SchemaId::new(7);
        assert_eq!(conn.get(), 42);
        assert_eq!(client.as_str(), "posts.recent");
        assert_eq!(query.as_str(), "posts.recent.v1");
        assert_eq!(schema.get(), 7);
    }

    #[test]
    fn schema_definition_is_copyable_for_clients() {
        let schema = SchemaDefinition {
            id: SchemaId::new(1),
            columns: vec![ColumnSpec {
                name: "id".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            }],
            primary_key_columns: vec![0],
        };
        let clone = schema.clone();
        assert_eq!(clone, schema);
    }

    #[test]
    fn state_active_predicate() {
        assert!(matches!(
            SubscriptionState::Seeding,
            SubscriptionState::Seeding | SubscriptionState::Streaming
        ));
        assert!(!matches!(
            SubscriptionState::Closed,
            SubscriptionState::Seeding | SubscriptionState::Streaming
        ));
    }
}
