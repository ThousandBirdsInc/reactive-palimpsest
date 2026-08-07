// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared schema / snapshot / subscription plumbing for scenarios.

use std::cell::RefCell;
use std::time::Instant;

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_permissions::UserContext;
use palimpsest_server::{
    ClientSubscriptionId, ColumnSpec, ConnectionId, QueryId, SchemaDefinition, SchemaId,
    SnapshotBatch, SnapshotProvider, SnapshotTableRows, SubscribeRequest, SubscribeResponse,
    SubscriptionRouter,
};
use palimpsest_sql::lower::parse_and_lower;
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;

/// Column index of the embedded send-timestamp in workload rows.
pub const SENT_AT_COLUMN: usize = 2;

/// LSN the synthetic snapshots claim to be taken at.
pub const SNAPSHOT_LSN: u64 = 100;

/// Table id used by the single-table serving-path scenarios.
#[must_use]
pub const fn posts_table() -> TableId {
    TableId::new(1)
}

/// Schema for the synthetic `posts` table:
/// `(id BIGINT PK, author_id BIGINT, sent_at_ns BIGINT)`.
///
/// `sent_at_ns` carries the producer's monotonic send timestamp so
/// consumers can compute commit-to-client latency without a shared
/// side-channel map.
#[must_use]
pub fn posts_schema() -> SchemaDefinition {
    SchemaDefinition {
        id: SchemaId::new(11),
        columns: vec![
            ColumnSpec {
                name: "id".into(),
                datum_type: DatumType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "author_id".into(),
                datum_type: DatumType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "sent_at_ns".into(),
                datum_type: DatumType::I64,
                nullable: false,
            },
        ],
        primary_key_columns: vec![0],
    }
}

/// Nanoseconds elapsed since `epoch`, as an `i64` datum payload.
#[must_use]
pub fn now_ns(epoch: Instant) -> i64 {
    i64::try_from(epoch.elapsed().as_nanos()).unwrap_or(i64::MAX)
}

/// Builds a `posts` snapshot with `rows` synthetic rows.
#[must_use]
pub fn posts_snapshot(rows: usize, first_id: i64) -> SnapshotBatch {
    let rows = (0..rows)
        .map(|i| {
            smallvec![
                Datum::I64(first_id + i as i64),
                Datum::I64((i % 64) as i64),
                Datum::I64(0),
            ]
        })
        .collect();
    SnapshotBatch {
        snapshot_lsn: Lsn::new(SNAPSHOT_LSN),
        rows: vec![SnapshotTableRows {
            table: posts_table(),
            rows,
        }],
    }
}

/// One-shot snapshot provider (each subscribe consumes one batch).
pub struct OneShotProvider(RefCell<Option<SnapshotBatch>>);

impl OneShotProvider {
    /// Wraps a single snapshot batch.
    #[must_use]
    pub const fn new(batch: SnapshotBatch) -> Self {
        Self(RefCell::new(Some(batch)))
    }
}

impl SnapshotProvider for OneShotProvider {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        self.0.borrow_mut().take().ok_or_else(|| "exhausted".into())
    }
}

/// Subscribes a pass-through (no compiled plan, no rules) client to
/// the `posts` query with `snapshot_rows` bootstrap rows.
///
/// Mirrors the serving path the gRPC adapter uses for single-table
/// queries: raw WAL-shaped diffs are pumped straight to the client.
pub fn subscribe_pass_through(
    router: &SubscriptionRouter,
    connection: ConnectionId,
    label: &str,
    snapshot_rows: usize,
) -> Result<SubscribeResponse, String> {
    let graph = parse_and_lower("SELECT id, author_id, sent_at_ns FROM posts")
        .map_err(|e| format!("lower: {e}"))?;
    let provider = OneShotProvider::new(posts_snapshot(snapshot_rows, 0));
    router
        .subscribe(
            SubscribeRequest {
                connection,
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new(label),
                query: QueryId::new("posts.recent"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: posts_schema(),
                resume_lsn: None,
                compiled_plan: None,
                prerun_initial: None,
            },
            &provider,
        )
        .map_err(|e| format!("subscribe: {e}"))
}
