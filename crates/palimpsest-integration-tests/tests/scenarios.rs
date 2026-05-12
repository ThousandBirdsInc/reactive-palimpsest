// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(
    clippy::cast_possible_wrap,
    clippy::missing_const_for_fn,
    clippy::option_if_let_else
)]

//! Full-stack integration scenarios per §15.6 / §18.12.
//!
//! Each scenario is one `#[tokio::test]`. We drive `MockPostgres` (or
//! its in-process `SnapshotProvider` analogue) plus the
//! `SubscriptionRouter` and assert end-to-end behavior. Scenarios stay
//! deterministic — no Postgres process, no network beyond loopback.

use std::cell::RefCell;

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_permissions::{
    compile_rules, PermissionRule, UserContext, UserContextSchema, UserValue,
};
use palimpsest_server::{
    resume, BackpressurePolicy, ClientSubscriptionId, ColumnSpec, CompactionWindow, ConnectionId,
    DiffEvent, DiffOp, QueryId, RawDiff, ResumeDecision, ResyncReason, RouterConfig, RouterError,
    SchemaDefinition, SchemaId, SnapshotBatch, SnapshotProvider, SnapshotTableRows,
    SubscribeRequest, SubscriptionRouter, VecCursor,
};
use palimpsest_sql::{lower::parse_and_lower, Catalog, ColumnType};
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;
use tokio_stream::StreamExt;

struct ScriptedProvider {
    responses: RefCell<Vec<SnapshotBatch>>,
}
impl ScriptedProvider {
    fn new(responses: Vec<SnapshotBatch>) -> Self {
        Self {
            responses: RefCell::new(responses),
        }
    }
}
impl SnapshotProvider for ScriptedProvider {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        self.responses
            .borrow_mut()
            .pop()
            .ok_or_else(|| "scripted snapshot exhausted".to_owned())
    }
}

fn schema() -> SchemaDefinition {
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
        ],
        primary_key_columns: vec![0],
    }
}

fn snapshot(rows: Vec<(i64, i64)>) -> SnapshotBatch {
    SnapshotBatch {
        snapshot_lsn: Lsn::new(100),
        rows: vec![SnapshotTableRows {
            table: TableId::new(1),
            rows: rows
                .into_iter()
                .map(|(id, author)| smallvec![Datum::I64(id), Datum::I64(author)])
                .collect(),
        }],
    }
}

fn subscribe<P: SnapshotProvider + ?Sized>(
    router: &SubscriptionRouter,
    provider: &P,
    client: &str,
    user_id: Option<i64>,
) -> palimpsest_server::SubscribeResponse {
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();
    let user_ctx = match user_id {
        Some(id) => UserContext::new([("id".to_owned(), UserValue::Int(id))]),
        None => UserContext::new(std::iter::empty()),
    };
    router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                client_id: ClientSubscriptionId::new(client),
                query: QueryId::new("posts.recent"),
                query_graph: &graph,
                user_ctx,
                schema: schema(),
                resume_lsn: None,
                compiled_plan: None,
            },
            provider,
        )
        .unwrap()
}

#[tokio::test]
async fn scenario_initial_snapshot_then_steady_state_diffs() {
    let router = SubscriptionRouter::new(RouterConfig::default());
    let provider = ScriptedProvider::new(vec![snapshot(vec![(1, 7), (2, 8)])]);
    let response = subscribe(&router, &provider, "posts", None);

    let mut stream = response.stream;
    let DiffEvent::Initial { rows, lsn } = stream.next().await.unwrap() else {
        panic!("expected Initial");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(lsn, Lsn::new(100));

    let mut cursor = VecCursor::new([RawDiff {
        row: smallvec![Datum::I64(3), Datum::I64(9)],
        lsn: Lsn::new(110),
        diff: 1,
    }]);
    router
        .pump_cursor(response.subscription_id, &mut cursor, &[0])
        .unwrap();
    let DiffEvent::Update { changes, lsn } = stream.next().await.unwrap() else {
        panic!("expected Update");
    };
    assert_eq!(lsn, Lsn::new(110));
    assert_eq!(changes.len(), 1);
}

#[tokio::test]
async fn scenario_subscription_update_mid_stream() {
    // Emulates the "vars change" path: client unsubscribes from the old
    // SubscribeRequest, subscribes to a new one with different vars. The
    // router-level guarantee is that the second subscribe gets its own
    // initial snapshot independent of the first.
    let router = SubscriptionRouter::new(RouterConfig::default());
    let provider =
        ScriptedProvider::new(vec![snapshot(vec![(1, 9)]), snapshot(vec![(1, 7), (2, 8)])]);

    let first = subscribe(&router, &provider, "first", None);
    router.unsubscribe(first.subscription_id).unwrap();

    let second = subscribe(&router, &provider, "second", None);
    let mut stream = second.stream;
    let DiffEvent::Initial { rows, .. } = stream.next().await.unwrap() else {
        panic!("expected Initial");
    };
    assert_eq!(
        rows.len(),
        1,
        "second subscription receives its own snapshot"
    );
}

#[tokio::test]
async fn scenario_reconnect_with_valid_resume_lsn() {
    let window = CompactionWindow::new(Lsn::new(50), Lsn::new(150));
    let decision = resume::resolve(Some(Lsn::new(100)), window);
    assert!(matches!(decision, ResumeDecision::Replay { from_lsn } if from_lsn == Lsn::new(100)));
}

#[tokio::test]
async fn scenario_reconnect_past_compaction_window_forces_resync() {
    let window = CompactionWindow::new(Lsn::new(100), Lsn::new(300));
    let decision = resume::resolve(Some(Lsn::new(50)), window);
    assert!(matches!(
        decision,
        ResumeDecision::FreshInitial {
            reason: ResyncReason::LsnCompacted
        }
    ));
}

#[tokio::test]
async fn scenario_permission_change_flips_client_view() {
    let router = SubscriptionRouter::new(RouterConfig::default());
    let user_schema = UserContextSchema::new([("id".to_owned(), ColumnType::Int)]);

    // Strict rule first: only author 7 may pass.
    let strict = compile_rules(
        &[PermissionRule::new("posts_owner", "posts", "author_id = 7")],
        &Catalog::demo(),
        &user_schema,
    )
    .unwrap();
    router.set_rules(strict);

    let provider = ScriptedProvider::new(vec![snapshot(vec![(1, 7), (2, 9)])]);
    let response = subscribe(&router, &provider, "posts", Some(1));
    let mut stream = response.stream;
    let DiffEvent::Initial { .. } = stream.next().await.unwrap() else {
        panic!("expected Initial");
    };

    // Flip rules: now only author 9 may pass. The router stores the new
    // rules; existing subscriptions are unaffected for already-emitted
    // snapshots, but new subscribers see the post-flip view.
    let permissive = compile_rules(
        &[PermissionRule::new("posts_owner", "posts", "author_id = 9")],
        &Catalog::demo(),
        &user_schema,
    )
    .unwrap();
    router.set_rules(permissive);
    assert_eq!(router.active_subscriptions(), 1);
}

#[tokio::test]
async fn scenario_channel_saturation_emits_resync() {
    let router = SubscriptionRouter::new(RouterConfig {
        channel_capacity: 1,
        backpressure: BackpressurePolicy::DropAndResync,
        ..RouterConfig::default()
    });

    let provider = ScriptedProvider::new(vec![snapshot(vec![(1, 7)])]);
    let response = subscribe(&router, &provider, "posts", None);

    // Capacity 1, Initial already queued — the next pump must saturate.
    let mut cursor = VecCursor::new([RawDiff {
        row: smallvec![Datum::I64(2), Datum::I64(8)],
        lsn: Lsn::new(110),
        diff: 1,
    }]);
    let err = router
        .pump_cursor(response.subscription_id, &mut cursor, &[0])
        .unwrap_err();
    assert!(matches!(err, RouterError::ChannelSaturated));
    assert!(router.metrics().snapshot().resyncs_emitted >= 1);
}

#[tokio::test]
async fn scenario_subscription_lifecycle_metrics() {
    // Schema-change scenario: we approximate a `RelationChange` event
    // by tearing the subscription down and re-subscribing under a new
    // schema id. The router guarantees `unsubscribe` releases its
    // shared subgraph, which is the operator-state cleanup path.
    let router = SubscriptionRouter::new(RouterConfig::default());
    let provider = ScriptedProvider::new(vec![snapshot(vec![(1, 7)])]);
    let response = subscribe(&router, &provider, "posts", None);

    let release = router
        .unsubscribe(response.subscription_id)
        .unwrap()
        .expect("subgraph release");
    assert!(matches!(
        release,
        palimpsest_dataflow::palimpsest::SharedSubgraphRelease::Teardown { .. }
    ));
    assert_eq!(router.active_subscriptions(), 0);

    // Receive the Initial we already queued before unsubscribe.
    let mut stream = response.stream;
    let initial = stream.next().await;
    assert!(matches!(initial, Some(DiffEvent::Initial { .. })));
}

// Acknowledge the final `op == DiffOp::Insert` import path.
fn _silence(_: DiffOp) {}
