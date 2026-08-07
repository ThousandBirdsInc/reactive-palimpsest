// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end behavior tests for the subscription router.
//!
//! These exercise the public API the gRPC layer (§18.8) will adapt
//! onto a `tonic::Stream`: subscribe → pump → ack → unsubscribe, plus
//! the saturation / resume-out-of-window escape paths.

use std::cell::RefCell;

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_permissions::{
    compile_rules, PermissionRule, UserContext, UserContextSchema, UserValue,
};
use palimpsest_server::{
    BackpressurePolicy, ClientSubscriptionId, ColumnSpec, ConnectionId, DiffEvent, DiffOp, QueryId,
    RawDiff, RouterConfig, RouterError, SchemaDefinition, SchemaId, SnapshotBatch,
    SnapshotProvider, SnapshotTableRows, SubscribeRequest, SubscriptionRouter, VecCursor,
};
use palimpsest_sql::{lower::parse_and_lower, Catalog, ColumnType};
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;
use tokio_stream::StreamExt;

struct ScriptedProvider {
    responses: RefCell<Vec<SnapshotBatch>>,
}
impl ScriptedProvider {
    const fn new(responses: Vec<SnapshotBatch>) -> Self {
        Self {
            responses: RefCell::new(responses),
        }
    }
}
impl SnapshotProvider for ScriptedProvider {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        Ok(self
            .responses
            .borrow_mut()
            .pop()
            .expect("scripted snapshot exhausted"))
    }
}

fn schema_for_posts() -> SchemaDefinition {
    SchemaDefinition {
        id: SchemaId::new(11),
        columns: vec![
            ColumnSpec {
                name: "id".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            },
            ColumnSpec {
                name: "author_id".to_owned(),
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

#[tokio::test]
async fn lifecycle_subscribe_pump_ack_unsubscribe() {
    let router = SubscriptionRouter::new(RouterConfig::default());
    let provider = ScriptedProvider::new(vec![snapshot(vec![(1, 7), (2, 8)])]);
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();

    let response = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts.recent"),
                query: QueryId::new("posts.recent"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema_for_posts(),
                resume_lsn: None,
                compiled_plan: None,
                prerun_initial: None,
            },
            &provider,
        )
        .unwrap();
    assert_eq!(response.snapshot_lsn, Lsn::new(100));

    // Initial event arrives first.
    let mut stream = response.stream;
    let initial = stream.next().await.unwrap();
    let DiffEvent::Initial { rows, lsn } = initial else {
        panic!("expected initial");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(lsn, Lsn::new(100));

    // Push an insert at LSN 110.
    let mut cursor = VecCursor::new([RawDiff {
        table: None,
        row: smallvec![Datum::I64(3), Datum::I64(9)],
        lsn: Lsn::new(110),
        diff: 1,
    }]);
    router
        .pump_cursor(response.subscription_id, &mut cursor, &[0])
        .unwrap();

    let update = stream.next().await.unwrap();
    let DiffEvent::TransactionUpdate {
        changes,
        commit_lsn,
        ..
    } = update
    else {
        panic!("expected transaction update");
    };
    assert_eq!(commit_lsn, Lsn::new(110));
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].op, DiffOp::Insert);

    // Ack and unsubscribe.
    router.ack(response.subscription_id, Lsn::new(110)).unwrap();
    let release = router
        .unsubscribe(response.subscription_id)
        .unwrap()
        .expect("release outcome");
    assert!(matches!(
        release,
        palimpsest_dataflow::palimpsest::SharedSubgraphRelease::Teardown { .. }
    ));
    assert_eq!(router.active_subscriptions(), 0);

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.subscriptions_total, 1);
    assert_eq!(snapshot.diffs_sent, 1);
}

#[tokio::test]
async fn saturated_channel_emits_resync_and_drains() {
    let config = RouterConfig {
        channel_capacity: 1,
        backpressure: BackpressurePolicy::DropAndResync,
        ..RouterConfig::default()
    };
    let router = SubscriptionRouter::new(config);

    let provider = ScriptedProvider::new(vec![snapshot(vec![(1, 7)])]);
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();
    let response = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts"),
                query: QueryId::new("posts"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema_for_posts(),
                resume_lsn: None,
                compiled_plan: None,
                prerun_initial: None,
            },
            &provider,
        )
        .unwrap();

    // Capacity is 1 — Initial is already queued. Pump another batch
    // to force saturation.
    let mut cursor = VecCursor::new([RawDiff {
        table: None,
        row: smallvec![Datum::I64(3), Datum::I64(9)],
        lsn: Lsn::new(110),
        diff: 1,
    }]);
    let err = router
        .pump_cursor(response.subscription_id, &mut cursor, &[0])
        .unwrap_err();
    assert!(matches!(err, RouterError::ChannelSaturated));

    let metrics = router.metrics().snapshot();
    assert!(metrics.channel_full_events >= 1);
    assert!(metrics.resyncs_emitted >= 1);
}

#[tokio::test]
async fn permission_subscriptions_with_different_user_context_split_subgraphs() {
    let router = SubscriptionRouter::new(RouterConfig::default());
    let user_schema = UserContextSchema::new([("id".to_owned(), ColumnType::Int)]);
    let rules = compile_rules(
        &[PermissionRule::new(
            "posts_owner",
            "posts",
            "author_id = $user.id",
        )],
        &Catalog::demo(),
        &user_schema,
    )
    .unwrap();
    router.set_rules(rules.clone());

    let provider = ScriptedProvider::new(vec![snapshot(vec![]), snapshot(vec![])]);
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();

    // Rule-guarded subscribes need a compiled (permission-rewritten)
    // plan — pass-through would fail closed.
    let posts_lookup = |table: &str| {
        (table == "posts").then(|| {
            (
                TableId::new(1),
                palimpsest_dataflow::palimpsest::eval::ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("author_id".to_owned(), ColumnType::Int),
                ]),
            )
        })
    };
    let plan_for = |user_ctx: &UserContext| {
        let rewritten = palimpsest_permissions::rewrite(&graph, &rules, user_ctx)
            .unwrap()
            .graph;
        palimpsest_dataflow::palimpsest::compile_mir(&rewritten, &posts_lookup).unwrap()
    };

    let alice_ctx = UserContext::new([("id".to_owned(), UserValue::Int(1))]);
    let bob_ctx = UserContext::new([("id".to_owned(), UserValue::Int(2))]);
    let alice_plan = plan_for(&alice_ctx);
    let bob_plan = plan_for(&bob_ctx);

    let alice = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts"),
                query: QueryId::new("posts"),
                query_graph: &graph,
                user_ctx: alice_ctx,
                schema: schema_for_posts(),
                resume_lsn: None,
                compiled_plan: Some(alice_plan),
                prerun_initial: None,
            },
            &provider,
        )
        .unwrap();
    let bob = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(2),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts"),
                query: QueryId::new("posts"),
                query_graph: &graph,
                user_ctx: bob_ctx,
                schema: schema_for_posts(),
                resume_lsn: None,
                compiled_plan: Some(bob_plan),
                prerun_initial: None,
            },
            &provider,
        )
        .unwrap();
    assert_ne!(alice.subgraph.id(), bob.subgraph.id());
}

#[tokio::test]
async fn duplicate_client_label_within_connection_is_rejected() {
    let router = SubscriptionRouter::new(RouterConfig::default());
    let provider = ScriptedProvider::new(vec![snapshot(vec![]), snapshot(vec![])]);
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();

    router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts"),
                query: QueryId::new("posts"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema_for_posts(),
                resume_lsn: None,
                compiled_plan: None,
                prerun_initial: None,
            },
            &provider,
        )
        .unwrap();
    let result = router.subscribe(
        SubscribeRequest {
            connection: ConnectionId::new(1),
            subscription_id: router.allocate_subscription_id(),
            client_id: ClientSubscriptionId::new("posts"),
            query: QueryId::new("posts"),
            query_graph: &graph,
            user_ctx: UserContext::new(std::iter::empty()),
            schema: schema_for_posts(),
            resume_lsn: None,
            compiled_plan: None,
            prerun_initial: None,
        },
        &provider,
    );
    let Err(err) = result else {
        panic!("expected duplicate-id rejection")
    };
    assert!(matches!(err, RouterError::DuplicateClientSubscriptionId));
}

#[tokio::test]
async fn ack_advances_compaction_frontier() {
    let router = SubscriptionRouter::new(RouterConfig::default());
    let provider = ScriptedProvider::new(vec![snapshot(vec![(1, 7)])]);
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();
    let response = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts"),
                query: QueryId::new("posts"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema_for_posts(),
                resume_lsn: None,
                compiled_plan: None,
                prerun_initial: None,
            },
            &provider,
        )
        .unwrap();

    router.ack(response.subscription_id, Lsn::new(150)).unwrap();
    let stale = router.ack(response.subscription_id, Lsn::new(150)).unwrap();
    assert!(matches!(stale, palimpsest_server::AckOutcome::Stale));
}
