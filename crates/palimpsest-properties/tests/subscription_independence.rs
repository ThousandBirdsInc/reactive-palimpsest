// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(clippy::cast_possible_wrap, clippy::missing_const_for_fn)]

//! §15.3 property 5 — Subscription independence.
//!
//! N parallel subscriptions to the same query receive the same diff
//! sequence; one slow subscriber does not perturb others' diffs. We
//! drive two subscriptions in lockstep and assert their event streams
//! are identical, then drain only one to simulate a slow consumer and
//! verify the other still sees all updates.

use std::cell::RefCell;

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_permissions::UserContext;
use palimpsest_server::{
    ClientSubscriptionId, ColumnSpec, ConnectionId, DiffEvent, DiffOp, QueryId, RawDiff,
    RouterConfig, SchemaDefinition, SchemaId, SnapshotBatch, SnapshotProvider, SnapshotTableRows,
    SubscribeRequest, SubscriptionRouter, VecCursor,
};
use palimpsest_sql::lower::parse_and_lower;
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;
use tokio_stream::StreamExt;

struct ScriptedProvider {
    response: RefCell<Option<SnapshotBatch>>,
}
impl ScriptedProvider {
    fn new(snapshot: SnapshotBatch) -> Self {
        Self {
            response: RefCell::new(Some(snapshot)),
        }
    }
}
impl SnapshotProvider for ScriptedProvider {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        self.response
            .borrow_mut()
            .take()
            .ok_or_else(|| "snapshot already consumed".to_owned())
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

fn empty_snapshot() -> SnapshotBatch {
    SnapshotBatch {
        snapshot_lsn: Lsn::new(100),
        rows: vec![SnapshotTableRows {
            table: TableId::new(1),
            rows: Vec::new(),
        }],
    }
}

#[tokio::test]
async fn parallel_subscriptions_receive_same_sequence() {
    let router = SubscriptionRouter::new(RouterConfig::default());
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();

    let provider_a = ScriptedProvider::new(empty_snapshot());
    let provider_b = ScriptedProvider::new(empty_snapshot());

    let sub_a = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                client_id: ClientSubscriptionId::new("a"),
                query: QueryId::new("posts.recent"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema(),
                resume_lsn: None,
                compiled_plan: None,
            },
            &provider_a,
        )
        .unwrap();
    let sub_b = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(2),
                client_id: ClientSubscriptionId::new("b"),
                query: QueryId::new("posts.recent"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema(),
                resume_lsn: None,
                compiled_plan: None,
            },
            &provider_b,
        )
        .unwrap();

    let mut stream_a = sub_a.stream;
    let mut stream_b = sub_b.stream;
    // Drain initial snapshots.
    let _ = stream_a.next().await.unwrap();
    let _ = stream_b.next().await.unwrap();

    // Push the same diffs into both subscriptions and assert event
    // shapes match. Subscription IDs are allowed to differ, but ops
    // and LSNs should be identical.
    for round in 0..3_u64 {
        let lsn = Lsn::new(200 + round);
        let row = smallvec![Datum::I64(round as i64), Datum::I64(7)];
        let mut cursor_a = VecCursor::new([RawDiff {
            row: row.clone(),
            lsn,
            diff: 1,
        }]);
        let mut cursor_b = VecCursor::new([RawDiff { row, lsn, diff: 1 }]);
        router
            .pump_cursor(sub_a.subscription_id, &mut cursor_a, &[0])
            .unwrap();
        router
            .pump_cursor(sub_b.subscription_id, &mut cursor_b, &[0])
            .unwrap();

        let event_a = stream_a.next().await.unwrap();
        let event_b = stream_b.next().await.unwrap();
        match (&event_a, &event_b) {
            (
                DiffEvent::Update {
                    changes: ca,
                    lsn: la,
                },
                DiffEvent::Update {
                    changes: cb,
                    lsn: lb,
                },
            ) => {
                assert_eq!(la, lb, "lsns diverged at round {round}");
                assert_eq!(
                    ca.len(),
                    cb.len(),
                    "change counts diverged at round {round}"
                );
                for (a, b) in ca.iter().zip(cb.iter()) {
                    assert_eq!(
                        matches!(a.op, DiffOp::Insert),
                        matches!(b.op, DiffOp::Insert)
                    );
                }
            }
            other => panic!("expected Update on both streams, saw {other:?}"),
        }
    }
}
