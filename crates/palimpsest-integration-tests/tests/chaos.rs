// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(clippy::cast_possible_wrap)]

//! Chaos / fault-injection scenarios per §15.7 / §18.12.
//!
//! Each `Fault` variant gets its own test. Most assertions are about
//! how the harness records the fault and what shape the resulting
//! `MockPostgres` output takes — full reconnection plumbing through
//! the server lives behind the bidi gRPC service and is exercised in
//! `palimpsest-server/tests/grpc_server.rs`.

use std::cell::RefCell;

use palimpsest_dataflow::palimpsest::Lsn as DataflowLsn;
use palimpsest_permissions::UserContext;
use palimpsest_server::{
    BackpressurePolicy, ClientSubscriptionId, ColumnSpec, ConnectionId, QueryId, RawDiff,
    RouterConfig, RouterError, SchemaDefinition, SchemaId, SnapshotBatch, SnapshotProvider,
    SnapshotTableRows, SubscribeRequest, SubscriptionRouter, VecCursor,
};
use palimpsest_sql::lower::parse_and_lower;
use palimpsest_test_harness::{Fault, Lsn as HarnessLsn, MockPostgres, TableId as HarnessTableId};
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;

struct OneShot(RefCell<Option<SnapshotBatch>>);
impl SnapshotProvider for OneShot {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        self.0.borrow_mut().take().ok_or_else(|| "exhausted".into())
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
        snapshot_lsn: DataflowLsn::new(100),
        rows: vec![SnapshotTableRows {
            table: TableId::new(1),
            rows: Vec::new(),
        }],
    }
}

#[test]
fn fault_drop_connection_is_recorded_by_mock_postgres() {
    let mut server = MockPostgres::bind().expect("bind");
    server.fault(Fault::DropConnection);
    assert_eq!(server.faults(), &[Fault::DropConnection]);
}

#[test]
fn fault_hang_after_is_recorded_by_mock_postgres() {
    let mut server = MockPostgres::bind().expect("bind");
    server.fault(Fault::HangAfter { bytes: 256 });
    assert_eq!(server.faults(), &[Fault::HangAfter { bytes: 256 }]);
}

#[test]
fn fault_slow_send_is_recorded_by_mock_postgres() {
    let mut server = MockPostgres::bind().expect("bind");
    server.fault(Fault::SlowSend {
        rate_bytes_per_sec: 64,
    });
    assert_eq!(
        server.faults(),
        &[Fault::SlowSend {
            rate_bytes_per_sec: 64
        }]
    );
}

#[test]
fn fault_slot_gone_is_recorded_by_mock_postgres() {
    let mut server = MockPostgres::bind().expect("bind");
    server.fault(Fault::SlotGone);
    assert_eq!(server.faults(), &[Fault::SlotGone]);
}

#[test]
fn fault_lsn_rewind_is_recorded_by_mock_postgres() {
    let mut server = MockPostgres::bind().expect("bind");
    server.fault(Fault::LsnRewind {
        to: HarnessLsn::new(42),
    });
    assert_eq!(
        server.faults(),
        &[Fault::LsnRewind {
            to: HarnessLsn::new(42)
        }]
    );
}

#[test]
fn fault_schema_drift_is_recorded_by_mock_postgres() {
    let mut server = MockPostgres::bind().expect("bind");
    server.fault(Fault::SchemaDrift {
        table: HarnessTableId::new(7),
    });
    assert_eq!(
        server.faults(),
        &[Fault::SchemaDrift {
            table: HarnessTableId::new(7)
        }]
    );
}

#[tokio::test]
async fn slow_consumer_saturates_bounded_channel_into_resync() {
    // Models the `SlowSend` invariant at the *consumer* side: a slow
    // subscriber + bounded channel must trigger a Resync rather than
    // silently dropping diffs. We assert via the metrics counters
    // (`channel_full_events`, `resyncs_emitted`) since the resync is
    // emitted out-of-band and the in-channel slot is already full.
    let router = SubscriptionRouter::new(RouterConfig {
        channel_capacity: 1,
        backpressure: BackpressurePolicy::DropAndResync,
        ..RouterConfig::default()
    });
    let provider = OneShot(RefCell::new(Some(empty_snapshot())));
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();
    let response = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                subscription_id: router.allocate_subscription_id(),
                client_id: ClientSubscriptionId::new("posts"),
                query: QueryId::new("posts.recent"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema(),
                resume_lsn: None,
                compiled_plan: None,
                prerun_initial: None,
            },
            &provider,
        )
        .unwrap();

    // Initial is queued. Don't drain; pump another diff to saturate.
    let mut cursor = VecCursor::new([RawDiff {
        row: smallvec![Datum::I64(1), Datum::I64(7)],
        lsn: DataflowLsn::new(110),
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
