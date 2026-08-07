// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end integration test that drives the full
//! `subscribe → snapshot → live-diff` flow against a real
//! `SyncEngineService`. Exercises the same path the demo's chart
//! subscription takes, without any of the docker / nginx / browser
//! layers.
//!
//! The test:
//! 1. Spins up a `Palimpsest` server bound to an in-memory
//!    `TestEventsWal` (raw `events` table + journal cursor).
//! 2. Subscribes with the demo's aggregate SQL.
//! 3. Asserts the `Initial` event contains the per-category
//!    aggregate rows (and *not* the raw event rows).
//! 4. Pushes a fresh WAL diff into the journal.
//! 5. Asserts a `Diff` event arrives carrying the changed aggregate
//!    row — confirming the cursor pump routed the WAL diff through
//!    `PersistentHost::push_table_batch` and the host emitted a
//!    retract+assert pair the router collapsed into an update.

#![allow(clippy::significant_drop_tightening)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_dataflow::palimpsest::{Lsn, Row};
use palimpsest_permissions::{compile_rules, CompiledRule, PermissionRule, UserContextSchema};
use palimpsest_proto::palimpsest::sync::v1 as proto;
use palimpsest_proto::palimpsest::sync::v1::sync_engine_client::SyncEngineClient;
use palimpsest_server::cursor::RawDiff;
use palimpsest_server::snapshot::{SnapshotBatch, SnapshotTableRows};
use palimpsest_server::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_server::{AnonymousAuthenticator, Palimpsest, TraceCursor};
use palimpsest_sql::{Catalog, ColumnSchema, ColumnType, TableSchema};
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

// -----------------------------------------------------------------------------
// Test WAL runtime: a tiny `events` table whose snapshot + journal
// are both backed by an `Arc<Mutex<>>` so the test code can push
// mutations after the subscription has opened.
// -----------------------------------------------------------------------------

const EVENTS_TABLE_ID: u32 = 16_400;

#[derive(Clone)]
struct TestEventsWal {
    rows: Arc<Mutex<Vec<Row>>>,
    journal: Arc<Mutex<Vec<RawDiff>>>,
    lsn: Arc<Mutex<u64>>,
}

impl TestEventsWal {
    fn new(seed: Vec<Row>) -> Self {
        Self {
            rows: Arc::new(Mutex::new(seed)),
            journal: Arc::new(Mutex::new(Vec::new())),
            lsn: Arc::new(Mutex::new(1)),
        }
    }

    fn push_event(&self, row: Row) {
        let mut lsn_guard = self.lsn.lock().expect("lsn lock");
        *lsn_guard += 1;
        let new_lsn = Lsn::new(*lsn_guard);
        drop(lsn_guard);

        self.rows.lock().expect("rows lock").push(row.clone());
        self.journal.lock().expect("journal lock").push(RawDiff {
            table: None,
            row,
            lsn: new_lsn,
            diff: 1,
        });
    }
}

impl WalRuntime for TestEventsWal {
    fn fetch_snapshot(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        let rows = self.rows.lock().expect("rows lock").clone();
        let snapshot_lsn = Lsn::new(*self.lsn.lock().expect("lsn lock"));
        Ok(SnapshotBatch {
            snapshot_lsn,
            rows: vec![SnapshotTableRows {
                table: TableId::new(EVENTS_TABLE_ID),
                rows,
            }],
        })
    }

    fn query_schema(&self, _query: &QueryId) -> Result<SchemaDefinition, String> {
        Ok(SchemaDefinition {
            id: SchemaId::new(0),
            columns: vec![
                ColumnSpec {
                    name: "id".to_owned(),
                    datum_type: DatumType::I64,
                    nullable: false,
                },
                ColumnSpec {
                    name: "category_id".to_owned(),
                    datum_type: DatumType::I64,
                    nullable: false,
                },
                ColumnSpec {
                    name: "value".to_owned(),
                    datum_type: DatumType::I64,
                    nullable: false,
                },
            ],
            primary_key_columns: vec![0],
        })
    }

    fn open_cursor(
        &self,
        _query: &QueryId,
        from_lsn: Lsn,
    ) -> Result<Box<dyn TraceCursor + Send>, String> {
        Ok(Box::new(JournalCursor {
            journal: Arc::clone(&self.journal),
            next_index: 0,
            from_lsn,
        }))
    }

    fn table_schema(&self, table: &str) -> Option<(TableId, ScalarSchema)> {
        if table == "events" {
            Some((
                TableId::new(EVENTS_TABLE_ID),
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("category_id".to_owned(), ColumnType::Int),
                    ("value".to_owned(), ColumnType::Int),
                ]),
            ))
        } else {
            None
        }
    }
}

struct JournalCursor {
    journal: Arc<Mutex<Vec<RawDiff>>>,
    next_index: usize,
    from_lsn: Lsn,
}

impl TraceCursor for JournalCursor {
    fn next_diff(&mut self) -> Option<RawDiff> {
        let journal = self.journal.lock().expect("journal lock");
        while let Some(entry) = journal.get(self.next_index) {
            self.next_index += 1;
            if entry.lsn > self.from_lsn {
                return Some(entry.clone());
            }
        }
        None
    }

    fn peek_lsn(&self) -> Option<Lsn> {
        let journal = self.journal.lock().expect("journal lock");
        let mut idx = self.next_index;
        while let Some(entry) = journal.get(idx) {
            if entry.lsn > self.from_lsn {
                return Some(entry.lsn);
            }
            idx += 1;
        }
        None
    }
}

// -----------------------------------------------------------------------------
// Server harness
// -----------------------------------------------------------------------------

struct AggregateHarness {
    wal: TestEventsWal,
    grpc_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl AggregateHarness {
    async fn start(seed: Vec<Row>) -> Self {
        Self::start_with_rules(seed, Vec::new()).await
    }

    async fn start_with_rules(seed: Vec<Row>, rules: Vec<CompiledRule>) -> Self {
        let wal = TestEventsWal::new(seed);
        let grpc_addr: SocketAddr = bind_ephemeral();

        let mut builder = Palimpsest::builder()
            .with_wal(wal.clone())
            .with_auth(AnonymousAuthenticator)
            .with_grpc_addr(grpc_addr)
            .with_metrics_addr(None);
        if !rules.is_empty() {
            builder = builder.with_permissions(rules);
        }
        let server = builder.build().expect("build server");

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let _ = server
                .serve(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        wait_for_port(grpc_addr).await;

        Self {
            wal,
            grpc_addr,
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    async fn channel(&self) -> Channel {
        Endpoint::from_shared(format!("http://{}", self.grpc_addr))
            .expect("endpoint")
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .expect("connect")
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        }
    }
}

fn bind_ephemeral() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    addr
}

async fn wait_for_port(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("port {addr} never became reachable");
}

fn event_row(id: i64, category: i64, value: i64) -> Row {
    smallvec![Datum::I64(id), Datum::I64(category), Datum::I64(value)]
}

fn subscribe_message(client_id: &str, sql: &str) -> proto::ClientMessage {
    proto::ClientMessage {
        kind: Some(proto::client_message::Kind::Subscribe(
            proto::SubscribeRequest {
                client_subscription_id: client_id.to_owned(),
                sql: sql.to_owned(),
                vars: HashMap::new(),
                resume_lsn: None,
            },
        )),
    }
}

/// Decode a `Diff` event's row bytes (proto-encoded list of rows)
/// using the supplied schema, mirroring what the wasm client does.
/// Returns the rows in proto's natural order, with each `WireDatum`
/// mapped onto the equivalent `Datum` so tests can compare against
/// the same values the WAL runtime produces.
fn decode_diff_rows(diff: &proto::Diff, schema: &proto::Schema) -> Vec<Row> {
    use palimpsest_proto::wire::{SchemaRegistry, WireDatum};
    let mut registry = SchemaRegistry::new();
    registry.register(diff.schema_id, schema.clone());
    let wire_rows = registry.decode(diff).expect("decode diff");
    wire_rows
        .into_iter()
        .map(|wire_row| {
            wire_row
                .into_iter()
                .map(|wd| match wd {
                    WireDatum::Bool(b) => Datum::Bool(b),
                    WireDatum::I16(v) => Datum::I16(v),
                    WireDatum::I32(v) => Datum::I32(v),
                    WireDatum::I64(v) => Datum::I64(v),
                    WireDatum::F32(v) => Datum::F32(v),
                    WireDatum::F64(v) => Datum::F64(v),
                    WireDatum::Text(b) => Datum::Text(b.into()),
                    WireDatum::Null => Datum::Null,
                    other => panic!("unexpected wire datum in test: {other:?}"),
                })
                .collect::<Row>()
        })
        .collect()
}

fn decode_transaction_rows(update: &proto::TransactionUpdate, schema: &proto::Schema) -> Vec<Row> {
    use palimpsest_proto::wire::{SchemaRegistry, WireDatum};
    let mut registry = SchemaRegistry::new();
    registry.register(update.schema_id, schema.clone());
    let changes = registry
        .decode_transaction(update)
        .expect("decode transaction");
    changes
        .into_iter()
        .filter_map(|change| change.new.or(change.old))
        .map(|wire_row| {
            wire_row
                .into_iter()
                .map(|wd| match wd {
                    WireDatum::Bool(b) => Datum::Bool(b),
                    WireDatum::I16(v) => Datum::I16(v),
                    WireDatum::I32(v) => Datum::I32(v),
                    WireDatum::I64(v) => Datum::I64(v),
                    WireDatum::F32(v) => Datum::F32(v),
                    WireDatum::F64(v) => Datum::F64(v),
                    WireDatum::Text(b) => Datum::Text(b.into()),
                    WireDatum::Null => Datum::Null,
                    other => panic!("unexpected wire datum in test: {other:?}"),
                })
                .collect::<Row>()
        })
        .collect()
}

const AGGREGATE_SQL: &str = "WITH per_category AS (
    SELECT category_id, COUNT(*) AS n, SUM(value) AS total
    FROM events
    GROUP BY category_id
)
SELECT category_id, n, total
FROM per_category
ORDER BY total DESC
LIMIT 8";

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_event_carries_server_side_aggregate_rows() {
    let seed = vec![
        event_row(1, 7, 100),
        event_row(2, 7, 50),
        event_row(3, 9, 20),
    ];
    let harness = AggregateHarness::start(seed).await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("agg", AGGREGATE_SQL))
        .await
        .expect("send subscribe");

    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe call")
        .into_inner();

    let accepted_msg = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .expect("recv accepted timed out")
        .expect("stream closed")
        .expect("status");
    let accepted = match accepted_msg.kind.expect("accepted kind") {
        proto::server_message::Kind::Accepted(a) => a,
        other => panic!("expected Accepted, got {other:?}"),
    };
    let schema = accepted.schema.expect("schema attached");
    assert_eq!(
        schema.columns.len(),
        3,
        "aggregate output has 3 cols: category_id, n, total",
    );
    assert_eq!(schema.columns[0].name, "category_id");
    assert_eq!(schema.columns[1].name, "n");
    assert_eq!(schema.columns[2].name, "total");

    // The first Diff event after Accepted carries the initial
    // aggregate rows (op = INITIAL).
    let initial_msg = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .expect("recv initial timed out")
        .expect("stream closed")
        .expect("status");
    let initial = match initial_msg.kind.expect("initial kind") {
        proto::server_message::Kind::Diff(d) => d,
        other => panic!("expected Diff (Initial), got {other:?}"),
    };
    assert_eq!(initial.op, proto::DiffOp::Initial as i32);

    let rows = decode_diff_rows(&initial, &schema);
    assert_eq!(
        rows.len(),
        2,
        "two distinct categories in seed (7, 9); got {rows:?}",
    );
    for row in &rows {
        let category = match row.first() {
            Some(Datum::I64(v)) => *v,
            _ => panic!("category_id missing"),
        };
        let count = match row.get(1) {
            Some(Datum::I64(v)) => *v,
            _ => panic!("n missing"),
        };
        let total = match row.get(2) {
            Some(Datum::I64(v)) => *v,
            _ => panic!("total missing"),
        };
        match category {
            7 => {
                assert_eq!(count, 2, "cat 7 has 2 events");
                assert_eq!(total, 150, "cat 7 sums to 150");
            }
            9 => {
                assert_eq!(count, 1, "cat 9 has 1 event");
                assert_eq!(total, 20, "cat 9 sums to 20");
            }
            other => panic!("unexpected category {other}"),
        }
    }

    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(1), response.next()).await;
    harness.shutdown().await;
}

/// Build a catalog that knows about the test's `events` table so
/// `compile_rules` can resolve column references in a rule like
/// `value >= 50`.
fn events_catalog() -> Catalog {
    Catalog::new([TableSchema::new(
        "events",
        vec![
            ColumnSchema::new("id", ColumnType::Int),
            ColumnSchema::new("category_id", ColumnType::Int),
            ColumnSchema::new("value", ColumnType::Int),
        ],
    )])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permission_filter_threads_through_to_aggregate() {
    // Row-visibility rule: only events with value >= 50 contribute
    // to the aggregate. Both the snapshot and live diffs flow
    // through the same compiled filter.
    let rule = PermissionRule::new("events_min_value", "events", "value >= 50");
    let user_schema = UserContextSchema::default();
    let rules =
        compile_rules(&[rule], &events_catalog(), &user_schema).expect("compile permission rule");

    let seed = vec![
        event_row(1, 7, 100), // counted
        event_row(2, 7, 50),  // counted (>= 50)
        event_row(3, 7, 10),  // filtered out
        event_row(4, 9, 80),  // counted
    ];
    let harness = AggregateHarness::start_with_rules(seed, rules).await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("agg-perm", AGGREGATE_SQL))
        .await
        .expect("send subscribe");
    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe call")
        .into_inner();

    let accepted_msg = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .expect("accepted timeout")
        .expect("stream closed")
        .expect("status");
    let schema = match accepted_msg.kind.expect("accepted kind") {
        proto::server_message::Kind::Accepted(a) => a.schema.expect("schema"),
        other => panic!("expected Accepted, got {other:?}"),
    };
    let initial_msg = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .expect("initial timeout")
        .expect("stream closed")
        .expect("status");
    let initial = match initial_msg.kind.expect("initial kind") {
        proto::server_message::Kind::Diff(d) => d,
        other => panic!("expected Initial Diff, got {other:?}"),
    };

    let rows = decode_diff_rows(&initial, &schema);
    assert_eq!(rows.len(), 2, "two categories visible after filter");
    for row in &rows {
        let cat = match row.first() {
            Some(Datum::I64(v)) => *v,
            _ => panic!("category_id missing"),
        };
        let count = match row.get(1) {
            Some(Datum::I64(v)) => *v,
            _ => panic!("n missing"),
        };
        let total = match row.get(2) {
            Some(Datum::I64(v)) => *v,
            _ => panic!("total missing"),
        };
        match cat {
            7 => {
                // (id=3, value=10) is excluded by the filter.
                assert_eq!(count, 2);
                assert_eq!(total, 150);
            }
            9 => {
                assert_eq!(count, 1);
                assert_eq!(total, 80);
            }
            other => panic!("unexpected category {other}"),
        }
    }

    // Push an event below the threshold — should NOT change the
    // aggregate, so we expect no Diff event within a short window.
    harness.wal.push_event(event_row(5, 7, 5));
    let no_diff = tokio::time::timeout(Duration::from_millis(500), response.next()).await;
    assert!(
        no_diff.is_err(),
        "filtered-out diff should not produce an aggregate update; got {no_diff:?}",
    );

    // Push an event ABOVE the threshold — should change the aggregate.
    harness.wal.push_event(event_row(6, 9, 200));
    let msg = tokio::time::timeout(Duration::from_secs(3), response.next())
        .await
        .expect("diff timeout")
        .expect("stream closed")
        .expect("status");
    let update_msg = match msg.kind.expect("diff kind") {
        proto::server_message::Kind::TransactionUpdate(update) => update,
        other => panic!("unexpected message: {other:?}"),
    };
    let updated = decode_transaction_rows(&update_msg, &schema);
    let new_cat_9 = updated
        .iter()
        .find(|r| matches!(r.first(), Some(Datum::I64(9))))
        .expect("cat 9 in update");
    assert_eq!(new_cat_9.get(1), Some(&Datum::I64(2)));
    assert_eq!(new_cat_9.get(2), Some(&Datum::I64(280))); // 80 + 200

    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(1), response.next()).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_subscribers_on_same_sql_both_see_live_diffs() {
    // Two concurrent subscriptions to the *same* query. The cursor
    // pump assigns each its own `host_canonical_key` (sub-N), so
    // they get independent state today — but both should observe
    // the same WAL mutation's effect on the aggregate. This test
    // pins that behaviour against regressions; SharedSubgraphRegistry
    // refcount reuse is a follow-up that would make them literally
    // share state.
    let seed = vec![event_row(1, 7, 100), event_row(2, 9, 30)];
    let harness = AggregateHarness::start(seed).await;

    let mut client_a = SyncEngineClient::new(harness.channel().await);
    let mut client_b = SyncEngineClient::new(harness.channel().await);

    let (tx_a, rx_a) = tokio::sync::mpsc::channel(8);
    tx_a.send(subscribe_message("agg-a", AGGREGATE_SQL))
        .await
        .expect("send a");
    let mut resp_a = client_a
        .subscribe(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx_a,
        )))
        .await
        .expect("sub a")
        .into_inner();

    let (tx_b, rx_b) = tokio::sync::mpsc::channel(8);
    tx_b.send(subscribe_message("agg-b", AGGREGATE_SQL))
        .await
        .expect("send b");
    let mut resp_b = client_b
        .subscribe(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx_b,
        )))
        .await
        .expect("sub b")
        .into_inner();

    // Both subscribers should see their own Accepted + Initial.
    for resp in [&mut resp_a, &mut resp_b] {
        let accepted = tokio::time::timeout(Duration::from_secs(2), resp.next())
            .await
            .expect("accepted timeout")
            .expect("stream closed")
            .expect("status");
        assert!(matches!(
            accepted.kind,
            Some(proto::server_message::Kind::Accepted(_))
        ));
        let initial = tokio::time::timeout(Duration::from_secs(2), resp.next())
            .await
            .expect("initial timeout")
            .expect("stream closed")
            .expect("status");
        assert!(matches!(
            initial.kind,
            Some(proto::server_message::Kind::Diff(_))
        ));
    }

    // Single mutation — both subscribers should observe a transaction event.
    harness.wal.push_event(event_row(3, 7, 200));

    let next_a = tokio::time::timeout(Duration::from_secs(3), resp_a.next())
        .await
        .expect("a diff timeout")
        .expect("a stream closed")
        .expect("a status");
    let next_b = tokio::time::timeout(Duration::from_secs(3), resp_b.next())
        .await
        .expect("b diff timeout")
        .expect("b stream closed")
        .expect("b status");
    for msg in [next_a, next_b] {
        match msg.kind.expect("diff kind") {
            proto::server_message::Kind::TransactionUpdate(update) => {
                // Both subscribers see the cat-7 update.
                assert!(update.changes.iter().any(|change| {
                    change.op == proto::DiffOp::Update as i32
                        || change.op == proto::DiffOp::Insert as i32
                }));
            }
            other => panic!("expected TransactionUpdate, got {other:?}"),
        }
    }

    drop(tx_a);
    drop(tx_b);
    let _ = tokio::time::timeout(Duration::from_secs(1), resp_a.next()).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), resp_b.next()).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsubscribe_resubscribe_rebuilds_host_state() {
    // Confirms the host's release-on-disconnect + re-seed-on-resubscribe
    // cycle works without leaking stale aggregate state across runs.
    let seed = vec![event_row(1, 7, 100)];
    let harness = AggregateHarness::start(seed).await;

    // First subscription.
    {
        let mut client = SyncEngineClient::new(harness.channel().await);
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send(subscribe_message("first", AGGREGATE_SQL))
            .await
            .expect("send first");
        let mut resp = client
            .subscribe(Request::new(tokio_stream::wrappers::ReceiverStream::new(
                rx,
            )))
            .await
            .expect("sub first")
            .into_inner();
        // Drain Accepted + Initial then drop.
        let _ = tokio::time::timeout(Duration::from_secs(2), resp.next()).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), resp.next()).await;
        drop(tx);
        // Let the cursor pump notice the subscription is gone before
        // we tear down.
        let _ = tokio::time::timeout(Duration::from_millis(200), resp.next()).await;
    }

    // Mutate while no subscriber holds state — the journal grows but
    // the host's state for `first` was already released.
    harness.wal.push_event(event_row(2, 9, 50));

    // Fresh resubscribe. The new subscription should pick up the
    // current snapshot (id 1 + 2), not stale state from the prior
    // subscription.
    let mut client = SyncEngineClient::new(harness.channel().await);
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("second", AGGREGATE_SQL))
        .await
        .expect("send second");
    let mut resp = client
        .subscribe(Request::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
        .await
        .expect("sub second")
        .into_inner();

    let accepted = tokio::time::timeout(Duration::from_secs(2), resp.next())
        .await
        .expect("accepted timeout")
        .expect("stream closed")
        .expect("status");
    let schema = match accepted.kind.expect("accepted kind") {
        proto::server_message::Kind::Accepted(a) => a.schema.expect("schema"),
        other => panic!("expected Accepted, got {other:?}"),
    };
    let initial = tokio::time::timeout(Duration::from_secs(2), resp.next())
        .await
        .expect("initial timeout")
        .expect("stream closed")
        .expect("status");
    let initial = match initial.kind.expect("initial kind") {
        proto::server_message::Kind::Diff(d) => d,
        other => panic!("expected Initial Diff, got {other:?}"),
    };
    let rows = decode_diff_rows(&initial, &schema);
    assert_eq!(
        rows.len(),
        2,
        "fresh sub should see both seeded + mid-mutation rows, got {rows:?}",
    );

    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(1), resp.next()).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_diff_produces_aggregate_update() {
    let seed = vec![event_row(1, 7, 100), event_row(2, 9, 20)];
    let harness = AggregateHarness::start(seed).await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("agg-live", AGGREGATE_SQL))
        .await
        .expect("send subscribe");

    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe call")
        .into_inner();

    // Drain Accepted + Initial.
    let accepted_msg = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .expect("accepted timeout")
        .expect("stream closed")
        .expect("status");
    let schema = match accepted_msg.kind.expect("accepted kind") {
        proto::server_message::Kind::Accepted(a) => a.schema.expect("schema"),
        other => panic!("expected Accepted, got {other:?}"),
    };
    let initial_msg = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .expect("initial timeout")
        .expect("stream closed")
        .expect("status");
    match initial_msg.kind.expect("initial kind") {
        proto::server_message::Kind::Diff(_) => {}
        other => panic!("expected Initial Diff, got {other:?}"),
    }

    // Push a new event into category 9. The cursor pump runs the
    // dataflow over the cumulative input and should emit a retract
    // (9, 1, 20) + assert (9, 2, 120) — which the router pairs into
    // an UPDATE.
    harness.wal.push_event(event_row(3, 9, 100));

    // Wait for the next Diff event (cursor polls every 50ms).
    let msg = tokio::time::timeout(Duration::from_secs(3), response.next())
        .await
        .expect("diff timeout")
        .expect("stream closed")
        .expect("status");
    let update_msg = match msg.kind.expect("diff kind") {
        proto::server_message::Kind::TransactionUpdate(update) => update,
        other => panic!("unexpected message between Initial and TransactionUpdate: {other:?}"),
    };

    assert!(
        update_msg
            .changes
            .iter()
            .any(|change| change.op == proto::DiffOp::Update as i32),
        "cat-9 retract+assert pair should collapse to UPDATE",
    );
    let rows = decode_transaction_rows(&update_msg, &schema);
    let new_cat_9 = rows
        .iter()
        .find(|r| matches!(r.first(), Some(Datum::I64(9))))
        .expect("cat 9 in update payload");
    assert_eq!(new_cat_9.get(1), Some(&Datum::I64(2)), "n = 2");
    assert_eq!(new_cat_9.get(2), Some(&Datum::I64(120)), "total = 120");

    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(1), response.next()).await;
    harness.shutdown().await;
}
