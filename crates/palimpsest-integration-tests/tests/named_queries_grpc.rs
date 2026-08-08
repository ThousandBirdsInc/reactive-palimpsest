// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Named prepared queries over the real wire: the server registers an
//! sqlc-format query file at startup, the client subscribes by
//! `{name, params}` only, and:
//!
//! * a registered name streams the snapshot + live diffs,
//! * an unknown name is refused (`unknown_query`, fail closed),
//! * raw SQL is refused (`inline_sql_disabled`) because configuring a
//!   registry makes registered queries the only reachable surface,
//! * ill-typed params are refused (`invalid_params`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use palimpsest_client::{var_value, Auth, Client, DiffEvent, VarValue, WireDatum};
use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_server::snapshot::{SnapshotBatch, SnapshotTableRows};
use palimpsest_server::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_server::{Palimpsest, RawDiff, TraceCursor};
use palimpsest_sql::prepared::QueryRegistry;
use palimpsest_sql::{Catalog, ColumnSchema, ColumnType, TableSchema};
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;

const QUERIES_SQL: &str = "\
-- name: AllPosts :many
SELECT id FROM posts;

-- name: PostById :one
SELECT id FROM posts WHERE id = $1;
";

fn posts_catalog() -> Catalog {
    Catalog::new([TableSchema::new(
        "posts",
        vec![ColumnSchema::new("id", ColumnType::Int)],
    )])
}

/// WAL runtime over an appendable in-memory journal (same shape as the
/// `live_diff_grpc` scenario).
struct JournalWalRuntime {
    journal: Arc<Mutex<Vec<RawDiff>>>,
}

impl JournalWalRuntime {
    fn new() -> (Self, Arc<Mutex<Vec<RawDiff>>>) {
        let journal = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                journal: Arc::clone(&journal),
            },
            journal,
        )
    }
}

impl WalRuntime for JournalWalRuntime {
    fn fetch_snapshot(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        Ok(SnapshotBatch {
            snapshot_lsn: Lsn::new(1),
            rows: vec![SnapshotTableRows {
                table: TableId::new(1),
                rows: vec![smallvec![Datum::I64(1)]],
            }],
        })
    }

    fn query_schema(&self, _query: &QueryId) -> Result<SchemaDefinition, String> {
        Ok(SchemaDefinition {
            id: SchemaId::new(0),
            columns: vec![ColumnSpec {
                name: "id".to_owned(),
                datum_type: DatumType::I64,
                nullable: false,
            }],
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
}

struct JournalCursor {
    journal: Arc<Mutex<Vec<RawDiff>>>,
    next_index: usize,
    from_lsn: Lsn,
}

impl TraceCursor for JournalCursor {
    fn next_diff(&mut self) -> Option<RawDiff> {
        let journal = self.journal.lock().expect("journal");
        while let Some(entry) = journal.get(self.next_index) {
            self.next_index += 1;
            if entry.lsn > self.from_lsn {
                return Some(entry.clone());
            }
        }
        None
    }

    fn peek_lsn(&self) -> Option<Lsn> {
        let journal = self.journal.lock().expect("journal");
        let mut index = self.next_index;
        while let Some(entry) = journal.get(index) {
            if entry.lsn > self.from_lsn {
                return Some(entry.lsn);
            }
            index += 1;
        }
        None
    }
}

async fn next_event_within(
    sub: &mut palimpsest_client::Subscription,
    timeout: Duration,
) -> DiffEvent {
    tokio::time::timeout(timeout, sub.next_event())
        .await
        .expect("timed out waiting for event")
        .expect("stream closed")
        .expect("event error")
}

fn int_param(value: i64) -> VarValue {
    VarValue {
        kind: Some(var_value::Kind::IntValue(value)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_queries_are_the_only_reachable_surface() {
    let (runtime, journal) = JournalWalRuntime::new();

    // Registration happens at startup and would abort the process on
    // failure; `expect` here is that same loudness.
    let mut registry = QueryRegistry::new();
    registry
        .register_sqlc_source(QUERIES_SQL, "queries.sql", &posts_catalog())
        .expect("register sqlc query file");

    let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let grpc_addr = grpc_listener.local_addr().expect("addr");
    drop(grpc_listener);

    let server = Palimpsest::builder()
        .with_wal(runtime)
        .with_query_registry(registry)
        .with_grpc_addr(grpc_addr)
        .with_metrics_addr(None)
        .build()
        .expect("build server");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = server
            .serve(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(grpc_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let client = Client::connect(format!("http://{grpc_addr}"), Auth::Anonymous)
        .await
        .expect("connect");

    // 1. Raw SQL is refused: the registry is the authorization
    //    boundary, so the pre-registry subscribe path fails closed.
    let mut raw = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("send subscribe");
    let refusal = next_event_within(&mut raw, Duration::from_secs(5)).await;
    let DiffEvent::Error { code, .. } = &refusal else {
        panic!("raw SQL must be refused, got {refusal:?}");
    };
    assert_eq!(code, "inline_sql_disabled");

    // 2. Unknown names are refused.
    let mut unknown = client
        .subscribe_named("NotRegistered")
        .await
        .expect("send subscribe");
    let refusal = next_event_within(&mut unknown, Duration::from_secs(5)).await;
    let DiffEvent::Error { code, .. } = &refusal else {
        panic!("unknown name must be refused, got {refusal:?}");
    };
    assert_eq!(code, "unknown_query");

    // 3. Ill-typed params are refused.
    let mut illtyped = client
        .subscribe_named_with(
            "PostById",
            HashMap::from([(
                "id".to_owned(),
                VarValue {
                    kind: Some(var_value::Kind::StringValue("not-a-number".to_owned())),
                },
            )]),
        )
        .await
        .expect("send subscribe");
    let refusal = next_event_within(&mut illtyped, Duration::from_secs(5)).await;
    let DiffEvent::Error { code, .. } = &refusal else {
        panic!("ill-typed param must be refused, got {refusal:?}");
    };
    assert_eq!(code, "invalid_params");

    // 4. A registered name with well-typed params serves the snapshot
    //    and live diffs — full parity with the raw-SQL path.
    let mut sub = client
        .subscribe_named_with("PostById", HashMap::from([("id".to_owned(), int_param(1))]))
        .await
        .expect("send subscribe");
    let accepted = next_event_within(&mut sub, Duration::from_secs(5)).await;
    assert!(
        matches!(accepted, DiffEvent::Accepted { .. }),
        "{accepted:?}"
    );
    let initial = next_event_within(&mut sub, Duration::from_secs(5)).await;
    let DiffEvent::Diff { rows, .. } = &initial else {
        panic!("expected Initial diff, got {initial:?}");
    };
    assert_eq!(rows.len(), 1);

    // Committed write after the subscription is live: the diff must
    // arrive with no client action. (id = 1 matches the bound filter.)
    journal.lock().expect("journal").push(RawDiff {
        table: None,
        row: smallvec![Datum::I64(1)],
        lsn: Lsn::new(2),
        diff: 1,
    });
    let live = next_event_within(&mut sub, Duration::from_secs(5)).await;
    let live_row_present = match &live {
        DiffEvent::Transaction { changes, .. } => changes.iter().any(|change| {
            change
                .new
                .as_ref()
                .is_some_and(|row| row.contains(&WireDatum::I64(1)))
        }),
        DiffEvent::Diff { rows, .. } => rows.iter().any(|row| row.contains(&WireDatum::I64(1))),
        other => panic!("expected live diff, got {other:?}"),
    };
    assert!(
        live_row_present,
        "live event must carry the written row: {live:?}"
    );

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}
