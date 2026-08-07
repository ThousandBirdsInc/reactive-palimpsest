// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Full-stack live-diff scenario: a committed upstream write must
//! surface as a live `ServerMessage` diff on an **open** gRPC
//! subscription, with no client-side re-subscribe.
//!
//! This drives the real wire path — `palimpsest-client` →
//! tonic/gRPC → `SyncEngineService` → `SubscriptionRouter::pump_cursor`
//! → `BoundedDiffChannel` → outbound forwarder — against a WAL runtime
//! backed by an appendable diff journal (the same shape the demo app's
//! Postgres logical-replication mirror uses).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use palimpsest_client::{Auth, Client, DiffEvent, WireDatum};
use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_server::snapshot::{SnapshotBatch, SnapshotTableRows};
use palimpsest_server::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_server::{Palimpsest, RawDiff, TraceCursor};
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;

/// WAL runtime backed by an appendable in-memory diff journal. The
/// test appends to the journal *after* the subscription is open; the
/// server's cursor pump must pick the entries up and push them down
/// the live stream.
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

/// Cursor over the shared journal, anchored at `from_lsn`.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_write_reaches_open_subscription_without_resubscribe() {
    let (runtime, journal) = JournalWalRuntime::new();

    let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let grpc_addr = grpc_listener.local_addr().expect("addr");
    drop(grpc_listener);

    let server = Palimpsest::builder()
        .with_wal(runtime)
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
    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("subscribe");

    // Snapshot handshake: Accepted, then the Initial rows.
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

    // The "committed write": append to the journal after the
    // subscription is live. No client action follows — the diff must
    // arrive on the already-open stream.
    journal.lock().expect("journal").push(RawDiff {
        table: None,
        row: smallvec![Datum::I64(42)],
        lsn: Lsn::new(2),
        diff: 1,
    });

    let live = next_event_within(&mut sub, Duration::from_secs(5)).await;
    let inserted_row_present = match &live {
        DiffEvent::Transaction { changes, .. } => changes.iter().any(|change| {
            change
                .new
                .as_ref()
                .is_some_and(|row| row.contains(&WireDatum::I64(42)))
        }),
        DiffEvent::Diff { rows, .. } => rows.iter().any(|row| row.contains(&WireDatum::I64(42))),
        other => panic!("expected live diff, got {other:?}"),
    };
    assert!(
        inserted_row_present,
        "live event must carry the written row: {live:?}"
    );

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
}
