// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end tests for the local-first replica, driving a hand-built
//! subscription (no server) against the in-memory database.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::mpsc;

use palimpsest_proto::palimpsest::sync::v1::{Column, DatumType, DiffOp, ResyncReason, Schema};
use palimpsest_proto::wire::{WireDatum, WireRow, WireRowChange};

use crate::connection::{Command, ConnectionInbox};
use crate::error::ClientError;
use crate::subscription::{DiffEvent, Subscription};

use super::db::DbFuture;
use super::memory::MemoryDatabase;
use super::replica::{
    LocalReplica, MirrorConfig, MirrorQuery, Mutation, RemoteWriter, ReplicaEvent, TableSyncState,
    WriteRequest,
};

fn schema() -> Schema {
    Schema {
        columns: vec![
            Column {
                name: "id".into(),
                r#type: DatumType::I64.into(),
                nullable: false,
            },
            Column {
                name: "title".into(),
                r#type: DatumType::Text.into(),
                nullable: true,
            },
        ],
        primary_key_columns: vec![0],
    }
}

fn row(id: i64, title: &str) -> WireRow {
    vec![
        WireDatum::I64(id),
        WireDatum::Text(title.as_bytes().to_vec()),
    ]
}

struct Harness {
    replica: LocalReplica,
    db: Arc<MemoryDatabase>,
    events_tx: mpsc::Sender<Result<DiffEvent, ClientError>>,
    /// Commands (acks, updates) issued by the pump.
    commands_rx: mpsc::Receiver<Command>,
}

fn harness(writer: Option<Arc<dyn RemoteWriter>>) -> Harness {
    let (inbox, commands_rx) = ConnectionInbox::detached();
    let (events_tx, events_rx) = mpsc::channel(64);
    let sub = Subscription {
        id: "sub-test".into(),
        inbox,
        events: events_rx,
        cache: None,
    };
    let db = Arc::new(MemoryDatabase::new());
    let mirror = MirrorConfig {
        local_table: "posts".into(),
        query: MirrorQuery::Sql("SELECT * FROM posts".into()),
        vars: std::collections::HashMap::new(),
    };
    let replica = LocalReplica::assemble(db.clone(), writer, vec![(mirror, sub)]);
    Harness {
        replica,
        db,
        events_tx,
        commands_rx,
    }
}

async fn accepted(h: &Harness) {
    h.events_tx
        .send(Ok(DiffEvent::Accepted {
            schema_id: 1,
            snapshot_lsn: 10,
            schema: schema(),
        }))
        .await
        .unwrap();
}

async fn initial(h: &Harness, lsn: u64, rows: Vec<WireRow>) {
    h.events_tx
        .send(Ok(DiffEvent::Diff {
            lsn,
            op: DiffOp::Initial,
            rows,
        }))
        .await
        .unwrap();
}

async fn transaction(h: &Harness, commit_lsn: u64, changes: Vec<WireRowChange>) {
    h.events_tx
        .send(Ok(DiffEvent::Transaction {
            commit_lsn,
            begin_lsn: None,
            end_lsn: None,
            transaction_id: None,
            changes,
        }))
        .await
        .unwrap();
}

/// Wait (bounded) until the mirror reports the given live LSN.
async fn wait_live(h: &Harness, lsn: u64) {
    wait_for(|| async {
        h.replica.table_states().get("posts") == Some(&TableSyncState::Live { lsn })
    })
    .await;
}

async fn wait_for<F, Fut>(mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..500 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition not reached within timeout");
}

#[tokio::test]
async fn snapshot_then_incremental_commits() {
    let h = harness(None);
    accepted(&h).await;
    initial(&h, 10, vec![row(1, "a"), row(2, "b")]).await;
    wait_live(&h, 10).await;
    assert_eq!(h.db.snapshot("posts").len(), 2);

    transaction(
        &h,
        20,
        vec![
            WireRowChange {
                op: DiffOp::Update,
                old: Some(row(1, "a")),
                new: Some(row(1, "a2")),
            },
            WireRowChange {
                op: DiffOp::Delete,
                old: Some(row(2, "b")),
                new: None,
            },
            WireRowChange {
                op: DiffOp::Insert,
                old: None,
                new: Some(row(3, "c")),
            },
        ],
    )
    .await;
    wait_live(&h, 20).await;
    let rows = h.db.snapshot("posts");
    assert_eq!(rows, vec![row(1, "a2"), row(3, "c")]);
}

#[tokio::test]
async fn acks_each_applied_lsn() {
    let mut h = harness(None);
    accepted(&h).await;
    initial(&h, 10, vec![row(1, "a")]).await;
    wait_live(&h, 10).await;
    let cmd = h.commands_rx.recv().await.expect("ack command");
    match cmd {
        Command::Ack {
            subscription_id,
            lsn,
        } => {
            assert_eq!(subscription_id, "sub-test");
            assert_eq!(lsn, 10);
        }
        _ => panic!("expected ack"),
    }
}

#[tokio::test]
async fn resync_replaces_snapshot_and_reissues_query() {
    let mut h = harness(None);
    accepted(&h).await;
    initial(&h, 10, vec![row(1, "a"), row(2, "b")]).await;
    wait_live(&h, 10).await;

    h.events_tx
        .send(Ok(DiffEvent::Resync {
            reason: ResyncReason::PermissionsChanged,
            message: "rules changed".into(),
        }))
        .await
        .unwrap();
    // Row 2 is no longer visible under the new rules.
    initial(&h, 30, vec![row(1, "a")]).await;
    wait_live(&h, 30).await;
    assert_eq!(h.db.snapshot("posts"), vec![row(1, "a")]);

    // The pump re-issued the query via an Update command.
    let mut saw_update = false;
    while let Ok(cmd) = h.commands_rx.try_recv() {
        if matches!(cmd, Command::Update { .. }) {
            saw_update = true;
        }
    }
    assert!(saw_update, "resync should re-issue the mirror query");
}

/// Writer that records requests and completes when released.
struct GatedWriter {
    requests: StdMutex<Vec<WriteRequest>>,
    result: StdMutex<Result<(), String>>,
    gate: tokio::sync::Semaphore,
}

impl GatedWriter {
    fn new(result: Result<(), String>) -> Arc<Self> {
        Arc::new(Self {
            requests: StdMutex::new(Vec::new()),
            result: StdMutex::new(result),
            gate: tokio::sync::Semaphore::new(0),
        })
    }

    fn release(&self) {
        self.gate.add_permits(1);
    }
}

impl RemoteWriter for GatedWriter {
    fn write(&self, request: WriteRequest) -> DbFuture<'_, Result<(), String>> {
        self.requests.lock().unwrap().push(request);
        Box::pin(async move {
            let _permit = self.gate.acquire().await.unwrap();
            self.result.lock().unwrap().clone()
        })
    }
}

#[tokio::test]
async fn optimistic_update_applies_locally_then_settles() {
    let writer = GatedWriter::new(Ok(()));
    let h = harness(Some(writer.clone()));
    let mut events = h.replica.take_events().unwrap();
    accepted(&h).await;
    initial(&h, 10, vec![row(1, "a")]).await;
    wait_live(&h, 10).await;

    let token = h
        .replica
        .mutate(Mutation::update(
            "posts",
            [("id", WireDatum::I64(1))],
            [("title", WireDatum::Text(b"mine".to_vec()))],
        ))
        .await
        .unwrap();
    // Optimistic write is immediately visible locally.
    assert_eq!(h.db.snapshot("posts"), vec![row(1, "mine")]);
    assert_eq!(h.replica.pending_mutations("posts").await, 1);
    // The writer task is spawned; wait for the request to arrive.
    wait_for(|| async { writer.requests.lock().unwrap().first().map(|r| r.token) == Some(token) })
        .await;

    // Remote write succeeds; authoritative change streams back.
    writer.release();
    transaction(
        &h,
        20,
        vec![WireRowChange {
            op: DiffOp::Update,
            old: Some(row(1, "a")),
            new: Some(row(1, "mine")),
        }],
    )
    .await;
    wait_live(&h, 20).await;
    assert_eq!(h.replica.pending_mutations("posts").await, 0);
    assert_eq!(h.db.snapshot("posts"), vec![row(1, "mine")]);

    let mut settled = false;
    while let Ok(event) = events.try_recv() {
        if matches!(event, ReplicaEvent::MutationSettled { token: t, .. } if t == token) {
            settled = true;
        }
    }
    assert!(settled, "expected MutationSettled event");
}

#[tokio::test]
async fn failed_remote_write_rolls_back() {
    let writer = GatedWriter::new(Err("api rejected".into()));
    let h = harness(Some(writer.clone()));
    let mut events = h.replica.take_events().unwrap();
    accepted(&h).await;
    initial(&h, 10, vec![row(1, "a")]).await;
    wait_live(&h, 10).await;

    let token = h
        .replica
        .mutate(Mutation::delete("posts", [("id", WireDatum::I64(1))]))
        .await
        .unwrap();
    assert!(h.db.snapshot("posts").is_empty());

    writer.release();
    wait_for(|| async { h.replica.pending_mutations("posts").await == 0 }).await;
    // Rolled back to the authoritative row.
    assert_eq!(h.db.snapshot("posts"), vec![row(1, "a")]);

    let mut failed = false;
    for _ in 0..500 {
        if let Ok(event) = events.try_recv() {
            if matches!(&event, ReplicaEvent::MutationFailed { token: t, error, .. }
                if *t == token && error == "api rejected")
            {
                failed = true;
                break;
            }
        } else {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    assert!(failed, "expected MutationFailed event");
}

#[tokio::test]
async fn concurrent_server_change_rebases_pending_mutation() {
    let writer = GatedWriter::new(Ok(()));
    let h = harness(Some(writer.clone()));
    accepted(&h).await;
    initial(&h, 10, vec![row(1, "a")]).await;
    wait_live(&h, 10).await;

    h.replica
        .mutate(Mutation::update(
            "posts",
            [("id", WireDatum::I64(1))],
            [("title", WireDatum::Text(b"mine".to_vec()))],
        ))
        .await
        .unwrap();

    // A different writer changes the row before ours lands.
    transaction(
        &h,
        20,
        vec![WireRowChange {
            op: DiffOp::Update,
            old: Some(row(1, "a")),
            new: Some(row(1, "theirs")),
        }],
    )
    .await;
    wait_live(&h, 20).await;
    // Optimistic value stays on top; still pending.
    assert_eq!(h.db.snapshot("posts"), vec![row(1, "mine")]);
    assert_eq!(h.replica.pending_mutations("posts").await, 1);
}

#[tokio::test]
async fn insert_before_snapshot_is_rejected() {
    let h = harness(Some(GatedWriter::new(Ok(()))));
    let err = h
        .replica
        .mutate(Mutation::insert("posts", [("id", WireDatum::I64(1))]))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not mirrored"));
}

#[tokio::test]
async fn mutate_without_writer_is_rejected() {
    let h = harness(None);
    accepted(&h).await;
    initial(&h, 10, vec![]).await;
    wait_live(&h, 10).await;
    let err = h
        .replica
        .mutate(Mutation::insert(
            "posts",
            [("id", WireDatum::I64(1)), ("title", WireDatum::Null)],
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no remote writer"));
}
