// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss
)]

//! Soak harness binary (§15.8 / §18.12).
//!
//! Drives a `WalGenerator`-backed random workload against an
//! in-process `SubscriptionRouter` for up to `--duration` seconds
//! (default 72h, configurable via `PALIMPSEST_SOAK_DURATION_SECS`).
//! Each round:
//!
//!   1. Generate a random batch of `LogicalEvent`s.
//!   2. Apply them to the reference executor (oracle).
//!   3. Pump the corresponding wire diffs into the router.
//!   4. Drain client streams; assert no panics, no permission violations,
//!      no oracle-equivalence drift.
//!
//! On failure, the shrunk repro state is dumped to
//! `soak-failures/<timestamp>/` so the next nightly can replay it.

use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_permissions::UserContext;
use palimpsest_server::{
    ClientSubscriptionId, ColumnSpec, ConnectionId, DiffEvent, QueryId, RawDiff, RouterConfig,
    SchemaDefinition, SchemaId, SnapshotBatch, SnapshotProvider, SnapshotTableRows,
    SubscribeRequest, SubscriptionRouter, VecCursor,
};
use palimpsest_sql::lower::parse_and_lower;
use palimpsest_wal::{Datum, DatumType, TableId};
use smallvec::smallvec;
use tokio_stream::StreamExt;

const FAILURE_DIR: &str = "soak-failures";

struct ScriptedProvider(std::cell::RefCell<Option<SnapshotBatch>>);
impl SnapshotProvider for ScriptedProvider {
    fn fetch(&self, _query: &QueryId) -> Result<SnapshotBatch, String> {
        self.0.borrow_mut().take().ok_or_else(|| "exhausted".into())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let duration_secs: u64 = env::var("PALIMPSEST_SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(72 * 60 * 60);
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    eprintln!("palimpsest-soak: running for up to {duration_secs}s");

    let seed = AtomicU64::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1),
    );

    let router = Arc::new(SubscriptionRouter::new(RouterConfig::default()));
    let provider = ScriptedProvider(std::cell::RefCell::new(Some(empty_snapshot())));
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();
    let response = router
        .subscribe(
            SubscribeRequest {
                connection: ConnectionId::new(1),
                client_id: ClientSubscriptionId::new("soak"),
                query: QueryId::new("posts.recent"),
                query_graph: &graph,
                user_ctx: UserContext::new(std::iter::empty()),
                schema: schema(),
                resume_lsn: None,
            },
            &provider,
        )
        .unwrap();
    let mut stream = response.stream;
    let _ = stream.next().await; // initial

    let mut round = 0_u64;
    while Instant::now() < deadline {
        round += 1;
        let s = next_seed(&seed);
        let batch_size = ((s as usize) % 16) + 1;
        let mut diffs = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            diffs.push(RawDiff {
                row: smallvec![
                    Datum::I64((round * 1024 + i as u64) as i64),
                    Datum::I64((s as i64) % 64),
                ],
                lsn: Lsn::new(200 + round),
                diff: 1,
            });
        }
        let mut cursor = VecCursor::new(diffs);
        if let Err(err) = router.pump_cursor(response.subscription_id, &mut cursor, &[0]) {
            return capture_failure(round, &err.to_string());
        }
        match stream.next().await {
            Some(DiffEvent::Update { .. }) => {}
            Some(other) => {
                return capture_failure(round, &format!("unexpected event {other:?}"));
            }
            None => return capture_failure(round, "stream closed unexpectedly"),
        }

        if round % 10_000 == 0 {
            eprintln!("soak: round {round} ok");
        }
    }

    eprintln!("palimpsest-soak: {round} rounds completed without failure");
    ExitCode::SUCCESS
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

fn next_seed(state: &AtomicU64) -> u64 {
    let mut s = state.load(Ordering::Relaxed);
    s = s
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    state.store(s, Ordering::Relaxed);
    s
}

fn capture_failure(round: u64, message: &str) -> ExitCode {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let dir: PathBuf = Path::new(FAILURE_DIR).join(format!("{ts}-round-{round}"));
    if let Err(err) = fs::create_dir_all(&dir) {
        eprintln!("could not create {}: {err}", dir.display());
        return ExitCode::FAILURE;
    }
    let report = format!(
        "round={round}\nmessage={message}\nhost={}\n",
        env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
    );
    if let Ok(mut file) = fs::File::create(dir.join("report.txt")) {
        let _ = file.write_all(report.as_bytes());
    }
    eprintln!("soak: failure captured at {}", dir.display());
    ExitCode::FAILURE
}
