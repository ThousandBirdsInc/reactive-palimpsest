// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end test against a real Postgres 16 cluster with logical
//! replication: catalog introspection (enum + timestamptz + array
//! columns), replica-identity and publication ownership, the fenced
//! snapshot, and live insert/update/delete diffs through the journal
//! cursor.
//!
//! The test provisions its own throwaway cluster (initdb + pg_ctl). It
//! soft-skips when no Postgres installation is available.

use std::io::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_postgres::{PostgresRuntimeConfig, PostgresWalRuntime};
use palimpsest_server::subscription::QueryId;
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_sql::prepared::QueryRegistry;
use palimpsest_wal::Datum;

/// Locates the Postgres binaries directory, or `None` to skip.
fn pg_bin_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("PG_BIN") {
        let dir = PathBuf::from(dir);
        if dir.join("initdb").exists() {
            return Some(dir);
        }
    }
    for major in (13..=18).rev() {
        let dir = PathBuf::from(format!("/usr/lib/postgresql/{major}/bin"));
        if dir.join("initdb").exists() {
            return Some(dir);
        }
    }
    which("initdb").map(|path| path.parent().map(Path::to_path_buf))?
}

fn which(binary: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(binary))
            .find(|candidate| candidate.exists())
    })
}

/// Whether cluster commands must be wrapped in `su postgres` (initdb
/// refuses to run as root).
fn needs_user_switch() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "0")
}

struct Cluster {
    bin: PathBuf,
    data_dir: PathBuf,
    port: u16,
    as_postgres: bool,
}

impl Cluster {
    fn run(&self, program: &str, args: &[&str]) -> Result<String, String> {
        let binary = self.bin.join(program);
        let output = if self.as_postgres {
            let command_line = format!(
                "{} {}",
                binary.display(),
                args.iter()
                    .map(|arg| format!("'{}'", arg.replace('\'', "'\\''")))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            Command::new("su")
                .args(["-s", "/bin/sh", "postgres", "-c", &command_line])
                .output()
        } else {
            Command::new(&binary).args(args).output()
        }
        .map_err(|err| format!("spawn {program}: {err}"))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(format!(
                "{program} failed: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    fn start(bin: PathBuf) -> Result<Self, String> {
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map_err(|err| format!("pick port: {err}"))?
            .port();
        let data_dir = std::env::temp_dir().join(format!("palimpsest-pg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).map_err(|err| format!("mkdir: {err}"))?;

        let as_postgres = needs_user_switch();
        if as_postgres {
            let status = Command::new("chown")
                .args(["-R", "postgres:postgres"])
                .arg(&data_dir)
                .status()
                .map_err(|err| format!("chown: {err}"))?;
            if !status.success() {
                return Err("chown data dir failed".to_owned());
            }
        }

        let cluster = Self {
            bin,
            data_dir,
            port,
            as_postgres,
        };
        let data = cluster.data_dir.display().to_string();
        cluster.run(
            "initdb",
            &["-D", &data, "-U", "postgres", "--auth=trust", "-N"],
        )?;

        // Logical replication + a private socket dir (the default
        // /var/run/postgresql may not be writable).
        let conf = format!(
            "\nwal_level = logical\nmax_replication_slots = 4\nmax_wal_senders = 4\n\
             listen_addresses = '127.0.0.1'\nport = {}\nunix_socket_directories = '{}'\n",
            cluster.port, data
        );
        let conf_path = cluster.data_dir.join("postgresql.conf");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&conf_path)
            .map_err(|err| format!("open conf: {err}"))?;
        file.write_all(conf.as_bytes())
            .map_err(|err| format!("write conf: {err}"))?;
        drop(file);

        let log = cluster.data_dir.join("postgres.log").display().to_string();
        cluster.run("pg_ctl", &["-D", &data, "-l", &log, "-w", "start"])?;
        Ok(cluster)
    }

    fn dsn(&self) -> String {
        format!(
            "postgres://postgres@127.0.0.1:{}/postgres?sslmode=disable",
            self.port
        )
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let data = self.data_dir.display().to_string();
        let _ = self.run("pg_ctl", &["-D", &data, "-m", "immediate", "stop"]);
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

async fn admin(dsn: &str, sql: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("admin connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute(sql).await.expect("admin sql");
}

/// Polls the cursor until `expected` diffs arrive (or times out).
async fn collect_diffs(
    runtime: &PostgresWalRuntime,
    query: &QueryId,
    from: Lsn,
    expected: usize,
) -> Vec<palimpsest_server::cursor::RawDiff> {
    let mut cursor = runtime.open_cursor(query, from).expect("cursor");
    let mut diffs = Vec::new();
    for _ in 0..200 {
        while let Some(diff) = cursor.next_diff() {
            diffs.push(diff);
        }
        if diffs.len() >= expected {
            return diffs;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {expected} diffs; got {}: {diffs:?}",
        diffs.len()
    );
}

fn text(datum: &Datum) -> String {
    match datum {
        Datum::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("expected text datum, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_runtime_end_to_end() {
    let Some(bin) = pg_bin_dir() else {
        eprintln!("skipping: no Postgres installation found (set PG_BIN to override)");
        return;
    };
    let cluster = match Cluster::start(bin) {
        Ok(cluster) => cluster,
        Err(err) => {
            eprintln!("skipping: could not provision a throwaway cluster: {err}");
            return;
        }
    };
    let dsn = cluster.dsn();

    // Schema with the exact shapes the engine used to choke on: an
    // enum column, a timestamptz, and a text array. REPLICA IDENTITY
    // stays DEFAULT so the runtime has to fix it.
    admin(
        &dsn,
        "CREATE TYPE ticket_status AS ENUM ('open', 'closed');
         CREATE TABLE tickets (
             id bigint PRIMARY KEY,
             status ticket_status NOT NULL,
             created_at timestamptz NOT NULL,
             tags text[] NOT NULL,
             title text NOT NULL
         );
         INSERT INTO tickets VALUES
             (1, 'open', '2026-01-02 03:04:05+00', '{urgent,ui}', 'fix login'),
             (2, 'closed', '2026-01-03 00:00:00+00', '{}', 'update docs');",
    )
    .await;

    // The streamed set derives from the registry, not from config.
    let mut registry = QueryRegistry::new();
    registry
        .register(
            "AllTickets",
            "SELECT id, status, created_at, tags, title FROM tickets",
            &[],
        )
        .expect("register");
    let config = PostgresRuntimeConfig::from_registry(&dsn, &registry);
    assert_eq!(config.tables, vec!["tickets".to_owned()]);

    let (runtime, handle) = PostgresWalRuntime::connect(config)
        .await
        .expect("runtime connects, introspects, and seeds");

    // --- Introspection: table_schema comes from pg_catalog. ---
    let (_, schema) = runtime.table_schema("tickets").expect("introspected");
    let names: Vec<&str> = schema
        .columns()
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(names, ["id", "status", "created_at", "tags", "title"]);

    // --- Snapshot: enum decodes as its label, timestamptz and array
    //     as themselves. ---
    let query = QueryId::new("SELECT id, status, created_at, tags, title FROM tickets");
    let snapshot = runtime.fetch_snapshot(&query).expect("snapshot");
    assert_eq!(snapshot.rows.len(), 1);
    let mut rows = snapshot.rows[0].rows.clone();
    rows.sort();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::I64(1));
    assert_eq!(text(&rows[0][1]), "open");
    assert!(
        matches!(rows[0][2], Datum::TimestampTz(ts) if ts.micros_since_unix_epoch > 0),
        "timestamptz must cross as itself: {:?}",
        rows[0][2]
    );
    match &rows[0][3] {
        Datum::Array(tags) => {
            assert_eq!(tags.len(), 2);
            assert_eq!(text(&tags[0]), "urgent");
        }
        other => panic!("tags must decode as an array: {other:?}"),
    }

    // --- Ownership: replica identity was set, publication created. ---
    let (check, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("check connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let identity: String = check
        .query_one(
            "SELECT relreplident::text FROM pg_class WHERE relname = 'tickets'",
            &[],
        )
        .await
        .expect("identity query")
        .get(0);
    assert_eq!(identity, "f", "REPLICA IDENTITY FULL set");
    let publications: i64 = check
        .query_one(
            "SELECT COUNT(*) FROM pg_publication WHERE pubname = 'palimpsest'",
            &[],
        )
        .await
        .expect("publication query")
        .get(0);
    assert_eq!(publications, 1, "publication owned by the runtime");

    // --- Live inserts flow as +1 diffs with real datums. ---
    let anchor = snapshot.snapshot_lsn;
    admin(
        &dsn,
        "INSERT INTO tickets VALUES
            (3, 'open', '2026-02-01 00:00:00+00', '{backend}', 'add index');",
    )
    .await;
    let diffs = collect_diffs(&runtime, &query, anchor, 1).await;
    assert_eq!(diffs[0].diff, 1);
    assert_eq!(diffs[0].row[0], Datum::I64(3));
    assert_eq!(text(&diffs[0].row[1]), "open");

    // --- Updates retract the old row and assert the new one. ---
    let after_insert = runtime.current_lsn();
    admin(&dsn, "UPDATE tickets SET status = 'closed' WHERE id = 3;").await;
    let diffs = collect_diffs(&runtime, &query, after_insert, 2).await;
    let retract = diffs.iter().find(|d| d.diff == -1).expect("retraction");
    let asserted = diffs.iter().find(|d| d.diff == 1).expect("assertion");
    assert_eq!(text(&retract.row[1]), "open");
    assert_eq!(text(&asserted.row[1]), "closed");

    // --- Deletes carry the full old row (REPLICA IDENTITY FULL). ---
    let after_update = runtime.current_lsn();
    admin(&dsn, "DELETE FROM tickets WHERE id = 3;").await;
    let diffs = collect_diffs(&runtime, &query, after_update, 1).await;
    assert_eq!(diffs[0].diff, -1);
    assert_eq!(diffs[0].row[0], Datum::I64(3));
    assert_eq!(text(&diffs[0].row[4]), "add index");

    handle.shutdown().await;
}
