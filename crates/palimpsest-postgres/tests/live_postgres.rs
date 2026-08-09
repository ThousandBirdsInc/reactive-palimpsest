// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end test against a real Postgres 16 cluster with logical
//! replication: catalog introspection (enum + timestamptz + array
//! columns), replica-identity and publication ownership, the fenced
//! snapshot, and live insert/update/delete diffs through the journal
//! cursor.
//!
//! The test provisions its own throwaway cluster (`initdb` +
//! `pg_ctl`). It soft-skips when no Postgres installation is
//! available.

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
        Self::start_inner(bin, false, "palimpsest-pg")
    }

    /// Same cluster, with `ssl = on` and a self-signed certificate, so
    /// the TLS paths (management connection *and* walsender stream)
    /// are exercised against a real server.
    fn start_tls(bin: PathBuf) -> Result<Self, String> {
        Self::start_inner(bin, true, "palimpsest-pg-tls")
    }

    fn start_inner(bin: PathBuf, tls: bool, prefix: &str) -> Result<Self, String> {
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map_err(|err| format!("pick port: {err}"))?
            .port();
        let data_dir = std::env::temp_dir().join(format!("{prefix}-{}-{port}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).map_err(|err| format!("mkdir: {err}"))?;

        let as_postgres = needs_user_switch();
        let cluster = Self {
            bin,
            data_dir,
            port,
            as_postgres,
        };
        let data = cluster.data_dir.display().to_string();

        if tls {
            // Self-signed cert with CN=localhost so hostname
            // verification has something to match.
            let status = Command::new("openssl")
                .args([
                    "req",
                    "-new",
                    "-x509",
                    "-days",
                    "1",
                    "-nodes",
                    "-text",
                    "-subj",
                    "/CN=localhost",
                    "-out",
                ])
                .arg(cluster.data_dir.join("server.crt"))
                .arg("-keyout")
                .arg(cluster.data_dir.join("server.key"))
                .status()
                .map_err(|err| format!("openssl: {err}"))?;
            if !status.success() {
                return Err("openssl req failed".to_owned());
            }
            std::fs::set_permissions(
                cluster.data_dir.join("server.key"),
                std::os::unix::fs::PermissionsExt::from_mode(0o600),
            )
            .map_err(|err| format!("chmod key: {err}"))?;
        }

        if as_postgres {
            let status = Command::new("chown")
                .args(["-R", "postgres:postgres"])
                .arg(&cluster.data_dir)
                .status()
                .map_err(|err| format!("chown: {err}"))?;
            if !status.success() {
                return Err("chown data dir failed".to_owned());
            }
        }

        cluster.run(
            "initdb",
            &["-D", &data, "-U", "postgres", "--auth=trust", "-N"],
        )?;

        // Logical replication + a private socket dir (the default
        // /var/run/postgresql may not be writable).
        let mut conf = format!(
            "\nwal_level = logical\nmax_replication_slots = 4\nmax_wal_senders = 4\n\
             listen_addresses = '127.0.0.1'\nport = {}\nunix_socket_directories = '{}'\n",
            cluster.port, data
        );
        if tls {
            conf.push_str("ssl = on\nssl_cert_file = 'server.crt'\nssl_key_file = 'server.key'\n");
        }
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

    /// DSN that requires TLS (libpq `sslmode=require`: encrypt, do not
    /// authenticate the server).
    fn tls_dsn(&self) -> String {
        format!(
            "postgres://postgres@localhost:{}/postgres?sslmode=require",
            self.port
        )
    }

    fn root_ca_pem(&self) -> String {
        std::fs::read_to_string(self.data_dir.join("server.crt")).expect("read server cert")
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter("palimpsest_postgres=debug")
        .with_test_writer()
        .try_init();
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

    // --- Every diff above came off the replication stream. ---
    // Without this the test would still pass on a broken stream: the
    // reconnect path re-snapshots and reconciles, producing the same
    // diffs by a different route.
    assert_eq!(
        runtime.reconcile_count(),
        0,
        "diffs must arrive over the walsender stream, not via re-snapshot reconciliation"
    );
    // Commit LSNs are real WAL positions, not a synthetic counter.
    assert!(
        runtime.current_lsn().get() > 1_000_000,
        "expected a real WAL position, got {}",
        runtime.current_lsn().get()
    );

    handle.shutdown().await;
}

/// The same end-to-end path over TLS: both the management/snapshot
/// connection and the walsender stream must negotiate TLS and stream
/// live diffs. Runs twice — encryption-only (`sslmode=require`, no
/// root CA) and CA-verified (`verify-full` semantics).
#[tokio::test(flavor = "multi_thread")]
async fn postgres_runtime_streams_over_tls() {
    let Some(bin) = pg_bin_dir() else {
        eprintln!("skipping: no Postgres installation found (set PG_BIN to override)");
        return;
    };
    let cluster = match Cluster::start_tls(bin) {
        Ok(cluster) => cluster,
        Err(err) => {
            eprintln!("skipping: could not provision a TLS cluster: {err}");
            return;
        }
    };
    // Bootstrap over the plaintext loopback DSN; the runtime itself
    // will use the TLS one.
    admin(
        &cluster.dsn(),
        "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);
         INSERT INTO notes VALUES (1, 'first');",
    )
    .await;

    for (label, root_ca) in [
        ("encryption-only", None),
        ("ca-verified", Some(cluster.root_ca_pem())),
    ] {
        let mut registry = QueryRegistry::new();
        registry
            .register("AllNotes", "SELECT id, body FROM notes", &[])
            .expect("register");
        let mut config = PostgresRuntimeConfig::from_registry(cluster.tls_dsn(), &registry);
        config.slot = format!("palimpsest_tls_{}", label.replace('-', "_"));
        config.publication = config.slot.clone();
        config.tls_root_ca_pem = root_ca;

        let (runtime, handle) = PostgresWalRuntime::connect(config)
            .await
            .unwrap_or_else(|err| panic!("{label}: TLS connect failed: {err}"));

        let query = QueryId::new("SELECT id, body FROM notes");
        let snapshot = runtime.fetch_snapshot(&query).expect("snapshot over TLS");
        assert_eq!(snapshot.rows[0].rows.len(), 1, "{label}");

        // A live insert must arrive over the TLS-wrapped walsender
        // stream, not via a reconnect re-snapshot.
        let anchor = snapshot.snapshot_lsn;
        admin(
            &cluster.dsn(),
            &format!("INSERT INTO notes VALUES ({}, 'x');", 100 + label.len()),
        )
        .await;
        let diffs = collect_diffs(&runtime, &query, anchor, 1).await;
        assert_eq!(diffs[0].diff, 1, "{label}");
        assert_eq!(
            runtime.reconcile_count(),
            0,
            "{label}: diffs must arrive over the TLS replication stream"
        );

        // Prove the walsender session is actually encrypted rather
        // than inferring it from the absence of an error.
        let (check, connection) = tokio_postgres::connect(&cluster.dsn(), tokio_postgres::NoTls)
            .await
            .expect("check connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let encrypted_walsenders: i64 = check
            .query_one(
                "SELECT COUNT(*) FROM pg_stat_ssl s \
                 JOIN pg_stat_activity a ON a.pid = s.pid \
                 WHERE s.ssl AND a.backend_type = 'walsender'",
                &[],
            )
            .await
            .expect("pg_stat_ssl query")
            .get(0);
        assert!(
            encrypted_walsenders >= 1,
            "{label}: expected an SSL-encrypted walsender backend, found none"
        );

        handle.shutdown().await;
    }
}

/// Row types are generated from the live catalog: an adopter never
/// hand-declares a `bigint` field or a micros-to-Date decoder.
#[tokio::test(flavor = "multi_thread")]
async fn generates_typescript_row_types_from_the_live_catalog() {
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
    admin(
        &dsn,
        "CREATE TYPE task_state AS ENUM ('todo', 'done');
         CREATE TABLE tasks (
             id bigint PRIMARY KEY,
             ref_id uuid NOT NULL,
             state task_state NOT NULL,
             due_on date,
             updated_at timestamptz NOT NULL,
             labels text[] NOT NULL,
             weight numeric NOT NULL
         );",
    )
    .await;

    // Introspect first: the catalog registration validates against is
    // the database itself, not a hand-written fixture.
    let tables = palimpsest_postgres::introspect_database(&dsn, None)
        .await
        .expect("introspect");
    let catalog = palimpsest_postgres::sql_catalog(&tables);

    let mut registry = QueryRegistry::new();
    registry
        .register_sqlc_source_marked(
            "-- name: InsertTask :exec\n\
             INSERT INTO tasks (id) VALUES ($1);\n\n\
             -- palimpsest: live\n\
             -- name: OpenTasks :many\n\
             SELECT id, ref_id, state, due_on, updated_at, labels, weight\n\
             FROM tasks WHERE state = $1;\n",
            "queries.sql",
            &catalog,
        )
        .expect("marker-scoped registration against the live catalog");

    let module = palimpsest_postgres::typescript_module(&registry, &tables).expect("typegen");

    // Each field's TS type comes from the real Postgres type.
    assert!(module.contains("id: bigint;"), "{module}");
    assert!(module.contains("ref_id: string;"), "{module}");
    assert!(module.contains("state: string;"), "enum label: {module}");
    assert!(module.contains("due_on: Date;"), "date: {module}");
    assert!(
        module.contains("updated_at: Date;"),
        "timestamptz must generate as Date, not a hand-decoded number: {module}"
    );
    assert!(module.contains("labels: unknown[];"), "{module}");
    assert!(module.contains("weight: string;"), "numeric: {module}");
    assert!(module.contains("export interface OpenTasksRow"), "{module}");
    // The unmarked :exec block is not live-subscribable, so it gets no
    // row type.
    assert!(!module.contains("InsertTask"), "{module}");
}
