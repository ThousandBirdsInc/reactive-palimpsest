// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Postgres wire-protocol front end, driven by a real Postgres client
//! (`tokio-postgres`) against an in-memory WAL runtime.
//!
//! Covers: the startup handshake, the simple query cycle (text
//! results), the extended query cycle with binary results and typed
//! parameters, `SELECT` against a live aggregate that keeps updating
//! as WAL diffs land (the materialized-view promise), permission
//! rules, cleartext-password (JWT) authentication, the utility
//! statements drivers send on connect, `EXECUTE` of a registered
//! named query, and error reporting that leaves the session usable.

#![allow(clippy::significant_drop_tightening)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use jsonwebtoken::{encode, EncodingKey, Header};
use palimpsest_dataflow::palimpsest::eval::ScalarSchema;
use palimpsest_dataflow::palimpsest::{Lsn, Row};
use palimpsest_permissions::{compile_rules, CompiledRule, PermissionRule, UserContextSchema};
use palimpsest_server::cursor::RawDiff;
use palimpsest_server::snapshot::{SnapshotBatch, SnapshotTableRows};
use palimpsest_server::subscription::{ColumnSpec, QueryId, SchemaDefinition, SchemaId};
use palimpsest_server::wal_runtime::WalRuntime;
use palimpsest_server::{
    AnonymousAuthenticator, JwtAuthConfig, JwtAuthenticator, Palimpsest, TraceCursor,
};
use palimpsest_sql::prepared::QueryRegistry;
use palimpsest_sql::{Catalog, ColumnSchema, ColumnType, TableSchema};
use palimpsest_wal::{Datum, DatumType, TableId, TimestampTz};
use smallvec::smallvec;
use tokio::sync::oneshot;
use tokio_postgres::{NoTls, SimpleQueryMessage};

// -----------------------------------------------------------------------------
// In-memory `events` table with a journal the tests can append to.
// -----------------------------------------------------------------------------

const EVENTS_TABLE_ID: u32 = 16_400;

fn events_columns() -> Vec<(String, ColumnType)> {
    vec![
        ("id".to_owned(), ColumnType::Int),
        ("category_id".to_owned(), ColumnType::Int),
        ("value".to_owned(), ColumnType::Int),
        ("name".to_owned(), ColumnType::Text),
        ("score".to_owned(), ColumnType::Float),
        ("active".to_owned(), ColumnType::Bool),
        ("created_at".to_owned(), ColumnType::TimestampTz),
    ]
}

fn events_catalog() -> Catalog {
    Catalog::new([TableSchema::new(
        "events",
        events_columns()
            .into_iter()
            .map(|(name, ty)| ColumnSchema::new(name, ty))
            .collect(),
    )])
}

fn event_row(id: i64, category: i64, value: i64, name: &str, score: f64, active: bool) -> Row {
    smallvec![
        Datum::I64(id),
        Datum::I64(category),
        Datum::I64(value),
        Datum::Text(Bytes::from(name.to_owned())),
        Datum::F64(score.to_bits()),
        Datum::Bool(active),
        Datum::TimestampTz(TimestampTz {
            // 2024-01-02 03:04:05 UTC
            micros_since_unix_epoch: 1_704_164_645_000_000,
        }),
    ]
}

fn seed_rows() -> Vec<Row> {
    vec![
        event_row(1, 7, 100, "alpha", 1.5, true),
        event_row(2, 7, 50, "beta", 2.5, false),
        event_row(3, 9, 10, "gamma", 0.25, true),
    ]
}

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
        let mut lsn = self.lsn.lock().expect("lsn lock");
        *lsn += 1;
        let new_lsn = Lsn::new(*lsn);
        drop(lsn);
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
        Ok(SnapshotBatch {
            snapshot_lsn: Lsn::new(*self.lsn.lock().expect("lsn lock")),
            rows: vec![SnapshotTableRows {
                table: TableId::new(EVENTS_TABLE_ID),
                rows: self.rows.lock().expect("rows lock").clone(),
            }],
        })
    }

    fn query_schema(&self, _query: &QueryId) -> Result<SchemaDefinition, String> {
        let datum_type = |ty: ColumnType| match ty {
            ColumnType::Int => DatumType::I64,
            ColumnType::Text => DatumType::Text,
            ColumnType::Float => DatumType::F64,
            ColumnType::Bool => DatumType::Bool,
            ColumnType::TimestampTz => DatumType::TimestampTz,
            _ => DatumType::Text,
        };
        Ok(SchemaDefinition {
            id: SchemaId::new(0),
            columns: events_columns()
                .into_iter()
                .map(|(name, ty)| ColumnSpec {
                    name,
                    datum_type: datum_type(ty),
                    nullable: false,
                })
                .collect(),
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
        (table == "events").then(|| {
            (
                TableId::new(EVENTS_TABLE_ID),
                ScalarSchema::from_pairs(events_columns()),
            )
        })
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
        journal[self.next_index..]
            .iter()
            .find(|entry| entry.lsn > self.from_lsn)
            .map(|entry| entry.lsn)
    }
}

// -----------------------------------------------------------------------------
// Server harness
// -----------------------------------------------------------------------------

struct Harness {
    wal: TestEventsWal,
    pg_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

fn free_port() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("addr")
}

impl Harness {
    async fn start() -> Self {
        Self::start_with(|builder| builder.with_auth(AnonymousAuthenticator)).await
    }

    async fn start_with<F>(configure: F) -> Self
    where
        F: FnOnce(palimpsest_server::PalimpsestBuilder) -> palimpsest_server::PalimpsestBuilder,
    {
        let wal = TestEventsWal::new(seed_rows());
        let grpc_addr = free_port();
        let pg_addr = free_port();
        let builder = Palimpsest::builder()
            .with_wal(wal.clone())
            .with_grpc_addr(grpc_addr)
            .with_metrics_addr(None)
            .with_pgwire_addr(Some(pg_addr));
        let server = configure(builder).build().expect("build server");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            server
                .serve(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve");
        });
        wait_for_port(pg_addr).await;
        Self {
            wal,
            pg_addr,
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    fn config(&self) -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::new();
        config
            .host(self.pg_addr.ip().to_string())
            .port(self.pg_addr.port())
            .user("app")
            .dbname("palimpsest")
            .connect_timeout(Duration::from_secs(5));
        config
    }

    async fn connect(&self) -> tokio_postgres::Client {
        self.connect_with(self.config()).await.expect("connect")
    }

    async fn connect_with(
        &self,
        config: tokio_postgres::Config,
    ) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
        }
    }
}

async fn wait_for_port(addr: SocketAddr) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server never listened on {addr}");
}

fn min_value_rule() -> Vec<CompiledRule> {
    let rule = PermissionRule::new("events_min_value", "events", "value >= 50");
    compile_rules(&[rule], &events_catalog(), &UserContextSchema::default())
        .expect("compile permission rule")
}

/// Polls `query` until `predicate` holds or two seconds pass.
async fn eventually<F>(
    client: &tokio_postgres::Client,
    sql: &str,
    predicate: F,
) -> Vec<tokio_postgres::Row>
where
    F: Fn(&[tokio_postgres::Row]) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let rows = client.query(sql, &[]).await.expect("query");
        if predicate(&rows) || tokio::time::Instant::now() > deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_query_returns_text_rows() {
    let harness = Harness::start().await;
    let client = harness.connect().await;

    let messages = client
        .simple_query("SELECT id, name, score, active, created_at FROM events WHERE id <= 2")
        .await
        .expect("simple query");
    let mut rows = Vec::new();
    let mut completed = None;
    for message in messages {
        match message {
            SimpleQueryMessage::Row(row) => rows.push(row),
            SimpleQueryMessage::CommandComplete(count) => completed = Some(count),
            SimpleQueryMessage::RowDescription(columns) => {
                let names: Vec<&str> = columns
                    .iter()
                    .map(tokio_postgres::SimpleColumn::name)
                    .collect();
                assert_eq!(names, ["id", "name", "score", "active", "created_at"]);
            }
            _ => {}
        }
    }
    assert_eq!(completed, Some(2));
    rows.sort_by_key(|row| row.get(0).unwrap().to_owned());
    assert_eq!(rows[0].get(0), Some("1"));
    assert_eq!(rows[0].get(1), Some("alpha"));
    assert_eq!(rows[0].get(2), Some("1.5"));
    assert_eq!(rows[0].get(3), Some("t"));
    assert_eq!(rows[0].get(4), Some("2024-01-02 03:04:05+00"));
    assert_eq!(rows[1].get(1), Some("beta"));
    assert_eq!(rows[1].get(3), Some("f"));

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_query_with_typed_parameters_and_binary_results() {
    let harness = Harness::start().await;
    let client = harness.connect().await;

    // tokio-postgres uses Parse/Describe/Bind/Execute with binary
    // formats for both parameters and results; the parameter type is
    // inferred from the `id` column it is compared against.
    let rows = client
        .query(
            "SELECT id, name, score, active, created_at FROM events WHERE id > $1",
            &[&1_i64],
        )
        .await
        .expect("extended query");
    assert_eq!(rows.len(), 2);
    let mut ids: Vec<i64> = rows.iter().map(|row| row.get("id")).collect();
    ids.sort_unstable();
    assert_eq!(ids, [2, 3]);
    let gamma = rows
        .iter()
        .find(|row| row.get::<_, i64>("id") == 3)
        .unwrap();
    assert_eq!(gamma.get::<_, &str>("name"), "gamma");
    assert!((gamma.get::<_, f64>("score") - 0.25).abs() < f64::EPSILON);
    assert!(gamma.get::<_, bool>("active"));
    let created: std::time::SystemTime = gamma.get("created_at");
    assert_eq!(
        created
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros(),
        1_704_164_645_000_000
    );

    // A prepared statement can be re-executed with different values;
    // each binding is its own live view.
    let statement = client
        .prepare("SELECT name FROM events WHERE category_id = $1 AND active = $2")
        .await
        .expect("prepare");
    assert_eq!(
        statement.params(),
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::BOOL
        ]
    );
    let active_in_7: Vec<String> = client
        .query(&statement, &[&7_i64, &true])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(active_in_7, ["alpha"]);
    let inactive_in_7: Vec<String> = client
        .query(&statement, &[&7_i64, &false])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(inactive_in_7, ["beta"]);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_view_stays_live_across_executions() {
    let harness = Harness::start().await;
    let client = harness.connect().await;
    let sql =
        "SELECT category_id, SUM(value) AS total, COUNT(*) AS n FROM events GROUP BY category_id";

    let rows = client.query(sql, &[]).await.expect("first execution");
    let mut totals: Vec<(i64, i64, i64)> = rows
        .iter()
        .map(|row| (row.get("category_id"), row.get("total"), row.get("n")))
        .collect();
    totals.sort_unstable();
    assert_eq!(totals, [(7, 150, 2), (9, 10, 1)]);

    // A WAL diff lands after the first execution. The connection's
    // anchor keeps the plan registered and its pump running, so the
    // next execution reads the updated materialized view.
    harness
        .wal
        .push_event(event_row(4, 9, 30, "delta", 9.0, true));
    let rows = eventually(&client, sql, |rows| {
        rows.iter()
            .any(|row| row.get::<_, i64>("category_id") == 9 && row.get::<_, i64>("total") == 40)
    })
    .await;
    let mut totals: Vec<(i64, i64, i64)> = rows
        .iter()
        .map(|row| (row.get("category_id"), row.get("total"), row.get("n")))
        .collect();
    totals.sort_unstable();
    assert_eq!(totals, [(7, 150, 2), (9, 40, 2)]);

    // A second connection shares the same plan (same SQL, same user).
    let other = harness.connect().await;
    let rows = other.query(sql, &[]).await.expect("second connection");
    assert_eq!(rows.len(), 2);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permission_rules_filter_wire_protocol_results() {
    let harness = Harness::start_with(|builder| {
        builder
            .with_auth(AnonymousAuthenticator)
            .with_permissions(min_value_rule())
    })
    .await;
    let client = harness.connect().await;

    let rows = client
        .query("SELECT id FROM events", &[])
        .await
        .expect("filtered query");
    let mut ids: Vec<i64> = rows.iter().map(|row| row.get(0)).collect();
    ids.sort_unstable();
    assert_eq!(ids, [1, 2], "value 10 row is hidden by the rule");

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jwt_servers_take_the_token_as_the_password() {
    let auth = JwtAuthenticator::from_config(JwtAuthConfig {
        secret: Some("topsecret".to_owned()),
        ..JwtAuthConfig::default()
    })
    .await
    .expect("jwt config");
    let harness = Harness::start_with(move |builder| builder.with_auth(auth)).await;

    // No password: the server asks for one and the client has none.
    let err = harness
        .connect_with(harness.config())
        .await
        .expect_err("missing password must fail");
    // tokio-postgres reports the missing password as a configuration
    // error whose cause names the password.
    assert!(
        format!("{err:?}").contains("password"),
        "unexpected error: {err:?}"
    );

    // Wrong password: rejected with the Postgres invalid-password code.
    let mut bad = harness.config();
    bad.password("not-a-token");
    let err = harness
        .connect_with(bad)
        .await
        .expect_err("bad token must fail");
    assert_eq!(
        err.as_db_error().map(|e| e.code().code()),
        Some("28P01"),
        "unexpected error: {err}"
    );

    // Valid token: authenticated and able to query.
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    let token = encode(
        &Header::default(),
        &serde_json::json!({ "sub": "alice", "exp": exp }),
        &EncodingKey::from_secret(b"topsecret"),
    )
    .unwrap();
    let mut good = harness.config();
    good.password(&token);
    let client = harness.connect_with(good).await.expect("token accepted");
    let rows = client
        .query("SELECT COUNT(*) AS n FROM events", &[])
        .await
        .expect("query");
    assert_eq!(rows[0].get::<_, i64>("n"), 3);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_startup_statements_and_errors_keep_the_session_usable() {
    let harness = Harness::start().await;
    let client = harness.connect().await;

    // What JDBC / pools send on connect.
    client
        .simple_query("SET extra_float_digits = 3; SET application_name = 'svc'")
        .await
        .expect("SET is a no-op");
    let messages = client
        .simple_query("SHOW transaction_isolation")
        .await
        .expect("SHOW");
    let shown = messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap();
    assert_eq!(shown, "read committed");
    client
        .batch_execute("BEGIN; COMMIT")
        .await
        .expect("txn no-ops");

    // Errors carry SQLSTATEs and do not poison the connection.
    let err = client
        .query("SELEC id FROM events", &[])
        .await
        .expect_err("syntax error");
    assert_eq!(err.as_db_error().map(|e| e.code().code()), Some("42601"));
    let err = client
        .query("SELECT id FROM nowhere", &[])
        .await
        .expect_err("unknown table");
    assert_eq!(err.as_db_error().map(|e| e.code().code()), Some("42P01"));
    let err = client
        .query("DELETE FROM events", &[])
        .await
        .expect_err("read only");
    assert_eq!(err.as_db_error().map(|e| e.code().code()), Some("25006"));

    let rows = client
        .query("SELECT id FROM events WHERE id = $1", &[&2_i64])
        .await
        .expect("session still works");
    assert_eq!(rows.len(), 1);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_runs_registered_named_queries() {
    let mut registry = QueryRegistry::new();
    registry
        .register_sqlc_source(
            "-- name: EventsInCategory :many\nSELECT id, name FROM events WHERE category_id = $1;\n",
            "test",
            &events_catalog(),
        )
        .expect("register");
    // Registering a registry disables raw SQL: the named query is the
    // only reachable surface.
    let harness = Harness::start_with(move |builder| {
        builder
            .with_auth(AnonymousAuthenticator)
            .with_query_registry(registry)
    })
    .await;
    let client = harness.connect().await;

    let err = client
        .query("SELECT id FROM events", &[])
        .await
        .expect_err("raw SQL is disabled");
    assert_eq!(err.as_db_error().map(|e| e.code().code()), Some("42501"));

    let rows = client
        .query("EXECUTE EventsInCategory(7)", &[])
        .await
        .expect("named query");
    let mut names: Vec<String> = rows.iter().map(|row| row.get("name")).collect();
    names.sort();
    assert_eq!(names, ["alpha", "beta"]);

    let err = client
        .query("EXECUTE Missing(1)", &[])
        .await
        .expect_err("unknown name");
    assert_eq!(err.as_db_error().map(|e| e.code().code()), Some("42704"));

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portal_row_limits_suspend_and_resume() {
    let harness = Harness::start().await;
    let mut client = harness.connect().await;

    // `query_raw` with a row limit goes through Execute(max_rows):
    // tokio-postgres exposes this via a transaction's portal API.
    let transaction = client.transaction().await.expect("begin");
    let statement = transaction
        .prepare("SELECT id FROM events")
        .await
        .expect("prepare");
    let portal = transaction.bind(&statement, &[]).await.expect("bind");
    let first = transaction
        .query_portal(&portal, 2)
        .await
        .expect("first two");
    assert_eq!(first.len(), 2);
    let rest = transaction
        .query_portal(&portal, 2)
        .await
        .expect("remaining");
    assert_eq!(rest.len(), 1);
    let none = transaction.query_portal(&portal, 2).await.expect("drained");
    assert!(none.is_empty());
    transaction.commit().await.expect("commit");

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnecting_releases_anchored_views() {
    let harness = Harness::start().await;
    let sql = "SELECT category_id, COUNT(*) AS n FROM events GROUP BY category_id";
    {
        let client = harness.connect().await;
        client.query(sql, &[]).await.expect("query");
        // Dropping the client closes the socket; the session must
        // unsubscribe its anchor and let the host drop the plan.
    }
    // A fresh connection re-subscribes cleanly (cold path again) and
    // sees data pushed meanwhile.
    harness
        .wal
        .push_event(event_row(5, 11, 1, "eps", 0.0, true));
    let client = harness.connect().await;
    let rows = eventually(&client, sql, |rows| rows.len() == 3).await;
    assert_eq!(rows.len(), 3);

    harness.shutdown().await;
}
