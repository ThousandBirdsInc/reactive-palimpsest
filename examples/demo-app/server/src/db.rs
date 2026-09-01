//! Postgres connection + logical-replication consumer for the demo.
//!
//! Three responsibilities:
//!
//! 1. **Bootstrap** — connect, create the `issues` table + publication
//!    + slot, seed a believable project history if empty, return the
//!    live `Client` the write API uses.
//! 2. **Initial snapshot** — `SELECT * FROM issues` so the in-memory
//!    mirror starts populated; the cursor for diffs starts *after* the
//!    snapshot LSN.
//! 3. **Replication consumer** — a tokio task that polls
//!    `pg_logical_slot_get_binary_changes` every 100ms, feeds the
//!    bytes through `palimpsest_wal::decode_pgoutput_message`, and
//!    applies each `DecodedEvent::Row` to the in-memory mirror +
//!    journal. The journal is what the existing `WalRuntime` cursor
//!    reads to drive live diffs through the dataflow.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use palimpsest_dataflow::palimpsest::Row;
use palimpsest_wal::{decode_pgoutput_message, Catalog, DecodedEvent, RowOp, TableId};
use smallvec::SmallVec;
use tokio_postgres::{Client, Config, NoTls};
use tracing::{debug, info, warn};

use crate::auth::USERS;
use crate::state::{Issue, IssueChange, IssueStore};

/// Replication slot + publication names the demo uses. Picked to be
/// stable across reboots so the slot retains WAL across restarts.
const PUBLICATION: &str = "palimpsest_pub";
const SLOT: &str = "palimpsest_demo";

/// Number of issues the seed generates. Enough history that the
/// analytics page has meaningful distributions (a quarter's worth of
/// throughput, per-project cycle times) without slowing boot down.
const SEED_ISSUES: usize = 1_400;

/// How far back the seeded history reaches, in days.
pub const SEED_HISTORY_DAYS: i64 = 120;

/// Issue workflow states, in board order. The same list lives
/// client-side in `web/src/issues.ts`; keep the two in sync.
pub const STATUSES: &[&str] = &[
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "done",
    "cancelled",
];

/// Projects issues belong to. `security` is the interesting one: the
/// default permission rule hides it from non-admin personas. The same
/// list lives client-side in `web/src/issues.ts`.
pub const PROJECTS: &[&str] = &["sync-engine", "dataflow", "clients", "infra", "security"];

/// Story-point estimates the tracker uses (Fibonacci-ish).
const ESTIMATES: &[i64] = &[1, 2, 3, 5, 8];

/// Total wait time for Postgres to start accepting connections.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Replication-poll cadence. 100ms gives perceptually-live updates
/// without burning Postgres CPU.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Days since the Unix epoch, "today" from the server's clock.
pub fn today_epoch_day() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    i64::try_from(secs / 86_400).unwrap_or(0)
}

/// Tiny deterministic PRNG (xorshift64*) so the seed and the activity
/// simulator don't need a rand dependency and reseed reproducibly.
pub struct DemoRng(u64);

impl DemoRng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform integer in `[0, bound)`.
    pub fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        usize::try_from(self.next_u64() % bound as u64).unwrap_or(0)
    }

    /// Pick an index from a weight table.
    pub fn weighted(&mut self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|w| u64::from(*w)).sum();
        if total == 0 {
            return 0;
        }
        let mut roll = self.next_u64() % total;
        for (idx, w) in weights.iter().enumerate() {
            let w = u64::from(*w);
            if roll < w {
                return idx;
            }
            roll -= w;
        }
        weights.len() - 1
    }
}

/// Connection parameters sourced from env vars so docker-compose can
/// wire them. Default values match the demo compose file so a
/// developer running the binary against a local Postgres still
/// works.
#[derive(Debug, Clone)]
pub struct PgSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub publication: String,
    pub slot: String,
}

impl PgSettings {
    /// Read settings from the `PALIMPSEST_DEMO_PG_*` env vars; falls
    /// back to localhost defaults so a developer running the binary
    /// against their own Postgres still works.
    pub fn from_env() -> Self {
        let host =
            std::env::var("PALIMPSEST_DEMO_PG_HOST").unwrap_or_else(|_| "localhost".to_owned());
        let port = std::env::var("PALIMPSEST_DEMO_PG_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5432);
        let user =
            std::env::var("PALIMPSEST_DEMO_PG_USER").unwrap_or_else(|_| "palimpsest".to_owned());
        let password =
            std::env::var("PALIMPSEST_DEMO_PG_PASSWORD").unwrap_or_else(|_| "demo".to_owned());
        let database =
            std::env::var("PALIMPSEST_DEMO_PG_DB").unwrap_or_else(|_| "palimpsest_demo".to_owned());
        let publication = std::env::var("PALIMPSEST_DEMO_PG_PUBLICATION")
            .unwrap_or_else(|_| PUBLICATION.to_owned());
        let slot = std::env::var("PALIMPSEST_DEMO_PG_SLOT").unwrap_or_else(|_| SLOT.to_owned());
        Self {
            host,
            port,
            user,
            password,
            database,
            publication,
            slot,
        }
    }

    fn to_config(&self) -> Config {
        let mut cfg = Config::new();
        cfg.host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(&self.password)
            .dbname(&self.database);
        cfg
    }
}

/// Everything the demo server needs out of Postgres after bootstrap.
pub struct PgBootstrap {
    /// Client used by HTTP handlers for INSERT/UPDATE/DELETE.
    pub client: Arc<Client>,
    /// Stable settings — kept around so the consumer task can open
    /// its own connection.
    pub settings: PgSettings,
    /// Postgres OID for the `issues` table — used as the `TableId`
    /// throughout the dataflow.
    pub issues_oid: u32,
}

/// Connect, ensure schema + publication + slot, seed if empty, and
/// resolve the table OID the rest of the server uses as `TableId`.
pub async fn bootstrap() -> Result<PgBootstrap, String> {
    let settings = PgSettings::from_env();
    let client = connect_with_retry(&settings).await?;
    init_schema(&client).await?;
    ensure_publication_and_slot(&client, &settings).await?;
    seed_if_empty(&client).await?;
    // Discard any WAL the slot has accumulated so far. The seed (if
    // any) runs *after* the slot was created, so otherwise the
    // consumer would replay those same INSERTs and double-apply them
    // on top of the SELECT-based snapshot below.
    drain_slot(&client, &settings).await?;
    let issues_oid = discover_oid(&client).await?;
    info!(issues_oid, "postgres bootstrap complete");
    Ok(PgBootstrap {
        client: Arc::new(client),
        settings,
        issues_oid,
    })
}

/// Loop on `connect()` until Postgres accepts a connection. The
/// `docker-compose` healthcheck mostly handles this for us, but the
/// fallback covers boot-order races + the bare-binary developer path.
async fn connect_with_retry(settings: &PgSettings) -> Result<Client, String> {
    let deadline = std::time::Instant::now() + CONNECT_TIMEOUT;
    let mut backoff = Duration::from_millis(200);
    loop {
        match connect_once(settings).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                if std::time::Instant::now() >= deadline {
                    return Err(format!(
                        "postgres unreachable after {CONNECT_TIMEOUT:?}: {err}"
                    ));
                }
                debug!(?err, "postgres connect failed, retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
}

async fn connect_once(settings: &PgSettings) -> Result<Client, String> {
    let (client, connection) = settings
        .to_config()
        .connect(NoTls)
        .await
        .map_err(|err| err.to_string())?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            warn!(?err, "postgres connection task ended");
        }
    });
    Ok(client)
}

/// Create the tracker's `issues` table if it doesn't exist. Everything
/// the analytics page needs is denormalized into Int/Text columns
/// (`created_day` / `completed_day` / `cycle_days`) so throughput and
/// cycle-time queries are plain GROUP BYs over base columns.
async fn init_schema(client: &Client) -> Result<(), String> {
    let sql = r"
        CREATE TABLE IF NOT EXISTS issues (
            id            BIGSERIAL PRIMARY KEY,
            title         TEXT   NOT NULL,
            status        TEXT   NOT NULL DEFAULT 'todo',
            priority      BIGINT NOT NULL DEFAULT 2,
            assignee      TEXT   NOT NULL DEFAULT '',
            project       TEXT   NOT NULL DEFAULT 'sync-engine',
            estimate      BIGINT NOT NULL DEFAULT 3,
            created_day   BIGINT NOT NULL,
            completed_day BIGINT NOT NULL DEFAULT 0,
            cycle_days    BIGINT NOT NULL DEFAULT 0
        );

        -- `REPLICA IDENTITY FULL` makes UPDATE/DELETE WAL records
        -- carry the *old* row in full, not just the primary key.
        -- The dataflow's diff stream needs that to compute retracts.
        ALTER TABLE issues REPLICA IDENTITY FULL;
    ";
    client
        .batch_execute(sql)
        .await
        .map_err(|err| format!("init_schema: {err}"))?;
    Ok(())
}

/// Create the publication and slot if they don't yet exist. Both are
/// idempotent — repeated boots are a no-op.
async fn ensure_publication_and_slot(client: &Client, settings: &PgSettings) -> Result<(), String> {
    let pub_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&settings.publication],
        )
        .await
        .map_err(|err| err.to_string())?
        .get(0);
    if pub_exists {
        let pub_sql = format!(
            "ALTER PUBLICATION {} SET TABLE issues",
            quote_ident(&settings.publication),
        );
        client
            .batch_execute(&pub_sql)
            .await
            .map_err(|err| format!("ALTER PUBLICATION: {err}"))?;
    } else {
        let pub_sql = format!(
            "CREATE PUBLICATION {} FOR TABLE issues",
            quote_ident(&settings.publication),
        );
        client
            .batch_execute(&pub_sql)
            .await
            .map_err(|err| format!("CREATE PUBLICATION: {err}"))?;
        info!(publication = %settings.publication, "created publication");
    }

    let slot_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&settings.slot],
        )
        .await
        .map_err(|err| err.to_string())?
        .get(0);
    if !slot_exists {
        client
            .query_one(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&settings.slot],
            )
            .await
            .map_err(|err| format!("pg_create_logical_replication_slot: {err}"))?;
        info!(slot = %settings.slot, "created logical replication slot");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Seed: a believable quarter of project history.
// ---------------------------------------------------------------------------

const TITLE_VERBS: &[&str] = &[
    "Fix",
    "Implement",
    "Refactor",
    "Design",
    "Investigate",
    "Document",
    "Optimize",
    "Migrate",
    "Add",
    "Remove",
    "Harden",
    "Profile",
];

const TITLE_SUBJECTS: &[&str] = &[
    "WAL cursor backpressure",
    "snapshot chunking",
    "permission rewriter caching",
    "reconnect backoff jitter",
    "TopK spill path",
    "diff coalescing",
    "JWT clock skew handling",
    "schema-change resync",
    "browser transport framing",
    "slot recovery runbook",
    "replica ack batching",
    "dataflow arrangement compaction",
    "CTE reuse detection",
    "gRPC-Web trailer handling",
    "flaky conformance test",
    "memory ceiling alerts",
    "TLS cert rotation",
    "metrics cardinality",
    "onboarding docs",
    "load-suite scenario drift",
    "bincode wire budget",
    "pgoutput TOAST columns",
    "subscription fan-out lock",
    "type generation for enums",
];

/// Per-status weights for seeded issues (indexes line up with
/// [`STATUSES`]). Most history is done; a healthy chunk is queued.
const SEED_STATUS_WEIGHTS: &[u32] = &[16, 14, 7, 4, 55, 4];

/// Per-priority weights (`0` none … `4` urgent).
const SEED_PRIORITY_WEIGHTS: &[u32] = &[8, 22, 38, 24, 8];

/// Per-project weights (indexes line up with [`PROJECTS`]).
const SEED_PROJECT_WEIGHTS: &[u32] = &[30, 24, 22, 16, 8];

/// Generate one synthetic issue. `id` is left to BIGSERIAL.
fn seed_issue(rng: &mut DemoRng, today: i64) -> Issue {
    let status = STATUSES[rng.weighted(SEED_STATUS_WEIGHTS)];
    let priority = i64::try_from(rng.weighted(SEED_PRIORITY_WEIGHTS)).unwrap_or(2);
    let project = PROJECTS[rng.weighted(SEED_PROJECT_WEIGHTS)];
    let estimate = ESTIMATES[rng.below(ESTIMATES.len())];
    // Assignment: done/in-flight issues always have an owner; queued
    // ones are unassigned about a third of the time.
    let assigned = matches!(status, "done" | "in_progress" | "in_review") || rng.below(3) > 0;
    let assignee = if assigned {
        USERS[rng.below(USERS.len())].id.to_owned()
    } else {
        String::new()
    };
    let age = 1 + i64::try_from(rng.below(usize::try_from(SEED_HISTORY_DAYS).unwrap_or(120)))
        .unwrap_or(1);
    let created_day = today - age;
    // Cycle time skews with priority: urgent work lands in days,
    // low-priority work meanders for weeks.
    let (completed_day, cycle_days) = if status == "done" {
        let base = match priority {
            4 => 1 + i64::try_from(rng.below(3)).unwrap_or(0),
            3 => 1 + i64::try_from(rng.below(6)).unwrap_or(0),
            2 => 2 + i64::try_from(rng.below(10)).unwrap_or(0),
            _ => 3 + i64::try_from(rng.below(21)).unwrap_or(0),
        };
        let cycle = base.min(age.max(1));
        (created_day + cycle, cycle)
    } else {
        (0, 0)
    };
    let title = format!(
        "{} {}",
        TITLE_VERBS[rng.below(TITLE_VERBS.len())],
        TITLE_SUBJECTS[rng.below(TITLE_SUBJECTS.len())],
    );
    Issue {
        id: 0,
        title,
        status: status.to_owned(),
        priority,
        assignee,
        project: project.to_owned(),
        estimate,
        created_day,
        completed_day,
        cycle_days,
    }
}

/// Seed the tracker on first boot: `SEED_ISSUES` issues distributed
/// over the past `SEED_HISTORY_DAYS` days. Inserted in one explicit
/// transaction so the replication consumer's transaction-boundary
/// batching collapses the whole seed into a single mirror LSN.
/// Idempotent — once the table has any rows we leave it alone.
async fn seed_if_empty(client: &Client) -> Result<(), String> {
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM issues", &[])
        .await
        .map_err(|err| err.to_string())?
        .get(0);
    if count > 0 {
        return Ok(());
    }

    let today = today_epoch_day();
    let mut rng = DemoRng::new(0x_5EED_1CE5);
    let mut sql = String::from(
        "BEGIN;\nINSERT INTO issues \
         (title, status, priority, assignee, project, estimate, \
          created_day, completed_day, cycle_days) VALUES\n",
    );
    for i in 0..SEED_ISSUES {
        let issue = seed_issue(&mut rng, today);
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push_str(&format!(
            "({}, {}, {}, {}, {}, {}, {}, {}, {})",
            quote_literal(&issue.title),
            quote_literal(&issue.status),
            issue.priority,
            quote_literal(&issue.assignee),
            quote_literal(&issue.project),
            issue.estimate,
            issue.created_day,
            issue.completed_day,
            issue.cycle_days,
        ));
    }
    sql.push_str(";\nCOMMIT;");
    client
        .batch_execute(&sql)
        .await
        .map_err(|err| format!("seed issues: {err}"))?;
    info!(rows = SEED_ISSUES, "seeded issue history");
    Ok(())
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote a text value as a SQL literal (single quotes doubled). Seed
/// titles come from fixed word lists, but quote anyway.
fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Read every pending WAL frame from the slot and throw it away,
/// advancing the slot's `confirmed_flush_lsn` past the seed. Run
/// once during bootstrap so the consumer doesn't replay events the
/// initial SELECT snapshot already captured.
async fn drain_slot(client: &Client, settings: &PgSettings) -> Result<(), String> {
    let rows = client
        .query(
            "SELECT pg_logical_slot_get_binary_changes(
                $1, NULL, NULL,
                'proto_version', '4',
                'publication_names', $2
             )",
            &[&settings.slot, &settings.publication],
        )
        .await
        .map_err(|err| format!("drain_slot: {err}"))?;
    if !rows.is_empty() {
        debug!(frames = rows.len(), "drained slot during bootstrap");
    }
    Ok(())
}

/// Resolve `issues.regclass::oid`. This value feeds directly into the
/// `TableId` Palimpsest tags every row with — the dataflow doesn't
/// dereference it, but it has to be consistent end-to-end so the
/// consumer's `DecodedEvent::Row { table, .. }` lands in the mirror.
async fn discover_oid(client: &Client) -> Result<u32, String> {
    let oid: u32 = client
        .query_one("SELECT 'public.issues'::regclass::oid", &[])
        .await
        .map_err(|err| err.to_string())?
        .get::<_, tokio_postgres::types::Oid>(0);
    Ok(oid)
}

/// Snapshot `issues` into memory.
pub async fn snapshot_issues(client: &Client) -> Result<Vec<Issue>, String> {
    let rows = client
        .query(
            "SELECT id, title, status, priority, assignee, project, estimate,
                    created_day, completed_day, cycle_days
             FROM issues ORDER BY id",
            &[],
        )
        .await
        .map_err(|err| err.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| Issue {
            id: row.get::<_, i64>(0),
            title: row.get::<_, String>(1),
            status: row.get::<_, String>(2),
            priority: row.get::<_, i64>(3),
            assignee: row.get::<_, String>(4),
            project: row.get::<_, String>(5),
            estimate: row.get::<_, i64>(6),
            created_day: row.get::<_, i64>(7),
            completed_day: row.get::<_, i64>(8),
            cycle_days: row.get::<_, i64>(9),
        })
        .collect())
}

/// Spawn the background consumer. Owns its own Postgres connection
/// so the write client isn't blocked on the long-running polling
/// query. Each tick calls `pg_logical_slot_get_binary_changes`,
/// decodes each frame via `palimpsest_wal::decode_pgoutput_message`,
/// and applies the result to the in-memory mirror.
pub fn spawn_consumer(
    settings: PgSettings,
    issues_oid: u32,
    store: Arc<IssueStore>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match run_consumer(settings, issues_oid, store).await {
            Ok(()) => info!("postgres consumer exited cleanly"),
            Err(err) => warn!(?err, "postgres consumer crashed"),
        }
    })
}

async fn run_consumer(
    settings: PgSettings,
    issues_oid: u32,
    store: Arc<IssueStore>,
) -> Result<(), String> {
    let client = connect_with_retry(&settings).await?;
    info!(
        slot = %settings.slot,
        publication = %settings.publication,
        "postgres consumer starting"
    );

    let mut catalog = Catalog::default();
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Each call to `pg_logical_slot_get_binary_changes` spins up a
    // fresh logical-decoding session inside Postgres, which prints
    // a LOG line. At 100ms poll cadence that's 10 LOG lines/sec into
    // the demo's logs. Peeking the slot's confirmed_flush_lsn against
    // current WAL is a cheap regular query that doesn't trigger
    // decoding — so we only call get_changes when there's actually
    // new WAL.
    let has_new_wal_sql = "SELECT pg_current_wal_lsn() > confirmed_flush_lsn
                           FROM pg_replication_slots
                           WHERE slot_name = $1";
    let get_changes_sql = "SELECT lsn, data FROM pg_logical_slot_get_binary_changes(
            $1, NULL, NULL,
            'proto_version', '4',
            'publication_names', $2
         )";

    // Per-transaction buffer. Postgres sends every change wrapped in
    // a Begin / Commit pair; we accumulate row changes and flush them
    // at Commit so the mirror's LSN bumps once per transaction (a
    // simulator batch becomes one cursor-pump wakeup, not many).
    let mut txn: Vec<IssueChange> = Vec::new();

    loop {
        tick.tick().await;

        match client.query_one(has_new_wal_sql, &[&settings.slot]).await {
            Ok(row) => {
                let has_new: bool = row.get(0);
                if !has_new {
                    continue;
                }
            }
            Err(err) => {
                warn!(?err, "slot peek failed; will retry");
                continue;
            }
        }

        let rows = match client
            .query(get_changes_sql, &[&settings.slot, &settings.publication])
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(?err, "slot poll failed; will retry");
                continue;
            }
        };
        if rows.is_empty() {
            continue;
        }

        for row in rows {
            let bytes: Vec<u8> = row.get::<_, Vec<u8>>(1);
            let frame = Bytes::from(bytes);
            match decode_pgoutput_message(&mut catalog, frame) {
                Ok(event) => handle_event(event, issues_oid, &store, &mut txn),
                Err(err) => {
                    warn!(?err, "decode_pgoutput_message failed");
                }
            }
        }
    }
}

/// Route a single `DecodedEvent` into the transaction buffer; flush
/// on `Commit`. `Begin` resets in case a previous transaction was
/// truncated by an error mid-stream.
fn handle_event(
    event: DecodedEvent,
    issues_oid: u32,
    store: &IssueStore,
    txn: &mut Vec<IssueChange>,
) {
    match event {
        DecodedEvent::Begin { .. } => txn.clear(),
        DecodedEvent::Commit { .. } => {
            store.apply_txn(txn);
            txn.clear();
        }
        DecodedEvent::Row {
            table,
            op,
            old,
            new,
        } => {
            if table == TableId::new(issues_oid) {
                if let Some(change) = issue_change(op, old, new) {
                    txn.push(change);
                }
            }
        }
        DecodedEvent::Schema { .. }
        | DecodedEvent::Heartbeat { .. }
        | DecodedEvent::Origin(_)
        | DecodedEvent::Stream(_)
        | DecodedEvent::TwoPhase(_)
        | DecodedEvent::Truncate(_)
        | DecodedEvent::Type(_)
        | DecodedEvent::Reconnect { .. }
        | DecodedEvent::Resync { .. } => {
            // Demo doesn't react to these yet. A production consumer
            // would resync on `Reconnect` / `Resync` by re-snapshotting.
        }
    }
}

fn issue_change(
    op: RowOp,
    old: Option<palimpsest_wal::Tuple>,
    new: Option<palimpsest_wal::Tuple>,
) -> Option<IssueChange> {
    match op {
        RowOp::Insert => new
            .and_then(|t| issue_from_tuple(&t))
            .map(IssueChange::Insert),
        RowOp::Update => {
            let prev = old.and_then(|t| issue_from_tuple(&t))?;
            let curr = new.and_then(|t| issue_from_tuple(&t))?;
            Some(IssueChange::Update { prev, curr })
        }
        RowOp::Delete => old
            .and_then(|t| issue_from_tuple(&t))
            .map(IssueChange::Delete),
    }
}

/// Pull the full issue row out of a pgoutput tuple. Layout matches the
/// table's column declaration order in `init_schema`.
fn issue_from_tuple(tuple: &palimpsest_wal::Tuple) -> Option<Issue> {
    use palimpsest_wal::Datum;
    let int = |idx: usize| -> Option<i64> {
        match tuple.get(idx)? {
            Datum::I64(v) => Some(*v),
            Datum::I32(v) => Some(i64::from(*v)),
            _ => None,
        }
    };
    let text = |idx: usize| -> Option<String> {
        match tuple.get(idx)? {
            Datum::Text(b) => Some(std::str::from_utf8(b).ok()?.to_owned()),
            _ => None,
        }
    };
    Some(Issue {
        id: int(0)?,
        title: text(1)?,
        status: text(2)?,
        priority: int(3)?,
        assignee: text(4)?,
        project: text(5)?,
        estimate: int(6)?,
        created_day: int(7)?,
        completed_day: int(8)?,
        cycle_days: int(9)?,
    })
}

/// Convert an `Issue` into the `Row` shape the dataflow's BaseTable
/// for `issues` expects. Column order matches `init_schema`.
pub fn issue_to_row(issue: &Issue) -> Row {
    let mut row: SmallVec<[palimpsest_wal::Datum; 8]> = SmallVec::new();
    row.push(palimpsest_wal::Datum::I64(issue.id));
    row.push(palimpsest_wal::Datum::Text(
        issue.title.clone().into_bytes().into(),
    ));
    row.push(palimpsest_wal::Datum::Text(
        issue.status.clone().into_bytes().into(),
    ));
    row.push(palimpsest_wal::Datum::I64(issue.priority));
    row.push(palimpsest_wal::Datum::Text(
        issue.assignee.clone().into_bytes().into(),
    ));
    row.push(palimpsest_wal::Datum::Text(
        issue.project.clone().into_bytes().into(),
    ));
    row.push(palimpsest_wal::Datum::I64(issue.estimate));
    row.push(palimpsest_wal::Datum::I64(issue.created_day));
    row.push(palimpsest_wal::Datum::I64(issue.completed_day));
    row.push(palimpsest_wal::Datum::I64(issue.cycle_days));
    row
}
