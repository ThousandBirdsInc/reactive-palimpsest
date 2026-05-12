//! Postgres connection + logical-replication consumer for the demo.
//!
//! Three responsibilities:
//!
//! 1. **Bootstrap** — connect, create tables + publication + slot,
//!    seed if empty, return the live `Client` the write API uses.
//! 2. **Initial snapshot** — `SELECT * FROM posts` / `events` so the
//!    in-memory mirror starts populated; the cursor for diffs starts
//!    *after* the snapshot LSN.
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

use crate::state::{Event, EventChange, EventStore, Post, PostChange, Store};

/// Replication slot + publication names the demo uses. Picked to be
/// stable across reboots so the slot retains WAL across restarts.
const PUBLICATION: &str = "palimpsest_pub";
const SLOT: &str = "palimpsest_demo";

/// Number of synthetic events the seed script generates. Same target
/// the in-memory demo used; pgcompletes it with `generate_series` so
/// boot is fast (~few seconds).
const SEED_EVENT_COUNT: i64 = 300_000;

/// Total wait time for Postgres to start accepting connections.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Replication-poll cadence. 100ms gives perceptually-live updates
/// (snappier than the previous 50ms tick on a 300k-row dataflow
/// rerun) without burning Postgres CPU.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

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
        let host = std::env::var("PALIMPSEST_DEMO_PG_HOST")
            .unwrap_or_else(|_| "localhost".to_owned());
        let port = std::env::var("PALIMPSEST_DEMO_PG_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5432);
        let user = std::env::var("PALIMPSEST_DEMO_PG_USER")
            .unwrap_or_else(|_| "palimpsest".to_owned());
        let password = std::env::var("PALIMPSEST_DEMO_PG_PASSWORD")
            .unwrap_or_else(|_| "demo".to_owned());
        let database = std::env::var("PALIMPSEST_DEMO_PG_DB")
            .unwrap_or_else(|_| "palimpsest_demo".to_owned());
        let publication = std::env::var("PALIMPSEST_DEMO_PG_PUBLICATION")
            .unwrap_or_else(|_| PUBLICATION.to_owned());
        let slot = std::env::var("PALIMPSEST_DEMO_PG_SLOT")
            .unwrap_or_else(|_| SLOT.to_owned());
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
    /// its own connection (replication slot polling shares the same
    /// connection but the future may split).
    pub settings: PgSettings,
    /// Postgres OID for the `posts` table — used as the `TableId`
    /// throughout the dataflow.
    pub posts_oid: u32,
    /// Postgres OID for the `events` table.
    pub events_oid: u32,
}

/// Connect, ensure schema + publication + slot, seed if empty, and
/// resolve the table OIDs the rest of the server uses as `TableId`s.
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
    let (posts_oid, events_oid) = discover_oids(&client).await?;
    info!(
        posts_oid,
        events_oid,
        "postgres bootstrap complete",
    );
    Ok(PgBootstrap {
        client: Arc::new(client),
        settings,
        posts_oid,
        events_oid,
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

/// Create the demo's two tables if they don't exist. Schema is
/// purposefully simple — id / title / published for posts, and
/// id / category_id / value for events — to mirror the in-memory
/// types the rest of the server already knows about.
async fn init_schema(client: &Client) -> Result<(), String> {
    let sql = r"
        CREATE TABLE IF NOT EXISTS posts (
            id        BIGSERIAL PRIMARY KEY,
            title     TEXT      NOT NULL,
            published BOOLEAN   NOT NULL DEFAULT FALSE
        );

        -- `REPLICA IDENTITY FULL` makes UPDATE/DELETE WAL records
        -- carry the *old* row in full, not just the primary key.
        -- The dataflow's diff stream needs that to compute retracts.
        ALTER TABLE posts REPLICA IDENTITY FULL;

        CREATE TABLE IF NOT EXISTS events (
            id          BIGSERIAL PRIMARY KEY,
            category_id BIGINT    NOT NULL,
            value       BIGINT    NOT NULL
        );

        ALTER TABLE events REPLICA IDENTITY FULL;
    ";
    client
        .batch_execute(sql)
        .await
        .map_err(|err| format!("init_schema: {err}"))?;
    Ok(())
}

/// Create `palimpsest_pub` and `palimpsest_demo` if they don't yet
/// exist. Both are idempotent — repeated boots are a no-op.
async fn ensure_publication_and_slot(
    client: &Client,
    settings: &PgSettings,
) -> Result<(), String> {
    // Publication: covers both tables. Postgres complains if the
    // publication already exists, so check first.
    let pub_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&settings.publication],
        )
        .await
        .map_err(|err| err.to_string())?
        .get(0);
    if !pub_exists {
        let pub_sql = format!(
            "CREATE PUBLICATION {} FOR TABLE posts, events",
            quote_ident(&settings.publication),
        );
        client
            .batch_execute(&pub_sql)
            .await
            .map_err(|err| format!("CREATE PUBLICATION: {err}"))?;
        info!(publication = %settings.publication, "created publication");
    }

    // Replication slot: only create if missing. We deliberately use
    // a *named* slot so WAL is retained across server restarts.
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

/// Seed `posts` (3 hand-written rows) and `events` (300k synthetic
/// rows via `generate_series`) on first boot. Idempotent — once the
/// tables hold rows we leave them alone.
async fn seed_if_empty(client: &Client) -> Result<(), String> {
    let posts_count: i64 = client
        .query_one("SELECT COUNT(*) FROM posts", &[])
        .await
        .map_err(|err| err.to_string())?
        .get(0);
    if posts_count == 0 {
        client
            .batch_execute(
                r"
                INSERT INTO posts (title, published) VALUES
                    ('Welcome to Palimpsest', TRUE),
                    ('Subscribe to a live SQL view', TRUE),
                    ('Draft: in-progress writeup', FALSE);
                ",
            )
            .await
            .map_err(|err| format!("seed posts: {err}"))?;
        info!("seeded posts (3 rows)");
    }

    let events_count: i64 = client
        .query_one("SELECT COUNT(*) FROM events", &[])
        .await
        .map_err(|err| err.to_string())?
        .get(0);
    if events_count == 0 {
        // Reproducible randomness — `setseed` is per-session so the
        // demo's "top categories" view is deterministic between
        // fresh boots.
        client
            .batch_execute(&format!(
                r"
                SELECT setseed(0.42);
                INSERT INTO events (category_id, value)
                SELECT (floor(random() * 50) + 1)::bigint,
                       (floor(random() * 1000))::bigint
                FROM generate_series(1, {SEED_EVENT_COUNT});
                "
            ))
            .await
            .map_err(|err| format!("seed events: {err}"))?;
        info!(rows = SEED_EVENT_COUNT, "seeded events");
    }

    Ok(())
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
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

/// Resolve `posts.regclass::oid` + `events.regclass::oid`. These
/// values feed directly into the `TableId` Palimpsest tags every
/// row with — the dataflow doesn't dereference them, but they have
/// to be consistent end-to-end so the consumer's
/// `DecodedEvent::Row { table, .. }` lands in the right mirror.
async fn discover_oids(client: &Client) -> Result<(u32, u32), String> {
    let posts_oid: u32 = client
        .query_one("SELECT 'public.posts'::regclass::oid", &[])
        .await
        .map_err(|err| err.to_string())?
        .get::<_, tokio_postgres::types::Oid>(0);
    let events_oid: u32 = client
        .query_one("SELECT 'public.events'::regclass::oid", &[])
        .await
        .map_err(|err| err.to_string())?
        .get::<_, tokio_postgres::types::Oid>(0);
    Ok((posts_oid, events_oid))
}

/// Snapshot `posts` into memory.
pub async fn snapshot_posts(client: &Client) -> Result<Vec<Post>, String> {
    let rows = client
        .query("SELECT id, title, published FROM posts ORDER BY id", &[])
        .await
        .map_err(|err| err.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| Post {
            id: row.get::<_, i64>(0),
            title: row.get::<_, String>(1),
            published: row.get::<_, bool>(2),
        })
        .collect())
}

/// Snapshot `events` into memory. 300k rows ≈ 18MB on the wire
/// over the local socket; comfortably within psql's defaults.
pub async fn snapshot_events(client: &Client) -> Result<Vec<Event>, String> {
    let rows = client
        .query(
            "SELECT id, category_id, value FROM events ORDER BY id",
            &[],
        )
        .await
        .map_err(|err| err.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| Event {
            id: row.get::<_, i64>(0),
            category_id: row.get::<_, i64>(1),
            value: row.get::<_, i64>(2),
        })
        .collect())
}

/// Spawn the background consumer. Owns its own Postgres connection
/// so the write client isn't blocked on the long-running polling
/// query. Each tick calls `pg_logical_slot_get_binary_changes`,
/// decodes each frame via `palimpsest_wal::decode_pgoutput_message`,
/// and applies the result to the in-memory mirrors.
///
/// The task runs until `cancel` fires or the connection dies, then
/// exits — the parent supervises and would restart it in production.
pub fn spawn_consumer(
    settings: PgSettings,
    posts_oid: u32,
    events_oid: u32,
    store: Arc<Store>,
    event_store: Arc<EventStore>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match run_consumer(settings, posts_oid, events_oid, store, event_store).await {
            Ok(()) => info!("postgres consumer exited cleanly"),
            Err(err) => warn!(?err, "postgres consumer crashed"),
        }
    })
}

async fn run_consumer(
    settings: PgSettings,
    posts_oid: u32,
    events_oid: u32,
    store: Arc<Store>,
    event_store: Arc<EventStore>,
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
    // a LOG line ("starting logical decoding for slot ..."). At 100ms
    // poll cadence that's 10 LOG lines/sec into the demo's logs.
    // Peeking the slot's confirmed_flush_lsn against current WAL is
    // a cheap regular query that doesn't trigger decoding — so we
    // only call get_changes when there's actually new WAL.
    //
    // Streaming-replication (`START_REPLICATION SLOT ... LOGICAL`)
    // would be the right long-term answer (one decoding session per
    // connection, not per poll), but tokio-postgres 0.7 doesn't
    // expose the COPY_BOTH protocol needed for that. For the demo,
    // the peek-first dance is sufficient.
    let has_new_wal_sql = "SELECT pg_current_wal_lsn() > confirmed_flush_lsn
                           FROM pg_replication_slots
                           WHERE slot_name = $1";
    let get_changes_sql = "SELECT lsn, data FROM pg_logical_slot_get_binary_changes(
            $1, NULL, NULL,
            'proto_version', '4',
            'publication_names', $2
         )";

    // Per-transaction buffer. Postgres sends every change wrapped in
    // a Begin / Commit pair; we accumulate row changes per table and
    // flush them at Commit so the mirror's LSN bumps once per
    // transaction (a 1000-row bulk INSERT becomes one cursor-pump
    // wakeup, not 1000).
    let mut txn = TxnBuffer::default();

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
                Ok(event) => handle_event(
                    event,
                    posts_oid,
                    events_oid,
                    &store,
                    &event_store,
                    &mut txn,
                ),
                Err(err) => {
                    warn!(?err, "decode_pgoutput_message failed");
                }
            }
        }
    }
}

#[derive(Default)]
struct TxnBuffer {
    posts: Vec<PostChange>,
    events: Vec<EventChange>,
}

impl TxnBuffer {
    fn clear(&mut self) {
        self.posts.clear();
        self.events.clear();
    }

    fn flush(&mut self, store: &Store, event_store: &EventStore) {
        if !self.posts.is_empty() {
            store.apply_txn(&self.posts);
        }
        if !self.events.is_empty() {
            event_store.apply_txn(&self.events);
        }
        self.clear();
    }
}

/// Route a single `DecodedEvent` into the per-table buffer; flush
/// on `Commit`. `Begin` resets in case a previous transaction was
/// truncated by an error mid-stream.
fn handle_event(
    event: DecodedEvent,
    posts_oid: u32,
    events_oid: u32,
    store: &Store,
    event_store: &EventStore,
    txn: &mut TxnBuffer,
) {
    match event {
        DecodedEvent::Begin { .. } => {
            txn.clear();
        }
        DecodedEvent::Commit { .. } => {
            txn.flush(store, event_store);
        }
        DecodedEvent::Row {
            table,
            op,
            old,
            new,
        } => {
            if table == TableId::new(posts_oid) {
                if let Some(change) = post_change(op, old, new) {
                    txn.posts.push(change);
                }
            } else if table == TableId::new(events_oid) {
                if let Some(change) = event_change(op, old, new) {
                    txn.events.push(change);
                }
            }
        }
        DecodedEvent::Schema { .. }
        | DecodedEvent::Heartbeat { .. }
        | DecodedEvent::Origin(_)
        | DecodedEvent::Stream(_)
        | DecodedEvent::TwoPhase(_)
        | DecodedEvent::Truncate(_)
        | DecodedEvent::Reconnect { .. }
        | DecodedEvent::Resync { .. } => {
            // Demo doesn't react to these yet. A production consumer
            // would resync on `Reconnect` / `Resync` by re-snapshotting.
        }
    }
}

fn post_change(
    op: RowOp,
    old: Option<palimpsest_wal::Tuple>,
    new: Option<palimpsest_wal::Tuple>,
) -> Option<PostChange> {
    match op {
        RowOp::Insert => new.and_then(|t| post_from_tuple(&t)).map(PostChange::Insert),
        RowOp::Update => {
            let prev = old.and_then(|t| post_from_tuple(&t))?;
            let curr = new.and_then(|t| post_from_tuple(&t))?;
            Some(PostChange::Update { prev, curr })
        }
        RowOp::Delete => old.and_then(|t| post_from_tuple(&t)).map(PostChange::Delete),
    }
}

fn event_change(
    op: RowOp,
    old: Option<palimpsest_wal::Tuple>,
    new: Option<palimpsest_wal::Tuple>,
) -> Option<EventChange> {
    match op {
        RowOp::Insert => new
            .and_then(|t| event_from_tuple(&t))
            .map(EventChange::Insert),
        RowOp::Update => {
            let prev = old.and_then(|t| event_from_tuple(&t))?;
            let curr = new.and_then(|t| event_from_tuple(&t))?;
            Some(EventChange::Update { prev, curr })
        }
        RowOp::Delete => old
            .and_then(|t| event_from_tuple(&t))
            .map(EventChange::Delete),
    }
}

/// Pull `(id, title, published)` out of a pgoutput tuple. Layout
/// matches the table's column declaration order.
fn post_from_tuple(tuple: &palimpsest_wal::Tuple) -> Option<Post> {
    use palimpsest_wal::Datum;
    let id = match tuple.first()? {
        Datum::I64(v) => *v,
        Datum::I32(v) => i64::from(*v),
        _ => return None,
    };
    let title = match tuple.get(1)? {
        Datum::Text(b) => std::str::from_utf8(b).ok()?.to_owned(),
        _ => return None,
    };
    let published = match tuple.get(2)? {
        Datum::Bool(v) => *v,
        _ => return None,
    };
    Some(Post {
        id,
        title,
        published,
    })
}

fn event_from_tuple(tuple: &palimpsest_wal::Tuple) -> Option<Event> {
    use palimpsest_wal::Datum;
    let id = match tuple.first()? {
        Datum::I64(v) => *v,
        Datum::I32(v) => i64::from(*v),
        _ => return None,
    };
    let category_id = match tuple.get(1)? {
        Datum::I64(v) => *v,
        Datum::I32(v) => i64::from(*v),
        _ => return None,
    };
    let value = match tuple.get(2)? {
        Datum::I64(v) => *v,
        Datum::I32(v) => i64::from(*v),
        _ => return None,
    };
    Some(Event {
        id,
        category_id,
        value,
    })
}

// -----------------------------------------------------------------------------
// `Row` helpers re-used by the apply layer — keep here so state.rs
// stays focused on the in-memory snapshot + journal it exposes to
// the WAL runtime.
// -----------------------------------------------------------------------------

/// Convert a `Post` into the `Row` shape the dataflow's BaseTable
/// for `posts` expects. Column order matches `init_schema`.
pub fn post_to_row(post: &Post) -> Row {
    let mut row: SmallVec<[palimpsest_wal::Datum; 8]> = SmallVec::new();
    row.push(palimpsest_wal::Datum::I64(post.id));
    row.push(palimpsest_wal::Datum::Text(
        post.title.clone().into_bytes().into(),
    ));
    row.push(palimpsest_wal::Datum::Bool(post.published));
    row
}

/// Same shape as `post_to_row` for events.
pub fn event_to_row(ev: &Event) -> Row {
    let mut row: SmallVec<[palimpsest_wal::Datum; 8]> = SmallVec::new();
    row.push(palimpsest_wal::Datum::I64(ev.id));
    row.push(palimpsest_wal::Datum::I64(ev.category_id));
    row.push(palimpsest_wal::Datum::I64(ev.value));
    row
}
