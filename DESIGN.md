# Palimpsest — Sync Engine Design Doc

**Project name:** Palimpsest
**Status:** Draft v0.2
**Owner:** colton@thousandbirds.ai
**Last updated:** 2026-05-07

## 1. Summary

A Rust service that tails a Postgres write-ahead log, incrementally maintains
the result sets of developer-authored SQL queries (including CTEs), and pushes
row-level diffs to subscribed clients over gRPC-Web. Clients use a Rust-built
WASM library to subscribe, mutate, and re-subscribe queries from the browser.
The server is shipped as an embeddable Rust library: by default any client may
subscribe to any query, but operators can attach **permission queries** —
SQL predicates that filter what each client is allowed to see.

The design borrows partial-materialization and dataflow-compilation ideas from
**Noria**, uses **differential-dataflow on top of timely-dataflow** as the
incremental view-maintenance substrate, derives WAL ingest from
**pg-walstream**, and adopts Figma **LiveGraph**'s strategies for subquery
sharing, ordered exactly-once delivery, and server-side permission filtering.

## 2. Goals and non-goals

### Goals

1. Sub-100ms median update latency from Postgres commit to client receipt for
   small transactions.
2. Support a useful subset of read-only SQL: `SELECT`, joins, filters,
   projections, aggregations, `GROUP BY`, `ORDER BY` + `LIMIT`, `DISTINCT`,
   `UNION ALL`, and **non-recursive CTEs**.
3. Bounded memory: per-subscription state size grows with the result set the
   client cares about, *not* with the size of the underlying tables.
4. Configurable row-level authorization without forcing developers to fork
   their queries per role.
5. WASM client small enough to ship in a web app (target: < 500 KB gzipped).
6. At-least-once, in-order delivery per query subscription, with client-side
   dedupe by LSN.

### Non-goals (v1)

- Recursive CTEs, window functions, full-text search, JSONB path operators.
- Writes through the sync engine (clients write directly to Postgres; we only
  observe).
- Multi-Postgres / multi-tenant federation.
- Geographic replication of the sync engine itself.
- Schema migration orchestration.

## 3. Prior art

### 3.1 Noria (`/Users/coltonpierson/workspace/reference/noria`)

Noria is the closest academic precedent: a Rust dataflow engine that compiles
SQL into a graph of stateful operators and serves materialized views. The
pieces we steal:

- **MIR (middle intermediate representation)** sits between parsed SQL and
  the dataflow graph. `MirNodeType` enumerates `Aggregation`, `Filter`,
  `Join`, `LeftJoin`, `Project`, `Union`, `Distinct`, `TopK`, `Identity`,
  `Reuse`, `Leaf`
  (`server/mir/src/node.rs:399-491`).
- **Partial materialization with upqueries.** State is keyed by index columns;
  missing keys are "holes" (`mark_hole` / `mark_filled` in
  `server/dataflow/src/state/keyed_state.rs:36-79`). When a downstream
  operator needs a hole filled, an upquery walks back to a base table or
  persistent state and replays just the keys it needs. This is the single
  most important idea for our memory budget.
- **Domain scheduling.** The graph is partitioned into "domains"; each domain
  is a single tokio task with no internal locking (`server/dataflow/src/domain/mod.rs:114-136`).
- **Reuse detection.** A Finkelstein-style pass detects subqueries that are
  structurally equivalent and shares the operator subgraph
  (`server/src/controller/sql/reuse/mod.rs`).

What Noria does *not* give us:

- **No CTE support** in the parser or MIR compiler. Our users explicitly want
  CTEs, so we will add them.
- **No WAL ingestion.** Noria's base tables are written through a client API
  (`noria/src/table.rs:35-54`); we replace that path entirely.
- **No push subscriptions.** Clients hit `ReadQuery::Normal` as a point query
  and get an eviction error if their key was thrown out
  (`noria/src/view.rs:117-142`). We need long-lived streaming subscriptions.

### 3.2 timely-dataflow (`/Users/coltonpierson/workspace/reference/timely-dataflow`)

Timely is the execution substrate. Its **capability/frontier** progress system
is exactly what we need to map LSNs to logical time:

- A `Capability<T>` (`timely/src/dataflow/operators/capability.rs:63`) is a
  reference-counted token saying "I may still emit data at timestamp T."
- A `Frontier` (`timely/src/progress/frontier.rs:20`) is the antichain of
  minimum timestamps still in flight.
- Once all capabilities ≤ T are released, every operator in the graph
  knows no more data ≤ T will arrive — meaning it is safe to evict
  per-LSN state.

We use **LSN as the timestamp type.** When a Postgres `COMMIT` arrives at
LSN L, we downgrade the WAL operator's capability past L; downstream operators
treat this as "all updates ≤ L are now visible."

Timely is embeddable in tokio: `Worker::new(...)` + `worker.step()` is
non-blocking (`timely/examples/threadless.rs:1-34`), so we co-locate it with
our gRPC server in a single process.

Timely alone does not provide retractions or arrangements; **differential-
dataflow** (a separate crate built on timely) does. We use differential as
the operator library.

### 3.3 pg-walstream (`/Users/coltonpierson/workspace/reference/pg-walstream`)

Already speaks the Postgres logical replication protocol (pgoutput, v1–v4)
with streaming and two-phase commit support. Provides:

- `LogicalReplicationStream` with `next_event(...)` async iterator
  (`src/stream.rs:35`).
- Per-row diffs with old + new tuples
  (`EventType::{Insert,Update,Delete}` in `src/types.rs:475-497`).
- Explicit `Begin`/`Commit` framing and `StreamStart`/`StreamStop` for
  large transactions (`src/types.rs:500-603`).
- LSN feedback to advance the replication slot
  (`SharedLsnFeedback` in `src/lsn.rs:43`, status update in
  `src/stream.rs:1421-1426`).
- TOAST-aware: unchanged TOAST columns are skipped rather than buffered
  (`src/protocol.rs:345`).
- Reconnect with exponential backoff (`src/stream.rs:742+`).

We fork it into our own crate (`palimpsest-wal`) to add:

- A typed-tuple layer that maps Postgres OIDs to our internal `Datum` enum.
- Spill-to-disk for very large transactions (current code holds segment
  bytes in RAM).
- External slot-recovery hooks for failover scenarios.

### 3.4 Figma LiveGraph

Figma's blog post is the closest production system to what we are building.
Lessons we adopt directly:

- **Decompose views into a tree of trivial subqueries.** Each leaf is a
  `SELECT cols FROM table WHERE ...` with no joins. This makes deduplication
  obvious: "we can deduplicate a lot of work by sharing subqueries between
  live view trees, effectively creating a caching layer."
- **Filter on the server before transmission.** "The server will not send
  the client any instances of data that fail the permission checks."
- **Permissions can be data-dependent**, evaluated against subviews — so when
  the data backing a permission changes, the permission itself updates in
  real time.
- **Single ordered WAL stream → ordered, exactly-once delivery per
  subscription.** When a client disconnects, the library reconnects and
  refetches. (We do the same with an LSN resume token.)
- Reported throughput: ~10k writes/sec per instance, ms-scale propagation
  latency. We target the same envelope.

### 3.5 GlueSQL (`/Users/coltonpierson/workspace/reference/gluesql`)

GlueSQL is an embeddable SQL library — a `sqlparser-rs`-based frontend
plus a row-at-a-time Volcano executor with pluggable storage backends
(`README.md:11`, `Cargo.toml:3-24`). We considered it as a SQL frontend
shortcut. Useful pieces:

- **Translate layer** (`gluesql-core/src/translate.rs:48-50`) lowers
  `sqlparser::ast::Statement` to a normalized internal AST
  (`gluesql::ast::Statement` in `query.rs:49-141`), absorbing about
  300 lines of boilerplate around alias resolution and object-name
  handling.
- **Planner-annotated `Select`** (`query.rs:31-44`) carries
  `aggregate_slots` populated by the planner — a clean intermediate
  between AST and execution that we could lower into our MIR.
- Postgres-dialect parsing via `sqlparser-rs` `PostgreSqlDialect`
  (`parse_sql.rs:15`), including `::` casts.

What rules it out as more than a parser shortcut:

- **No CTE support.** Neither `WITH` nor `WITH RECURSIVE` exists in
  the AST or translate paths (no `Cte` variant in `ast/query.rs`).
  CTEs are a v1 feature for Palimpsest, so we'd be adding them
  ourselves either way.
- **Volcano executor is incompatible with IVM.** `Store::scan_data`
  returns a `Stream<(Key, Vec<Value>)>` (`store.rs:43-75`) and the
  executor materializes joins/aggregates row-by-row
  (`executor/join.rs`, `executor/aggregate.rs`). There is no
  changelog/delta hook; the execution code is dead weight for us.
- **No window functions, no right/full outer joins**
  (`ast/query.rs:152-155`).
- **`Value` enum is independent of Postgres OIDs**
  (`data/value.rs:43-71`). We'd have to bridge to our `Datum` type
  anyway.

**Decision.** Skip GlueSQL as a dependency. Use `sqlparser-rs` 0.52+
directly for parsing and write our own MIR lowering (per §8). We
treat GlueSQL's translate layer as a reference implementation for
alias/object-name normalization passes — copy the patterns, not the
code. Reconsidered if we ever want to expose a non-Postgres SQL
dialect, where GlueSQL's dialect-pluggable frontend would be more
valuable.

## 4. High-level architecture

```
   ┌──────────────┐
   │  Postgres    │
   │  (primary)   │
   └──────┬───────┘
          │ logical replication (pgoutput)
          ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │                      sync-engine process                           │
   │                                                                    │
   │  ┌──────────────┐  ┌──────────┐    ┌────────────────────────────┐  │
   │  │ palimpsest-  │→ │ Decoder/ │ →  │   timely worker            │  │
   │  │    wal       │  │  Catalog │    │   (differential dataflow)  │  │
   │  └──────────────┘  └──────────┘    │                            │  │
   │                                    │   - shared op subgraphs    │  │
   │                                    │   - partial materialization│  │
   │                                    │   - permission predicates  │  │
   │                                    └─────────────┬──────────────┘  │
   │                                                  │                 │
   │                                                  ▼                 │
   │                                    ┌────────────────────────────┐  │
   │                                    │  Subscription Router       │  │
   │                                    │  (per-client diff fan-out) │  │
   │                                    └─────────────┬──────────────┘  │
   │                                                  │                 │
   │                                    ┌─────────────▼──────────────┐  │
   │                                    │  gRPC-Web server (tonic +  │  │
   │                                    │  tonic-web)                │  │
   │                                    └─────────────┬──────────────┘  │
   └──────────────────────────────────────────────────┼─────────────────┘
                                                      │
                                                      ▼
                                          ┌──────────────────────────┐
                                          │  palimpsest-client       │
                                          │  (Rust → wasm32)         │
                                          │  - subscribe(query)      │
                                          │  - update(query)         │
                                          │  - on_diff(...)          │
                                          └──────────────────────────┘
```

A single tokio runtime hosts:

1. The `palimpsest-wal` consumer task (one per upstream Postgres).
2. A timely `Worker` driven by an `async fn step_loop()` that pulls
   decoded events, advances input frontiers, and calls `worker.step()`
   in a bounded loop until the probe catches up
   (mirrors `threadless.rs:8-32`).
3. The tonic gRPC server, which holds `Sender<Diff>` handles into a
   fan-out router.

Co-locating these in one process keeps the diff path zero-copy: a row
update produced by an operator session can be cloned by `bytes::Bytes`
arc-share into N subscription channels without re-serializing.

## 5. Crate layout

| Crate                  | Purpose                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `palimpsest-proto`      | `tonic-build` generated gRPC types and shared wire structs.             |
| `palimpsest-wal`        | Fork of pg-walstream; logical replication client + Postgres catalog.    |
| `palimpsest-sql`        | SQL parser (sqlparser-rs) + MIR + planner; emits dataflow build plans.  |
| `palimpsest-dataflow`   | Operator implementations on timely + differential; partial state mgmt.  |
| `palimpsest-permissions`| Permission-query DSL, compilation, and rewriter that joins predicates.  |
| `palimpsest-server`     | Embeddable library: ties WAL → dataflow → router → gRPC.                |
| `palimpsest-client`     | Rust client with `wasm32-unknown-unknown` target; uses `tonic-web-wasm-client`. |
| `palimpsest-cli`        | Optional binary for running the server with a config file.              |

`palimpsest-server` is the integration crate users embed; `palimpsest-cli` is a
thin wrapper for the standalone case.

## 6. Component: WAL ingest (`palimpsest-wal`)

Forked from pg-walstream. Surface:

```rust
pub struct WalSource {
    inner: LogicalReplicationStream, // upstream type
    catalog: Arc<RwLock<Catalog>>,
}

pub enum DecodedEvent {
    Begin    { xid: u32, commit_lsn: Lsn },
    Row      { table: TableId, op: RowOp, old: Option<Tuple>, new: Option<Tuple> },
    Commit   { commit_lsn: Lsn, end_lsn: Lsn },
    Schema   { table: TableId, columns: Vec<ColumnDef> },
    Heartbeat{ lsn: Lsn },
}

impl WalSource {
    pub fn new(cfg: WalConfig) -> Result<Self> { ... }
    pub fn stream(&mut self) -> impl Stream<Item = Result<DecodedEvent>>;
    pub fn ack(&mut self, lsn: Lsn);  // → SharedLsnFeedback::update_applied_lsn
}
```

Differences from pg-walstream:

- `Tuple` is a typed `SmallVec<[Datum; 8]>` with the OID-to-Rust mapping
  done once, in the decoder, rather than passing `ColumnValue::{Text,Binary}(Bytes)`
  through the pipeline. This is also where TOAST "unchanged" markers are
  expanded by looking up the prior value (only when an operator needs the
  full tuple — see §9 on memory).
- `Schema` events trigger a catalog refresh, which may invalidate
  compiled queries. v1 reaction: terminate affected subscriptions with
  a typed error and let clients re-submit.
- Large transactions: when pg-walstream emits `StreamStart`, we route
  segments through a bounded mpsc channel. If the channel saturates, we
  spill segments to a temp file keyed by xid and replay on `StreamCommit`.
  This is the gap noted in the pg-walstream survey (`src/stream.rs:590`
  buffers an entire event in RAM); we extend it.

Slot management: we keep `restart_lsn` in our own metadata table so that
on Postgres failover or accidental slot deletion we can recreate a slot
and reconcile by replaying from the snapshot exported at slot creation
(`src/stream.rs:416-432`).

## 7. Component: Catalog

A `Catalog` holds, per-table:

- OID → `TableId` mapping.
- Column definitions with Postgres type OID + Rust `DatumType`.
- Replica identity (Default / Full / Index / Nothing). Determines what
  `old` tuple values pg gives us for `UPDATE`/`DELETE`. Operators that
  need full prior rows but only get a key require an upquery (§9).
- Primary key columns. Used as the default row identity in client diffs.

The catalog is loaded at startup via a single SQL probe (`pg_class`,
`pg_attribute`, `pg_index`) and refreshed on `Schema` WAL events.

## 8. Component: Query compilation (`palimpsest-sql`)

Pipeline: **SQL text → AST → MIR → DFG (dataflow graph) → handle**.

### 8.1 Parsing

We use `sqlparser-rs` directly (broader feature support than `nom-sql`,
Noria's parser; and we rejected GlueSQL as a wrapper — see §3.5).
The parser already understands CTEs (`WITH ... AS (...)`), which is
the user-facing reason to take this dependency. For alias and
object-name normalization passes between AST and MIR we mirror the
patterns in GlueSQL's `translate.rs` rather than depending on it.

### 8.2 MIR

A pared-down version of Noria's MIR
(`server/mir/src/node.rs:399-491`):

```rust
pub enum MirNodeKind {
    BaseTable { table: TableId, project: Vec<ColumnRef> },
    Filter    { predicate: Predicate },
    Project   { columns: Vec<Expr> },
    Join      { kind: JoinKind, on: Vec<(ColumnRef, ColumnRef)> },
    Aggregate { group_by: Vec<ColumnRef>, aggs: Vec<AggExpr> },
    Distinct,
    Union,
    TopK      { order_by: Vec<OrderKey>, limit: usize, offset: usize },
    CteRef    { cte: CteId },          // pointer into the CTE table
    Leaf      { name: QueryId },
}
```

CTE handling: a `WITH x AS (q) SELECT ...` parses to a `Query` with a
`Vec<Cte>`. The MIR builder compiles each CTE into its own MIR subgraph
and replaces references with `CteRef { cte: CteId }`. CTEs referenced
multiple times in the same query become a single subgraph with multiple
outgoing edges — the same mechanism we use for cross-query reuse (§8.4).

Recursive CTEs (`WITH RECURSIVE`) are rejected at parse time in v1.

### 8.3 MIR → dataflow

A topological walk lowers MIR to a differential-dataflow build plan.
Each MIR node maps to one or more differential operators. Examples:

- `Filter` → `.filter(|row| ...)`.
- `Project` → `.map(|row| ...)`.
- `Join` (equi-join on key columns) → `.join_map(...)` on arrangements
  of both inputs keyed by the join columns.
- `Aggregate` → `.reduce(...)` over `(group_key, value)` collections.
- `TopK` → `.reduce` followed by `.consolidate` and a custom slice op.
- `BaseTable` → reads from the `Collection<Row, Lsn, isize>` produced
  by the WAL operator (§9).

The build plan is a `dyn Fn(&mut Scope)` closure submitted to the timely
worker. The worker returns a `ProbeHandle` and a `Trace` (arrangement
handle) that the subscription router uses to read the current snapshot
and stream subsequent diffs.

### 8.4 Canonicalization and reuse

Before lowering, we canonicalize the MIR (alpha-renaming column aliases,
sorting commutative operands, normalizing predicate forms) and hash it.
Two queries — or two CTEs — with the same canonical form share a single
operator subgraph (and therefore a single arrangement). This is the
dataflow analogue of Figma's "shared subquery cache." We track sharing
with a refcount; the subgraph and its state are only torn down when the
last subscription holding it goes away.

### 8.5 Validation

Compile-time checks:

- All referenced tables exist in the catalog.
- All referenced columns exist with compatible types.
- `JOIN ... ON` uses equi-predicates (we reject theta joins in v1; they
  would produce unbounded intermediate state).
- `ORDER BY` with no `LIMIT` is rejected (would require materializing
  full ordering).
- Aggregations must have a `GROUP BY` matching the projection or be
  scalar (single-row).

## 9. Component: Dataflow engine and memory strategy

This is where the design earns or loses on memory. The plan stacks four
techniques.

### 9.1 Partial materialization

Borrowed wholesale from Noria
(`server/dataflow/src/state/keyed_state.rs:36-79`). Operator state is
indexed by the keys downstream consumers actually ask for. A key with
no outstanding subscription is a hole; on first request it triggers an
**upquery** that backfills only the requested keys by replaying from
parent state or, ultimately, from a fresh `SELECT ... WHERE key IN (...)`
snapshot against Postgres.

For our streaming-subscription model the rule is:

> An operator must hold state for every key K such that **some live
> subscription's result depends on K**.

Concretely:

- A subscription starts with a snapshot `SELECT` issued against Postgres
  at LSN L₀. The result rows seed the leaf node's state.
- The keys touched by that snapshot are propagated as upquery requests
  back through the operator chain, populating intermediate arrangements
  on the path.
- WAL events at LSN > L₀ thread through the same arrangements as
  retraction-and-insert pairs (differential's native form).
- When the subscription is dropped, refcounts on the touched keys
  drop; keys that hit zero are evicted.

### 9.2 Subgraph sharing

The canonicalization pass (§8.4) ensures that N subscriptions to
"posts where author_id = $me" share one filtered arrangement keyed by
`author_id`. The arrangement holds only those `author_id` values
currently subscribed.

### 9.3 Bounded per-subscription buffers + backpressure

Each subscription has an mpsc channel of fixed depth (default 256
diffs). If a slow client fills the buffer, we either:

- Drop the channel and disconnect the client with `RESYNC_REQUIRED`,
  forcing a re-subscribe (initial snapshot re-issued at current LSN).
- Or, if the result set is small, switch to "coalesced" mode: collapse
  pending diffs into a single snapshot before transmission.

We never grow per-subscription buffers without bound. This is the
biggest practical defense against memory blow-ups under client
slowness.

### 9.4 LSN watermark eviction

Differential's `Trace::set_logical_compaction(...)` lets us tell each
arrangement "no consumer will ever ask for a frontier earlier than
LSN L_min." We compute L_min as `min(active_subscription_lsns)` and
advance it whenever a subscription acks. Older versions are then
compacted out of arrangements, bounding history depth.

### 9.5 TOAST handling

pg-walstream skips TOAST "unchanged" columns (`src/protocol.rs:345`).
For operators that need the full row (e.g. an aggregate whose value
depends on a TOASTed column), we look up the prior value in the
operator's own arrangement rather than re-fetching from Postgres. If
the operator does not have that value (cold path), we issue a targeted
`SELECT col FROM t WHERE pk = $1` upquery.

### 9.6 Spill-to-disk (optional, v1.5)

For deployments with extreme cardinality, differential supports
swapping arrangement backings. We can plug in a RocksDB-backed trace
behind a feature flag. v1 is RAM-only; we add this if we hit the
ceiling in production.

## 10. Component: Subscription router

Per-subscription state:

```rust
struct Subscription {
    id: SubscriptionId,
    query: QueryId,                     // canonical query handle
    user_ctx: UserContext,              // fed to permission predicates
    trace: TraceHandle<Row, Lsn, isize>,
    cursor_lsn: Lsn,                    // last LSN delivered
    sender: mpsc::Sender<Diff>,         // bounded
    permission_handles: Vec<TraceHandle>, // one per permission rule
}
```

The router runs a lightweight task per subscription that:

1. On creation: reads from the trace as of `cursor_lsn = L₀`, emits an
   `Initial { rows }` message, then transitions to streaming.
2. In streaming mode: subscribes to the trace's frontier, batches diffs
   per LSN, applies permission filtering (§11), and forwards to the
   client over gRPC.
3. On client ack: advances `cursor_lsn`, contributes to the global
   compaction frontier (§9.4).
4. On client disconnect: the bounded sender errors; the router drops
   refcounts on shared arrangements.

Note the asymmetry with Noria's read path: Noria does point-in-time
reads with eviction errors (`noria/src/view.rs:117-142`); we hold an
open trace cursor per subscription.

## 11. Component: Permissions (`palimpsest-permissions`)

### 11.1 Model

Operators configure zero or more **permission rules**. A rule is:

```rust
struct PermissionRule {
    name: String,
    table: TableId,                       // rule applies when this table appears
    predicate: PredicateTemplate,         // SQL boolean expr; may reference $user
    mode: Mode,                           // RowVisibility | Subscribe | Both
}
```

Default config has zero rules → any client can subscribe to anything,
matching the user's stated default.

The predicate is parsed by the same SQL frontend as user queries and
may reference:

- Columns of `table`.
- `$user.id`, `$user.org_id`, etc. — fields of the connection's
  `UserContext`.
- Other tables, via subqueries (matching LiveGraph's "permissions can
  be data-dependent on subviews"). These compile into joined inputs
  in the operator graph, so when the underlying data changes, row
  visibility changes in real time.

### 11.2 Compilation

When a user submits query Q, the compiler:

1. Walks Q's MIR and finds every `BaseTable` reference.
2. For each base table, looks up matching rules.
3. Inserts a `Filter` (or, for cross-table rules, a `Join`) just above
   the `BaseTable` node, parameterized on `UserContext`.
4. Rewrites `CteRef` consumers to point at the permission-filtered
   subgraph.

Two users with identical queries but different `UserContext` therefore
share the parts of the graph above the permission filter and split at
the filter — the canonicalization key includes the rule predicates and
the user-context fields the rule reads.

### 11.3 Default-open

If no rules are configured for a table, the inserted filter is a no-op
and elided — there is zero overhead in the unconfigured case.

## 12. Component: Client (`palimpsest-client`, WASM)

Rust crate with two targets: native (`tokio` runtime) and
`wasm32-unknown-unknown` (uses `tonic-web-wasm-client` and a
`wasm-bindgen-futures` executor). Surface:

```rust
pub struct Client { /* ... */ }

impl Client {
    pub async fn connect(url: &str, auth: Auth) -> Result<Self>;

    pub fn subscribe(&self, query: &str, vars: Vars)
        -> impl Stream<Item = Diff> + 'static;

    pub fn update(&self, sub: &Subscription, vars: Vars) -> Result<()>;

    pub fn unsubscribe(&self, sub: Subscription);
}

pub enum Diff {
    Initial { rows: Vec<Row>, lsn: Lsn },
    Insert  { row: Row, lsn: Lsn },
    Update  { old: Row, new: Row, lsn: Lsn },
    Delete  { row: Row, lsn: Lsn },
    Resync  { reason: ResyncReason },
}
```

Key behaviors:

- **Reconnect.** On disconnect, the client reconnects and resubmits all
  active subscriptions with their last-acked LSN as a resume token. The
  server replays diffs from that LSN if still within compaction window;
  otherwise issues a fresh `Initial`.
- **Local cache.** The client maintains a primary-key-indexed
  `BTreeMap` per subscription so that `Insert` / `Update` / `Delete`
  diffs can be applied to produce a current view without the framework
  doing it. Optional — power users who want raw diffs can disable.
- **Mutations.** Out of scope for the engine. The client provides a
  thin `execute_sql(...)` helper that proxies to a separate
  application API; we do not insert ourselves into the write path.
- **Size budget.** Strip `serde_json`, use `bincode` over the wire;
  use `wee_alloc` and `--profile release-wasm` to hit ~500 KB gz.

## 13. Wire protocol

gRPC service (defined in `palimpsest-proto`):

```proto
service SyncEngine {
  rpc Subscribe(stream ClientMessage) returns (stream ServerMessage);
}

message ClientMessage {
  oneof kind {
    Subscribe   subscribe   = 1;  // sql, vars, optional resume_lsn
    Update      update      = 2;  // sub_id, new vars
    Unsubscribe unsubscribe = 3;
    Ack         ack         = 4;  // sub_id, lsn
  }
}

message ServerMessage {
  oneof kind {
    Accepted accepted = 1;        // sub_id assigned, schema, snapshot_lsn
    Diff     diff     = 2;        // sub_id, lsn, op, rows
    Resync   resync   = 3;        // sub_id, reason (lsn out of compaction window, schema change, ...)
    Error    error    = 4;
  }
}
```

A single bidirectional stream multiplexes all subscriptions for a
client. `tonic-web` is the transport on the server; `tonic-web-wasm-client`
on the client.

Diff payloads are bincode-encoded `Row` arrays (`Vec<Datum>`) referenced
by the schema attached to `Accepted`. We avoid sending column names per
row.

## 14. Consistency and failure modes

### 14.1 Timestamps and ordering

LSN is the global logical clock. Two events with LSN A < B are
delivered in that order to every subscription that observes both.
Within a single transaction, all events share the COMMIT LSN — the
client sees an atomic batch.

### 14.2 Delivery semantics

Per-subscription **at-least-once + in-order**. The client dedupes by
LSN: any diff whose LSN ≤ `last_applied_lsn` is dropped. This matches
the LiveGraph semantics ("guaranteed to be received in order and
exactly once" from the client's perspective).

### 14.3 Postgres failover

If the upstream Postgres fails over and the replication slot is
preserved (logical slot forwarding), `palimpsest-wal` reconnects via
the existing retry loop (`pg-walstream/src/stream.rs:742+`).

If the slot is lost, we recreate it, re-export the snapshot, and force
all subscriptions to `Resync`. We surface this loudly to clients so
their UIs can show a banner.

### 14.4 Sync engine restart

State is reconstructed on startup:

1. Read persisted `restart_lsn` from our metadata table.
2. Reopen the replication slot at that LSN.
3. Active queries are *not* restored — clients reconnect and
   resubscribe, which rebuilds operator state lazily through
   snapshots + upqueries.

We deliberately keep the engine itself stateless (in the
durable-storage sense) for v1. Restart cost is "every client resyncs,"
which is acceptable for our latency targets and saves us from
implementing durable arrangement persistence.

### 14.5 Client disconnect

Bounded buffer fills → channel closes → router emits `Resync` on
reconnect. Refcounts on shared arrangements drop; keys with no
remaining subscribers are evicted within one compaction tick.

## 15. Testing and validation

Correctness for an IVM engine is harder than for a query-at-rest
database: a bug can manifest as "the diff after the 47th transaction
disagrees with what Postgres would have shown," which no unit test
will catch by itself. The strategy below stacks several layers, each
catching a different class of bug.

### 15.1 Per-crate unit tests

Standard `#[cfg(test)]` coverage. Targets per crate:

- `palimpsest-wal`: pgoutput message parsing for every event type;
  TOAST "unchanged" expansion; replica-identity edge cases; LSN
  feedback math.
- `palimpsest-sql`: parser → MIR for a corpus of fixture queries;
  rejection of unsupported features (recursive CTEs, theta joins,
  unbounded `ORDER BY`); canonicalization-key equality on
  alpha-equivalent queries.
- `palimpsest-dataflow`: each operator in isolation against
  hand-built input/output sequences.
- `palimpsest-permissions`: rule compilation; predicate rewriting
  produces the expected MIR.

Aim for line coverage ≥ 80% in `palimpsest-sql`, `palimpsest-wal`,
and `palimpsest-permissions` (the deterministic, pure crates), and
≥ 60% in `palimpsest-dataflow` (the rest is exercised by the
integration tier).

### 15.2 Snapshot tests (`insta`)

Snapshot tests pin the *shape* of intermediate representations so that
unintended changes light up in code review. Tracked artifacts:

- **Parser AST** for the fixture-query corpus.
- **MIR after each pass** (raw, canonicalized, permission-rewritten).
  This is the single highest-value snapshot target — most planner
  bugs surface as a different MIR.
- **Dataflow build plan** as a printable DAG (operator names + input
  edges + arrangement keys). Catches regressions in the lowering
  step without needing to run the engine.
- **Canonicalization key (hash inputs)**: snapshot the pre-hash
  serialized form per query so we can see *why* two queries did or
  did not collapse to the same subgraph.
- **Wire protocol messages**: a recorded session of `ServerMessage`
  bytes for the demo queries, decoded with field names. Locks the
  on-the-wire shape that clients depend on.
- **Permission rewrites**: AST-before / AST-after for each rule.

Snapshots live under `<crate>/snapshots/`. CI runs `cargo insta test
--unreferenced reject` to forbid orphan snapshots. Reviewer protocol
when a snapshot diff lands in a PR: every changed snapshot must be
explicitly approved with a one-line justification.

### 15.3 Property tests (`proptest`)

Property tests are the main defense for the IVM correctness claim.
Generators we maintain:

- `arb_schema()` — random table set with realistic column types and
  primary keys.
- `arb_data(schema)` — random rows, with shrink toward minimal
  failing inputs.
- `arb_query(schema)` — random `SELECT` over the schema using the
  supported subset (filters, joins, aggregates, CTEs, `LIMIT`).
- `arb_wal_trace(schema)` — random sequence of `INSERT` / `UPDATE` /
  `DELETE` / multi-statement transactions, including the rare-but-
  legal cases (update to PK, update touching only TOAST columns,
  empty transaction, large transaction split into segments).

Properties asserted:

1. **Oracle equivalence.** For an arbitrary
   `(schema, query, wal_trace)`, feed the trace to Palimpsest via
   the synthetic WAL harness (§15.4) and feed the same trace to an
   in-process reference executor; assert that the materialized
   result of the live subscription equals the reference's result of
   `query`. This is *the* property for the engine. Implementation
   does not touch a real Postgres process — the harness mocks both
   the WAL stream and the reference executor.
2. **Snapshot/replay equivalence.** Subscribe at LSN L₀, apply
   diffs through LSN Lₙ, and assert the result equals re-subscribing
   at Lₙ from a fresh snapshot. Catches diff-application bugs.
3. **Determinism.** Two runs over the same WAL trace produce
   byte-identical wire output (up to subscription IDs).
4. **Batch invariance.** A WAL trace replayed in any partition into
   transactions consistent with commit order produces the same final
   state. Catches operator state that secretly depends on batch
   shape.
5. **Subscription independence.** N parallel subscriptions to the
   same query receive the same diff sequence; one slow subscriber
   does not perturb others' diffs.
6. **Permission soundness.** For a random `(rules, user_ctx)`, no
   row appearing in client output should fail
   `eval(rule, row, user_ctx)`. Equivalent to the Postgres
   equivalence test against the rewritten query.
7. **Permission liveness.** When a row that previously failed a
   permission becomes permitted (because the data backing the
   permission changed), the client receives an `Insert`; the
   reverse produces a `Delete`.
8. **Memory bounds.** For any trace, total operator state size is
   bounded by `O(|active subscription keys|)`, not `O(|table|)`.
   Asserted by an instrumentation hook that counts arrangement
   entries.

Property tests run with the standard `proptest` budget locally
(default 256 cases) and an extended budget in CI (`PROPTEST_CASES=4096`
on a nightly job).

### 15.4 Postgres-free test harness

The default test path **does not run a Postgres process**. Spinning
up a real cluster — even `pg-tmp` — costs tens of milliseconds, taints
property-test budgets, makes CI flaky, and forces every developer to
have Postgres on their machine. We replace it with three in-process
components, all living in a `palimpsest-test-harness` crate:

#### 15.4.1 `WalGenerator` — synthetic pgoutput producer

A deterministic encoder that takes a high-level event sequence and
emits well-formed pgoutput bytes wrapped in `XLogData`/`Keepalive`
replication messages. It is the inverse of the decoder in
`palimpsest-wal/src/protocol.rs`; the two share the same wire-format
constants and (where reasonable) struct definitions.

```rust
pub struct WalGenerator {
    schema: Arc<Catalog>,
    next_lsn: Lsn,
    relation_emitted: HashSet<TableId>,
}

pub enum LogicalEvent {
    Begin { xid: u32 },
    Insert { table: TableId, new: Tuple },
    Update { table: TableId, old: Option<Tuple>, new: Tuple },
    Delete { table: TableId, old: Tuple },
    Commit,
    // Variants exercising the rare paths:
    StreamStart { xid: u32 },
    StreamStop,
    StreamCommit,
    Truncate { tables: Vec<TableId>, options: TruncateOpts },
    RelationChange { table: TableId, new_columns: Vec<ColumnDef> },
    Keepalive,
}

impl WalGenerator {
    pub fn encode(&mut self, events: &[LogicalEvent]) -> Vec<Bytes>;
    pub fn current_lsn(&self) -> Lsn;
    pub fn skip_lsn(&mut self, by: u64);   // simulate gaps
}
```

The encoder auto-emits a `Relation` message before the first row
event for any table not yet seen, matching real Postgres behavior.
LSNs increase monotonically; gaps are configurable.

A round-trip property — `decode(encode(events)) == events` — pins
the generator against the production decoder. Any divergence is a
bug in one or the other and must be resolved before the property
suite can be trusted.

#### 15.4.2 `MockPostgres` — fake server speaking just enough wire protocol

`palimpsest-wal` connects to Postgres over TCP and issues a small
set of commands. We don't need to implement Postgres; we need to
implement *exactly that subset*:

- **Startup**: respond with `AuthenticationOk`, a fixed
  `ParameterStatus` set (server_version, integer_datetimes,
  client_encoding, …), `BackendKeyData`, `ReadyForQuery`. No TLS;
  tests use plain TCP on localhost.
- **Catalog probes**: pattern-match the small set of `SELECT`
  strings the catalog loader issues against `pg_class`,
  `pg_attribute`, `pg_index`, `pg_namespace`, and return canned
  rows assembled from the test schema. The match table is
  centralized so a catalog-loader change updates exactly one
  place.
- **Replication commands**: `IDENTIFY_SYSTEM`,
  `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput`, and
  `START_REPLICATION SLOT ... LOGICAL <lsn> ...`. The first two
  return canned responses (system identifier, fake snapshot name,
  starting LSN). The third enters CopyBoth mode and begins
  forwarding bytes from a configured `WalGenerator`.
- **Standby status updates**: receive `r`-messages from the client
  and record them so tests can assert on flush/apply LSN
  acknowledgement without checking real Postgres slot state.

```rust
pub struct MockPostgres {
    listener: TcpListener,
    schema: Arc<Catalog>,
    wal_source: Box<dyn FnMut() -> Option<Bytes> + Send>,
    acks: Arc<Mutex<Vec<StandbyStatus>>>,
}

impl MockPostgres {
    pub async fn start(schema: Arc<Catalog>) -> (Self, ConnInfo);
    pub fn drive(&mut self, events: &[LogicalEvent]);   // push via WalGenerator
    pub fn fault(&mut self, fault: Fault);              // see §15.7
    pub fn acks(&self) -> Vec<StandbyStatus>;
}
```

`MockPostgres` lives in the same process as the test; no Docker, no
filesystem, no port collisions (port 0 + connect by returned addr).
Startup cost is microseconds.

Implementing it costs us a few hundred lines today and saves us
millions of CI-seconds over the project's lifetime. The protocol
subset is small and stable — the message-format docs at
postgresql.org/docs/current/protocol-message-formats.html are short
and pinned to a wire-protocol version that has not changed in over
a decade.

#### 15.4.3 `ReferenceExecutor` — the oracle

A pure-Rust naive executor for the SQL subset Palimpsest supports.
Lives in `palimpsest-test-harness::reference`:

```rust
pub struct ReferenceExecutor {
    tables: HashMap<TableId, BTreeMap<PrimaryKey, Row>>,
    schema: Arc<Catalog>,
}

impl ReferenceExecutor {
    pub fn apply(&mut self, events: &[LogicalEvent]);
    pub fn execute(&self, query: &MirNode) -> Vec<Row>;
}
```

Constraints we hold it to, in order of importance:

1. **Auditable.** Implements each MIR node in the most obvious way:
   filters by predicate eval, joins by nested loop over hash maps,
   aggregates by a single pass building a `HashMap<GroupKey, Acc>`,
   `TopK` by `BinaryHeap`. No optimizations. The total reference
   executor target size is **under 1500 lines**; if it grows beyond
   that we have made it too clever to trust.
2. **Same SQL surface as Palimpsest.** It re-uses the production
   `palimpsest-sql` parser and MIR. Only the lowering target
   changes — production lowers to differential operators, reference
   lowers to `fn(MirNode, &Tables) -> Vec<Row>`. This guarantees
   the reference and the production engine accept identical query
   text.
3. **Deterministic.** No floating-point summation order surprises;
   when a query's result is order-sensitive (e.g. `LIMIT` without
   `ORDER BY`), the test wraps the comparison in a set-equality
   helper rather than asserting list equality.

The reference executor is *not* an IVM engine. It re-runs from
scratch whenever a test asks for a result. That is fine because
`apply` is `O(events)` and the supported workloads in tests are
small.

#### 15.4.4 Composed harness

```rust
let schema = Schema::from_ddl(SCHEMA_FIXTURE);
let mut wal = WalGenerator::new(schema.clone());
let mut reference = ReferenceExecutor::new(schema.clone());

let (mut mock_pg, conn) = MockPostgres::start(schema.clone()).await;
let engine = Palimpsest::connect(&conn).await?;
let mut sub = engine.subscribe(QUERY).await?;

for batch in test_case.batches() {
    let bytes = wal.encode(&batch.events);
    mock_pg.push(bytes);
    reference.apply(&batch.events);
    sub.drain_until(wal.current_lsn()).await;

    assert_eq_set(sub.snapshot(), reference.execute(&query_mir));
}
```

This composed harness is the substrate for §15.3 (property tests),
§15.6 (integration tests), and §15.8 (soak). Total time for a
single property-test case: sub-millisecond on a developer laptop,
which is what makes 4096-case CI runs feasible.

#### 15.4.5 Why this is sound

The risk of mocking the upstream is that the mock diverges from
real Postgres and a class of bugs becomes invisible. We mitigate by:

- **Round-trip property** on `WalGenerator` (§15.4.1) — encoded
  bytes must decode back to the original events.
- **Catalog-loader contract test** — the canned `SELECT` responses
  in `MockPostgres` are validated against a captured response
  fixture taken from a real Postgres of each supported major
  version. The fixture lives in `palimpsest-test-harness/fixtures/`
  and is regenerated by a one-shot script; its checksum is in
  source control.
- **Conformance suite (§15.11)** — a small set of nightly runs
  against real Postgres serves as a drift detector. Bugs that
  exist in the real protocol but not our mock should fail there.

We accept that some Postgres-version-specific quirks (e.g. a v17
behavioral change) will only be caught in conformance, not in PR
gates. This is an explicit trade for fast, deterministic everyday
tests.

### 15.5 Fuzzing (`cargo-fuzz`)

Targets:

- pgoutput message decoder — random bytes in, must not panic and
  must round-trip when re-encoded.
- SQL parser — random ASCII in, must not panic; if parse succeeds,
  re-print → re-parse must match.
- Wire protocol decoder (server-side) — protect against malicious
  clients.

Run nightly in CI with a 30-minute budget per target; corpora
checked into `<crate>/fuzz/corpus/`. Crashes auto-file an issue.

### 15.6 Integration tests (full stack)

A `palimpsest-integration-tests` crate stands up `MockPostgres`
(§15.4.2) plus the Palimpsest server in-process and drives end-to-
end scenarios:

- Initial snapshot + steady-state diffs.
- Subscription update mid-stream (vars change).
- Reconnect with a still-valid resume LSN.
- Reconnect with an LSN past the compaction window → `Resync`.
- Permission changes to backing data flip a client's view.
- Schema change (`RelationChange` event) → all affected
  subscriptions receive `Resync`.
- WASM client smoke test (headless Chrome via
  `wasm-bindgen-test`) — same scenarios driven from the browser
  client to verify the WASM build path.

Each scenario is a deterministic script of `LogicalEvent` batches,
not a Postgres SQL session. This keeps integration tests under one
second total.

### 15.7 Chaos and fault-injection

`MockPostgres` exposes a `Fault` enum so the chaos suite can inject
upstream failures without `toxiproxy`, Docker, or root:

```rust
pub enum Fault {
    DropConnection,                       // RST mid-stream
    HangAfter { bytes: usize },           // half-open / black-hole
    SlowSend { rate_bytes_per_sec: u32 }, // saturate the bounded buffer
    SlotGone,                             // respond ERROR to next START_REPLICATION
    LsnRewind { to: Lsn },                // simulate failover with diverged history
    SchemaDrift { table: TableId },       // canned catalog returns mismatched columns
}
```

Asserted invariants under each fault:

- `DropConnection` mid-transaction → reconnect → client sees no
  missing or duplicated diffs (modulo client-side LSN dedupe).
- `SlotGone` → forced `Resync` to all clients with a recognizable
  error code.
- `SlowSend` for longer than the buffer-drain budget → bounded
  channel triggers `Resync`.
- `LsnRewind` → engine refuses to silently continue; surfaces a
  loud error and forces operator-mediated re-bootstrap.
- `SchemaDrift` → either a clean `Resync` or a typed error; never
  a panic, never a silently-wrong row.

Real-Postgres equivalents of these faults are exercised once a
night in §15.11 to confirm the mock's behavior matches reality.

### 15.8 Soak and load tests

Continuous run on a dedicated host:

- **Soak.** Random `(schema, query corpus, write workload)` for
  72 h, driven by `WalGenerator` against an in-process server.
  Assertions: no memory growth past expected bound, no oracle
  equivalence violations, no internal panics.
- **Load.** Replay a recorded production-shaped workload at
  10k writes/s with 1k subscribers (the Figma envelope). Reports
  p50/p99 update latency and per-subscriber memory.

Soak failures attach the minimal `proptest`-shrunk repro to the
issue.

### 15.9 Benchmarks

`cargo bench` (`criterion`) tracks:

- Parse + MIR-build latency per query class.
- Per-operator throughput (rows/sec) on synthetic input.
- WAL-decode throughput (events/sec).
- End-to-end latency (commit-to-client) for the demo workload.

Bench results are pushed to a long-lived dashboard; PRs causing
> 10% regression on any tracked metric are blocked pending
explanation.

### 15.10 What we deliberately do NOT test

- **Postgres itself.** We trust Postgres' WAL semantics. If
  Postgres reports COMMIT at LSN L, we trust it.
- **`differential-dataflow` correctness.** We trust the upstream
  crate. Bugs there manifest as oracle-test failures in our suite,
  which is the right place to surface them.
- **`tokio` / `tonic` runtime correctness.**

Inverting the rule: any test we write must isolate a property of
**Palimpsest's own code**, not the dependencies underneath it.

### 15.11 Real-Postgres conformance (drift detection)

The only tests that touch a real Postgres process live under
`palimpsest-conformance/`. They run **nightly, not per-PR**, and
exist to catch drift between our mock infrastructure and the real
upstream:

1. **Wire-byte conformance.** For a fixed catalog of operations
   (one INSERT, one UPDATE without RI-FULL, one UPDATE with
   RI-FULL, one DELETE, one TRUNCATE, one streamed large
   transaction, one TOAST-only update), execute against a real
   Postgres via `psql`, capture the pgoutput bytes via logical
   replication, and `assert_eq!` against `WalGenerator`'s output
   for the same logical events. A mismatch fails the build and
   files an issue.
2. **Catalog-response conformance.** Run the production catalog
   probe against a real Postgres of each supported major version
   and `assert_eq!` against the canned responses inside
   `MockPostgres`. Regenerates the fixture file when intentional.
3. **Oracle conformance.** Run each query in the property-test
   corpus against both the `ReferenceExecutor` and a real
   Postgres of the latest stable major; assert set-equality of
   results. Catches reference-executor bugs in the supported SQL
   subset.
4. **Real-replication smoke.** A handful of the §15.6 integration
   scenarios re-run against a real Postgres to catch anything the
   mock misses.

Conformance failures are P1 issues but never block PRs. Supported
Postgres major versions are listed in `palimpsest-conformance/
versions.toml`; adding a new version is a matter of adding a row
and regenerating fixtures.

## 16. Open questions

1. **CTE materialization policy.** Postgres 12+ defaults to inlining
   CTEs unless they're referenced multiple times. Should we mirror
   that, or always treat a CTE as a shared subgraph? Leaning toward
   "always shared" — it's the whole point of CTEs in our model.
2. **`ORDER BY` without `LIMIT`.** Reject (current plan), or accept
   with a configurable cap?
3. **Pagination.** LiveGraph called this out as hard. The honest
   answer is "v1 has no pagination; clients fetch top-K and
   scroll-load via separate subscriptions."
4. **Differential vs. handwritten operators.** Differential brings a
   lot of dependencies and a learning curve. Worth prototyping the
   join + aggregate operators by hand on raw timely to compare?
5. **Permission predicate language.** SQL `WHERE`-style expressions
   are familiar but limited. Do we need a richer language for
   cross-table rules, or do subqueries suffice?
6. **Scale-out.** v1 is single-process. The architecture supports
   sharding by query (Noria-style) or by client (LiveGraph "edge
   server" style). We do not commit either way until we have
   numbers.

## 17. Phasing

| Phase | Deliverable                                                                          |
|-------|--------------------------------------------------------------------------------------|
| 0     | `palimpsest-wal` fork + typed-tuple decoder + slot lifecycle tests.                   |
| 1     | `palimpsest-sql` MIR + a hand-rolled in-memory engine for filters/projects only.      |
| 2     | Swap engine to differential-dataflow; add joins and aggregates; partial state.       |
| 3     | gRPC server + bidi subscription protocol; Rust client (native).                      |
| 4     | WASM client target; gRPC-Web in browser; size budget enforcement.                    |
| 5     | Permission rules + canonical reuse; benchmark against the Figma envelope.            |
| 6     | TOAST upqueries; spill-to-disk arrangements; failover hardening.                     |

## 18. Implementation TODO

A granular checklist by crate. The phasing table above (§17) groups
these into milestones; this section is the working backlog. Items
are intentionally small enough to land in single PRs.

### 18.1 Workspace foundation (Phase 0)

- [x] Create cargo workspace with `[workspace.package]` shared
  metadata (version, edition=2021, MSRV pin).
- [x] Add workspace-level lints: `clippy::pedantic`, `clippy::nursery`
  with explicit allow-list documented in `clippy.toml`.
- [x] Add `rustfmt.toml` with project conventions; enforce in CI.
- [x] CI: `cargo build --all-features`, `cargo test --all-features`,
  `cargo clippy -- -D warnings`, `cargo fmt --check`,
  `cargo deny check` (licenses + advisories).
- [x] CI matrix: stable + nightly Rust on Linux + macOS.
- [x] Cache `target/` and `~/.cargo` between CI runs.
- [x] Create `palimpsest-proto` crate with `tonic-build` build script.
- [x] Pin upstream major versions: `tokio`, `tonic`, `tonic-web`,
  `sqlparser`, `differential-dataflow`, `timely`, `bytes`,
  `proptest`, `insta`.
- [x] Add `xtask` runner crate for project-specific scripts
  (regen-fixtures, size-check, etc.).
- [x] Initial `README.md` with workspace map.
- [x] License headers (MIT/Apache-2 dual).

### 18.2 `palimpsest-wal` — WAL ingest (Phase 1)

- [ ] Vendor `pg-walstream` source into `crates/palimpsest-wal/`.
- [ ] Strip features we will not use (e.g. libpq backend if we
  commit to rustls only).
- [ ] Define `Datum` enum with variants for all supported Postgres
  types: `Bool`, `I16/I32/I64`, `F32/F64`, `Numeric(BigDecimal)`,
  `Text(Bytes)`, `Bytea(Bytes)`, `Date`, `Time`, `Timestamp`,
  `TimestampTz`, `Interval`, `Uuid`, `Json(Bytes)`, `Jsonb(Bytes)`,
  `Array(Vec<Datum>)`, `Null`.
- [ ] Build OID → `DatumType` mapping table; cover at least the
  pg_type rows present in stock Postgres 16.
- [ ] Implement decoder: `ColumnValue::{Text,Binary}(Bytes)` →
  `Datum`, both for binary mode and text mode.
- [ ] Replace pg-walstream's `RowData` with `Tuple = SmallVec<[Datum; 8]>`.
- [ ] TOAST handling: detect `'u'` (unchanged) markers and surface
  them as `Datum::Unchanged` rather than dropping.
- [ ] Implement `WalSource` and `DecodedEvent` per design §6.
- [ ] Backpressure: bounded mpsc between decoder task and consumer;
  saturation triggers spill (next item).
- [ ] Spill-to-disk path: when the bounded channel saturates during
  a streamed transaction, write segments to a temp file keyed by
  xid; drain on `StreamCommit`.
- [ ] `restart_lsn` persistence: small SQLite table (or file) we
  control; updated on each `Ack`.
- [ ] Slot recreation on slot-gone error: re-export snapshot,
  reconcile with downstream by forcing `Resync`.
- [ ] Reconnect/retry: keep upstream's exponential backoff; surface
  reconnect events to consumer.
- [ ] Catalog probe queries against `pg_class`, `pg_attribute`,
  `pg_index`, `pg_namespace`; populate `Catalog`.
- [ ] Refresh catalog on `RelationChange` events.
- [ ] Unit tests: every pgoutput message variant, including v3
  two-phase commit and v4 streaming.
- [ ] Round-trip test with `WalGenerator` (after §18.3 lands).
- [ ] Documentation: pgoutput message reference table in module
  docstring.

### 18.3 `palimpsest-test-harness` (Phase 1, parallel)

- [x] Create crate with `dev-dependencies = false` so it can be a
  regular dependency of test-only consumers.
- [x] `WalGenerator` skeleton with `next_lsn`, `Catalog`,
  `relation_emitted` set.
- [ ] Encode each pgoutput message type symmetrically with the
  decoder; share enum definitions where possible.
- [x] `LogicalEvent` enum per §15.4.1.
- [x] `WalGenerator::encode(&[LogicalEvent]) -> Vec<Bytes>`.
- [x] Auto-emit `Relation` before first row event per table.
- [x] `skip_lsn` to simulate gaps.
- [ ] Round-trip property: `decode(encode(events)) == events`,
  with `proptest` generators for `LogicalEvent`.
- [ ] `MockPostgres`: TCP listener on port 0, returns connection
  string.
- [ ] Implement startup handshake (Auth, ParameterStatus, BKD,
  RFQ).
- [ ] Implement catalog-query pattern matching with canned
  responses; centralize match table in
  `mock_postgres::catalog::DISPATCH`.
- [ ] Implement replication commands: `IDENTIFY_SYSTEM`,
  `CREATE_REPLICATION_SLOT`, `START_REPLICATION`.
- [ ] Implement CopyBoth mode: stream `XLogData` payloads from
  `WalGenerator`; emit periodic `Keepalive`.
- [ ] Receive and record `StandbyStatus` from client; expose via
  `MockPostgres::acks()`.
- [ ] `Fault` enum per §15.7; one test per variant.
- [ ] Catalog-response fixtures: capture from real Postgres
  (16, 17) via an `xtask regen-pg-fixtures` script; check
  fixtures + their checksums into source.
- [ ] `ReferenceExecutor` skeleton with table HashMaps.
- [ ] Apply `LogicalEvent` to `ReferenceExecutor` state.
- [ ] Lower MIR → reference plan; one match arm per MIR variant.
- [ ] Filter, Project, Equi-join (nested loop), LeftJoin,
  Aggregate (HashMap), Distinct, Union, TopK (BinaryHeap),
  CteRef (substitution), Leaf.
- [ ] Predicate evaluation with three-valued logic.
- [ ] Set-equality assertion helper for order-insensitive
  comparisons.
- [ ] Fixture-line-count budget guard: CI fails if reference
  executor exceeds 1500 lines.
- [ ] Composed harness builder: schema → wal-gen → mock-pg →
  engine, plus reference-executor twin; match what §15.4.4
  shows.

### 18.4 `palimpsest-sql` — SQL frontend (Phase 1)

- [x] Wire `sqlparser-rs` with `PostgreSqlDialect`.
- [ ] AST normalization passes:
  - [ ] expand `*` projections,
  - [ ] resolve qualified names against catalog,
  - [ ] propagate aliases,
  - [ ] desugar `BETWEEN`, `IN (list)`, `NOT IN`, `IS NULL`.
- [ ] Reject unsupported features at parse time with typed errors:
  - [x] `WITH RECURSIVE`
  - [x] window functions
  - [x] `RIGHT/FULL JOIN`
  - [x] theta joins
  - [x] `ORDER BY` without `LIMIT`
  - [x] scalar subqueries with unbounded result
- [x] `MirNodeKind` enum + `MirGraph` newtype around `petgraph`.
- [ ] Lowering: AST `Statement::Query` → MIR.
  - [x] single-table `SELECT` with `WHERE`, projection,
    `DISTINCT`, `ORDER BY` + `LIMIT/OFFSET`
  - [x] equi-joins and left equi-joins
  - [x] basic aggregates with optional `GROUP BY`
  - [x] `UNION ALL`
  - [x] derived tables
  - [ ] `UNION`/`EXCEPT`/`INTERSECT`
- [x] CTE handling: lift each `WITH` arm into its own subgraph;
  `CteRef` placeholders in consumer position.
- [ ] Column-reference resolution against MIR's schema attribute.
- [ ] Type inference for expressions; reject type mismatches.
- [ ] Predicate canonicalization (commute commutative ops, sort
  conjuncts, normalize literals).
  - [x] commute equality operands
  - [x] sort `AND` conjuncts
  - [ ] normalize literals
- [x] Canonical hash function for an MIR subgraph (SipHash; output
  is the canonicalization key).
- [ ] Reuse pass: hash-equal subgraphs collapse to a single node.
- [ ] Query validation pass per §8.5 (table/column existence,
  equi-join only, etc.).
- [ ] Snapshot tests: AST, MIR raw, MIR canonicalized, MIR
  permission-rewritten, build plan.
- [x] Property test: alpha-renaming a CTE produces same canonical
  hash.
- [x] Property test: parsing then unparsing then reparsing yields
  identical AST (for the supported subset).
- [x] Documentation: supported SQL grammar reference table.

### 18.5 `palimpsest-dataflow` — execution engine (Phase 2)

- [ ] Embed `timely::worker::Worker` inside a tokio task; design
  the `step_loop` from `threadless.rs`.
- [ ] `Lsn` newtype implementing `timely::progress::Timestamp`
  with a `Summary` of `u64` advancement.
- [ ] Custom `Container` implementation for `Row` to avoid
  per-row Vec heap allocations.
- [ ] WAL → dataflow source operator: receives `DecodedEvent`,
  emits `(Row, Lsn, +1/-1)` triples.
- [ ] Operator: `Filter` via differential `.filter()`.
- [ ] Operator: `Project` via `.map()`.
- [ ] Operator: `Equi-join` via `.join_map()` over arrangements
  keyed by join columns.
- [ ] Operator: `LeftJoin` via differential's `outer_join` or a
  custom op if upstream lacks one.
- [ ] Operator: `Aggregate` via `.reduce()`; support `count`,
  `sum`, `min`, `max`, `avg` (= sum/count), `count_distinct`.
- [ ] Operator: `Distinct` via `.distinct()`.
- [ ] Operator: `Union` via `.concat()`.
- [ ] Operator: `TopK` via `.reduce()` + custom slice (no built-in
  in differential at the time of writing; verify).
- [ ] Operator: `CteRef` resolves to a shared arrangement handle.
- [ ] Build-plan executor: `dyn Fn(&mut Scope)` → registered
  `(ProbeHandle, TraceHandle)`.
- [ ] Partial materialization: arrangement key-set tracking;
  upqueries when consumer requests an unindexed key.
- [ ] Upquery resolver: walk MIR backward, issue
  `SELECT ... WHERE pk IN (...)` against Postgres for base table
  state.
- [ ] LSN watermark eviction:
  `Trace::set_logical_compaction(min_subscriber_lsn)`.
- [ ] Reference counting on shared subgraphs; teardown when last
  subscriber drops.
- [ ] TOAST upquery: when an operator needs an `Unchanged` value,
  consult its own arrangement first; fall back to point-select.
- [ ] Memory instrumentation: per-operator arrangement size;
  expose via `Metrics`.
- [ ] Unit tests per operator: hand-rolled input traces,
  hand-checked output diffs.
- [ ] Property test: oracle equivalence on operator-by-operator
  basis.
- [ ] Bench: per-operator throughput.

### 18.6 `palimpsest-permissions` (Phase 5)

- [ ] `PermissionRule` types + DSL surface (likely a TOML/YAML
  schema for declarative rules; SQL predicate body parsed by
  `palimpsest-sql`).
- [ ] `UserContext` typed map with declared field schemas.
- [ ] Rule compilation: predicate text → MIR fragment with
  free `$user.*` variables.
- [ ] Rewriter pass: walks the user query's MIR, finds each
  `BaseTable`, splices in `Filter`/`Join` per matching rules.
- [ ] No-op elision: rules whose predicate is `true` (or table
  has no rules) compile to identity.
- [ ] Canonical-key inputs include rule identity + relevant
  `UserContext` fields, so two users with different orgs share
  graph above the rule and split at the filter.
- [ ] Configuration loader: parse rules at server startup;
  validate against catalog; reject ambiguous rules.
- [ ] Hot-reload story: rule edits trigger `Resync` for affected
  subscriptions (v1.5; v1 is restart-only).
- [ ] Snapshot tests: AST-before / AST-after for each rule in a
  fixture set.
- [ ] Property test: permission soundness (#6 in §15.3).
- [ ] Property test: permission liveness (#7 in §15.3).

### 18.7 Subscription router (Phase 3, lives in `palimpsest-server`)

- [ ] `Subscription` struct per §10.
- [ ] Subscription registry keyed by client-supplied id.
- [ ] Initial-snapshot path: issue `SELECT` against Postgres at
  `snapshot_lsn`; rows seed the trace's keys.
- [ ] Convert snapshot rows + LSN watermark into seed events
  on the dataflow input.
- [ ] Trace-cursor consumer task: read from
  `TraceHandle::stream_diffs(...)`; batch by LSN.
- [ ] Per-subscription bounded channel (default depth 256).
- [ ] Backpressure policy: drop with `Resync` on saturation, or
  switch to coalesced mode for small result sets (config flag).
- [ ] Permission filter applied before send.
- [ ] Diff serialization: bincode `Row` arrays, schema reference
  in `Accepted`.
- [ ] LSN ack handling: advance `cursor_lsn`, contribute to
  global compaction frontier.
- [ ] Resume path: client provides `resume_lsn`; if within
  compaction window, replay diffs since; else re-issue
  `Initial`.
- [ ] Subscription teardown: drop refcounts on shared
  subgraphs; evict zero-ref keys within one tick.
- [ ] Metrics: subscriptions in flight, p50/p99 fan-out latency,
  channel-full events.

### 18.8 `palimpsest-server` — gRPC service (Phase 3)

- [ ] `tonic` server with `tonic-web` middleware.
- [ ] Service per `palimpsest-proto` definitions.
- [ ] Bidi stream multiplexing per §13.
- [ ] Auth middleware: pluggable trait
  `fn (Headers) -> Result<UserContext>`; default impl reads a
  signed JWT from `Authorization` header.
- [ ] Connection lifecycle: on disconnect, drop all
  subscriptions for that connection.
- [ ] Health check endpoint (`grpc.health.v1`).
- [ ] Metrics: Prometheus endpoint via `axum` sidecar on a
  separate port.
- [ ] Tracing: `tracing` + `tracing-subscriber`; structured logs
  with subscription/connection IDs.
- [ ] Embeddable API: `Palimpsest::builder()` with `.with_wal()`,
  `.with_permissions()`, `.with_auth()`, `.serve()`.
- [ ] Standalone CLI: `palimpsest-cli` reads a TOML config and
  starts the embedded server.
- [ ] Graceful shutdown: drain subscriptions with `Resync`,
  flush LSN ack, close upstream.

### 18.9 `palimpsest-proto` — wire protocol (Phase 3)

- [ ] `.proto` file per §13: `SyncEngine` service,
  `ClientMessage`, `ServerMessage`, `Diff`.
- [ ] `tonic-build` integration (build.rs).
- [ ] Manual `Row` codec: bincode payload referenced by
  `schema_id` from `Accepted`.
- [ ] Wire-format snapshot tests against captured byte fixtures.
- [ ] Versioning policy: additive-only changes; major bump
  forces re-subscribe.

### 18.10 `palimpsest-client` — native Rust client (Phase 3)

- [ ] `Client::connect(url, auth) -> Result<Self>`.
- [ ] `subscribe(query, vars) -> impl Stream<Item = Diff>`.
- [ ] `update(sub, vars)`.
- [ ] `unsubscribe(sub)`.
- [ ] Reconnect with exponential backoff; resubscribe with
  `last_acked_lsn` as resume token.
- [ ] Local primary-key cache (default on; opt-out).
- [ ] Diff application to local cache.
- [ ] LSN dedupe.
- [ ] Tests: drive against `palimpsest-server` in same process.

### 18.11 `palimpsest-client` — WASM target (Phase 4)

- [ ] Add `wasm32-unknown-unknown` build job.
- [ ] Replace tokio runtime with `wasm-bindgen-futures` executor.
- [ ] Use `tonic-web-wasm-client` for transport.
- [ ] Strip `serde_json`; use `bincode` exclusively over the
  wire.
- [ ] `wee_alloc` global allocator behind a feature flag.
- [ ] Dedicated release profile `release-wasm` with
  `opt-level = "z"`, `lto = true`, `codegen-units = 1`.
- [ ] Size budget guard: `xtask check-wasm-size` fails CI if
  gz > 500 KB.
- [ ] `wasm-bindgen-test` headless smoke test through the
  full WASM build path.
- [ ] JS-friendly wrapper crate (`palimpsest-client-js`) with
  `wasm-bindgen` exports for `subscribe`, `update`, `on_diff`.
- [ ] React hook example in `examples/react-hello`.

### 18.12 Testing layers (cross-cutting, Phase 5+)

- [ ] Establish coverage targets and `cargo-llvm-cov` job in CI.
- [ ] Insta snapshot CI gate: `cargo insta test --unreferenced
  reject`.
- [ ] Proptest CI job: default budget on PRs,
  `PROPTEST_CASES=4096` nightly.
- [ ] Implement each property in §15.3 (1–8) as a separate
  `#[test]` for clarity.
- [ ] `cargo-fuzz` targets: pgoutput decoder, SQL parser, wire
  decoder. 30-minute nightly budget per target. Corpora
  checked in.
- [ ] Fuzz crash → autofile GitHub issue with seed.
- [ ] Integration scenarios listed in §15.6 — each as a single
  `#[tokio::test]`.
- [ ] Chaos `Fault` cases listed in §15.7 — each as a single
  test.
- [ ] Soak harness: standalone binary that runs against a
  random workload generator for 72 h.
- [ ] Soak failure capture: dump shrunk repro + state into a
  `soak-failures/` directory.
- [ ] Load harness: replay recorded production-shaped trace at
  10k writes/s × 1k subscribers; measure latency and memory.
- [ ] `criterion` benches: parse, MIR build, per-operator
  throughput, WAL decode, end-to-end commit→client latency.
- [ ] Bench dashboard publication step (cargo-criterion + a
  static site or codspeed.io).
- [ ] PR regression gate: > 10% bench regression blocks merge.

### 18.13 Real-Postgres conformance (nightly only)

- [ ] `palimpsest-conformance` crate; opt-in via cargo feature.
- [ ] Postgres-version matrix: 16, 17.
- [ ] Wire-byte conformance: real-PG capture vs. `WalGenerator`
  output for a fixed catalog of operations.
- [ ] Catalog-response conformance: real-PG SELECTs vs. canned
  fixtures; regenerator script.
- [ ] Oracle conformance: real-PG vs. `ReferenceExecutor` for
  the full property-test query corpus.
- [ ] Real-replication smoke: a subset of §15.6 scenarios re-run
  end-to-end against real PG.
- [ ] Failures auto-file P1 issues; never block PRs.

### 18.14 Operations (Phase 6)

- [ ] `palimpsest-cli` binary: subcommands `serve`,
  `validate-config`, `dump-catalog`, `slot-info`.
- [ ] TOML config schema: upstream connection, replication slot,
  permission rules path, listen address, auth.
- [ ] `tracing` integration with JSON output for production.
- [ ] Prometheus metrics:
  - subscription count,
  - WAL lag (current LSN − applied LSN),
  - per-operator memory bytes,
  - per-subscription channel depth,
  - bytes sent per client,
  - resync events.
- [ ] OpenTelemetry trace export (optional feature).
- [ ] Health endpoints: `/healthz` (process up),
  `/readyz` (caught up to a freshness threshold).
- [ ] Dockerfile (multi-stage; final image ≤ 30 MB).
- [ ] Helm chart skeleton (optional, post-v1).
- [ ] Runbook: slot-stuck, slow-consumer, schema-drift recovery.

### 18.15 Security and hardening

- [ ] Auth integration test matrix: missing token, expired,
  wrong audience, malformed.
- [ ] Rate-limit subscription creation per connection.
- [ ] Rate-limit reconnect attempts per IP.
- [ ] Bound query size (parser input length, MIR node count).
- [ ] Bound number of subscriptions per connection.
- [ ] Bound permission-rule evaluation depth.
- [ ] TLS termination story (likely upstream proxy; document).
- [ ] `cargo deny` advisories check in CI; pin RustSec audit
  schedule.
- [ ] Threat model document (what happens if a client lies
  about user ID? If permissions misconfigure? If WAL stalls?).

### 18.16 Documentation

- [ ] API rustdoc with `#![warn(missing_docs)]` on public
  crates.
- [ ] User guide: how to author queries, supported SQL surface,
  CTE recipes.
- [ ] Operator guide: deploying, configuring, tuning.
- [ ] Permissions guide: rule DSL, examples, common pitfalls.
- [ ] Architecture overview (export of this design doc to the
  public site).
- [ ] WASM client guide: minimal HTML+JS quickstart.
- [ ] Migration guide from "polling REST" to Palimpsest.
- [ ] Troubleshooting matrix mapping symptom → metric → action.

### 18.17 Open-question resolutions (must-decide before v1)

- [ ] Pick a CTE materialization policy (§16 #1).
- [ ] Decide `ORDER BY` without `LIMIT` policy (§16 #2).
- [ ] Pagination model decision (§16 #3).
- [ ] Differential vs. handwritten operators decision (§16 #4) —
  dependent on early bench results.
- [ ] Permission predicate language scope (§16 #5).
- [ ] Scale-out path (§16 #6) — defer past v1, but document the
  shape we'd take.

### 18.18 Release readiness

- [ ] All §15.3 properties pass at `PROPTEST_CASES=4096`.
- [ ] Conformance suite green on Postgres 16 and 17.
- [ ] Soak: 72 h with no panics, no oracle violations, memory
  growth under bound.
- [ ] Load: 10k writes/s × 1k subscribers, p99 commit→client
  latency < 100 ms.
- [ ] WASM client gz ≤ 500 KB.
- [ ] Public-API rustdoc complete; user guide published.
- [ ] CHANGELOG.md curated; semver discipline established.
- [ ] crates.io publishing checklist (READMEs, license tags,
  repository links) verified.

## 19. Glossary

- **Arrangement** — differential-dataflow's indexed, time-versioned
  state for a `Collection`.
- **Capability** — timely token authorizing emission at a timestamp.
- **CTE** — Common Table Expression, `WITH name AS (subquery) ...`.
- **Frontier** — antichain of minimum timestamps still pending.
- **Hole** — a key for which an arrangement has no value because no
  one has asked for it yet (Noria term).
- **LSN** — Postgres log sequence number; our logical timestamp.
- **MIR** — Middle Intermediate Representation between SQL and the
  dataflow graph.
- **Upquery** — a backwards request to fill a hole by replaying state.
