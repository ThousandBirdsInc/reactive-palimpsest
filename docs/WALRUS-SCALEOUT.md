# Walrus Scale-Out Design

This document turns the scale-out direction in
[`ARCHITECTURE.md`](ARCHITECTURE.md) into an execution plan against the
current source tree. The goal is horizontal server-side scaling without
asking Palimpsest users to run Kafka.

The chosen design is query-shard scaling with a Palimpsest-managed
distributed Walrus cluster. Public clients keep using the existing
`SyncEngine.Subscribe` bidirectional stream.

## Source Context

The current code is still shaped as one process:

- `palimpsest-server::embed::Palimpsest` wires one
  `SubscriptionRouter`, one `WalRuntime`, one authenticator, one gRPC
  listener, and one optional metrics listener.
- `palimpsest-server::grpc::SyncEngineService` accepts the public
  stream, parses SQL, builds a schema, calls `SubscriptionRouter`, then
  starts a cursor pump and a forwarder task per subscription.
- `palimpsest-server::wal_runtime::WalRuntime` is the main source
  seam: it provides snapshots, schemas, table schemas, and a
  per-query `TraceCursor`.
- `SubscriptionRouter` owns local process state: subscription ids,
  client-id mapping, channel backpressure, ack tracking, permission
  rewriting, and shared-subgraph reference counts.
- `palimpsest-cli` currently boots `EmptyWalRuntime` in `serve`, while
  `[upstream]` config is only used by `slot-info`.
- The Helm chart is a single Deployment with `replicaCount: 1` and no
  internal log service.

Those boundaries are useful. The scale-out implementation should not
rewrite the client protocol or the router first. It should split server
roles around the existing router and `WalRuntime` contracts.

## Walrus Facts That Matter

Walrus is a distributed log streaming engine with Raft-backed metadata,
segment leadership, automatic leader rotation, and a simple TCP client
protocol. Clients can connect to any node; Walrus routes commands to the
current leader. See the upstream README and architecture docs:

- <https://github.com/nubskr/walrus>
- <https://github.com/nubskr/walrus/blob/master/distributed-walrus/docs/architecture.md>

The protocol currently exposes:

```text
REGISTER <topic>
PUT <topic> <payload>
GET <topic>
STATE <topic>
METRICS
```

The important constraint is read ownership. Walrus documents `GET
<topic>` as reading the next entry from a shared cursor. That means two
Palimpsest readers must not consume the same topic unless they are
intentionally sharing one stream. For v1 scale-out, use one Walrus topic
per Palimpsest query shard.

Do not depend directly on Walrus APIs throughout `palimpsest-server`.
Hide Walrus behind a small Palimpsest-owned interface so we can absorb
protocol changes, add batch operations, or swap transport details later.

## Target Architecture

```text
                 public gRPC
Clients  ----------------------------+
                                      |
                                      v
                            +------------------+
                            | frontend replicas |
                            | SyncEngine proxy  |
                            +---------+--------+
                                      |
                         internal subscription RPC
                                      |
             +------------------------+------------------------+
             |                        |                        |
             v                        v                        v
      +-------------+          +-------------+          +-------------+
      | query shard |          | query shard |          | query shard |
      | shard_id 0  |          | shard_id 1  |          | shard_id N  |
      +------+------+          +------+------+          +------+------+
             ^                        ^                        ^
             |                        |                        |
             |                 Walrus shard topics              |
             +------------------------+------------------------+
                                      ^
                                      |
                           +----------+----------+
                           | ingest singleton    |
                           | Postgres replication|
                           +----------+----------+
                                      ^
                                      |
                                  Postgres
```

### Roles

`all`
: Local and test mode. Runs frontend, ingest, and one query shard in one
  process. This preserves the easiest development path.

`frontend`
: Public gRPC role. Authenticates clients, applies connection limits,
  parses subscribe requests enough to derive the canonical key, routes
  each subscription to the owning query shard, and proxies server
  messages back to the client. It should not own dataflow state.

`ingest`
: Singleton role for one Postgres source. It owns logical replication,
  decodes pgoutput, creates Palimpsest change envelopes, and appends
  them to Walrus shard topics. It advances Postgres slot feedback only
  after the matching Walrus append succeeds.

`query_shard`
: Dataflow and router owner for a deterministic shard id. It reads one
  Walrus topic, applies change envelopes into shard-local runtime state,
  owns the `SubscriptionRouter` for its canonical keys, and streams
  diffs to frontend proxies.

## Data Model

Add a versioned envelope type in a new `palimpsest-walrus` crate or a
temporary internal server module:

```text
ChangeBatchEnvelope {
  version: u16,
  cluster_id: String,
  source_id: String,
  shard_id: u32,
  schema_epoch: u64,
  commit_lsn: u64,
  sequence: u64,
  event_id: [u8; 32],
  updates: Vec<WalUpdateRecord>,
}

WalUpdateRecord {
  table_id: u32,
  diff: i32,
  encoded_row: bytes,
}
```

`event_id` must be deterministic over source id, commit LSN, sequence,
table id, row bytes, and diff. Query shards use it for idempotent
replay when ingest retries or Walrus redelivers after restart.

Topic names:

```text
palimpsest/{cluster_id}/{source_id}/shard/{shard_id}
```

Optional diagnostic topic, disabled by default:

```text
palimpsest/{cluster_id}/{source_id}/audit
```

Shard routing:

```text
shard_id = stable_hash(canonical_query_key) % shard_count
```

The first implementation can route each WAL update to every shard
topic. That is simple and correct, but write-amplifies by shard count.
A later optimization can maintain table-interest registration and append
only to shards that have active queries referencing a changed table.

## Subscription Flow

Public client flow stays the same from the client's point of view.
Internally:

1. Frontend receives `Subscribe`.
2. Frontend authenticates the stream using the existing authenticator.
3. Frontend parses and lowers SQL with the same limits as
   `SyncEngineService`.
4. Frontend derives the canonical query key and shard id.
5. Frontend opens an internal stream to the owning query shard.
6. Query shard calls its local `SubscriptionRouter::subscribe`.
7. Query shard returns `Accepted`, `Diff`, `Resync`, and `Error`
   messages over the internal stream.
8. Frontend forwards those messages onto the existing public stream.
9. Client acks and unsubscribes go back through the same frontend to the
   shard that owns the subscription.

The internal service should use the existing proto message shapes where
possible. Add an internal proto only for routing metadata that the
public client must not know about: connection id, authenticated user
context, canonical key, shard id, and frontend stream id.

## Snapshot Fencing

The current `WalRuntime::fetch_snapshot(query)` API is not enough for
distributed subscribe. A subscription must not miss changes that happen
between registering interest and finishing the initial snapshot.

Add an explicit barrier:

```text
register_interest(canonical_key, tables) -> barrier_lsn
fetch_snapshot(query, min_lsn = barrier_lsn) -> SnapshotBatch
open_cursor(query, from_lsn = snapshot_lsn)
```

For the first version, if every shard receives every WAL update, the
barrier can be the ingest's last appended LSN observed through Walrus.
When table-interest routing is added, ingest must acknowledge the
interest before returning the barrier.

The invariant is:

- every update with `commit_lsn <= snapshot_lsn` is represented in the
  snapshot, and
- every update with `commit_lsn > snapshot_lsn` is available from the
  shard's Walrus stream.

If the runtime cannot prove that invariant, the shard must return a
`Resync` or subscribe error instead of streaming from an unsafe gap.

## Progress, Acks, and Compaction

Do not conflate three kinds of progress:

- Client ack progress: existing per-subscription LSN acks used for
  resume and dataflow trace compaction.
- Walrus read progress: the shard's position in its shard topic. Walrus
  currently stores a shared cursor per topic.
- Postgres slot feedback: ingest should report progress to Postgres only
  after decoded changes are durable in Walrus.

For v1 scale-out, keep client ack handling shard-local. The query shard
already owns `AckTracker` and knows the relevant subscriptions. Ingest
does not need per-client acks to safely advance the Postgres slot if
Walrus is the durable handoff.

Walrus retention is a separate policy. Until retention is implemented
and exposed clearly enough for Palimpsest, configure managed Walrus to
retain enough history for operational recovery and treat query-shard
restart from old offsets as best effort. If a shard cannot replay the
needed range, it should force affected clients through `Resync`.

## Configuration

Extend TOML config with role and cluster sections:

```toml
[cluster]
id = "default"
role = "all" # all | frontend | ingest | query_shard
node_id = "palimpsest-0"

[walrus]
mode = "managed" # managed | external
endpoints = ["walrus-0.walrus:8080", "walrus-1.walrus:8080", "walrus-2.walrus:8080"]
topic_prefix = "palimpsest"

[sharding]
shard_count = 4
shard_id = 0 # required for query_shard
directory = "static"

[internal]
addr = "0.0.0.0:50052"
advertise_addr = "palimpsest-shard-0:50052"
```

Keep the existing `[grpc]`, `[metrics]`, `[auth]`, `[permissions]`, and
`[upstream]` sections. `[upstream]` becomes required for `ingest` and
`all` when using a real WAL runtime.

## Deployment

Helm should move from one Deployment to role-specific workloads:

- Walrus StatefulSet, default 3 replicas, persistent volumes, headless
  service, client port, Raft port, and readiness based on `METRICS`.
- Palimpsest ingest Deployment with one replica by default. Later this
  can become active/passive with leader election.
- Palimpsest query-shard StatefulSet or indexed Deployment, one pod per
  shard id, each with a stable internal address.
- Palimpsest frontend Deployment, horizontally scalable behind the
  existing public service.

Readiness rules:

- frontend is ready only if it can resolve every configured shard.
- ingest is ready only if it can reach Postgres and Walrus.
- query shard is ready only if it can reach Walrus, owns exactly one
  shard id, and no duplicate owner is registered for that shard id.

## Failure Model

Walrus append fails
: Ingest stops reading or stops acknowledging Postgres slot feedback.
  No decoded commit may be considered handed off until Walrus returns
  success.

Ingest crashes after append before feedback
: Postgres may replay the same commit. Query shards dedupe by
  deterministic `event_id`.

Query shard crashes
: On restart it resumes from its Walrus topic cursor if available. If it
  cannot prove continuity for active subscriptions, it emits or requires
  `Resync`.

Frontend crashes
: Public streams drop. Clients reconnect with normal resume LSNs. The
  new frontend routes them to the owning shard.

Duplicate query-shard owner
: Fail readiness for the later owner. Do not let two query-shard
  processes consume the same Walrus topic, because the current Walrus
  cursor is shared.

Walrus cluster unavailable
: Ingest and query shards fail readiness. Existing subscriptions should
  eventually receive `Resync` or disconnect depending on where the
  outage appears.

## Execution Checklist

### Documentation and Scaffolding

- [ ] Add this document as the source of truth for Walrus scale-out.
- [ ] Update `docs/ARCHITECTURE.md` to link to this document from the
  Scale-out section.
- [ ] Update `docs/OPERATOR-GUIDE.md` with role and managed-Walrus
  deployment notes after implementation lands.
- [ ] Update `deploy/helm/palimpsest/README.md` with managed Walrus and
  role-specific values after chart changes land.

### Walrus Integration Layer

- [ ] Add `palimpsest-walrus` crate, or a temporary
  `palimpsest-server::walrus` module if crate boundaries are premature.
- [ ] Define a `WalrusLog` trait with `register_topic`, `append_batch`,
  `read_next`, `state`, and `metrics`.
- [ ] Implement the distributed Walrus TCP protocol:
  length-prefixed command text, `REGISTER`, `PUT`, `GET`, `STATE`, and
  `METRICS`.
- [ ] Base64-encode binary Palimpsest envelopes for `PUT` payloads.
- [ ] Add retry and endpoint selection logic that can connect to any
  Walrus node.
- [ ] Add unit tests for framing, response parsing, empty reads, and
  reconnect behavior.

### Envelopes and Routing

- [ ] Add versioned `ChangeBatchEnvelope` and `WalUpdateRecord` types.
- [ ] Add stable binary encoding tests.
- [ ] Add deterministic `event_id` tests for duplicate replay.
- [ ] Add shard-topic naming helpers.
- [ ] Add stable canonical-key hashing tests.
- [ ] Add idempotent apply tests at the query-shard consumer boundary.

### Server Roles

- [ ] Add role config: `all`, `frontend`, `ingest`, and `query_shard`.
- [ ] Extend `palimpsest-cli` config parsing for `[cluster]`,
  `[walrus]`, `[sharding]`, and `[internal]`.
- [ ] Keep existing single-process behavior available as `all`.
- [ ] Add a `ShardDirectory` abstraction for canonical key to shard
  address lookup.
- [ ] Add duplicate shard-owner detection and readiness failure.

### Ingest Role

- [ ] Implement real ingest runtime wiring from `[upstream]` instead of
  `EmptyWalRuntime`.
- [ ] Convert decoded WAL events to `WalUpdate` batches using existing
  `WalSourceState`.
- [ ] Append each committed batch to the appropriate Walrus shard topics.
- [ ] Advance Postgres slot feedback only after Walrus append success.
- [ ] Add retry behavior that preserves ordering by commit LSN.
- [ ] Add metrics for append latency, append failures, last appended LSN,
  and Postgres feedback LSN.

### Query Shard Role

- [ ] Build a Walrus-backed runtime that reads exactly one shard topic.
- [ ] Feed envelopes into shard-local dataflow state.
- [ ] Keep `SubscriptionRouter` local to the shard.
- [ ] Replace direct public cursor pumping in scale-out mode with
  shard-owned cursor pumping.
- [ ] Add event-id dedupe state with bounded memory or persisted
  high-watermark strategy.
- [ ] Add metrics for topic lag, consumed LSN, dedupe drops, active
  subscriptions, and resyncs.

### Frontend Role

- [ ] Split public `SyncEngineService` into local execution and remote
  shard proxy paths.
- [ ] Add an internal frontend-to-shard streaming service.
- [ ] Route `Subscribe`, `Ack`, and `Unsubscribe` to the owning shard.
- [ ] Preserve existing public message shapes and error behavior.
- [ ] Add connection cleanup that notifies shards when a frontend stream
  closes.

### Snapshot Fencing

- [ ] Extend the runtime contract with barrier registration.
- [ ] Extend snapshot fetch with a minimum LSN or equivalent fencing
  token.
- [ ] Prove by tests that no commit is missed between subscribe and
  snapshot completion.
- [ ] Return `Resync` or a subscribe error when the runtime cannot prove
  the snapshot-stream boundary.

### Helm and Operations

- [ ] Add managed Walrus StatefulSet, service, PVC values, and probes.
- [ ] Split Palimpsest workloads into frontend, ingest, and query-shard
  templates.
- [ ] Add values for shard count, Walrus endpoints, role resources, and
  internal service ports.
- [ ] Keep a simple one-process dev values file.
- [ ] Add Helm template tests or golden renders for managed and external
  Walrus modes.

### Tests

- [ ] Unit-test the Walrus client protocol and envelope encoding.
- [ ] Integration-test ingest retry around append success/failure.
- [ ] Integration-test query-shard replay and dedupe.
- [ ] End-to-end test 1 ingest, 2 query shards, and 2 frontends with mock
  Postgres WAL.
- [ ] Failure-test frontend crash and reconnect with resume LSN.
- [ ] Failure-test duplicate shard owner readiness.
- [ ] Failure-test Walrus outage causing readiness failure and resync.

## Non-Goals for the First Implementation

- No public client protocol changes.
- No Kafka, Redis, NATS, or other external broker support.
- No client-shard scaling mode that duplicates dataflow state on every
  edge server.
- No dependence on Walrus consumer groups unless upstream support is
  added and verified.
- No dynamic shard rebalancing in v1. Use static shard count and stable
  hashing first.
