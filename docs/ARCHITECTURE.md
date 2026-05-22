# Palimpsest Architecture

This is the public-site condensed export of the design document. The
authoritative reference is [`DESIGN.md`](../DESIGN.md) at the repo
root; this page exists so newcomers can build a mental model in
five minutes.

## What it is

A live SQL view server. You connect, send a `SELECT`, and receive a
snapshot followed by an unbounded stream of in-order, at-least-once
diffs as the underlying Postgres tables change.

The execution engine is differential dataflow — every operator is
incremental. A query that joins ten tables and aggregates millions of
rows produces O(commit-size) work per upstream commit, not
O(query-cost).

## High-level architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                              Postgres                              │
│                  (logical replication via pgoutput)                │
└──────────────────────────────────┬─────────────────────────────────┘
                                   │
                                   │ WAL events
                                   ▼
┌────────────────────────────────────────────────────────────────────┐
│                       palimpsest serve replica                     │
│                                                                    │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────────┐   │
│   │  WAL ingest  │ ──▶ │   Catalog    │ ──▶ │   Differential   │   │
│   │ (palimpsest- │     │ (relations,  │     │     dataflow     │   │
│   │     wal)     │     │   columns)   │     │  (operators &    │   │
│   └──────────────┘     └──────────────┘     │   arrangements)  │   │
│                                             └────────┬─────────┘   │
│                                                      │             │
│   ┌─────────────────────────────────────┐    ┌───────▼────────┐    │
│   │       Subscription router           │ ◀──│ Diff streams   │    │
│   │   (canonical-form keying, sharing,  │    └────────────────┘    │
│   │    permissions, ack tracking)       │                          │
│   └────────────────┬────────────────────┘                          │
│                    │                                               │
│   ┌────────────────▼─────────────────────┐                         │
│   │   gRPC SyncEngine (palimpsest-server) │                        │
│   │   - JWT auth                          │                        │
│   │   - per-IP / per-conn rate limits     │                        │
│   │   - tonic-web for browsers            │                        │
│   └────────────────┬──────────────────────┘                        │
└────────────────────┼───────────────────────────────────────────────┘
                     │
                     ▼
        Native or WASM clients (palimpsest-client)
```

## Crates at a glance

| Crate | Role |
| --- | --- |
| `palimpsest-wal` | Postgres logical replication consumer. Decodes pgoutput, tracks slot LSN, owns the catalog. |
| `palimpsest-sql` | Parses Postgres-flavored SELECTs, lowers to a mid-level relational IR (MIR). |
| `palimpsest-dataflow` | Differential dataflow primitives — operators, arrangements, traces. |
| `palimpsest-permissions` | Permission-rule DSL, compilation, MIR rewriter for `$user.*` predicates. |
| `palimpsest-server` | Subscription router, gRPC service, per-connection state, drain protocol. |
| `palimpsest-proto` | Generated protobuf bindings + a manual `Row` codec. |
| `palimpsest-client` | Native + WASM Rust client. Resume + ack semantics. |
| `palimpsest-client-js` | wasm-bindgen JS shim around `palimpsest-client`. |
| `palimpsest-cli` | `palimpsest serve` binary. |
| `palimpsest-test-harness` | Postgres-free in-memory harness (mock pg, reference executor). |
| `palimpsest-conformance` / `-properties` / `-soak` / `-integration-tests` | Test layers (real-pg conformance, IVM properties, soak, end-to-end). |

## Key concepts

### Canonical form and sharing

Two SQL queries that lower to the same MIR shape after normalization
share runtime state. The **canonical key** is a stable hash over the
normalized MIR + the per-user-context bindings; the router uses it to
route both clients to the same arrangement. This is what makes
"thousands of subscribers running the same query" cheap.

### LSN watermarks and acks

Every diff carries the upstream Postgres LSN at which the change
became visible. Clients ack the highest LSN they've durably consumed;
the server folds those acks into a global compaction frontier and
discards trace history below it. Clients that ack slowly hold back
compaction for everyone — but only for queries they share state with.

### Resume semantics

On reconnect, a client passes the last LSN it saw. Three outcomes:

- The slot is warm and within the compaction window → diffs replay
  from that LSN.
- The slot is gone or the LSN is below the compaction frontier → the
  server emits a `Resync` and starts a fresh snapshot.
- The connection is brand-new → standard snapshot + tail.

### Permission rewriting

Permission rules are compiled to MIR `Filter` nodes that splice into
each affected subscription's graph at the bottom of the tree. The
`UserContext` bindings are folded into the canonical key so two users
with identical bindings share state, and so a user with different
bindings gets their own arrangement.

### Backpressure and resync

Each subscription has a bounded diff channel. When a client lags far
enough that the channel saturates, the server emits a `Resync` rather
than drop diffs. From the client's perspective: snapshot, diffs,
diffs, diffs, *Resync*, snapshot, diffs, diffs … always in-order,
always recoverable.

## Scale-out

v1 is single-process. One `palimpsest serve` binary holds the WAL
consumer, the dataflow workers, the subscription router, and the gRPC
frontend. This is enough for the deployments in scope: tens of
thousands of subscribers fanning out from one Postgres logical slot.

Past v1, the planned shape is **sharding by query**, not by client:

- The subscription router is already partitioned by canonical query
  key. Each "query shard" owns the arrangements for a set of canonical
  keys; the gRPC frontend hashes new subscriptions to a shard.
- WAL ingest stays singleton — Postgres only allows one consumer per
  replication slot. Ingest fans events to query shards over an
  internal channel, keyed by base table.
- Compaction frontiers become per-shard; the global LSN ack channel
  still flows back to ingest so the slot can advance.
- Permission rules stay co-located with queries. They're already part
  of the canonical key, so they shard naturally.

The alternative — "sharding by client" (LiveGraph's edge-server
model) — was considered and rejected for a future Palimpsest because
it duplicates arrangement state across edges and loses the
cross-subscription sharing that makes the canonical-key model
worthwhile in the first place.

See [`DESIGN.md`](../DESIGN.md) §16.1 for the formal resolution.

## Wire protocol

gRPC bidirectional stream `SyncEngine.Subscribe`. The client sends
`ClientMessage` (`Subscribe`, `Unsubscribe`, `Ack`); the server sends
`ServerMessage` (`Accepted`, `Snapshot`, `Diff`, `Resync`, `Error`).
JWT auth in the `authorization` metadata header. `tonic-web` enabled
for browser clients. Schema versioning policy and stability guarantees
are documented in
[`palimpsest-proto/VERSIONING.md`](../crates/palimpsest-proto/VERSIONING.md).

## Where to go next

- [USER-GUIDE.md](USER-GUIDE.md) — how to author queries.
- [OPERATOR-GUIDE.md](OPERATOR-GUIDE.md) — deploy and tune.
- [DATABASE-MIGRATION-RESILIENCE.md](DATABASE-MIGRATION-RESILIENCE.md) —
  design for schema changes and high-volume data backfills.
- [TRANSACTIONAL-CLIENT-UPDATES.md](TRANSACTIONAL-CLIENT-UPDATES.md) —
  design for delivering each database transaction as one client state
  update.
- [PERMISSIONS.md](PERMISSIONS.md) — rule DSL.
- [WASM-CLIENT.md](WASM-CLIENT.md) — browser quickstart.
- [PAAS-DESIGN.md](PAAS-DESIGN.md) — managed platform design.
- [DESIGN.md](../DESIGN.md) — full design with phasing, open
  questions, and implementation TODOs.
