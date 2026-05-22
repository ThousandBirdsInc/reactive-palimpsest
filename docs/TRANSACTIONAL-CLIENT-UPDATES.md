# Transactional Client Updates

Palimpsest should push the result of each upstream Postgres transaction
to a subscription as one coherent client-visible state update. If one
database transaction inserts, updates, and deletes many rows, the client
should not observe a sequence of partial states. It should receive one
transactional update for that subscription, apply it atomically to its
local cache, and ack the commit LSN only after that apply succeeds.

This document describes the implementation changes needed to make that
contract explicit through WAL ingest, dataflow, routing, the wire
protocol, and client caches.

## Problem

The design already says that events in one transaction share a commit
LSN and that clients see an atomic batch. The current implementation is
close but not strict enough:

- `WalSourceState` buffers row events until `Commit` and stamps emitted
  `WalUpdate`s with the commit LSN.
- `PersistentHost::push_table_batch` can recompute a query once for a
  batch of input diffs at one LSN.
- `TraceCursor::next_batch` groups raw diffs by LSN.
- `SubscriptionRouter::pump_batch` turns one `LsnBatch` into one
  `DiffEvent::Update`.

Those pieces make small tests behave transactionally, but the boundary
is still inferred from a shared LSN rather than represented as a
first-class transaction. That leaves several gaps:

- A real trace cursor can expose multiple batches for one commit if the
  dataflow output is drained in chunks.
- The router cannot distinguish "this LSN is complete" from "more rows
  at this LSN may still arrive."
- The protobuf `Diff` carries one `DiffOp` plus encoded rows; mixed
  insert/update/delete batches are summarized as a dominant op, losing
  per-row operation shape.
- Client APIs that deliver a stream of row diffs encourage UI code to
  render intermediate states instead of applying a commit as one unit.

The fix is to carry a transaction envelope through the system and make
client delivery happen only when the dataflow output for that
transaction is complete.

## Goals

- Deliver at most one state update per subscription per upstream
  Postgres transaction.
- Never expose an intermediate state from inside a transaction.
- Preserve commit order for every subscription.
- Preserve per-row operation detail inside the transaction envelope.
- Keep ack semantics LSN-based: ack means "the client durably applied
  all subscription changes through this commit LSN."
- Bound memory for large transactions and slow clients.
- Keep an incremental migration path from the existing `Diff` message.

## Non-goals

- Cross-subscription atomic delivery. Two subscriptions observing the
  same database transaction may receive their subscription-specific
  envelopes at different wall-clock times, but each subscription must
  still see a coherent update.
- Changing write behavior. Clients still write through the application
  or database path Palimpsest observes.
- Exactly-once transport. Delivery remains at-least-once and in-order;
  the client dedupes by commit LSN and transaction sequence.

## Client Contract

For each subscription, the server emits:

1. `Initial`: a snapshot at `snapshot_lsn`.
2. `TransactionUpdate`: all visible changes caused by one committed
   upstream transaction, identified by `commit_lsn`.
3. `Resync`: an explicit reset when the stream cannot continue.

The client applies a `TransactionUpdate` inside one local cache
mutation:

```text
begin local batch
  apply every row change in the transaction envelope
  publish one observer notification / render invalidation
end local batch
ack(commit_lsn)
```

If a duplicate `TransactionUpdate` arrives with `commit_lsn <=
last_applied_lsn`, the client drops it. If a transaction is split into
multiple transport frames, the client applies it only after the final
frame arrives.

## Transaction Envelope

Introduce a router-level event that represents the dataflow output for
one committed database transaction:

```rust
pub struct TransactionUpdate {
    pub subscription_id: SubscriptionId,
    pub transaction_id: Option<u32>,
    pub begin_lsn: Option<Lsn>,
    pub commit_lsn: Lsn,
    pub end_lsn: Option<Lsn>,
    pub schema_id: SchemaId,
    pub changes: Vec<RowChange>,
}

pub struct RowChange {
    pub op: DiffOp,
    pub old: Option<Row>,
    pub new: Option<Row>,
}
```

The important semantic difference from the current `DiffEvent::Update`
is that this type is complete by construction. The router should create
it only after it knows no more output for `commit_lsn` can arrive for
that subscription.

For large transactions, the wire protocol can split the envelope into
chunks while retaining one logical transaction:

```proto
message TransactionUpdate {
  string subscription_id = 1;
  uint64 commit_lsn = 2;
  optional uint64 begin_lsn = 3;
  optional uint64 end_lsn = 4;
  optional uint32 transaction_id = 5;
  uint64 schema_id = 6;
  uint32 chunk_index = 7;
  uint32 chunk_count = 8;
  repeated RowChange changes = 9;
}

message RowChange {
  DiffOp op = 1;
  bytes old_row = 2;
  bytes new_row = 3;
}
```

`chunk_count = 1` is the normal path. The client buffers chunks for the
same `(subscription_id, commit_lsn)` and applies them only when all
chunks are present. If a chunk is missing past a timeout or a later LSN
arrives first, the client requests resync or reconnects with its last
acked LSN.

## Implementation Plan

### 1. WAL ingest: emit commit batches

Replace "event in, `Vec<WalUpdate>` out" with a first-class commit
batch:

```rust
pub struct WalTransaction {
    pub xid: Option<u32>,
    pub begin_lsn: Option<Lsn>,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub updates: Vec<WalUpdate>,
}
```

`WalSourceState` should keep buffering row events until a commit marker,
then return `Some(WalTransaction)`. Streaming transactions and spilled
transactions should still produce one `WalTransaction` at
`StreamCommit` / `Commit`, with their row events read back from memory
or spill files.

Schema, heartbeat, origin, truncate, and reconnect events should remain
out-of-band control events. Truncate can later become a transaction
batch if the query engine learns how to express table-wide retractions
efficiently.

### 2. Dataflow: advance only after the transaction is complete

When a `WalTransaction` reaches the dataflow host:

1. Insert every input diff from `updates` at `commit_lsn`.
2. Flush the input session.
3. Advance the input frontier past `commit_lsn`.
4. Step/probe until the output frontier has also advanced past
   `commit_lsn`.
5. Drain all output diffs at `commit_lsn`.
6. Emit one completed transaction output per affected canonical query.

The persistent replay host already has the right shape in
`push_table_batch`: it applies a batch and recomputes once. Rename and
generalize that path to `push_transaction` so the transaction metadata
is preserved and so multi-table plans can receive all touched table
diffs in one call:

```rust
pub fn push_transaction(
    &self,
    canonical: &str,
    transaction: &WalTransaction,
) -> QueryTransactionDelta;
```

The long-running timely host should use the same contract. The frontier
past `commit_lsn` is the completion signal; no router event should be
emitted before that frontier movement is observed.

### 3. Cursor protocol: complete transaction batches, not best-effort LSN groups

Change `TraceCursor` from "next raw diff" to "next completed
transaction batch" for the router-facing API:

```rust
pub trait TraceCursor {
    fn next_transaction(&mut self) -> Option<QueryTransactionDelta>;
}
```

`QueryTransactionDelta` should carry the transaction metadata plus the
raw dataflow diffs for one subscription or canonical query. A helper can
still exist for tests that builds one from a vector of `RawDiff`s, but
the production cursor should not expose partial batches.

This removes the current ambiguity in `next_batch`: `None` means "no
completed transaction is ready", not "there are no more rows for the
current LSN right now."

### 4. Router: build one envelope per transaction

Replace `pump_batch(sub, LsnBatch, primary_key)` with:

```rust
pub fn pump_transaction(
    &self,
    sub: SubscriptionId,
    delta: QueryTransactionDelta,
    primary_key: &[usize],
) -> Result<(), RouterError>;
```

The router still pairs `+1` and `-1` records into `Update` row changes,
but it does so across the entire transaction output, not one cursor
drain chunk. The result is a `TransactionUpdate` stored in the bounded
per-subscription channel.

Backpressure remains subscription-local:

- If the transaction envelope fits, enqueue it.
- If it is too large but below the configured transaction byte limit,
  chunk it at the transport layer while keeping one logical
  transaction.
- If it exceeds the limit or the channel is full, emit `Resync` and
  stop the stream rather than delivering a partial transaction.

The router should update `cursor_lsn` only when the client acks the
transaction's `commit_lsn`, not when the envelope is enqueued.

### 5. Wire protocol: preserve mixed row operations

The existing `Diff` message cannot faithfully carry mixed row changes
because it has one `op` for the whole payload. Add a protocol revision
that introduces `TransactionUpdate` and `RowChange`.

Compatibility options:

- Preferred: add `transaction_update = 5` to `ServerMessage` and teach
  new clients to prefer it.
- Temporary bridge: keep emitting `Diff` only when a transaction is
  homogeneous; emit `TransactionUpdate` for mixed transactions.
- Eventually: mark row-level `Diff` as a legacy convenience message and
  use `TransactionUpdate` for all live updates.

`Initial` can stay as a distinct snapshot diff or become
`SnapshotUpdate`; it is already atomic from the client's perspective.

### 6. Client cache: apply in a local transaction

The TypeScript and Rust clients should expose a transaction-shaped
callback, even if they also keep row-level convenience APIs:

```ts
client.subscribe(sql, vars, {
  onInitial(snapshot) {},
  onTransaction(update) {},
  onResync(reason) {},
});
```

The default cache should:

1. Buffer chunks for one transaction.
2. Drop duplicates whose `commitLsn <= lastAppliedLsn`.
3. Apply every `RowChange` to the primary-key index in memory.
4. Notify subscribers once.
5. Ack `commitLsn`.

Framework adapters should make the atomicity visible. For React, the
hook should call one state setter per transaction, not one setter per
row change.

## Ordering And Acks

Per subscription:

- `Initial(snapshot_lsn)` establishes the baseline.
- `TransactionUpdate(commit_lsn = L)` is valid only for `L >
  last_applied_lsn`.
- The server must emit transaction updates in strictly increasing
  `commit_lsn` order.
- The client must ack only after applying the full transaction.
- The compaction frontier can advance past `L` only after all relevant
  subscribers have acked `L` or have been resynced/dropped.

If Postgres emits multiple transactions with the same commit LSN, add a
monotonic `commit_seq` assigned by WAL ingest and include it in the
transaction identity. The ordering key becomes `(commit_lsn,
commit_seq)`, while the ack can remain the highest fully applied LSN
when all sequences for that LSN are complete.

## Large Transactions

Large transactions need special handling, but they should not weaken
the client contract.

- WAL ingest may spill row events before commit.
- Dataflow may process the transaction in internal chunks, but output
  remains withheld until the commit frontier completes.
- Router channels should be byte-limited as well as message-limited.
- Transport may chunk the serialized envelope.
- Client apply remains all-or-resync: partial chunks are never exposed
  to the app cache.

If a transaction exceeds configured limits, the correct behavior is:

1. Keep draining WAL so the replication slot stays healthy.
2. Mark affected subscriptions stale.
3. Emit `Resync`.
4. Rebuild from a fresh snapshot at or after the transaction's
   `end_lsn`.

This is the same philosophy as migration mode: correctness first,
bounded memory second, row-level continuity only when affordable.

## Testing Strategy

Add tests at each layer:

- WAL unit test: multiple row events between `Begin` and `Commit`
  produce one `WalTransaction`.
- Persistent host test: two input diffs in one transaction produce one
  aggregate transaction delta, not two visible updates.
- Router test: mixed insert/update/delete output at one commit becomes
  one `TransactionUpdate` with per-row operations preserved.
- Protocol test: encode/decode a mixed transaction update without
  collapsing it to a dominant op.
- Client test: applying a transaction with several row changes triggers
  one cache notification and one ack.
- Property test: for generated transaction traces, applying
  transaction envelopes produces the same final state as replaying the
  source transactions against the reference executor, and no observer
  sees an intermediate state.

The key property is prefix visibility: after the client applies update
N, its state must equal the query result after exactly the first N
committed database transactions that affect that subscription.

## Rollout

1. Introduce transaction structs and tests while adapting existing
   `LsnBatch` code as a compatibility layer.
2. Add `TransactionUpdate` to the proto and generated clients.
3. Update router and gRPC forwarding to emit transaction updates for
   all new clients.
4. Update client caches and React hooks to apply transaction updates
   atomically.
5. Move production cursor implementations to frontier-completed
   transaction batches.
6. Deprecate legacy row-level `Diff` for live updates after all first
   party clients consume `TransactionUpdate`.

## Open Questions

- Should `commit_seq` be included from the start, or only added if real
  Postgres streams show same-LSN commits in practice?
- Should chunked transaction payloads use `chunk_count` or an explicit
  `is_final` flag for better streaming?
- Should snapshot delivery move to the same transaction envelope shape
  so clients have exactly one apply path?
- What byte limit should trigger `Resync` for a single transaction
  envelope?
- Should the server expose metrics for transaction sizes by table and
  by subscription to help tune those limits?
