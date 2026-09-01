# Local-First Replica

Keep a client-side Postgres-compatible database — a pgrust/pglite-style
Postgres-in-WASM build in the browser, an embedded engine or the bundled
in-memory store natively — in sync with the remote database. The replica
behaves as an **optimistic, local-first clone of the permissioned subset
of the remote database**:

- **Permissioned subset.** Every mirror is an ordinary Palimpsest
  subscription, so the server's permission rewriter filters rows before
  transmission. The local database only ever contains rows the
  authenticated user is allowed to see; grant revocations arrive as
  retractions (or a `PERMISSIONS_CHANGED` resync) like any other diff.
- **Same queries against both.** Mirrored tables are created locally
  from the wire schema with real Postgres types, so the same
  Postgres-dialect SQL registered with the server runs unchanged against
  the mirror — instant reads, no round trip, works offline.
- **Optimistic writes.** Mutations apply to the local database
  immediately, are forwarded through the application's own write path,
  and are reconciled when the authoritative change streams back through
  the WAL.
- **Efficient reconciliation.** One atomic local transaction per remote
  commit; rebase work is proportional to *pending optimistic writes*,
  never to table size; LSN acks make reconnects resume incrementally
  instead of re-snapshotting.

## Architecture

```text
        Postgres WAL ──► Palimpsest server ──► permission rewriter
                                                     │
                              Initial snapshot + per-commit diffs
                              (only rows this user may see)
                                                     │
                 ┌───────────────────────────────────▼──────────────┐
                 │  LocalReplica (palimpsest-client)                 │
                 │                                                   │
                 │  per-mirror pump ──► Reconciler ──► LocalDatabase │
                 │        │                 ▲              │         │
                 │      ack LSN       pending-mutation     │         │
                 │                       ledger        pgrust WASM / │
                 │                          ▲          MemoryDatabase│
                 └──────────────────────────┼──────────────┬─────────┘
                                            │              │
   app write path (HTTP API / RPC / SQL) ◄─ mutate()    query()
        │                                                  ▲
        └────────► Postgres ──► WAL ──► ... ──► settles ───┘
```

Palimpsest never inserts itself into the write path (a design
non-goal): optimistic mutations are forwarded to a `RemoteWriter` the
application supplies, and the engine *observes* the resulting WAL
change to confirm them.

## Mirrors

A replica mirrors any mix of:

| Spec | Subscription | Local table |
| --- | --- | --- |
| table name | `SELECT * FROM <table>` (wildcard-expanded and permission-rewritten server-side) | the table name |
| raw SQL + local name | the SQL, permission-rewritten | the chosen name |
| named prepared query | `{name, params}` — no SQL ships to the client | the query name |

Per mirror the pump:

1. **`Accepted`** — creates the local table from the wire schema
   (`CREATE TABLE IF NOT EXISTS`, primary key included).
2. **First `Initial` diff** — atomically replaces the table's contents
   with the snapshot (subsequent consecutive `Initial` chunks append).
3. **Each `TransactionUpdate` / diff** — applies as **one atomic local
   transaction**, so local readers never observe a torn remote commit.
4. **Acks the commit LSN** after the local apply succeeds. On
   reconnect, the client resubscribes with the last acked LSN and the
   server replays only what's missing; only a compacted/lost position
   forces a fresh snapshot.
5. **`Resync`** — re-issues the query and installs the replacement
   snapshot (this is also how permission changes retract rows).

## Optimistic mutations and reconciliation

`mutate(...)` accepts inserts, updates (by primary key), and deletes.
The flow:

1. The change is staged in the **ledger** and applied to the local
   database immediately.
2. The mutation is handed to the application's `RemoteWriter`. A
   rejected write **rolls back** the local change.
3. The authoritative change eventually streams back from the WAL and
   **settles** the mutation.

The ledger keeps, per key with pending mutations only:

- `base` — the authoritative (server) row image,
- the ordered pending mutations for that key.

The visible local row is always `replay(base, pending)`. When a server
change arrives for a key:

- if it **confirms** the oldest pending mutation (post-image agrees on
  every column the mutation wrote — so server-side defaults and
  triggers don't block confirmation), the mutation settles;
- if it disagrees while the remote write is still **in flight**, the
  pending mutation is *rebased* on the new base (a concurrent writer
  touched the row; ours is still coming);
- if it disagrees **after** the writer acknowledged, the **server
  wins**: the optimistic overlay is dropped and a
  `MutationConflicted` event fires.

Keys without pending mutations take a fast path: the server row is
written through untouched. Snapshot replacement (`Initial`, resync)
rebases every pending mutation over the fresh base, so optimistic state
survives reconnects and permission churn (unless the new rules retract
the row itself — then the server wins, as always).

Everything is observable through events (`applied`,
`mutationSettled` / `mutationConflicted` / `mutationFailed`,
`tableState`, `mirrorError`) and a per-table sync-state watch.

## Rust usage

```rust
use std::sync::Arc;
use palimpsest_client::{Auth, Client, WireDatum};
use palimpsest_client::local::{MemoryDatabase, Mutation};

let client = Client::connect("http://127.0.0.1:50051", Auth::bearer(jwt)).await?;
let replica = client
    .local_replica(Arc::new(MemoryDatabase::new()))   // or SqlLocalDatabase over any Postgres engine
    .mirror_table("posts")
    .mirror_named("workspace_members")
    .with_writer(Arc::new(MyApiWriter::new(api)))     // your write path
    .start()
    .await?;

// Same query the server serves, answered locally:
let rows = replica.query("SELECT * FROM posts", Vec::new()).await?;

// Optimistic write; settles when the WAL round-trip confirms it.
let token = replica
    .mutate(Mutation::update(
        "posts",
        [("id", WireDatum::I64(1))],
        [("title", WireDatum::Text(b"draft".to_vec()))],
    ))
    .await?;
```

To back the replica with a real embedded Postgres engine, implement
`local::SqlExecutor` (one `exec(sql, params)` method on a single
session) and wrap it in `local::SqlLocalDatabase` — DDL, upserts, and
transaction framing are generated by the adapter so every backend
applies changes identically.

## Browser usage (TypeScript)

```ts
import { PalimpsestClient, postgresWasmDriver } from "@1kbirds/palimpsest-client";

const pg = await PgRust.create();               // pgrust/pglite-style Postgres WASM
const client = await PalimpsestClient.connect({ url, token, wasm });

const replica = await client.localReplica({
  database: postgresWasmDriver(pg),             // or any { exec(sql, params) => Promise<{rows}> }
  mirrors: ["posts", { name: "workspace_members" }],
  writer: (req) => api.applyMutation(req),      // reject to roll back
});

// Same SQL, local latency, permissioned subset only:
const { rows } = await replica.query(
  "SELECT * FROM posts WHERE author_id = $1 ORDER BY created_at DESC",
  [me],
);

const token = await replica.mutate({
  table: "posts",
  key: { id: 1 },
  set: { title: "draft" },
});

replica.onEvent((e) => {
  if (e.kind === "applied") refreshQueries();
  if (e.kind === "mutationConflicted") toast("Edited elsewhere — server version kept");
});
```

React apps can use the `useLocalQuery` hook
(`@1kbirds/palimpsest-client/react`) to keep a local query live: it
re-runs automatically whenever the replica applies a batch.

## Consistency model

- **Read-your-writes**: local queries see optimistic mutations
  immediately.
- **Monotonic per mirror**: each mirror applies commits in LSN order,
  atomically; acks are only sent after the local apply succeeds.
- **Server-authoritative**: on any disagreement after the write is
  acknowledged, the server's row wins; the app is told via
  `mutationConflicted`.
- **Cross-mirror**: mirrors are independent subscriptions; two mirrors
  may apply the same upstream transaction at slightly different times
  (same contract as any two Palimpsest subscriptions).

## Limitations

- Mutations require a primary key on the mirror (updates/deletes are
  keyed; key columns can't be changed by an update — delete and
  re-insert instead).
- Array-typed columns are mirrored as `jsonb` (the wire schema does not
  carry element types).
- Inserts stage unlisted columns as `NULL` locally; server-side
  defaults appear when the authoritative row streams back.
- The local database is a cache: it must not be written to except
  through the replica, or reconciliation state and local contents will
  diverge.
