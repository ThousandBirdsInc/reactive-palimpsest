# Database Migration Resilience

Palimpsest tails Postgres logical replication and turns upstream
changes into live query diffs. That makes normal OLTP writes cheap, but
database migrations are a different workload:

- DDL can change table shape underneath compiled subscriptions.
- Data migrations can rewrite millions of rows, producing a WAL volume
  that is correct but not useful to stream one row at a time to clients.

This document describes the design direction for making the system
resilient to both cases without compromising the core contract:
clients see a coherent snapshot followed by ordered diffs, or they get
an explicit `Resync`.

## Goals

- Preserve correctness across DDL and data backfills.
- Keep the Postgres replication slot draining so upstream WAL does not
  grow without bound.
- Bound Palimpsest memory during large transactions and long-running
  backfills.
- Avoid pushing low-value migration churn to clients when a fresh
  snapshot is cheaper and clearer.
- Make migration impact operator-visible before it becomes an incident.

## Non-goals

- Palimpsest should not become a schema migration runner. Rails,
  Flyway, Liquibase, application-specific jobs, and manual SQL remain
  the systems that perform migrations.
- Palimpsest cannot prevent Postgres from generating WAL for changes to
  tables in its publication. The design focuses on draining,
  coalescing, skipping stale work, and resyncing clients safely.
- v1 does not need perfect online compatibility for every possible DDL.
  The fallback is always affected-subscription `Resync`.

## Migration Classes

| Class | Examples | Default handling |
| --- | --- | --- |
| Compatible additive DDL | `ADD COLUMN nullable`, `ADD COLUMN ... DEFAULT NULL`, new index | Advance catalog epoch; old subscriptions keep their projected schema when possible. |
| Incompatible DDL | drop/rename column, type change, primary-key change, table rewrite | Mark affected canonical keys stale and emit `Resync{reason: SchemaChanged}`. |
| Small data migration | Correcting a small tenant, backfilling a few thousand rows | Stream as ordinary WAL diffs. |
| Large data migration | Rewriting a hot table, tenant-wide denormalization, bulk import | Enter migration mode for affected tables; drain WAL, suppress low-value client diffs, and resnapshot affected subscriptions. |
| Long transaction | `UPDATE big_table SET ...` in one transaction | Spill WAL segments to disk, apply backpressure limits, and prefer resync over unbounded buffers. |

## Schema Epochs

Every catalog state should have a stable `schema_epoch` derived from
the relation metadata that matters to subscriptions:

- table OID and qualified name,
- column names, logical types, and nullability,
- primary-key / replica-identity information,
- publication membership for tracked tables.

The WAL consumer already receives `Relation` messages and maintains a
catalog. The resilience layer should turn those changes into explicit
epoch transitions:

1. Decode relation change.
2. Compute the new table fingerprint.
3. Compare old and new fingerprints for compatibility.
4. Record a `SchemaTransition { table, old_epoch, new_epoch, class }`.
5. Notify the subscription router for any canonical key that references
   the changed table.

Subscriptions should be compiled against a specific schema epoch. A
diff stream is valid only while the subscription's compiled epoch
matches the epoch of the input rows feeding that plan.

### Compatibility Rules

The first implementation can use conservative rules:

- Adding a nullable column to the end of a relation is compatible for
  existing subscriptions that do not project `SELECT *`.
- Adding a column is incompatible for `SELECT *` subscriptions because
  the advertised output schema changes.
- Dropping, renaming, reordering, or changing the type of any referenced
  column is incompatible.
- Primary-key or replica-identity changes are incompatible for all
  subscriptions that reference the table because row identity and
  old-value reconstruction may change.
- Permission predicate dependencies count as referenced columns.

Conservative false positives are acceptable. A needless resync is
better than delivering rows decoded under the wrong shape.

## Data Migration Mode

A data migration can produce enough valid WAL to overwhelm three
places at once:

- WAL ingest buffers and large-transaction state,
- dataflow arrangements for affected canonical keys,
- per-subscription outbound channels and client render loops.

The system should support an explicit migration mode scoped by table,
optional tenant predicate, and optional LSN window:

```toml
[[migration_mode]]
name = "posts-status-backfill-2026-05"
tables = ["public.posts"]
mode = "resync_affected"
start_lsn = "auto"
end_lsn = "operator"
reason = "bulk data backfill"
```

During the window:

1. Ingest continues reading WAL and advancing the replication slot.
2. Events for unaffected tables continue normally.
3. Canonical keys that reference affected tables are marked stale.
4. Stale subscriptions receive a `Resync` and stop receiving per-row
   migration diffs.
5. Stale arrangements may skip applying row-level WAL for the affected
   tables, because they will be rebuilt from a post-migration snapshot.
6. When the migration closes, affected subscriptions re-subscribe and
   receive a fresh snapshot at or after the closing LSN.

This turns "stream 20 million row updates to every browser" into
"tell affected clients the live stream is being reset, keep Postgres
healthy, then send one coherent snapshot".

### Mode Selection

| Mode | Use when | Behavior |
| --- | --- | --- |
| `stream` | Small or user-visible migrations where every diff matters | Process WAL normally. |
| `coalesce` | Many repeated updates collapse to a small final delta | Apply WAL to dataflow, hold outbound diffs behind a bounded buffer, emit consolidated changes if still within limits. |
| `resync_affected` | Large backfills or table rewrites | Mark affected plans stale, drain/discard affected row diffs for those plans, and force fresh snapshots. |
| `pause_new_subscriptions` | Operator wants to avoid snapshotting during churn | Reject or queue new subscriptions for affected tables until the window closes. |

`resync_affected` should be the default recommendation for high-volume
data migrations.

## Automatic Backfill Detection

Explicit operator-declared windows are safest, but Palimpsest should
also detect migration-like traffic:

- WAL bytes per table exceed a configured threshold.
- Row events per table exceed a threshold for several consecutive
  seconds.
- One transaction spills more than a configured number of segments.
- A single table dominates `palimpsest_wal_lag_bytes` growth.
- Per-subscription channel saturation correlates with one input table.

When thresholds trip, the first response should be observability:
log the suspected table, increment metrics, and expose readiness
degradation. Automatic `resync_affected` can be enabled later behind a
configuration flag once operators trust the classification.

Proposed metrics:

```text
palimpsest_wal_events_by_table_total{table}
palimpsest_wal_bytes_by_table_total{table}
palimpsest_large_transaction_spill_bytes_total
palimpsest_migration_mode_active{table,mode}
palimpsest_resyncs_by_reason_total{reason="migration_backfill"}
palimpsest_stale_canonical_keys{table}
```

The current wire protocol can map migration-triggered resets to
`ResyncReason::Backpressure` or `ResyncReason::SchemaChanged`
depending on the trigger. A future protocol revision should add a
distinct `MigrationBackfill` reason so clients and operators can
distinguish planned maintenance from client slowness.

## Router Behavior

The subscription router already keys shared state by canonical query
shape. Migration resilience should build on that:

1. Maintain `table -> canonical_key` interest indexes.
2. On schema or migration transition, find affected canonical keys.
3. Mark those keys as `Healthy`, `Draining`, `Stale`, or `Rebuilding`.
4. Emit `Resync` to subscribers attached to stale keys.
5. Reject, queue, or immediately snapshot new subscribers depending on
   the key state and configured policy.

State model:

```text
Healthy -> Draining -> Stale -> Rebuilding -> Healthy
             |                         ^
             +------ compatible DDL ---+
```

- `Healthy`: normal diffs.
- `Draining`: known transition in progress; existing in-flight diffs
  below the barrier LSN may still flush.
- `Stale`: arrangement state is no longer trusted; clients must resync.
- `Rebuilding`: a fresh snapshot is being loaded after the migration
  barrier.

## WAL Ingest Behavior

The ingest path must never stop draining solely because clients cannot
consume migration diffs.

Required behavior:

- Keep the existing large-transaction spill path for protocol v2
  streaming transactions.
- Put hard limits on in-memory transaction buffers.
- Persist enough per-table / per-transaction counters to explain lag.
- Separate "decode and advance slot" from "deliver every row to every
  active plan".
- Allow the router to declare that specific table/key pairs are stale,
  so ingest can skip work that would be thrown away.

For correctness, skipping is only allowed when every dependent
arrangement has been marked stale and will rebuild from a snapshot at a
known post-migration LSN. If any healthy subscription still depends on
the table, ingest must continue feeding that subscription's plan.

## Operator Workflow

For planned high-volume migrations:

1. Estimate affected tables, row count, and whether clients need
   per-row visibility.
2. Put affected tables into `migration_mode = "resync_affected"`.
3. Wait for Palimpsest to log the start barrier LSN.
4. Run the migration in Postgres.
5. Watch WAL lag, spill bytes, stale canonical keys, and resync counts.
6. Close the migration window and wait for the end barrier LSN.
7. Confirm affected subscriptions resnapshot and lag returns to normal.

For unexpected migrations:

1. Detect via WAL/table thresholds or schema transition logs.
2. If lag is growing, manually enable migration mode for the hot table.
3. Let clients resync rather than increasing buffers without bound.
4. After recovery, classify whether the migration should have been
   declared in advance and update deployment runbooks.

## Client Contract

Clients should treat migration resilience as another resync cause:

- A `Resync` means the current stream is no longer authoritative.
- The client should drop cached rows for that subscription.
- The client should re-subscribe and use the schema attached to the new
  `Accepted` message.
- UI layers should batch rendering after resubscribe because a fresh
  snapshot can be large.

No client should infer that `Resync` is an error. It is the intended
escape hatch for keeping correctness under bounded resources.

## Testing Plan

Add coverage in layers:

- Unit tests for schema compatibility classification.
- Router tests for `table -> canonical_key` invalidation.
- WAL tests for large-transaction spill and drain-after-stale behavior.
- Integration tests where a backfill updates many rows and affected
  subscriptions receive one resync instead of unbounded diffs.
- Property tests asserting snapshot-after-resync equals the reference
  executor result after arbitrary DDL/backfill sequences.
- Soak tests with mixed normal writes, one long data migration, and
  slow clients to verify slot lag eventually returns to zero.

## Rollout Plan

1. Add schema fingerprints and conservative compatibility
   classification.
2. Add table-interest indexes in the router.
3. Add migration mode configuration and operator metrics.
4. Implement `resync_affected` for declared migration windows.
5. Add automatic backfill detection in observe-only mode.
6. Add protocol-level `MigrationBackfill` resync reason in the next
   compatible protocol revision.
7. Consider `coalesce` mode after the simpler resync path is proven.

## Open Questions

- Should migration mode live in static config, an admin API, or both?
- What is the right default threshold for automatic backfill detection
  across small and large deployments?
- Should queued subscriptions wait for the migration barrier or receive
  an immediate snapshot with a warning that another resync may follow?
- How much per-table metric cardinality is acceptable in hosted
  multi-tenant deployments?
