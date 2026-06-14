# Managed Postgres Branching API Design

**Status:** In progress. Phase 1 (data model, core types, `DropDatabase`
node-agent primitive, sql_store CRUD) and the Phase 2 HEAD copy-on-write branch
endpoints (create/list/get/delete) are implemented. Point-in-time branches
(Phase 3) and UI (Phase 4) are pending.
**Scope:** A Neon-style database branching API for the Palimpsest managed
Postgres PaaS, layered on the existing copy-on-write (CoW) clone and
point-in-time-recovery (PITR) restore primitives. Standard PostgreSQL only —
no new storage engine, no second database backend.

## 1. Motivation

Palimpsest positions itself as an open-source, Postgres-native alternative to
hosted realtime backends. A frequent ask from that audience is **fast database
branching** — cheap, named, throwaway copies of a database for development,
staging, CI, and "preview environments," in the style of
[Neon](https://neon.tech/). Neon's core is open source but is impractical to
self-host and is now a managed Databricks product, so adopting it would
undercut the "standard Postgres you operate yourself" positioning.

We do not need a different database to offer branching. The managed Postgres
control plane already ships the two primitives a branching API needs:

- **CoW database clones** — `CREATE DATABASE target TEMPLATE source STRATEGY
  FILE_COPY` on PostgreSQL 18+, exposed as
  `NodeAgentAction::CreateCopyOnWriteDatabaseClone`. Instant on reflink-capable
  filesystems (XFS/Btrfs/ZFS); shares blocks with the parent.
- **PITR restore** — `NodeAgentAction::PrepareRestore { .., recovery_target_lsn,
  redaction_policy, target_port, database }`, which materializes a backup and
  replays WAL to a target LSN/time into a fresh data directory.

What is missing is the **branch abstraction**: identity, lineage (a parent →
child tree), lifecycle (create/list/delete with tombstones), and a stable
connection endpoint per branch. This document specifies that layer.

## 2. Branch model

A **branch** is a named, addressable database state with a parent pointer.
Two creation modes are supported:

| Mode | Backed by | Branch point | Instant? | Isolation |
| --- | --- | --- | --- | --- |
| **HEAD branch** | `CreateCopyOnWriteDatabaseClone` | current state of parent | Yes (CoW) | New database in the *same* cluster instance |
| **Point-in-time branch** | `PrepareRestore` with `recovery_target_lsn` | arbitrary past LSN/time | No (full restore + WAL replay) | New cluster instance on its own port/data dir |

The two modes are deliberately different in cost and isolation, and the API
makes that explicit rather than hiding it. HEAD branches are the common,
cheap, instant case; point-in-time branches are the "time travel" case that
necessarily pays for a restore.

### 2.1 HEAD branches (CoW)

A HEAD branch is a CoW clone of the parent's `branch_database` into a new
database name **on the same Postgres instance** (same `data_dir`/`port`). It
reuses `CreateCopyOnWriteDatabaseClone` unchanged. Connection to the branch is
the same host/port with a different `dbname`.

Constraints inherited from `FILE_COPY`:

- **Parent must be quiescent.** `CREATE DATABASE ... TEMPLATE` requires no other
  sessions on the source database; the existing `terminate_source_connections`
  flag clears them first. Branching a continuously-written parent is therefore
  briefly disruptive to that parent database. Mitigation: branch from a
  read replica/standby, or accept the brief termination.
- **CoW only on reflink filesystems.** On other filesystems `FILE_COPY` is a
  full byte copy and is not instant.

### 2.2 Point-in-time branches (PITR)

A point-in-time branch restores the parent's most recent base backup into a new
data directory, replays WAL to `recovery_target_lsn` (or a target time mapped
to an LSN), and starts a new Postgres instance on an allocated port. This is
the same owned `prepare_restore` path used by customer restores and restore
drills, so it shares their redaction-policy enforcement.

This mode is **not CoW** and **not instant**; it is a full restore. It exists
for parity with Neon's "branch from a past point" capability and for
debugging/forensics ("give me the database as of 14:03 yesterday").

## 3. Data model

New migration `V50__managed_postgres_branches.sql`:

```sql
CREATE TABLE managed_postgres_branches (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    name TEXT NOT NULL,
    parent_branch_id TEXT REFERENCES managed_postgres_branches(id),
    -- 'head_cow' | 'point_in_time'
    mode TEXT NOT NULL CHECK (mode IN ('head_cow', 'point_in_time')),
    source_database TEXT NOT NULL,
    -- The database (head_cow) or new cluster id (point_in_time) backing this branch.
    branch_database TEXT,
    branch_cluster_id TEXT REFERENCES managed_postgres_clusters(id),
    created_from_lsn TEXT,            -- NULL for head_cow (means "HEAD at creation")
    redaction_policy_id TEXT REFERENCES managed_postgres_clone_redaction_policies(id),
    lifecycle_state TEXT NOT NULL
        CHECK (lifecycle_state IN ('creating', 'ready', 'failed', 'deleting', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX managed_postgres_branches_cluster_name_active_idx
    ON managed_postgres_branches(cluster_id, name)
    WHERE deleted_at IS NULL;

CREATE INDEX managed_postgres_branches_parent_idx
    ON managed_postgres_branches(parent_branch_id)
    WHERE deleted_at IS NULL;
```

Notes:

- Every cluster has an implicit **root branch** (the primary database). It can
  be represented by a seeded row with `parent_branch_id IS NULL` and
  `mode = 'head_cow'`, or treated as a virtual parent. A seeded row is simpler
  for lineage queries and the UI tree.
- `branch_database` is set for `head_cow`; `branch_cluster_id` is set for
  `point_in_time`. Exactly one is non-null per branch.
- The partial unique index enforces unique active branch names per cluster
  while allowing name reuse after deletion (matching the deletion-tombstone
  pattern already used for clusters).

## 4. API surface

All routes are scoped and authorized exactly like the existing managed-Postgres
resource APIs (`ResourceScope::from_cluster`, owner/admin for mutations).

```
POST   /v1/managed-postgres/clusters/{cluster_id}/branches
GET    /v1/managed-postgres/clusters/{cluster_id}/branches
GET    /v1/managed-postgres/clusters/{cluster_id}/branches/{branch_id}
DELETE /v1/managed-postgres/clusters/{cluster_id}/branches/{branch_id}
```

### Create request

```jsonc
{
  "name": "feature-x",
  "parent_branch_id": null,          // null => root branch of the cluster
  "mode": "head_cow",                // or "point_in_time"
  "recovery_target_lsn": null,       // required for point_in_time; ignored for head_cow
  "redaction_policy_id": null,       // optional; carried into restore/clone redaction
  "terminate_source_connections": false  // head_cow only
}
```

Validation:

- `name` validated as a safe branch identifier; derived `branch_database` runs
  through the existing `validate_database_identifier` /
  `sanitize_identifier_component` helpers.
- `head_cow` requires source cluster `Ready` and `postgres_version.major() >=
  MIN_SUPPORTED_POSTGRES_MAJOR` (same gates as the current clone endpoint).
- `point_in_time` requires a succeeded base backup and a `recovery_target_lsn`
  within the cluster's WAL retention window (reuse PITR continuity checks).
- A branch may only be created from a `ready` parent branch.

The handler creates the branch row (`lifecycle_state = 'creating'`), enqueues
the appropriate node-agent command, records an operation
(`OperationKind::CreateBranch`), and writes a
`managed_postgres_branch.create` audit event. Branch becomes `ready` when the
operation succeeds; `failed` otherwise.

### List / get

Returns the branch rows; the list response includes enough to render the
**tree** (parent pointers) and per-branch connection info. This extends the
existing Cluster detail **Clones** tab (or a new **Branches** tab).

### Delete

- Reject if the branch has any non-deleted children (`409`), or support
  `?cascade=true` to delete the subtree depth-first.
- `head_cow`: enqueue a new `NodeAgentAction::DropDatabase` (see §5).
- `point_in_time`: tear down the branch cluster via the existing delete-cluster
  path.
- On success, set `lifecycle_state = 'deleted'`, `deleted_at = now()`
  (tombstone), and write `managed_postgres_branch.delete`.

## 5. Node-agent additions

The only new primitive required is database drop for HEAD branches:

```rust
// palimpsest-paas-core: NodeAgentAction
DropDatabase {
    data_dir: String,
    port: u16,
    database: String,
    #[serde(default)]
    terminate_connections: bool,
},
```

The agent step runs `DROP DATABASE <database> [WITH (FORCE)]` (FORCE when
`terminate_connections`), guarded so it can never target the cluster's primary
database. Point-in-time branch deletion reuses existing cluster teardown
(`DeletePostgresData` + tombstone) and needs no new action.

New `OperationKind` variants: `CreateBranch`, `DeleteBranch`. Point-in-time
creation internally still drives the existing restore command; the branch
operation wraps it.

## 6. Connection / endpoint model

- **HEAD branch:** same host/port as the parent cluster, `dbname =
  branch_database`. No new endpoint resource; the connection string is derived.
- **Point-in-time branch:** its own cluster instance, so it gets a managed
  endpoint exactly like any restored cluster.

This means HEAD branches share one postmaster, connection pool, shared buffers,
and filesystem with the parent — excellent for dev/staging/CI fan-out, but not
the per-branch isolated compute Neon provides. That trade-off is acceptable for
v1 and documented as a known limitation.

## 7. Palimpsest sync integration (follow-up)

Because each branch is a logical-replication-capable Postgres database, a branch
can be given its own Palimpsest **SyncDeployment** (publication + replication
slot scoped to `branch_database`). Branch-aware live queries — "subscribe to the
same query against branch `feature-x`" — then fall out naturally and are the
differentiated story versus plain Postgres branching. This is specified as a
follow-up: branch creation gains an optional `provision_sync_deployment` flag
that chains a `StartSyncDeployment` operation after the branch is `ready`.

## 8. Divergences from Neon (known limitations)

1. **HEAD branches are HEAD-only.** Instant CoW branches are taken from the
   current state of the parent. Past-point branches exist but are full PITR
   restores, not instant.
2. **Parent disruption on CoW branch.** `FILE_COPY`/`TEMPLATE` needs a quiescent
   source database; Neon never disrupts the parent.
3. **Shared compute for HEAD branches.** All HEAD branches of a cluster share
   one Postgres instance. No per-branch compute isolation.
4. **No merge.** Branches are forks; there is no branch merge or diff.
5. **CoW depends on the filesystem.** Without reflink support, `FILE_COPY` is a
   full copy.

## 9. Phased implementation plan

1. **Migration + core types.** ✅ Done. `V50__managed_postgres_branches.sql`,
   `ManagedPostgresBranch` model, `DropDatabase` action, `CreateBranch` /
   `DeleteBranch` operation kinds, sql_store CRUD.
2. **HEAD branch path.** ✅ Done. Create/list/get/delete endpoints wired to
   `CreateCopyOnWriteDatabaseClone` and `DropDatabase`; lineage validation;
   `creating → ready/failed` and `deleting → deleted/failed` transitions driven
   from agent command results; audit events; node-agent `DropDatabase` step +
   protected-database guard.
3. **Point-in-time branch path.** (Pending.) Create with `recovery_target_lsn` via
   `PrepareRestore`; reuse redaction-policy enforcement and PITR continuity
   checks; branch cluster teardown on delete.
4. **UI.** Extend the Cluster detail Clones/Branches tab with the branch tree,
   create dialog (mode + branch point), and delete (with cascade confirmation).
5. **Sync integration (optional follow-up).** `provision_sync_deployment` flag.

Each phase ships with control-plane unit tests, a node-agent step test for
`DropDatabase` (including the primary-database guard), and an integration
scenario covering create → connect → delete and the children-block rule.

## 10. Open questions

- Root branch: seeded row vs. virtual parent. (Leaning seeded row for simpler
  tree queries and UI.)
- Should `point_in_time` branches accept a target **timestamp** directly, with
  the control plane resolving it to an LSN via PITR metadata, or require the
  caller to supply the LSN? (Timestamp is friendlier; needs the resolver.)
- Quota/limits: max active branches per cluster/environment, and whether HEAD
  branches count against storage quota given block sharing.
