# Managed Postgres Design

**Status:** Draft
**Scope:** PostgreSQL 18+ lifecycle management for the Palimpsest PaaS.
**Decision:** We do not use Kubernetes to run customer databases. We build and
operate a Rust control plane, Rust data-plane agents, and a host-based
scheduler that we own.

## 1. Summary

Managed Postgres is a first-class Palimpsest product. Every hosted
environment gets a customer-dedicated PostgreSQL 18+ cluster configured for
logical replication and paired with a Palimpsest SyncDeployment.

The platform owns the complete lifecycle:

- Provision a database on a regional host.
- Initialize PostgreSQL 18+ with Palimpsest-compatible settings.
- Create least-privilege roles, publication, and replication slot.
- Manage TLS, secrets, config, process supervision, storage, backups, WAL
  archival, PITR, restore, resize, upgrades, and deletion.
- Report health and usage to the control plane.
- Keep local development close to production through the same config model.

We avoid Kubernetes because customer databases are long-lived, stateful
workloads with storage, backup, failure, and maintenance semantics that are
better represented by our own database-specific state machines than by a
generic container scheduler. The platform may run on cloud VMs or bare metal,
but the scheduling, reconciliation, and database operations are Palimpsest
owned.

## 2. Goals And Non-Goals

### Goals

1. Support managed PostgreSQL 18 and newer only.
2. Provision a production database and Palimpsest sync runtime without manual
   operator steps.
3. Keep each customer environment isolated at the process, data directory,
   credential, port, metric, backup, and audit boundary.
4. Make database lifecycle operations explicit state machines with durable
   intent and idempotent reconciliation.
5. Keep running databases independent from short control-plane outages.
6. Make every destructive operation gated, audited, and reversible until the
   documented deletion window expires.
7. Use ordinary PostgreSQL data formats and tools so customers can export and
   leave.
8. Avoid Kubernetes, Kubernetes operators, CRDs, StatefulSets, and Helm charts
   for managed database runtime.

### Non-Goals

- Supporting hosted PostgreSQL versions below 18.
- Exposing arbitrary root or superuser access to customers.
- Running multiple customer clusters in one Postgres postmaster.
- Building a general-purpose scheduler for arbitrary workloads.
- Supporting customer-supplied Postgres extensions on day one.
- Multi-region active-active Postgres in the first release.

## 3. Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                         Rust Control Plane                         │
│                                                                    │
│  Public API ──▶ Metadata DB ──▶ Reconciler ──▶ Placement Engine     │
│      │              │              │              │                │
│      │              │              ├──▶ Backup Orchestrator        │
│      │              │              ├──▶ Upgrade Orchestrator       │
│      │              │              ├──▶ Secret Issuer              │
│      │              │              └──▶ Audit / Usage Events       │
└───────────────────────────────┬────────────────────────────────────┘
                                │ desired state / leases / commands
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                         Regional Host Fleet                        │
│                                                                    │
│  Host A                         Host B                             │
│  ┌──────────────────────────┐   ┌──────────────────────────┐       │
│  │ palimpsest-node-agent    │   │ palimpsest-node-agent    │       │
│  │  ├── postgres supervisor │   │  ├── postgres supervisor │       │
│  │  ├── volume manager      │   │  ├── volume manager      │       │
│  │  ├── backup/WAL agent    │   │  ├── backup/WAL agent    │       │
│  │  ├── metrics collector   │   │  ├── metrics collector   │       │
│  │  └── sync supervisor     │   │  └── sync supervisor     │       │
│  │                          │   │                          │       │
│  │  postgres env acme/prod  │   │  postgres env globex/prod│       │
│  │  palimpsest sync worker  │   │  palimpsest sync worker  │       │
│  └──────────────────────────┘   └──────────────────────────┘       │
│                                                                    │
│  Regional Gateway ──▶ customer database endpoints + sync endpoints │
└────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                 Object Storage / WAL Archive / Backups             │
└────────────────────────────────────────────────────────────────────┘
```

### Control Plane Components

- **Public API:** Rust HTTP/gRPC service used by dashboard, CLI, CI, and
  support tooling.
- **Metadata DB:** Durable platform database for desired state, observed
  state, operation records, leases, audit events, and usage aggregates.
- **Reconciler:** Rust service that turns desired state into idempotent
  commands for node agents.
- **Placement engine:** Chooses a host for each database based on region,
  capacity, storage class, failure domain, maintenance windows, and tenant
  limits. The first implementation rejects placement when active host reports
  and existing managed Postgres assignments show insufficient cluster slots or
  storage GiB.
- **Backup orchestrator:** Schedules base backups, WAL retention checks,
  restore tests, clone creation, and backup garbage collection.
- **Upgrade orchestrator:** Coordinates minor updates and major-version
  upgrades for PostgreSQL 18+ clusters.
- **Secret issuer:** Creates short-lived agent credentials and database role
  secrets. Local development can use plaintext `local-dev` material, while
  production control planes store generated role passwords as envelope
  ciphertext with an owned key reference.

### Data Plane Components

- **`palimpsest-node-agent`:** Rust daemon installed on every database host.
  It owns local reconciliation for assigned workloads and continues operating
  during control-plane outages.
- **Postgres supervisor:** Starts, stops, reloads, restarts, and probes
  PostgreSQL processes. It writes generated config and validates postmaster
  state.
- **Volume manager:** Prepares data directories, mounts or attaches storage,
  checks filesystem health, manages disk quotas, and coordinates deletion.
- **Backup/WAL agent:** Runs base backups, streams or ships WAL, verifies
  archive continuity, and performs restore operations.
- **Metrics collector:** Emits Postgres, OS, storage, backup, WAL, and sync
  metrics with bounded cardinality.
- **Sync supervisor:** Runs the paired Palimpsest SyncDeployment on the same
  host or in the same failure domain.

## 4. Runtime Model

Each customer environment maps to one `ManagedPostgresCluster` and one
`SyncDeployment`.

```
ManagedPostgresCluster
  id
  organization_id
  project_id
  environment_id
  region
  postgres_version >= 18
  tier
  storage_gib
  host_assignment
  network_endpoint
  environment_endpoint
  backup_policy
  maintenance_policy
  lifecycle_state
```

One physical or virtual host may run multiple customer clusters, but each
cluster gets:

- A dedicated Unix user or equivalent privilege boundary.
- A dedicated data directory.
- A dedicated PostgreSQL port or local socket path.
- Dedicated TLS material.
- Dedicated database roles and rotatable passwords.
- Dedicated backup prefix and WAL archive prefix.
- Dedicated metrics labels and audit records.
- Dedicated Palimpsest publication and replication slot.

The first release should use one PostgreSQL postmaster per customer
environment. This costs more than multi-tenant databases inside one
postmaster, but it gives clean isolation, straightforward restore, predictable
resource accounting, and lower blast radius.

## 5. Why Not Kubernetes

The managed Postgres runtime should not depend on Kubernetes primitives:

- No CRDs for database intent.
- No StatefulSets for customer clusters.
- No Kubernetes operators for failover, backup, or upgrade.
- No Helm charts for the managed database runtime.
- No Kubernetes service discovery as the source of truth for database
  endpoints.

Reasons:

1. The core control loops are database-specific: backups, WAL continuity,
   PITR, replication slots, version upgrades, vacuum health, and deletion
   guarantees.
2. We need durable lifecycle state that outlives individual hosts and is
   expressed in product terms, not generic pod state.
3. A small owned scheduler can expose exactly the states customers and
   operators need.
4. The local developer workflow should map to the same concepts without
   requiring a Kubernetes cluster.
5. Owning the database manager reduces operational ambiguity: there is one
   reconciler, one agent protocol, one audit trail, and one state machine.

We can still use commodity infrastructure: cloud VMs, disks, load balancers,
object storage, DNS, KMS, and images. The orchestration logic remains ours.

## 6. State Machines

Every long-running operation is represented as an operation row with an
idempotency key, target resource, desired transition, current step, lease
owner, retry policy, and audit trail.

### Cluster Lifecycle

```text
Requested
  -> Placing
  -> AllocatingStorage
  -> InitializingPostgres
  -> ConfiguringRoles
  -> ConfiguringReplication
  -> Starting
  -> Verifying
  -> Ready
```

Failure states:

```text
Ready -> Degraded -> Repairing -> Ready
Ready -> Maintenance -> Ready
Ready -> RestoreRequested -> Restoring -> Ready
Ready -> DeletionRequested -> Draining -> Snapshotting -> Deleting -> Deleted
```

Rules:

- State transitions are monotonic inside one operation.
- Steps are safe to retry after agent or control-plane restart.
- Observed state never overwrites desired state without recording an operation.
- Destructive transitions require explicit confirmation and retention policy
  checks.

The current SQL-backed control plane records durable operations for
agent-backed managed Postgres create/reconcile steps, stop, resize, delete,
base backup, WAL archive, and restore preparation. Each queued node-agent
command links back to its `operations` row, and command completion advances
the operation to `succeeded`, `failed`, or `cancelled`.

### Backup Lifecycle

```text
Scheduled
  -> LeaseAcquired
  -> RunningBaseBackup
  -> UploadingManifest
  -> VerifyingRestoreMetadata
  -> Complete
```

WAL archival is continuous. The backup orchestrator verifies that every base
backup has an unbroken WAL range covering the configured PITR window.

### Restore Lifecycle

```text
Requested
  -> SelectingBackup
  -> AllocatingStorage
  -> DownloadingBaseBackup
  -> ReplayingWal
  -> StartingPostgres
  -> VerifyingConsistency
  -> Ready
```

Restores can target the original environment after destructive confirmation or
a new clone environment for staging and development.

## 7. Host Agent Design

`palimpsest-node-agent` is the only process on a host allowed to mutate
managed customer database state.

Responsibilities:

- Register host capacity and health with the control plane.
- Acquire leases for assigned operations.
- Materialize desired cluster specs into local files and processes.
- Start and supervise PostgreSQL 18+.
- Run `initdb`, `pg_ctl`, `pg_basebackup`, `pg_controldata`, `pg_waldump`,
  `psql`, and extension checks through typed Rust wrappers.
- Upload backups and WAL segments.
- Report observed state and metrics.
- Refuse unsafe commands that do not match local resource ownership.

The agent should expose a narrow mTLS-protected API to the control plane:

```text
RegisterHost(HostFacts) -> HostLease
ApplyClusterSpec(ClusterSpec, OperationId) -> Accepted
GetObservedState(ClusterId) -> ObservedState
StartOperation(OperationCommand) -> OperationStatus
StreamLogs(ResourceId, Cursor) -> LogBatch
RotateSecret(ResourceId, SecretVersion) -> RotationStatus
```

Agent commands must be declarative where possible. The control plane says what
cluster should exist; the agent decides the local command sequence needed to
make the host converge.

## 8. Storage Layout

Each cluster receives a storage root:

```text
/var/lib/palimpsest/postgres/<cluster_id>/
  data/
  config/
    postgresql.conf
    pg_hba.conf
    generated.env
  sockets/
  logs/
  tmp/
  backup-state/
```

Storage requirements:

- Data directory encryption at rest through the host volume layer.
- Per-cluster disk quota or volume size enforcement.
- `noexec` where practical for non-data paths.
- Strict ownership by the cluster Unix user and node agent group.
- Disk pressure alerts before PostgreSQL reaches unsafe free-space levels.
- Deletion tombstones to avoid accidental cluster-id reuse.

The first implementation can use one attached block volume per cluster or a
partitioned host volume with per-cluster quotas. Dedicated volumes are simpler
for restore, resize, and deletion; shared host volumes are cheaper. The design
should keep the volume manager interface abstract enough to support both.

## 9. PostgreSQL Configuration

Managed clusters run PostgreSQL 18+ with generated configuration. The config
compiler owns defaults and validates plan-specific limits.

Required settings:

```conf
wal_level = logical
max_replication_slots = <plan-derived>
max_wal_senders = <plan-derived>
archive_mode = on
archive_command = '<palimpsest WAL archive helper>'
hot_standby = on
ssl = on
listen_addresses = '<private interface>'
shared_preload_libraries = '<approved list>'
```

Plan-derived settings:

- `shared_buffers`
- `work_mem`
- `maintenance_work_mem`
- `max_connections`
- `effective_cache_size`
- `autovacuum_*`
- WAL retention and checkpoint settings

The generated `pg_hba.conf` should allow only:

- Application role access from customer-approved networks or app gateways.
- Migration role access from customer-approved networks or CI allowlists.
- Replication role access from the local SyncDeployment.
- Support role access through audited break-glass paths.

## 10. Roles And Permissions

Every cluster has separate roles:

| Role | Purpose |
| --- | --- |
| `app` | Customer application reads/writes. |
| `migrator` | DDL and migration tooling. |
| `palimpsest_repl` | Logical replication for the SyncDeployment. |
| `palimpsest_observer` | Metrics and health probes. |
| `support` | Time-bound audited operational access. |

The platform owns role creation and rotation. Customers receive app and
migration credentials, not unmanaged superuser credentials. Privileged
operations are exposed through platform APIs with audit records.

## 11. Palimpsest Sync Integration

For each managed database, the node agent configures:

- Publication name, default `palimpsest_pub`.
- Replication slot name, default derived from environment id.
- Replication role credentials.
- SyncDeployment connection URL over local socket or private network.
- Health check that confirms the slot advances.

The SyncDeployment should be colocated with the database for the first
release. Colocation reduces WAL latency, avoids public database exposure for
replication, and makes per-environment failure analysis simpler.

The customer app may connect from outside the host through a database endpoint
gateway. Palimpsest replication should stay private.

## 12. Backups, WAL Archive, And PITR

Backups are mandatory. A cluster cannot be marked `Ready` on a paid tier until
the backup policy is active.

Backup components:

- Base backups through `pg_basebackup` or an equivalent PostgreSQL-native
  mechanism.
- Continuous WAL archival through `archive_command` or a local WAL shipper.
- Object storage layout by organization, environment, cluster, and timeline.
- Backup manifests containing PostgreSQL version, system identifier, timeline,
  LSN range, checksums, config hash, and encryption metadata.
- Periodic restore tests into an isolated validation cluster.

The first Rust node-agent implementation writes a `manifest.json` next to each
`pg_basebackup` output with backup id, cluster id, host id, detected
PostgreSQL version, source data directory, creation timestamp, and artifact
metadata. The SQL control plane now also keeps a durable backup-artifact
catalog for each base backup, recording the provider, object URI, manifest
path/hash, size, lifecycle status, and error metadata. The first provider is
the owned local filesystem path used by local development and smoke tests; the
control plane automatically creates that local artifact record when a
`run_base_backup` node-agent command succeeds. Later object-storage runners
should write the same artifact records after
uploading to S3-compatible storage and should extend the manifest with system
identifier, timeline, LSN ranges, checksums, config hash, and encryption
metadata before uploads are considered production-grade.

Restore drills are modeled separately from customer restore requests. The SQL
control plane stores `managed_postgres_restore_drills` rows linked to a source
cluster, backup, restore request, and isolated target cluster. Operators can
request a drill directly through the scoped restore-drill API or run
`POST /v1/scheduler/restore-drills/run-once` to schedule drills for ready
clusters whose latest successful drill is absent or older than the requested
age. The drill uses the same owned Rust node-agent `prepare_restore` path as a
customer clone, so drill success proves the current backup artifact can be
materialized by the production restore mechanism. Long-running SQL control
plane processes can also run this scheduler internally with
`PALIMPSEST_PAAS_RESTORE_DRILL_SCHEDULER_INTERVAL_SECONDS`; the freshness
window defaults to seven days and can be overridden with
`PALIMPSEST_PAAS_RESTORE_DRILL_MAX_AGE_HOURS`.

Development and staging clone restores are guarded by clone redaction policy.
The control plane stores environment-scoped redaction rules, rejects
cross-environment restores without an active source policy, records the policy
id on the restore request, and passes the policy to the node agent. The local
agent writes a redaction manifest and SQL artifact alongside the restored data
directory, starts the clone on its assigned port, applies `null`,
`static_value`, and `hash_sha256` rules with `psql`, writes an applied marker,
and stops the clone before reporting restore success.

PITR continuity checks are persisted separately as `managed_postgres_pitr_checks`.
The run-once PITR scheduler validates that a ready cluster has a succeeded base
backup and a contiguous succeeded WAL segment sequence on a single timeline.
This is a metadata continuity gate; restore drills remain the artifact
materialization gate. Long-running SQL control plane processes can also run
this scheduler internally with
`PALIMPSEST_PAAS_PITR_CHECK_SCHEDULER_INTERVAL_SECONDS`; the freshness window
defaults to 24 hours and can be overridden with
`PALIMPSEST_PAAS_PITR_CHECK_MAX_AGE_HOURS`.

Failover state is persisted as `managed_postgres_failovers`. The implementation
queues owned Rust node-agent commands to fence the source primary and then
promote the target standby, records the failover lifecycle, and marks the target
cluster ready after successful promotion. The environment-level
`managed_postgres_endpoints` record tracks the active cluster target; initial
cluster creation claims it only when it is empty, and successful failover cuts
it over to the promoted target with `updated_by_failover_id` for routing and
audit checks. A failover request can bind to a prepared `standby_id`, which
requires the standby to belong to the source cluster and to have reached
`succeeded` before promotion. This is the orchestration skeleton for HA;
production readiness still requires physical streaming standby startup
supervision, stronger host-level fencing, and full gateway traffic cutover
checks.

Standby preparation is persisted as `managed_postgres_standbys`. The first
implementation restores a base backup into a target cluster directory and
renders `standby.signal`, `primary_conninfo`, `primary_slot_name`, and WAL
restore configuration via the owned Rust node-agent protocol. The node agent
also creates an idempotent physical replication slot on the source before the
standby files are rendered. `primary_conninfo` is built from the managed
replication role credential stored in the control-plane secret backend rather
than the source superuser. Standby preparation is recorded as a first-class
`prepare_standby` operation for audit and operation history.

Role credential rotation is persisted in
`managed_postgres_role_credential_rotations`. New app, migration, replication,
and support passwords are first written under rotation-scoped pending secret
refs, then attached to the queued node-agent access command. Canonical role
secret refs are promoted only when that command succeeds; retries after an
already-applied completion are idempotent, and failed commands mark the rotation
failed without replacing the prior working credentials. Because the previous
credentials continue to serve traffic, failed rotation completion returns the
cluster lifecycle to `ready` while the rotation record carries the failure.

Standby lag checks are persisted as `managed_postgres_standby_checks`. The
control plane queues a Rust-owned `check_postgres_standby_lag` command against
the source cluster; the node agent runs a PostgreSQL assertion over
`pg_replication_slots` and fails the command if the physical slot is missing or
the WAL lag exceeds the requested threshold. The latest check feeds the
customer-facing `standby` health component. Production readiness still requires
continuous streaming startup supervision, replication credential rotation,
fencing, and cross-host failure-domain enforcement.

Object storage layout:

```text
s3://palimpsest-backups/<region>/<cluster_id>/
  base/<backup_id>/manifest.json
  base/<backup_id>/data.tar.zst
  wal/<timeline>/<segment>
  restore-tests/<restore_id>/result.json
```

Retention:

- Starter: short PITR window and daily base backups.
- Standard: longer PITR window and scheduled restore tests.
- Enterprise: configurable retention, restore tests, and export support.
- Active clusters can opt into `managed_postgres_backup_retention_policies`.
  The owned SQL control plane evaluates successful backups with `sqlx`,
  preserves the newest configured minimum, and queues Rust node-agent
  `delete_backup_data` commands for older artifacts after the retention window.
  This is separate from deletion tombstones so final backups retained during
  cluster deletion are governed by the tombstone retention flow.

Deletion:

- Deleting a cluster first disables new connections.
- A final snapshot is taken if policy requires it.
  The first implementation supports `{"final_backup":true}` on the delete API
  for ready clusters; the control plane queues a final base backup, then a
  stop command, then data deletion using the owned Rust node-agent protocol.
- Backup objects are tombstoned, then deleted after the retention window.
  A `managed_postgres_deletion_tombstones` metadata row is written after the
  node agent successfully removes data; it preserves cluster scope, version,
  retained backup reference, deletion time, and unrecoverable time so future
  cleanup cannot accidentally reopen the same cluster ID. Delete requests can
  set `tombstone_retention_days`; operators can list tombstones and run an
  expiration pass that marks records expired once their retention window has
  elapsed. Expiration queues a node-agent `delete_backup_data` command for
  retained final-backup artifacts and updates backup metadata when cleanup
  completes.
- Audit events record who requested deletion and when data became
  unrecoverable.
- Endpoint integrity after deletion is a known gap. The environment
  `managed_postgres_endpoints.active_cluster_id` is **not** automatically
  reassigned or cleared when its cluster is deleted, so an endpoint can be left
  pointing at a `deleted` cluster until a new primary is configured. Migration
  `V49__repair_deleted_managed_postgres_endpoints.sql` was a one-time backfill
  that repointed such dangling endpoints at the newest `ready` cluster in the
  same environment. Production readiness requires making this reassignment an
  explicit, audited step in the deletion/failover flow (clear the endpoint, or
  cut it over to a designated replacement) rather than a repair migration.

## 13. Networking

Customer traffic and internal replication traffic are separate:

- **Database endpoint:** Customer-facing Postgres endpoint with TLS.
- **Sync endpoint:** Customer-facing Palimpsest endpoint.
- **Replication path:** Private path from SyncDeployment to Postgres.
- **Agent path:** mTLS from node agent to control plane.
- **Backup path:** Host egress to object storage.

The database endpoint should be implemented by our gateway or a small Rust
TCP proxy that understands routing and telemetry but does not terminate the
Postgres protocol unless required for future features. TLS certificates are
issued per endpoint and rotated by the platform.

The first owned endpoint implementation is `palimpsest-paas-db-proxy`, a Rust
TCP pass-through binary in the PaaS gateway crate. It consumes route files that
map a public listen address to a managed Postgres upstream and records tenant
and cluster scope in the route model. That route state is also persisted in the
SQL control plane as `database_proxy_routes`, so the proxy can start from
owned control-plane configuration rather than static files.

Managed Postgres environments use an environment-owned database endpoint
binding. `managed_postgres_endpoints.database_proxy_listen_addr` stores the
stable customer-facing listen address, while `database_proxy_routes.upstream_addr`
is derived from the active cluster host assignment. Initial endpoint
configuration and successful failover cutover both regenerate the route row, so
customers keep the same database endpoint as primaries move between owned
Postgres instances. The DB proxy watches SQL-backed route discovery on a
short refresh interval and updates the active upstream for new connections on
an existing listener without a process restart. For TLS-enabled routes it
handles PostgreSQL `SSLRequest`, terminates TLS with rustls, validates startup
packets, enforces route-level allowed user/database policy, rejects forbidden
startup parameters such as `replication` and `options`, and can require exact
startup parameter values before forwarding accepted traffic to the managed
upstream. This keeps customer database traffic on our runtime while leaving
deeper SQL-message inspection after startup as explicit hardening work.

Endpoint certificate lifecycle is owned by the Rust control plane.
`managed_postgres_endpoint_certificates` records certificate status, validity,
fingerprint, certificate secret ref, and private key secret ref. Issuing a new
certificate stores material in the platform secret backend, revokes the
previous active certificate for that environment, and records the active
certificate on `managed_postgres_endpoints.active_certificate_id`. The current
local-dev issuer creates self-signed X.509 PEM material with `rcgen` so
lifecycle, secret storage, auditability, and rotation semantics are exercised
by the same DB proxy TLS path used in production. Database proxy route discovery
includes active certificate metadata but never includes private key material;
owned proxy infrastructure retrieves PEM bundles through a scoped certificate
bundle endpoint backed by the same secret backend.
Deconfiguring a managed endpoint proxy binding clears
`database_proxy_listen_addr`, deletes the corresponding SQL-backed
`database_proxy_routes` row, revokes active endpoint certificates, and leaves
the database cluster itself untouched.

Hosted sync routes are persisted in the SQL control plane as `gateway_routes`
so endpoint discovery, tenant scoping, TLS policy, and per-environment limits
are owned by the Rust control plane. The gateway can continue to load static
route files for local development, but production route state should be
exported or watched from this SQL-backed source.

## 14. Scheduling And Placement

The placement engine assigns clusters to hosts. Inputs:

- Region.
- Requested tier.
- Available CPU, memory, storage, and ports.
- Failure domain.
- Existing tenant placement.
- Maintenance windows.
- Host health and recent incident history.

Hard constraints:

- Do not place two replicas of the same future HA cluster in one failure
  domain.
- Do not exceed reserved storage or memory.
- Do not place a cluster on a host with incompatible PostgreSQL image support.
- Do not place a new cluster on a draining host.
- Do not place a new cluster on a host in maintenance or offline state.

Soft preferences:

- Keep database and SyncDeployment together.
- Spread tenants across hosts.
- Keep dev/staging on lower-cost pools.
- Prefer hosts with the same minor version image already cached.

## 15. High Availability

Initial HA should be deliberately simple:

### MVP

- Single primary.
- Mandatory backups and WAL archive.
- Fast process restart on the same host.
- Restore to a new host if the host or disk is lost.
- Palimpsest clients resync after SyncDeployment restart.

### HA Tier

- Primary plus standby in a separate failure domain.
- Streaming replication managed by the node agents.
- Control-plane-orchestrated failover with fencing.
- Replication slot recreation or failover-slot support for Palimpsest.
- Clear RPO/RTO by plan.

Failover must be conservative. The control plane should prefer degraded
availability over split brain. A failed primary must be fenced before a
standby is promoted.

### Resize

The first implementation treats resize as a controlled storage increase only.
The control plane accepts resize requests for `Ready` clusters, rejects
shrinks, verifies that the assigned active host has enough remaining storage
capacity, updates desired `storage_gib`, and queues a node-agent
`resize_postgres_storage` command. The current host-local command records
quota intent in the cluster data directory; production volume managers should
replace that intent file with provider-specific disk or filesystem quota
application while preserving the same command contract.

## 16. Upgrades And Maintenance

### Minor Updates

The first implementation exposes
`POST /v1/managed-postgres/clusters/{cluster_id}/update-minor` for ready
clusters. The control plane rejects cross-major requests, persists the target
PostgreSQL 18+ version, moves the cluster into `updating_postgres`, and queues
an owned Rust node-agent `update_postgres_minor` command. The local agent
records the target image/version intent in the cluster data directory; the
production runner should replace that with image pull, restart, and health
verification steps while keeping the same command contract.
The control plane also has a run-once maintenance scheduler that reads active
auto-minor-upgrade windows and queues the same command for ready clusters only
when the current UTC maintenance clock is inside the customer window.

1. Mark cluster `MaintenanceScheduled`.
2. Confirm backups and WAL archival are healthy.
3. Drain or block new long-running maintenance-conflicting operations.
4. Restart PostgreSQL onto the new minor image during the maintenance window.
5. Verify database health, slot health, app connectivity, and sync catch-up.
6. Mark maintenance complete.

### Major Upgrades

Managed Postgres supports PostgreSQL 18 and newer. The first major-upgrade
control-plane path is durable and additive: operators request a target
PostgreSQL major greater than the current cluster major, choose
`logical_replication_copy` or `pg_upgrade_copy`, and the control plane records
a `managed_postgres_major_upgrades` row linked to the queued operation and
node-agent command. The control plane includes the assigned source port in new
commands so the owned Rust node agent can render a local upgrade plan, run a
SQL preflight against the source server, and record the preflight and target
version artifacts in the cluster data directory. The preflight verifies that
the connected server major matches the expected source, that the target major
is higher, that logical-replication upgrades have `wal_level=logical`, and
that there are no invalid or not-ready indexes. Production runners extend the
same command contract with the actual copy/replication, cutover, and
post-cutover verification sequence.

The PostgreSQL 18 to 19 upgrade path should use one of:

- Blue/green logical replication into a new cluster.
- `pg_upgrade` on a copied volume with rollback.

The default should be blue/green for lower risk and clearer rollback, even if
it costs more during the migration window.

## 17. Observability

The node agent emits:

- Postgres process up/down and restart count.
- Connection counts by role.
- Transaction rate.
- WAL generated, archived, and retained.
- Replication slot lag.
- Database size and storage free space.
- Autovacuum activity and wraparound risk.
- Locks and slow queries.
- Backup success/failure and duration.
- Restore test age and result.
- Agent reconciliation success/failure.
- Operation state and step duration.

The first Prometheus contract intentionally keeps cardinality bounded to one
series per cluster for lifecycle state, allocated storage, backup freshness,
WAL archive freshness, PITR continuity freshness, restore-drill freshness, and
standby-check freshness. Host storage pressure remains host-scoped so
operators can distinguish customer allocation from host capacity pressure.
The first live runtime diagnostics are persisted as
`managed_postgres_runtime_checks`: the control plane connects through the
managed support role, samples connection count, max connections,
replication-slot lag bytes, long-running query count, blocked locks, oldest
transaction age, and autovacuum activity, then exposes scoped list/detail
history for operator dashboards.

The node-agent heartbeat also reports observed local state: which managed
Postgres clusters and SyncDeployments the agent actually finds on the host,
each with its data directory and running flag. The control plane reconciles
this into `node_host_observed_clusters` and
`node_host_observed_sync_deployments`, replacing the per-host snapshot on each
heartbeat. This gives operators a desired-vs-observed drift signal (a cluster
the control plane believes is assigned to a host but the agent does not see, or
a process the agent sees running that the control plane did not place) and feeds
the host detail view.

Customer-facing health should compress internal detail into actionable states:

| State | Meaning |
| --- | --- |
| `Healthy` | Database, backups, restore drills, WAL archive, and sync are within thresholds. |
| `Degraded` | Customer traffic works, but an internal safety signal needs action. |
| `AtRisk` | Backup, WAL, storage, or replication health threatens recoverability. |
| `Maintenance` | Planned operation is in progress. |
| `Unavailable` | Customer traffic is not being served. |

## 18. Security

Security requirements:

- mTLS between control plane and node agents.
- Short-lived agent leases and operation tokens. Lease responses now include
  a command-scoped operation token, and completion requires the matching token
  before lifecycle or operation state can advance.
- Scoped HMAC signatures for node-agent registration, heartbeat, lease, and
  command completion. The current implementation signs the host id, operation
  scope, and Unix timestamp with `PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64`
  and rejects stale requests. Registered hosts can be rotated to per-host
  signing credentials stored in the control-plane metadata database; the
  shared environment key remains only as bootstrap material. Operators can
  list and revoke per-host credential metadata, and successful signed requests
  update last-used operation metadata without exposing secret material.
- Shared bearer agent tokens only as a local/bootstrap fallback before mTLS
  and per-agent key issuance.
- Envelope-encrypted secret storage with owned KMS/key-management integration.
  The metadata plane now includes `secret_encryption_keys`, an owned registry
  for active, retiring, and retired envelope/KMS key refs used by secret
  envelopes, plus `secret_rewrap_plans` for auditable key-to-key rotation
  planning and wrapped-secret counts. The SQL control plane can now run a
  rewrap plan by opening matching `secret_refs` with the configured owned
  backend, resealing them to the target key ref, and recording succeeded or
  failed plan status. Production KMS adapters still need external-key
  integration beyond the local/env-envelope backend.
- Per-environment managed Postgres endpoint certificate lifecycle and DB proxy
  TLS termination. Certificate authority providers are now durable control
  plane resources with local-dev, ACME, and external-PKI kinds, active/disabled
  status, and a default managed-Postgres issuer. Endpoint certificate issuance
  resolves through that provider registry. ACME issuance creates owned order
  rows, generated private-key and CSR secrets, and HTTP-01 key-authorization
  secrets that the control plane serves at `/.well-known/acme-challenge`.
  Orders must pass the owned challenge-validation endpoint before finalization
  activates only certificates that match the generated private key. Invalid
  finalization material marks the order and provisioning certificate failed.
  A run-once scheduler endpoint and optional background interval advance
  pending orders through the owned validation transition. Full external ACME
  directory polling remains to be wired.
- Separate customer, replication, observer, and support credentials.
- Audited break-glass support access. The control plane now stores
  cluster-scoped support-access sessions with reason, ticket reference,
  requester, approver, revoker, expiry, and lifecycle status. Approval and
  revocation are separate audited operations so future support tooling can
  require an active unexpired session before resolving support credentials.
- No customer superuser by default.
- Immutable audit log for lifecycle operations. The Flyway-managed metadata
  schema now rejects direct `UPDATE`, `DELETE`, and `TRUNCATE` operations on
  `audit_events`, so audit history remains append-only even if a caller
  bypasses the HTTP API.
- Signed deployment and cluster specs.
- Host hardening baseline and regular vulnerability patching. The metadata
  plane now records node-host hardening checks with image, OS release, kernel,
  PostgreSQL 18+ minimum support, container runtime, disk-encryption,
  firewall, unattended-upgrade, and patch-freshness evidence. The owned Rust
  node agent can scan and submit this evidence through
  `palimpsest-paas-node-agent hardening-check <control-plane-url>`, with
  environment overrides for image and host-security signals during bootstrap
  or local development. The production scanner should run from the owned
  Rust node-agent bootstrap and heartbeat path.

The node agent validates every command against local ownership metadata before
executing it. A command for cluster A must not be able to mutate cluster B
even if the control plane sends a malformed request.

## 19. Local Development

Local development uses the same logical model with fewer moving parts:

- `palimpsest dev up` starts PostgreSQL 18+ and Palimpsest.
- Generated config uses `database.mode = "local"`.
- Logical replication, publication, slot, and roles are created
  automatically.
- Backups are optional locally.
- The local stack can run through Docker Compose or a direct process runner.

Local mode should share config validation code with managed mode so users find
version, role, publication, and replication issues before deploying.

## 20. Implementation Phases

### Phase A: Specs And Local Mode

- Define `ManagedPostgresCluster`, `BackupPolicy`, `RestoreRequest`,
  `PostgresRole`, and `HostAssignment` schemas.
- Add local PostgreSQL 18+ config and init SQL.
- Add CLI `dev up/down/reset`.
- Validate managed/local configs reject PostgreSQL versions below 18.

### Phase B: Node Agent Prototype

- Implement host registration and heartbeat.
- Implement local cluster create/start/stop/delete against a development host.
- Generate PostgreSQL config and roles.
- Emit observed state and logs.

### Phase C: Backup And Restore

- Implement base backup and WAL archival.
- Add backup manifests and restore verification.
- Add clone-to-new-environment flow.
- Add deletion tombstones and retention handling. **Started with optional
  final-backup deletion that preserves a succeeded backup before stop/delete
  execution, plus durable deletion tombstones that block cluster-id reuse.
  Tombstone listing/detail, expiration marking, and retained-backup cleanup
  commands are implemented. Active-cluster backup retention policies now add
  opt-in expiration for older successful backups while preserving a configured
  minimum count; broader object-store lifecycle policy remains.**

### Phase D: Control Plane Reconciler

- Add durable operation state.
- Add placement engine.
- Add reconciler loop and agent leases.
- Add audit and usage events for lifecycle operations.

### Phase E: Production Hardening

- Add host draining and maintenance. **Started with an operator host-state
  API; node-agent heartbeat preserves operator-held draining, maintenance,
  and offline states so placement will not accidentally reopen a host. Host
  list/detail APIs expose capacity, state, region, and failure-domain filters
  for operator inventory and maintenance views.**
- Add resize.
- Add minor version updates. **Started with a control-plane update-minor API,
  `updating_postgres` lifecycle state, durable update operations, and a Rust
  node-agent command for PostgreSQL 18+ target-version intent.**
- Add HA tier design implementation.
- Add restore drills, dashboards, and alerting.

## 21. Open Questions

1. Should the first host pool use one attached volume per cluster or one large
   encrypted host volume with per-cluster quotas?
2. Which PostgreSQL 18+ extensions are supported on day one?
3. Should the customer database endpoint be a raw TCP proxy or a protocol-aware
   gateway from the start?
4. What is the smallest HA tier we are willing to sell publicly?
5. How long should deletion tombstones and final snapshots be retained by
   default? The control plane supports explicit tombstone retention windows,
   but product defaults remain open.
6. Do we need local direct-process mode in addition to Docker Compose for
   developers who avoid Docker?
