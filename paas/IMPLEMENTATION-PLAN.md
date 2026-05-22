# PaaS Implementation Plan

**Status:** Implementation started
**Goal:** Add a managed Postgres + Palimpsest platform to this repository
without disrupting the existing sync engine, CLI, SDKs, tests, or deployment
artifacts.

The PostgreSQL runtime details are specified in
[MANAGED-POSTGRES-DESIGN.md](MANAGED-POSTGRES-DESIGN.md). This plan focuses
on how to add that system to the repository incrementally.

## 1. Guiding Constraints

The PaaS should be built as an additive layer around the current project.
Standalone Palimpsest remains a first-class deliverable.

1. Do not change `palimpsest serve` behavior to require hosted-platform
   services.
2. Keep PaaS code under `paas/` until individual pieces are stable enough to
   promote into shared crates or root-level deploy artifacts.
3. Treat changes to existing crates as narrow interface additions: config
   parsing hooks, telemetry hooks, lifecycle hooks, and CLI delegation points.
4. Avoid PaaS-specific assumptions in `palimpsest-server`,
   `palimpsest-wal`, `palimpsest-dataflow`, and client packages.
5. Keep managed Postgres support PostgreSQL 18+ only. Local development uses
   PostgreSQL 18+ as well.
6. Do not use Kubernetes for managed database runtime. The PaaS uses a Rust
   control plane, Rust node agents, and host-based process supervision that we
   own.
7. Make every phase independently testable and reversible.

## 2. Proposed Directory Layout

The initial `paas/` tree should keep specifications, local tooling, and
service prototypes together:

```text
paas/
  README.md
  IMPLEMENTATION-PLAN.md
  specs/
    environment.schema.json
    deployment-spec.schema.json
    usage-event.schema.json
    quota-alert.schema.json
    query-permission-policy.schema.json
    jwt-issuer.schema.json
    webhook-endpoint.schema.json
    sso-provider.schema.json
  local/
    docker-compose.yaml
    postgres/
      init.sql
      postgresql.conf
  control-plane/
    README.md
    migrations/
    api/
  data-plane/
    README.md
    gateway/
    sync-deployment/
    postgres-fleet/
    node-agent/
  deploy/
    host-images/
    terraform/
  observability/
    alerts/
    dashboards/
```

Do not add all directories at once unless there is content for them. Create
them as phases land.

## 3. Integration Boundaries

### Existing Core Crates

Allowed changes:

- Add public APIs that expose runtime state already available internally.
- Add config fields that have no effect unless set.
- Add metrics with stable names and low cardinality.
- Add tests for existing behavior when a PaaS-adjacent hook is introduced.

Avoid:

- Making control-plane concepts visible in core execution types.
- Requiring hosted metadata, billing, tenants, or cloud credentials to run
  the engine.
- Changing wire protocol semantics for hosted-only reasons.
- Moving existing crates into `paas/`.

### Existing CLI

The existing `palimpsest` CLI should stay the developer entrypoint. Add cloud
and local commands incrementally:

```text
palimpsest dev up
palimpsest dev down
palimpsest dev reset
palimpsest db create
palimpsest db psql
palimpsest deploy
```

Implementation should delegate most logic to modules under `paas/` or a
future shared `palimpsest-paas-*` crate. `serve`, `validate-config`,
`dump-catalog`, and `slot-info` should keep their current behavior.
**Started:** `dev up/down/reset/status/env` manage the local stack;
`db create` posts a SQL-backed managed PostgreSQL 18+ cluster intent to the
control plane; and `db psql` opens `psql` against the local dev stack or a
configured database URL.

### Existing Deploy Artifacts

Keep `deploy/helm/palimpsest` focused on the standalone self-hosted sync
engine. The managed PaaS runtime must not depend on Kubernetes, Kubernetes
operators, CRDs, StatefulSets, or Helm charts. Put owned host images,
system-level units, bootstrap scripts, and cloud infrastructure under
`paas/deploy/` first. **Started with node-host systemd units and
`paas/deploy/host-images/bootstrap-node-host.sh`.** Promote shared templates
only after the managed runtime has stabilized.

## 4. Phase 0: Repo Scaffold And Specs

Deliverables:

- `paas/README.md` and this implementation plan. **Started.**
- Environment config schema with `database.mode = managed | local | external`.
  **Started in `paas/specs/environment.schema.json`.**
- Deployment spec schema signed by the future control plane. **Started in
  `paas/specs/deployment-spec.schema.json`.**
- SyncDeployment state schema. **Started in
  `paas/specs/sync-deployment.schema.json`.**
- Usage event schema with idempotency fields. **Started in
  `paas/specs/usage-event.schema.json`.**
- Shared Rust model crate for managed Postgres intent, PostgreSQL 18+
  validation, and node-agent command contracts. **Started in
  `paas/crates/palimpsest-paas-core`.**
- Rust control-plane skeleton for placement and reconciliation. **Started in
  `paas/crates/palimpsest-paas-control-plane`.**
- Rust node-agent skeleton for host-local PostgreSQL command planning.
  **Started in `paas/crates/palimpsest-paas-node-agent`.**
- ADR documenting PostgreSQL 18+ as the managed and local support floor.
  **Added in `paas/adr/0001-managed-postgres-version-floor.md`.**
- ADR documenting the no-Kubernetes runtime decision and the Rust
  control-plane / node-agent model. **Added in
  `paas/adr/0002-owned-rust-runtime-no-kubernetes.md`.**

Acceptance criteria:

- No existing binaries change behavior.
- Root workspace builds exactly as before.
- PaaS schemas can be validated independently in CI.

## 5. Phase 1: Local Developer Stack

Purpose: give users the same database-plus-sync shape locally before building
the hosted control plane.

Deliverables:

- `paas/local/docker-compose.yaml` with PostgreSQL 18+ and Palimpsest.
  **Started in `paas/local/docker-compose.yaml`.**
- Postgres config enabling logical replication. **Started in
  `paas/local/postgres/postgresql.conf`.**
- Init SQL that creates the development database, publication, replication
  role, application role, and replication slot. **Started in
  `paas/local/postgres/init/01-palimpsest.sql`.**
- `palimpsest dev up`, `dev down`, and `dev reset`. **Started in
  `crates/palimpsest-cli`.**
- Support for optional seed SQL and migration directories. **Started in
  `paas/local/postgres/migrations` and `paas/local/postgres/seeds`.**
- Generated `.env` output for app connection strings and SDK endpoint.
  **Started with `palimpsest dev env`.**
- Local database shell helper. **Started with `palimpsest db psql`, which
  defaults to the local PostgreSQL 18 dev stack URL and can also use explicit
  or environment-provided database URLs.**

Acceptance criteria:

- A developer can start local Postgres plus Palimpsest with one command.
- Existing demo app can point at the local stack.
- `cargo test --workspace --all-features` remains unaffected.

## 6. Phase 2: Managed Postgres Primitive

Purpose: define the smallest production database unit the platform owns.

Deliverables:

- Cluster lifecycle model: create, resize, minor update, pause if supported, restore, and
  delete. **Started: create/reconcile, stop, delete, backup, WAL archive, and
  restore are implemented; resize now exposes
  `POST /v1/managed-postgres/clusters/{cluster_id}/resize`, checks assigned
  host storage capacity, moves the cluster through `resizing`, and queues a
  Rust node-agent `resize_postgres_storage` command that records host-local
  quota intent. Minor updates now expose
  `POST /v1/managed-postgres/clusters/{cluster_id}/update-minor`, require the
  same PostgreSQL major version, move the cluster through `updating_postgres`,
  and queue a Rust node-agent `update_postgres_minor` command that records the
  target PostgreSQL 18+ image intent. Pause/resume is now started with
  `POST /v1/managed-postgres/clusters/{cluster_id}/pause` as a customer-facing
  stop alias and `POST /v1/managed-postgres/clusters/{cluster_id}/resume`,
  which queues `start_postgres`, records a `start_cluster` operation, and
  returns a previously configured stopped cluster to `ready` after command
  success. Major upgrades now expose
  `POST /v1/managed-postgres/clusters/{cluster_id}/major-upgrades`, validate a
  higher PostgreSQL 18+ major target, persist an auditable upgrade record, and
  queue `upgrade_postgres_major` with an explicit strategy and source port.
  The Rust node agent now renders a major-upgrade plan, runs SQL preflight
  checks on the source server when the port is present, and records preflight
  and target-version artifacts without changing the external API.**
- PostgreSQL 18+ image/version policy.
- Rust node-agent command protocol and host registration model. **Started in
  `paas/crates/palimpsest-paas-node-agent`; commands now support planning
  and execution through an injectable process runner, plus `poll-once` and
  `poll-once-container`/`poll-once-dry-run` for leasing and completing
  SQL-backed control-plane commands over HTTP.**
- Host deployment artifacts for the owned runtime. **Started in
  `paas/deploy/systemd` with register, heartbeat, and queue-poll units plus
  a bootstrap script that creates the dedicated host user, runtime
  directories, config file, and systemd unit installation. Agent-facing
  routes now support scoped HMAC signatures using
  `PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64`, with bearer tokens retained as
  a local/bootstrap fallback. The SQL control plane can issue and rotate
  database-backed per-host signing credentials through
  `POST /v1/node-hosts/{host_id}/agent-credentials`; node agents send the
  returned `PALIMPSEST_PAAS_AGENT_SIGNING_KEY_ID` so the server validates
  against the active host credential instead of the shared bootstrap key.
  Operators can list and revoke per-host credential metadata, and successful
  signed requests update `last_used_at` plus `last_used_operation` for
  rotation audits without exposing secret material.
  Command leases now also return a short-lived operation token; the control
  plane stores only its SHA-256 hash and requires the token on completion.
  Node-host hardening checks now persist image, OS, kernel, PostgreSQL 18+
  floor, container runtime, disk-encryption, firewall, unattended-upgrade, and
  patch-freshness evidence for each host. The Rust node agent can generate and
  submit this evidence with `palimpsest-paas-node-agent hardening-check
  <control-plane-url>`; operators can still record checks through the same SQL
  API for bootstrap or remediation evidence.**
- Role model for app, migration, replication, and support access. **Started
  in `palimpsest-paas-core::DatabaseRoleSpec` and
  `DatabaseRoleCredential`; the reconciler now emits a node-agent
  `configure_postgres_access` command that creates app, migration,
  replication, and support roles against a running PostgreSQL 18 instance.
  Break-glass support access is now represented by
  `managed_postgres_support_access_sessions`, cluster-scoped request/list/detail
  routes, explicit approve/revoke routes, bounded expiry, and audit events so
  future support tooling can require an active session before resolving the
  support credential.**
- Backup policy model with base backups, WAL archive, PITR window, and
  restore metadata. **Started with structured backup and WAL archive status
  models plus `paas/specs/node-agent-status.schema.json`; node-agent base
  backup, WAL archive, and restore-preparation command planning/execution are
  started. The SQL control plane now persists managed backup records and
  `POST /v1/managed-postgres/clusters/{cluster_id}/backups` queues a
  PostgreSQL 18 `pg_basebackup` command through the Rust node agent. Backup
  history and detail are exposed through scoped SQL-backed list/get endpoints
  for status pages and operators. WAL
  segment archival is durable through
  `POST /v1/managed-postgres/clusters/{cluster_id}/wal-archives`, which
  queues a Rust node-agent `archive_wal_segment` command. WAL archive segment
  history and detail are exposed through scoped SQL-backed list/get endpoints.
  The SQL control
  plane also exposes `POST /v1/scheduler/backups/run-once` to queue base
  backups for ready clusters missing a requested, running, or successful
  backup, and `serve-sql-api` can run that scheduler periodically with
  `PALIMPSEST_PAAS_BACKUP_SCHEDULER_INTERVAL_SECONDS`. Successful node-agent
  base backups now write a `manifest.json` artifact with backup id, cluster
  id, host id, detected PostgreSQL version, source data directory, and
  artifact metadata; restore preparation skips that manifest when
  materializing a data directory. The SQL control plane also exposes
  per-backup artifact catalog endpoints so local filesystem and later
  S3-compatible object-store uploads can be tracked with provider, object URI,
  manifest path/hash, size, lifecycle status, and error metadata. Successful
  `run_base_backup` command completion now automatically records every artifact
  reported by the node agent, so callers do not need to hand-register backup
  artifacts after the agent reports success. The node agent now reports
  manifest SHA-256 and artifact byte size in the completion payload, and the
  control plane persists those fields on auto-recorded artifacts. When
  `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_DIR` is configured, the node agent also
  copies completed base backups into an owned filesystem-backed S3-compatible
  object-store layout and reports the resulting `s3://` URI beside the local
  filesystem artifact. When
  `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_ENDPOINT` is configured, it can instead
  PUT each backup file to a path-style HTTP or HTTPS S3-compatible endpoint
  and report a `s3_compatible_http` artifact. Optional bearer/API-token headers
  are supported through `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AUTH_TOKEN` plus
  header/scheme overrides. HTTPS endpoints are supported with an operator
  supplied CA bundle via `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_CA_CERT_FILE` and
  optional `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_TLS_SERVER_NAME`.
  AWS Signature Version 4 request signing is supported for S3-compatible
  endpoints through `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_ACCESS_KEY_ID`,
  `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_SECRET_ACCESS_KEY`, optional region,
  and optional service env vars. Non-SigV4 provider-specific request signing
  remains follow-on production adapter work.
  Tombstone and retention cleanup queueing now marks matching backup artifacts
  `expired` before the node-agent delete command advances them to `deleted`,
  giving operators a visible lifecycle state while physical cleanup is
  pending.**
- Restore metadata and clone preparation. **Started with
  `managed_postgres_restores` in the Flyway schema and
  `POST /v1/managed-postgres/clusters/{cluster_id}/restores`, which creates a
  clone cluster, queues a Rust node-agent `prepare_restore` command, and
  records restore success or failure. Cross-environment clone restores now
  require an active source-environment clone redaction policy from
  `managed_postgres_clone_redaction_policies`; the policy is included in the
  node-agent restore command. The node agent writes the redaction manifest and
  generated SQL into the clone directory, starts the restored clone on its
  assigned port, applies null/static/hash redaction rules, writes an applied
  marker, and stops the clone before reporting restore success. Restore
  history and detail are exposed through scoped SQL-backed list/get endpoints.
  Restore drills now have
  dedicated `managed_postgres_restore_drills` metadata, scoped
  `restore-drills` list/detail endpoints, and
  `POST /v1/scheduler/restore-drills/run-once`, which schedules a clone from
  the latest successful backup when the latest successful drill is absent or
  older than the requested age. `serve-sql-api` can run the same restore-drill
  scheduler periodically with
  `PALIMPSEST_PAAS_RESTORE_DRILL_SCHEDULER_INTERVAL_SECONDS` and
  `PALIMPSEST_PAAS_RESTORE_DRILL_MAX_AGE_HOURS`. PITR continuity checks now persist
  `managed_postgres_pitr_checks`, expose scoped `pitr-checks` endpoints, and
  provide `POST /v1/scheduler/pitr-checks/run-once` to validate that a ready
  cluster has a successful base backup plus a contiguous succeeded WAL segment
  sequence on one timeline. `serve-sql-api` can run the same PITR-check
  scheduler periodically with
  `PALIMPSEST_PAAS_PITR_CHECK_SCHEDULER_INTERVAL_SECONDS` and
  `PALIMPSEST_PAAS_PITR_CHECK_MAX_AGE_HOURS`. Failover metadata is now started with
  `managed_postgres_failovers`, scoped `failovers` endpoints, and a
  Rust node-agent fence-then-promote path that records source
  `fence_postgres_primary` intent before `promote_postgres_standby`, then
  advances the target cluster to `ready` after command success. Environment
  active database target metadata is now persisted in
  `managed_postgres_endpoints`: initial cluster creation sets the endpoint when
  absent, `GET /v1/environments/{environment_id}/managed-postgres-endpoint`
  exposes it, and successful failover cuts it over to the promoted target while
  recording `updated_by_failover_id`. The endpoint can now own a stable
  database proxy listen address, and the control plane regenerates the
  SQL-backed proxy route from the active cluster assignment on initial
  configuration and failover. Endpoint certificate lifecycle is now started
  with `managed_postgres_endpoint_certificates`, scoped
  `certificates` issue/list/detail/bundle endpoints, secret-backed certificate
  and private key material, previous-active revocation, and
  `active_certificate_id` on the endpoint record. Database proxy route
  discovery now includes active certificate metadata without exposing private
  key material. External-PKI providers can now activate an endpoint
  certificate by importing already-issued PEM certificate and private-key
  material into the owned secret backend, giving production operators a
  controlled handoff path. ACME providers now create provisioning endpoint
  certificate rows, generated private-key and CSR secrets, and first-class
  `managed_postgres_acme_orders` with HTTP-01 challenge tokens and
  key-authorization secret refs; scoped `acme-order`, `acme-challenge/validate`,
  and `acme-finalize` endpoints expose the owned order lifecycle,
  `/.well-known/acme-challenge` serves pending HTTP-01 key authorizations from
  the owned secret backend, validation advances orders to `ready_to_finalize`,
  and finalize verifies the returned PEM chain matches the generated private
  key before activation. Invalid ACME finalization material now records a
  failed order and failed provisioning certificate with the validation error.
  `POST /v1/scheduler/acme-orders/run-once` and
  `PALIMPSEST_PAAS_ACME_ORDER_SCHEDULER_INTERVAL_SECONDS` now let the owned
  control plane advance pending ACME orders in the background after HTTP-01
  responder material is available.
  Endpoint certificates can now be renewed through
  an owned control-plane route that reuses the active common name, enforces a
  renewal window unless forced, stores replacement material through the secret
  backend, and revokes the old active record only after replacement activation.
  Managed endpoint proxy bindings can now be
  deconfigured through the environment endpoint API, which removes the
  SQL-backed proxy route and revokes active endpoint certificates.
  Failover can now bind to a succeeded standby by `standby_id` so arbitrary
  target promotion can be phased out behind a compatibility path.
  The Rust node-agent fencing path now attempts a local immediate Postgres stop
  before recording a durable fence marker, so promotion is no longer only a
  metadata intent on hosts managed by the agent.
  Standby preparation is now started with `managed_postgres_standbys`, scoped
  `standbys` endpoints, and a Rust node-agent `prepare_postgres_standby`
  command that restores a base backup and renders `standby.signal`,
  `primary_conninfo`, `primary_slot_name`, WAL restore configuration, and the
  assigned target Postgres port after creating an idempotent physical
  replication slot on the source. `primary_conninfo` now uses the managed
  replication role credential resolved from the control-plane secret backend
  instead of the source `postgres` superuser. For managed API-created standbys,
  the node agent starts the restored target as a hot standby and the control
  plane marks the target cluster `ready` only after the command succeeds. Standby
  preparation is tracked as a first-class `prepare_standby` operation. Standby
  lag checks now persist `managed_postgres_standby_checks`, expose scoped
  nested `checks` endpoints, queue a Rust node-agent
  `check_postgres_standby_lag` command against the source, and feed a
  customer-facing `standby` health component.**
- Per-cluster metrics contract. **Started with bounded-cardinality
  Prometheus metrics for lifecycle state, allocated storage, last successful
  backup, last successful WAL archive, last successful PITR check, last
  successful restore drill, and last successful standby check.**
- Internal runbook for backup restore drills.

Acceptance criteria:

- A cluster can be provisioned and configured for Palimpsest without manual
  SQL. **Started: the local smoke now provisions PostgreSQL 18, creates the
  managed roles, creates the Palimpsest publication, creates the logical
  replication slot, and reconciles the cluster to `ready`.**
- A Rust node agent can create, start, stop, and report a PostgreSQL 18+
  cluster on a development host without Kubernetes. **Partially implemented:
  create/config/start/stop/report execution paths exist, and initial hardened
  systemd units are present. Host hardening metadata can be recorded and read
  through the SQL control plane; production live scanning and Postgres
  integration tests remain.**
- Backups and WAL archive status are visible through structured status.
  **Partially implemented: base-backup requests are durable in
  `managed_postgres_backups`, command completion updates backup status,
  SQL-backed list/detail backup status endpoints are available, and the local
  smoke verifies a real `pg_basebackup` artifact. Restore requests
  are durable, SQL-backed restore list/detail endpoints are available, and the
  local smoke verifies a restored clone data directory. Restore-drill requests
  are durable, the run-once and opt-in background schedulers are idempotent for
  recently successful drills, and the local smoke verifies a real drill clone and a healthy
  restore-drill health component. PITR continuity checks are durable,
  SQL-backed list/detail endpoints and an opt-in background scheduler are
  available, and the local smoke verifies a succeeded check against the archived WAL sequence.
  Failover requests are durable, queue an owned node-agent promotion command,
  and the local smoke verifies a clone-to-clone promotion path without
  Kubernetes. Standby preparation requests are durable, queue an owned
  node-agent standby command, and the local smoke verifies base-backup restore
  plus standby recovery files.
  WAL archive requests are durable and the local smoke verifies a real segment
  copied from `pg_wal` into the managed archive prefix. SQL-backed WAL segment
  list/detail endpoints expose archive status. Continuity checks across a PITR
  window remain.**
- The sync engine can connect through least-privilege replication credentials.
  **Partially implemented: app, migration, replication, and support roles are
  created with control-plane-issued credentials, app/replication secret
  refs are persisted, and `POST
  /v1/managed-postgres/clusters/{cluster_id}/roles/rotate` now regenerates
  owned app/migration/replication/support secrets as pending refs, queues the
  Rust node-agent access-configuration command to apply them without returning
  plaintext in the rotate response, and promotes canonical secret refs only
  after successful node-agent completion. Rotation attempts are now persisted
  in `managed_postgres_role_credential_rotations`, with pending/applying/applied/
  failed state and idempotent completion handling. Failed rotation completion
  leaves the cluster `ready` because existing canonical credentials are still
  valid. SyncDeployment rendering can now resolve a database password secret ref
  through a pluggable secret resolver before rendering the upstream database
  URL. Production secret-store adapter remains.**

## 7. Phase 3: SyncDeployment Wrapper

Purpose: run the existing engine as a managed workload without forking it.

Deliverables:

- Wrapper process or sidecar that renders `palimpsest serve` config from a
  signed deployment spec. **Started in
  `paas/crates/palimpsest-paas-sync-wrapper`; Ed25519 signature verification
  is available for production specs, and database password secret refs can be
  resolved before config rendering.**
- Health checks that distinguish Postgres, WAL slot, dataflow, gateway, and
  config failures. **Started with `SyncDeploymentHealth`.**
- Config hot-reload classification: safe reload, drain/restart, and blocked.
  **Started with `classify_reload`.**
- Drain behavior that preserves current resync semantics. **Started with
  supervisor transition plans for safe reload, drain/restart, and blocked
  identity changes.**
- Telemetry export for environment id, deployment id, region, and shard id.
  **Started in rendered `[managed]` config metadata.**

Acceptance criteria:

- `palimpsest serve` remains runnable without the wrapper. **Maintained: the
  wrapper is a separate crate and binary.**
- Managed runtime failures produce actionable status without changing core
  protocol semantics. **Partially implemented with component health status.**
- Restart and drain paths are covered by integration tests. **Started with
  `paas/crates/palimpsest-paas-sync-wrapper/tests/supervisor_transitions.rs`,
  which exercises public supervisor planning for initial start, safe reload,
  drain/restart, and blocked identity changes.**

## 8. Phase 4: Control Plane Skeleton

Purpose: introduce durable platform state and APIs.

Deliverables:

- Metadata schema for organizations, projects, environments, managed
  Postgres clusters, SyncDeployments, config versions, secrets references,
  usage events, and audit events. **Started in
  `paas/control-plane/migrations/V1__initial_control_plane.sql`.**
- API for project/environment CRUD. **Started as a typed service layer and
  in-memory plus SQL-backed HTTP APIs in
  `paas/crates/palimpsest-paas-control-plane`.**
- CLI entrypoint for managed cluster creation. **Started with
  `palimpsest db create`, which reuses the shared PaaS core model and rejects
  PostgreSQL versions below 18 before posting a cluster intent to the SQL
  control plane.**
- Config upload, validation, diff, and deployment status. **Started with
  config-version records and diff reporting in the control-plane service.**
- SyncDeployment state API. **Started with a typed `SyncDeployment` model,
  `POST /v1/sync-deployments`, SQL persistence in `sync_deployments`, and
  `paas/examples/sync-deployment.requested.json`. SQL-backed list/detail
  endpoints now expose scoped deploy state through
  `GET /v1/sync-deployments` and `GET /v1/sync-deployments/{deployment_id}`
  for dashboard deploy history and support status views.**
- Secret reference model backed by the chosen secret manager. **Started: the
  SQL control plane persists `secret_refs`, links app and replication secret
  refs back to managed clusters, and injects issued credentials into the
  node-agent access-configuration command. Local development uses the
  `local-dev` provider; production-like runs can select `env-envelope`, which
  stores generated role passwords as AES-256-GCM envelopes with a key ref
  supplied by the owned control plane environment, or `file-envelope`, which
  loads the same envelope keyring from a mounted JSON file delivered by
  systemd credentials, a sidecar, or an internal KMS sync process. The SQL
  control plane now tracks owned secret-encryption key refs through
  `secret_encryption_keys`,
  with active, retiring, and retired states plus platform-admin list/detail
  routes. It also records auditable `secret_rewrap_plans` that validate a
  source key and active target key, count currently wrapped secret refs, and
  expose list/detail status. Plans can now be run through the SQL API, which
  opens matching secret refs with the configured owned backend, reseals them
  to the target key ref, and records rewrapped counts and succeeded/failed
  status.**
- Provisioner loop that reconciles desired state into one development data
  plane through Rust node agents. **Started with the Rust reconciler and
  SQL-backed reconcile route in
  `paas/crates/palimpsest-paas-control-plane`.**
- Durable SQL-backed store. **Started in
  `paas/crates/palimpsest-paas-control-plane/src/sql_store.rs` using `sqlx`;
  migrations are versioned for Flyway under `paas/control-plane/migrations`,
  and `serve-sql-api` exposes the initial routes against Postgres. Managed
  Postgres commands that flow through Rust node agents now create durable
  `operations` rows and link queued `agent_commands.operation_id` to those
  operations; command completion marks the operation `succeeded`, `failed`,
  or `cancelled`.**
- Usage-event ingestion. **Started with `POST /v1/usage-events`, idempotent
  `usage_events` persistence through `sqlx`, filtered usage-event export via
  `GET /v1/usage-events`, and
  `paas/examples/usage-event.sync-egress.json`.**
- Quota policy enforcement. **Started with
  `paas/control-plane/migrations/V2__quota_policies.sql`,
  `POST /v1/quota-policies`, and over-quota rejection on usage-event
  ingestion.**

Acceptance criteria:

- Control-plane outages do not break an already running data-plane workload.
- Every mutating API writes an audit event. **Covered for the initial service
  mutations by unit tests.**
- Managed Postgres cluster creation is represented in both the API surface and
  durable schema. **Started with `POST /v1/managed-postgres/clusters` and
  `managed_postgres_clusters` persistence.**
- Node-agent coordination is represented as SQL-backed host registration,
  heartbeat, command enqueue, lease, and completion routes. **Started in the
  SQL API and `agent_commands` schema. Agent registration, heartbeat, lease,
  and completion routes can require `PALIMPSEST_PAAS_AGENT_TOKEN`; completion
  now also requires the short-lived operation token issued by the lease route.
  mTLS remains. Agent-backed managed Postgres reconcile, stop, resize, delete,
  backup, WAL archive, and restore commands are now linked to durable
  operation rows.**
- Placement respects host cluster slots and storage capacity. **Started by
  deriving effective host capacity from active node-host reports plus assigned
  managed Postgres clusters, then rejecting reconcile placement when a
  requested cluster would exceed either max cluster count or storage GiB.**
- Host draining and maintenance. **Started with
  `POST /v1/node-hosts/{host_id}/state`, which lets operators mark hosts
  `active`, `draining`, `maintenance`, or `offline`; placement only considers
  active hosts, and node-agent heartbeat preserves operator-held drain,
  maintenance, and offline states. Host inventory is exposed through
  `GET /v1/node-hosts` and `GET /v1/node-hosts/{host_id}` with state,
  region, and failure-domain filters for capacity and maintenance views.
  Hardening evidence is exposed through
  `POST /v1/node-hosts/{host_id}/hardening-checks`,
  `GET /v1/node-hosts/{host_id}/hardening-checks`, and check detail routes.**
- Control-plane reconciliation can now place a SQL-backed managed Postgres
  cluster on an active node host, persist next cluster state, and enqueue
  resulting node-agent commands. **Started with
  `POST /v1/managed-postgres/clusters/{cluster_id}/reconcile`.**
- Cluster-scoped operation and agent-command history is exposed through
  `GET /v1/managed-postgres/clusters/{cluster_id}/operations`,
  `GET /v1/managed-postgres/clusters/{cluster_id}/operations/{operation_id}`,
  `GET /v1/managed-postgres/clusters/{cluster_id}/agent-commands`, and
  `GET /v1/managed-postgres/clusters/{cluster_id}/agent-commands/{command_id}`
  with status/kind filters for dashboard and support views.
- Successful node-agent command completion advances SQL-backed cluster
  lifecycle state and enables the next reconcile step. **Started for
  `prepare_postgres -> starting`, `start_postgres -> configuring_roles`,
  `configure_postgres_access -> ready`, and `stop_postgres -> stopped`.**
- Controlled stop is exposed through
  `POST /v1/managed-postgres/clusters/{cluster_id}/stop`, which records the
  cluster as `stopping` and queues a Rust node-agent `stop_postgres` command.
  `POST /v1/managed-postgres/clusters/{cluster_id}/pause` uses the same
  durable stop path for dashboard wording, and
  `POST /v1/managed-postgres/clusters/{cluster_id}/resume` queues a
  `start_postgres` command for stopped clusters and marks them ready after
  successful command completion.
- Controlled deletion is exposed through
  `POST /v1/managed-postgres/clusters/{cluster_id}/delete` for stopped or
  failed clusters. It records the cluster as `deleting`, queues a Rust
  node-agent `delete_postgres_data` command, and advances successful
  completion to `deleted`. Ready clusters can request
  `{"final_backup":true}`; the control plane queues a final base backup, then
  a stop command, then the delete command, with durable operation rows for
  each step. Successful delete completion now writes a
  `managed_postgres_deletion_tombstones` row that records scope, version,
  retained backup, deletion time, and unrecoverable time; new creates reject
  tombstoned cluster IDs. Tombstones can now carry
  `tombstone_retention_days`, be listed/detail-read through control-plane
  APIs, and be marked expired by an operator expiration pass once retention
  expires. Expiration queues a Rust node-agent `delete_backup_data` command
  for retained backup artifacts and marks the backup row `deleted` on success.
- Active-cluster backup retention policies are now represented by
  `managed_postgres_backup_retention_policies` and managed through
  `POST/GET /v1/managed-postgres/clusters/{cluster_id}/backup-retention-policy`.
  Operators can run `POST /v1/scheduler/backup-retention/run-once` to queue
  owned node-agent cleanup commands for successful backups outside the
  retention window while preserving the configured minimum successful backups.
- WAL segment archival is exposed through
  `POST /v1/managed-postgres/clusters/{cluster_id}/wal-archives`. The
  control plane records a durable segment row, derives safe source/archive
  paths from the cluster assignment, queues a Rust node-agent
  `archive_wal_segment` command, and records success or failure on completion.
- Config versions are immutable after deployment. **Implemented for both the
  in-memory service and SQL API: once a `config_versions` row reaches
  `deployed`, later upload attempts for the same config id return conflict
  and leave the rendered hash/status untouched. SQL-backed config history is
  exposed through `GET /v1/configs?environment_id=<env>`, and
  `POST /v1/environments/{environment_id}/configs/rollback` creates a new
  deployed config version from the previous deployed rendered hash so rollback
  is auditable instead of mutating historic rows.**

## 9. Phase 5: Data Plane Gateway

Purpose: provide the hosted public endpoint without embedding platform
concerns in the sync engine.

Deliverables:

- Project/environment routing.
- TLS termination plan and optional mTLS to SyncDeployments.
- Coarse connection and request rate limits.
- gRPC-Web and WebSocket protocol decision.
- Per-environment egress accounting.
- Gateway logs and metrics.
- Gateway crate. **Started in `paas/crates/palimpsest-paas-gateway` with an
  Axum HTTP surface, host-based route resolution, per-environment limiting,
  HTTP proxying to the configured SyncDeployment endpoint, response egress
  accounting, a runnable gateway binary, structured request logs, a
  lightweight `/metrics` endpoint, and buffered `UsageEvent` generation for
  routed egress exposed through an internal drain endpoint. SQL-backed gateway
  route state is now persisted in `gateway_routes`, with scoped
  `POST /v1/gateway-routes`, list, detail, and delete endpoints for hosted
  endpoint discovery and gateway configuration export. The gateway binary can
  now load those routes from `PALIMPSEST_GATEWAY_CONTROL_PLANE_URL`, refresh
  them in place with `PALIMPSEST_GATEWAY_ROUTE_REFRESH_SECS`, replace stale
  in-memory routes after deletion, and use optional bearer auth via
  `PALIMPSEST_GATEWAY_CONTROL_PLANE_TOKEN`, while retaining static route files
  for local development. `mutual_tls_to_sync` routes now carry CA, paired
  client certificate/private-key, and server-name refs in SQL. The gateway
  validates those routes as HTTPS-only, fetches scoped mTLS bundles from
  `GET /v1/gateway-routes/:host/mtls-bundle`, resolves PEM material through the
  control-plane secret backend, and uses rustls client configuration for
  outbound HTTPS to SyncDeployments. `PALIMPSEST_GATEWAY_MTLS_SECRETS` remains
  available for static local route files. Custom domain metadata is now SQL-backed in
  `domains`, with scoped upsert/list/detail/delete routes for dashboard domain
  setup, DNS verification status, TLS status, and environment overview
  aggregation. Public connectivity metadata is now SQL-backed through
  environment-scoped IP allowlist rules and static egress IPs, with scoped
  upsert/list/detail/delete routes for dashboard firewall setup flows.**
- Owned database endpoint proxy. **Started in
  `paas/crates/palimpsest-paas-gateway` with the
  `palimpsest-paas-db-proxy` binary, a JSON route contract, route validation,
  and byte-preserving TCP forwarding for Postgres traffic. Database proxy
  route state is now SQL-backed in `database_proxy_routes`, exposed through
  scoped create/list/detail APIs, derived from environment managed Postgres
  endpoints for customer routes, and loadable by the proxy at startup through
  `PALIMPSEST_DB_PROXY_CONTROL_PLANE_URL`. The proxy now refreshes
  control-plane routes on an interval and updates upstream targets in place for
  new connections on existing listeners. Manual route deletion is exposed
  through `DELETE /v1/database-proxy-routes/:listen_addr`, and the running
  proxy stops listeners removed from SQL-backed discovery on the next refresh.
  It fetches active certificate bundles for TLS routes, handles PostgreSQL
  `SSLRequest`, terminates TLS with rustls, validates the first post-TLS
  PostgreSQL startup packet, enforces route-level allowed user/database policy,
  rejects configured forbidden startup parameters such as `replication` and
  `options`, supports exact required startup parameters, and forwards accepted
  traffic to the managed upstream. Managed Postgres
  certificate-authority providers are
  now durable SQL resources exposed through platform-admin APIs, and endpoint
  certificate issuance resolves through the selected/default CA provider.
  ACME now has an owned order/finalization path plus HTTP-01 challenge-serving
  from the control-plane secret backend; automated ACME directory polling and
  deeper SQL-message inspection remain hardening work.**

Acceptance criteria:

- Clients can connect using hosted endpoint discovery.
- A noisy environment can be limited without affecting another environment.
- Gateway failures are observable separately from sync engine failures.

## 10. Phase 6: Observability, Metering, And Billing

Purpose: make platform operation and customer usage explainable.

Deliverables:

- Internal dashboards for Postgres health, backup health, WAL archive
  failures, sync
  latency, resync reasons, gateway traffic, and deployment status. **Started
  with `paas/observability/dashboards/palimpsest-paas-overview.json`, which
  now covers gateway traffic, managed Postgres lifecycle, storage allocation,
  host storage pressure, backup/WAL/PITR/restore-drill/standby-check
  freshness, quota usage, billing export failures, and node-agent command
  failures.**
- Customer-facing environment health model. **Started with
  `CustomerEnvironmentHealth`, `paas/specs/environment-health.schema.json`,
  `paas/examples/environment-health.degraded.json`, and
  `GET /v1/environments/{environment_id}/health` on the SQL control plane,
  derived from managed Postgres lifecycle, backup, restore-drill, WAL archive,
  host storage, and SyncDeployment state.**
- Signed usage events for database compute, storage, backup storage, WAL
  bytes, active subscriptions, unique canonical queries, egress, and replay
  windows. **Started with the control-plane usage-event schema and SQL-backed
  ingestion path; the gateway can now drain routed egress into shared
  `UsageEvent` records. Usage events now carry optional
  `hmac_sha256_v1` signature metadata; `serve-sql-api` requires and verifies
  it when `PALIMPSEST_PAAS_USAGE_EVENT_SIGNING_KEY_BASE64` is configured, and
  Flyway migration V25 persists the verified signature fields for billing
  export provenance.**
- Quota enforcement points and quota-exceeded errors.
  **Started in the control-plane usage ingestion path for metered events.
  Quota policies can now be listed and retrieved through scoped SQL-backed
  `GET /v1/quota-policies` and `GET /v1/quota-policies/{policy_id}` routes
  for billing/settings pages. Quota alerts are now durable in SQL through
  `quota_alerts`, configurable by threshold basis points, evaluable against
  recent usage windows through `POST /v1/quota-alerts/evaluate`, exposed for
  dashboard filtering through list/detail routes, and included in local smoke
  coverage.**
- Billing export pipeline.
  **Started with filtered SQL-backed usage-event export for billing and
  reconciliation jobs plus durable billing export snapshots in
  `billing_exports` through `POST /v1/billing-exports`. The `local-jsonl`
  destination now writes a durable JSONL artifact and records `delivery_ref`,
  `delivered_at`, and delivery errors in the SQL export row. Billing exports
  can now be listed and retrieved through scoped SQL-backed
  `GET /v1/billing-exports` and `GET /v1/billing-exports/{export_id}` routes.**
- Alert rules for core platform failure modes. **Started with
  `paas/observability/alerts/palimpsest-paas.rules.yaml`, including stale
  managed Postgres backup, PITR, restore-drill, and standby-check alerts.**
- Prometheus metric emitters. **Started with `/metrics` on the SQL-backed
  control plane for node-agent failures, billing export failures, quota usage,
  managed Postgres backup freshness, WAL archive failures, per-cluster
  lifecycle/storage/recoverability timestamps, host storage pressure, and
  firing quota alerts; the gateway also exposes request, rejection,
  rate-limit, and egress counters. Live Postgres runtime diagnostics are now
  durable through `managed_postgres_runtime_checks` and sampled by
  `POST /v1/managed-postgres/clusters/{cluster_id}/runtime-checks/probe`,
  which connects with the managed support role and records connection counts,
  max connections, replication-slot lag bytes, long-running query count,
  blocked lock count, oldest transaction age, and autovacuum activity.**

Acceptance criteria:

- Customer usage can be reconstructed idempotently from usage events.
- Operators can diagnose slot lag, failed backups, storage pressure, backup
  retention drift, and resync storms from dashboards. **Started with scoped
  runtime-check list and detail APIs for slot lag, connection pressure, lock
  pressure, and slow-query pressure, plus retention-policy APIs and an
  operator-triggered retention scheduler.**
- Quota enforcement cannot corrupt sync state.

## 11. Phase 7: Dashboard And Self-Serve Beta

Purpose: remove manual operator steps from the customer journey.

Deliverables:

- Signup, organization, project, and environment flows. **Started with
  `POST /v1/onboarding/workspaces`, which creates an organization, first
  project, first environment, owner team membership, and audit events in one
  SQL transaction for browser signup flows.**
- Managed Postgres creation and status pages. **Started with SQL-backed create,
  lifecycle action routes, and scoped list/get endpoints for dashboard status
  views. Cluster-scoped operation history and node-agent command history are
  now available through SQL-backed list/detail endpoints so dashboards can
  show durable lifecycle progress without direct metadata database access.
  Environment overview aggregation is now exposed through
  `GET /v1/environments/{environment_id}/overview`, combining environment
  health, active database endpoint, managed Postgres clusters,
  SyncDeployments, config history, quota alerts, custom domains, network
  access records, maintenance windows, and active incidents for status pages.
  SQL-backed incident routes now cover scoped upsert/list/detail workflows for
  the first customer-visible status-page backend. Maintenance-window metadata
  now covers scoped upsert/list/detail/delete workflows for customer-editable
  weekly windows and auto-minor-upgrade preference, and
  `POST /v1/scheduler/maintenance/run-once` queues owned Rust node-agent
  minor-update commands for ready clusters only inside active customer
  windows.**
- SQL console and schema browser. **Started with
  `GET /v1/managed-postgres/clusters/{cluster_id}/schema` and
  `POST /v1/managed-postgres/clusters/{cluster_id}/sql-console/query`.
  The first SQL console backend uses `sqlx` to connect to the managed
  PostgreSQL 18+ app role, enforces a single SELECT/WITH statement, runs in a
  read-only transaction with statement timeouts, and caps returned rows. The
  local smoke verifies schema discovery, successful read-only querying, and
  mutation rejection against the live managed PostgreSQL 18 container.**
- Query explorer and permission policy editor. **Started with
  Flyway-managed `query_permission_policies`, a typed
  `QueryPermissionPolicy` model, scoped SQL-backed create/list/detail routes,
  a dry-run endpoint for sample JSON user contexts, and
  `POST /v1/managed-postgres/clusters/{cluster_id}/query-explorer/inspect`,
  which validates read-only SQL and returns canonical SQL plus PostgreSQL
  `EXPLAIN (FORMAT JSON)` estimate metadata without returning row data. The
  local smoke verifies both paths against a live PostgreSQL 18 container.**
- Config deploy history and rollback-to-previous-config action. **Started with
  SQL-backed config history listing and rollback creation endpoints.**
- API key, JWT issuer, and team RBAC screens. **Started at the control-plane
  backend layer with Flyway-managed `team_memberships` and `api_keys` tables,
  typed API-key models, `POST /v1/api-keys`, and
  `POST /v1/api-keys/{key_id}/revoke`. API-key creation returns the plaintext
  token once, stores only a SHA-256 token hash plus prefix, and revocation is
  durable and audited. SQL-backed control-plane routes now accept non-revoked
  `Authorization: Bearer plmp_...` API-key tokens and resolve them to
  `api_key:<key_id>` audit actors; revoked or unknown API-key tokens are
  rejected with HTTP 401. Viewer API keys are treated as read-only and are
  rejected from mutating SQL-backed routes. Tenant-scoped API keys are now
  checked against resolved organization, project, and environment scope before
  SQL-backed mutations run; owner/admin keys can create or revoke API keys only
  within their allowed scope, admin keys cannot grant or revoke owner/admin
  roles, and platform host controls are owner/admin-only. Team memberships now
  have SQL-backed upsert/list routes on `team_memberships` with the same scope
  and role-escalation rules. API keys can be listed by scope for dashboard and
  operator views without returning token hashes or plaintext tokens. JWT issuer
  configuration has begun with Flyway-managed `jwt_issuers`, typed issuer and
  claim-mapping models, scoped create/list/detail routes, admin-only mutation
  authorization, and smoke coverage for an environment-scoped issuer. Webhook
  endpoint configuration has begun with Flyway-managed `webhook_endpoints`,
  typed endpoint models, secret-reference metadata for signing secrets,
  scoped create/list/detail routes, admin-only mutation authorization, and
  smoke coverage for an environment-scoped endpoint. SSO identity-provider
  configuration has begun with Flyway-managed `sso_identity_providers`, typed
  SAML/OIDC provider models, certificate secret-reference metadata,
  organization-scoped create/list/detail routes, admin-only mutation
  authorization, and smoke coverage for an organization-scoped SAML
  provider.**
- Audit logs. **Started with `GET /v1/audit-events`, which returns
  SQL-backed audit events filtered by organization/project/environment scope,
  action, actor, resource, and limit. Viewer API keys can read scoped audit
  history, while tenant-scoped API keys cannot expand outside their scope.
  Flyway migration `V38` makes `audit_events` append-only at the database
  layer by rejecting update, delete, and truncate operations.**
- Billing usage and quota alerts. **Started with SQL-backed billing exports,
  usage-event listing, quota policy CRUD, durable quota-alert thresholds,
  scoped quota-alert list/detail routes, alert evaluation, audit events, and a
  firing-alert Prometheus metric.**

Acceptance criteria:

- A new customer can create managed Postgres, deploy sync, and subscribe from
  a browser without operator help.
- A developer can reproduce the hosted environment locally with the CLI.
- Support can inspect runtime state without exposing row data by default.

## 12. CI And Release Strategy

Start with separate checks for `paas/` so PaaS work does not slow or destabilize
the core workspace:

- Schema validation for `paas/specs`.
  **Started with `paas/ci/validate-json-schemas.rb`, which validates every
  `paas/examples/*.json` payload against the intended local schema and now
  checks dashboard/alert artifacts without requiring external CI dependencies.**
- Local stack smoke test behind an opt-in CI job.
  **Started with the `smoke` job in `.github/workflows/paas.yml`, gated behind
  a manual `workflow_dispatch` input so normal PR checks do not need Docker
  runtime minutes for the full end-to-end path.**
- PaaS service unit tests in their own job.
  **Started with the `rust` job in `.github/workflows/paas.yml`, scoped to the
  PaaS crates and PaaS formatting.**
- Integration tests that provision PostgreSQL 18+ only.
  **Started with the `flyway` job in `.github/workflows/paas.yml`, which
  provisions PostgreSQL 18 through Docker Compose and applies the Flyway
  migrations without cloud credentials.**

Promotion gates:

- A PaaS feature may touch existing crates only after it has a narrow interface
  proposal and tests.
- Shared abstractions can move from `paas/` into `crates/` only when they are
  useful to standalone Palimpsest as well.
- Root CI should remain green without cloud credentials.

## 13. First Pull Request Sequence

1. Add `paas/` docs and schemas.
2. Add local PostgreSQL 18+ Docker assets.
3. Add `palimpsest dev up/down/reset` behind small CLI modules.
4. Add deployment spec rendering for local mode.
5. Add managed Postgres lifecycle model and tests.
6. Add Rust node-agent prototype for local host reconciliation.
7. Add SyncDeployment wrapper prototype.
8. Add control-plane metadata migrations.

This order gives users local value early while preserving the existing engine
and leaving cloud-specific services isolated until their interfaces harden.
