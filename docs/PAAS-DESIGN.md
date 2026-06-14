# Palimpsest PaaS Design

**Status:** Draft
**Scope:** Product and systems design for a managed platform built around
the existing Palimpsest sync engine.

## 1. Summary

Palimpsest today is a deployable sync engine: it tails Postgres logical
replication, incrementally maintains live SQL subscriptions, and streams
diffs to native, WASM, and TypeScript clients. A complete PaaS around it
would turn that engine into a managed developer platform where teams can
create projects, provision Postgres, define sync policy, ship clients, observe
runtime health, scale capacity, and pay for usage without operating
databases, replication slots, pods, certificates, metrics, or upgrades
themselves.

The core product is **managed Postgres with live sync built in**. Customers
should be able to start with a Palimpsest-managed Postgres instance by
default, then run the same shape locally for development. Connecting an
external Postgres database remains useful for migration and advanced
enterprise deployments, but it is not the primary product path and should not
outsource the core database experience.

The platform should not try to become a generic cloud provider first. It
should provide the minimum set of cloud primitives needed to make hosted
Postgres plus Palimpsest reliable, secure, and easy to adopt in production:

1. A hosted control plane for projects, environments, users, API keys, policy,
   billing, and observability.
2. A regional data plane that runs isolated Postgres and Palimpsest workloads
   close to users.
3. A developer workflow made of CLI commands, dashboard flows, SDKs, starter
   templates, and CI-friendly configuration.
4. Managed operations for provisioning, upgrades, failover, incident response,
   audit trails, and compliance evidence.

## 2. Existing Foundation

The repository already contains the pieces that should become the first data
plane runtime:

| Existing component | PaaS role |
| --- | --- |
| `palimpsest serve` | Per-environment sync worker that owns WAL ingest, query execution, routing, and gRPC streaming. |
| `palimpsest-cli` | Local development and future cloud CLI entry point. |
| `palimpsest-server` | Embeddable data-plane service wrapped by the managed runtime. |
| `palimpsest-wal` | Postgres connector and logical replication implementation used by hosted and local databases. |
| `palimpsest-permissions` | Server-side row authorization layer. |
| `palimpsest-client`, `palimpsest-client-js`, TypeScript package | Client SDK foundation. |
| `deploy/helm/palimpsest` | Standalone Kubernetes deployment path for self-hosted Palimpsest, not the managed PaaS runtime. |
| Operational docs | Starting point for managed runbooks, support playbooks, and customer-facing guidance. |

The PaaS should keep the open-source engine usable standalone. Cloud-specific
behavior belongs in additional services, wrappers, deployment manifests, and
configuration APIs, not in hard-coded assumptions inside the core crates.
The repo integration plan lives in
[`paas/IMPLEMENTATION-PLAN.md`](../paas/IMPLEMENTATION-PLAN.md).
The managed Postgres runtime design lives in
[`paas/MANAGED-POSTGRES-DESIGN.md`](../paas/MANAGED-POSTGRES-DESIGN.md).

## 3. Goals And Non-Goals

### Goals

1. Let a developer create a new managed Postgres database with Palimpsest sync
   enabled in under 5 minutes.
2. Let a developer connect an existing Postgres database and subscribe from a
   browser in under 15 minutes.
3. Provide production-grade isolation, reliability, observability, and
   upgrade management without requiring customers to run Postgres or Palimpsest
   themselves.
4. Support multiple projects, environments, regions, organizations, roles,
   and API credentials.
5. Make sync behavior configurable as code: publications, query limits,
   database settings, permissions, JWT settings, domains, and client tokens
   should be reviewable and reproducible.
6. Price and meter the product using dimensions customers can understand:
   database compute, storage, backups, unique canonical queries, active
   subscriptions, egress, WAL volume, and retention windows.
7. Preserve a clean path from local development to hosted deployment.

### Non-Goals For The First PaaS Release

- Hosting arbitrary application code.
- Multi-database joins or cross-customer federation.
- A general event streaming product.
- Custom edge compute or user-defined serverless functions.
- Hard real-time guarantees beyond documented sync SLOs.

## 4. Product Surface

### Dashboard

The dashboard is the primary operator UI. It should include:

- Organization, project, and environment management.
  The first self-serve backend increment exposes a single onboarding workspace
  API that creates the organization, first project, first environment, owner
  team membership, and audit events transactionally.
- Managed Postgres creation, resize, pause, restore, and delete flows.
  Pause/resume is backed by Rust-owned node-agent start/stop commands and
  durable operation history, not by an external orchestrator.
  Delete flows should expose final-backup intent for ready clusters so
  destructive removal can preserve a recoverable artifact before stop/delete
  commands run.
- Database connection setup for import and external database deployments.
- Guided publication and replication slot validation.
- Schema browser, migration history, backup status, PITR controls, and
  read-only SQL console.
  The first backend increment exposes scoped schema discovery and read-only
  query execution on managed PostgreSQL 18+ clusters through the Rust control
  plane. Queries are limited to one SELECT/WITH statement and execute through
  `sqlx` in a read-only transaction.
- Permission editor covering two models: a per-environment
  `palimpsest-permissions` Rule DSL document edited in a code editor and run
  through a verifier that compiles the rules against a catalog (a built-in demo
  catalog or a live cluster schema) before activation, and per-table query
  permission policies with dry-run against sample user contexts.
- Query explorer that shows canonical form, explain output, sharing behavior,
  current result size, and estimated cost.
- API-key, JWT issuer, and team access configuration backed by scoped control
  plane routes so dashboard screens do not need direct metadata database access.
- Webhook endpoint configuration backed by scoped control-plane routes and
  secret-reference metadata for signing secrets.
- Organization-scoped SSO identity-provider configuration backed by
  control-plane routes and secret-reference metadata for SAML certificates.
  The first backend increment stores query permission policies in the SQL
  control plane, supports scoped list/detail and dry-run APIs, and exposes a
  query-inspection endpoint that returns canonical SQL plus PostgreSQL
  `EXPLAIN (FORMAT JSON)` cost metadata without returning row data.
  A separate per-environment Rule DSL document (`permission_rule_documents`)
  holds the `palimpsest-permissions` TOML config; `POST /v1/permissions/verify`
  compiles it with `palimpsest_permissions::{parse_config, compile_rules}`
  against a catalog and reports per-rule compilation, user-context fields, and
  tautology elision. Verification is currently an explicit author-time check
  rather than a save-time gate.
- Live subscriptions view with lag, fanout, resync reasons, and slow clients.
- Metrics, logs, traces, incidents, deploy history, and audit events.
  Managed Postgres status views should include durable operation history and
  node-agent command history so provisioning, backup, restore, resize, and
  deletion progress can be inspected without exposing customer row data.
  The first dashboard overview API aggregates environment health, active
  database endpoint, managed Postgres clusters, SyncDeployments, config
  history, quota alerts, custom domain records, network access records,
  maintenance windows, and active incident records so status pages do not need
  direct metadata database access.
  Internal operator views should include node-host inventory, capacity, state,
  region, and failure-domain filters so draining and maintenance decisions are
  made through the control plane rather than direct metadata database access.
  Node-host hardening evidence should also be exposed through platform-admin
  APIs so image, OS, kernel, PostgreSQL 18+ support floor, runtime, encryption,
  firewall, unattended-upgrade, and patch posture are visible without direct
  database access. The Rust node agent now includes a `hardening-check`
  command that scans local signals and records the result through that API.
  SyncDeployment status views should be backed by scoped list/detail APIs so
  deploy history can be shown without operator database access.
  Audit views should use organization/project/environment-scoped APIs rather
  than direct metadata database access.
- API key, JWT issuer, webhook, team role, and SSO configuration.
- Billing usage, plan limits, invoices, and quota alerts.
  Billing and quota pages should read from scoped quota-policy and
  billing-export APIs rather than direct metadata database access. Quota-alert
  state should also be read through scoped list/detail APIs and evaluated from
  usage windows by the control plane.

### CLI

The existing `palimpsest` CLI should gain cloud commands:

```sh
palimpsest login
palimpsest projects create <name>
palimpsest env create <project>/<env>
palimpsest db create <project>/<env> --region us-west-2 --tier starter
palimpsest db connect <project>/<env>
palimpsest db psql <project>/<env>
palimpsest db backups <project>/<env>
palimpsest config pull <project>/<env>
palimpsest config push <project>/<env>
palimpsest permissions validate permissions.toml
palimpsest deploy <project>/<env>
palimpsest logs <project>/<env>
palimpsest metrics <project>/<env>
palimpsest dev up
palimpsest dev down
palimpsest tunnel <project>/<env>
```

The CLI should support non-interactive CI use through service tokens and
machine-readable output.

`palimpsest dev up` should start a local Postgres container with logical
replication enabled, create a publication and replication slot, run the
Palimpsest sync engine, and print browser/client connection settings. The
local stack should use the same config file shape as hosted environments so
teams can promote changes without translating concepts.

### SDKs

The current client packages become the base SDKs. The hosted platform should
add:

- Cloud endpoint discovery by project/environment.
- Token refresh hooks and short-lived client credentials.
- Browser-safe configuration loading.
- React integration for auth state, reconnect state, and offline resync.
- Diagnostics hooks that can report client version, subscription count,
  reconnect reasons, and last acknowledged LSN back to the platform.

## 5. Tenancy Model

The control plane owns the durable resource hierarchy:

```
Organization
  Project
    Environment
      ManagedPostgresCluster
      ExternalDatabaseConnection
      SyncDeployment
      PermissionPolicy
      ApiKey / JwtIssuer
      Domain
      UsageMeter
```

Recommended environment names are `dev`, `staging`, and `prod`, but the
model should allow arbitrary names.

### Isolation

The default isolation unit is one managed Postgres cluster plus one
`SyncDeployment` per customer environment. Each environment owns:

- Its Postgres primary, storage volume, backup policy, and database users.
- Its Palimpsest worker group.
- Its replication slot or failover slot pair.
- Its secrets.
- Its metrics labels.
- Its rate limits and quotas.

For the first hosted release, avoid placing multiple customer environments in
the same Postgres cluster or Palimpsest process. Shared infrastructure can
exist below the workload boundary, but customer data, WAL streams, secrets,
memory accounting, permissions auditing, and incident blast radius should be
isolated by environment.

## 6. High-Level Architecture

```
                         ┌────────────────────────────┐
                         │        Dashboard / CLI      │
                         └──────────────┬─────────────┘
                                        │
                                        ▼
┌────────────────────────────────────────────────────────────────────┐
│                         Control Plane                              │
│                                                                    │
│  API Gateway  ──▶  Tenant API  ──▶  Metadata DB                    │
│                      │                                             │
│                      ├──▶ Config compiler                          │
│                      ├──▶ Provisioner                              │
│                      ├──▶ Postgres fleet manager                   │
│                      ├──▶ Billing + usage pipeline                 │
│                      ├──▶ Audit log                                │
│                      └──▶ Support / incident tooling               │
└──────────────────────────┬─────────────────────────────────────────┘
                           │ desired state
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│                       Regional Data Plane                          │
│                                                                    │
│  Ingress / TLS / WAF                                               │
│        │                                                           │
│        ▼                                                           │
│  Connection Gateway ──▶ Palimpsest SyncDeployment(s)               │
│        │                    │                                      │
│        │                    ├── metrics / logs / traces            │
│        │                    ├── slot supervisor                    │
│        │                    └── config sidecar                     │
│        │                    │                                      │
│        │                    ▼                                      │
│        │              Managed Postgres Cluster                     │
│        │                    ├── primary / future replicas          │
│        │                    ├── backups / PITR                     │
│        │                    └── storage / WAL archive              │
│        ▼                                                           │
│  Browser / native clients                                          │
└────────────────────────────────────────────────────────────────────┘
```

### Control Plane

The control plane is globally reachable and region-aware. It does not sit on
the hot path for subscription diffs. It manages desired state, credentials,
configuration, usage collection, billing, audit records, and support
workflows.

Core services:

- **Public API:** REST or gRPC API used by dashboard, CLI, CI, and support
  tooling.
- **Metadata store:** Durable Postgres database for tenants, environments,
  config versions, deploy history, quotas, and audit metadata.
- **Config compiler:** Validates customer config, compiles permission rules,
  checks query limits, and emits signed deployment specs.
- **Provisioner:** Reconciles desired state into each regional data plane.
- **Postgres fleet manager:** Rust service that provisions clusters, applies
  version upgrades, coordinates backups and restores, manages database users,
  and exposes safe operational actions to the dashboard and CLI.
- **Secret manager integration:** Stores database URLs, JWT secrets,
  customer-managed keys, and webhook secrets.
- **Usage pipeline:** Ingests metering events and produces billable usage.
- **Audit service:** Records human and machine actions with immutable
  retention.

### Data Plane

The data plane serves customer traffic and runs customer databases close to
users. It should be deployable per region and should be able to continue
serving existing subscriptions during short control-plane outages.

Core services:

- **Connection gateway:** Terminates public gRPC-Web/WebSocket/HTTP
  connections, validates project routing, enforces coarse rate limits, and
  forwards traffic to the right SyncDeployment.
- **Managed Postgres runtime:** The CloudNativePG operator on Kubernetes. The
  control plane renders desired state into CloudNativePG `Cluster` resources
  (via `palimpsest-paas-runtime`) and applies them; the operator owns
  placement, failover, backups, PITR, upgrades, and storage resize.
- **SyncDeployment:** A managed Palimpsest runtime built around
  `palimpsest-server` and `palimpsest-wal`.
- **Managed Postgres cluster:** Customer-dedicated database instance with logical
  replication preconfigured for Palimpsest, encrypted storage, backups,
  version management, and guarded administrative access.
- **Slot supervisor:** Creates, validates, advances, and repairs replication
  slots according to policy.
- **Backup and restore controller:** Coordinates base backups, WAL archiving,
  point-in-time restore, clone creation, and restore drills.
- **Config sidecar:** Watches signed config versions and applies safe runtime
  changes without full restarts where possible.
- **Telemetry agent:** Ships metrics, logs, traces, profiles, and health
  events to the platform observability stack.
- **Egress controller:** Measures customer egress and enforces bandwidth
  quotas.

## 7. Provisioning Flow

### Create Managed Postgres

This is the default path for new projects:

1. Customer chooses project, environment, region, database tier, and supported
   PostgreSQL version. Managed clusters support PostgreSQL 18 and newer only.
2. Platform provisions an isolated Postgres cluster with encrypted storage.
3. Platform enables logical replication, creates the Palimpsest publication,
   creates the replication slot, and creates scoped database roles with
   rotatable owned secrets.
4. Platform deploys the SyncDeployment in the same region.
5. Platform runs a smoke test against Postgres and the sync endpoint.
6. Dashboard opens the SQL console and query explorer.
7. SDK endpoint, database connection strings, and client configuration are
   generated.
8. Backups, PITR, telemetry, quotas, and alerts are enabled by default.

The managed database should be ordinary Postgres from the customer's point of
view. Customers should be able to connect with `psql`, run migrations from
their existing tools, export data, and leave without a proprietary data
format.

### Connect Existing Postgres

1. Customer enters a database URL or installs a private connectivity agent.
2. Platform verifies network reachability from the chosen region.
3. Platform checks `wal_level`, publication, role privileges,
   `max_replication_slots`, and `max_wal_senders`.
4. Platform offers SQL snippets for missing prerequisites.
5. Customer confirms publication scope.
6. Platform creates or validates the replication slot.
7. Platform deploys the SyncDeployment.
8. Query explorer subscribes to a smoke-test query.
9. SDK endpoint and client configuration are generated.

This path exists for migration, enterprise networking constraints, and
customers with an existing database estate. It is not the preferred first-run
experience.

### Local Development

Local development should mirror the managed path:

1. `palimpsest dev up` creates a Docker network.
2. It starts a PostgreSQL 18+ image with `wal_level=logical`, a development
   database, a publication, and a replication slot.
3. It starts `palimpsest serve` pointed at that database.
4. It writes a generated local config file and `.env` values for the SDK.
5. It optionally runs seed SQL and migrations from configured paths.
6. `palimpsest dev down` stops the stack, and `palimpsest dev reset` drops
   local state and recreates it.

The local workflow should not require a cloud account. A cloud login is only
needed for deploys, tunnels, remote logs, and managed backups.

### Private Connectivity

The first release can support public IP allowlists and TLS. Enterprise
customers will need private connectivity:

- AWS PrivateLink, GCP Private Service Connect, and Azure Private Link.
- Customer-installed outbound agent for databases that cannot accept inbound
  traffic.
- Static egress IPs per region for firewall allowlists.

## 8. Managed Postgres Product

Managed Postgres is part of the core platform, not an integration. The
platform owns the customer database lifecycle end to end through a Rust control
plane that persists desired state and policy. The managed runtime is Kubernetes
with the CloudNativePG operator: the control plane renders desired state into
CloudNativePG `Cluster` resources and applies them, and the platform ships as a
Helm chart. See [ADR 0003](../paas/adr/0003-kubernetes-cloudnativepg-runtime.md).
(This reverses the original no-Kubernetes decision; sections below that mention
the Rust node agent and host fleet are historical record.)

- Provision Postgres clusters in supported regions.
- Support PostgreSQL 18 and newer only for managed clusters.
- Configure logical replication for Palimpsest automatically.
- Create least-privilege roles for application traffic, migrations,
  replication, and support access.
- Provide connection strings for application servers, migrations, `psql`, and
  local tooling.
- Run automated backups and WAL archiving with point-in-time restore.
- Prepare physical standbys from managed backups with `primary_conninfo`
  rendered from the owned replication role credential, not the `postgres`
  superuser.
- Rotate app, migration, replication, and support credentials by staging
  rotation-scoped pending secret refs, applying them through the Rust node
  agent, and promoting canonical refs only after successful completion.
- Support database clones for development and staging. Cross-environment
  restore requests now require an active source-environment clone redaction
  policy. The node agent writes the selected policy and generated redaction
  SQL into the clone directory, starts the restored clone on its assigned
  port, applies null/static/hash rules, writes an applied marker, and stops
  the clone before reporting restore success.
- Apply minor version updates through controlled maintenance windows.
  The first backend increment exposes an update-minor API that stays within
  the current PostgreSQL major version, records the target PostgreSQL 18+
  version, and drives an owned Rust node-agent command. Environment-scoped
  maintenance-window metadata is also persisted for dashboard policy editing,
  and the owned control plane can now run an idempotent maintenance scheduler
  that queues those same minor-update commands only inside active
  auto-minor-upgrade windows.
- Provide a documented major-version upgrade path. The first implementation
  adds a durable major-upgrade API and metadata table, validates PostgreSQL
  18+ targets with a greater major than the active cluster, and queues an
  owned node-agent `upgrade_postgres_major` command for the selected strategy.
  New commands include the assigned source port, allowing the Rust node agent
  to render a local plan and run source SQL preflight checks before recording
  the upgrade artifact.
- Expose resize operations for CPU, memory, and storage.
- Monitor vacuum health, bloat, locks, slow queries, connection saturation,
  storage growth, replication lag, and backup freshness.
- Support export and deletion workflows so customers are not locked into a
  proprietary data format.

The first implementation should prefer operationally boring Postgres over a
large feature surface. Required day-one features are PostgreSQL 18+ dedicated
clusters, encrypted storage, backups, PITR, role management, logical
replication, basic extensions, metrics, logs, and restore. Read replicas,
branching, automatic query tuning, custom extensions, and multi-region
database replication can arrive after the managed sync path is stable.

Local Postgres should be treated as the developer edition of the same
product. It should use an ordinary PostgreSQL 18+ image plus generated
config, not a mock database or special in-memory mode.

## 9. Configuration As Code

Hosted environments should be reproducible from a versioned config bundle:

```toml
[project]
name = "acme"

[environment]
name = "prod"
region = "us-west-2"

[database]
mode = "managed"
postgres_version = "18"
tier = "starter"
storage_gib = 50
publication = "palimpsest_pub"
slot_name = "palimpsest_acme_prod"

[database.backups]
pitr_window = "7d"
daily_backup_retention = "14d"

[sync]
compaction_window = "30s"
max_subscriptions_per_connection = 256

[auth.jwt]
issuer = "https://auth.acme.example"
audience = "palimpsest"
jwks_url = "https://auth.acme.example/.well-known/jwks.json"

[limits]
max_unique_canonical_queries = 10000
max_active_subscriptions = 250000
max_egress_mib_per_minute = 8192
```

The SQL-backed control plane keeps config versions append-friendly until a
version is deployed, then treats that deployed row as immutable. Environment
history is exposed by listing config versions for an environment, and rollback
creates a new deployed config version from the previous deployed rendered hash
so deploy history remains auditable.

Managed and local deployments reject `postgres_version` values below `18`.
Existing-database deployments use the same shape with
`database.mode = "external"` and a `connection_ref` secret. External database
support can validate older Postgres versions for migration paths, but
Palimpsest-managed Postgres is PostgreSQL 18+ only. Local deployments use
`database.mode = "local"` and can add seed and migration paths.

The config compiler should produce a deployable spec plus diagnostics:

- Validation errors that block deployment.
- Warnings for expensive query shapes or unsafe limits.
- Diff against the active config.
- Postgres changes that require maintenance windows or restart.
- Required restart or hot-reload classification.
- Estimated cost impact.

## 10. Security Model

### Identity And Access

Platform users authenticate to the control plane through email/password,
OAuth, SAML SSO, or SCIM-provisioned identities. Authorization is RBAC with
organization, project, and environment scopes.

Suggested roles:

| Role | Capabilities |
| --- | --- |
| Owner | Billing, org settings, SSO, delete org. |
| Admin | Manage projects, environments, members, and secrets. |
| Operator | Deploy config, view metrics, rotate runtime credentials. |
| Developer | Read config, use query explorer, create dev environments. |
| Viewer | Read-only access to dashboard, metrics, and audit logs. |

Client applications authenticate to the data plane through customer JWTs,
short-lived platform-issued tokens, or server-side API keys depending on the
deployment model.

The first control-plane implementation starts the self-serve access backend
with durable `team_memberships` and `api_keys` tables. API-key creation returns
the plaintext token once, stores only a token prefix plus SHA-256 hash, and
revocation is represented by `revoked_at` plus an audit event. SQL-backed
control-plane routes accept non-revoked `Authorization: Bearer plmp_...`
API-key tokens, resolve them to `api_key:<key_id>` audit actors, and reject
revoked or unknown API-key tokens. Viewer keys are read-only and cannot call
mutating control-plane routes. Tenant-scoped keys are enforced against
organization, project, and environment resource scopes; owner/admin keys can
create and revoke keys only inside their allowed scope, and admin keys cannot
grant or revoke owner/admin roles. The same backend now exposes durable team
membership upsert and list routes backed by `team_memberships`. API-key list
responses expose key metadata and token prefixes only, never token hashes or
plaintext tokens.

JWT issuer configuration is also a durable control-plane resource. The first
implementation stores issuer URL, audience, JWKS URL, status, and claim-to-user
field mappings in `jwt_issuers`; owner/admin API-key actors can upsert scoped
issuers, and read routes expose scoped list/detail views for future dashboard
screens.

Webhook endpoint configuration follows the same pattern. The first
implementation stores endpoint URL, event type filters, status, and a
secret-manager reference for the signing secret in `webhook_endpoints`; read
routes expose scoped list/detail views without exposing secret material.

SSO identity-provider configuration starts as durable organization metadata.
The first implementation stores SAML/OIDC kind, issuer, SSO URL,
claim-to-user-field mappings, status, and a secret-manager reference for
certificate material in `sso_identity_providers`; read routes expose scoped
list/detail views without returning the underlying certificate bytes.

Managed Postgres status pages are backed by scoped cluster list and detail
routes that expose lifecycle state, assignment, version, tier, and storage
metadata without requiring direct SQL access to the metadata database.
Backup history pages use scoped per-cluster backup list and detail endpoints
that expose backup lifecycle state, artifact location, and errors without
exposing database contents. Per-backup artifact endpoints add provider,
object URI, manifest path/hash, size, lifecycle status, and error metadata so
the platform can move from local filesystem artifacts to owned object storage
without changing the customer-facing backup surface. The node agent can now
stage completed base backups into an owned filesystem-backed S3-compatible
object-store layout and report both local and `s3://` artifacts to the control
plane. It can also PUT every completed backup file to an owned path-style HTTP
or HTTPS S3-compatible endpoint behind the same artifact contract, including
optional bearer/API-token headers for owned object-store frontends. HTTPS
object-store frontends are supported when operators configure a trusted CA
bundle and, where needed, a TLS server-name override. S3-compatible frontends
that require AWS Signature Version 4 can be signed directly by the node agent;
non-SigV4 provider-specific request signing remains the production
object-store adapter boundary.
WAL archive history pages expose archived segment names, lifecycle state,
archive location, and errors through scoped per-cluster list and detail
endpoints.
Managed database endpoint certificates are issued through the owned CA
provider registry. Local-dev and external-PKI issuers can activate
certificates immediately; ACME issuers create a durable order with generated
key, CSR, HTTP-01 token, and key-authorization secret refs. The control plane
serves pending HTTP-01 challenges at `/.well-known/acme-challenge/{token}` and
requires an owned challenge-validation step to move orders to
`ready_to_finalize` before it activates finalized PEM chains that match the
generated private key. Invalid finalization material fails both the ACME order
and the provisioning certificate so operators do not see a stuck pending
certificate. A run-once ACME order scheduler and optional background interval
advance pending orders through the same owned validation transition.
Restore history pages use the same scoped model for clone/PITR restore
requests, including source cluster, target cluster, backup id, lifecycle state,
recovery target, and error metadata.
Restore drill history is tracked separately from customer restore requests and
links each drill to the source cluster, backup, restore record, isolated target
cluster, lifecycle state, and error metadata.
PITR check history tracks metadata continuity validation across the latest
successful base backup and succeeded WAL archive sequence, so operators can
distinguish missing archive continuity from restore-artifact materialization
failures.
Failover history tracks source cluster, promoted target cluster, lifecycle
state, command outcome, and errors so HA events become auditable operations
instead of out-of-band host actions.
Standby history tracks source cluster, target cluster, seed backup, lifecycle
state, command outcome, and errors so HA preparation is visible before a
failover is requested.

### Secrets

Secrets should never be stored in the metadata database as plaintext. Use a
cloud KMS-backed secret store and keep only references plus encrypted
metadata in the control plane. Support customer-managed encryption keys for
enterprise plans. Envelope/KMS key refs are tracked in the owned
`secret_encryption_keys` registry with active, retiring, and retired states so
rotation state is durable and auditable before rewrap execution. Key-to-key
rewrap intent is tracked in `secret_rewrap_plans`, including wrapped-secret
counts and lifecycle status. The SQL control plane can run a plan by opening
matching secret refs with the configured owned backend, resealing them to the
target key ref, and recording succeeded or failed status. Local development can
use plaintext `local-dev`; owned deployments can use environment-delivered or
mounted-file envelope keyrings so generated database passwords and certificate
material stay encrypted in Postgres while KMS delivery remains outside the
request path.

### Network Security

- TLS required for all public endpoints.
- Optional mTLS between gateway and SyncDeployments. Gateway route state stores
  the SyncDeployment CA ref, optional paired gateway client certificate and
  private-key refs, and an optional server-name override. Owned gateway
  processes fetch scoped mTLS bundles from the control plane, open PEM material
  through the configured secret backend, and use rustls for outbound HTTPS,
  while normal route discovery continues to expose refs rather than private key
  material.
- Per-environment firewall rules and static egress IPs.
- Managed Postgres DB proxy routes terminate customer TLS after PostgreSQL
  `SSLRequest`, reject nested SSL negotiation or malformed startup packets
  inside TLS, enforce route-level allowed user/database policy on startup
  packets, reject forbidden startup parameters such as `replication` and
  `options`, enforce required startup parameters when configured, and only
  forward accepted PostgreSQL startup/cancel packets to the managed upstream.
- WAF and DDoS controls in front of the connection gateway.
- Strict separation between control-plane APIs and data-plane streaming
  endpoints.

### Audit And Compliance

Audit events should cover:

- Login and SSO activity.
- Project, environment, config, and secret changes.
- Database creation, resize, restore, deletion, connection tests, and slot
  mutations.
- Permission policy changes.
- API key creation, use, and revocation.
- Support access and break-glass actions.
- Billing and plan changes.

Break-glass support access is modeled as a cluster-scoped support-access
session. Requests require a reason, optional ticket reference, bounded expiry,
and explicit approval before a future support tool can use the session to
resolve support credentials; revocation is durable and audited separately.
Audit history is append-only at the database layer through triggers that
reject updates, deletes, and truncation of `audit_events`.

Compliance-ready operation requires retention policy controls, exportable
audit logs, vulnerability management, dependency scanning, access reviews,
backup restore drills, database deletion guarantees, and documented incident
response. Host vulnerability management should be backed by durable hardening
check records and, later, Rust node-agent scanners that report the same shape
automatically.

## 11. Reliability And Scaling

### SLOs

Initial public SLOs should be conservative:

| Dimension | Target |
| --- | --- |
| Control-plane API availability | 99.9% monthly |
| Data-plane connection availability | 99.95% monthly per region |
| Managed Postgres availability | 99.9% monthly for starter, higher for HA tiers |
| P50 commit-to-client latency | < 100 ms for small transactions in-region |
| P99 commit-to-client latency | < 1 s for small transactions in-region |
| RPO for platform metadata | < 5 minutes |
| RPO for managed Postgres | PITR window dependent; target < 5 minutes on paid tiers |
| RTO for regional data-plane workload recreation | < 30 minutes |
| RTO for managed Postgres restore | Tier dependent; target < 30 minutes for standard tiers |

Latency SLOs should exclude upstream database lag, customer network issues,
and client-side backpressure, but the dashboard must expose those causes
clearly.

### Scaling Model

The first release should scale each environment vertically and by query shard,
matching the existing architecture direction:

- One managed Postgres primary per environment.
- One WAL ingest path per database publication/slot.
- Query shards partitioned by canonical query key.
- Per-shard compaction and memory accounting.
- Gateway routing by environment and subscription key.
- Autoscaling triggered by WAL lag, CPU, active subscriptions, unique
  canonical keys, diff queue depth, and egress.

Customer-visible quotas should map to actual bottlenecks:

- Database CPU, memory, and storage tier.
- Database connections.
- Backup retention and PITR window.
- Active client connections.
- Active subscriptions.
- Unique canonical queries.
- Rows materialized per environment.
- WAL bytes decoded per minute.
- Diff messages and egress bytes per minute.
- Permission policy size and query complexity.

### High Availability

Recommended HA progression:

1. **MVP:** Single SyncDeployment per environment with fast restart and
   client resync; single managed Postgres primary with automated backups and
   PITR.
2. **Database HA tier:** Managed Postgres standby, automated failover, and
   clear maintenance windows.
3. **Warm sync standby:** Secondary Palimpsest deployment validates config and
   can take over
   with a failover slot or recreated slot.
4. **Shard-level HA:** Query shards can restart independently.
5. **Regional failover:** Customer opts into another region with documented
   RPO/RTO and database connectivity requirements.

Because Palimpsest can recover by resyncing clients, early HA should optimize
for correctness and fast recovery over complex active-active replication.

## 12. Observability

The platform needs two observability views: internal operator telemetry and
customer-facing product telemetry.

### Internal Signals

- Postgres CPU, memory, storage, connections, locks, slow queries, and
  autovacuum health.
  The first runtime-check API samples live PostgreSQL state through the
  managed support role and persists connection pressure, replication-slot lag
  bytes, long-running query count, blocked locks, oldest transaction age, and
  autovacuum activity for scoped dashboard reads.
- Backup success, WAL archive freshness, restore test age, and PITR window.
- WAL lag bytes and time.
- Slot health and feedback LSN.
- Commit-to-diff latency.
- Gateway connection counts and errors.
- SyncDeployment CPU, memory, restarts, and queue depth.
- Resync counts by reason.
- Query compilation failures.
- Permission rewrite failures.
- Per-shard compaction frontier.
- Egress and message throughput.

### Customer-Facing Signals

- Environment health.
- Managed Postgres health, storage growth, backup status, and upcoming
  maintenance.
- Current database replication status.
- Active connections and subscriptions.
- Unique canonical query count.
- Slow or expensive queries.
- Client reconnect and resync rates.
- Estimated billable usage.
- Recent deploys and config changes.
- Actionable alerts with remediation steps.

## 13. Billing And Metering

Billing should be based on metered platform cost drivers while avoiding
surprises. Suggested dimensions:

- Base fee per project or production environment.
- Managed Postgres compute tier.
- Database storage.
- Backup storage and PITR retention.
- Optional standby or HA tier.
- Compute tier for SyncDeployment capacity.
- Active subscriptions averaged over time.
- Unique canonical queries above plan allowance.
- WAL bytes decoded.
- Egress bytes delivered to clients.
- Retention or replay window size.
- Private connectivity and dedicated region add-ons.

Usage events should be generated in the data plane, buffered locally during
control-plane outages, signed with the owned `hmac_sha256_v1` usage-event
contract, and uploaded to the usage pipeline with idempotency keys. The SQL
control plane can require signature verification by configuration and stores
signature metadata with each durable usage event for billing provenance.

Quota alerts are threshold rules over quota policies. The control plane should
evaluate them from durable usage events, persist `ok` or `firing` state, emit
operator metrics, and expose scoped APIs for billing and dashboard pages.

## 14. Developer Experience

The platform should make local and hosted workflows feel like the same
system:

- `palimpsest dev up` starts local Postgres plus Palimpsest with logical
  replication preconfigured.
- `palimpsest serve` remains available for advanced local engine work.
- `palimpsest deploy` promotes validated config to cloud.
- `palimpsest db psql` opens a shell to the managed or local database.
- `palimpsest db clone prod dev` creates development data from a managed
  backup subject to access controls and redaction policy.
- Starter apps include React, TypeScript, auth provider examples, and CI.
- The dashboard query explorer can export working client snippets.
- Errors include stable codes, remediation text, and links to docs.
- A local tunnel can connect a cloud environment to a developer database for
  testing without exposing the database publicly.

Local mode should be boring: Docker Compose or an equivalent generated stack,
ordinary Postgres images, mounted volumes for persistence, optional seed SQL,
and clear reset semantics. The goal is to let users build against the same
replication and sync behavior they will get in production, not a mock sync
engine.

## 15. Data Model

Minimum metadata tables:

| Table | Purpose |
| --- | --- |
| `organizations` | Tenant boundary and billing owner. |
| `users` | Human identities. |
| `memberships` | User-to-org roles. |
| `projects` | Product/application grouping. |
| `environments` | Deployable runtime scope. |
| `managed_postgres_clusters` | Hosted database instances, region, tier, version, storage, lifecycle state. |
| `managed_postgres_endpoints` | Environment active-cluster target, stable database proxy listen address, active certificate, and failover cutover metadata. |
| `managed_postgres_endpoint_certificates` | Managed Postgres endpoint certificate lifecycle, validity, fingerprint, and secret refs for certificate/private-key material. |
| `managed_postgres_certificate_authority_providers` | Owned CA issuer registry for managed Postgres endpoint certificates, including default issuer selection. |
| `managed_postgres_acme_orders` | Owned ACME order lifecycle for endpoint certificates, including CSR refs, HTTP-01 challenge tokens, key authorization refs, and finalization status. |
| `managed_postgres_deletion_tombstones` | Deleted cluster IDs, scope, retained backup, retention expiry, and unrecoverable timestamps. |
| `managed_postgres_backup_retention_policies` | Active-cluster backup expiry policy, retention days, and minimum successful backup count to preserve. |
| `managed_postgres_backup_artifacts` | Provider/object-store artifact catalog for base backup manifests, object URIs, sizes, lifecycle status, and errors. |
| `managed_postgres_major_upgrades` | Auditable PostgreSQL major-upgrade requests, target version, strategy, command, operation, status, and completion metadata. |
| `managed_postgres_role_credential_rotations` | Durable role credential rotation attempts, command binding, pending secret prefix, status, and applied/failure metadata. |
| `managed_postgres_clone_redaction_policies` | Environment-scoped redaction rules required for production-to-development clone restores. |
| `managed_postgres_support_access_sessions` | Break-glass support-access requests, approvals, revocations, expiry, reason, and ticket reference by cluster. |
| `secret_encryption_keys` | Owned envelope/KMS key registry for active, retiring, and retired secret-encryption keys. |
| `secret_rewrap_plans` | Auditable secret-envelope key-rotation plans, wrapped-secret counts, and run status. |
| `database_connections` | External database metadata and secret references. |
| `database_roles` | Generated application, migration, support, and replication roles. |
| `backup_policies` | Backup cadence, retention, PITR window, and restore-test policy. |
| `backups` | Backup artifacts, WAL archive ranges, restore metadata, and lifecycle state. |
| `restore_drills` | Scheduled restore validation runs linked to backups and isolated target clusters. |
| `pitr_checks` | Recovery-window continuity checks over base backups and archived WAL segments. |
| `failovers` | Source local-stop fencing, standby promotion operations, and lifecycle state. |
| `standbys` | Source-to-target standby preparation operations, seed backup metadata, physical slot setup, hot-standby startup, lag checks, and promotion eligibility. |
| `sync_deployments` | Runtime instances and desired state. |
| `config_versions` | Versioned config bundle, validation result, deployment status. |
| `node_host_agent_credentials` | Per-host node-agent signing credentials, rotation/revocation state, encrypted secret refs, and last-used metadata. |
| `node_host_hardening_checks` | Per-host hardening evidence for image, OS, kernel, PostgreSQL 18+ support floor, runtime, disk encryption, firewall, unattended upgrades, and patch freshness. |
| `node_host_observed_clusters` | Per-host observed managed Postgres clusters (data directory, running state) reported on node-agent heartbeat for desired-vs-observed drift detection. |
| `node_host_observed_sync_deployments` | Per-host observed SyncDeployments and running state reported on node-agent heartbeat. |
| `permission_rule_documents` | One per-environment `palimpsest-permissions` DSL (TOML) document authored in the UI and compiled by the verifier against a catalog. |
| `query_permission_policies` | Per-table read/subscribe predicate policies with draft/active status and sample-context dry-run. |
| `api_keys` | Control-plane and server-side keys. |
| `jwt_issuers` | Accepted client auth issuers. |
| `gateway_routes` | Hosted sync endpoint hostnames, upstreams, TLS policy, and per-environment limits. |
| `database_proxy_routes` | Customer database endpoint listeners, active upstream addresses derived from managed endpoint state, and tenant scope. |
| `domains` | Custom endpoint routing. |
| `usage_events` | Raw or aggregated metering data. |
| `audit_events` | Immutable operator and user activity. |
| `incidents` | Customer-visible incident records. |

## 16. Rollout Plan

### Phase 0: Managed Runtime Skeleton

- Define the managed Postgres deployment primitive and local development
  Postgres profile.
- Package `palimpsest serve` as a managed deployment with config sidecar.
- Define signed deployment spec format.
- Add cloud-aware health checks and metering events.
- Add gateway routing for one project/environment.
- Stand up internal-only dashboard and metadata database.

### Phase 1: Private Alpha

- Organization, project, and environment CRUD.
- Managed Postgres creation with logical replication, publication, slot,
  backups, and database roles configured automatically.
- `palimpsest dev up` and `palimpsest dev down` for local Postgres plus sync.
- Existing Postgres connection flow for import and advanced testers.
- Config validation and deployment history.
- Basic dashboard metrics and logs.
- Cloud CLI login, config push/pull, deploy, logs.
- CLI `db create`, `db psql`, and basic backup visibility.
- Hosted endpoint for TypeScript/React clients.
- Manual billing and quota enforcement.

### Phase 2: Public Beta

- Self-serve signup and billing.
- Automated usage metering.
- Team RBAC, audit logs, API keys.
- Query explorer and permission editor.
- Managed Postgres resize, restore, and maintenance windows.
- Regional data planes in at least two regions.
- Static egress IPs and IP allowlists.
- Alerting, status pages, and support tooling.

### Phase 3: Production GA

- SSO/SAML and SCIM.
- Private connectivity.
- Managed Postgres HA tiers, warm sync standby, and managed failover.
- Customer-managed encryption keys.
- Compliance evidence package.
- Dedicated deployments for enterprise customers.
- Formal SLOs and support tiers.

## 17. Open Questions

1. Should browser clients connect through gRPC-Web directly, or should the
   hosted gateway expose WebSocket as the primary public protocol?
2. What is the smallest useful pricing metric: database tier, active
   subscription, unique canonical query, egress, or environment tier?
3. How much config should be hot-reloadable before the operational complexity
   outweighs restart-and-resync simplicity?
4. When should the platform invest in query-shard scale-out versus larger
   single-process deployments?
5. Which PostgreSQL 18+ extensions should be supported on day one?
6. Which redaction methods beyond null/static/hash should be available for production-to-development clones?
7. How should customer support safely inspect query plans and runtime state
   without exposing row data?

## 18. Success Criteria

The PaaS is complete enough for production when:

- A new customer can create an organization, provision managed Postgres,
  deploy a sync environment, and subscribe from a browser without manual
  operator help.
- A developer can run the same database-plus-sync shape locally with one CLI
  command.
- The platform can explain and enforce every customer-facing quota.
- Operators can detect, diagnose, and recover from slot lag, resync storms,
  Postgres storage pressure, failed backups, bad configs, failed upgrades,
  and regional incidents.
- Customers can manage access, rotate credentials, view audit logs, and
  understand their bill.
- The open-source Palimpsest engine remains independently usable and is not
  coupled to hosted-only services.
