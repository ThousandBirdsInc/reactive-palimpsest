# PaaS Control Plane

This directory contains control-plane artifacts that are not Rust crate source:
metadata migrations, operational notes, and future API specs.

The first migration creates the durable state described by the PaaS design:
organizations, projects, environments, node hosts, managed PostgreSQL
clusters, sync deployments, operations, agent commands, audit events, usage
events, and secret references.

## HTTP APIs

The current `palimpsest-paas-control-plane` binary can start an in-memory HTTP
API for local development:

```text
cargo run -p palimpsest-paas-control-plane -- serve-api 127.0.0.1:8088
```

It can also serve the same initial API surface from the durable SQL store:

```text
cargo run -p palimpsest-paas-control-plane -- serve-sql-api 127.0.0.1:8088 postgres://user:pass@localhost:5432/palimpsest_control
```

Initial routes on both API variants:

- `POST /v1/organizations`
- `POST /v1/projects`
- `POST /v1/environments`
- `GET /v1/environments/{environment_id}/health`
- `GET /v1/environments/{environment_id}/managed-postgres-endpoint`
- `POST /v1/environments/{environment_id}/managed-postgres-endpoint/database-proxy-route`
- `DELETE /v1/environments/{environment_id}/managed-postgres-endpoint/database-proxy-route`
- `POST /v1/environments/{environment_id}/managed-postgres-endpoint/certificates`
- `POST /v1/environments/{environment_id}/managed-postgres-endpoint/certificates/renew`
- `GET /v1/environments/{environment_id}/managed-postgres-endpoint/certificates?status=active`
- `GET /v1/environments/{environment_id}/managed-postgres-endpoint/certificates/{certificate_id}`
- `GET /v1/environments/{environment_id}/managed-postgres-endpoint/certificates/{certificate_id}/bundle`
- `GET /v1/environments/{environment_id}/managed-postgres-endpoint/certificates/{certificate_id}/acme-order`
- `POST /v1/environments/{environment_id}/managed-postgres-endpoint/certificates/{certificate_id}/acme-challenge/validate`
- `POST /v1/environments/{environment_id}/managed-postgres-endpoint/certificates/{certificate_id}/acme-finalize`
- `POST /v1/configs`
- `GET /v1/configs/diff?old=<config>&new=<config>`
- `POST /v1/managed-postgres/clusters`
- `POST /v1/sync-deployments`
- `GET /v1/sync-deployments?organization_id=<org>&project_id=<project>&environment_id=<env>`
- `GET /v1/sync-deployments/{deployment_id}`
- `POST /v1/gateway-routes`
- `GET /v1/gateway-routes?organization_id=<org>&project_id=<project>&environment_id=<env>`
- `GET /v1/gateway-routes/{host}`
- `POST /v1/domains`
- `GET /v1/domains?organization_id=<org>&project_id=<project>&environment_id=<env>&verification_status=<status>&tls_status=<status>`
- `GET /v1/domains/{hostname}`
- `DELETE /v1/domains/{hostname}`
- `POST /v1/ip-allowlist-rules`
- `GET /v1/ip-allowlist-rules?organization_id=<org>&project_id=<project>&environment_id=<env>&purpose=<purpose>&status=<status>`
- `GET /v1/ip-allowlist-rules/{rule_id}`
- `DELETE /v1/ip-allowlist-rules/{rule_id}`
- `POST /v1/static-egress-ips`
- `GET /v1/static-egress-ips?organization_id=<org>&project_id=<project>&environment_id=<env>&region=<region>&status=<status>`
- `GET /v1/static-egress-ips/{egress_ip_id}`
- `DELETE /v1/static-egress-ips/{egress_ip_id}`
- `POST /v1/maintenance-windows`
- `GET /v1/maintenance-windows?organization_id=<org>&project_id=<project>&environment_id=<env>&status=<status>`
- `GET /v1/maintenance-windows/{window_id}`
- `DELETE /v1/maintenance-windows/{window_id}`
- `POST /v1/scheduler/maintenance/run-once`
- `POST /v1/database-proxy-routes`
- `GET /v1/database-proxy-routes?organization_id=<org>&project_id=<project>&environment_id=<env>`
- `GET /v1/database-proxy-routes/{listen_addr}`
- `POST /v1/api-keys`
- `GET /v1/api-keys?organization_id=<org>&project_id=<project>&environment_id=<env>`
- `POST /v1/api-keys/{key_id}/revoke`
- `POST /v1/secret-encryption-keys`
- `GET /v1/secret-encryption-keys?provider=<provider>&purpose=<purpose>&status=<status>`
- `GET /v1/secret-encryption-keys/{key_ref}`
- `POST /v1/secret-rewrap-plans`
- `GET /v1/secret-rewrap-plans?source_key_ref=<key>&target_key_ref=<key>&status=<status>`
- `GET /v1/secret-rewrap-plans/{plan_id}`
- `POST /v1/secret-rewrap-plans/{plan_id}/run`
- `POST /v1/jwt-issuers`
- `GET /v1/jwt-issuers?organization_id=<org>&project_id=<project>&environment_id=<env>&status=<status>`
- `GET /v1/jwt-issuers/{issuer_id}`
- `POST /v1/webhook-endpoints`
- `GET /v1/webhook-endpoints?organization_id=<org>&project_id=<project>&environment_id=<env>&status=<status>`
- `GET /v1/webhook-endpoints/{endpoint_id}`
- `POST /v1/sso-providers`
- `GET /v1/sso-providers?organization_id=<org>&kind=<kind>&status=<status>`
- `GET /v1/sso-providers/{provider_id}`
- `POST /v1/incidents`
- `GET /v1/incidents?organization_id=<org>&project_id=<project>&environment_id=<env>&severity=<severity>&status=<status>`
- `GET /v1/incidents/{incident_id}`

Additional SQL-backed node-agent and reconciliation routes:

- `POST /v1/onboarding/workspaces`
- `GET /v1/environments/{environment_id}/overview`
- `POST /v1/node-hosts`
- `GET /v1/node-hosts?state=<state>&region=<region>&failure_domain=<domain>`
- `GET /v1/node-hosts/{host_id}`
- `POST /v1/node-hosts/{host_id}/agent-credentials`
- `GET /v1/node-hosts/{host_id}/agent-credentials?state=<state>`
- `POST /v1/node-hosts/{host_id}/agent-credentials/{key_id}/revoke`
- `POST /v1/node-hosts/{host_id}/hardening-checks`
- `GET /v1/node-hosts/{host_id}/hardening-checks?status=<status>`
- `GET /v1/node-hosts/{host_id}/hardening-checks/{check_id}`
- `POST /v1/node-hosts/{host_id}/state`
- `POST /v1/node-hosts/{host_id}/heartbeat`
- `POST /v1/node-hosts/{host_id}/commands`
- `POST /v1/node-hosts/{host_id}/commands/lease`
- `POST /v1/node-hosts/{host_id}/commands/{command_id}/complete`
- `POST /v1/managed-postgres/clusters/{cluster_id}/reconcile`
- `GET /v1/managed-postgres/clusters?organization_id=<org>&project_id=<project>&environment_id=<env>`
- `GET /v1/managed-postgres/clusters/{cluster_id}`
- `GET /v1/managed-postgres/clusters/{cluster_id}/schema?database=<db>`
- `POST /v1/managed-postgres/clusters/{cluster_id}/sql-console/query` with optional `database`
- `POST /v1/managed-postgres/clusters/{cluster_id}/query-explorer/inspect`
- `POST /v1/managed-postgres/clusters/{cluster_id}/runtime-checks/probe`
- `GET /v1/managed-postgres/clusters/{cluster_id}/runtime-checks?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/runtime-checks/{check_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/backup-retention-policy`
- `GET /v1/managed-postgres/clusters/{cluster_id}/backup-retention-policy`
- `POST /v1/scheduler/backup-retention/run-once`
- `POST /v1/managed-postgres/certificate-authority-providers`
- `GET /v1/managed-postgres/certificate-authority-providers?status=<status>&default_for_managed_postgres=<bool>`
- `GET /v1/managed-postgres/certificate-authority-providers/{ca_provider_id}`
- `POST /v1/query-permission-policies`
- `GET /v1/query-permission-policies?organization_id=<org>&project_id=<project>&environment_id=<env>&table_schema=<schema>&table_name=<table>&operation=<operation>&status=<status>`
- `GET /v1/query-permission-policies/{policy_id}`
- `POST /v1/query-permission-policies/{policy_id}/dry-run`
- `GET /v1/managed-postgres/clusters/{cluster_id}/operations?status=<status>&kind=<kind>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/operations/{operation_id}`
- `GET /v1/managed-postgres/clusters/{cluster_id}/agent-commands?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/agent-commands/{command_id}`
- `GET /v1/managed-postgres/deletion-tombstones?organization_id=<org>&project_id=<project>&environment_id=<env>&expired=<bool>`
- `GET /v1/managed-postgres/deletion-tombstones/{cluster_id}`
- `POST /v1/managed-postgres/deletion-tombstones/expire`
- `POST /v1/managed-postgres/clusters/{cluster_id}/pause`
- `POST /v1/managed-postgres/clusters/{cluster_id}/resume`
- `POST /v1/managed-postgres/clusters/{cluster_id}/stop`
- `POST /v1/managed-postgres/clusters/{cluster_id}/roles/rotate`
- `POST /v1/managed-postgres/clusters/{cluster_id}/update-minor`
- `POST /v1/managed-postgres/clusters/{cluster_id}/major-upgrades`
- `GET /v1/managed-postgres/clusters/{cluster_id}/major-upgrades?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/major-upgrades/{upgrade_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/resize`
- `POST /v1/managed-postgres/clusters/{cluster_id}/backups`
- `GET /v1/managed-postgres/clusters/{cluster_id}/backups?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/backups/{backup_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/backups/{backup_id}/artifacts`
- `GET /v1/managed-postgres/clusters/{cluster_id}/backups/{backup_id}/artifacts?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/backups/{backup_id}/artifacts/{artifact_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/wal-archives`
- `GET /v1/managed-postgres/clusters/{cluster_id}/wal-archives?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/wal-archives/{segment_name}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/pitr-checks`
- `GET /v1/managed-postgres/clusters/{cluster_id}/pitr-checks?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/pitr-checks/{check_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/failovers`
- `GET /v1/managed-postgres/clusters/{cluster_id}/failovers?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/failovers/{failover_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/standbys`
- `GET /v1/managed-postgres/clusters/{cluster_id}/standbys?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/standbys/{standby_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/standbys/{standby_id}/checks`
- `GET /v1/managed-postgres/clusters/{cluster_id}/standbys/{standby_id}/checks?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/standbys/{standby_id}/checks/{check_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/restores`
- `GET /v1/managed-postgres/clusters/{cluster_id}/restores?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/restores/{restore_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/database-clones`
- `POST /v1/managed-postgres/clone-redaction-policies`
- `GET /v1/managed-postgres/clone-redaction-policies?organization_id=<org>&project_id=<project>&environment_id=<env>&status=<status>`
- `GET /v1/managed-postgres/clone-redaction-policies/{policy_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/support-access-sessions`
- `GET /v1/managed-postgres/clusters/{cluster_id}/support-access-sessions?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/support-access-sessions/{session_id}`
- `POST /v1/managed-postgres/clusters/{cluster_id}/support-access-sessions/{session_id}/approve`
- `POST /v1/managed-postgres/clusters/{cluster_id}/support-access-sessions/{session_id}/revoke`
- `POST /v1/managed-postgres/clusters/{cluster_id}/restore-drills`
- `GET /v1/managed-postgres/clusters/{cluster_id}/restore-drills?status=<status>`
- `GET /v1/managed-postgres/clusters/{cluster_id}/restore-drills/{drill_id}`
- `POST /v1/configs`
- `GET /v1/configs?environment_id=<env>`
- `GET /v1/configs/diff?old=<config>&new=<config>`
- `POST /v1/environments/{environment_id}/configs/rollback`
- `POST /v1/usage-events`
- `GET /v1/usage-events?environment_id=<env>&metric=<metric>`
- `POST /v1/quota-policies`
- `GET /v1/quota-policies?organization_id=<org>&project_id=<project>&environment_id=<env>&metric=<metric>`
- `GET /v1/quota-policies/{policy_id}`
- `POST /v1/quota-alerts`
- `GET /v1/quota-alerts?organization_id=<org>&project_id=<project>&environment_id=<env>&metric=<metric>&state=<state>`
- `POST /v1/quota-alerts/evaluate?environment_id=<env>&metric=<metric>`
- `GET /v1/quota-alerts/{alert_id}`
- `POST /v1/billing-exports`
- `GET /v1/billing-exports?organization_id=<org>&project_id=<project>&environment_id=<env>&metric=<metric>&status=<status>`
- `GET /v1/billing-exports/{export_id}`
- `POST /v1/scheduler/backups/run-once`
- `POST /v1/scheduler/restore-drills/run-once`
- `POST /v1/scheduler/pitr-checks/run-once`
- `POST /v1/scheduler/acme-orders/run-once`
- `POST /v1/api-keys`
- `GET /v1/api-keys?organization_id=<org>&project_id=<project>&environment_id=<env>`
- `POST /v1/api-keys/{key_id}/revoke`
- `POST /v1/team-memberships`
- `GET /v1/team-memberships?organization_id=<org>`
- `GET /v1/audit-events?organization_id=<org>&project_id=<project>&environment_id=<env>&action=<action>&actor_id=<actor>&resource_id=<resource>`
- `GET /metrics`

Mutating routes read `x-actor-id` and write audit events through the same
service layer or SQL store path that the unit tests exercise.
SQL-backed mutating control-plane routes also accept
`Authorization: Bearer plmp_...` API-key tokens. Valid, non-revoked keys are
resolved to audit actors of the form `api_key:<key_id>`. Revoked or unknown
API-key tokens return 401, and `viewer` API keys are treated as read-only and
cannot call mutating routes.
Tenant-scoped API keys are constrained to their organization, project, and
environment scope. Owner/admin keys can create or revoke API keys within their
own scope, and admin keys cannot grant or revoke owner/admin roles. Team
membership upserts use the same role-escalation rules. Platform host controls
remain owner/admin-only.

## Copy-On-Write Database Clones

Managed PostgreSQL clusters render `file_copy_method = clone` for PostgreSQL
18+ nodes. `POST /v1/managed-postgres/clusters/{cluster_id}/database-clones`
queues a node-agent command that runs:

```sql
CREATE DATABASE target_db TEMPLATE source_db STRATEGY FILE_COPY;
```

The source cluster must be ready. The request accepts `source_database`,
`target_database`, and `terminate_source_connections`; when termination is
enabled the agent first clears active source database sessions so PostgreSQL's
template clone can take the file-copy path.

`POST /v1/onboarding/workspaces` is the first self-serve signup workflow
backend. It creates an organization, first project, first environment, and
owner team membership in one SQL transaction and writes the corresponding
audit events. Unauthenticated local signup requests can provide `x-actor-id`
plus `owner_actor_id`; API-key-authenticated calls are constrained by the same
scope and role-management rules as the individual resource APIs.

`GET /v1/environments/{environment_id}/overview` is the first dashboard status
aggregation endpoint. It returns the customer-facing health model, active
managed Postgres endpoint, managed Postgres clusters, SyncDeployments, config
history, quota alerts, custom domains, IP allowlists, static egress IPs,
maintenance windows, and active incidents for one environment while applying
the same scoped read authorization as the underlying APIs.

Domain routes provide the first custom endpoint backend. They bind a verified
customer hostname to an existing gateway route, track DNS verification and TLS
status, and preserve tenant scope for dashboard setup flows.

Network access routes provide the first public-connectivity backend. They
track environment-scoped inbound CIDR allowlists by purpose and owned static
egress IPs by region so customers can configure firewalls before private
connectivity is available.

Maintenance-window routes provide the first customer-editable maintenance
policy backend for managed Postgres. They track environment-scoped weekly
windows, duration, auto-minor-upgrade preference, and active/disabled state for
the owned lifecycle scheduler. `POST /v1/scheduler/maintenance/run-once`
accepts a PostgreSQL 18+ target version, checks active windows in UTC or a
caller-supplied day/time for deterministic operations, and queues the same Rust
node-agent minor-update command used by the manual update API.

Incident routes provide the first customer-visible status-page backend. They
store scoped incident records with severity, lifecycle status, impacted
services, and resolved timestamps so dashboard and support tooling can publish
runtime impact without exposing database row data.

Runtime-check routes provide the first live Postgres diagnostics backend. The
probe route connects through the managed support role, which receives
`pg_monitor` from the node-agent access configuration, and records connection
count, max connections, replication-slot lag bytes, long-running query count,
blocked lock count, oldest transaction age, and autovacuum activity as durable
SQL-backed evidence for operators and support views.

Config uploads are append-friendly until a config version is marked
`deployed`; deployed config versions are immutable and later uploads for the
same id return conflict.

The environment health route returns a customer-facing state of `healthy`,
`degraded`, `at_risk`, `maintenance`, or `unavailable` with component details
for managed Postgres, backup, WAL archive, storage, and SyncDeployment state.

Billing exports support the owned `local-jsonl` destination. By default the
SQL control plane writes one JSON usage event per line under
`/tmp/palimpsest-paas-billing-exports`; override the directory with
`PALIMPSEST_PAAS_BILLING_EXPORT_DIR`, or use `local-jsonl:/path/to/dir` as the
destination.

The backup scheduler route is an operator/internal run-once hook. It queues a
base backup for ready managed Postgres clusters that do not already have a
requested, running, or successful backup. For a long-running control plane, set
`PALIMPSEST_PAAS_BACKUP_SCHEDULER_INTERVAL_SECONDS` to a positive number to run
the same backup scheduler loop inside `serve-sql-api`.

The restore-drill scheduler route is also an operator/internal run-once hook.
It queues a restore clone from the latest successful backup for ready clusters
whose latest successful drill is older than `max_age_hours` or absent. Drill
completion is advanced by the same Rust node-agent `prepare_restore` command
path as customer restore clones, and environment health reports a
`restore_drill` component. `serve-sql-api` can run this loop periodically with
`PALIMPSEST_PAAS_RESTORE_DRILL_SCHEDULER_INTERVAL_SECONDS`; the default
freshness window is seven days and can be overridden with
`PALIMPSEST_PAAS_RESTORE_DRILL_MAX_AGE_HOURS`.

The PITR-check scheduler validates control-plane recovery continuity metadata
for ready clusters. It requires a latest successful base backup, at least one
succeeded WAL segment, and a contiguous succeeded WAL segment sequence on one
timeline. Results are persisted under the cluster `pitr-checks` endpoints and
feed the customer-facing `pitr` health component. `serve-sql-api` can run this
loop periodically with `PALIMPSEST_PAAS_PITR_CHECK_SCHEDULER_INTERVAL_SECONDS`;
the default freshness window is 24 hours and can be overridden with
`PALIMPSEST_PAAS_PITR_CHECK_MAX_AGE_HOURS`.

Each environment has one active managed Postgres endpoint record. Initial
cluster creation sets `managed_postgres_endpoints.active_cluster_id` when the
environment does not already have an active target. Fence-then-promote failover
cuts that endpoint over to the promoted target cluster and records
`updated_by_failover_id` for auditability and routing convergence checks.
`POST /v1/environments/{environment_id}/managed-postgres-endpoint/database-proxy-route`
binds a stable customer-facing listen address to that environment endpoint.
The control plane derives the upstream from the active cluster host assignment
and rewrites `database_proxy_routes` after failover, so the endpoint remains
stable while the active Postgres host changes. SQL-backed managed Postgres
routes include database-proxy startup policy that allows only the managed
app, migration, and support roles to connect to the `postgres` database through
the customer-facing proxy, blocks startup `replication` and `options`
parameters, and leaves a structured hook for exact required startup
parameters on stricter routes.
`POST /v1/managed-postgres/clusters/{cluster_id}/roles/rotate` generates fresh
owned app, migration, replication, and support role secrets as pending
rotation refs, and queues a Rust node-agent `configure_postgres_access`
command to apply the new passwords without returning plaintext material in the
rotate response. Canonical secret refs are promoted only after the node-agent
reports successful password application; failed rotations leave the previous
canonical refs in place. Rotation attempts are tracked in
`managed_postgres_role_credential_rotations` with `pending`, `applying`,
`applied`, and `failed` states, which makes command completion retries
idempotent and leaves an audit trail for staged secret material. A failed
rotation returns the cluster lifecycle to `ready` because the previously
promoted credentials remain valid.
`DELETE /v1/environments/{environment_id}/managed-postgres-endpoint/database-proxy-route`
clears that binding, deletes the SQL-backed proxy route, and revokes the
active endpoint certificate so owned DB proxies stop serving the customer
database endpoint on their next route refresh.
Endpoint certificates are tracked in
`managed_postgres_endpoint_certificates`. Issuing a new active certificate
stores certificate and private key material in `secret_refs`, revokes the
previous active certificate for that environment, and sets
`managed_postgres_endpoints.active_certificate_id`. The local-dev issuer uses
`rcgen` to create a self-signed X.509 certificate. External-PKI providers
accept already-issued `certificate_pem` and `private_key_pem`, import that
material into the same owned secret backend, and activate the certificate on
the endpoint. ACME providers create a provisioning certificate plus a
`managed_postgres_acme_orders` record containing generated private-key, CSR,
HTTP-01 challenge token, and key-authorization secret refs. The scoped
`acme-order` endpoint exposes the order metadata, `GET
/.well-known/acme-challenge/{token}` serves the pending HTTP-01 key
authorization from the owned secret backend for challenge responders, and the
`acme-challenge/validate` endpoint verifies the owned responder material and
moves the order to `ready_to_finalize`. `acme-finalize` only accepts ready
orders, stores the returned PEM chain, verifies that it matches the generated
private key, marks the order succeeded, activates the certificate, and only
then revokes the previous active certificate. Invalid finalization material
marks the ACME order and provisioning certificate `failed` with the validation
error. `POST /v1/scheduler/acme-orders/run-once` scans pending ACME orders and
runs the same challenge-validation transition for background order progress;
`PALIMPSEST_PAAS_ACME_ORDER_SCHEDULER_INTERVAL_SECONDS` enables that loop in
the SQL control-plane process. The renewal endpoint
reuses the active certificate common name, enforces a default 30-day renewal
window unless `force` is set, and then creates the replacement through the
selected provider path. Normal
certificate list/detail responses expose only secret refs; the scoped `bundle`
endpoint opens those refs through the configured secret backend and is
intended for owned gateway/proxy infrastructure. Database proxy route
discovery includes the active certificate metadata under `tls` when an
endpoint has an active certificate, but it never includes private key material.
The DB proxy fetches the bundle, handles PostgreSQL `SSLRequest`, terminates
TLS with rustls, validates the first post-TLS PostgreSQL startup packet, and
then forwards accepted traffic to the managed upstream.

Gateway route state is SQL-backed through `gateway_routes`. The control plane
persists hosted sync endpoint hostnames, tenant scope, internal sync upstream,
TLS policy, mTLS secret refs, and per-environment rate limits so the gateway
can be driven from owned control-plane state instead of handwritten route
files. `mutual_tls_to_sync` routes require an HTTPS sync endpoint, a CA secret
ref, and optionally paired client certificate/private-key refs plus a server
name override. The gateway binary reads this state at startup when
`PALIMPSEST_GATEWAY_CONTROL_PLANE_URL` is set, treats the SQL result as
authoritative, refreshes it in place on
`PALIMPSEST_GATEWAY_ROUTE_REFRESH_SECS`, and can authenticate with
`PALIMPSEST_GATEWAY_CONTROL_PLANE_TOKEN`. For each discovered
`mutual_tls_to_sync` route the gateway calls
`GET /v1/gateway-routes/:host/mtls-bundle`, opens the referenced CA and client
identity material through the control-plane secret backend, and refreshes the
in-process PEM cache before applying the route set. For local development,
`PALIMPSEST_GATEWAY_MTLS_SECRETS` can still point at a JSON object whose keys
are route secret refs and whose values are PEM blocks. Route deletion is
exposed through `DELETE /v1/gateway-routes/:host` so deprovisioned endpoints
are removed from long-running gateways on the next refresh.
Database proxy routes follow the same pattern through `database_proxy_routes`.
Manual route upsert remains available for operator overrides and local proxy
tests, but managed Postgres environments should configure routes through the
environment endpoint API above. The owned DB proxy reads route state at startup
when
`PALIMPSEST_DB_PROXY_CONTROL_PLANE_URL` is set and can authenticate with
`PALIMPSEST_DB_PROXY_CONTROL_PLANE_TOKEN`. It then refreshes that route state
every 15 seconds by default, or by `PALIMPSEST_DB_PROXY_ROUTE_REFRESH_SECS`;
updates to an existing listen address replace the upstream for new connections
and deleted routes stop their listeners without restarting the proxy process.
Manual proxy route deletion is exposed through
`DELETE /v1/database-proxy-routes/:listen_addr`.

Failover routes record source-to-target promotion intent and queue Rust-owned
node-agent commands for `fence_postgres_primary` followed by
`promote_postgres_standby`. Callers can pass a prepared `standby_id`; the
control plane verifies that the standby belongs to the source cluster and
completed successfully before promotion. The current implementation persists
failover lifecycle state, asks the node agent to stop the source Postgres
process locally before writing a durable fence marker, marks the promoted
target cluster `ready` after command success, and updates the environment
endpoint target; production gateway convergence and multi-host network fencing
remain explicit follow-on work.

Standby routes prepare a target cluster from the latest successful source
backup through the Rust-owned `prepare_postgres_standby` node-agent command.
The command restores the base backup, writes `standby.signal`, and renders
`primary_conninfo`, `primary_slot_name`, WAL restore configuration, and the
assigned target Postgres port after creating an idempotent physical
replication slot on the source. The control plane resolves the managed
replication role secret and renders that least-privilege role into
`primary_conninfo`, so standbys do not use the `postgres` superuser for
streaming. Managed API-created standbys are started as hot standbys by the
node agent, and the target cluster moves to `ready` after the command succeeds.
Production streaming credential rotation, stronger fencing, and cross-host
network validation remain follow-on work.

Standby-check routes queue `check_postgres_standby_lag` node-agent commands
against the source cluster. The node agent executes a PostgreSQL assertion over
`pg_replication_slots`, so a succeeded check proves the physical standby slot
exists and is below the requested lag threshold. The latest check also feeds
the customer-facing `standby` health component.

Node-agent registration, heartbeat, lease, and command-completion routes can
require scoped HMAC request signatures by setting
`PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64` on both the SQL control plane and
node-agent processes. Each request signs the host id, operation scope, and a
Unix timestamp; the control plane rejects missing, mismatched, or stale
signatures. `PALIMPSEST_PAAS_AGENT_TOKEN` remains a local/bootstrap bearer
fallback when no signing key is configured. When neither value is set, the
local development behavior remains open.

Leased commands include a short-lived `operation_token`. The SQL control
plane stores only a SHA-256 hash of that token on `agent_commands`, expires it
after 15 minutes, and requires the node agent to echo the token when
completing the command. List/detail command APIs never return operation
tokens.

After a host is registered, operators can rotate it onto a database-backed
per-host signing key with:

```sh
curl -X POST http://127.0.0.1:8088/v1/node-hosts/local-dev-host/agent-credentials
```

The response contains `key_id` and `signing_key_base64`; install them as
`PALIMPSEST_PAAS_AGENT_SIGNING_KEY_ID` and
`PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64` on that node-agent. When the key id
header is present, the SQL control plane validates against the active per-host
credential stored in metadata rather than the shared bootstrap key.
Operators can list credential metadata and revoke a key without exposing the
secret material. Successful signed agent requests update `last_used_at` and
`last_used_operation`, giving the control plane a durable usage trail for
rotation and incident response.

## SQL Store

`paas/crates/palimpsest-paas-control-plane/src/sql_store.rs` contains the
first Postgres-backed persistence methods for the same resources. Server-side
Postgres access uses `sqlx` with a pooled `PgPool`; migrations are not run
from Rust.

Quota policies, usage events, billing exports, and quota alerts are all backed
by Flyway-managed tables. Quota alerts store dashboard-configured threshold
basis points, evaluate current usage from the policy window, transition between
`ok` and `firing`, and emit a `palimpsest_paas_quota_alerts_firing_total`
metric.

Usage events can be HMAC signed by data-plane producers. The payload
`signature` object uses `algorithm = "hmac_sha256_v1"` and signs this canonical
string with newline separators:

```text
event_id
idempotency_key
organization_id
project_id
environment_id
metric
quantity
occurred_at
```

Unsigned events are accepted by default for local development. Set
`PALIMPSEST_PAAS_USAGE_EVENT_SIGNING_KEY_BASE64` on `serve-sql-api` to require
and verify signatures on `POST /v1/usage-events`. Set
`PALIMPSEST_PAAS_USAGE_EVENT_SIGNING_KEY_ID` to require a specific payload
`signature.key_id`. Verified signature metadata is stored with the durable
usage event and included in usage-event exports and billing snapshots.

## Secret Material

The SQL control plane supports three owned secret-material backends:

- `local-dev` is the default. It stores generated managed Postgres role
  passwords as plaintext `secret_material` for easy local development.
- `env-envelope` stores generated role passwords as AES-256-GCM envelopes in
  `encrypted_material` and records the wrapping key name in `key_ref`.
- `file-envelope` uses the same AES-256-GCM envelope format but loads its
  primary key ref and keyring from a mounted JSON file instead of environment
  variables. This is the preferred owned-host shape when keys are delivered by
  systemd credentials, a sidecar, or an internal KMS sync process.

Enable envelope mode by setting:

```text
PALIMPSEST_PAAS_SECRET_PROVIDER=env-envelope
PALIMPSEST_PAAS_SECRET_KEY_REF=prod-control-plane-key-1
PALIMPSEST_PAAS_SECRET_KEY_BASE64=<32-byte-base64-key>
```

Enable file-backed envelope mode with:

```text
PALIMPSEST_PAAS_SECRET_PROVIDER=file-envelope
PALIMPSEST_PAAS_SECRET_KEYRING_FILE=/run/palimpsest/secret-keyring.json
```

The keyring file format is:

```json
{
  "primary_key_ref": "prod-control-plane-key-2",
  "keys": {
    "prod-control-plane-key-1": "<32-byte-base64-key>",
    "prod-control-plane-key-2": "<32-byte-base64-key>"
  }
}
```

The envelope backend is intentionally local to the owned control plane. A
production KMS service can supply and rotate the 32-byte root keys into the
file or environment keyring behind the same interface without changing managed
Postgres lifecycle code.

## Flyway Migrations

Control-plane schema migrations are Flyway versioned migrations in
`migrations/`. Apply them from this directory:

```text
cd paas/control-plane
flyway -configFiles=flyway.conf -url=jdbc:postgresql://localhost:5432/palimpsest_control -user=user -password=pass migrate
```

For the local control-plane metadata database:

```text
docker compose -f paas/control-plane/docker-compose.yaml up -d control-plane-postgres
docker compose -f paas/control-plane/docker-compose.yaml run --rm flyway
```

The initial migrations are `migrations/V1__initial_control_plane.sql`,
`migrations/V2__quota_policies.sql`, `migrations/V3__billing_exports.sql`,
`migrations/V4__secret_envelopes.sql`, and
`migrations/V5__billing_export_delivery.sql`. Later additive migrations add
billing delivery state, node-agent credentials, API-key/RBAC metadata,
managed Postgres deletion tombstones, status-page incidents, custom domain
metadata, network access metadata, maintenance-window metadata, and
maintenance-window-driven minor update scheduling. Runtime-check migration
`V30` adds durable live Postgres diagnostics sampled by the SQL control plane
with `sqlx`. Backup-retention migration `V31` adds an opt-in policy table for
expiring old successful base backups while always preserving a configured
minimum number of restorable backup artifacts. Agent-command migration `V32`
adds hashed short-lived operation tokens for lease-to-completion binding.
Certificate-authority migration `V33` adds a managed Postgres CA-provider
registry and a durable default local-development issuer that endpoint
certificate issuance resolves through. Node-agent credential migration `V34`
adds revocation and last-used metadata for per-host signing keys. Major
upgrade migration `V35` adds durable PostgreSQL major-upgrade records linked
to the queued node-agent command and operation; new commands include the
assigned source port so the Rust node agent can render an upgrade plan and run
source SQL preflight before recording the upgrade artifact. Clone-redaction
migration `V36` adds environment-scoped redaction policies and requires an active source
policy when restoring a managed Postgres backup into another environment; the
node-agent restore command now renders and applies null/static/hash redaction
SQL before the clone is reported succeeded.
Support-access migration `V37` adds durable break-glass support-access
sessions with requested, approved, revoked, and scoped audit states. Audit
immutability migration `V38` installs database triggers that reject updates,
deletes, and truncation on `audit_events`. Secret-key migration `V39` adds an
owned registry for envelope/KMS key refs with active, retiring, and retired
states. Secret rewrap migration `V40` adds auditable rewrap plans that count
currently wrapped secrets for a source key and target active key; the SQL API
can now run a plan by opening matching `secret_refs`, resealing them to the
target key through the configured owned secret backend, and recording
succeeded/failed counts. Node-host hardening migration `V41` records per-host
baseline checks for image, OS, kernel, PostgreSQL 18+ support floor,
container runtime, disk encryption, firewall, unattended upgrades, and patch
freshness; the Rust node agent can now generate and submit that evidence with
`palimpsest-paas-node-agent hardening-check <control-plane-url>`. Backup
artifact migration `V42` adds a durable artifact catalog for completed base
backups, including provider, object URI, manifest path/hash, size, lifecycle
status, and error metadata. Successful `run_base_backup` command completion
automatically records every artifact reported by the node agent. By default
that includes the local filesystem artifact. When
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_DIR` is configured, the node agent also
copies the completed base backup into a filesystem-backed S3-compatible object
store layout, reports an `s3://` object URI, and the control plane catalogs it
beside the local artifact. When
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_ENDPOINT=http://host:port[/base-path]` or
an `https://` endpoint is configured, the same upload step PUTs every completed
backup file to
`/{bucket}/{object_key}/{relative_path}` on that endpoint and reports provider
`s3_compatible_http` by default. The endpoint can use `http://` for local
deployments or `https://` with a configured CA bundle for owned internal object
stores. Set
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AUTH_TOKEN` to add an `Authorization:
Bearer <token>` header to each PUT. Override the header name or scheme with
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AUTH_HEADER` and
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AUTH_SCHEME`. For HTTPS object-store
endpoints, set `PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_CA_CERT_FILE` to a PEM CA
bundle trusted by the node agent; use
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_TLS_SERVER_NAME` when the certificate name
differs from the endpoint host. For S3-compatible endpoints that require AWS
Signature Version 4, set
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_ACCESS_KEY_ID`,
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_SECRET_ACCESS_KEY`, and optionally
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_REGION` and
`PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_SERVICE` (`s3` by default). Migration
`V46` enforces unique active `(host_id, host_port)` managed Postgres
assignments, and the SQL control plane allocates restore, standby, and drill
clone ports from the host's first free slot instead of fixed offsets. Other
provider-specific request signing remains production adapter work.

## Local Smoke

The local smoke script starts the control-plane metadata database, runs
Flyway, starts the SQL API, creates a small organization/project/environment,
registers a local node host, records a usage event, creates a managed
PostgreSQL 18 cluster, reconciles it, and has the node-agent poll and complete
the queued prepare command using the PostgreSQL 18 container image. It then
reconciles through ready state, creates SyncDeployment state, exercises WAL
archive/base backup/restore, and verifies stop/delete lifecycle transitions:

```text
bash paas/control-plane/smoke-local.sh
```

The smoke mutates only `PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT`, defaulting to
`/tmp/palimpsest-paas-smoke`, and removes it on success unless
`PALIMPSEST_PAAS_KEEP_SMOKE=1` is set. Managed Postgres clusters default to
port `55000`; override `PALIMPSEST_PAAS_AGENT_FIRST_PORT` if that port is
already occupied locally.
