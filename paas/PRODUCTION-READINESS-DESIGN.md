# Palimpsest PaaS Production Readiness Design

**Status:** Proposed
**Scope:** Production-readiness plan for the Palimpsest managed Postgres 18+
and SyncDeployment PaaS.
**Decision context:** The PaaS keeps the owned Rust control plane, Rust
node-agent, gateway, and host-based runtime described in
`MANAGED-POSTGRES-DESIGN.md`. Production readiness means hardening that owned
runtime, not replacing it with Kubernetes.
**Primary artifact:** This document is the production-readiness checklist and
sequencing plan. `IMPLEMENTATION-PLAN.md` remains the incremental build plan;
this document defines what must be true before customer production traffic is
allowed on the managed PaaS.

## 1. Summary

The current PaaS prototype proves the core product shape on a local machine:
provision a PostgreSQL 18+ cluster, configure it for Palimpsest, run lifecycle
commands through a Rust control plane and node agent, expose an operator UI,
track operations, and exercise backups, WAL archival, restore, failover, and
gateway/database-proxy paths.

Production readiness requires turning those local and partially implemented
paths into a secure, multi-host, recoverable, observable managed data platform.
The target product is intentionally narrow:

- One managed PostgreSQL 18+ cluster per customer environment.
- One paired Palimpsest SyncDeployment.
- Logical replication configured and monitored by the platform.
- Managed database endpoints through the owned gateway/database proxy.
- Backups, WAL archival, PITR, restore drills, HA, credential rotation, quotas,
  audit, billing, and support workflows.

We are not building a general-purpose application scheduler for arbitrary
customer workloads. The production bar should therefore be expressed in
database and sync-runtime terms: cluster lifecycle, WAL continuity, endpoint
availability, tenant isolation, recoverability, and operator response.

## 2. Current Prototype Boundary

The current repository status is local-evaluation only. The important
prototype boundaries are:

- The UI/control-plane path assumes a trusted local caller in the default
  stack.
- The data plane is single-host by default through `local-dev-host`.
- Local secret material is available and production secret-store integration is
  incomplete.
- Database connection routing can fall back to direct local backend ports.
- Several HA, backup, certificate, gateway, and upgrade flows have durable
  metadata and local smoke coverage, but still need production host, network,
  object-storage, and failure-mode validation.

These boundaries are acceptable for local development. They are not acceptable
for a shared, internet-facing, customer-data-bearing service.

### Current Readiness Snapshot

| Area | Current state | Production gap |
| --- | --- | --- |
| Control plane | SQL-backed APIs, operations, audit events, config versions, usage events, quota policies, and many resource models exist. | Production auth must be enforced on every route, all mutating paths need audited scope checks, and HA deployment of the control plane itself needs a runbook. |
| Node agent | Local command leasing, completion, Postgres lifecycle actions, backup/WAL/restore/failover commands, and hardening evidence are started. | Real multi-host installation, upgrade, mTLS identity, host-loss recovery, and production command idempotency drills are required. |
| Managed Postgres | PostgreSQL 18+ provisioning, roles, logical replication, backups, WAL archival, restore drills, PITR checks, resize, updates, standbys, and failover metadata are started. | Object-store durability, streaming standby behavior, endpoint cutover, restore/failover drills, and recovery SLO evidence need production validation. |
| Gateway/proxy | Gateway routes, database proxy routes, TLS handling, route refresh, rate limits, and certificate metadata are started. | Internet-facing hardening, automated ACME polling/renewal, route convergence tests, abuse controls, and external checks are required. |
| Secrets | Local-dev, env-envelope, file-envelope, key registry, and rewrap records exist. | Production KMS/HSM adapter, secret access policy, secret audit, and production-mode refusal of local plaintext providers are required. |
| UI | Operator console covers core resource views and local connection flows. | Login/session flow, scoped role-aware UX, customer billing/usage pages, support workflows, and status/incident flows need production wiring. |
| Observability | Prometheus metrics, dashboard, alerts, health model, incidents, and runtime checks are started. | Paging policies, runbooks, external probes, drill evidence, and alert ownership are required. |
| Billing/quotas | Usage events, signed event option, quota policies/alerts, and export snapshots are started. | Provider integration, production-required signing, quota enforcement at all control/data-plane boundaries, and customer-visible billing are required. |

## 3. Production Definition

The PaaS is production ready when a new customer can:

1. Sign up, create an organization/project/environment, and authenticate
   through a production identity path.
2. Provision a managed PostgreSQL 18+ cluster and paired SyncDeployment without
   manual operator steps.
3. Connect through stable managed database and sync endpoints.
4. Rely on enforced tenant isolation, scoped credentials, TLS/mTLS, backups,
   WAL archival, PITR, restore drills, and audited destructive operations.
5. See health, usage, quotas, audit history, incidents, and billing in the
   product.

The platform operator is production ready when they can:

1. Add and drain data-plane hosts safely across regions/failure domains.
2. Detect, diagnose, and recover from host failure, Postgres storage pressure,
   backup failure, WAL/archive gaps, replication slot lag, resync storms, bad
   configs, failed upgrades, certificate failures, and regional incidents.
3. Perform upgrades, failovers, restores, credential rotations, and support
   access through audited workflows.
4. Prove recoverability through scheduled restore drills and PITR continuity
   checks.
5. Operate from documented runbooks, dashboards, alerts, and release gates.

## 4. Readiness Levels

Production readiness should be managed as explicit levels. A feature may be
implemented and still not be production-ready if it has not passed the level
required for customer traffic.

| Level | Meaning | Allowed use |
| --- | --- | --- |
| L0: Local prototype | Works on one developer machine with trusted local callers and disposable data. | Local development and demos only. |
| L1: Staging integrated | Runs on production-like infrastructure with real auth, mTLS, object storage, and multiple hosts. | Internal staging and automated drills. |
| L2: Limited production beta | Runs for selected customers with support coverage, documented limits, and manual operator approval for high-risk actions. | Early production customers and low-volume workloads. |
| L3: General availability | Self-serve, supportable, metered, monitored, and covered by published SLOs for the supported product surface. | Broad production use. |

Minimum launch requirements:

- Secure beta foundation requires L1 for every critical control path and L2
  for auth, secrets, backup/restore, gateway/proxy, and tenant isolation.
- Production beta requires L2 for every critical workstream.
- General availability requires L3 for auth, provisioning, backup/PITR,
  gateway/proxy, billing/quotas, observability, support access, and release
  gates.

## 5. Production Control Loops

The production platform is a set of durable control loops. Each loop must have
a source of truth, reconciler, executor, observability contract, and failure
policy.

| Loop | Source of truth | Executor | Production invariant |
| --- | --- | --- | --- |
| Cluster lifecycle | `managed_postgres_clusters`, `operations`, `agent_commands` | Control-plane reconciler and node agent | A cluster state change is durable, idempotent, audited, and recoverable after control-plane or agent restart. |
| Host placement | `node_hosts`, observed resources, existing assignments | Placement engine | Placement only targets active, healthy hosts with sufficient storage, ports, slots, and failure-domain policy. |
| Credential lifecycle | `secret_refs`, role credential records, support-access sessions | Secret issuer and node agent | Credentials are scoped, rotatable, never returned after initial issuance, and support access requires approval. |
| Backup/WAL/PITR | backup, WAL, artifact, PITR, restore-drill records | Backup scheduler and node agent | A ready cluster always has current recoverability evidence or an alert. |
| Endpoint routing | managed endpoint, gateway route, database proxy route, certificate records | Control plane, gateway, database proxy | New connections route to the active cluster and certificate material matches the endpoint identity. |
| SyncDeployment | signed deployment specs, config versions, deployment state | Sync wrapper/supervisor | Config changes are classified before application and unsafe identity changes are blocked or drained. |
| Usage/quota/billing | usage events, quota policies, quota alerts, billing exports | Gateway, control plane, billing exporter | Usage is idempotent, signed in production, explainable, and quota decisions are auditable. |
| Observability/incidents | metrics, runtime checks, alerts, incidents, audit events | Control plane, agents, gateway, operators | Every customer-visible degradation has a health state, alert path, and incident workflow. |

Each production loop must define:

- Idempotency key or deduplication behavior.
- Retry policy and terminal failure state.
- Lease ownership and timeout behavior.
- Audit event shape.
- Metrics and health signals.
- Operator runbook.
- Staging drill that proves the failure path.

## 6. Critical Workstreams

### 6.1 Authentication, Authorization, And Tenant Isolation

Production must remove trusted-local assumptions from every customer and
operator path.

Required work:

- Dashboard login and session management.
- API-key enforcement on all UI/API paths, not only selected SQL-backed
  routes.
- Organization, project, and environment scoped RBAC across every read and
  mutation.
- Owner/admin/viewer semantics that cannot be bypassed by route shape,
  missing scope fields, or fallback headers.
- SSO/OIDC/SAML support for organization login.
- Support-access enforcement so support credentials cannot be resolved without
  an active, approved, unexpired session.
- Security tests for cross-tenant reads, cross-tenant writes, privilege
  escalation, revoked credentials, stale sessions, and missing scopes.

Production gate:

- No customer-facing or operator mutation accepts only `x-actor-id`.
- Every API route has an explicit auth mode and scope check.
- Production mode rejects startup if local trusted-header auth remains enabled
  for customer or operator APIs.
- Cross-tenant access tests cover the dashboard, API keys, support tooling,
  gateway route discovery, database proxy route discovery, audit logs, and
  billing/usage exports.

Dependencies:

- Team membership and API-key scope rules.
- Dashboard session model.
- SSO/OIDC/SAML provider configuration.
- Support-access session enforcement.

### 6.2 Multi-Host Regional Data Plane

Production needs a real host fleet, not a single local node-agent.

Required work:

- Host bootstrap images/scripts for production environments.
- Node-agent installation, upgrade, rollback, and health management.
- Host registration with per-host credentials and mTLS identity.
- Heartbeats, observed resources, hardening evidence, and host state.
- Placement across active hosts using region, storage, cluster slots,
  maintenance state, and failure domain.
- Draining, maintenance, offline, and repair flows.
- Capacity planning for CPU, memory, storage, ports, WAL volume, and backup
  bandwidth.
- Host loss handling when a node-agent disappears mid-operation.

Production gate:

- A staging fleet can provision clusters across at least two hosts, drain one
  host, reject placement onto unhealthy hosts, and recover from node-agent
  interruption without corrupting cluster lifecycle state.
- Host bootstrap and upgrade steps are reproducible from versioned artifacts.

Dependencies:

- Production host image or bootstrap script.
- Node-agent mTLS identity.
- Observed resource reporting.
- Host maintenance/drain control-plane states.

### 6.3 Production Secrets, KMS, And mTLS

Local-dev secret providers and bearer fallbacks must be replaced by production
identity and key management.

Required work:

- Production KMS or HSM-backed envelope encryption adapters.
- Key rotation and secret rewrap runbooks.
- mTLS between control plane, node agents, gateway, database proxy, and
  SyncDeployments where applicable.
- Per-host node-agent credential issuance, rotation, revocation, and audit.
- Removal or hard disablement of shared bearer-token fallbacks in production.
- Secret access logs for database credentials, certificate material, gateway
  mTLS bundles, and support credentials.

Production gate:

- Production mode refuses to start with local plaintext secret providers or
  shared bearer-only agent authentication.
- Credential rotation and key rewrap are exercised in staging without service
  interruption.
- Secret access for support and route material is logged with actor, scope,
  reason, and resource.

Dependencies:

- Chosen production KMS/HSM provider.
- mTLS certificate authority and rotation path.
- Secret resolver used by SyncDeployment, gateway, database proxy, and support
  tooling.

### 6.4 Managed Postgres Recoverability

Recoverability is a core product promise. Backups and WAL archival must be
boring, verified, and alertable.

Required work:

- Production object-store adapter coverage for the selected provider(s).
- Backup artifact encryption, manifest hashes, size checks, and lifecycle
  tracking.
- WAL archival continuity over the configured PITR window.
- Restore drill scheduler enabled by default for production tiers.
- PITR check scheduler enabled by default for production tiers.
- Retention enforcement for active clusters and deletion tombstones.
- Backup deletion safety checks.
- Operator runbooks for failed backups, failed WAL archive, corrupt artifact,
  restore failure, and retention drift.

Production gate:

- Every production cluster has a recent successful base backup, continuous WAL
  evidence for its PITR window, and a recent successful restore drill.
- A staging incident drill can restore a cluster from object storage and prove
  the restored database can serve Palimpsest-compatible logical replication.
- Backup deletion and tombstone expiration cannot remove the last recovery
  point still inside the documented retention window.

Dependencies:

- Production object storage and credentials.
- Backup artifact encryption and manifest verification.
- Alert thresholds by tier.
- Restore-drill capacity and cleanup policy.

### 6.5 High Availability And Failover

The existing HA primitives need production-grade end-to-end behavior.

Required work:

- Physical streaming standby startup on real hosts.
- Standby lag monitoring and alerting.
- Replication slot creation, validation, and cleanup for physical and logical
  paths.
- Primary fencing that handles process, host, and network failure modes.
- Endpoint failover through the database proxy and gateway route model.
- Gateway/database-proxy convergence tests after failover.
- RPO/RTO targets by tier.
- Regular failover drills.

Production gate:

- In staging, a primary-host failure can be fenced, a standby promoted, and the
  customer endpoint moved without manual database surgery.
- Failover leaves an auditable record tying together the source cluster,
  target cluster, fence command, promote command, endpoint cutover, and
  customer-facing health transition.
- Post-failover Palimpsest replication-slot behavior is verified for the
  supported tier, either through failover-slot support or a documented slot
  recreation/resync path.

Dependencies:

- Streaming standby implementation.
- Fencing implementation covering local process failure and lost host contact.
- Endpoint routing convergence.
- RPO/RTO tier definitions.

### 6.6 Gateway, Database Proxy, TLS, And Domains

The hosted endpoint path must withstand real internet traffic and certificate
lifecycle events.

Required work:

- Automated ACME directory polling and renewal.
- External-PKI handoff runbooks.
- DNS verification and certificate issuance state surfaced in the dashboard.
- Database proxy route refresh/convergence guarantees.
- PostgreSQL TLS handling under connection churn.
- Startup-packet policy hardening and negative tests.
- Gateway and database proxy rate limits, abuse controls, logs, metrics, and
  alerts.
- Public endpoint SLO checks from outside the data plane.

Production gate:

- A customer environment can add a custom domain, issue/renew a certificate,
  connect through TLS, fail over the backing cluster, and keep new connections
  routed to the active target.
- External probes verify the public endpoint path independently from internal
  service health.

Dependencies:

- Certificate authority provider selection.
- DNS verification path.
- Gateway/proxy route refresh policy.
- Abuse/rate-limit policy.

### 6.7 SyncDeployment Runtime

The managed Palimpsest runtime must be operationally distinct from Postgres
while staying tied to the environment lifecycle.

Required work:

- Supervisor execution path for signed specs on production hosts.
- Safe reload, drain/restart, and blocked config-change behavior.
- Health checks for Postgres reachability, WAL slot state, dataflow progress,
  gateway reachability, and config validity.
- Resync storm detection and throttling.
- Query-shard or vertical scale decision for the first production tiers.
- Logs, metrics, and traces with bounded cardinality.

Production gate:

- A signed config deploy can move from requested to running, reject unsafe
  changes, roll back to a previous config version, and surface actionable
  health without coupling hosted-only concepts into standalone
  `palimpsest serve`.
- Resync storm detection can put the environment into a degraded or throttled
  state without corrupting customer-visible sync semantics.

Dependencies:

- Signed deployment spec verification.
- Secret resolver for database URLs.
- Gateway route registration.
- Runtime metrics from `palimpsest-server`.

### 6.8 Observability, Incidents, And Runbooks

Production operation requires more than metrics existing; it requires
actionable diagnosis and recovery.

Required work:

- Paging-grade alerts for backups, WAL continuity, PITR checks, restore
  drills, standby lag, host storage pressure, node-agent failures, gateway
  rejection spikes, certificate expiry, quota exhaustion, and billing export
  failures.
- Dashboards for operators and customer-facing health views.
- Incident records connected to affected environments.
- Runbooks for each alert class.
- Status-page workflow for customer-visible degradation.
- Support tooling that exposes schema/query plans/runtime state without
  default row-data access.

Production gate:

- Every page has an owner, a severity, a runbook, and a staging drill proving
  the runbook still works.
- Customer-facing environment health is derived from the same underlying
  signals operators use to diagnose the issue.

Dependencies:

- Metrics from control plane, node agents, gateway, database proxy, and
  SyncDeployment.
- Incident model and status-page flow.
- Alert routing and on-call ownership.

### 6.9 Upgrades, Maintenance, And Host Lifecycle

The platform owns Postgres and node-agent upgrades, so upgrade safety is a
production requirement.

Required work:

- Minor Postgres update execution against production host images.
- Major upgrade preflight, execution, and rollback/fail-forward policy.
- Node-agent and gateway binary rollout strategy.
- Maintenance-window enforcement.
- Customer notification and audit records.
- Upgrade test matrix for supported PostgreSQL 18+ versions.

Production gate:

- A staging environment can perform minor Postgres updates, node-agent
  upgrades, and a major-version rehearsal with clear failure handling and
  preserved recovery options.
- Maintenance-window enforcement is visible to customers and cannot start a
  disruptive operation outside the selected policy except through audited
  emergency override.

Dependencies:

- Versioned host images or packages.
- Maintenance-window model.
- Backup and restore safety checks before upgrades.
- Customer notification path.

### 6.10 Billing, Quotas, And Account Lifecycle

The PaaS cannot be production self-serve without a complete customer account
and billing loop.

Required work:

- Signup and organization setup connected to production auth.
- Plan/tier selection and allowed resource limits.
- Usage-event signing required in production.
- Quota enforcement at provisioning, gateway, storage, backup, and egress
  boundaries.
- Billing export delivery to the chosen billing provider.
- Customer-visible usage, quota, and invoice views.

Production gate:

- A customer can understand what they are using, what limits apply, why an
  action was rejected, and what they will be billed for.
- Production usage ingestion requires signed events from trusted data-plane
  emitters.

Dependencies:

- Plan/tier model.
- Billing-provider export destination.
- Gateway, storage, backup, and cluster lifecycle metering.
- Quota error contract.

### 6.11 CI, Release, And Soak Testing

Production needs a release process that exercises the owned runtime under
failure, not just unit tests.

Required work:

- PaaS crate unit tests in CI.
- Schema/example validation in CI.
- Flyway migration tests against PostgreSQL 18+.
- Multi-host integration tests.
- End-to-end local smoke in CI.
- Backup/restore/PITR tests.
- Failover tests.
- Upgrade tests.
- Load and soak tests for gateway, database proxy, node-agent queues, and
  SyncDeployment behavior.
- Security tests for auth and tenant isolation.
- Reproducible host images and signed release artifacts.

Production gate:

- A release cannot promote unless migrations, multi-host provisioning,
  backup/restore, failover, auth isolation, gateway routing, and rollback
  tests pass in staging.
- Release artifacts are reproducible enough that an operator can identify the
  exact control-plane, node-agent, gateway, proxy, and UI versions running in
  an incident.

Dependencies:

- Production-like staging environment.
- Versioned host images or packages.
- Smoke, integration, chaos, and soak test jobs.
- Release manifest.

## 7. Dependency Order

The workstreams are interdependent. The intended order is:

1. **Lock down identity first.** Production auth, tenant scope checks, API-key
   enforcement, and support-access approval must land before any shared
   production environment.
2. **Establish production trust roots.** KMS/HSM integration, mTLS identity,
   per-host credentials, and production-mode refusal of local secret providers
   are prerequisites for real data-plane hosts.
3. **Bring up multi-host staging.** Host bootstrap, node-agent upgrade,
   placement, observed resources, and drain/maintenance flows must be exercised
   before HA or production backup claims are meaningful.
4. **Prove recoverability.** Object-store backups, WAL continuity, PITR checks,
   and restore drills must pass before production customer data is accepted.
5. **Harden endpoint routing.** Gateway, database proxy, certificates, custom
   domains, rate limits, and external probes must be production-like before
   customers connect.
6. **Add HA tier.** Streaming standbys, lag checks, fencing, failover, and
   endpoint cutover depend on the host fleet and routing layers.
7. **Close the product loop.** Billing, quotas, customer health, incidents,
   support, and account lifecycle complete the beta/GA surface.
8. **Gate releases.** CI, staging drills, rollback/fail-forward docs, and
   release manifests become mandatory before production beta.

No phase should claim production readiness based only on local smoke coverage.
Local smoke proves the contract shape; staging drills prove operational
readiness.

## 8. Phased Path To Production

### Phase A: Secure Beta Foundation

Goal: allow trusted early users on isolated environments.

Required outcomes:

- Production auth enforced across dashboard/API.
- Per-host agent credentials and mTLS path established.
- Production secret backend selected and wired.
- Multi-host staging fleet operational.
- Backup/WAL/PITR/restore-drill schedulers active in staging.
- Gateway and database proxy run against SQL-backed route discovery.
- Basic billing/usage/quota views available.

Exit criteria:

- One customer environment can be provisioned end to end in staging without
  manual SQL or direct host access.
- Cross-tenant auth tests pass.
- A restore drill succeeds from production-like object storage.
- The launch-blocker list in Section 11 has no critical open item for auth,
  secrets, host fleet, or recoverability.

### Phase B: Production Beta

Goal: serve limited production customers with explicit support coverage.

Required outcomes:

- At least two production-ready regions or failure domains, depending on the
  initial geography promise.
- HA tier with standby, lag checks, failover drill, and endpoint cutover.
- Paging alerts and runbooks for critical failure modes.
- Certificate automation and renewal.
- Maintenance windows and minor update flow.
- Support-access approval and revocation enforced.
- Customer-facing incident/status workflows.

Exit criteria:

- Operators can complete backup restore, failover, credential rotation, host
  drain, and certificate renewal drills using documented workflows.
- Production beta SLOs and support expectations are written and measured.
- Every critical alert has an on-call route and a tested runbook.

### Phase C: General Availability

Goal: make the platform self-serve for the supported product surface.

Required outcomes:

- SSO/SAML/OIDC and mature team/RBAC flows.
- Formal SLOs, support tiers, and on-call ownership.
- Customer-managed key option if required by target accounts.
- Compliance evidence package.
- Billing-provider integration and customer invoice workflow.
- Upgrade and release promotion process with rollback/fail-forward guidance.
- Documented limits for tiers, regions, Postgres versions, extensions, and
  operational guarantees.

Exit criteria:

- A new customer can sign up, provision, connect, deploy, observe usage, rotate
  credentials, and receive support without manual operator intervention.
- Published limits, SLOs, support tiers, pricing/quotas, and compliance
  evidence match the actual implemented platform.

## 9. Validation And Evidence Plan

Production readiness must be demonstrated with evidence that maps to the
control loops above.

| Evidence | Required coverage |
| --- | --- |
| Route authorization matrix | Every API route has auth mode, allowed roles, tenant scope fields, and tests for allowed/denied access. |
| Multi-host staging drill | Provision, drain, maintenance, host loss, agent restart, command retry, and placement rejection. |
| Backup/restore drill | Base backup, WAL archive, object-store artifact verification, PITR check, restore drill, retention cleanup. |
| Failover drill | Standby creation, lag check, source fencing, target promotion, endpoint cutover, proxy route refresh, post-failover sync behavior. |
| Certificate drill | Domain verification, issuance, renewal, invalid finalization rejection, proxy reload, expiry alert. |
| Secret drill | Credential rotation, support-access approval/revocation, KMS key rotation, secret rewrap, revoked access rejection. |
| Upgrade drill | Control-plane migration, node-agent rollout, gateway/proxy rollout, Postgres minor update, major upgrade rehearsal. |
| Load/soak run | Gateway traffic, database proxy connections, node-agent command queue, backup bandwidth, SyncDeployment resync behavior. |
| Incident drill | Alert fires, runbook used, incident created, customer health/status updated, resolution audited. |
| Billing/usage reconciliation | Signed usage events, quota alerts, rejected over-quota action, export delivery, customer-visible usage. |

Evidence should be stored as durable artifacts: CI logs, staging drill reports,
dashboard snapshots, incident records, release manifests, and audit-event
queries. A checklist item is not complete unless the evidence names the exact
environment, version, timestamp, and pass/fail result.

## 10. Initial SLO And Safety Targets

Initial public SLOs should stay conservative and match the architecture
already described in `docs/PAAS-DESIGN.md`.

| Target | Initial production-beta value |
| --- | --- |
| Managed sync endpoint availability | Tier-specific, measured at the gateway edge. |
| Managed database endpoint availability | Tier-specific, measured through the database proxy. |
| Platform metadata RPO | Target less than 5 minutes. |
| Managed Postgres RPO | PITR-window dependent; target less than 5 minutes on paid tiers. |
| Regional data-plane workload recreation RTO | Target less than 30 minutes. |
| Managed Postgres restore RTO | Tier dependent; target less than 30 minutes for standard tiers. |
| Backup freshness | At least one successful base backup inside the configured policy window. |
| WAL continuity | No unexplained archive gap inside the PITR window. |
| Restore-drill freshness | Tier-specific; default target at least every 30 days. |

Safety targets:

- Destructive operations require explicit intent, audit events, and retention
  windows where applicable.
- A failed credential rotation must leave the previous canonical credentials
  usable.
- A failed config deploy must not mutate the last deployed immutable config
  version.
- A failed backup cleanup must not delete the only retained recovery point.
- A failed failover must leave the active endpoint either on the old primary or
  on the promoted target, never on an unknown cluster.

## 11. Launch Blockers

The following are hard blockers for any customer production traffic:

- Trusted-local UI/API auth is enabled in production mode.
- Shared bearer-only node-agent authentication is accepted in production mode.
- Local plaintext secret provider is used for production role passwords,
  certificate private keys, or gateway mTLS material.
- A cluster can be provisioned without a backup policy and recoverability
  health signal.
- A ready cluster can lack current backup/WAL/PITR/restore-drill evidence
  without alerting.
- Cross-tenant API, audit, gateway-route, proxy-route, billing, or support
  access succeeds in security tests.
- Endpoint failover can update metadata without gateway/database-proxy
  convergence evidence.
- Backup deletion can remove all valid recovery points inside the retention
  window.
- A release can run migrations without rollback/fail-forward instructions.
- A critical page can fire without an owner and runbook.

## 12. Non-Goals For Production Readiness

These are not required for the first production release:

- Arbitrary customer application workloads.
- Kubernetes-native installation as the primary runtime.
- Multi-region active-active Postgres.
- Customer superuser access.
- PostgreSQL versions below 18.
- Unlimited customer-supplied extensions.
- Fully generic compute autoscaling.

Avoiding these keeps the production plan aligned with the actual Palimpsest
PaaS product: a managed data platform, not a generic cloud runtime.

## 13. Open Decisions

These decisions should be closed before production beta:

1. Which production KMS/HSM provider is the first supported backend?
2. Which regions or failure domains are included in the beta promise?
3. What are the first published database and sync endpoint SLOs by tier?
4. Do paid tiers require warm standby by default or as an opt-in HA tier?
5. Is post-failover Palimpsest recovery handled by failover slots, slot
   recreation, or controlled resync for the first release?
6. Which object-store provider is the first production backup target?
7. What is the first supported custom-domain certificate path: ACME-only,
   external PKI, or both?
8. Which billing provider and export format are required for beta?
9. Which PostgreSQL extensions, if any, are supported on day one?
10. What compliance evidence package is required for target customers?

## 14. Production Readiness Checklist

Before a production launch, the following must all be true:

- Auth/RBAC: no trusted-local auth path is available in production mode.
- Tenant isolation: cross-tenant API, UI, support, gateway, proxy, and billing
  tests pass.
- Host fleet: multi-host placement, draining, maintenance, and host-loss tests
  pass.
- Secrets: production KMS/envelope backend, mTLS, credential rotation, and key
  rewrap are tested.
- Recoverability: backups, WAL archival, PITR checks, restore drills, and
  retention cleanup are active and alerting.
- HA: standby creation, lag checks, fencing, promotion, and endpoint cutover
  are tested.
- Gateway/proxy: TLS, custom domains, route refresh, rate limits, and failure
  alerts are tested.
- SyncDeployment: signed config deployment, reload classification, rollback,
  and health reporting are tested.
- Observability: every critical alert has a runbook and owner.
- Upgrades: Postgres, node-agent, gateway, and control-plane upgrade paths are
  tested in staging.
- Billing/quotas: usage reconstruction, quota enforcement, and billing export
  delivery are tested.
- Release: CI/staging gates block promotion on migration, auth, recoverability,
  failover, or routing regressions.
