# Palimpsest PaaS

This directory is the home for managed-platform work around Palimpsest:
managed PostgreSQL 18+ clusters, hosted SyncDeployments, the control plane,
local development orchestration, billing, observability, and operational
tooling.

The existing open-source sync engine remains rooted in `crates/`, `packages/`,
`deploy/`, and `docs/`. PaaS code and specs should start here so hosted
platform work can evolve without disrupting standalone `palimpsest serve`,
the current client SDKs, or the existing test harnesses.

## Status: prototype (local use)

> **This is an early prototype, intended primarily for local development and
> evaluation — not production.**
>
> The self-hosted PaaS runs end-to-end on a single machine: it provisions real
> PostgreSQL 18 instances, drives their lifecycle through an owned Rust control
> plane and node agent, and exposes an operator console. But several pieces
> that a production deployment requires are deliberately stubbed or local-only:
>
> - **No authentication.** The control-plane API and UI assume a trusted local
>   caller. The UI sends an `x-actor-id` header for audit attribution only;
>   there is no login, API-key enforcement on the UI path, or RBAC.
> - **Local Kubernetes.** Managed databases run as CloudNativePG `Cluster`
>   resources on whatever Kubernetes cluster your kube context targets
>   (locally, a `kind` cluster). Scheduling, failover, and storage are handled
>   by Kubernetes and the CloudNativePG operator, not an owned host fleet.
> - **Local secret material by default.** Role passwords use a local-dev
>   plaintext provider unless you opt into envelope encryption (see below).
> - **Connection routing falls back to the backend port.** Until a certificate
>   workflow publishes a database-proxy route, the UI surfaces a direct
>   `127.0.0.1:<port>` connection string (clearly flagged as a dev fallback).
>
> Treat data created here as disposable. Run it on a workstation, not a shared
> or internet-facing host.

## What it does

The current slice can, entirely on a local machine:

- **Provision managed PostgreSQL 18+ clusters** — create a cluster intent,
  reconcile it onto the local host, and drive lifecycle actions (pause, resume,
  resize storage, rotate role credentials, delete / cancel an un-provisioned
  request).
- **Copy-on-write database clones** — clone a cluster's database into a new
  database on the same cluster, and track the clone operations.
- **Backups, PITR, and recovery** — request base backups, archive WAL,
  inspect point-in-time-recovery continuity checks, and run restore drills.
- **High-availability primitives** — prepare standbys, run standby-lag checks,
  fence a primary, and fail over the environment endpoint to a new cluster.
- **A read-only SQL console + schema browser** per cluster, with query
  explain, history, and a one-click sample-dataset seeder for evaluation.
- **Connection information** for every cluster and clone — host, port,
  database, role, and ready-to-copy `psql` / libpq URI / keyword DSN / JDBC
  strings (passwords are never exposed by the API; rotate via the Roles tab).
- **An operator console (React UI)** with environment overview and per-resource
  health, plus pages for clusters, node hosts, incidents, quota, gateway and
  database-proxy routes, and the audit log.
- **A control plane** that turns desired cluster/deployment state into
  CloudNativePG `Cluster` manifests and applies them to Kubernetes, tracks
  operations, records an audit trail, enforces quota policies and alerts, and
  exposes Prometheus metrics at `/metrics`.
- **The CloudNativePG operator**, which owns host-local PostgreSQL 18 lifecycle
  — provisioning, failover, backups, PITR, upgrades, and storage resize.
- **An owned gateway and raw database TCP proxy** for hosted SyncDeployments
  and managed Postgres endpoints, including PostgreSQL TLS (`SSLRequest`)
  negotiation, startup-packet validation, and route-level user/database policy.
- **Hosted SyncDeployment tooling** — render standalone Palimpsest configs from
  signed deployment specs and classify config-reload behavior.

A more detailed component map is in [Initial Implementation](#initial-implementation)
below; the UI design is documented in [PAAS-UI-DESIGN.md](PAAS-UI-DESIGN.md).

## Prerequisites

The local workflow runs the platform on a throwaway Kubernetes cluster. You
need:

- **Docker** — the container runtime backing the local `kind` cluster.
- **kind** — creates the local Kubernetes cluster.
- **kubectl** — talks to the cluster.
- **Helm** (3.13+) — installs the `palimpsest-paas` chart and the CloudNativePG
  operator.
- **Rust** (stable toolchain, `cargo`) — builds the control plane, gateway, and
  runtime images.
- **Node.js** (18+) and `npm` — builds the React UI.

Managed PostgreSQL 18 runs inside the cluster as CloudNativePG `Cluster`
resources; the operator supplies the Postgres 18 image, so no host-local
Postgres binaries are required.

## Design docs

Start with:

- [IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md) for the repo integration
  and buildout plan.
- [MANAGED-POSTGRES-DESIGN.md](MANAGED-POSTGRES-DESIGN.md) for the
  PostgreSQL 18+ runtime and control-plane design.
- [PRODUCTION-READINESS-DESIGN.md](PRODUCTION-READINESS-DESIGN.md) for the
  concrete work needed to move the current prototype to a production PaaS.
- [../docs/PAAS-DESIGN.md](../docs/PAAS-DESIGN.md) for the product and
  systems design.
- [adr/0001-managed-postgres-version-floor.md](adr/0001-managed-postgres-version-floor.md)
  for the version floor, and
  [adr/0003-kubernetes-cloudnativepg-runtime.md](adr/0003-kubernetes-cloudnativepg-runtime.md)
  for the Kubernetes + CloudNativePG runtime decision (superseding
  [adr/0002](adr/0002-owned-rust-runtime-no-kubernetes.md)).

## Run the self-hosted PaaS (local)

Bring up a local `kind` cluster, install the CloudNativePG operator and the
`palimpsest-paas` chart, and reach the console. The `paas/local` helper script
wraps these steps:

```text
cd paas
./local/up.sh        # create kind cluster + helm install palimpsest-paas
```

Under the hood this:

1. Creates a `kind` cluster (if one is not already running).
2. Runs `helm dependency build` and installs the
   [`palimpsest-paas`](deploy/helm/palimpsest-paas) chart into the
   `palimpsest-paas` namespace, which deploys the CloudNativePG operator, the
   control plane and its CloudNativePG-backed metadata database, the gateway,
   the database proxy, and the React console.

Watch it come up:

```text
kubectl -n palimpsest-paas get pods
kubectl get clusters.postgresql.cnpg.io -A
```

Reach the console and APIs by port-forwarding the services:

```text
kubectl -n palimpsest-paas port-forward svc/paas-palimpsest-paas-ui 8090:80
kubectl -n palimpsest-paas port-forward svc/paas-palimpsest-paas-control-plane 8088:8088
```

Managed databases created from the UI are reconciled into CloudNativePG
`Cluster` resources by the control plane; watch them with
`kubectl get clusters.postgresql.cnpg.io -A`. Tear everything down with
`./local/down.sh` (deletes the `kind` cluster).

## Initial Implementation

The first implementation slice is deliberately additive:

- `crates/palimpsest-paas-core` defines shared PaaS models, including the
  PostgreSQL 18+ version gate and managed-cluster desired state.
- `crates/palimpsest-paas-control-plane` contains the SQL-backed control plane
  that persists desired state and reconciles managed clusters into
  CloudNativePG resources.
- `crates/palimpsest-paas-runtime` renders CloudNativePG `Cluster` /
  `ScheduledBackup` manifests from managed-Postgres desired state and applies
  them with `kubectl`. It replaces the former host-local node agent.
- `crates/palimpsest-paas-sync-wrapper` renders standalone Palimpsest
  configs from signed deployment specs and classifies reload behavior.
- `crates/palimpsest-paas-gateway` contains the hosted gateway server,
  host-based route resolution, HTTP proxying to SyncDeployments,
  per-environment limiting, egress accounting, and buffered usage-event
  generation. It also contains the first owned raw database TCP proxy binary.
- `specs/` contains JSON schemas for environment intent, managed Postgres
  cluster state, SyncDeployment state, node host registration, deployment
  specs, node-agent commands/status, usage events, quota policies, billing
  exports, quota alerts, query permission policies, JWT issuers, webhook
  endpoints, SSO providers, and customer-facing environment health.
- `examples/` contains sample JSON payloads for exercising the first slice.
- `local/` contains the local PaaS dev flow: `up.sh`/`down.sh` scripts that
  install the Helm chart and CloudNativePG onto a `kind` cluster.
- `control-plane/` contains durable control-plane artifacts, starting with
  metadata migrations and a SQL-backed onboarding workspace flow for creating
  the first organization/project/environment scope. Dashboard status pages can
  use the environment overview endpoint to read health, active database,
  cluster, deployment, config, quota-alert, custom-domain, network-access,
  maintenance-window, and active incident state in one scoped request. The
  SQL control plane also includes a maintenance scheduler for auto-minor-upgrade
  windows. (Some control-plane operations still carry the legacy host-command
  data model; see ADR 0003 for the in-progress migration to CloudNativePG.)
- `deploy/` contains the `palimpsest-paas` Helm chart, which deploys the
  control plane, gateway, database proxy, console, the control plane's
  CloudNativePG-backed metadata database, and the CloudNativePG operator.
- `observability/` contains first-pass Prometheus alert rules and a Grafana
  overview dashboard for control-plane, managed Postgres, and gateway signals.
  The SQL-backed control plane and gateway both expose Prometheus text at
  `/metrics`.
- `ui/` contains the first React operator console for monitoring environment
  health, managed Postgres clusters, node hosts, SyncDeployments, quota
  alerts, incidents, gateway routes, and database proxy routes. It can also
  create managed PostgreSQL 18+ database intents and trigger common lifecycle
  actions through the control-plane API.

The UI design is documented in [PAAS-UI-DESIGN.md](PAAS-UI-DESIGN.md).

## Running components individually

`./local/up.sh` is the recommended path. The commands below run individual
pieces by hand — useful when iterating on a single component.

Run the local UI with:

```text
cd paas/ui
npm install
npm run dev -- --host 127.0.0.1
```

The Vite development server proxies `/api` to the SQL control plane at
`http://127.0.0.1:18088`, so run `serve-sql-api` separately when you want live
data and mutating actions.

Run the focused checks with:

```text
cargo test -p palimpsest-paas-core -p palimpsest-paas-control-plane -p palimpsest-paas-runtime -p palimpsest-paas-sync-wrapper -p palimpsest-paas-gateway
```

Validate the local JSON contract examples against the PaaS schemas and check
the observability dashboard/alert artifacts with:

```text
ruby paas/ci/validate-json-schemas.rb
```

Render the CloudNativePG manifests for a managed cluster's desired state:

```text
cargo run -p palimpsest-paas-runtime -- render paas/examples/cluster.requested.json
```

Run the SQL control plane with envelope-encrypted managed Postgres role
passwords instead of local-dev plaintext secret material:

```text
PALIMPSEST_PAAS_SECRET_PROVIDER=env-envelope \
PALIMPSEST_PAAS_SECRET_KEY_REF=local-control-plane-key \
PALIMPSEST_PAAS_SECRET_KEY_BASE64=<32-byte-base64-key> \
  cargo run -p palimpsest-paas-control-plane -- serve-sql-api 127.0.0.1:18088 postgres://user:pass@localhost:5432/palimpsest_control
```

Apply those manifests to the cluster your kube context targets (this is what
the control plane does internally on reconcile). Backups and WAL archiving are
configured by setting `PALIMPSEST_PAAS_RUNTIME_BACKUP_OBJECT_STORE`, which adds
a `barmanObjectStore` stanza and a `ScheduledBackup` to the rendered output:

```text
cargo run -p palimpsest-paas-runtime -- reconcile paas/examples/cluster.requested.json
kubectl get clusters.postgresql.cnpg.io -A
```

`render` prints YAML to stdout; `reconcile` pipes it through
`kubectl apply --server-side`. CloudNativePG then owns provisioning, base
backups, WAL archiving, PITR, failover, upgrades, and storage resize — the work
the former node agent did by hand.

Render a managed SyncDeployment config:

```text
cargo run -p palimpsest-paas-sync-wrapper -- render-config paas/examples/deployment.signed.json
```

Verify a production signed SyncDeployment spec:

```text
cargo run -p palimpsest-paas-sync-wrapper -- verify-signature <signed-spec.json>
```

Classify a deployment config change:

```text
cargo run -p palimpsest-paas-sync-wrapper -- classify-reload paas/examples/deployment.config-version-1.json paas/examples/deployment.config-version-2.json
```

Plan SyncDeployment process supervision:

```text
cargo run -p palimpsest-paas-sync-wrapper -- plan-start paas/examples/deployment.signed.json
cargo run -p palimpsest-paas-sync-wrapper -- plan-transition paas/examples/deployment.config-version-1.json paas/examples/deployment.config-version-2.json
```

Start the hosted gateway against a route file:

```text
PALIMPSEST_GATEWAY_ROUTES=paas/examples/gateway-route.json \
  cargo run -p palimpsest-paas-gateway
```

Or start it from SQL-backed control-plane route discovery:

```text
PALIMPSEST_GATEWAY_CONTROL_PLANE_URL=http://127.0.0.1:18088 \
  PALIMPSEST_GATEWAY_ROUTE_REFRESH_SECS=15 \
  cargo run -p palimpsest-paas-gateway
```

Set `PALIMPSEST_GATEWAY_CONTROL_PLANE_TOKEN` when the control-plane route list
requires a bearer API key. When control-plane discovery is enabled, the gateway
treats SQL-backed routes as authoritative and replaces its in-memory route
table using `PALIMPSEST_GATEWAY_ROUTE_REFRESH_SECS` so new and deleted hosted
sync endpoints do not require a process restart.

The gateway binds `127.0.0.1:8089` by default. Override it with
`PALIMPSEST_GATEWAY_ADDR`. `GET /healthz` returns liveness and
`GET /metrics` returns the initial request and egress counters. Gateway route
`sync_endpoint` values are proxied as HTTP upstreams in the current slice.
`POST /usage-events/drain` returns and clears buffered egress usage events for
internal metering collection.

Start the raw database endpoint proxy against a route file:

```text
PALIMPSEST_DB_PROXY_ROUTES=paas/examples/database-proxy-route.json \
  cargo run -p palimpsest-paas-gateway --bin palimpsest-paas-db-proxy
```

Or start it from SQL-backed control-plane route discovery:

```text
PALIMPSEST_DB_PROXY_CONTROL_PLANE_URL=http://127.0.0.1:18088 \
  cargo run -p palimpsest-paas-gateway --bin palimpsest-paas-db-proxy
```

Set `PALIMPSEST_DB_PROXY_CONTROL_PLANE_TOKEN` when the control-plane route list
requires a bearer API key. When control-plane discovery is enabled, the proxy
refreshes route state every 15 seconds by default. Override that with
`PALIMPSEST_DB_PROXY_ROUTE_REFRESH_SECS`; refreshed routes update the upstream
for new connections on an existing listen address and stop listeners for
deleted routes without restarting the proxy process.

For managed Postgres, configure the stable environment endpoint through the
control plane instead of hand-writing the route:

```text
curl -X POST http://127.0.0.1:18088/v1/environments/env_123/managed-postgres-endpoint/database-proxy-route \
  -H 'content-type: application/json' \
  -H 'x-actor-id: local-dev' \
  --data '{"listen_addr":"127.0.0.1:55430"}'
```

The control plane stores the listen address on `managed_postgres_endpoints`,
derives the upstream from the active managed Postgres cluster, and rewrites the
SQL-backed `database_proxy_routes` row during failover cutover.

Issue a local-dev endpoint certificate record:

```text
curl -X POST http://127.0.0.1:18088/v1/environments/env_123/managed-postgres-endpoint/certificates \
  -H 'content-type: application/json' \
  -H 'x-actor-id: local-dev' \
  --data '{"common_name":"db.env-123.palimpsest.local","validity_days":30}'
```

The control plane stores certificate and private key material through
`secret_refs`, marks the new certificate active, and records it on
`managed_postgres_endpoints.active_certificate_id`. This is the owned lifecycle
hook consumed by the DB proxy. Database proxy route discovery includes active
certificate metadata under `tls`, while the scoped certificate `bundle`
endpoint is the path for owned infrastructure to retrieve PEM material from
the secret backend.

The database proxy supports PostgreSQL `SSLRequest` negotiation on TLS-enabled
routes, terminates TLS with rustls, validates startup packets, and enforces
route-level allowed-user and allowed-database policy before forwarding bytes to
the managed upstream. This keeps customer database traffic on an owned Rust
endpoint process instead of delegating it to a third-party proxy.

The SQL-backed control plane also exposes `GET /metrics` for aggregate
node-agent command failures, billing export failures, quota usage ratios,
firing quota alerts, managed Postgres lifecycle state, storage allocation,
backup freshness, WAL archive freshness, PITR freshness, restore-drill
freshness, standby-check freshness, WAL archive failures, and host storage
pressure. Quota alerts are SQL-backed threshold rules over quota policies;
`POST /v1/quota-alerts/evaluate` computes firing state from recent usage
windows for dashboard and alerting surfaces.
Usage-event payloads can include `hmac_sha256_v1` signatures; when
`PALIMPSEST_PAAS_USAGE_EVENT_SIGNING_KEY_BASE64` is configured on the SQL
control plane, `POST /v1/usage-events` requires a valid signature and stores
the signature metadata with the durable event.

Start the local stack:

```text
cargo run -p palimpsest-cli -- dev up
```

Print `.env` settings:

```text
cargo run -p palimpsest-cli -- dev env
```

The local Postgres app connection string is:

```text
postgres://palimpsest_app:palimpsest_app@localhost:54329/palimpsest_dev
```

Optional local migrations and seed SQL live under
`local/postgres/migrations/` and `local/postgres/seeds/`. Use
`cargo run -p palimpsest-cli -- dev reset` to recreate the database and rerun
those files.
