# Palimpsest PaaS

This directory is the home for managed-platform work around Palimpsest:
managed PostgreSQL 18+ clusters, hosted SyncDeployments, the control plane,
local development orchestration, billing, observability, and operational
tooling.

The existing open-source sync engine remains rooted in `crates/`, `packages/`,
`deploy/`, and `docs/`. PaaS code and specs should start here so hosted
platform work can evolve without disrupting standalone `palimpsest serve`,
the current client SDKs, or the existing test harnesses.

Start with:

- [IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md) for the repo integration
  and buildout plan.
- [MANAGED-POSTGRES-DESIGN.md](MANAGED-POSTGRES-DESIGN.md) for the
  PostgreSQL 18+ runtime, Rust control plane, and host-agent design.
- [../docs/PAAS-DESIGN.md](../docs/PAAS-DESIGN.md) for the product and
  systems design.
- [adr/0001-managed-postgres-version-floor.md](adr/0001-managed-postgres-version-floor.md)
  and [adr/0002-owned-rust-runtime-no-kubernetes.md](adr/0002-owned-rust-runtime-no-kubernetes.md)
  for the initial platform decisions.

## Run The Local PaaS Stack

The easiest local workflow is Tilt:

```text
cd paas
tilt up
```

The [Tiltfile](Tiltfile) starts the control-plane PostgreSQL 18 container,
applies Flyway migrations, starts the SQL-backed control-plane API, seeds the
default local scope, starts a node-agent poll loop, installs UI dependencies,
and starts the React PaaS console.

Local endpoints:

- PaaS UI: `http://127.0.0.1:8090/`
- Control-plane API: `http://127.0.0.1:8088`
- Control-plane metrics: `http://127.0.0.1:8088/metrics`
- Control-plane Postgres: `127.0.0.1:54330`
- Managed Postgres clusters created by the Tilt node-agent start at port `56000`

The UI defaults to `org_123 / project_123 / env_123`, which the Tilt seed
resource creates. The node-agent resource registers `local-dev-host`, sends
heartbeats, and repeatedly polls for queued managed Postgres commands, so
database create/reconcile actions from the UI can progress locally.

The `paas-smoke` Tilt resource is manual. Trigger it from Tilt when you want
the full Docker-backed end-to-end smoke path. It uses an isolated smoke API
on `127.0.0.1:8188`, an isolated control-plane Postgres host port `54331`,
and managed Postgres cluster ports starting at `57000` so it can run without
competing with the live Tilt stack.

## Initial Implementation

The first implementation slice is deliberately additive:

- `crates/palimpsest-paas-core` defines shared PaaS models, including the
  PostgreSQL 18+ version gate and the control-plane to node-agent command
  contract.
- `crates/palimpsest-paas-control-plane` contains a small placement and
  reconciliation skeleton that turns a managed Postgres cluster state into
  node-agent commands.
- `crates/palimpsest-paas-node-agent` contains a host-local planner for
  PostgreSQL lifecycle commands. It can plan or apply one command locally,
  and it can poll the control-plane HTTP queue once to lease, execute, and
  complete a command.
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
- `local/` contains the first local PaaS stack: PostgreSQL 18, logical
  replication config, init SQL, and a Palimpsest config.
- `control-plane/` contains durable control-plane artifacts, starting with
  metadata migrations and a SQL-backed onboarding workspace flow for creating
  the first organization/project/environment scope. Dashboard status pages can
  use the environment overview endpoint to read health, active database,
  cluster, deployment, config, quota-alert, custom-domain, network-access,
  maintenance-window, and active incident state in one scoped request. The
  SQL control plane also includes a maintenance scheduler that converts active
  auto-minor-upgrade windows into owned node-agent update commands.
- `deploy/` contains owned non-Kubernetes host deployment artifacts, including
  initial systemd units and a node-host bootstrap script.
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

Run the local UI with:

```text
cd paas/ui
npm install
npm run dev -- --host 127.0.0.1
```

The Vite development server proxies `/api` to the SQL control plane at
`http://127.0.0.1:8088`, so run `serve-sql-api` separately when you want live
data and mutating actions.

Run the focused checks with:

```text
cargo test -p palimpsest-paas-core -p palimpsest-paas-control-plane -p palimpsest-paas-node-agent -p palimpsest-paas-sync-wrapper -p palimpsest-paas-gateway
```

Validate the local JSON contract examples against the PaaS schemas and check
the observability dashboard/alert artifacts with:

```text
ruby paas/ci/validate-json-schemas.rb
```

Render a first control-plane command plan:

```text
cargo run -p palimpsest-paas-control-plane -- plan paas/examples/cluster.requested.json
```

Run the SQL control plane with envelope-encrypted managed Postgres role
passwords instead of local-dev plaintext secret material:

```text
PALIMPSEST_PAAS_SECRET_PROVIDER=env-envelope \
PALIMPSEST_PAAS_SECRET_KEY_REF=local-control-plane-key \
PALIMPSEST_PAAS_SECRET_KEY_BASE64=<32-byte-base64-key> \
  cargo run -p palimpsest-paas-control-plane -- serve-sql-api 127.0.0.1:8088 postgres://user:pass@localhost:5432/palimpsest_control
```

Render host-local node-agent steps:

```text
cargo run -p palimpsest-paas-node-agent -- plan paas/examples/node-agent.prepare-postgres.json
```

Execute a node-agent command on the current host:

```text
cargo run -p palimpsest-paas-node-agent -- apply paas/examples/node-agent.prepare-postgres.json
```

`apply` mutates the configured host runtime root and invokes local PostgreSQL
18 binaries. Use `plan` first when reviewing command shape.

Poll the SQL-backed control-plane queue once:

```text
cargo run -p palimpsest-paas-node-agent -- register http://127.0.0.1:8088
cargo run -p palimpsest-paas-node-agent -- heartbeat http://127.0.0.1:8088
cargo run -p palimpsest-paas-node-agent -- poll-once http://127.0.0.1:8088
cargo run -p palimpsest-paas-node-agent -- poll-once-container http://127.0.0.1:8088
cargo run -p palimpsest-paas-node-agent -- poll-once-dry-run http://127.0.0.1:8088
```

`register` upserts the local node host, `heartbeat` updates capacity and
state, and `poll-once` uses the local node-agent host id, leases one pending
command from `/v1/node-hosts/{host_id}/commands/lease`, executes it, and posts
completion status back to the control plane. `poll-once-container` uses the
PostgreSQL 18 container image as the source of Postgres binaries for controlled
local execution. `poll-once-dry-run` follows the same queue path, but only
renders the host-local plan before completing the command.

Plan a base backup command:

```text
cargo run -p palimpsest-paas-node-agent -- plan paas/examples/node-agent.run-base-backup.json
```

Plan a restore preparation command:

```text
cargo run -p palimpsest-paas-node-agent -- plan paas/examples/node-agent.prepare-restore.json
```

Plan a WAL archive command:

```text
cargo run -p palimpsest-paas-node-agent -- plan paas/examples/node-agent.archive-wal-segment.json
```

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
PALIMPSEST_GATEWAY_CONTROL_PLANE_URL=http://127.0.0.1:8088 \
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
PALIMPSEST_DB_PROXY_CONTROL_PLANE_URL=http://127.0.0.1:8088 \
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
curl -X POST http://127.0.0.1:8088/v1/environments/env_123/managed-postgres-endpoint/database-proxy-route \
  -H 'content-type: application/json' \
  -H 'x-actor-id: local-dev' \
  --data '{"listen_addr":"127.0.0.1:55430"}'
```

The control plane stores the listen address on `managed_postgres_endpoints`,
derives the upstream from the active managed Postgres cluster, and rewrites the
SQL-backed `database_proxy_routes` row during failover cutover.

Issue a local-dev endpoint certificate record:

```text
curl -X POST http://127.0.0.1:8088/v1/environments/env_123/managed-postgres-endpoint/certificates \
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
