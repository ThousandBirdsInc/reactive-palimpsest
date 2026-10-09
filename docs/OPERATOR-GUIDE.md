# Palimpsest Operator Guide

This guide is for the team that runs Palimpsest in production. The
focus is the deploy / configure / tune lifecycle. For the on-the-wire
recovery procedures, see [`RUNBOOK.md`](RUNBOOK.md). For the security
posture see [`THREAT-MODEL.md`](THREAT-MODEL.md) and
[`TLS.md`](TLS.md).

## What you're deploying

A single binary (`palimpsest serve`) that:

1. Connects to a Postgres primary as a logical replication consumer.
2. Compiles SQL subscriptions into a differential dataflow.
3. Streams diffs to gRPC clients with at-least-once delivery and
   client-driven LSN acks.

It is stateless modulo the Postgres replication slot it owns. Restart
is safe; warm-state is best-effort.

## Deployment shape

```
              ┌──────────────────────┐
              │   Upstream Postgres  │
              │   (logical decoding) │
              └──────────┬───────────┘
                         │ pgoutput
                         ▼
        ┌──────────────────────────────────┐
        │  palimpsest serve  (Nx replicas) │
        │  - WAL ingest                    │
        │  - Subscription router           │
        │  - gRPC :50051                   │
        │  - /metrics :9090                │
        └────┬───────────────────────────┬─┘
             │                           │
       ┌─────▼──────┐             ┌──────▼─────┐
       │  TLS proxy │  (optional) │  Prometheus│
       │  / Ingress │             │  scrape    │
       └─────┬──────┘             └────────────┘
             │
       gRPC + Web
             │
       ┌─────▼─────┐
       │  Clients  │
       └───────────┘
```

Replicas of `palimpsest serve` are independent — each owns its own
slot. Run one for HA + failover; do not load-balance subscriptions
across replicas because in-flight cursor state is per-process.

## Postgres prerequisites

- `wal_level = logical`
- `max_replication_slots ≥ <number of palimpsest replicas>`
- `max_wal_senders ≥ <same>`
- A role with `REPLICATION` and read access to the tables Palimpsest
  publishes.
- A publication that includes the tables you intend to expose.

```sql
ALTER SYSTEM SET wal_level = 'logical';
SELECT pg_reload_conf();
CREATE PUBLICATION palimpsest_pub FOR TABLE posts, authors, comments;
CREATE ROLE palimpsest WITH LOGIN REPLICATION PASSWORD '...';
GRANT SELECT ON posts, authors, comments TO palimpsest;
```

## Installing

Palimpsest is distributed as a prebuilt binary and as a container
image; neither needs a Rust toolchain. Every `v*` tag publishes:

- Tarballs on the
  [GitHub Release](https://github.com/ThousandBirdsInc/reactive-palimpsest/releases)
  for Linux x86_64/aarch64 (fully static, run on any distribution) and
  macOS x86_64/aarch64, with a `SHA256SUMS` file. The one-liner picks
  the right one and verifies it:

  ```sh
  curl -fsSL https://raw.githubusercontent.com/ThousandBirdsInc/reactive-palimpsest/main/install.sh | sh
  ```

  Pin with `PALIMPSEST_VERSION=v0.1.1`; choose the directory with
  `PALIMPSEST_INSTALL=/opt/palimpsest/bin`.

- `ghcr.io/thousandbirdsinc/reactive-palimpsest:<version>` (also
  `<major>.<minor>` and `latest`), a distroless image for linux/amd64
  and linux/arm64 that runs as non-root and expects the config at
  `/etc/palimpsest/palimpsest.toml`. The Helm chart under
  [`deploy/helm/palimpsest`](../deploy/helm/palimpsest) wraps it.

Windows is not built; use the container image or WSL.

## Configuration

`palimpsest serve <config>` reads everything from one TOML file; there
are no other flags. Minimal production example:

```toml
[grpc]
addr = "0.0.0.0:50051"

[metrics]
addr = "0.0.0.0:9090"

[database]
dsn = "postgres://palimpsest:secret@db:5432/app?sslmode=require"
slot = "palimpsest_main"
publication = "palimpsest_pub"
tls_root_ca_file = "/etc/palimpsest/ca.pem"   # verify-full

[queries]
files = ["queries/live.sql"]

[auth]
kind = "jwt"
jwks_url = "https://issuer.example/.well-known/jwks.json"
audience = "palimpsest"
[auth.claim_to_field]
sub = "id"

[permissions.user_schema]
id = "int"

[[permissions.rules]]
name = "own_rows"
table = "tickets"
predicate = "owner_id = $user.id"
```

`[database]` plus `[queries]` is the entire database-side surface: the
engine introspects the catalog, derives the streamed tables from the
registered queries, and creates or resumes the slot and publication.
Without `[database]` the server boots against an in-memory stub and
the built-in demo catalog, which is useful for client development.

The annotated, complete reference is
[`crates/palimpsest-cli/palimpsest.example.toml`](../crates/palimpsest-cli/palimpsest.example.toml);
the permission DSL is in [`PERMISSIONS.md`](PERMISSIONS.md) and query
files in [`NAMED-QUERIES.md`](NAMED-QUERIES.md).

Run `palimpsest validate-config palimpsest.toml` in CI and before
every deploy: with `[database]` configured it introspects the live
catalog and rejects exactly what `serve` would reject (unknown
tables, bad predicates, unsupported query constructs). Add
`--offline` where the database is unreachable.

## TLS

Palimpsest does not terminate TLS in-process. Place a proxy in front
(envoy, nginx, ingress-nginx, a service mesh) and let it handle certs.
[`TLS.md`](TLS.md) has worked examples.

## Tuning knobs that matter

| Knob | Default | Increase when | Decrease when |
| --- | --- | --- | --- |
| `subscribe_rate.burst` | 32 | trusted callers issue large bulk subscriptions | clients are noisy / hostile |
| `subscribe_rate.refill_per_sec` | 8 | ditto | ditto |
| `reconnect_rate` | 60 / 60s | many short-lived clients per IP (e.g. mobile) | strict per-IP isolation |
| `max_subscriptions_per_connection` | 256 | a single trusted gateway fans out many subscriptions | shared multi-tenant deployments |
| `query_limits.max_input_bytes` | 65 536 | machine-generated SQL legitimately needs it | hand-authored SQL only |
| `query_limits.max_mir_nodes` | 256 | wide CTE-heavy queries | strict bound desired |
| `router.channel_capacity` | 1 024 diffs | bursty commit volume; clients can keep up | bound memory tightly |
| `router.compaction_window` | 30 s | long-tailed clients reconnecting after pauses | low-latency resync acceptable |

All limits are configurable through `PalimpsestBuilder::with_security_limits`
and `RouterConfig`; the same fields drive embedded uses (e.g. tests).

## Metrics worth alerting on

Prometheus is exposed on `metrics_addr`. The metrics that should drive
pages, with the symptom they catch:

- `palimpsest_wal_lag_bytes` climbing without bound → upstream is
  producing faster than the slot can consume; check
  [`RUNBOOK.md`](RUNBOOK.md) §1.
- `palimpsest_resyncs_by_reason_total{reason="channel_saturation"}`
  spiking → at least one client cannot keep up; bound diffs / s per
  subscription.
- `palimpsest_resyncs_by_reason_total{reason="slot_recreated"}` non-zero
  → server lost its slot; investigate.
- `palimpsest_active_subscriptions` near
  `max_subscriptions_per_connection × <client count>` → clients are
  near the cap.
- `palimpsest_subscribe_rejected_total{code="rate_limited"}` surging
  per IP → consider tightening `reconnect_rate`.

A baseline Prometheus + Grafana setup ships under `deploy/prom/` (if
present) — adapt to your alerting infra.

## Capacity planning

Subscriptions share state through canonical-form keying: two clients
running `SELECT id FROM posts WHERE org_id = 7` share an arrangement.
Heuristic for memory:

- Each unique canonical key: `O(rows in scope) × bytes-per-row` for
  the arrangement, plus a small constant per subscriber.
- Plan capacity by **distinct canonical-key count**, not raw
  subscription count.

The `palimpsest_unique_canonical_keys` gauge is the most actionable
signal; if it grows linearly with subscriptions you have an
over-personalised query shape (typically `$user.id` in the predicate
without a sharing dimension).

## Runbook & oncall

- [`RUNBOOK.md`](RUNBOOK.md) covers slot stuck, channel saturation,
  client-side resync storms.
- [`DATABASE-MIGRATION-RESILIENCE.md`](DATABASE-MIGRATION-RESILIENCE.md)
  covers planned DDL and high-volume data backfills.
- [`SECURITY-PROCESS.md`](SECURITY-PROCESS.md) covers vuln intake and
  the weekly RustSec sweep.
- [`THREAT-MODEL.md`](THREAT-MODEL.md) covers the threat surface.

## Upgrade procedure

1. Drain a replica: send `SIGTERM`, the server emits `Resync` to all
   live subscriptions and exits cleanly within `shutdown_grace`.
2. Swap the binary.
3. Bring it back; clients resync against the new instance.
4. Repeat per-replica.

The wire protocol is versioned (`palimpsest-proto/VERSIONING.md`); a
client built against `v1.x` is forward-compatible across `v1.y`
servers.
