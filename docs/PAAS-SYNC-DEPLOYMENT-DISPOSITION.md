# PaaS SyncDeployment / `palimpsest-paas-sync-wrapper` Disposition

**Status:** Decision required (product) + cleanup runbook
**Date:** 2026-06-16
**Related:** [ADR 0003: Kubernetes + CloudNativePG Runtime](../paas/adr/0003-kubernetes-cloudnativepg-runtime.md),
[Host-Agent Removal](./PAAS-HOST-AGENT-REMOVAL.md)

## What this is (and is not)

`palimpsest-paas-sync-wrapper` lives under `paas/` but has **nothing to do with
PostgreSQL or CloudNativePG**. It is the helper for a separate feature,
**SyncDeployment**: hosting Palimpsest's own **sync engine** (`palimpsest serve`)
for a tenant.

It is a one-shot CLI (`paas/crates/palimpsest-paas-sync-wrapper`) that:

- `render-config` — renders the sync engine's TOML from a signed deployment intent,
- `verify-signature` — verifies the intent's **Ed25519** signature,
- `classify-reload`, `plan-start`, `plan-transition` — plans start/reload/drain.

Because CNPG only replaces the *Postgres* runtime, it does **not** replace this.
The two are unrelated capabilities.

## Why it is currently orphaned (broken end-to-end)

SyncDeployment was designed to run on the same **host-agent execution layer** the
k8s migration deleted (see [Host-Agent Removal](./PAAS-HOST-AGENT-REMOVAL.md)).
What remains is only the front half:

- Control plane still exposes the API (`/v1/sync-deployments`,
  `…/lib.rs` `api_create_sync_deployment` / `sql_api_create_sync_deployment`) and
  still enqueues `NodeAgentAction::StartSyncDeployment`
  (`…/lib.rs:1753`, `:3576`) under `OperationKind::StartSyncDeployment`.
- **Nothing consumes those commands** (no node agent), and **no Helm template
  runs the wrapper** — `db-proxy.yaml`, `gateway.yaml`, `control-plane.yaml`,
  `ui.yaml`, `migrations-job.yaml` do not reference it.
- The binary is still built into `Dockerfile.paas` (`--bin
  palimpsest-paas-sync-wrapper`, COPY at the bottom) and tested in
  `.github/workflows/paas.yml`.

Net: **SyncDeployment accepts requests, queues work, and never executes it.** The
wrapper is dead weight at the deployment layer today.

## The decision (yours to make)

This is a roadmap question, not a code question. Pick one.

### Path A — Keep SyncDeployment (it is on the roadmap)
The feature needs a Kubernetes-native execution path, the same kind of rework
Postgres got with CNPG. Concretely:

1. Decide the runtime: run `palimpsest serve` as a CNPG-adjacent workload — a
   `Deployment`/`StatefulSet` per SyncDeployment, rendered by the control plane
   (mirror `palimpsest-paas-runtime`'s renderer pattern) and `kubectl apply`d via
   a `ClusterRuntime`-style seam.
2. Reuse the wrapper's pure logic (config rendering, Ed25519 verification,
   transition planning) inside that path — it stays valuable; only its *invoker*
   changed.
3. Add a Helm template + RBAC for the workload; wire the control plane's
   `start/reload/drain` to apply/patch it instead of enqueuing
   `StartSyncDeployment`.
4. Remove the `StartSyncDeployment` host-command path as part of
   [Host-Agent Removal](./PAAS-HOST-AGENT-REMOVAL.md).

**Until this is built, mark the `/v1/sync-deployments` API as unavailable** (501 /
feature-flag off) so it doesn't advertise a non-functional capability.

### Path B — Shelve / remove it (not building it now)
Remove the orphaned surface so the image and API don't ship a dead feature:

1. **Crate:** delete `paas/crates/palimpsest-paas-sync-wrapper/`; remove it from
   the workspace `members` in the root `Cargo.toml`.
2. **Image:** remove the `--bin palimpsest-paas-sync-wrapper` build line and the
   `COPY … /usr/local/bin/palimpsest-paas-sync-wrapper` line from
   `Dockerfile.paas`.
3. **CI:** remove the `palimpsest-paas-sync-wrapper` package/steps from
   `.github/workflows/paas.yml` (and the `--exclude palimpsest-paas-sync-wrapper`
   in `coverage.yml`, now moot).
4. **Control plane:** remove the `/v1/sync-deployments` routes and handlers,
   `NodeAgentAction::StartSyncDeployment` / `StopSyncDeployment` /
   `ReportSyncDeployment`, `OperationKind::StartSyncDeployment`, and the
   `SyncDeployment` types/methods in `palimpsest-paas-core` + `sql_store`.
5. **Schema:** add a forward Flyway migration dropping any `sync_deployment*`
   tables (do not edit old migrations).
6. **Verify:** `cargo clippy --workspace --all-targets --all-features -- -D
   warnings`; integration suite; Flyway migrate fresh + from a populated DB.

> Steps 4–6 overlap with [Host-Agent Removal](./PAAS-HOST-AGENT-REMOVAL.md);
> sequence them together to avoid touching the same files twice.

## Recommendation
If SyncDeployment is not actively being built this quarter, take **Path B** now —
shipping a non-functional API and an unrun binary is a maintenance and
security-surface cost. Re-introduce it via Path A when it's prioritized; the
wrapper's pure logic is small and easy to resurrect from history.
