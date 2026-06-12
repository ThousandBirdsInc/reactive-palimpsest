# Palimpsest Documentation

This directory is the documentation set for Palimpsest — the sync engine that
tails Postgres logical replication and streams live SQL query diffs to native,
WASM, and TypeScript clients. This page is the index; start here and follow the
link that matches what you're trying to do.

The authoritative, full-length design reference is
[`../DESIGN.md`](../DESIGN.md) at the repo root. Most pages here are focused
guides or deep-dives that assume that bigger picture.

## Start here

| Doc | What it covers |
|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Condensed architecture overview — build a mental model in five minutes. Distilled from the root `DESIGN.md`. |
| [`../DESIGN.md`](../DESIGN.md) | The authoritative, full design document for the sync engine. |

## Using Palimpsest (application developers)

| Doc | What it covers |
|---|---|
| [`USER-GUIDE.md`](USER-GUIDE.md) | Subscribing to live SQL views from an app, assuming an operator has already deployed the server. |
| [`MIGRATION-FROM-REST.md`](MIGRATION-FROM-REST.md) | Side-by-side mapping for moving from polling REST endpoints to Palimpsest subscriptions. |
| [`WASM-CLIENT.md`](WASM-CLIENT.md) | Minimal HTML + JS quickstart for subscribing to a query from a browser via the WASM client. |
| [`supported-sql.md`](supported-sql.md) | The deliberately small PostgreSQL `SELECT` subset the SQL frontend accepts. |
| [`PERMISSIONS.md`](PERMISSIONS.md) | The row-level access TOML rule DSL, evaluated at subscribe time and folded into every diff. |

## Operating Palimpsest (running it in production)

| Doc | What it covers |
|---|---|
| [`OPERATOR-GUIDE.md`](OPERATOR-GUIDE.md) | The deploy / configure / tune lifecycle for the team running Palimpsest. |
| [`RUNBOOK.md`](RUNBOOK.md) | On-call recovery procedures for the failure modes you're most likely to hit. |
| [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) | Fast triage matrix: symptom → metric → action. |

## Security

| Doc | What it covers |
|---|---|
| [`THREAT-MODEL.md`](THREAT-MODEL.md) | The v1 threat model for `palimpsest-server`: security boundaries, in-scope threats, and explicit non-goals. |
| [`TLS.md`](TLS.md) | TLS termination story — why v1 ships without in-process TLS and how to front it with a proxy. |
| [`SECURITY-PROCESS.md`](SECURITY-PROCESS.md) | The CI dependency-security hooks (`cargo deny`, `cargo audit`). |

## Design deep-dives

| Doc | What it covers |
|---|---|
| [`DATABASE-MIGRATION-RESILIENCE.md`](DATABASE-MIGRATION-RESILIENCE.md) | How DDL and large data migrations are handled against compiled subscriptions and WAL volume. |
| [`TRANSACTIONAL-CLIENT-UPDATES.md`](TRANSACTIONAL-CLIENT-UPDATES.md) | Pushing each upstream Postgres transaction as one atomic, coherent client-visible update. |
| [`EVALUATION-OF-PERMISSIONS.md`](EVALUATION-OF-PERMISSIONS.md) | Draft design for formally evaluating/verifying permission rules beyond the existing rewriter. |
| [`WALRUS-SCALEOUT.md`](WALRUS-SCALEOUT.md) | Execution plan for horizontal server-side scaling via query-shard scale-out (no Kafka). |

## Managed platform (PaaS)

[`PAAS-DESIGN.md`](PAAS-DESIGN.md) is the product and systems design for the
managed platform built around the sync engine. The remaining PaaS design set
lives under [`../paas/`](../paas/):

| Doc | What it covers |
|---|---|
| [`PAAS-DESIGN.md`](PAAS-DESIGN.md) | Top-level product/systems design for managed Postgres + live sync. |
| [`../paas/README.md`](../paas/README.md) | Entry point and current state of the PaaS implementation. |
| [`../paas/MANAGED-POSTGRES-DESIGN.md`](../paas/MANAGED-POSTGRES-DESIGN.md) | PostgreSQL 18+ lifecycle management: the owned Rust control plane, node agents, and host scheduler. |
| [`../paas/PRODUCTION-READINESS-DESIGN.md`](../paas/PRODUCTION-READINESS-DESIGN.md) | Production-readiness checklist, control loops, readiness levels, and launch blockers. |
| [`../paas/PAAS-UI-DESIGN.md`](../paas/PAAS-UI-DESIGN.md) | The routed multi-page operator console design. |
| [`../paas/IMPLEMENTATION-PLAN.md`](../paas/IMPLEMENTATION-PLAN.md) | Incremental, phase-by-phase build plan with per-deliverable status annotations. |
