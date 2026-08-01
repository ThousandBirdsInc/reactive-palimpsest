# PaaS Host-Agent Subsystem Removal

**Status:** Proposed (cleanup runbook)
**Date:** 2026-06-16
**Related:** [ADR 0003: Kubernetes + CloudNativePG Runtime](../paas/adr/0003-kubernetes-cloudnativepg-runtime.md)

ADR 0003 migrated the managed-Postgres lifecycle to Kubernetes + CloudNativePG
(CNPG) and notes that the legacy host-fleet and agent-command persistence is
"deprecated and slated for removal ... retained only until those operations and
their read paths are migrated." Those operations are now migrated. This document
is the runbook for actually removing the dead subsystem.

## Background: what the host-agent subsystem is

Before ADR 0003 the architecture was **control-plane = brain, node-agents =
hands**:

- The control plane decided what a cluster needed and wrote a row into the
  `agent_command` queue — a `NodeAgentCommand` carrying a `NodeAgentAction`
  (`StartPostgres`, `PreparePostgresStandby`, `RunBaseBackup`, …).
- A `palimpsest-paas-node-agent` process on each registered host (`node_host`)
  leased the next command, ran it against PostgreSQL on the box, and reported
  status.
- The control plane advanced a per-cluster state machine from that report
  (`advance_*_after_agent_command`, `next_state_after_agent_command_for_operation`).

CNPG now owns this. The control plane renders a CNPG `Cluster`/`Backup` custom
resource and `kubectl apply`s it (`palimpsest-paas-control-plane/src/runtime.rs`,
`ClusterRuntime`); Kubernetes and the operator are the "hands." The
`palimpsest-paas-node-agent` binary is already gone.

## Why it still exists

The migration swapped the **call sites** (failover/standby/upgrade handlers now
call `ClusterRuntime`) but left the command/queue/node-host machinery compiled
and wired. It is **obsolete but not dead**: it still compiles, is
endpoint-reachable, and is unit-tested — so it cannot simply be deleted in one
pass.

## Current footprint

| Area | Location | Notes |
|---|---|---|
| Action vocabulary | `paas/crates/palimpsest-paas-core/src/lib.rs` — `NodeAgentAction`, `NodeAgentCommand`, `OperationKind`, host-assignment types | Serde contract persisted in the DB |
| State machine + executors | `…/palimpsest-paas-control-plane/src/sql_store.rs` — `next_state_after_agent_command_for_operation`, `advance_*_after_agent_command` | pure-logic, unit-tested |
| Persistence | `…/sql_store.rs` — **~216 references** to `agent_command` / `node_host` / `host_assignment` | runtime SQL (see risks) |
| HTTP surface | `…/lib.rs` — node-host registration, agent-command lease/complete endpoints (e.g. `/v1/node-hosts/:host_id`), the command-completion handler (~`lib.rs:1873`) | completion endpoint is live |
| Action-kind labels | `…/lib.rs` — `NodeAgentAction` → string match (~`lib.rs:8234`) | |
| Schema | `paas/control-plane/migrations/` — **7 of 50** migrations create these tables: `V1`, `V6`, `V32`, `V34`, `V41`, `V45`, `V48` | verify exact set before dropping |
| Tests | `…/sql_store.rs` `#[cfg(test)]` blocks | exercise the variants |

Already removed (branch `claude/fix-ci`, the first safe slice):
`NodeAgentAction::FencePostgresPrimary` (never constructed post-CNPG — fencing
uses the `cnpg.io/fencing` annotation in `runtime.rs`).

## Constraints that make this risky (read before starting)

1. **No compile-time SQL safety.** `sql_store.rs` uses **297 runtime
   `sqlx::query(...)` calls and 0 compile-checked `query!`**. Dropping a table or
   column will **not** produce a compile error — broken queries only fail at
   runtime against a real Postgres. Every schema-touching step needs validation
   against a live DB.
2. **Integration tests need infra.** The crate's `#[test]` unit tests are pure
   and run anywhere, but the end-to-end paths need Postgres (+ a kind cluster for
   the CNPG path). Stages that change SQL/endpoints must be validated with the
   integration suite, not just `cargo test`.
3. **Persisted enum contract.** `NodeAgentAction` is serialized into
   `agent_command` rows. Removing variants is safe only if no such rows exist
   (they won't for never-emitted actions, but confirm in any long-lived env).

## Staged removal plan

Do these as **separate commits/PRs**, each independently green, in order. Never
edit historical Flyway migrations — only add new forward ones.

### Stage 1 — Stop emitting obsolete actions (compiler-verifiable)
- Audit every construction site of `NodeAgentAction::*` in `lib.rs`. Anything on
  the managed-Postgres lifecycle (prepare/standby/promote/upgrade/minor/resize/
  base-backup/clone) must already route through `ClusterRuntime`; delete any
  remaining producer that still enqueues a host command for these.
- Leave the queue/endpoints in place for now.
- **Verify:** `cargo clippy --workspace --all-targets --all-features -- -D warnings`.

### Stage 2 — Delete now-unreachable Rust (compiler-verifiable)
- Remove the `NodeAgentAction` variants no longer constructed, plus their
  `advance_*_after_agent_command` executors, state-machine arms, label-match
  arms, and the `#[cfg(test)]` cases that build them.
- Remove `OperationKind` variants and `ClusterLifecycleState` states that only
  those actions transitioned through.
- **Verify:** `cargo check/clippy --all-targets --all-features` + `cargo test -p
  palimpsest-paas-control-plane` (pure unit tests).

### Stage 3 — Retire the host-fleet HTTP + read paths (needs a DB)
- Remove node-host registration and agent-command lease/complete endpoints and
  their router registrations.
- Remove the `sql_store` methods behind them (the bulk of the ~216 references).
- Keep the tables for now so a rollback is data-safe.
- **Verify:** integration suite against Postgres; confirm no remaining caller
  references the deleted methods.

### Stage 4 — Drop the schema (needs a DB)
- Add a **new forward migration** `paas/control-plane/migrations/V51__drop_host_agent_tables.sql`
  dropping the host-fleet/agent-command tables (`node_*`, `agent_command*`, …).
- Delete any remaining dead query code.
- **Verify:** run Flyway migrate on a fresh DB and on a clone of a populated DB;
  run the integration suite; confirm `xtask check-coverage` still passes.

### Stage 5 — Re-measure coverage
- Remove the `--exclude palimpsest-paas-*` lines added to
  `.github/workflows/coverage.yml` (at least for `palimpsest-paas-control-plane`)
  now that the large, thinly-tested subsystem is gone, and raise
  `PALIMPSEST_COVERAGE_FLOOR` to the new real number.

## Done when
- No `NodeAgentAction` / `node_host` / `agent_command` references remain outside
  history.
- Flyway migrates cleanly fresh and from a populated DB.
- Integration suite green; control-plane no longer excluded from coverage.
- ADR 0003's "Consequences" bullet about the deprecated host-fleet persistence is
  updated to "removed."
