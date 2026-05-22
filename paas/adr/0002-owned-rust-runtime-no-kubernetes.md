# ADR 0002: Owned Rust Runtime Without Kubernetes

**Status:** Accepted
**Date:** 2026-05-17

## Context

The PaaS must run managed PostgreSQL instances that we operate directly. The
project should not depend on Kubernetes primitives, operators, StatefulSets,
CRDs, Helm charts, or a database partner to provide the managed database
runtime.

PostgreSQL lifecycle operations need tight control over host placement,
filesystem ownership, port allocation, WAL archiving, base backups, restore
preparation, credential configuration, and failure reporting. Those operations
are small enough to model directly in our own Rust services and important
enough that hiding them behind a Kubernetes operator would reduce operational
clarity.

## Decision

The managed database runtime is owned by Palimpsest and implemented as:

- A Rust control plane that persists desired state and commands in PostgreSQL
  using `sqlx`.
- Flyway-managed metadata migrations for the control-plane database.
- Rust node agents that run on database hosts, lease commands, and perform
  host-local PostgreSQL lifecycle work.
- Host-based process or container supervision for PostgreSQL 18+ instances.
- Local development assets that exercise the same control-plane and node-agent
  contracts without Kubernetes.

## Consequences

- The platform keeps direct ownership of database lifecycle behavior and
  customer data-plane operations.
- Host images, system services, filesystem layout, and runtime hardening must
  be designed and tested as first-class platform artifacts.
- Scheduling is intentionally simpler than Kubernetes at the start: the
  control plane places clusters onto known node hosts using explicit capacity
  and health signals.
- The deployment surface remains additive under `paas/` and does not disturb
  the existing standalone Palimpsest server, clients, or Helm artifacts.
- Any future orchestration dependency must justify itself against this owned
  Rust control-plane and node-agent model rather than becoming the default.
