# ADR 0003: Kubernetes + CloudNativePG Runtime

**Status:** Accepted
**Date:** 2026-06-14
**Supersedes:** [ADR 0002](0002-owned-rust-runtime-no-kubernetes.md)

## Context

ADR 0002 committed the PaaS to an owned, non-Kubernetes runtime: a Rust node
agent that leased commands from the control plane and ran PostgreSQL binaries
directly on registered hosts, a placement engine that scheduled clusters onto
those hosts, and systemd units plus host-image bootstrap scripts to operate the
fleet.

That model re-implemented, by hand, capabilities that mature Kubernetes
operators already provide: scheduling and bin-packing, failure-domain spreading,
rolling updates, instance failover, volume provisioning and resize, and backup
orchestration. The owned node agent and host fleet were a large surface to
build, secure, and maintain, and they made every environment a bespoke host
setup rather than a portable workload.

[CloudNativePG] is a CNCF project and the de-facto standard operator for running
PostgreSQL on Kubernetes. It covers exactly the lifecycle the node agent
implemented — primary/replica topology, synchronous replication, switchover and
failover, base backups and WAL archiving to object storage, point-in-time
recovery, online minor upgrades, and storage expansion.

## Decision

The managed-database runtime is Kubernetes, with managed PostgreSQL provided by
the CloudNativePG operator and the platform packaged with Helm:

- The control plane persists desired state and, on reconcile, renders it into
  CloudNativePG `Cluster` (and `ScheduledBackup` / `Pooler`) manifests via the
  `palimpsest-paas-runtime` crate, then applies them with its in-cluster
  service account. It no longer queues commands for a host agent.
- Managed PostgreSQL lifecycle work — placement, failover, backups, PITR,
  minor/major upgrades, storage resize — is owned by CloudNativePG.
- The platform (control plane, gateway, database proxy, operator console, and
  the control plane's own metadata database) ships as the
  `paas/deploy/helm/palimpsest-paas` Helm chart. The metadata database is itself
  a CloudNativePG `Cluster`.
- Local development uses a `kind` cluster with the same chart and operator,
  exercising the real reconciliation path.

The following are removed: the `palimpsest-paas-node-agent` crate, the host
placement engine, the systemd units, and the host-image bootstrap scripts.

## Consequences

- The platform inherits Kubernetes scheduling, self-healing, and a large
  ecosystem instead of an owned host fleet and supervisor.
- Operators need a Kubernetes cluster and the CloudNativePG operator; there are
  no owned database hosts to image, patch, or harden directly — that moves to
  the node pool / managed Kubernetes layer.
- The control plane requires RBAC to manage `postgresql.cnpg.io` resources,
  Secrets, and (for per-environment isolation) Namespaces.
- Cluster provisioning and deletion reconcile to CloudNativePG today (the
  reconcile loop renders + applies a `Cluster`, and delete removes it). The
  remaining cluster operations (resume/pause, resize, backup, restore, clone,
  branch, standby, failover, and major/minor upgrade) still carry the legacy
  host-command shape and currently error on the Kubernetes runtime; they are
  being converted to CloudNativePG-native equivalents (storage expansion,
  `Backup`/recovery, replica clusters, and image-tag changes).
- The control plane's legacy host-fleet and agent-command persistence (node-host
  tables, the `agent_commands` queue, and related endpoints/UI) is deprecated
  and slated for removal; it is retained only until those operations and their
  read paths are migrated.
- Any future decision to leave Kubernetes must justify itself against this
  operator-based model.

[CloudNativePG]: https://cloudnative-pg.io
