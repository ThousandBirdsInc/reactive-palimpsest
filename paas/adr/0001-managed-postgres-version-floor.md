# ADR 0001: Managed PostgreSQL Version Floor

**Status:** Accepted
**Date:** 2026-05-17

## Context

The PaaS runs PostgreSQL for customers as a managed database product. That
runtime is part of the product surface: it defines the logical replication
features, backup tooling, operational behavior, and local development shape we
must support.

Supporting a wide version range would force the control plane, node agent,
backup/restore flow, and SyncDeployment wrapper to branch across PostgreSQL
behaviors. It would also make local reproduction less faithful to hosted
runtime behavior.

## Decision

Managed PostgreSQL supports PostgreSQL 18 and newer only.

The local developer stack also uses PostgreSQL 18 or newer. Version validation
belongs in shared PaaS models so control-plane APIs, specs, node-agent
commands, and tests all reject older versions consistently.

## Consequences

- The managed runtime can use PostgreSQL 18 behavior as the support floor for
  logical replication, backup, restore, and role configuration.
- Local development mirrors the hosted database version policy.
- The control plane should fail fast when a managed cluster intent asks for
  PostgreSQL 17 or older.
- Customer import and migration tooling must handle upgrades into PostgreSQL
  18+ before a database is accepted into the managed fleet.
- Future PostgreSQL major versions can be enabled by policy and test coverage
  without preserving compatibility branches for older managed versions.
