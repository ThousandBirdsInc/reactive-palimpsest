# PaaS Observability

This directory contains the first operator observability artifacts for the
owned Palimpsest PaaS runtime.

- `alerts/palimpsest-paas.rules.yaml` defines initial Prometheus alert rules
  for node-agent command failures, managed Postgres backup/WAL/PITR/restore
  and standby-check health, gateway errors, and quota pressure.
- `dashboards/palimpsest-paas-overview.json` is a compact Grafana dashboard
  for the same first set of signals, including managed Postgres lifecycle,
  storage allocation, host storage pressure, and recoverability freshness.

Metric names match the initial Rust service surfaces. The gateway exposes
Prometheus text at `/metrics`, and the SQL-backed control plane exposes
aggregate control-plane, quota, billing, and bounded-cardinality managed
Postgres metrics at `/metrics`, including per-cluster lifecycle state,
allocated storage, backup freshness, WAL archive freshness, PITR continuity
freshness, restore-drill freshness, and standby-check freshness.

`paas/ci/validate-json-schemas.rb` validates the dashboard JSON structure,
required panel metric coverage, and alert-rule shape in addition to the PaaS
contract examples.
