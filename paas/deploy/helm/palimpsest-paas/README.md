# palimpsest-paas Helm chart

Deploys the Palimpsest managed PaaS onto Kubernetes:

- **control plane** — the SQL-backed reconciliation + API service. It renders
  managed-Postgres desired state into [CloudNativePG] `Cluster` manifests and
  applies them to the cluster (it no longer queues commands for a host agent).
- **metadata database** — the control plane's own state, run as a CloudNativePG
  `Cluster` (dogfooding the operator that backs customer databases).
- **gateway** — hosted HTTP ingress and usage accounting for SyncDeployments.
- **db proxy** — raw PostgreSQL TCP proxy for managed Postgres endpoints.
- **operator console (UI)** — the React operator console.
- **CloudNativePG operator** — bundled as a subchart dependency.

This replaces the former owned host runtime (systemd units, host bootstrap
scripts, and the `palimpsest-paas-node-agent`). See
[`paas/adr/0003-kubernetes-cloudnativepg-runtime.md`](../../../adr/0003-kubernetes-cloudnativepg-runtime.md).

## Prerequisites

- Kubernetes 1.28+
- Helm 3.13+
- Container images for the control plane, gateway, UI, and a Flyway migrations
  image bundling `paas/control-plane/migrations` (see `migrations.image`).

## Install

```sh
# Pull the CloudNativePG operator subchart.
helm dependency build paas/deploy/helm/palimpsest-paas

helm install paas paas/deploy/helm/palimpsest-paas \
  --namespace palimpsest-paas --create-namespace \
  --set controlPlane.image.repository=ghcr.io/thousandbirds/palimpsest-paas-control-plane \
  --set runtime.backupObjectStoreBase=s3://my-bucket/managed-postgres
```

If the CloudNativePG operator is already installed cluster-wide, disable the
bundled copy with `--set cloudnative-pg.enabled=false`.

## How managed databases are provisioned

The control plane owns desired state in its metadata database. On reconcile it
renders a CloudNativePG `Cluster` (plus a `ScheduledBackup` when an object store
is configured) using `palimpsest-paas-runtime` and applies it with the
in-cluster service account. The operator then handles instance placement,
failover, backups, PITR, minor/major upgrades, and storage resize — the work the
old node agent did by hand.

The control plane needs RBAC to manage `postgresql.cnpg.io` resources, Secrets,
and (for per-environment namespaces) Namespaces. The chart grants this via
`rbac.create=true` (cluster-scoped by default; set `rbac.clusterWide=false` to
restrict to the release namespace).

## Key values

| Key | Default | Description |
| --- | --- | --- |
| `controlPlane.image.repository` | `ghcr.io/thousandbirds/palimpsest-paas-control-plane` | Control-plane image (must include `kubectl`). |
| `runtime.postgresImageRepository` | `ghcr.io/cloudnative-pg/postgresql` | Managed PostgreSQL image repo. |
| `runtime.namespacePerEnvironment` | `true` | One namespace per environment. |
| `runtime.backupObjectStoreBase` | `""` | Object-store base URI for WAL/base backups. |
| `runtime.backupCredentialsSecret` | `""` | Secret with S3 credentials. |
| `controlPlaneDatabase.instances` | `1` | HA replicas for the metadata DB. |
| `cloudnative-pg.enabled` | `true` | Install the operator subchart. |
| `rbac.clusterWide` | `true` | Grant cluster-scoped runtime RBAC. |

See [`values.yaml`](values.yaml) for the full set.

[CloudNativePG]: https://cloudnative-pg.io
