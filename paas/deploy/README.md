# PaaS Deploy Artifacts

The managed PaaS runs on **Kubernetes**, with managed PostgreSQL provided by the
[CloudNativePG] operator and the platform packaged as a **Helm chart**. This
supersedes the former owned host runtime — the systemd units, host-image
bootstrap scripts, and `palimpsest-paas-node-agent` were removed. See
[`../adr/0003-kubernetes-cloudnativepg-runtime.md`](../adr/0003-kubernetes-cloudnativepg-runtime.md).

## Helm chart

[`helm/palimpsest-paas/`](helm/palimpsest-paas) deploys the control plane, its
CloudNativePG-backed metadata database, the gateway, the database proxy, the
operator console, and (optionally) the CloudNativePG operator itself.

```sh
helm dependency build helm/palimpsest-paas
helm install paas helm/palimpsest-paas \
  --namespace palimpsest-paas --create-namespace
```

See the [chart README](helm/palimpsest-paas/README.md) for values and details.

## How managed databases run

The control plane persists desired state and, on reconcile, renders it into
CloudNativePG `Cluster` (and `ScheduledBackup`) manifests via the
`palimpsest-paas-runtime` crate, applying them with its in-cluster service
account. CloudNativePG then performs host-local lifecycle work — placement,
failover, backups, PITR, minor/major upgrades, and storage resize.

There are no owned database hosts, node agents, or host bootstrap steps to
operate: nodes, scheduling, and process supervision are Kubernetes' job.

[CloudNativePG]: https://cloudnative-pg.io
