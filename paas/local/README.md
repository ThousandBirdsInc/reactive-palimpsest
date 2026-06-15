# Local PaaS development

The PaaS runs on Kubernetes. Locally that means a [`kind`](https://kind.sigs.k8s.io/)
cluster with the [`palimpsest-paas`](../deploy/helm/palimpsest-paas) Helm chart,
which installs the CloudNativePG operator and the platform components.

## Quickstart

```sh
cd paas
./local/up.sh     # create kind cluster + helm install
# ... work ...
./local/down.sh   # delete the kind cluster
```

`up.sh` honors a few env vars:

- `PALIMPSEST_PAAS_KIND_CLUSTER` — kind cluster name (default `palimpsest-paas`).
- `PALIMPSEST_PAAS_NAMESPACE` — release namespace (default `palimpsest-paas`).
- `PALIMPSEST_PAAS_HELM_ARGS` — extra `helm` args, e.g. image overrides:
  `PALIMPSEST_PAAS_HELM_ARGS="--set controlPlane.image.tag=dev" ./local/up.sh`

You must build and load the control-plane, gateway, and UI images into the kind
cluster (`kind load docker-image ...`) and point the chart at them with
`--set <component>.image.repository=...,<component>.image.tag=...` unless you are
pulling published images.

Managed databases created through the console are reconciled into CloudNativePG
`Cluster` resources:

```sh
kubectl get clusters.postgresql.cnpg.io -A
```

## Legacy fixtures

`palimpsest.toml` and `postgres/` are leftover standalone-Postgres fixtures from
the previous host-based local stack. They are not used by the Kubernetes flow
and will be removed once nothing references them.
