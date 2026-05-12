# palimpsest Helm chart (skeleton)

Status: skeleton — meets the §18.14 "Helm chart skeleton (optional,
post-v1)" line. Production hardening (HPAs, NetworkPolicies, mTLS,
Secret integration for JWT keys, etc.) is post-v1 work.

## Install

```sh
helm install palimpsest ./deploy/helm/palimpsest \
  --set image.repository=ghcr.io/yourorg/palimpsest \
  --set image.tag=0.1.0 \
  --values your-values.yaml
```

## Customising the config

The chart inlines the `palimpsest.toml` server config under
`.Values.config`. The default exposes a no-auth dev configuration on
ports 50051 (gRPC) and 9090 (metrics + `/healthz` + `/readyz`). For
real deployments override `.Values.config` with the schema documented
in [`crates/palimpsest-cli/palimpsest.example.toml`](../../../crates/palimpsest-cli/palimpsest.example.toml).
