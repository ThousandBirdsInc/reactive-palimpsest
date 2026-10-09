# palimpsest Helm chart (skeleton)

Status: skeleton — meets the §18.14 "Helm chart skeleton (optional,
post-v1)" line. Production hardening (HPAs, NetworkPolicies, mTLS,
Secret integration for JWT keys, etc.) is post-v1 work.

## Install

```sh
helm install palimpsest ./deploy/helm/palimpsest \
  --set image.tag=0.1.1 \
  --values your-values.yaml
```

The default image is `ghcr.io/thousandbirdsinc/reactive-palimpsest`,
published for linux/amd64 and linux/arm64 by the release workflow on
every `v*` tag; `image.tag` defaults to the chart's `appVersion`. No
build step is involved: the whole deployment is the image plus the
inline config below.

## Customising the config

The chart inlines the `palimpsest.toml` server config under
`.Values.config`. The default exposes a no-auth dev configuration on
ports 50051 (gRPC) and 9090 (metrics + `/healthz` + `/readyz`). For
real deployments override `.Values.config` with the schema documented
in [`crates/palimpsest-cli/palimpsest.example.toml`](../../../crates/palimpsest-cli/palimpsest.example.toml).
