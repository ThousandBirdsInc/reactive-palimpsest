#!/usr/bin/env bash
# Copyright 2026 Thousand Birds Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Bring up the Palimpsest PaaS on a local kind cluster with Helm + CloudNativePG.
set -euo pipefail

CLUSTER_NAME="${PALIMPSEST_PAAS_KIND_CLUSTER:-palimpsest-paas}"
RELEASE="${PALIMPSEST_PAAS_RELEASE:-paas}"
NAMESPACE="${PALIMPSEST_PAAS_NAMESPACE:-palimpsest-paas}"
CHART_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../deploy/helm/palimpsest-paas" && pwd)"

for bin in kind kubectl helm; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' not found on PATH" >&2; exit 1; }
done

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo ">> creating kind cluster '$CLUSTER_NAME'"
  kind create cluster --name "$CLUSTER_NAME"
else
  echo ">> kind cluster '$CLUSTER_NAME' already exists"
fi

kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null

echo ">> building chart dependencies (CloudNativePG operator)"
helm dependency build "$CHART_DIR"

echo ">> installing release '$RELEASE' into namespace '$NAMESPACE'"
# Override image values via PALIMPSEST_PAAS_HELM_ARGS, e.g.:
#   PALIMPSEST_PAAS_HELM_ARGS="--set controlPlane.image.tag=dev" ./local/up.sh
helm upgrade --install "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" --create-namespace \
  --wait --timeout 10m \
  ${PALIMPSEST_PAAS_HELM_ARGS:-}

cat <<EOF

PaaS is installed. Useful commands:

  kubectl -n $NAMESPACE get pods
  kubectl get clusters.postgresql.cnpg.io -A
  kubectl -n $NAMESPACE port-forward svc/${RELEASE}-palimpsest-paas-ui 8090:80
  kubectl -n $NAMESPACE port-forward svc/${RELEASE}-palimpsest-paas-control-plane 8088:8088

Tear down with: ./local/down.sh
EOF
