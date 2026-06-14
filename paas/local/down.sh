#!/usr/bin/env bash
# Copyright 2026 Thousand Birds Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Tear down the local Palimpsest PaaS kind cluster.
set -euo pipefail

CLUSTER_NAME="${PALIMPSEST_PAAS_KIND_CLUSTER:-palimpsest-paas}"

command -v kind >/dev/null 2>&1 || { echo "error: 'kind' not found on PATH" >&2; exit 1; }

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo ">> deleting kind cluster '$CLUSTER_NAME'"
  kind delete cluster --name "$CLUSTER_NAME"
else
  echo ">> kind cluster '$CLUSTER_NAME' is not running"
fi
