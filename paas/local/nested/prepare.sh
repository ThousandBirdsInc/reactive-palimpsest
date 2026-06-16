#!/usr/bin/env bash
# Copyright 2026 Thousand Birds Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Host-side preparation for running kind nested inside a cgroup-v1 microVM:
# mount the cgroup controllers the node needs, build the patched node image, and
# ensure an IPv4-only podman network. Safe to re-run. Requires root + podman.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_IMAGE_BASE="${PALIMPSEST_PAAS_KIND_NODE_IMAGE:-docker.io/kindest/node:v1.32.2}"
NESTED_IMAGE="${PALIMPSEST_PAAS_KIND_NESTED_IMAGE:-localhost/palimpsest-kindnode-nested:latest}"
NETWORK="${PALIMPSEST_PAAS_KIND_NETWORK:-kind}"
SUBNET="${PALIMPSEST_PAAS_KIND_SUBNET:-10.89.0.0/24}"

# The kind node's systemd (and the kubelet) require these v1 controllers to be
# mounted; a private cgroup namespace cannot create the named systemd hierarchy
# itself, but it can attach to one that already exists on the host.
echo ">> [nested] mounting cgroup v1 controllers (idempotent)"
for c in systemd cpuset hugetlb; do
  mkdir -p "/sys/fs/cgroup/$c"
  if ! mountpoint -q "/sys/fs/cgroup/$c"; then
    if [ "$c" = systemd ]; then
      mount -t cgroup -o none,name=systemd cgroup "/sys/fs/cgroup/$c"
    else
      mount -t cgroup -o "$c" cgroup "/sys/fs/cgroup/$c"
    fi
  fi
done

# kind bind-mounts /lib/modules:ro from the host into every node. The microVM
# ships no kernel modules, so the directory is absent and podman aborts node
# creation with `statfs /lib/modules: no such file or directory`. An empty tree
# satisfies the bind mount; the modules kind needs (overlay, br_netfilter, ...)
# are built into the sandbox kernel, so nothing has to be loaded from it.
echo ">> [nested] ensuring /lib/modules exists for the node bind mount"
mkdir -p "/lib/modules/$(uname -r)"

echo ">> [nested] building patched kind node image '$NESTED_IMAGE'"
podman build --build-arg "NODE_IMAGE=$NODE_IMAGE_BASE" -t "$NESTED_IMAGE" "$HERE" >/dev/null

echo ">> [nested] ensuring IPv4-only podman network '$NETWORK' ($SUBNET)"
podman network exists "$NETWORK" 2>/dev/null || podman network create "$NETWORK" --subnet "$SUBNET" >/dev/null
