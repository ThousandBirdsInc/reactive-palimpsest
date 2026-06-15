#!/usr/bin/env bash
# Copyright 2026 Thousand Birds Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Trust the host's extra CA certificates inside a kind node, so containerd can
# pull images through a TLS-intercepting egress proxy. No-op when the host has
# no extra CAs. Usage: inject-host-cas.sh <node-container-name>
set -euo pipefail

NODE="${1:?usage: inject-host-cas.sh <node-container>}"
shopt -s nullglob
cas=(/usr/local/share/ca-certificates/*.crt)
if [ "${#cas[@]}" -eq 0 ]; then
  echo ">> [nested] no extra host CAs to inject"
  exit 0
fi

echo ">> [nested] injecting ${#cas[@]} host CA(s) into '$NODE'"
for crt in "${cas[@]}"; do
  podman cp "$crt" "$NODE:/usr/local/share/ca-certificates/$(basename "$crt")"
done
podman exec "$NODE" bash -c 'update-ca-certificates >/dev/null 2>&1; systemctl restart containerd; sleep 3'
