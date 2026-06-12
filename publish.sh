#!/usr/bin/env bash
set -euo pipefail

# Publish the crates required by the installable `palimpsest` CLI to crates.io.

PACKAGES=(
  palimpsest-sql
  palimpsest-test-harness
  palimpsest-wal
  palimpsest-permissions
  palimpsest-proto
  palimpsest-dataflow
  palimpsest-server
  palimpsest-paas-core
  palimpsest-cli
)

if [[ ! -f Cargo.toml || ! -d crates/palimpsest-cli ]]; then
  echo "Error: must be run from the repository root"
  exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Error: there are uncommitted changes. Commit before publishing."
  echo ""
  git status --short
  exit 1
fi

version="$(
  cargo metadata --no-deps --format-version 1 \
    | grep -o '"version":"[^"]*"' \
    | head -1 \
    | cut -d'"' -f4
)"

echo "The following crates will be published to crates.io in dependency order:"
for package in "${PACKAGES[@]}"; do
  echo "  - ${package} v${version}"
done

echo ""
read -r -p "Dry-run and publish these crates to crates.io? [y/N] " confirm
if [[ "${confirm}" != [yY] ]]; then
  echo "Aborted."
  exit 0
fi

publish_with_retry() {
  local package="$1"
  local attempt

  for attempt in 1 2 3 4 5; do
    echo ""
    echo "==> Publishing ${package} (attempt ${attempt}/5)..."
    if cargo publish -p "${package}"; then
      return 0
    fi

    if [[ "${attempt}" == "5" ]]; then
      echo "Error: failed to publish ${package}"
      return 1
    fi

    echo "Publish failed. Waiting for crates.io index propagation before retrying..."
    sleep 20
  done
}

for package in "${PACKAGES[@]}"; do
  echo ""
  echo "==> Dry-run: ${package}"
  cargo publish -p "${package}" --dry-run

  publish_with_retry "${package}"
done

echo ""
echo "Done. Published palimpsest CLI release v${version}."
echo "Install with: cargo install palimpsest-cli"
