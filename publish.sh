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
  palimpsest-cli
)
PUBLISH_RETRY_SLEEP_SECONDS="${PUBLISH_RETRY_SLEEP_SECONDS:-90}"
PUBLISH_RETRY_AFTER_BUFFER_SECONDS="${PUBLISH_RETRY_AFTER_BUFFER_SECONDS:-15}"

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
  local log_file
  local retry_after_epoch
  local now_epoch
  local sleep_seconds

  for attempt in 1 2 3 4 5; do
    echo ""
    echo "==> Publishing ${package} (attempt ${attempt}/5)..."
    log_file="$(mktemp)"
    if cargo publish -p "${package}" 2>&1 | tee "${log_file}"; then
      rm -f "${log_file}"
      return 0
    fi

    if crate_version_published "${package}"; then
      echo "${package} v${version} is already available on crates.io; continuing."
      rm -f "${log_file}"
      return 0
    fi

    if [[ "${attempt}" == "5" ]]; then
      echo "Error: failed to publish ${package}"
      rm -f "${log_file}"
      return 1
    fi

    sleep_seconds="${PUBLISH_RETRY_SLEEP_SECONDS}"
    if retry_after_epoch="$(retry_after_epoch_from_log "${log_file}")"; then
      now_epoch="$(date -u +%s)"
      sleep_seconds=$((retry_after_epoch - now_epoch + PUBLISH_RETRY_AFTER_BUFFER_SECONDS))
      if ((sleep_seconds < 1)); then
        sleep_seconds=1
      fi
      echo "Publish was rate-limited. Waiting until the crates.io retry-after time plus ${PUBLISH_RETRY_AFTER_BUFFER_SECONDS}s."
    fi
    rm -f "${log_file}"

    echo "Publish failed. Waiting ${sleep_seconds}s before retrying..."
    sleep "${sleep_seconds}"
  done
}

retry_after_epoch_from_log() {
  local log_file="$1"
  local retry_after

  retry_after="$(
    grep -Eo 'try again after [A-Za-z]{3}, [0-9]{2} [A-Za-z]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT' "${log_file}" \
      | tail -1 \
      | sed 's/^try again after //' \
      || true
  )"
  if [[ -z "${retry_after}" ]]; then
    return 1
  fi

  date -u -j -f "%a, %d %b %Y %H:%M:%S %Z" "${retry_after}" +%s 2>/dev/null \
    || date -u -d "${retry_after}" +%s 2>/dev/null
}

crate_version_published() {
  local package="$1"

  cargo search "${package}" --limit 1 2>/dev/null \
    | grep -Eq "^${package} = \"${version}\""
}

for package in "${PACKAGES[@]}"; do
  if crate_version_published "${package}"; then
    echo ""
    echo "==> Skipping ${package}; v${version} is already published."
    continue
  fi

  echo ""
  echo "==> Dry-run: ${package}"
  cargo publish -p "${package}" --dry-run

  publish_with_retry "${package}"
done

echo ""
echo "Done. Published palimpsest CLI release v${version}."
echo "Install with: cargo install palimpsest-cli"
