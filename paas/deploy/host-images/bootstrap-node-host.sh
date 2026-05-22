#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SYSTEMD_DIR="${ROOT}/deploy/systemd"

PALIMPSEST_USER="${PALIMPSEST_USER:-palimpsest}"
PALIMPSEST_GROUP="${PALIMPSEST_GROUP:-palimpsest}"
CONFIG_DIR="${CONFIG_DIR:-/etc/palimpsest/paas}"
RUNTIME_ROOT="${PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT:-/var/lib/palimpsest}"
LOG_DIR="${LOG_DIR:-/var/log/palimpsest}"
SYSTEMD_TARGET_DIR="${SYSTEMD_TARGET_DIR:-/etc/systemd/system}"

require_root() {
  if [[ "$(id -u)" != "0" ]]; then
    echo "bootstrap-node-host.sh must run as root" >&2
    exit 1
  fi
}

ensure_user() {
  if ! getent group "$PALIMPSEST_GROUP" >/dev/null; then
    groupadd --system "$PALIMPSEST_GROUP"
  fi
  if ! id "$PALIMPSEST_USER" >/dev/null 2>&1; then
    useradd \
      --system \
      --gid "$PALIMPSEST_GROUP" \
      --home-dir "$RUNTIME_ROOT" \
      --shell /usr/sbin/nologin \
      "$PALIMPSEST_USER"
  fi
}

install_directories() {
  install -d -m 0750 -o "$PALIMPSEST_USER" -g "$PALIMPSEST_GROUP" "$RUNTIME_ROOT"
  install -d -m 0750 -o "$PALIMPSEST_USER" -g "$PALIMPSEST_GROUP" "$RUNTIME_ROOT/postgres"
  install -d -m 0750 -o "$PALIMPSEST_USER" -g "$PALIMPSEST_GROUP" "$RUNTIME_ROOT/backups"
  install -d -m 0750 -o "$PALIMPSEST_USER" -g "$PALIMPSEST_GROUP" "$RUNTIME_ROOT/wal"
  install -d -m 0750 -o "$PALIMPSEST_USER" -g "$PALIMPSEST_GROUP" "$LOG_DIR"
  install -d -m 0755 "$CONFIG_DIR"
  install -d -m 0755 "$SYSTEMD_TARGET_DIR"
}

install_config() {
  if [[ ! -f "$CONFIG_DIR/node-agent.env" ]]; then
    install -m 0640 -o root -g "$PALIMPSEST_GROUP" \
      "$SYSTEMD_DIR/palimpsest-paas-node-agent.env.example" \
      "$CONFIG_DIR/node-agent.env"
  fi
}

install_units() {
  install -m 0644 "$SYSTEMD_DIR/palimpsest-paas-node-agent-register.service" "$SYSTEMD_TARGET_DIR/"
  install -m 0644 "$SYSTEMD_DIR/palimpsest-paas-node-agent-heartbeat.service" "$SYSTEMD_TARGET_DIR/"
  install -m 0644 "$SYSTEMD_DIR/palimpsest-paas-node-agent-heartbeat.timer" "$SYSTEMD_TARGET_DIR/"
  install -m 0644 "$SYSTEMD_DIR/palimpsest-paas-node-agent-poll.service" "$SYSTEMD_TARGET_DIR/"
  install -m 0644 "$SYSTEMD_DIR/palimpsest-paas-node-agent-poll.timer" "$SYSTEMD_TARGET_DIR/"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
  fi
}

main() {
  require_root
  ensure_user
  install_directories
  install_config
  install_units
  echo "installed Palimpsest PaaS node host units"
}

main "$@"
