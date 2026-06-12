#!/usr/bin/env bash
# One-shot launcher for the Palimpsest demo. Builds the server image,
# builds the web image (Rust → wasm → Vite → nginx), and brings both
# containers up.
#
# Usage:
#   ./run.sh             # build + up (foreground, Ctrl-C to stop)
#   ./run.sh --detach    # build + up -d (background)
#   ./run.sh --rebuild   # force a clean rebuild before bringing up
#   ./run.sh --logs      # tail logs of already-running containers
#   ./run.sh --down      # stop and remove containers
#   ./run.sh --help
# Defaults:
#   WEB_PORT=18080 API_PORT=13017 GRPC_PORT=56051

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

WEB_PORT="${WEB_PORT:-18080}"
API_PORT="${API_PORT:-13017}"
GRPC_PORT="${GRPC_PORT:-56051}"
export WEB_PORT API_PORT GRPC_PORT

usage() {
    sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

# Pick `docker compose` (v2) over `docker-compose` (v1) when both exist.
compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose "$@"
    elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose "$@"
    else
        echo "error: neither 'docker compose' nor 'docker-compose' is available." >&2
        echo "       install Docker Desktop or the docker-compose plugin." >&2
        exit 1
    fi
}

require_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        echo "error: 'docker' not found in PATH." >&2
        echo "       install Docker Desktop: https://docs.docker.com/get-docker/" >&2
        exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
        echo "error: cannot reach the Docker daemon (is Docker Desktop running?)." >&2
        exit 1
    fi
}

mode="up"
detach=""
rebuild=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --detach|-d)  detach="--detach" ;;
        --rebuild)    rebuild="1" ;;
        --logs)       mode="logs" ;;
        --down)       mode="down" ;;
        --help|-h)    usage ;;
        *) echo "unknown flag: $1 (use --help)"; exit 2 ;;
    esac
    shift
done

require_docker

case "$mode" in
    down)
        compose down
        echo "stopped."
        exit 0
        ;;
    logs)
        compose logs --tail=100 --follow
        exit 0
        ;;
    up)
        if [[ -n "$rebuild" ]]; then
            echo "→ rebuilding images from scratch..."
            compose build --no-cache
        fi

        echo "→ bringing up demo (this may take a few minutes the first time"
        echo "  while Rust + wasm-bindgen + npm install run)..."
        echo

        if [[ -n "$detach" ]]; then
            compose up --build --detach
            cat <<EOF

demo up. open one of:

  web UI                 → http://localhost:${WEB_PORT}
  write API              → http://localhost:${API_PORT}/api/posts
  palimpsest gRPC-Web    → http://localhost:${GRPC_PORT} (gRPC, not browseable)

stop with:  ./run.sh --down
tail logs:  ./run.sh --logs
EOF
        else
            cat <<EOF

once startup completes, open:

  web UI                 → http://localhost:${WEB_PORT}
  write API              → http://localhost:${API_PORT}/api/posts

Ctrl-C in this terminal to stop both containers.

EOF
            compose up --build
        fi
        ;;
esac
