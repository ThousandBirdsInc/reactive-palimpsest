# Palimpsest Demo App

End-to-end demo that runs a React + WASM browser client against a Rust
write API and an embedded Palimpsest SyncEngine.

The demo is intentionally small, but it exercises the production shape:
the application owns writes through HTTP, Palimpsest owns live SQL
subscriptions, and the browser keeps multiple result sets updated from
one long-lived connection.

## What You Get

- React + Vite frontend served by nginx on `http://localhost:8080`.
- Rust `axum` write API on `http://localhost:3000`.
- Embedded Palimpsest gRPC SyncEngine on `localhost:50051`.
- Browser-compatible WebSocket bridge at `/ws/subscribe`.
- In-memory `posts` table with create, publish/unpublish, and delete
  actions.
- Two live subscriptions:
  - filtered `SELECT id, title, published FROM posts` list
  - counts-by-status query using a CTE
- Snapshot delivery and live insert/update/delete diffs from the demo
  WAL journal.

## Architecture

```text
Browser
  |
  | http://localhost:8080
  v
nginx web container
  |-- serves React + Vite + palimpsest-client-js WASM bundle
  |-- proxies /api/* to server:3000
  `-- proxies /ws/*  to server:3000

server container
  |-- axum write API (:3000)
  |     GET    /api/health
  |     GET    /api/posts
  |     POST   /api/posts
  |     PATCH  /api/posts/:id
  |     DELETE /api/posts/:id
  |
  |-- WebSocket bridge (:3000/ws/subscribe)
  |     browser binary WS frames
  |       <-> palimpsest.sync.v1 ClientMessage/ServerMessage protobufs
  |       <-> in-process tonic client
  |
  `-- Palimpsest SyncEngine (:50051)
        Subscribe RPC
        DemoWalRuntime
        in-memory posts table + append-only diff journal
```

The browser uses `@palimpsest/client/react`, backed by the
`palimpsest-client-js` WASM bundle. In the Docker setup, nginx keeps
everything same-origin, so the frontend does not need CORS-specific
configuration.

## Requirements

For the Docker path:

- Docker Desktop, or Docker Engine with `docker compose`.

For local, non-Docker development:

- Rust toolchain with `wasm32-unknown-unknown`.
- `protobuf-compiler`.
- `wasm-bindgen-cli`.
- Optional: `wasm-opt` from Binaryen for a smaller WASM artifact.
- Node.js 20 or newer.

The Docker build uses Rust 1.88 for image builds because the
`wasm-bindgen-cli` locked dependency set currently requires a newer
compiler than the workspace MSRV.

## Quick Start

From this directory:

```sh
./run.sh
```

Open:

```text
http://localhost:8080
```

The first run can take a few minutes because Docker builds the Rust
server, the WASM client bundle, the TypeScript wrapper package, and the
Vite app.

Use Ctrl-C in the foreground terminal to stop the stack.

## Run Script

`run.sh` is a thin wrapper around Docker Compose.

| Command | Action |
| --- | --- |
| `./run.sh` | Build images and start the stack in the foreground. |
| `./run.sh --detach` | Build images and start the stack in the background. |
| `./run.sh --rebuild` | Force a clean image rebuild before starting. |
| `./run.sh --logs` | Tail logs for already-running containers. |
| `./run.sh --down` | Stop and remove the containers. |
| `./run.sh --help` | Print usage. |

You can also run Compose directly:

```sh
docker compose up --build
docker compose down
```

## Ports

| Port | Owner | Purpose |
| --- | --- | --- |
| `8080` | nginx web container | Browser UI and same-origin proxy. |
| `3000` | Rust server | Write API and WebSocket bridge. |
| `50051` | Rust server | Palimpsest gRPC SyncEngine. Not directly browseable. |

## Using the App

The page starts with three seeded posts. You can:

- create a new published post
- filter the live list by all, published, or drafts
- publish or unpublish an existing row
- delete a row
- watch the counts-by-status chart update independently

Both panels subscribe through Palimpsest. Writes go through the HTTP API,
mutate the in-memory store, append `RawDiff` entries to the demo journal,
and flow back to active subscriptions as live diffs.

## HTTP API

The API speaks JSON.

### Health

```sh
curl http://localhost:3000/api/health
```

Response:

```json
{"status":"ok"}
```

### List Posts

```sh
curl http://localhost:3000/api/posts
```

Response:

```json
[
  {"id":1,"title":"Welcome to Palimpsest","published":true}
]
```

### Create Post

```sh
curl -X POST http://localhost:3000/api/posts \
  -H 'content-type: application/json' \
  -d '{"title":"Hello from curl","published":true}'
```

### Update Publish State

```sh
curl -X PATCH http://localhost:3000/api/posts/1 \
  -H 'content-type: application/json' \
  -d '{"published":false}'
```

### Delete Post

```sh
curl -X DELETE http://localhost:3000/api/posts/1
```

## Frontend Subscriptions

The list view subscribes to one of these SQL queries depending on the
selected filter:

```sql
SELECT id, title, published
FROM posts
```

```sql
SELECT id, title, published
FROM posts
WHERE published = true
```

```sql
SELECT id, title, published
FROM posts
WHERE published = false
```

The chart view subscribes to:

```sql
WITH stats AS (
  SELECT published, COUNT(*) AS n
  FROM posts
  GROUP BY published
)
SELECT published, n
FROM stats
ORDER BY published
```

The current demo server still returns raw `posts` rows for the CTE
subscription, so the React app performs the chart aggregation
client-side. The SQL is still sent through the client and server parsing
path, which is the useful part for this example.

## Local Development Without Docker

Run the Rust server:

```sh
cargo run --manifest-path server/Cargo.toml --release
```

Build the WASM bundle and start Vite:

```sh
cd web
./build-wasm.sh
npm install
VITE_API_URL=http://localhost:3000 \
VITE_PALIMPSEST_URL=http://localhost:3000 \
npm run dev
```

Open:

```text
http://localhost:5173
```

For local Vite development, `VITE_PALIMPSEST_URL` should point at the
HTTP server on port `3000`, because the browser transport uses
`/ws/subscribe`. The raw gRPC port `50051` is for the server-side bridge
and native gRPC clients.

### Alternate Ports

If ports `3000` or `50051` are already in use:

```sh
PALIMPSEST_DEMO_HTTP_ADDR=127.0.0.1:13000 \
PALIMPSEST_DEMO_GRPC_ADDR=127.0.0.1:60051 \
cargo run --manifest-path server/Cargo.toml --release
```

Then start Vite with the matching HTTP endpoint:

```sh
cd web
VITE_API_URL=http://localhost:13000 \
VITE_PALIMPSEST_URL=http://localhost:13000 \
npm run dev
```

## Build Details

The Docker frontend image has four stages:

1. `wasm-builder` builds `palimpsest-client-js` for
   `wasm32-unknown-unknown`, runs `wasm-bindgen`, and optimizes the
   artifact with `wasm-opt`.
2. `ts-builder` builds the local `@palimpsest/client` TypeScript
   package.
3. `web-builder` installs the demo web dependencies and runs
   `npm run build`.
4. `runtime` serves the static Vite output with nginx.

The Docker server image builds the standalone
`palimpsest-demo-server` binary and copies it into a distroless runtime
image.

## File Layout

```text
demo-app/
|-- README.md
|-- docker-compose.yml
|-- run.sh
|-- server/
|   |-- Cargo.toml
|   |-- Dockerfile
|   `-- src/
|       |-- api.rs      # HTTP write API routes
|       |-- main.rs     # process bootstrap, ports, embedded Palimpsest
|       |-- state.rs    # posts store, DemoWalRuntime, diff journal
|       `-- ws.rs       # WebSocket <-> gRPC Subscribe bridge
`-- web/
    |-- Dockerfile
    |-- build-wasm.sh
    |-- index.html      # HTML shell and demo CSS
    |-- nginx.conf      # static serving plus /api and /ws proxying
    |-- package.json
    |-- vite.config.ts
    `-- src/
        |-- App.tsx
        |-- api.ts
        `-- main.tsx
```

## Environment Variables

### Server

| Variable | Default | Description |
| --- | --- | --- |
| `PALIMPSEST_DEMO_HTTP_ADDR` | `0.0.0.0:3000` | HTTP write API and WebSocket bind address. |
| `PALIMPSEST_DEMO_GRPC_ADDR` | `0.0.0.0:50051` | Palimpsest gRPC bind address. |
| `RUST_LOG` | `info,palimpsest_demo_server=debug` | Rust tracing filter. |
| `HEALTHCHECK_TARGET` | `127.0.0.1:3000` | Target used by `--healthcheck`. |

### Web

| Variable | Default | Description |
| --- | --- | --- |
| `VITE_API_URL` | same origin | Base URL for `/api/*` writes. |
| `VITE_PALIMPSEST_URL` | `window.location.origin` | Base URL used by the WASM client transport. |

In Docker, both web variables can stay unset because nginx proxies the
API and WebSocket paths from the same origin.

## Known Limitations

- Data is in memory. Restarting the server resets the seeded posts.
- The demo journal is append-only and unbounded. Real deployments compact
  behind an acknowledged LSN watermark.
- Query execution is deliberately incomplete in this example. The
  frontend shows SQL parsing and subscription wiring, but some projection,
  filtering, and aggregation behavior is still applied client-side until
  the dataflow execution path is fully wired through the server.
- Authentication uses `AnonymousAuthenticator`. The app does not mint or
  validate user tokens.

## Troubleshooting

### Docker daemon is unavailable

Start Docker Desktop or the Docker daemon, then rerun:

```sh
./run.sh
```

### A port is already in use

Stop the conflicting process or run the server locally with alternate
addresses as shown in "Alternate Ports". The Compose file currently maps
fixed host ports `8080`, `3000`, and `50051`.

### The UI loads but never opens a subscription

Check the server and web logs:

```sh
./run.sh --logs
```

In Docker, make sure nginx is proxying `/ws/` and the server logs show
`ws bridge: subscribe stream opened`.

### Local Vite cannot find the WASM module

Build the WASM bundle first:

```sh
cd web
./build-wasm.sh
npm run dev
```

The script writes generated files into `web/pkg`, which is imported by
`web/src/App.tsx`.

## Related Docs

- [`../../docs/WASM-CLIENT.md`](../../docs/WASM-CLIENT.md)
- [`../../docs/USER-GUIDE.md`](../../docs/USER-GUIDE.md)
- [`../../docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md)
- [`../../packages/palimpsest-client-typescript/README.md`](../../packages/palimpsest-client-typescript/README.md)
