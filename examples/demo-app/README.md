# Palimpsest Tracker — Demo App

A Linear-style issue tracker where **every view is a live SQL
subscription**: a React + WASM browser client against a Rust write API,
a real Postgres, and an embedded Palimpsest SyncEngine.

The demo exercises the production shape: the application owns writes
through HTTP, Postgres owns the data, Palimpsest tails the WAL and owns
live SQL subscriptions, and the browser keeps every view updated from
one long-lived connection.

## What You Get

- React + Vite frontend served by nginx on `http://localhost:18080`.
- Rust `axum` write API on `http://localhost:13017`.
- Embedded Palimpsest gRPC SyncEngine on `localhost:56051`.
- Browser-compatible WebSocket bridge at `/ws/subscribe`.
- A Postgres-backed `issues` table seeded with ~1,400 issues of
  believable history (statuses, priorities, assignees, projects,
  completion dates) so the analytics land populated.
- **Board** (`#/`): a five-column kanban driven by two subscriptions —
  open issues as row-level WAL diffs, and a "recently done" column
  maintained as an incremental `ORDER BY … LIMIT` TopK. Create, move,
  reprioritize, assign, and delete issues.
- **Analytics** (`#/analytics`): live `GROUP BY`/CTE aggregates — KPI
  tiles, issues by workflow stage, daily throughput, workload by
  assignee, and cycle time by project. The browser only ever receives
  the aggregate rows.
- **Local-first** (`#/local-first`): a pglite (Postgres-in-WASM) mirror
  of the permissioned subset with optimistic writes reconciled against
  the WAL — see [docs/LOCAL-FIRST.md](../../docs/LOCAL-FIRST.md).
- An **activity simulator** (top bar): synthetic team events pushed
  through the ordinary write API so every page visibly moves.
- A permissions-DSL playground: edit the TOML rule DSL in the browser,
  apply it to the running SyncEngine (`PUT /api/permissions` →
  `PalimpsestHandle::update_permissions`), and watch every live
  subscription resync under the new rules. The default rule hides the
  `security` project from non-admin personas. Rejected rule sets
  surface the compile error inline and leave the active rules
  untouched.

## Architecture

```text
Browser
  |
  | http://localhost:18080
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
  |     GET    /api/permissions
  |     PUT    /api/permissions
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
http://localhost:18080
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

By default the script maps the Docker services to less common host ports:
`WEB_PORT=18080`, `API_PORT=13017`, and `GRPC_PORT=56051`. Set any of those
environment variables before running the script to override them.

You can also run Compose directly:

```sh
docker compose up --build
docker compose down
```

## Ports

| Port | Owner | Purpose |
| --- | --- | --- |
| `18080` | nginx web container | Browser UI and same-origin proxy. |
| `13017` | Rust server | Write API and WebSocket bridge. |
| `56051` | Rust server | Palimpsest gRPC SyncEngine. Not directly browseable. |

## Using the App

The app has three pages behind a hash router, all sharing one persona
picker and one WebSocket connection:

- `#/` — **Board**: the kanban. Hover a card for its actions (move
  between columns, change priority, assign, delete). Create issues with
  the composer; filter by project or "only mine". The permissions
  playground lives at the bottom of this page.
- `#/analytics` — **Analytics**: live aggregate dashboards. Turn on
  "team activity" in the top bar and watch the KPIs, the throughput
  columns, and the workload bars move as the simulator files, triages,
  progresses, and completes issues through the ordinary write API.
- `#/local-first` — **Local-first**: see the next section.

Switching personas (Alice is an admin; Bob and Carol are members) mints
a fresh JWT and reconnects every subscription under the new `$user.*`
context — with the default rules, `security`-project issues vanish for
members on all three pages.

## Local-First Replica Page

The `#/local-first` page (see [docs/LOCAL-FIRST.md](../../docs/LOCAL-FIRST.md)
for the architecture) runs a real Postgres-in-WASM engine —
[pglite](https://github.com/electric-sql/pglite), loaded lazily so the
live-queries page doesn't pay for it — inside the tab and points a
Palimpsest `LocalReplica` at it:

- The replica mirrors the **permissioned subset** of `posts` into the
  local engine (one atomic local transaction per remote commit) and
  acks LSNs so reconnects resume incrementally.
- The "same query, two engines" panel runs identical SQL as a live
  server subscription *and* against the local mirror, timing the local
  answer — including the CTE aggregate.
- Writes are optimistic: `replica.mutate(...)` applies locally at once
  and forwards through the demo's ordinary HTTP write API (`writer`
  callback). Inserts use client-chosen timestamp-scale ids so the
  optimistic row and the WAL row share a primary key. The event log
  shows each mutation settling on the WAL round-trip (or rolling back
  when the API rejects it).
- Switching personas — or applying different rules in the board
  page's permissions playground — re-snapshots the local database to a
  different permissioned subset, live.

## HTTP API

The API speaks JSON.

### Health

```sh
curl http://localhost:13017/api/health
```

### List Issues

```sh
curl http://localhost:13017/api/issues
```

### Create Issue

```sh
curl -X POST http://localhost:13017/api/issues \
  -H 'content-type: application/json' \
  -d '{"title":"Hello from curl","project":"clients","priority":3}'
```

Optional fields: `id` (client-chosen, used by the local-first page),
`status`, `assignee`, `estimate`.

### Update Issue

Any of `title`, `status`, `priority`, `assignee`, `estimate`. Moving an
issue into `done` stamps `completed_day`/`cycle_days`; moving it out
clears them.

```sh
curl -X PATCH http://localhost:13017/api/issues/1 \
  -H 'content-type: application/json' \
  -d '{"status":"in_progress"}'
```

### Delete Issue

```sh
curl -X DELETE http://localhost:13017/api/issues/1
```

### Simulate Team Activity

```sh
curl -X POST http://localhost:13017/api/simulate \
  -H 'content-type: application/json' \
  -d '{"events":10}'
```

Applies up to 200 synthetic events per call (create / progress /
triage / cancel), each as its own transaction so subscribers see one
diff batch per event.

## Frontend Subscriptions

The board subscribes to open issues and a TopK of recent completions:

```sql
SELECT id, title, status, priority, assignee, project, estimate
FROM issues
WHERE status IN ('backlog', 'todo', 'in_progress', 'in_review')
```

```sql
SELECT id, title, status, priority, assignee, project, estimate, completed_day
FROM issues
WHERE status = 'done'
ORDER BY completed_day DESC, id DESC
LIMIT 12
```

The analytics page runs live aggregates — for example:

```sql
SELECT status, COUNT(*) AS n
FROM issues
GROUP BY status
```

```sql
SELECT completed_day, COUNT(*) AS n, SUM(estimate) AS points
FROM issues
WHERE status = 'done'
GROUP BY completed_day
ORDER BY completed_day DESC
LIMIT 42
```

```sql
WITH completed AS (
  SELECT project, cycle_days
  FROM issues
  WHERE status = 'done'
)
SELECT project, COUNT(*) AS n, AVG(cycle_days) AS avg_cycle_days
FROM completed
GROUP BY project
```

The dataflow compiles each into an incremental plan
(BaseTable → Filter → Aggregate → TopK) and ships aggregate-row
retract/assert deltas, never raw issues. One current engine limit worth
knowing: the compiled plan advertises output column 0 as the row
identity, so live queries group by exactly **one** key each (the
workload chart runs one `GROUP BY assignee` subscription per stage
rather than a two-key `GROUP BY assignee, status`).

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

Stop the conflicting process, override `WEB_PORT`, `API_PORT`, or
`GRPC_PORT` for Docker runs, or run the server locally with alternate
addresses as shown in "Alternate Ports". The Compose file defaults to host
ports `18080`, `13017`, and `56051`.

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
