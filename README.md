<p align="center">
  <img src=".github/palimpsest-banner.svg" alt="Palimpsest" width="720" />
</p>

<h1 align="center">Palimpsest</h1>

<p align="center">
  A <b>Postgres WAL-backed live query sync engine</b>. It keeps SQL query
  result sets current from a logical replication stream and pushes
  <b>row-level diffs</b> to clients over the Palimpsest <code>SyncEngine</code>
  protocol &mdash; a Rust server, WAL decoder, SQL frontend, permission
  rewriter, and native + WASM clients in one repository.
</p>

<p align="center">
  <a href="#quick-start-demo-app"><b>🚀 Quick Start</b></a> &middot;
  <a href="DESIGN.md"><b>📐 Design</b></a> &middot;
  <a href="docs/ARCHITECTURE.md"><b>🏛️ Architecture</b></a> &middot;
  <a href="docs/USER-GUIDE.md"><b>📚 User Guide</b></a> &middot;
  <a href="docs/TROUBLESHOOTING.md"><b>🔧 Troubleshooting</b></a>
</p>

## Status

Palimpsest is under active development. Core crates, the CLI, the browser
client path, and the demo app are present, but some production execution
paths are still being completed. See [DESIGN.md](DESIGN.md) for the
implementation plan and [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
for known operational failure modes.

## Positioning

If you know [Convex](https://www.convex.dev/), Palimpsest is aimed at a
similar application model: queries that stay live, clients that receive
realtime updates, and backend tooling that removes most cache invalidation
and WebSocket plumbing from application code. The main difference is that
Palimpsest is open source and built on top of Postgres: your source of truth
is a standard PostgreSQL database, and Palimpsest derives live query updates
from the Postgres WAL.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `crates/palimpsest-cli` | `palimpsest` command-line binary for running and validating a server config. |
| `crates/palimpsest-server` | Embeddable SyncEngine server, subscription router, auth wiring, metrics, and cursor pumping. |
| `crates/palimpsest-client` | Native and WASM-capable Rust client for the SyncEngine protocol. |
| `crates/palimpsest-client-js` | `wasm-bindgen` wrapper used by browser clients. |
| `packages/palimpsest-client-typescript` | TypeScript wrapper and React hooks for the WASM client. |
| `crates/palimpsest-proto` | Generated protobuf and gRPC types plus protocol versioning helpers. |
| `crates/palimpsest-wal` | Typed Postgres WAL ingest and `pgoutput` decoding. |
| `crates/palimpsest-sql` | SQL parsing, validation, normalization, and MIR lowering. |
| `crates/palimpsest-dataflow` | Incremental view maintenance and dataflow execution pieces. |
| `crates/palimpsest-permissions` | Permission-rule DSL, compilation, and query rewriting. |
| `crates/palimpsest-test-harness` | Postgres-free fixtures and harness utilities. |
| `crates/palimpsest-integration-tests` | Full-stack integration scenarios. |
| `crates/palimpsest-conformance` | Opt-in real-Postgres conformance harness. |
| `crates/palimpsest-properties` | Property tests for correctness invariants. |
| `crates/palimpsest-soak` | Soak and load-test binaries. |
| `examples/demo-app` | Dockerized React + WASM + Rust end-to-end demo. |
| `docs` | Architecture, user, operator, security, TLS, migration, and runbook docs. |
| `paas` | Managed Postgres + Palimpsest platform design and implementation plan. |
| `xtask` | Project maintenance commands. |

## Quick Start: Demo App

The fastest way to see Palimpsest working end-to-end is the demo app:

```sh
cd examples/demo-app
./run.sh
```

Open:

```text
http://localhost:8080
```

The demo starts a Rust write API, an embedded Palimpsest SyncEngine, a
WebSocket bridge for browser subscriptions, and a React + WASM frontend.
See [examples/demo-app/README.md](examples/demo-app/README.md) for the
full walkthrough.

## Building The Workspace

Requirements:

- Rust toolchain compatible with the workspace MSRV in `Cargo.toml`.
- `protobuf-compiler` available as `protoc`.
- Docker, if you want to build the production-style image or run the demo.
- Node.js 20 or newer for the TypeScript package and demo frontend.
- `wasm32-unknown-unknown` plus `wasm-bindgen-cli` for browser WASM work.

Common checks:

```sh
cargo fmt --all --check
cargo build --workspace --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo deny check
```

Build the CLI:

```sh
cargo build --release --bin palimpsest
```

Install the CLI from a local checkout:

```sh
cargo install --path crates/palimpsest-cli
```

Install the latest published CLI from crates.io:

```sh
cargo install palimpsest-cli
```

Release the CLI crate set to crates.io:

```sh
./publish.sh
```

Build the Docker image:

```sh
docker build -t palimpsest:dev .
```

## CLI

The main binary is `palimpsest`.

```sh
cargo run -p palimpsest-cli -- help
```

Supported commands:

| Command | Purpose |
| --- | --- |
| `serve [config]` | Run the embedded server. This is the default command. |
| `validate-config <config>` | Parse TOML config and compile permission rules. |
| `permissions eval <config> --query <sql>` | Compile configured permissions and show the before/after query rewrite. |
| `dump-catalog [config]` | Print the configured catalog as JSON. |
| `dev up\|down\|reset\|status\|env` | Manage the local PostgreSQL 18 + Palimpsest PaaS stack. |
| `db create` | Create a SQL-backed managed PostgreSQL 18+ cluster intent. |
| `db psql` | Open `psql` against the local stack or a configured database URL. |
| `slot-info <config>` | Print replication slot status when built with `--features slot-info`. |

Example config:

```sh
crates/palimpsest-cli/palimpsest.example.toml
```

Evaluate the permissions model against a query:

```sh
palimpsest permissions eval palimpsest.toml \
  --query 'SELECT id FROM posts' \
  --user id=42
```

Validate it with:

```sh
cargo run -p palimpsest-cli -- validate-config crates/palimpsest-cli/palimpsest.example.toml
```

Start the local PaaS stack with PostgreSQL 18:

```sh
cargo run -p palimpsest-cli -- dev up
```

Print `.env`-compatible local settings with:

```sh
cargo run -p palimpsest-cli -- dev env
```

Open a local database shell with:

```sh
cargo run -p palimpsest-cli -- db psql --local
```

## Clients

Native Rust clients use `crates/palimpsest-client`.

Browser clients use:

- `crates/palimpsest-client-js` for the WASM transport and codec.
- `packages/palimpsest-client-typescript` for typed subscriptions and
  React hooks.

Useful docs:

- [docs/WASM-CLIENT.md](docs/WASM-CLIENT.md)
- [packages/palimpsest-client-typescript/README.md](packages/palimpsest-client-typescript/README.md)

## SQL And Permissions

The SQL frontend parses and lowers supported query shapes into
Palimpsest MIR. Permission rules are compiled separately and rewrite
queries before execution.

Start with:

- [docs/supported-sql.md](docs/supported-sql.md)
- [docs/USER-GUIDE.md](docs/USER-GUIDE.md)
- [docs/PERMISSIONS.md](docs/PERMISSIONS.md)

## Operations And Security

Operational docs live under `docs/`:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/OPERATOR-GUIDE.md](docs/OPERATOR-GUIDE.md)
- [docs/RUNBOOK.md](docs/RUNBOOK.md)
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
- [docs/TLS.md](docs/TLS.md)
- [docs/THREAT-MODEL.md](docs/THREAT-MODEL.md)
- [docs/SECURITY-PROCESS.md](docs/SECURITY-PROCESS.md)
- [docs/MIGRATION-FROM-REST.md](docs/MIGRATION-FROM-REST.md)
- [docs/PAAS-DESIGN.md](docs/PAAS-DESIGN.md)
- [paas/IMPLEMENTATION-PLAN.md](paas/IMPLEMENTATION-PLAN.md)
- [paas/MANAGED-POSTGRES-DESIGN.md](paas/MANAGED-POSTGRES-DESIGN.md)

## Maintenance Commands

Project-specific commands are exposed through `xtask`:

```sh
cargo run -p xtask -- help
```

Available tasks include:

| Task | Purpose |
| --- | --- |
| `regen-fixtures` | Regenerate Postgres-backed fixtures. |
| `check-reference-budget` | Check reference implementation budget. |
| `check-wasm-size` | Build the WASM client and enforce the gzip size budget. |
| `check-coverage --lcov <path>` | Check coverage input. |
| `check-bench-regression --threshold <pct>` | Check benchmark regression budget. |
| `render-bench-dashboard --input <ndjson> --out <dir>` | Render benchmark dashboard output. |

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
