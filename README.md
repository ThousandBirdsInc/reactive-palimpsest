# Palimpsest

Palimpsest is a Postgres WAL-backed live query sync engine. It keeps SQL
query result sets current from a logical replication stream and pushes
row-level diffs to clients over the Palimpsest SyncEngine protocol.

The repository contains the Rust server, WAL decoder, SQL frontend,
permission rewriter, native and WASM clients, TypeScript/React client
package, test harnesses, operational docs, and an end-to-end demo app.

## Status

Palimpsest is under active development. Core crates, the CLI, the browser
client path, and the demo app are present, but some production execution
paths are still being completed. See [DESIGN.md](DESIGN.md) for the
implementation plan and [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
for known operational failure modes.

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
| `dump-catalog [config]` | Print the configured catalog as JSON. |
| `dev up\|down\|reset\|status\|env` | Manage the local PostgreSQL 18 + Palimpsest PaaS stack. |
| `db create` | Create a SQL-backed managed PostgreSQL 18+ cluster intent. |
| `db psql` | Open `psql` against the local stack or a configured database URL. |
| `slot-info <config>` | Print replication slot status when built with `--features slot-info`. |

Example config:

```sh
crates/palimpsest-cli/palimpsest.example.toml
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
