# palimpsest-cli

Standalone CLI that boots the embedded `palimpsest-server` (§18.14).
Everything is driven by one TOML file, so a prebuilt binary is a
complete install: no Rust toolchain, no code.

## Install

Prebuilt binaries (Linux x86_64/aarch64 as static musl builds, macOS
x86_64/aarch64) are attached to every
[GitHub Release](https://github.com/ThousandBirdsInc/reactive-palimpsest/releases):

```sh
curl -fsSL https://raw.githubusercontent.com/ThousandBirdsInc/reactive-palimpsest/main/install.sh | sh
```

The script picks the tarball for your OS/CPU, verifies it against the
release's `SHA256SUMS`, and installs to `/usr/local/bin` (or
`~/.local/bin`). Pin a version with `PALIMPSEST_VERSION=v0.1.1`, or
pick an install dir with `PALIMPSEST_INSTALL=…`. Each tarball also
carries `palimpsest.example.toml`, the full config reference.

Container image (linux/amd64 + linux/arm64, distroless, runs as
non-root):

```sh
docker run --rm -p 50051:50051 -p 9090:9090 \
  -v "$PWD/palimpsest.toml:/etc/palimpsest/palimpsest.toml" \
  ghcr.io/thousandbirdsinc/reactive-palimpsest:latest
```

From source, if you do have a toolchain:

```sh
cargo install palimpsest-cli                 # published crate
cargo install --path crates/palimpsest-cli   # local checkout
```

Repository maintainers cut a release by tagging the workspace version
(`git tag v0.1.1 && git push origin v0.1.1`); `.github/workflows/release.yml`
builds the binaries, the GitHub Release, and the image. `./publish.sh`
publishes the crate set to crates.io.

## Subcommands

```
palimpsest serve [config]              # default if no command given
palimpsest validate-config <config> [--offline]
                                       # parse + compile + register queries; 0 ok, 1 error
palimpsest permissions eval <config> --query <sql>
                                       # rewrite query MIR with configured permissions
palimpsest typegen <config> --out <f>  # TypeScript row/param types for registered queries
palimpsest skills install              # install Codex and Claude skills for this CLI
palimpsest dump-catalog [config]       # emit the catalog as JSON on stdout
palimpsest slot-info <config>          # show replication slot status for [database]
palimpsest help
```

If invoked with a single positional argument that isn't a subcommand,
the CLI behaves as `serve <config>` for backwards compatibility.

Exit codes: `0` success, `2` usage error, `1` everything else.

`validate-config`, `permissions eval`, and `dump-catalog` work against
the catalog introspected from `[database]` when it is configured, so
rules and query files that name your real tables are checked exactly
as `serve` would check them. Without `[database]` they use the
built-in demo catalog (tables `posts`, `archived_posts`, `authors`,
`comments`), which is also what `serve` runs against then.

`--offline` never touches the database. For `validate-config` with
`[database]` configured it still parses the config, types the user
schema, and reads every query file, but skips rule compilation and
query registration instead of checking them against the wrong catalog;
for `permissions eval` and `dump-catalog` it substitutes the demo
catalog.

## Quick start against a real Postgres

1. Write a query file (sqlc format):

   ```sql
   -- queries/live.sql
   -- name: OpenTickets :many
   SELECT id, title, owner_id FROM tickets WHERE closed = false;
   ```

2. Write `palimpsest.toml`:

   ```toml
   [grpc]
   addr = "0.0.0.0:50051"

   [metrics]
   addr = "0.0.0.0:9090"

   [database]
   dsn = "postgres://palimpsest:secret@db.internal:5432/app?sslmode=require"

   [queries]
   files = ["queries/live.sql"]

   [auth]
   kind = "jwt"
   jwks_url = "https://issuer.example/.well-known/jwks.json"
   audience = "palimpsest"
   [auth.claim_to_field]
   sub = "id"

   [permissions.user_schema]
   id = "int"

   [[permissions.rules]]
   name = "own_tickets"
   table = "tickets"
   predicate = "owner_id = $user.id"
   ```

3. Validate, then serve:

   ```sh
   palimpsest validate-config palimpsest.toml
   palimpsest serve palimpsest.toml
   ```

   The engine introspects the catalog, derives the streamed tables from
   the registered queries, and creates (or resumes) the slot and
   publication. Postgres needs `wal_level = logical` and a role with
   `REPLICATION` plus `SELECT` on the queried tables.

4. Generate client types from the same catalog:

   ```sh
   palimpsest typegen palimpsest.toml --out src/queries.ts
   ```

## Agent skills

Install the Palimpsest CLI skill for both Codex and Claude:

```sh
palimpsest skills install
```

By default this writes `palimpsest-cli/SKILL.md` under
`$CODEX_HOME/skills` or `~/.codex/skills`, and `$CLAUDE_HOME/skills` or
`~/.claude/skills`. Use `--codex`, `--claude`, `--codex-dir`,
`--claude-dir`, `--dry-run`, or `--force` to control the install.

## Permission evaluator

`palimpsest permissions eval` compiles the same nested `[permissions]`
configuration used by `serve`, lowers one or more queries, rewrites them
with the configured row-visibility rules, and prints canonical MIR before
and after the rewrite.

```sh
palimpsest permissions eval palimpsest.toml \
  --query 'SELECT id FROM posts' \
  --user id=42
```

Use repeated `--query` or `--query-file` options to test multiple queries in
one invocation. User values are typed from `[permissions.user_schema]` and
can be supplied with repeated `--user field=value` options or a JSON object:

```sh
palimpsest permissions eval palimpsest.toml \
  --query 'SELECT id FROM posts' \
  --user-json '{"id":42,"is_admin":false}' \
  --json
```

## Configuration

The CLI reads a single TOML file (default: `./palimpsest.toml`). All
sections are optional; sensible defaults are used for anything you
omit. [`palimpsest.example.toml`](palimpsest.example.toml) is the
annotated, complete reference; the sections are:

| Section | Purpose |
| --- | --- |
| `[grpc]` | `addr` for the gRPC + gRPC-Web + `/ws/subscribe` listener (default `127.0.0.1:50051`). |
| `[metrics]` | `addr` for `/metrics`, `/healthz`, `/readyz`; omit to disable. |
| `[database]` | `dsn`, `slot`, `publication`, `tls_root_ca_file`, `manage_replica_identity`. Turns on the real WAL runtime; requires `[queries]`. |
| `[queries]` | `files` (sqlc-format query files, resolved relative to the config), `inline_sql`. |
| `[auth]` | `kind = "anonymous"` or `"jwt"` with `secret` / `public_key_pem` / `jwks` / `jwks_url`, `algorithm`, `issuer`, `audience`, `leeway_secs`, and `[auth.claim_to_field]`. |
| `[permissions]` | `user_schema` (typed `$user.*` fields) and `[[permissions.rules]]` (`name`, `table`, `predicate`, `mode`). |
| `[upstream]` | Legacy; `slot-info` still reads `url` / `slot_name` / `publication` from it when `[database]` is absent. |

### `[database]`

Supplying `dsn` is the whole of the database-side adoption surface:
the engine introspects the catalog, derives the streamed tables from
the registered queries, and owns the slot, the publication, and
`REPLICA IDENTITY`. Set `manage_replica_identity = false` to refuse to
start rather than issue `ALTER TABLE … REPLICA IDENTITY FULL`.

TLS follows libpq: `sslmode=disable` never negotiates TLS;
`prefer`/`require` encrypt without verifying the server;
`tls_root_ca_file` (PEM) upgrades to full chain + hostname
verification, the equivalent of `sslmode=verify-full`. Use it for
anything that crosses a network you do not own.

### `[queries]`

sqlc-format query files registered at startup (see
[`docs/NAMED-QUERIES.md`](../../docs/NAMED-QUERIES.md)). Registration
failures abort startup naming the query and the exact rejected
construct. Configuring any file disables raw-SQL subscribes unless
`inline_sql = true`.

### `[auth]`

`kind = "anonymous"` (default) accepts every connection with an empty
`UserContext`. Suitable for dev and for trusted-network deployments.

`kind = "jwt"` validates a JWT in the gRPC `authorization` header,
maps configured claims onto user-context fields, and rejects malformed,
expired, or wrong-audience tokens. Supply exactly one key source
(`secret`, `public_key_pem`, `jwks`, or `jwks_url`). See
`palimpsest_server::JwtAuthConfig` for the field shape.

### `[permissions]`

`user_schema` declares the typed shape of `$user.*` references that
permission predicates can use. Field types are `bool`, `int`, `float`,
`text`, `timestamp`, `timestamptz`, `date`, `time`, `interval`,
`numeric`, `bytea`, `uuid`, `jsonb` (alias `json`), `enum`, and
`array`. `rules` are compiled at startup; bad predicates fail
`validate-config` (and `serve`) with a precise diagnostic.

## Health and readiness

The metrics sidecar exposes:

- `GET /healthz` — 200 OK as long as the process is alive (kubelet
  liveness probe).
- `GET /readyz` — 200 OK when WAL lag is below
  `HealthConfig::readiness_lag_bytes` (default 16 MiB), 503 otherwise.
  See [`docs/RUNBOOK.md`](../../docs/RUNBOOK.md) for what to do when
  the readiness probe goes red.
- `GET /metrics` — Prometheus exposition.

## Cargo features

| feature | enables |
|---------|---------|
| (default) | every subcommand, including `slot-info` |
| `slot-info` | no-op, kept so older `--features slot-info` builds keep working |

The server crate also exposes an `otel` feature that wires
`tracing-opentelemetry` and forwards spans to an OTLP collector when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set. Build with
`cargo build -p palimpsest-cli --features palimpsest-server/otel` to
opt in; the prebuilt binaries do not include it.
