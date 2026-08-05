# palimpsest-cli

Standalone CLI that boots the embedded `palimpsest-server` (§18.14).

## Subcommands

```
palimpsest serve [config]              # default if no command given
palimpsest validate-config <config>    # parse + permission compile; 0 ok, 1 error
palimpsest permissions eval <config> --query <sql>
                                       # rewrite query MIR with configured permissions
palimpsest skills install              # install Codex and Claude skills for this CLI
palimpsest dump-catalog [config]       # emit catalog as JSON on stdout
palimpsest slot-info <config>          # show upstream replication slot status
                                       # (requires --features slot-info)
palimpsest help
```

If invoked with a single positional argument that isn't a subcommand,
the CLI behaves as `serve <config>` for backwards compatibility.

Install locally with Cargo:

```sh
cargo install --path crates/palimpsest-cli
```

Install the latest published CLI:

```sh
cargo install palimpsest-cli
```

Repository maintainers can publish the CLI crate set from the repository root:

```sh
./publish.sh
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
omit.

```toml
[grpc]
addr = "0.0.0.0:50051"   # gRPC SyncEngine listener

[metrics]
addr = "0.0.0.0:9090"    # axum sidecar serving /metrics, /healthz, /readyz

[auth]
kind = "anonymous"       # or "jwt" (see below)

# JWT auth (optional)
# [auth]
# kind = "jwt"
# secret = "super-secret"
# issuer = "palimpsest"
# audience = "clients"
# [auth.claim_to_field]
# sub = "id"
# org = "org_id"

# Permissions schema + rules (optional)
[permissions.user_schema]
# id = "int"
# org_id = "int"
# tenant_id = "uuid"
# prefs = "jsonb"
# role = "enum"

# [[permissions.rules]]
# name = "posts_owner"
# table = "posts"
# mode = "both"          # row_visibility, subscribe, or both (default)
# predicate = "author_id = $user.id"

# Upstream Postgres connection (optional, required for slot-info)
# [upstream]
# url = "postgres://palimpsest:secret@localhost:5432/app"
# slot_name = "palimpsest"          # default: "palimpsest"
# publication = "palimpsest_pub"    # default: "palimpsest_pub"
```

### `[auth]`

`kind = "anonymous"` (default) accepts every connection with an empty
`UserContext`. Suitable for dev and for trusted-network deployments.

`kind = "jwt"` validates a JWT in the gRPC `authorization` header,
maps configured claims onto user-context fields, and rejects malformed,
expired, or wrong-audience tokens. See `palimpsest_server::JwtAuthConfig`
for the field shape.

### `[permissions]`

`user_schema` declares the typed shape of `$user.*` references that
permission predicates can use. Field types are `bool`, `int`, `float`,
`text`, `timestamp`, `uuid`, `jsonb` (alias `json`), and `enum`.
`rules` are compiled at startup; bad predicates fail `validate-config`
(and `serve`) with a precise diagnostic.

### `[upstream]`

Currently only consumed by `slot-info`. Will be consumed by the real
WAL runtime once that lands; defining it now means your config is
forward-compatible.

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
| (default) | core CLI; serve, validate-config, dump-catalog |
| `slot-info` | adds `tokio-postgres` and the `slot-info` subcommand |

The server crate also exposes an `otel` feature that wires
`tracing-opentelemetry` and forwards spans to an OTLP collector when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set. Build with
`cargo build -p palimpsest-cli --features palimpsest-server/otel` to
opt in.
