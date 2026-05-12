# Threat model

This document is the v1 threat model for `palimpsest-server`. It exists
to make the security boundaries explicit, name the things we worry
about, and call out what is *out* of scope so reviewers know not to
expect a defence we never promised.

## In scope

- The gRPC service exposed by `palimpsest-server`.
- The TOML configuration loaded by `palimpsest-cli`.
- The Postgres logical-replication slot consumed by the WAL runtime.
- The browser/native clients shipped under `palimpsest-client*`.

## Out of scope

- Compromise of the underlying Postgres cluster's superuser. If the
  attacker can `pg_dump`, they don't need Palimpsest.
- Compromise of the host OS / kubelet / container runtime. We assume
  the runtime sandbox is honest.
- Side-channel attacks against the TLS terminator (covered by your
  proxy's threat model, not ours; see `docs/TLS.md`).
- Confidentiality of subscription metadata at rest in CDN logs / proxy
  logs. If the proxy logs the URL path, we cannot retroactively make
  that secret.

## Trust boundaries

```
   ┌────── untrusted ──────┐ ┌── trusted edge ──┐ ┌── trusted core ──┐
   │ browser / native      │ │ TLS proxy        │ │ palimpsest-server │
   │ client                │ │ (terminates TLS, │ │ (Rust, this repo) │
   │                       │ │  forwards JWT)   │ │                   │
   └────────────┬──────────┘ └────────┬─────────┘ └────────┬──────────┘
                │ TLS, JWT in header   │ h2c, JWT          │ libpq, ssl=verify-full
                ▼                      ▼                   ▼
                                                        Postgres
                                                        (logical replication)
```

The two boundaries that matter:

1. **Client → server.** Hostile browsers, hostile native clients,
   attackers who have compromised a legitimate user's session.
2. **Server → Postgres.** Network adversary between Palimpsest and
   the upstream Postgres.

## Threats and mitigations

### T1 — Client lies about user identity

> An attacker forges a JWT, or steals one and replays it after
> revocation.

- **Forgery**: HS256 verification with the configured secret.
  `JwtAuthenticator::authenticate` rejects bad signatures, malformed
  headers, missing `Authorization`, expired `exp`, and wrong `aud`/`iss`
  (see `crates/palimpsest-server/tests/security_matrix.rs` for the
  on-the-wire matrix).
- **Replay before exp**: Tokens are short-lived by convention; we do
  not currently honour a revocation list. Operators rotating a user's
  permissions out from under them rely on a short token TTL plus the
  permission-rule reload path.
- **Stolen secret**: HS256 means the secret = the verifier. Treat it
  as a Postgres password equivalent: store in a secret manager, rotate
  on any operator turnover, and audit for log leakage. Future work:
  RS256 / EdDSA support so the verifier doesn't share material with
  the issuer.

### T2 — Permission misconfiguration leaks rows

> An operator writes a permission rule that's broader than they
> intended, or forgets to write one at all.

- **Default deny**: Tables not covered by any rule are subscribable
  by *anyone* in v1. This is documented; we are not a default-deny
  system. Operators MUST review the permission DSL before exposing
  Palimpsest to untrusted users.
- **Compile-time validation**: rules are compiled on startup. Unknown
  tables, unknown columns, malformed `$user.*` references, ambiguous
  rules, and excessively deep predicates (>32 AST levels — see
  `MAX_PREDICATE_DEPTH`) all hard-fail `validate-config` and `serve`.
- **Compile-time uniqueness**: Two rules over the same table with the
  same canonical predicate are rejected (`AmbiguousRule`); the loader
  refuses to guess which one was authoritative.
- **Runtime invariant**: rules are pure SQL `WHERE` predicates against
  the same table the user is querying, so they cannot grant access to
  rows of other tables.

### T3 — WAL stalls or breaks the slot

> Postgres' replication slot fills up; or the slot is dropped behind
> our back; or the upstream database promotes a replica.

- **Resync on stale resume**: If a client requests a resume LSN that
  has fallen out of the compaction window (because the WAL was
  truncated), the router downgrades to a fresh `Initial` snapshot
  (`ResyncReason::LsnCompacted`). The contract is at-least-once with
  resync; we never silently drop diffs.
- **Slot recreation**: If the WAL runtime detects that the slot was
  dropped and recreated under a new identity, every active subscription
  is resynced (`ResyncReason::SlotRecreated`).
- **Operator-visible signal**: `palimpsest_wal_lag_bytes` gauge plus
  the `/readyz` 503 response. See `docs/RUNBOOK.md` for the slot-stuck
  recovery procedure.
- **What we don't defend against**: wholesale Postgres data corruption.
  Palimpsest faithfully reflects whatever Postgres tells it.

### T4 — Resource exhaustion (DoS)

> A single client (or IP) tries to exhaust server CPU / memory /
> connection table.

- **Per-IP reconnect rate limit** (`SecurityLimits::reconnect_rate`):
  inbound `Subscribe` RPCs from the same IP exceeding the sliding
  window get `Status::resource_exhausted` immediately.
- **Per-connection subscribe rate** (`SecurityLimits::subscribe_rate`):
  token bucket on `Subscribe` ClientMessages within a single gRPC
  stream.
- **Per-connection cap** (`SecurityLimits::max_subscriptions_per_connection`):
  hard ceiling on live subscriptions per stream.
- **Query size bound** (`palimpsest_sql::QueryLimits::DEFAULT`):
  64 KiB SQL input, 256 MIR nodes; either trip yields a
  `query_too_large` / `query_too_complex` error before the parser
  runs.
- **Permission depth bound**: as per T2 (32 AST levels), so a
  malicious operator can't ship a runaway predicate.
- **Bounded per-subscription channel**: the router caps the
  per-subscription event mpsc; on saturation the client gets
  `ResyncReason::Backpressure` rather than the server unbounded-buffering.

### T5 — Cross-tenant data leakage on a shared deployment

> Two tenants run subscriptions on the same shared dataflow. Could
> one tenant's rows leak into the other's diff stream?

- **Canonical-key partitioning**: shared subgraphs are keyed by
  `(query, user_context)`. Two users with different `$user.*` bindings
  get different canonical keys and therefore different dataflows.
  See `canonical_subgraph_key` in `crates/palimpsest-server/src/router.rs`.
- **Permission rewriting happens at subscribe**, not at egress: the
  filter is part of the dataflow, so rows the user can't see never
  enter the diff stream in the first place.
- **What we don't currently support**: untrusted-client multi-tenancy
  with shared Postgres roles. Each Palimpsest deployment is intended
  to serve a single trust boundary; if you need hard isolation, run
  separate Palimpsest instances per tenant.

### T6 — Client-side spoofing of `subscription_id` / `lsn`

> A malicious client lies about which subscription an `Ack` or
> `Unsubscribe` belongs to, hoping to corrupt another tenant's state.

- The mapping `client_subscription_id → server-assigned subscription_id`
  is **per-connection** (`ConnectionState::by_client_id` in
  `grpc.rs`). A client can only reference subscriptions on its own
  stream. Cross-connection spoofing is structurally impossible.
- Forged `Ack` LSNs are bounded by the router's existing watermark
  invariant; they cannot retroactively un-emit diffs the client has
  already received.

### T7 — Network adversary between server and Postgres

> Someone reads or modifies traffic between Palimpsest and the
> upstream Postgres.

- The `[upstream]` config exposes the libpq connection string; we
  recommend `sslmode=verify-full` (documented in
  `crates/palimpsest-cli/README.md`).
- **What we don't enforce**: the server does not refuse to start
  with `sslmode=disable`. The recommendation is operator policy, not
  a hard check. Open issue if you want a startup-time refusal.

### T8 — Observability surface leaks secrets

> Logs / metrics / traces leak JWTs, secrets, or PII.

- `tracing` log fields are restricted to opaque IDs (subscription id,
  connection id, LSN, error code). The auth path **never** logs the
  `Authorization` header value.
- `/metrics` exposes counts and histograms only; no per-row payload.
- OTLP traces (`otel` feature) carry the same shape — span attributes
  are IDs, not row contents.
- **Operator policy**: do not ship full SQL strings into trace
  attributes. The current code does not, but reviewers should flag
  any change that does.

## Review cadence

This document is reviewed:

- At every minor release.
- When `palimpsest-server`'s public surface changes (new RPC, new
  authn mechanism, new config field).
- Whenever a CVE in a transitive dependency reaches us via
  `audit.yml` and changes any of the assumptions above.
