# External Authorization

**Status:** Draft design
**Owner:** Palimpsest

## Summary

Palimpsest currently decides "who can see which rows" entirely from local
inputs: a TOML rule DSL (`permissions.toml`) whose predicates reference
columns and scalar `$user.*` fields, with those fields populated once per
connection by a pluggable `Authenticator` (anonymous or JWT claims). That
model is deterministic, offline-checkable, and cheap — but it assumes the
authorization *policy* can be expressed as per-table SQL predicates over a
handful of token claims.

Most applications past a certain size keep authorization in a dedicated
system instead — increasingly a Zanzibar-style relationship-based engine
such as [SpiceDB](https://authzed.com/spicedb). There, "can Alice view
document 42" is answered by graph evaluation over relationship tuples
(`document:42#viewer@user:alice`, group memberships, folder inheritance),
not by a predicate over columns. This document designs the integration
seam that lets an external authorizer — SpiceDB first — govern access to
Palimpsest data while preserving the three properties the current system
gets right:

1. **Determinism.** Diff output remains a pure function of the WAL stream
   and subscription inputs. No RPC to an external service ever sits on the
   per-diff hot path.
2. **Simulatability.** Every enforcement path can run in the Postgres-free
   test harness (`palimpsest-test-harness`) with fixture-backed
   authorization, and can be validated offline by the permission evaluator
   designed in [`EVALUATION-OF-PERMISSIONS.md`](EVALUATION-OF-PERMISSIONS.md).
3. **Liveness.** Palimpsest is a *live* query engine. A revoked permission
   must retract rows from open subscriptions; a granted permission must
   push them. Authorization that is only checked at subscribe time is not
   acceptable as an end state.

The core design move: **external authorization decisions enter Palimpsest
as data, never as inline calls.** SpiceDB is consulted at two well-defined
points — a subscribe-time gate (control plane) and a relationship
materializer that mirrors flattened grants into a Postgres table flowing
through the WAL (data plane). Inside the engine, everything remains the
existing deterministic predicate/rewrite machinery.

## Source Context

The seams this design builds on, by symbol:

- `Authenticator` (`crates/palimpsest-server/src/auth.rs`) — async trait,
  called once per gRPC stream (`grpc.rs`, `subscribe()`), resolves headers
  to a `UserContext`. Ships `AnonymousAuthenticator` and
  `JwtAuthenticator` (HS256, `claim_to_field` mapping).
- `UserContext` / `UserValue` (`crates/palimpsest-permissions/src/user.rs`)
  — scalar-only values (`Bool | Int | Float | Text | Timestamp | Null`).
  There is no set/array value today.
- `PermissionRule { name, table, predicate, mode }` and
  `compile_rules` / `rewrite` (`crates/palimpsest-permissions`) — predicates
  are a restricted SQL subset (no subqueries, no function calls,
  constant-only `IN` lists), spliced as `Filter` nodes above each
  `BaseTable` in the MIR at subscribe time
  (`crates/palimpsest-server/src/permissions.rs::install_permission_filters`,
  called from `SubscriptionRouter::subscribe`).
- `canonical_subgraph_key(query, user_ctx)`
  (`crates/palimpsest-server/src/router.rs`) — dataflow sharing key: raw
  SQL text plus **every** `UserContext` field/value. Identical contexts
  share one arrangement; any differing field forks a private dataflow.
- Wire protocol (`crates/palimpsest-proto`) — `Resync` reasons are a closed
  enum (`LSN_COMPACTED | SCHEMA_CHANGED | BACKPRESSURE | SLOT_RECREATED |
  PERMISSIONS_CHANGED`).
- Test infrastructure — `TestHarness::drive(&[LogicalEvent])`,
  `WalGenerator`, `MockPostgres`, `ReferenceExecutor`
  (`crates/palimpsest-test-harness`); property suites in
  `crates/palimpsest-properties/tests/` (`permission_soundness.rs`,
  `permission_liveness.rs`); the integration scenario
  `scenario_permission_change_flips_client_view`
  (`crates/palimpsest-integration-tests/tests/scenarios.rs`).

Known gaps in the current implementation that this design depends on or
must not make worse (verified in code):

- `Mode::Subscribe` is defined but dead — `Mode::affects_subscribe()` has
  no production call site. There is no subscribe-time deny today beyond a
  missing-user-field error.
- ~~Multiple rules on one table compile to stacked `Filter` nodes (AND)~~
  Resolved: the rewriter now composes rules on one table disjunctively
  (one `Filter` with `(p1) OR (p2)`), matching `PERMISSIONS.md` and
  Postgres `PERMISSIVE`; pinned by
  `permission_soundness.rs::multiple_rules_compose_disjunctively`.
- ~~`ANY($user.team_ids)` cannot compile~~ Resolved: `UserValue::List`
  materializes as `ANY(ARRAY[...])` and the dataflow evaluator executes
  it; the canonical key normalizes lists as sets (order/duplicates do
  not fork dataflows).
- ~~Nothing re-evaluates a running stream~~ Partially resolved:
  `SubscriptionRouter::set_rules` (via `Palimpsest::update_permissions`)
  now forces `Resync(PermissionsChanged)` onto every active
  subscription, so a rule swap propagates within one resubscribe round
  trip, with lag published as
  `palimpsest_permission_revocation_lag_p{50,99}_microseconds`. What
  remains open is *automatic* re-evaluation driven by an external
  grant store (the subject of this design).
- `palimpsest_subscribe_rejected_total{reason="permissions"}` is
  documented but not emitted.

## Goals

- Let an external system be the source of truth for data-access decisions,
  with SpiceDB as the reference integration.
- Keep the per-diff path free of network calls and nondeterminism.
- Make revocation and grant changes propagate to open subscriptions with
  bounded, observable lag.
- Keep the whole enforcement story runnable in CI without Postgres or
  SpiceDB, and runnable in conformance mode against real SpiceDB.
- Extend — not fork — the offline evaluation story in
  `EVALUATION-OF-PERMISSIONS.md` so proofs can span local predicates and
  external grants.
- Preserve dataflow sharing for the common case (many users subscribing to
  the same query shape).

## Non-goals

- Palimpsest does not become a policy engine. It consumes decisions and
  relationship data; it does not evaluate SpiceDB schema semantics
  (unions, intersections, exclusions, caveats) itself.
- No enforcement on the write path. Palimpsest remains read-only;
  mutations go through the application's own API, which should already be
  consulting SpiceDB.
- No general OPA/Cedar/etc. adapters in v1. The provider interface is
  designed so they can be added, but only SpiceDB and fixture providers
  are specified here.
- Column masking / field-level redaction. Row-level visibility only,
  matching the existing rewriter.

## Architecture Overview

```text
                       ┌─────────────────────────────┐
                       │           SpiceDB           │
                       │  (policy + relationships)   │
                       └─────┬───────────────┬───────┘
             CheckPermission │               │ Watch / LookupSubjects
             (subscribe gate)│               │ (flattened grant tuples)
                             │               ▼
                             │      ┌─────────────────────┐
                             │      │ Grant Materializer  │  upserts/deletes
                             │      │ (sidecar or task)   ├──────────────┐
                             │      └─────────────────────┘              │
                             │                                          ▼
┌──────────┐  Subscribe  ┌───┴──────────────────────────┐      ┌────────────────┐
│  Client  ├────────────▶│      palimpsest-server       │      │    Postgres    │
│          │◀────────────┤ Authenticator → Authorizer   │      │ app tables +   │
└──────────┘   diffs     │ gate → rewrite → dataflow    │◀─────┤ __palimpsest_  │
                         │ (semi-join on grants table)  │ WAL  │ grants mirror  │
                         └──────────────────────────────┘      └────────────────┘
```

Two planes, deliberately separated:

- **Control plane (subscribe-time gate).** May call SpiceDB. Async,
  cacheable, fail-closed. Answers "may this principal open this
  subscription at all."
- **Data plane (row visibility).** Never calls anything. Row-level access
  is enforced by the existing rewriter against either (a) grant tuples
  mirrored into a Postgres table that flows through the WAL, or (b)
  bounded ID sets resolved into the `UserContext` at subscribe time.
  Because both are ordinary engine inputs, both are deterministic and
  simulatable.

## Component: `ExternalAuthorizer` provider interface

A new trait in `palimpsest-server` (module `authz.rs`), parallel to
`Authenticator` and injected the same way through the embed builder:

```rust
/// Consistency handle returned by the provider (SpiceDB: a ZedToken).
pub struct AuthzRevision(pub String);

pub struct CheckRequest<'a> {
    pub subject: SubjectRef<'a>,      // e.g. user:alice, derived from UserContext
    pub resource: ResourceRef<'a>,    // e.g. table:documents or document:42
    pub permission: &'a str,          // e.g. "subscribe", "view"
}

#[async_trait]
pub trait ExternalAuthorizer: Send + Sync + 'static {
    /// Batched permission checks for the subscribe-time gate.
    async fn check_all(
        &self,
        requests: &[CheckRequest<'_>],
        consistency: Consistency,
    ) -> Result<CheckOutcome, AuthzError>;

    /// Resolve the bounded set of resource IDs `subject` holds
    /// `permission` on (context-expansion mode). Must respect `limit`;
    /// exceeding it is an error, not a truncation.
    async fn lookup_resources(
        &self,
        subject: SubjectRef<'_>,
        resource_type: &str,
        permission: &str,
        limit: usize,
    ) -> Result<(Vec<UserValue>, AuthzRevision), AuthzError>;
}
```

Implementations:

- **`SpiceDbAuthorizer`** — wraps the authzed gRPC API:
  `CheckBulkPermissions` for `check_all`, `LookupResources` for
  `lookup_resources`. Configured with endpoint, preshared key (from an
  env var, never inline in TOML), TLS, and a consistency mode
  (`minimize_latency` | `at_least_as_fresh` | `fully_consistent`).
- **`StaticAuthorizer`** — fixture-backed, loaded from TOML/JSON. The
  harness and CI default. Deterministic by construction.
- **`AllowAllAuthorizer` / `DenyAllAuthorizer`** — dev and fail-safe
  postures.

Provider errors are never interpreted as "allow". An unreachable provider
fails the subscribe with `permission_denied` (fail-closed), unless the
operator explicitly configures `on_error = "allow"` for dev environments.

### Subject identity

The gate needs a stable subject ID. Configuration names the `UserContext`
field that carries it:

```toml
[authz]
provider = "spicedb"
endpoint = "https://spicedb.internal:50051"
token_env = "SPICEDB_PRESHARED_KEY"
subject_type = "user"
subject_field = "sub"        # $user.sub carries the SpiceDB object id
on_error = "deny"            # deny | allow
```

`Authenticator` (JWT) remains the authentication step; the authorizer
consumes its output. An unauthenticated (anonymous) context with a
configured external authorizer maps to a well-known subject
(`anonymous_subject = "user:anonymous"`) or is denied outright,
per config.

## Component: subscribe-time gate

The gate revives `Mode::Subscribe` semantics and extends rules with an
optional external check:

```toml
[[rule]]
name = "documents_gate"
table = "documents"
mode = "subscribe"
check = { resource_type = "palimpsest_table", resource_id = "documents", permission = "subscribe" }
```

At `handle_subscribe` (before the `spawn_blocking` compilation step, which
is synchronous — the async provider call must happen here), the server:

1. Collects every base table referenced by the lowered query.
2. Gathers each table's `mode = subscribe`/`both` rules; local predicates
   are evaluated as today, `check` entries are batched into a single
   `check_all` call.
3. Denies with `permission_denied` (and a stable proof/decision ID in the
   error detail) if any check fails; emits
   `palimpsest_subscribe_rejected_total{reason="permissions"}` — finally
   implementing the documented metric.
4. On success, records the returned `AuthzRevision` on the subscription
   for observability and for context-expansion re-resolution (below).

Decisions are cached per `(subject, resource, permission)` with a
configurable TTL (default 30 s) keyed additionally on the provider
revision when available. The gate is *coarse* by design: it protects
against subscribing at all, not row-level access — that is the data
plane's job. A gate-only deployment (Phase 2) is already useful for
table-level entitlements (feature flags, plan tiers, internal tools).

## Component: row visibility via grant mirroring (primary mode)

This is the recommended end state and the reason the design works well
with SpiceDB *and* stays reactive.

### The mirror table

A dedicated Postgres table, included in the Palimpsest publication,
holding **flattened** (subject, permission, resource) grants for exactly
the `(resource_type, permission)` pairs referenced by permission rules:

```sql
CREATE TABLE __palimpsest_grants (
    resource_type text NOT NULL,
    permission    text NOT NULL,
    resource_id   text NOT NULL,
    subject_id    text NOT NULL,
    revision      text NOT NULL,   -- ZedToken (or source LSN) that produced this tuple
    PRIMARY KEY (resource_type, permission, resource_id, subject_id)
);
```

Two ownership modes, chosen per deployment:

- **`spicedb_primary`.** SpiceDB owns relationships. A **grant
  materializer** (a task inside `palimpsest-server` or a standalone
  sidecar sharing the crate) consumes the SpiceDB **Watch API** and, for
  each affected `(resource_type, permission)` pair, recomputes flattened
  membership via `LookupSubjects`, then upserts/deletes rows in
  `__palimpsest_grants`. Postgres is a derived index of SpiceDB.
- **`postgres_primary`.** The application already stores relationship
  facts in Postgres (memberships, ACL rows) and writes grant rows
  **in the same transaction** as the data they protect; SpiceDB is fed
  from those tables (outbox/CDC) for its point-check API. For Palimpsest
  this is the gold standard: grant and data changes share one WAL
  ordering, so a row and its authorization become visible atomically at
  the same LSN, and the new-enemy window collapses to zero *within the
  stream*.

The design treats both identically downstream — the engine only ever sees
a table on the WAL.

### The `authz()` rule predicate

Rules gain sugar that compiles to a semi-join against the mirror:

```toml
[[rule]]
name = "documents_viewers"
table = "documents"
predicate = "authz('document', id, 'view')"
```

Compilation (extending `compile.rs` / `rewriter.rs`): instead of a
`Filter`, the rewriter splices a **semi-join** above the `BaseTable`:

```text
documents ⋉ σ(resource_type='document' ∧ permission='view'
              ∧ subject_id = $user.sub)(__palimpsest_grants)
    on documents.id = grants.resource_id
```

DESIGN.md §11.1 already anticipates cross-table permission rules
compiling into joined inputs ("permissions can be data-dependent on
subviews"); this is the first concrete instance, restricted to the one
well-known grants relation rather than general subqueries. Notes:

- The grants arrangement — keyed by
  `(resource_type, permission, subject_id)` — is **shared across all
  subscriptions**. The per-user part of the graph is only the semi-join
  parameterized on the scalar `$user.sub`, so the canonical-key story is
  unchanged from today's scalar filters: users fork at the filter, share
  everything above it, and share the (single, incrementally-maintained)
  grants arrangement below it. No per-user ID-set blowup.
- `resource_id` is `text` in the mirror; the compiler inserts the cast
  from the guarded table's key column type, validated against the catalog
  at rule-compile time like any column reference today.
- `__palimpsest_grants` gets an implicit `predicate = "false"` rule so
  clients can never subscribe to the mirror itself (default-open would
  otherwise leak the entire ACL — see THREAT-MODEL.md T2).

### Reactivity for free

Because grants are rows, the existing differential machinery does the
hard part: a revoked SpiceDB relationship becomes a tuple delete, flows
through the WAL, and **retracts matching rows from every open
subscription** as ordinary `Delete` diffs — no resync, no re-auth pass,
no new protocol surface. A new grant pushes `Insert` diffs the same way.
End-to-end revocation latency is `spicedb→materializer lag` +
`replication lag`, both observable (below). This is the property that
makes mirroring strictly better than periodic re-checking for a live
query engine.

### Materializer semantics

- **Bootstrap:** on start (or when a new `(resource_type, permission)`
  pair appears in rules), full `LookupSubjects` sweep per pair →
  reconcile the mirror table (insert missing, delete stale) inside batch
  transactions, then tail Watch from the sweep's ZedToken.
- **Incremental:** each Watch event names touched relationships; the
  materializer maps them to affected `(pair, resource | subject)` scopes
  and recomputes just those flattened rows. Fan-out amplification (one
  group edit → many tuples) is bounded by per-pair recompute batching and
  surfaced via lag metrics rather than hidden.
- **Idempotence:** all writes are upsert/delete by primary key with the
  producing revision; replaying a Watch window is safe.
- **Checkpointing:** last-applied ZedToken persisted in a companion row
  (`__palimpsest_grants_meta`) so restart resumes without a full sweep;
  a Watch-window-expired error triggers re-bootstrap.
- **Scope discipline:** only pairs referenced by loaded rules are
  mirrored. Adding a rule for a new pair triggers bootstrap for that pair
  before the rule is considered active (subscriptions using it are gated
  until the sweep completes — fail closed).

### Consistency posture

SpiceDB revisions (ZedTokens) and Postgres LSNs are not comparable; the
mirror provides **eventual consistency with measured lag**:

- `palimpsest_authz_mirror_lag_seconds` — now minus the timestamp of the
  last applied Watch event.
- `palimpsest_authz_mirror_last_revision` / bootstrap status gauges.
- Configurable `max_mirror_lag`: when exceeded, new subscribes on
  `authz()`-guarded tables fail closed (or warn, per
  `on_stale = "deny" | "warn"`); open streams continue — their contents
  are consistent with the mirror *as of each LSN*, which is the
  deterministic contract.

The known skew (new-enemy) windows and their bounds:

| Mode | Grant→visible lag | Revoke→retracted lag |
| --- | --- | --- |
| `postgres_primary`, same-tx writes | 0 (same LSN) | 0 (same LSN) |
| `postgres_primary`, separate tx | ≤ app write skew | ≤ app write skew |
| `spicedb_primary` | watch + recompute + commit lag | same |

## Component: context expansion (secondary mode)

For small, slow-moving grant sets (org membership, team lists), calling
`LookupResources` at subscribe time and binding the result into the
`UserContext` is simpler than mirroring:

```toml
[[rule]]
name = "docs_in_my_orgs"
table = "documents"
predicate = "org_id = ANY($user.org_ids)"

[authz.expand.org_ids]
resource_type = "organization"
permission = "member"
limit = 256          # hard cap; exceeding it fails the subscribe
```

Required engine work, which doubles as fixing documented-but-broken
behavior:

- New list values `UserValue::IntList(Vec<i64>)` /
  `UserValue::TextList(Vec<String>)`, with sorted-deduped
  `canonical_repr` so two users with the same memberships still share a
  dataflow.
- `ANY($user.field)` support in the predicate compiler (compiling to the
  existing constant `IN` list machinery after materialization) — making
  `PERMISSIONS.md`'s existing `ANY($user.team_ids)` example real.

Trade-offs, stated plainly: the set is a snapshot (stale until
re-resolved), unbounded sets are forbidden rather than truncated, and
distinct sets fork dataflows (hence the low default cap and the guidance
to prefer mirroring for per-resource grants). Staleness is bounded by
**re-resolution**: a lightweight Watch consumer notes changes touching
`(subject, expanded pair)`, re-runs `lookup_resources`, and if the set
changed, tears the affected subscriptions down with a new
`Resync { reason: PERMISSIONS_CHANGED }` (proto enum addition per
`palimpsest-proto/VERSIONING.md`; older clients treat unknown reasons as
a generic resync) so the client resubscribes and recompiles against the
fresh context. Absent Watch, a TTL re-resolve (`refresh_interval`)
provides the coarse fallback.

This mode is also the bridge for external authorizers that have no
Watch/mirroring story: anything that can answer "list my org IDs" at
subscribe time can plug in, at the cost of liveness.

## Simulation and validation

The design keeps every enforcement decision inside deterministic engine
inputs precisely so the existing test pyramid extends rather than forks.

### Harness (Postgres-free, SpiceDB-free)

- `StaticAuthorizer` fixtures answer the gate and `lookup_resources` from
  TOML: `[[grant]] subject = "user:alice" permission = "view" resource =
  "document:42"`.
- Mirror-mode tests need no authorizer at all: grant tuples are just rows,
  so `TestHarness::drive(&[LogicalEvent])` feeds
  `__palimpsest_grants` inserts/deletes through `WalGenerator` like any
  table. `scenario_permission_change_flips_client_view` generalizes
  directly: *insert grant → row appears; delete grant → row retracts.*
- The `ReferenceExecutor` oracle learns one thing: when a rule uses
  `authz()`, apply the semi-join naively against the fixture grants. The
  property suites then cover external authz with the same invariants:
  - **Soundness** (`permission_soundness.rs`): no emitted diff ever
    contains a row without a matching grant tuple at that LSN.
  - **Liveness** (`permission_liveness.rs`): every grant/revoke event
    produces the corresponding insert/retract diff.
  - **Determinism / batch invariance**: unchanged — grants are ordinary
    input rows, so the existing properties apply to them automatically.
  - **Sharing**: two subjects with distinct grants never share a
    canonical key's post-filter subgraph; the grants arrangement is
    instantiated once.
- Materializer unit tests run against a scripted fake Watch stream:
  bootstrap reconciliation, idempotent replay, window-expiry
  re-bootstrap, fan-out batching.

### Conformance (real SpiceDB)

`spicedb serve-testing` provides a hermetic in-memory server, so
conformance follows the existing double-gate pattern
(`real-postgres` feature + `PALIMPSEST_PG_URL`): a `spicedb` feature +
`PALIMPSEST_SPICEDB_URL`, exercised in `palimpsest-conformance`:

- Load a `.zed` schema + relationship fixtures; run the materializer;
  assert the mirror equals SpiceDB's own `LookupSubjects` answers
  (drift check).
- End-to-end: write a relationship via SpiceDB API → observe the diff on
  an open subscription; delete it → observe the retract; measure and
  assert bounded lag.
- Gate parity: for a corpus of (subject, table) cases, the gate's
  decision equals `zed permission check`.

A `palimpsest authz check-drift` CLI subcommand ships the same
mirror-vs-SpiceDB comparison for production debugging.

### Offline evaluation (extending `EVALUATION-OF-PERMISSIONS.md`)

The evaluator gains one new atom instead of a new engine:
`Grant(resource_type, resource_id, permission, subject)` — a symbolic
relation. `authz('document', id, 'view')` lowers to membership in that
relation, so containment and join-safety proofs compose unchanged (a
grant-guarded table's allowed region is
`∃ Grant('document', id, 'view', S)`).

What makes SpiceDB specifically good here is that its **schema is itself
a checkable artifact**:

- The evaluator accepts `--authz-schema schema.zed` and compiles relation
  and permission definitions (unions, intersections, exclusions, arrow
  traversals) into derivation rules over the symbolic `Grant` relation.
  Constructs outside the supported subset (caveats, wildcard subjects)
  degrade to `Unknown` — explicit uncertainty, never silent allow,
  matching the evaluator's existing contract.
- SpiceDB's native validation file (`zed validate`: assertions +
  expected-relations blocks) doubles as the evaluator's concrete-subject
  fixtures, so one file answers both "does the SpiceDB schema mean what
  we think" and "does Palimpsest expose what the schema implies".
- The evaluation report adds `authz_schema_hash` and
  `grants_fixture_hash` next to the existing policy/catalog hashes, and
  policy diffing flags a schema change that widens any `Grant`-derived
  region exactly like a weakened predicate.

This yields the full question the user actually asks — *"who can see
which rows, given our SpiceDB schema and our Palimpsest rules?"* — as a
CI-checkable proof artifact:

```text
palimpsest permissions eval \
  --catalog catalog.json \
  --permissions permissions.toml \
  --authz-schema schema.zed \
  --authz-relationships fixtures.yaml \
  --subjects subjects.toml \
  --output report.json
```

## Security properties

- **Fail closed.** Provider errors, missing subject fields, un-bootstrapped
  mirror pairs, and over-limit expansions all deny; the only allow-on-error
  is an explicit dev-mode config.
- **Sound denial at every LSN.** In mirror mode, a row appears in a diff
  only if a matching grant tuple exists in the mirror at that LSN — this
  is the property `permission_soundness.rs` enforces mechanically.
- **Bounded, observable revocation.** Revocation latency is the sum of two
  exported lags; `max_mirror_lag` turns unbounded staleness into denial.
- **No ACL exfiltration.** The mirror table is implicitly deny-all to
  subscribers; the materializer's Postgres role is the only writer.
- **Determinism preserved.** All external inputs are either WAL rows or
  subscribe-time context values covered by the canonical key; replaying
  the same inputs yields the same diffs, which is what keeps the property
  and oracle suites meaningful.
- **Explicit uncertainty offline.** Evaluator claims about `.zed` schemas
  are limited to the modeled subset; everything else is `Unknown`.

## Metrics

| Metric | Meaning |
| --- | --- |
| `palimpsest_subscribe_rejected_total{reason="permissions"}` | Gate + rule denials (finally implemented). |
| `palimpsest_authz_check_total{outcome, provider}` | Gate decisions, incl. cache hits. |
| `palimpsest_authz_check_latency_seconds` | Provider round-trip. |
| `palimpsest_authz_mirror_lag_seconds` | Watch-to-Postgres staleness. |
| `palimpsest_authz_mirror_rows{resource_type, permission}` | Mirror cardinality per pair. |
| `palimpsest_authz_resync_total{reason="permissions_changed"}` | Context-expansion re-auth churn. |

## Implementation phases

### Phase 0: semantic prerequisites (in `palimpsest-permissions`)

- Resolve the AND/OR multi-rule discrepancy (implement documented OR
  semantics or amend the docs) — external rules must compose predictably
  with local ones before anything else lands.
- Emit the documented `subscribe_rejected` metric.

### Phase 1: provider seam + gate

- `ExternalAuthorizer` trait, `StaticAuthorizer`, `AllowAll`/`DenyAll`;
  embed-builder and TOML config wiring.
- Revive `Mode::Subscribe`: gate evaluation in `handle_subscribe` (async,
  pre-`spawn_blocking`), `check` rule field, decision cache, fail-closed
  error mapping with proof IDs.
- Harness + security-matrix coverage (deny, allow, provider-down,
  cache TTL).

### Phase 2: SpiceDB provider

- `SpiceDbAuthorizer` over authzed gRPC (`CheckBulkPermissions`,
  `LookupResources`), TLS + preshared-key config, consistency modes.
- Conformance suite against `spicedb serve-testing`; demo-app persona
  wiring (alice/bob/carol get a tiny `.zed` schema).

### Phase 3: grant mirror + `authz()` predicate

- Mirror table DDL + publication guidance; implicit deny-all rule.
- `authz()` parsing, validation, and semi-join rewriting; grants
  arrangement sharing in the dataflow layer.
- Grant materializer (bootstrap, Watch tailing, checkpoint, reconcile),
  `spicedb_primary` and `postgres_primary` modes, lag metrics,
  `max_mirror_lag` posture, `authz check-drift` CLI.
- Property + oracle extensions (soundness/liveness over grant events),
  integration scenario generalizing
  `scenario_permission_change_flips_client_view`.

### Phase 4: context expansion

- List-valued `UserValue` with canonical sorted representation;
  `ANY($user.field)` compilation (fixes the documented example).
- `[authz.expand.*]` config, subscribe-time resolution with hard limits.
- `PERMISSIONS_CHANGED` resync reason (proto addition per VERSIONING.md);
  Watch/TTL-driven re-resolution and teardown.

### Phase 5: evaluator integration

- Symbolic `Grant` relation in the evaluator; `.zed` schema lowering for
  the supported subset; `zed validate` fixture ingestion; report/diff
  extensions and CI wiring.

## Open questions

- **Materializer packaging.** In-process task (simplest ops) vs standalone
  sidecar (isolates SpiceDB fan-out load, one instance per database when
  multiple Palimpsest servers share a slot)? Leaning: in-process for v1
  with a Postgres advisory lock so exactly one instance materializes.
- **Caveats.** SpiceDB caveated relationships are context-dependent and
  cannot be flattened into static tuples. v1: reject rules whose
  `(resource_type, permission)` reachability includes caveats (`Unknown`
  offline, config error at load). Later: caveat context from `$user.*`?
- **Wildcards.** `subject: user:*` flattens badly. Represent as a
  `subject_id = '*'` row with the semi-join extended to match it, or
  reject in v1?
- **Mirror table typing.** Single `text` resource_id column with casts vs
  per-type mirror tables. Casts are simpler; per-type tables avoid cast
  cost on hot joins.
- **Gate cache invalidation.** Is TTL-only acceptable for v1, or should
  the Watch consumer also invalidate the gate cache (cheap once the
  materializer exists)?
- **Anonymous subjects.** Deny by default vs configurable well-known
  subject — which default ships?
- **Postgres-primary tooling.** Do we ship the outbox→SpiceDB feeder, or
  document the pattern and leave the feeder to the application? (Leaning:
  document only; it's application-schema-specific.)
