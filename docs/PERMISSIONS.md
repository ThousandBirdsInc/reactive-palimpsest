# Permissions Guide

Palimpsest enforces row-level access through a TOML rule DSL evaluated
at subscribe time and folded into every diff. Rules are compiled
against the catalog and the user-context schema; ambiguous,
under-specified, or pathological rules are rejected at load time so
the rewriter never sees them.

## What a rule looks like

```toml
[[user_context]]
name = "id"
type = "int"

[[user_context]]
name = "org_id"
type = "int"

[[user_context]]
name = "is_admin"
type = "bool"

[[rule]]
name = "posts_in_org"
table = "posts"
mode = "both"
predicate = "org_id = $user.org_id"

[[rule]]
name = "comments_by_author"
table = "comments"
predicate = "author_id = $user.id"

[[rule]]
name = "authors_self_or_admin"
table = "authors"
predicate = "id = $user.id OR $user.is_admin"
```

## Field meanings

- **`[[user_context]]`** — declares the shape of the `UserContext`
  attached to each subscription. Every `$user.<name>` referenced from
  any rule must appear here, with the right type. Fields are typed as
  `bool` / `int` / `float` / `text` / `timestamp` / `uuid` / `jsonb` /
  `enum`.

  - `uuid` — an RFC 4122 UUID. Values are validated at subscribe time
    and normalized to the lowercase hyphenated form (so canonical keys
    match regardless of input casing). Plain strings are accepted where
    a `uuid` field is declared, as long as they parse.
  - `jsonb` — a JSON document, compared structurally against `jsonb`
    columns. Only structured values are accepted — a raw JSON string is
    rejected so a document is never confused with a text scalar.
  - `enum` — a Postgres enum label, compared as text. The taxonomy is
    coarse: all enum types collapse into one, and no per-type label
    list is enforced.
  - **Lists** — a field may be bound to a *list* of scalars at
    subscribe time (e.g. a `team_ids` JWT claim that is a JSON array).
    Declare the field with its **element** type (`team_ids` above is
    declared `int`); list-ness is a property of the bound value, not
    the declaration. Every element must match the declared type;
    nested lists and lists of JSON documents are rejected. Reference
    the field with `column = ANY($user.field)` — it materializes as
    `column = ANY(ARRAY[...])`.

- **`[[rule]]`** — a row-visibility / subscribe-authorization predicate
  applied to a specific table.

  - `name` — stable identifier, must be unique. Used in error messages
    and metrics.
  - `table` — the table the rule guards.
  - `predicate` — a SQL boolean expression over columns of `table` and
    `$user.<field>`. Same supported subset as query predicates: `AND`,
    `OR`, `NOT`, `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`,
    `IS NOT NULL`, `IN (...)` (with constants only), parenthesisation.
  - `mode` — one of `row_visibility`, `subscribe`, or `both` (default).

## Modes — what gets gated

| Mode | Subscribe-time check | Row visibility filter |
| --- | --- | --- |
| `row_visibility` | ❌ | ✅ |
| `subscribe` | ✅ | ❌ |
| `both` (default) | ✅ | ✅ |

`row_visibility` is the bread-and-butter mode: rejected rows simply
never appear in the diff stream. `subscribe` lets you reject the
*subscription itself* if the user has no possible access (e.g. rule
is `false` after substitution). `both` is conservative and the right
default.

## How rules compose

If multiple rules cover the same table, **a row is visible iff at
least one rule's predicate evaluates to `true`** (the rules are
disjunctive). This matches Postgres RLS's `PERMISSIVE` semantics.

Two rules over the same table whose canonical predicates are
**identical** are rejected at load time (`AmbiguousRule`). This
catches accidental duplication. If you want overlapping rules with
different names, vary the predicate text (semantic difference, not
just whitespace — the canonical-form pass strips trivial differences).

A table with **no rule** is visible to everyone — there is no implicit
deny. Rule absence means "no policy"; if you want a default-deny
posture, add `predicate = "false"` for tables you haven't reviewed.

## Examples

### Per-tenant isolation

```toml
[[user_context]]
name = "tenant_id"
type = "uuid"

[[rule]]
name = "tenant_isolation"
table = "documents"
predicate = "tenant_id = $user.tenant_id"
```

`tenant_id` here is a `uuid` field; the subscriber's value is validated
and normalized before it is substituted into the filter.

### Role gate on an enum column

```toml
[[user_context]]
name = "role"
type = "enum"

[[rule]]
name = "staff_only"
table = "audit_log"
predicate = "visibility = $user.role OR visibility = 'everyone'"
```

### Owner or admin

```toml
[[rule]]
name = "owner_or_admin"
table = "secret_notes"
predicate = "owner_id = $user.id OR $user.is_admin"
```

### Soft delete + visibility window

```toml
[[rule]]
name = "live_only"
table = "posts"
predicate = "deleted_at IS NULL AND published_at <= $user.now"
```

The `now` field is a `timestamp` declared in `[[user_context]]`; the
client sends a freshly-generated value at subscribe time. (Don't use
`now()` in the predicate — Palimpsest predicates must be deterministic.)

### Scoped read with admin override

```toml
[[user_context]]
name = "team_ids"
type = "int"          # element type; the bound value is a list

[[rule]]
name = "team_read"
table = "tasks"
mode = "row_visibility"
predicate = "team_id = ANY($user.team_ids) OR $user.is_admin"
```

A subscriber whose `team_ids` is `[1, 3]` sees rows from teams 1 and 3
and from no others.

**Dataflow sharing for list values:** the canonical subgraph key treats
a list as a *set* — element order and duplicates are normalized away,
so `[2, 1, 1]` and `[1, 2]` share one dataflow. Subscribers with
genuinely different team sets see different rows, so each distinct set
necessarily gets its own dataflow (exactly as two subscribers with
different scalar `$user.org_id` values do). If your tenancy model puts
most users in one team, sharing degrades to per-team, not per-user.

## Common pitfalls

### ❌ Forgetting to declare a `$user.*` field

```toml
[[rule]]
name = "x"
table = "posts"
predicate = "author_id = $user.author_id"   # NOT in user_context
```

Compile-time error: `UnknownUserField`. Add the corresponding
`[[user_context]]` block.

### ❌ Predicate references a column that doesn't exist

```toml
[[rule]]
predicate = "author_idd = $user.id"   # typo
```

Compile-time error: `UnknownColumn`.

### ❌ Pathologically nested predicates

```toml
predicate = "((((((... 33+ levels deep ...))))))"
```

Compile-time error: `PredicateTooDeep`. The default cap is 32. If you
genuinely need deeper nesting, you almost certainly want a CTE-based
view shape instead.

### ❌ Trying to filter by a derived column

```toml
predicate = "EXTRACT(year FROM created_at) = $user.year"
```

Function calls in predicates are not supported. Move the derivation
into the row itself (a generated column or a view) and filter on the
materialized form.

### ❌ Using `now()` or `random()` in a predicate

```toml
predicate = "created_at > now() - interval '1 day'"
```

Predicates must be deterministic over their inputs. Pass the
"as-of" timestamp through `$user.<field>` instead.

### ✅ Default-deny for unfamiliar tables

```toml
[[rule]]
name = "deny_internal_audit"
table = "internal_audit_log"
predicate = "false"
```

Stops *anyone* from subscribing until you write a real rule.

## Operational notes

- Rule changes can be applied to a running server via
  `Palimpsest::update_permissions` (or
  `PalimpsestHandle::update_permissions`). Future subscribes compile
  against the new rules immediately; every active subscription is sent
  `Resync(PermissionsChanged)` before the call returns, forcing a
  resubscribe under the new rules. The revocation bound is therefore
  *one resubscribe round trip* after the swap; the server-side half is
  published as
  `palimpsest_permission_revocation_lag_p{50,99}_microseconds`, with
  `palimpsest_permission_rule_updates_total` and
  `palimpsest_permission_resyncs_forced_total` counting swaps and
  forced resyncs.
- The full rule set is hashed into the canonical-form key, so two
  servers with diverging rule sets will produce diverging canonical
  keys — don't let configurations drift.
- Watch `palimpsest_subscribe_rejected_total{reason="permissions"}`;
  a sudden surge after a rule edit usually means you tightened the
  predicate further than intended.
