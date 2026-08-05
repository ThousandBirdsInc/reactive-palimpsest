# Palimpsest User Guide

This guide is for application developers who want to subscribe to live
SQL views over a Postgres database. It assumes the operator has already
deployed Palimpsest in front of your Postgres instance — see
`OPERATOR-GUIDE.md` for the deployment side.

## What you're subscribing to

A subscription is `SELECT … FROM …` issued once. You receive:

1. An initial **snapshot** of the current rows.
2. A continuous stream of **diffs** (insert / update / delete) as the
   underlying tables change. Diffs are delivered in commit order and
   carry an LSN you can ack to drive the global compaction frontier.

Palimpsest evaluates your query incrementally — it does not re-run the
SELECT on each commit. A query that joins ten tables and aggregates
across millions of rows still produces small per-commit diffs.

## Authoring a query

```sql
SELECT post_id, title, author_id, created_at
FROM posts
WHERE published = true
```

Palimpsest accepts a deliberately small PostgreSQL `SELECT` subset.
The full surface — what's supported, rejected, or deferred — is in
[`supported-sql.md`](supported-sql.md). The summary:

| Feature | Status |
| --- | --- |
| Single `SELECT` | ✅ |
| `WHERE`, equi-join `INNER`/`LEFT JOIN ... ON` | ✅ |
| `GROUP BY` + `count` / `sum` / `min` / `max` / `avg` | ✅ |
| `SELECT DISTINCT`, `DISTINCT ON (…)` | ✅ |
| CTEs (`WITH name AS …`), `WITH RECURSIVE` | ✅ |
| `LIMIT` with explicit `ORDER BY` | ✅ |
| Correlated `[NOT] EXISTS`, `JOIN LATERAL` | ✅ |
| `CAST` / `::` casts, `= ANY(array)`, `cardinality()` | ✅ |
| Window functions, scalar subqueries | ❌ |
| `CROSS JOIN`, `RIGHT/FULL JOIN`, theta joins | ❌ |
| `INSERT` / `UPDATE` / `DELETE` | ❌ (read-only) |

If the parser rejects your query, the gRPC server returns an `Error`
`ServerMessage` with a code like `query_too_large`,
`query_too_complex`, or a parser error.

## CTE recipes

Non-recursive CTEs are the main way to keep a query readable when it
joins many tables. Two common patterns:

### Filtering before joining

```sql
WITH active_authors AS (
  SELECT id FROM authors WHERE deactivated_at IS NULL
)
SELECT p.id, p.title
FROM posts p
INNER JOIN active_authors a ON a.id = p.author_id
```

The CTE is evaluated once per commit window and shared across the join.

### Layered aggregates

```sql
WITH per_author AS (
  SELECT author_id, count(*) AS post_count
  FROM posts
  GROUP BY author_id
),
prolific AS (
  SELECT author_id FROM per_author WHERE post_count > 10
)
SELECT a.id, a.name
FROM authors a
INNER JOIN prolific p ON p.author_id = a.id
```

Each CTE compiles to its own MIR subgraph; identical CTEs across two
subscriptions share state, so the canonical form matters — use the
same CTE shape in both queries when you want them to share work.

## Pagination

v1 has no server-side cursor. The pattern is **top-K + per-page
subscriptions**:

```sql
-- Page 1: open and keep live
SELECT id, title, created_at
FROM posts
WHERE published = true
ORDER BY created_at DESC
LIMIT 50
```

When the user scrolls past the bottom, remember the last `created_at`
the client has seen and open a *second* subscription:

```sql
-- Page 2: open separately, also kept live
SELECT id, title, created_at
FROM posts
WHERE published = true
  AND created_at < $cursor          -- bound: last created_at from page 1
ORDER BY created_at DESC
LIMIT 50
```

Both subscriptions stay open. Each one delivers live diffs for its own
window, and the client stitches them by primary key. This is more
plumbing than a "give me the next page" cursor, but it preserves the
incremental-update property — pagination would otherwise force the
server to materialize global ordering, which is what `ORDER BY` without
`LIMIT` is rejected for.

If you only need a snapshot and don't care about live updates after the
first page, point your application's existing REST API at this — see
[`MIGRATION-FROM-REST.md`](MIGRATION-FROM-REST.md) for where REST still
makes sense.

## Permissions and `$user.*`

Operators may attach permission rules that filter rows by the user's
identity. Predicates can reference `$user.<field>`:

```toml
# operator-side configuration
[[rule]]
name = "posts_in_org"
table = "posts"
predicate = "org_id = $user.org_id"
```

You don't write `$user.*` in your own queries — it's the operator's
configuration. But you do attach a `UserContext` when you connect:

```rust
client.connect()
    .with_user_field("org_id", UserValue::Int(42))
    .with_user_field("is_admin", UserValue::Bool(false))
    .with_user_field("tenant_id", UserValue::uuid("67e55044-10b1-426f-9247-bb680e5fe0c8")?)
    .await?;
```

Values may be `bool`, `int`, `float`, `text`, `timestamp`, `uuid`,
`jsonb`, or `enum` — matching whichever type the operator declared for
the field.

The rule is applied as a row-visibility filter you cannot bypass; rows
the rule excludes never appear in your subscription.

See [`PERMISSIONS.md`](PERMISSIONS.md) for the full DSL.

## Resume and ack

When your client disconnects, the server holds a slot for a short
window. On reconnect:

```rust
let resume = client.subscribe(sql)
    .resume_from(last_seen_lsn)
    .await?;
```

If the slot is still warm, you receive every diff after `last_seen_lsn`.
If the requested LSN is outside the compaction window or the slot was
recycled, the server emits a `Resync` and starts a fresh snapshot.

Acks drive compaction: until your client acks LSN `N`, the server keeps
the trace alive at or before `N` for your subscription. Don't ack until
you've durably committed the change to your application's state.

## Common pitfalls

- **`ORDER BY` without `LIMIT`**: rejected. Sorting an unbounded stream
  has no incrementally meaningful answer.
- **Subqueries in `WHERE`**: rejected. Lift them to a CTE.
- **Predicates with side effects** (`now()`, `random()`): not supported
  — all expressions must be deterministic over their inputs.
- **Very long queries**: capped at 64 KiB by default and 256 MIR nodes.
  Operators can raise both, but most queries that hit the cap are
  generated SQL and worth refactoring.
