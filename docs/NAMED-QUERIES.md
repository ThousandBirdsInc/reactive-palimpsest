# Named prepared queries

Server-registered queries that clients refer to **by name**. The client
never holds or sends SQL — it subscribes with `Subscribe{query_name,
params}` and the server binds the params into a template it registered
at startup.

## Why

Subscribing with raw SQL over the socket has three structural problems
that every adopter ends up patching by hand:

1. **A hand-rolled allowlist.** To stop arbitrary reads, bridges grow a
   canonical-query allowlist (persisted SQL file + wildcard matching of
   literals) that pins the accepted query shape server-side. That
   allowlist also ends up carrying a security load: tables that must
   not have their own row-visibility rule (e.g. the right side of an
   anti-join, where a rule's filter would change the join's meaning)
   are readable by *any* accepted query, so the allowlist is the only
   thing standing between them and an arbitrary `SELECT`.
2. **Schema internals in the client bundle.** The full SQL ships to
   every browser — table names, soft-delete columns, exclusion logic —
   and any SQL change is a frontend deploy.
3. **Literal-inlining guards.** `SubscribeRequest.vars` existed on the
   wire but was never read, so clients inlined parameters as SQL
   literals behind regex guards.

The registry replaces all three: the set of registered queries *is* the
accepted shape (fail closed), the client bundle carries only names, and
parameters travel as typed wire values that the **server** binds.

## Registering queries

### From an sqlc query file

sqlc's query-file annotation format is the registration format — do not
invent a second one. A file that sqlc accepts registers as written, or
the registrar rejects it **at registration time** with the query name
and the exact unsupported construct:

```sql
-- name: BoardCards :many
SELECT cards.id, cards.position
FROM cards
WHERE cards.board_id = $1 AND cards.archived = false
ORDER BY cards.position;

-- name: CardsInBoards :many
SELECT id FROM cards WHERE board_id = ANY($1);

-- name: CardsAt :many
SELECT id FROM cards WHERE position = sqlc.arg(pos);
```

Supported header verbs are `:one` and `:many` (both stream diffs the
same way; `:one` is a client-side cardinality hint). Mutation verbs
(`:exec`, …) are rejected — they have no meaning for a live
subscription engine.

Via the CLI, list the files in the config:

```toml
[queries]
files = ["queries/board.sql"]
# inline_sql = true   # opt back into raw-SQL subscribes (default: off
                      # once any query file is configured)
```

Embedding the server directly:

```rust
let mut registry = QueryRegistry::new();
registry.register_sqlc_source(&std::fs::read_to_string("board.sql")?, "board.sql", &catalog)?;

let server = Palimpsest::builder()
    .with_wal(runtime)
    .with_query_registry(registry)   // also disables raw SQL
    .build()?;
```

### Programmatically, with an explicit parameter schema

`RegisterQuery(name, sql, param schema)`:

```rust
registry.register(
    "TitledBoards",
    "SELECT id FROM boards WHERE title = $1",
    &[ParamDecl::new("title", ColumnType::Text)],
)?;
```

### Registration is the loud failure point

Every template is validated when registered — parsed, checked against
the supported SQL surface, and lowered to MIR with representative
values bound for each parameter. A query the engine cannot execute
fails registration (startup / CI) with the exact construct named, e.g.

```
query 'Bad': unsupported SQL feature: RIGHT JOIN
```

Subscribe time can only fail for client-side reasons: unknown name,
missing/extra/ill-typed parameter.

## Parameters

Positional `$1..$N` (sqlc's canonical form) and `sqlc.arg(name)` /
`sqlc.narg(name)` (nullable) are supported. Types are inferred from the
catalog the same way sqlc infers them from the database schema:

| use site                          | inferred type            |
| --------------------------------- | ------------------------ |
| `col <op> $1` (either side)       | type of `col`            |
| `col = ANY($1)`                   | **list** of `col`'s type |
| `col IN ($1, $2)`                 | type of `col` per param  |
| `col BETWEEN $1 AND $2`           | type of `col`            |
| `col LIKE $1` / `ILIKE`           | text                     |
| `LIMIT $1` / `OFFSET $1`          | int                      |
| `$1::uuid` / `CAST($1 AS uuid)`   | the cast target          |

A parameter whose type cannot be inferred (or is inferred
inconsistently across use sites) fails registration with a reason;
annotate with a cast or register programmatically with an explicit
schema.

Wire names are derived sqlc-style from the compared column
(`board_id = $1` → `board_id`), with positional fallbacks (`argN`,
`name_N` on collision). Clients may always bind by position (`"$1"`).

### Binding is typed literal substitution, not string splicing

At subscribe time the server type-checks each wire value against the
declared parameter and substitutes a **typed literal AST node** into a
clone of the parsed template. String values become single-quoted
literal nodes whose rendering escapes embedded quotes; uuids must match
the canonical `8-4-4-4-12` form; int/float/bool values must parse
strictly. A parameter value can never alter the query shape. Lists are
bounded (1024 elements) and text values are capped (16 KiB).

The bound SQL text is the query's identity: two clients subscribing to
the same name with the same params share one canonical dataflow plan,
exactly like two clients sending identical raw SQL.

`col = ANY($1)` with a list parameter is the supported home for
"filter by a list" — the list is bound server-side behind the registry
instead of being inlined by every client. The same server-side
`ARRAY[...]` rendering now also backs `col = ANY($user.list)` in
permission rules (previously D2: it never compiled): the expression
validator admits the `$user.*` sentinel inside `ANY(...)` and the
rewriter materializes the list before evaluation.

## Fail-closed posture

* `Subscribe{query_name}` for an unregistered name →
  `unknown_query`. A server with no registry refuses every name.
* Configuring a registry **disables raw-SQL subscribes** — attempts get
  `inline_sql_disabled`. Re-enable explicitly with
  `with_inline_sql(true)` / `inline_sql = true` if you want both
  surfaces during a migration.
* Bad parameters → `invalid_params`; `sql` and `query_name` both set →
  `invalid_request`.

With raw SQL disabled, the registered queries are the **authorization
boundary**: a table that is reachable only through registered queries
cannot be read any other way, so it needs no row-visibility rule of its
own. This structurally fixes the anti-join fail-open class — rule-free
right-side tables are no longer one arbitrary `SELECT` away — without
per-adopter allowlist code. Row-visibility rules still compose on top:
a named query's bound MIR goes through the same permission rewrite as
any other subscription.

## Client usage

TypeScript:

```ts
const sub = await client.subscribeNamed<CardRow>("BoardCards", {
  board_id: "a5e9e2c0-0000-4000-8000-000000000042",
});
```

React:

```tsx
const { rows } = usePalimpsestNamedSubscription<CardRow>(
  client,
  "BoardCards",
  { board_id: boardId },
);
```

Rust:

```rust
let sub = client
    .subscribe_named_with("BoardCards", HashMap::from([(
        "board_id".to_owned(),
        VarValue { kind: Some(var_value::Kind::StringValue(board_id)) },
    )]))
    .await?;
```

Param values by JS type: string → string, boolean → bool, integral
number → int, other number → float, `null` → SQL null (only for
`sqlc.narg` params), array → list (for `= ANY($n)` params).

## What this deletes from an adopter

* The bridge-side allowlist module and its persisted SQL file — the
  registry (with raw SQL disabled) subsumes it.
* The SQL constant in the client bundle — the client holds a name.
* The UUID-regex literal-inlining guard — params are typed wire values,
  bound and validated server-side.
