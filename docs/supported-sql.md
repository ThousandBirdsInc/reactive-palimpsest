# Supported SQL

Palimpsest currently accepts a deliberately small PostgreSQL-flavored SELECT
subset. This page tracks the Phase 1 SQL frontend surface implemented in
`palimpsest-sql`.

`parse_select` parses with `sqlparser-rs`'s `PostgreSqlDialect`, validates the
frontier features Palimpsest rejects early, and requires exactly one statement.
`parse_and_lower` then lowers the supported subset into MIR.

## Parsing vs. evaluation

A query passes through two gates, and the support table below has one
column for each:

- **Parses** — the SQL frontend (`palimpsest-sql`) accepts the query
  and lowers it to MIR. "Rejected" here means the subscribe fails
  immediately with a named `SqlError`.
- **Evaluates** — the incremental dataflow (`palimpsest-dataflow`'s
  MIR compiler) can execute the lowered MIR, so results are computed
  server-side and updated by WAL diffs.

The two are **not** the same surface. A query that parses but does not
compile onto the dataflow is served by the v1 *pass-through* path: the
server ships the referenced base tables' snapshot rows and raw WAL
diffs verbatim, **without applying the query's operators server-side**.
Every relational shape the frontend accepts now compiles; the fallback
remains only as a safety net for scalar expressions outside the
evaluator's operator surface (e.g. `LIKE`, which the evaluator does
not implement — see the `WHERE` row for the supported operators) and
for a projection wildcard combined with an expression-valued `ORDER BY`
key that the projection does not carry. Server-side permission row filters are part
of the compiled dataflow, so pass-through cannot enforce them — for
that reason a subscribe **fails closed** when row-visibility rules
apply to any table the query reads and no compiled plan exists: the
server rejects it with the `permission_unenforceable` error code
instead of serving unfiltered rows. "Parses: yes / Evaluates: no" rows
are therefore only reachable for tables with no row-visibility rules.

Scalar functions are the one place the two surfaces are forced to
agree at parse time: any function call outside the evaluable
allowlist (`count`, `sum`, `min`, `max`, `avg`, `coalesce`,
`cardinality`) is rejected by the frontend with
`SqlError::UnsupportedFunction` instead of being accepted and never
evaluated.

## Support Table

"Parses" is the frontend gate; "Evaluates" is the dataflow gate (see
above). A dash means the row never reaches that gate.

| SQL surface | Parses | Evaluates | Notes |
| --- | --- | --- | --- |
| Single `SELECT` query | Yes | Yes | Non-query statements and multi-statement inputs are rejected. |
| `WITH name AS (...)` | Yes | Yes | Non-recursive CTEs lower to `CteRef` placeholders plus expansion edges. |
| `WITH RECURSIVE` | Yes | Yes | Self-referential CTEs must be `base UNION [ALL] step` with exactly one self-reference in the step term (linear recursion). `UNION` recursion converges on the distinct set; `UNION ALL` recursion circulates per-iteration waves and accumulates them — over cyclic data it does not terminate, exactly as in Postgres. An earlier recursive CTE may be used inside a later recursive step; it is hoisted out of the iteration as a loop-invariant input. |
| Projection expressions | Yes | Yes | Column references, aliases, casts, arithmetic, `coalesce`, `cardinality`, and literals (including bare `NULL`) evaluate. Output types are inferred; a column whose type cannot be inferred is advertised permissively and its datums describe themselves on the wire. |
| `*` and `relation.*` | Yes | Yes | Expanded at compile time against the input schema; `relation.*` resolves through per-column provenance, so it also works on join outputs. |
| `FROM table` | Yes | Yes | Table names lower to `BaseTable` MIR nodes. |
| Derived tables | Yes | Partial | Subqueries in `FROM (...) AS alias` lower through the nested query path; they evaluate when every lowered operator is a compiled one. |
| `JOIN LATERAL (...) ON TRUE`, `CROSS JOIN LATERAL` | Yes | Yes | Correlated equality predicates in the subquery's `WHERE` are decorrelated into equi-join keys; `LIMIT`/`OFFSET` inside a lateral subquery is rejected (per-row limits have no MIR encoding). |
| Multiple comma-separated `FROM` items | Rejected during lowering | — | Joins must be expressed with explicit join syntax. |
| Table functions and special table factors | Rejected | — | Includes unsupported table-factor forms from `sqlparser-rs`. |
| `INNER JOIN ... ON` | Yes | Yes | Join predicates must be equi-joins between column references; the dataflow keys both sides on the resolved columns and joins incrementally. `NULL` join keys never match, per SQL. |
| `LEFT JOIN ... ON` | Yes | Yes | Conjunctions of equi-join predicates are supported; unmatched left rows are null-extended to the right side's width. |
| `RIGHT JOIN`, `FULL JOIN` | Rejected | — | Rejected during validation. |
| `JOIN ... USING`, natural joins, joins without `ON` | Rejected during lowering | — | The parser validator permits some forms, but MIR lowering requires `ON`. |
| `CROSS JOIN` | Rejected during lowering | — | Cross products are not in the Phase 1 MIR subset. |
| Theta joins | Rejected | — | Join predicates must be equality predicates over column references. |
| `WHERE` | Yes | Yes | Predicates are retained as canonicalized strings and compiled by the expression evaluator (boolean logic, comparisons, arithmetic, `[NOT] IN (list)`, `[NOT] BETWEEN`, `IS [NOT] NULL`, `ANY(array)`, casts, `coalesce`, `cardinality`). |
| `[NOT] EXISTS (...)` | Yes | Yes | Correlated `EXISTS` conjuncts in `WHERE` lower to semi/anti joins on the correlation columns and compile onto the dataflow (each left row emitted at most once, regardless of match count); uncorrelated `EXISTS` and `EXISTS` under `OR` are rejected. |
| Scalar and `IN` subqueries | Rejected | — | Subquery expressions are rejected as unbounded scalar subqueries. |
| Window functions | Rejected | — | Any function with an `OVER` clause is rejected. |
| `CAST(expr AS type)` / `expr::type` | Yes | Yes | Cast targets map onto the coarse column-type taxonomy (text, int, float, bool, uuid, jsonb); values a cast cannot convert evaluate to `NULL`. `TRY_CAST`/`SAFE_CAST` and `CAST ... FORMAT` are rejected. |
| `expr = ANY(array)` | Yes | Yes | `ANY`/`SOME` with a comparison operator over an array literal; `ANY(subquery)` is rejected. Also the materialized form of list-valued `$user.*` permission predicates. |
| `cardinality(array)` | Yes | Yes | Validated for arity and typed as integer; evaluates to the array length (`NULL` for non-array input). |
| `coalesce(...)` | Yes | Yes | Returns the first non-`NULL` argument. |
| Other scalar functions | Rejected | — | Rejected at parse time with `SqlError::UnsupportedFunction` so the accepted surface never exceeds what the dataflow evaluates. |
| `SELECT DISTINCT` | Yes | Yes | Lowers to a `Distinct` MIR node, compiled onto differential's `distinct` operator. |
| `DISTINCT ON (exprs)` | Yes | Yes | With an `ORDER BY`, the Postgres rule applies (`DISTINCT ON` expressions must match the initial `ORDER BY` expressions) and the order keys pick the surviving row per group; without one the surviving row is arbitrary but deterministic. Order keys the projection dropped are carried as hidden columns and re-projected away above the sort. |
| Plain `GROUP BY` | Yes | Yes | Grouping expressions must be column references; the dataflow compiles any number of grouping columns (including zero — see Aggregates). |
| `GROUP BY ALL`, grouping sets, rollup, cube | Rejected | — | Group-by modifiers are outside the Phase 1 subset. |
| Aggregates | Yes | Yes | `count`, `sum`, `min`, `max`, and `avg` are recognized in projections and folded incrementally, each over its own value expression. A global aggregate (no `GROUP BY`) returns one row even over an empty input (`COUNT` = 0, other aggregates `NULL`). `SUM` follows its input's numeric type (integer in, integer out; float in, float out), `MIN`/`MAX` order any datum type, `AVG` yields a float, and `COUNT(col)` skips `NULL`s. `SUM`/`MIN`/`MAX` over empty or all-`NULL` groups yield `NULL`. |
| Aggregate `DISTINCT` | Yes | Yes | `COUNT`/`SUM`/`AVG(DISTINCT col)` fold each distinct value once; `MIN`/`MAX(DISTINCT)` are equivalent to their plain forms. |
| Named aggregate arguments | Rejected | — | Aggregate arguments must be unnamed expressions or wildcards. |
| `HAVING` | Yes | Yes | Lowers to a filter above the aggregate. Aggregate calls in the predicate reference aggregate output columns; aggregates that appear only in `HAVING` are computed as hidden columns and dropped by the projection. Requires aggregates or `GROUP BY`; `EXISTS` inside `HAVING` is rejected. |
| `ORDER BY ... LIMIT` | Yes | Yes | Literal integer `LIMIT` and optional literal integer `OFFSET` are supported. The dataflow compiles any number of typed sort keys with per-key `ASC`/`DESC` (TopK); `NULL`s sort last ascending and first descending, matching Postgres defaults. Sort keys need not appear in the projection — missing keys are carried as hidden columns and dropped after the sort. |
| `ORDER BY` without `LIMIT` | Yes | Yes | Plans as a TopK with an unbounded limit ("sort the whole result set"). |
| `TOP`, `SELECT INTO`, non-standard SELECT clauses | Rejected during lowering | — | Includes prewhere, cluster/distribute/sort-by, named windows, qualify, value-table mode, and connect-by. |
| `UNION ALL` | Yes | Yes | Lowers to a `Union` MIR node; branches are concatenated positionally (both branches must have the same column count; output columns are named after the left branch). |
| `UNION` without `ALL` | Yes | Yes | Concatenation followed by duplicate elimination. |
| `EXCEPT [ALL]`, `INTERSECT [ALL]` | Yes | Yes | SQL bag semantics per quantifier: set variants deduplicate, `ALL` variants use bag difference / bag minimum of per-row counts. |
| `VALUES`, `TABLE`, `INSERT`/`UPDATE` query bodies | Rejected | — | Only SELECT-shaped query bodies are supported. |

## Examples

Supported single-table query:

```sql
SELECT DISTINCT id, title AS post_title
FROM posts
WHERE author_id = 42
ORDER BY created_at DESC
LIMIT 5 OFFSET 10;
```

Supported join:

```sql
SELECT posts.id
FROM posts
JOIN authors ON posts.author_id = authors.id;
```

Supported aggregate:

```sql
SELECT author_id, count(*) AS post_count, max(created_at)
FROM posts
WHERE author_id = 42
GROUP BY author_id;
```

Supported CTE:

```sql
WITH recent_posts AS (
  SELECT id, author_id FROM posts WHERE author_id = 42
)
SELECT id FROM recent_posts;
```

Supported `UNION ALL`:

```sql
SELECT id FROM posts
UNION ALL
SELECT id FROM archived_posts;
```
