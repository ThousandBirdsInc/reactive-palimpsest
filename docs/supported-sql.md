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
diffs verbatim, **without applying the query's operators server-side**
(joins, set ops, `DISTINCT`, casts in projections, and recursive CTEs
are in this bucket today). Note that server-side permission row
filters are part of the compiled dataflow, so they also only apply on
the Evaluates path — treat "Parses: yes / Evaluates: no" rows as
unsuitable for permission-sensitive data until the pass-through path
is retired.

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
| `WITH RECURSIVE` | Yes | Pass-through | Self-referential CTEs must be `base UNION [ALL] step` with exactly one self-reference in the step term (linear recursion); they lower to a `Fixpoint` MIR node, which the dataflow compiler does not execute yet. |
| Projection expressions | Yes | Partial | Column references, aliases, `coalesce`, and `cardinality` evaluate; other expressions make the plan fall back to pass-through. |
| `*` and `relation.*` | Yes | Pass-through | Wildcards are preserved as projection strings; expansion against catalog metadata is not implemented yet. |
| `FROM table` | Yes | Yes | Table names lower to `BaseTable` MIR nodes. |
| Derived tables | Yes | Partial | Subqueries in `FROM (...) AS alias` lower through the nested query path; they evaluate when every lowered operator is a compiled one. |
| `JOIN LATERAL (...) ON TRUE`, `CROSS JOIN LATERAL` | Yes | Pass-through | Correlated equality predicates in the subquery's `WHERE` are decorrelated into equi-join keys; `LIMIT`/`OFFSET` inside a lateral subquery is rejected (per-row limits have no MIR encoding). Joins are not compiled onto the dataflow yet. |
| Multiple comma-separated `FROM` items | Rejected during lowering | — | Joins must be expressed with explicit join syntax. |
| Table functions and special table factors | Rejected | — | Includes unsupported table-factor forms from `sqlparser-rs`. |
| `INNER JOIN ... ON` | Yes | Pass-through | Join predicates must be equi-joins between column references. The `Join` MIR node is not compiled onto the dataflow yet. |
| `LEFT JOIN ... ON` | Yes | Pass-through | Conjunctions of equi-join predicates are supported. Same dataflow gap as `INNER JOIN`. |
| `RIGHT JOIN`, `FULL JOIN` | Rejected | — | Rejected during validation. |
| `JOIN ... USING`, natural joins, joins without `ON` | Rejected during lowering | — | The parser validator permits some forms, but MIR lowering requires `ON`. |
| `CROSS JOIN` | Rejected during lowering | — | Cross products are not in the Phase 1 MIR subset. |
| Theta joins | Rejected | — | Join predicates must be equality predicates over column references. |
| `WHERE` | Yes | Yes | Predicates are retained as canonicalized strings and compiled by the expression evaluator (boolean logic, comparisons, `IS [NOT] NULL`, `ANY(array)`, `coalesce`, `cardinality`). |
| `[NOT] EXISTS (...)` | Yes | Pass-through | Correlated `EXISTS` conjuncts in `WHERE` lower to semi/anti joins on the correlation columns; uncorrelated `EXISTS` and `EXISTS` under `OR` are rejected. Semi/anti joins are not compiled onto the dataflow yet. |
| Scalar and `IN` subqueries | Rejected | — | Subquery expressions are rejected as unbounded scalar subqueries. |
| Window functions | Rejected | — | Any function with an `OVER` clause is rejected. |
| `CAST(expr AS type)` / `expr::type` | Yes | Pass-through | Cast targets map onto the coarse column-type taxonomy for validation; `TRY_CAST`/`SAFE_CAST` and `CAST ... FORMAT` are rejected. The evaluator does not execute casts yet. |
| `expr = ANY(array)` | Yes | Yes | `ANY`/`SOME` with a comparison operator over an array literal; `ANY(subquery)` is rejected. Also the materialized form of list-valued `$user.*` permission predicates. |
| `cardinality(array)` | Yes | Yes | Validated for arity and typed as integer; evaluates to the array length (`NULL` for non-array input). |
| `coalesce(...)` | Yes | Yes | Returns the first non-`NULL` argument. |
| Other scalar functions | Rejected | — | Rejected at parse time with `SqlError::UnsupportedFunction` so the accepted surface never exceeds what the dataflow evaluates. |
| `SELECT DISTINCT` | Yes | Pass-through | Lowers to a `Distinct` MIR node, which the dataflow compiler does not execute yet. |
| `DISTINCT ON (exprs)` | Yes | Pass-through | Lowers to a `DistinctOn` MIR node; with an `ORDER BY`, the Postgres rule applies (`DISTINCT ON` expressions must match the initial `ORDER BY` expressions) and the order keys pick the surviving row per group. |
| Plain `GROUP BY` | Yes | Yes | Grouping expressions must be column references; the dataflow compiles single-column grouping keys. |
| `GROUP BY ALL`, grouping sets, rollup, cube | Rejected | — | Group-by modifiers are outside the Phase 1 subset. |
| Aggregates | Yes | Yes | `count`, `sum`, `min`, `max`, and `avg` are recognized in projections and folded incrementally. |
| Aggregate `DISTINCT` | Yes | Yes | Preserved in aggregate argument metadata (`COUNT(DISTINCT ...)`). |
| Named aggregate arguments | Rejected | — | Aggregate arguments must be unnamed expressions or wildcards. |
| `HAVING` | Rejected during lowering | — | Post-aggregate filtering is not implemented yet. |
| `ORDER BY ... LIMIT` | Yes | Yes | `LIMIT` is required; literal integer `LIMIT` and optional literal integer `OFFSET` are supported. The dataflow compiles single-column sort keys (TopK). |
| `ORDER BY` without `LIMIT` | Rejected | — | Palimpsest does not represent unbounded ordering. |
| `TOP`, `SELECT INTO`, non-standard SELECT clauses | Rejected during lowering | — | Includes prewhere, cluster/distribute/sort-by, named windows, qualify, value-table mode, and connect-by. |
| `UNION ALL` | Yes | Pass-through | Lowers to a `Union` MIR node, which the dataflow compiler does not execute yet. |
| `UNION` without `ALL` | Rejected during lowering | — | Duplicate elimination for set operations is not implemented yet. |
| `EXCEPT`, `INTERSECT` | Rejected during lowering | — | Not part of the Phase 1 subset. |
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
