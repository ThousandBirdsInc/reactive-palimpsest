# Supported SQL

Palimpsest currently accepts a deliberately small PostgreSQL-flavored SELECT
subset. This page tracks the Phase 1 SQL frontend surface implemented in
`palimpsest-sql`.

`parse_select` parses with `sqlparser-rs`'s `PostgreSqlDialect`, validates the
frontier features Palimpsest rejects early, and requires exactly one statement.
`parse_and_lower` then lowers the supported subset into MIR.

## Support Table

| SQL surface | Status | Notes |
| --- | --- | --- |
| Single `SELECT` query | Supported | Non-query statements and multi-statement inputs are rejected. |
| `WITH name AS (...)` | Supported | Non-recursive CTEs lower to `CteRef` placeholders plus expansion edges. |
| `WITH RECURSIVE` | Supported | Self-referential CTEs must be `base UNION [ALL] step` with exactly one self-reference in the step term (linear recursion); they lower to a `Fixpoint` MIR node whose step reads a `RecursiveRef` leaf. |
| Projection expressions | Supported | Projection names are stored as expression strings or explicit aliases. |
| `*` and `relation.*` | Parsed and lowered | Wildcards are preserved as projection strings; expansion against catalog metadata is not implemented yet. |
| `FROM table` | Supported | Table names lower to `BaseTable` MIR nodes. |
| Derived tables | Supported | Subqueries in `FROM (...) AS alias` lower through the nested query path. |
| `JOIN LATERAL (...) ON TRUE`, `CROSS JOIN LATERAL` | Supported | Correlated equality predicates in the subquery's `WHERE` are decorrelated into equi-join keys; `LIMIT`/`OFFSET` inside a lateral subquery is rejected (per-row limits have no MIR encoding). |
| Multiple comma-separated `FROM` items | Rejected during lowering | Joins must be expressed with explicit join syntax. |
| Table functions and special table factors | Rejected | Includes unsupported table-factor forms from `sqlparser-rs`. |
| `INNER JOIN ... ON` | Supported | Join predicates must be equi-joins between column references. |
| `LEFT JOIN ... ON` | Supported | Conjunctions of equi-join predicates are supported. |
| `RIGHT JOIN`, `FULL JOIN` | Rejected | Rejected during validation. |
| `JOIN ... USING`, natural joins, joins without `ON` | Rejected during lowering | The parser validator permits some forms, but MIR lowering requires `ON`. |
| `CROSS JOIN` | Rejected during lowering | Cross products are not in the Phase 1 MIR subset. |
| Theta joins | Rejected | Join predicates must be equality predicates over column references. |
| `WHERE` | Supported | Predicates are retained as canonicalized strings for the current MIR scaffold. |
| `[NOT] EXISTS (...)` | Supported | Correlated `EXISTS` conjuncts in `WHERE` lower to semi/anti joins on the correlation columns; uncorrelated `EXISTS` and `EXISTS` under `OR` are rejected. |
| Scalar and `IN` subqueries | Rejected | Subquery expressions are rejected as unbounded scalar subqueries. |
| Window functions | Rejected | Any function with an `OVER` clause is rejected. |
| `CAST(expr AS type)` / `expr::type` | Supported | Cast targets map onto the coarse column-type taxonomy for validation; `TRY_CAST`/`SAFE_CAST` and `CAST ... FORMAT` are rejected. |
| `expr = ANY(array)` | Supported | `ANY`/`SOME` with a comparison operator over an array expression; `ANY(subquery)` is rejected. |
| `cardinality(array)` | Supported | Validated for arity and typed as integer. |
| `SELECT DISTINCT` | Supported | Lowers to a `Distinct` MIR node. |
| `DISTINCT ON (exprs)` | Supported | Lowers to a `DistinctOn` MIR node; with an `ORDER BY`, the Postgres rule applies (`DISTINCT ON` expressions must match the initial `ORDER BY` expressions) and the order keys pick the surviving row per group. |
| Plain `GROUP BY` | Supported | Grouping expressions must be column references. |
| `GROUP BY ALL`, grouping sets, rollup, cube | Rejected | Group-by modifiers are outside the Phase 1 subset. |
| Aggregates | Supported | `count`, `sum`, `min`, `max`, and `avg` are recognized in projections. |
| Aggregate `DISTINCT` | Supported | Preserved in aggregate argument metadata. |
| Named aggregate arguments | Rejected | Aggregate arguments must be unnamed expressions or wildcards. |
| `HAVING` | Rejected during lowering | Post-aggregate filtering is not implemented yet. |
| `ORDER BY ... LIMIT` | Supported | `LIMIT` is required; literal integer `LIMIT` and optional literal integer `OFFSET` are supported. |
| `ORDER BY` without `LIMIT` | Rejected | Palimpsest does not represent unbounded ordering. |
| `TOP`, `SELECT INTO`, non-standard SELECT clauses | Rejected during lowering | Includes prewhere, cluster/distribute/sort-by, named windows, qualify, value-table mode, and connect-by. |
| `UNION ALL` | Supported | Lowers to a `Union` MIR node. |
| `UNION` without `ALL` | Rejected during lowering | Duplicate elimination for set operations is not implemented yet. |
| `EXCEPT`, `INTERSECT` | Rejected during lowering | Not part of the Phase 1 subset. |
| `VALUES`, `TABLE`, `INSERT`/`UPDATE` query bodies | Rejected | Only SELECT-shaped query bodies are supported. |

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
