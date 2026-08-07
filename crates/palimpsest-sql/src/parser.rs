// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQL parsing entry points (Postgres dialect).

use core::ops::ControlFlow;

use sqlparser::{
    ast::{
        visit_relations, BinaryOperator, Cte, Expr, JoinConstraint, JoinOperator, Query, Select,
        SetExpr, SetOperator, Statement, TableFactor, TableWithJoins, Value, Visit, Visitor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::{limits::enforce_input_size, QueryLimits, SqlError};

/// Parses a single `SELECT` statement under the default
/// [`QueryLimits`].
///
/// # Errors
/// Returns [`SqlError`] if the input fails parsing, validation, or
/// size limits.
pub fn parse_select(sql: &str) -> Result<Statement, SqlError> {
    parse_select_with_limits(sql, QueryLimits::DEFAULT)
}

/// Like [`parse_select`] but with a caller-supplied [`QueryLimits`].
///
/// # Errors
/// Returns [`SqlError::QueryTooLarge`] before invoking the parser if
/// the input exceeds `limits.max_input_bytes`; otherwise propagates
/// any parse / validation error.
pub fn parse_select_with_limits(sql: &str, limits: QueryLimits) -> Result<Statement, SqlError> {
    enforce_input_size(sql, limits)?;
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    if statements.len() != 1 {
        return Err(SqlError::StatementCount(statements.len()));
    }

    let statement = statements.remove(0);
    let Statement::Query(query) = &statement else {
        return Err(SqlError::UnsupportedStatement);
    };

    validate_query(query)?;
    Ok(statement)
}

/// Walks a parsed query tree and rejects features outside the v1
/// supported surface (window functions, scalar subqueries, RIGHT/FULL
/// joins, etc), and enforces shape rules for `WITH RECURSIVE` CTEs.
///
/// # Errors
/// Returns [`SqlError::UnsupportedFeature`] (or related variants) on
/// the first construct that lies outside the supported surface.
pub fn validate_query(query: &Query) -> Result<(), SqlError> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if with.recursive
                && count_relation_references(cte.query.as_ref(), &cte.alias.name.value) > 0
            {
                validate_recursive_cte(cte)?;
            }
            validate_query(&cte.query)?;
        }
    }

    // ORDER BY without LIMIT is allowed: the lowerer plans it as a
    // `TopK` with `limit = usize::MAX`, i.e. "sort the whole result
    // set." Cheap for the small result-sets typical of live
    // subscriptions; the QueryLimits node-count budget is the real
    // ceiling on how big a sort the server will accept.

    validate_expression_surface(query)?;
    validate_set_expr(&query.body)
}

fn validate_set_expr(expr: &SetExpr) -> Result<(), SqlError> {
    match expr {
        SetExpr::Select(select) => validate_select(select),
        SetExpr::Query(query) => validate_query(query),
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expr(left)?;
            validate_set_expr(right)
        }
        SetExpr::Values(_) => Err(SqlError::UnsupportedFeature("VALUES queries")),
        SetExpr::Insert(_) => Err(SqlError::UnsupportedFeature("INSERT in query body")),
        SetExpr::Update(_) => Err(SqlError::UnsupportedFeature("UPDATE in query body")),
        SetExpr::Table(_) => Err(SqlError::UnsupportedFeature("TABLE queries")),
    }
}

fn validate_select(select: &Select) -> Result<(), SqlError> {
    for table in &select.from {
        validate_table_with_joins(table)?;
    }

    Ok(())
}

fn validate_table_with_joins(table: &TableWithJoins) -> Result<(), SqlError> {
    validate_table_factor(&table.relation)?;

    for join in &table.joins {
        match &join.join_operator {
            JoinOperator::RightOuter(_) => {
                return Err(SqlError::UnsupportedFeature("RIGHT JOIN"));
            }
            JoinOperator::FullOuter(_) => {
                return Err(SqlError::UnsupportedFeature("FULL JOIN"));
            }
            JoinOperator::Inner(constraint) | JoinOperator::LeftOuter(constraint) => {
                validate_join_constraint(constraint)?;
            }
            JoinOperator::CrossJoin => {}
            _ => return Err(SqlError::UnsupportedFeature("non-standard joins")),
        }

        validate_table_factor(&join.relation)?;
    }

    Ok(())
}

fn validate_table_factor(table: &TableFactor) -> Result<(), SqlError> {
    match table {
        TableFactor::Table { .. } => Ok(()),
        TableFactor::Derived { subquery, .. } => validate_query(subquery),
        _ => Err(SqlError::UnsupportedFeature(
            "table functions or special table factors",
        )),
    }
}

fn validate_join_constraint(constraint: &JoinConstraint) -> Result<(), SqlError> {
    match constraint {
        JoinConstraint::On(expr) if is_equi_join_predicate(expr) => Ok(()),
        JoinConstraint::On(_) => Err(SqlError::UnsupportedFeature("theta joins")),
        JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None => Ok(()),
    }
}

fn is_equi_join_predicate(expr: &Expr) -> bool {
    match expr {
        // `ON TRUE` — the idiomatic constraint for correlated
        // `JOIN LATERAL` sources, where the real join keys live in the
        // subquery's WHERE clause.
        Expr::Value(Value::Boolean(true)) => true,
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            matches!(
                left.as_ref(),
                Expr::Identifier(_) | Expr::CompoundIdentifier(_)
            ) && matches!(
                right.as_ref(),
                Expr::Identifier(_) | Expr::CompoundIdentifier(_)
            )
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => is_equi_join_predicate(left) && is_equi_join_predicate(right),
        _ => false,
    }
}

fn validate_expression_surface(query: &Query) -> Result<(), SqlError> {
    let mut visitor = UnsupportedExprVisitor;
    match query.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

struct UnsupportedExprVisitor;

impl Visitor for UnsupportedExprVisitor {
    type Break = SqlError;

    /// Gate every scalar expression to the surface the dataflow's
    /// expression evaluator implements, so an accepted query always
    /// compiles onto the dataflow instead of silently degrading to the
    /// pass-through path. Sub-expressions of allowed forms are visited
    /// by the caller's traversal, so each arm only vets its own node.
    #[allow(clippy::too_many_lines)]
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        use sqlparser::ast::{CastKind, UnaryOperator};
        match expr {
            Expr::Function(function) if function.over.is_some() => {
                ControlFlow::Break(SqlError::UnsupportedFeature("window functions"))
            }
            // Aggregates fold in the dataflow's aggregate operator;
            // `coalesce` / `cardinality` evaluate per-row. Anything
            // else would be accepted and never evaluated, so it is
            // rejected here with a named error.
            Expr::Function(function) => {
                let name = function.name.to_string().to_ascii_lowercase();
                if matches!(
                    name.as_str(),
                    "count" | "sum" | "min" | "max" | "avg" | "coalesce" | "cardinality"
                ) {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(SqlError::UnsupportedFunction { name })
                }
            }
            // EXISTS subqueries are boolean (bounded) — supported, but
            // their relational structure gets the same validation as
            // the outer query.
            Expr::Exists { subquery, .. } => match validate_query(subquery) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            },
            Expr::InSubquery { .. } | Expr::Subquery(_) => ControlFlow::Break(
                SqlError::UnsupportedFeature("scalar subqueries with unbounded result"),
            ),
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Nested(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsDistinctFrom(..)
            | Expr::IsNotDistinctFrom(..)
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::Case { .. }
            | Expr::Array(_) => ControlFlow::Continue(()),
            Expr::Value(value) => match value {
                Value::Boolean(_)
                | Value::Number(..)
                | Value::SingleQuotedString(_)
                | Value::DoubleQuotedString(_)
                | Value::EscapedStringLiteral(_)
                | Value::UnicodeStringLiteral(_)
                | Value::NationalStringLiteral(_)
                | Value::Null => ControlFlow::Continue(()),
                _ => ControlFlow::Break(SqlError::UnsupportedFeature("unsupported literal form")),
            },
            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
                | BinaryOperator::StringConcat => ControlFlow::Continue(()),
                _ => {
                    ControlFlow::Break(SqlError::UnsupportedFeature("unsupported binary operator"))
                }
            },
            Expr::UnaryOp { op, .. } => match op {
                UnaryOperator::Not | UnaryOperator::Minus | UnaryOperator::Plus => {
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Break(SqlError::UnsupportedFeature("unsupported unary operator")),
            },
            Expr::Like { any: false, .. } | Expr::ILike { any: false, .. } => {
                ControlFlow::Continue(())
            }
            Expr::Like { .. } | Expr::ILike { .. } => {
                ControlFlow::Break(SqlError::UnsupportedFeature("LIKE ANY"))
            }
            Expr::AnyOp {
                compare_op, right, ..
            } => {
                let comparison = matches!(
                    compare_op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                );
                let array_like = matches!(
                    right.as_ref(),
                    Expr::Array(_) | Expr::Value(Value::SingleQuotedString(_))
                );
                if comparison && array_like {
                    ControlFlow::Continue(())
                } else if matches!(right.as_ref(), Expr::Subquery(_)) {
                    ControlFlow::Break(SqlError::UnsupportedFeature(
                        "ANY over subqueries with unbounded result",
                    ))
                } else {
                    ControlFlow::Break(SqlError::UnsupportedFeature(
                        "ANY over non-array expressions",
                    ))
                }
            }
            Expr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                data_type,
                format: None,
                ..
            } => {
                if crate::catalog::ColumnType::from_cast_target(data_type).is_some() {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(SqlError::UnsupportedFeature("unsupported cast target"))
                }
            }
            Expr::Cast { .. } => ControlFlow::Break(SqlError::UnsupportedFeature(
                "TRY_CAST / SAFE_CAST / CAST ... FORMAT",
            )),
            Expr::Interval(_) => {
                ControlFlow::Break(SqlError::UnsupportedFeature("INTERVAL literals"))
            }
            _ => ControlFlow::Break(SqlError::UnsupportedFeature(
                "unsupported scalar expression",
            )),
        }
    }
}

/// Counts `FROM`-position references to a relation named `name`
/// anywhere inside `node` (including nested subqueries).
pub(crate) fn count_relation_references<V: Visit>(node: &V, name: &str) -> usize {
    let mut count = 0_usize;
    let _: ControlFlow<()> = visit_relations(node, |relation| {
        if relation.0.last().is_some_and(|part| part.value == name) {
            count += 1;
        }
        ControlFlow::Continue(())
    });
    count
}

/// Enforces the Postgres shape rules for a self-referential CTE in a
/// `WITH RECURSIVE` list: the body must be `base UNION [ALL] step`,
/// the base term must not reference the CTE, and the step term must
/// reference it exactly once (linear recursion).
fn validate_recursive_cte(cte: &Cte) -> Result<(), SqlError> {
    let name = &cte.alias.name.value;
    let SetExpr::SetOperation {
        op: SetOperator::Union,
        left,
        right,
        ..
    } = &*cte.query.body
    else {
        return Err(SqlError::InvalidQuery(format!(
            "recursive CTE {name} must have the form 'base term UNION [ALL] recursive term'"
        )));
    };

    if count_relation_references(left.as_ref(), name) > 0 {
        return Err(SqlError::InvalidQuery(format!(
            "recursive CTE {name} must not reference itself in the base (non-recursive) term"
        )));
    }
    match count_relation_references(right.as_ref(), name) {
        1 => Ok(()),
        0 => Err(SqlError::InvalidQuery(format!(
            "recursive CTE {name} must reference itself in the recursive term"
        ))),
        _ => Err(SqlError::InvalidQuery(format!(
            "recursive CTE {name} may reference itself only once (non-linear recursion)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_select;

    #[test]
    fn parses_postgres_cte() {
        parse_select(
            "WITH recent_posts AS (
                SELECT id, author_id FROM posts WHERE created_at > '2026-01-01'
             )
             SELECT id FROM recent_posts ORDER BY id LIMIT 10",
        )
        .expect("CTE query should parse");
    }

    #[test]
    fn rejects_functions_outside_the_evaluable_allowlist() {
        // Every accepted function must be one the engine evaluates —
        // a query that parses but never computes would silently
        // degrade to the pass-through path.
        let err =
            parse_select("SELECT upper(title) FROM posts").expect_err("upper() is not evaluable");
        assert!(
            matches!(&err, crate::SqlError::UnsupportedFunction { name } if name == "upper"),
            "got {err:?}"
        );

        let err = parse_select("SELECT id FROM posts WHERE created_at > now()")
            .expect_err("now() is not evaluable (and not deterministic)");
        assert!(
            matches!(&err, crate::SqlError::UnsupportedFunction { name } if name == "now"),
            "got {err:?}"
        );
    }

    #[test]
    fn rejects_unevaluable_operators() {
        let err = parse_select("SELECT id FROM posts WHERE title ~ 'x'")
            .expect_err("regex match is not evaluable");
        assert!(err.to_string().contains("binary operator"), "got {err}");

        let err = parse_select("SELECT id FROM posts WHERE title SIMILAR TO 'x%'")
            .expect_err("SIMILAR TO is not evaluable");
        assert!(
            err.to_string().contains("unsupported scalar expression"),
            "got {err}"
        );
    }

    #[test]
    fn accepts_like_case_concat_and_distinctness() {
        parse_select(
            "SELECT id,
                    CASE WHEN published THEN 'live' ELSE 'draft' END,
                    title || '!'
             FROM posts
             WHERE title LIKE 'a%'
               AND title NOT ILIKE '%zzz%'
               AND author_id IS DISTINCT FROM 7",
        )
        .expect("evaluable operator forms should parse");
    }

    #[test]
    fn parses_recursive_cte() {
        parse_select(
            "WITH RECURSIVE nums(n) AS (
                SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10
             )
             SELECT n FROM nums",
        )
        .expect("well-formed recursive CTE should parse");
    }

    #[test]
    fn rejects_recursive_cte_without_union() {
        let err = parse_select(
            "WITH RECURSIVE nums(n) AS (
                SELECT n + 1 FROM nums WHERE n < 10
             )
             SELECT n FROM nums",
        )
        .expect_err("recursive CTE must be base UNION step");

        assert!(err.to_string().contains("UNION"));
    }

    #[test]
    fn rejects_recursive_reference_in_base_term() {
        let err = parse_select(
            "WITH RECURSIVE nums(n) AS (
                SELECT n FROM nums UNION ALL SELECT n + 1 FROM nums WHERE n < 10
             )
             SELECT n FROM nums",
        )
        .expect_err("self-reference in the base term is invalid");

        assert!(err.to_string().contains("base"));
    }

    #[test]
    fn rejects_nonlinear_recursion() {
        let err = parse_select(
            "WITH RECURSIVE nums(n) AS (
                SELECT 1
                UNION ALL
                SELECT a.n + b.n FROM nums AS a JOIN nums AS b ON a.n = b.n
             )
             SELECT n FROM nums",
        )
        .expect_err("two self-references are non-linear recursion");

        assert!(err.to_string().contains("only once"));
    }

    #[test]
    fn parses_correlated_exists() {
        parse_select(
            "SELECT id FROM posts
             WHERE EXISTS (
                SELECT 1 FROM comments WHERE comments.post_id = posts.id
             )",
        )
        .expect("correlated EXISTS should parse");
    }

    #[test]
    fn exists_subquery_gets_relational_validation() {
        let err = parse_select(
            "SELECT id FROM posts
             WHERE EXISTS (
                SELECT 1 FROM comments RIGHT JOIN authors ON comments.author_id = authors.id
                WHERE comments.post_id = posts.id
             )",
        )
        .expect_err("RIGHT JOIN inside EXISTS is still rejected");

        assert!(err.to_string().contains("RIGHT JOIN"));
    }

    #[test]
    fn parses_any_over_array() {
        parse_select("SELECT id FROM posts WHERE id = ANY('{1,2,3}')")
            .expect("ANY over an array literal should parse");
    }

    #[test]
    fn rejects_any_over_subquery() {
        let err = parse_select("SELECT id FROM posts WHERE id = ANY(SELECT id FROM posts)")
            .expect_err("ANY over a subquery is out of scope for v1");

        assert!(err.to_string().contains("subqueries"));
    }

    #[test]
    fn parses_casts() {
        parse_select("SELECT CAST(id AS TEXT) FROM posts WHERE id::text = title")
            .expect("CAST and :: casts should parse");
    }

    #[test]
    fn parses_join_on_true() {
        parse_select("SELECT posts.id FROM posts JOIN authors ON TRUE")
            .expect("ON TRUE join constraint should parse");
    }

    #[test]
    fn accepts_order_by_without_limit() {
        // Lowered as TopK { limit: usize::MAX } — "sort the whole
        // result set." See `lower.rs::lower_query_with_context`.
        parse_select("SELECT id FROM posts ORDER BY created_at")
            .expect("ORDER BY without LIMIT is supported");
    }

    #[test]
    fn rejects_right_join() {
        let err = parse_select(
            "SELECT posts.id
             FROM posts RIGHT JOIN authors ON posts.author_id = authors.id",
        )
        .expect_err("RIGHT JOIN is out of scope for v1");

        assert!(err.to_string().contains("RIGHT JOIN"));
    }

    #[test]
    fn rejects_theta_join() {
        let err = parse_select(
            "SELECT posts.id
             FROM posts JOIN authors ON posts.author_id > authors.id",
        )
        .expect_err("theta joins are out of scope for v1");

        assert!(err.to_string().contains("theta joins"));
    }

    #[test]
    fn rejects_window_functions() {
        let err = parse_select(
            "SELECT row_number() OVER (PARTITION BY author_id ORDER BY created_at)
             FROM posts",
        )
        .expect_err("window functions are out of scope for v1");

        assert!(err.to_string().contains("window functions"));
    }

    #[test]
    fn rejects_scalar_subqueries() {
        let err = parse_select("SELECT (SELECT max(id) FROM posts) FROM authors")
            .expect_err("scalar subqueries are out of scope for v1");

        assert!(err.to_string().contains("scalar subqueries"));
    }
}
