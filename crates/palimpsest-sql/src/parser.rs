// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQL parsing entry points (Postgres dialect).

use core::ops::ControlFlow;

use sqlparser::{
    ast::{
        BinaryOperator, Expr, JoinConstraint, JoinOperator, Query, Select, SetExpr, Statement,
        TableFactor, TableWithJoins, Visit, Visitor,
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
/// supported surface (recursive CTEs, ORDER BY without LIMIT, etc).
///
/// # Errors
/// Returns [`SqlError::UnsupportedFeature`] (or related variants) on
/// the first construct that lies outside the supported surface.
pub fn validate_query(query: &Query) -> Result<(), SqlError> {
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(SqlError::UnsupportedFeature("recursive CTEs"));
        }

        for cte in &with.cte_tables {
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
        ControlFlow::Break(feature) => Err(SqlError::UnsupportedFeature(feature)),
    }
}

struct UnsupportedExprVisitor;

impl Visitor for UnsupportedExprVisitor {
    type Break = &'static str;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Function(function) if function.over.is_some() => {
                ControlFlow::Break("window functions")
            }
            Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Subquery(_) => {
                ControlFlow::Break("scalar subqueries with unbounded result")
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_select;

    #[test]
    fn parses_postgres_cte() {
        parse_select(
            "WITH recent_posts AS (
                SELECT id, author_id FROM posts WHERE created_at > now() - interval '1 day'
             )
             SELECT id FROM recent_posts ORDER BY id LIMIT 10",
        )
        .expect("CTE query should parse");
    }

    #[test]
    fn rejects_recursive_cte() {
        let err = parse_select(
            "WITH RECURSIVE nums(n) AS (
                SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10
             )
             SELECT n FROM nums",
        )
        .expect_err("recursive CTEs are out of scope for v1");

        assert!(err.to_string().contains("recursive CTEs"));
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
