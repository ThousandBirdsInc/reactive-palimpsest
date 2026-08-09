// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! FROM-alias resolution.
//!
//! The MIR carries no notion of a table alias: `BaseTable` nodes are
//! keyed by table name, and column provenance downstream attributes
//! columns to table names. A query written `FROM tickets t ... t.id`
//! therefore used to miss provenance attribution entirely and fall
//! back to bare-name (last-wins) lookup — silently binding qualified
//! references to the wrong side of a join.
//!
//! This pass runs on the parsed AST before lowering and rewrites every
//! alias-qualified column reference to its canonical relation name
//! (`t.id` → `tickets.id`), then drops the alias from the table
//! factor. Resolution is scope-aware: a subquery's own FROM aliases
//! shadow the enclosing query's, and references that escape a
//! subquery (correlated `EXISTS` / `LATERAL` predicates) resolve
//! against the enclosing scopes.
//!
//! Aliases on derived tables (`(SELECT ...) x`) are left untouched:
//! the alias is the only name such a relation has. Aliases carrying
//! column renames (`t (a, b)`) and aliases on schema-qualified names
//! are also left untouched — those forms keep their present behavior.

use std::collections::HashMap;

use sqlparser::ast::{
    Distinct, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    ObjectName, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins,
};

/// One lexical scope's alias → canonical relation name map.
type Scope = HashMap<String, String>;

/// Resolves every FROM-alias-qualified column reference in `query` to
/// its canonical relation name and strips the now-redundant aliases.
pub fn resolve_from_aliases(query: &mut Query) {
    resolve_query(query, &[]);
}

/// `scopes` is ordered outermost-first; lookups scan innermost-first.
fn lookup<'a>(scopes: &'a [Scope], alias: &str) -> Option<&'a str> {
    scopes
        .iter()
        .rev()
        .find_map(|scope| scope.get(alias).map(String::as_str))
}

fn resolve_query(query: &mut Query, outer: &[Scope]) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            // CTE bodies are not correlated with the enclosing FROM;
            // they open a fresh scope chain.
            resolve_query(&mut cte.query, &[]);
        }
    }

    let local = resolve_set_expr(&mut query.body, outer);

    // Query-level ORDER BY references the SELECT body's relations.
    let mut scopes = outer.to_vec();
    if let Some(local) = local {
        scopes.push(local);
    }
    if let Some(order_by) = &mut query.order_by {
        for key in &mut order_by.exprs {
            resolve_expr(&mut key.expr, &scopes);
        }
    }
}

/// Resolves one set-expression body. Returns the FROM scope when the
/// body is a plain `SELECT` so the caller can resolve query-level
/// ORDER BY keys against it; set operations return `None` (their
/// ORDER BY keys address output columns positionally or by name).
fn resolve_set_expr(body: &mut SetExpr, outer: &[Scope]) -> Option<Scope> {
    match body {
        SetExpr::Select(select) => Some(resolve_select(select, outer)),
        SetExpr::Query(query) => {
            resolve_query(query, outer);
            None
        }
        SetExpr::SetOperation { left, right, .. } => {
            resolve_set_expr(left, outer);
            resolve_set_expr(right, outer);
            None
        }
        _ => None,
    }
}

/// Collects the alias map from one table factor and strips the alias
/// when it can be canonically replaced. Derived tables recurse with
/// the scopes visible at their position.
fn bind_table_factor(factor: &mut TableFactor, scopes: &[Scope], local: &mut Scope) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            // Only single-part names get a mapping: the canonical
            // replacement must be a single identifier for downstream
            // `rel.column` splitting to recognize it.
            if let Some(table_alias) = alias {
                if table_alias.columns.is_empty() && name.0.len() == 1 {
                    local.insert(table_alias.name.value.clone(), name.0[0].value.clone());
                    *alias = None;
                }
            }
        }
        TableFactor::Derived {
            subquery, lateral, ..
        } => {
            // A LATERAL subquery sees the enclosing FROM items; a
            // plain derived table does not.
            if *lateral {
                let mut inner_scopes = scopes.to_vec();
                inner_scopes.push(local.clone());
                resolve_query(subquery, &inner_scopes);
            } else {
                resolve_query(subquery, &[]);
            }
        }
        _ => {}
    }
}

fn resolve_select(select: &mut Select, outer: &[Scope]) -> Scope {
    let mut local = Scope::new();

    // Two passes over FROM: first collect every alias binding (and
    // strip the aliases), then resolve join constraints and lateral
    // subqueries with the complete map. Joins may reference aliases
    // bound later in the clause only in invalid SQL, so the complete
    // map is safe — and simpler than threading partial visibility.
    for item in &mut select.from {
        let TableWithJoins { relation, joins } = item;
        bind_table_factor(relation, outer, &mut local);
        for join in joins.iter_mut() {
            bind_table_factor(&mut join.relation, outer, &mut local);
        }
    }

    let mut scopes = outer.to_vec();
    scopes.push(local);

    for item in &mut select.from {
        for join in &mut item.joins {
            use sqlparser::ast::{JoinConstraint, JoinOperator};
            let constraint = match &mut join.join_operator {
                JoinOperator::Inner(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::FullOuter(constraint)
                | JoinOperator::LeftSemi(constraint)
                | JoinOperator::RightSemi(constraint)
                | JoinOperator::LeftAnti(constraint)
                | JoinOperator::RightAnti(constraint)
                | JoinOperator::AsOf { constraint, .. } => Some(constraint),
                JoinOperator::CrossJoin
                | JoinOperator::CrossApply
                | JoinOperator::OuterApply => None,
            };
            if let Some(JoinConstraint::On(expr)) = constraint {
                resolve_expr(expr, &scopes);
            }
        }
    }

    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => resolve_expr(expr, &scopes),
            SelectItem::ExprWithAlias { expr, .. } => resolve_expr(expr, &scopes),
            SelectItem::QualifiedWildcard(object_name, _) => {
                resolve_object_name(object_name, &scopes);
            }
            SelectItem::Wildcard(_) => {}
        }
    }

    if let Some(selection) = &mut select.selection {
        resolve_expr(selection, &scopes);
    }
    if let GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
        for expr in exprs {
            resolve_expr(expr, &scopes);
        }
    }
    if let Some(having) = &mut select.having {
        resolve_expr(having, &scopes);
    }
    if let Some(Distinct::On(exprs)) = &mut select.distinct {
        for expr in exprs {
            resolve_expr(expr, &scopes);
        }
    }

    scopes.pop().expect("local scope pushed above")
}

/// Rewrites a single-part qualifier (`alias.*`, function wildcards)
/// when it names an alias in scope.
fn resolve_object_name(name: &mut ObjectName, scopes: &[Scope]) {
    if name.0.len() == 1 {
        if let Some(canonical) = lookup(scopes, &name.0[0].value) {
            name.0[0] = Ident::new(canonical);
        }
    }
}

/// Recursively rewrites alias qualifiers inside an expression.
/// Variants without column references (or outside the engine's
/// supported SQL surface) pass through untouched — an unrewritten
/// alias degrades to today's bare-name fallback, never to a new
/// failure mode.
#[allow(clippy::too_many_lines)]
fn resolve_expr(expr: &mut Expr, scopes: &[Scope]) {
    match expr {
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                if let Some(canonical) = lookup(scopes, &parts[0].value) {
                    parts[0] = Ident::new(canonical);
                }
            }
        }
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. }
        | Expr::Ceil { expr: inner, .. }
        | Expr::Floor { expr: inner, .. }
        | Expr::Extract { expr: inner, .. } => resolve_expr(inner, scopes),
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            resolve_expr(left, scopes);
            resolve_expr(right, scopes);
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        }
        | Expr::ILike {
            expr: inner,
            pattern,
            ..
        }
        | Expr::SimilarTo {
            expr: inner,
            pattern,
            ..
        } => {
            resolve_expr(inner, scopes);
            resolve_expr(pattern, scopes);
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            resolve_expr(inner, scopes);
            resolve_expr(low, scopes);
            resolve_expr(high, scopes);
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            resolve_expr(inner, scopes);
            for element in list {
                resolve_expr(element, scopes);
            }
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            ..
        } => {
            resolve_expr(inner, scopes);
            resolve_query(subquery, scopes);
        }
        Expr::Exists { subquery, .. } => resolve_query(subquery, scopes),
        Expr::Subquery(subquery) => resolve_query(subquery, scopes),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                resolve_expr(operand, scopes);
            }
            for condition in conditions {
                resolve_expr(condition, scopes);
            }
            for result in results {
                resolve_expr(result, scopes);
            }
            if let Some(else_result) = else_result {
                resolve_expr(else_result, scopes);
            }
        }
        Expr::Tuple(elements) | Expr::Array(sqlparser::ast::Array { elem: elements, .. }) => {
            for element in elements {
                resolve_expr(element, scopes);
            }
        }
        Expr::Position {
            expr: inner, r#in, ..
        } => {
            resolve_expr(inner, scopes);
            resolve_expr(r#in, scopes);
        }
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            resolve_expr(inner, scopes);
            if let Some(from) = substring_from {
                resolve_expr(from, scopes);
            }
            if let Some(length) = substring_for {
                resolve_expr(length, scopes);
            }
        }
        Expr::Trim {
            expr: inner,
            trim_what,
            trim_characters,
            ..
        } => {
            resolve_expr(inner, scopes);
            if let Some(what) = trim_what {
                resolve_expr(what, scopes);
            }
            if let Some(characters) = trim_characters {
                for character in characters {
                    resolve_expr(character, scopes);
                }
            }
        }
        Expr::Function(function) => {
            resolve_function_arguments(&mut function.args, scopes);
            if let Some(filter) = &mut function.filter {
                resolve_expr(filter, scopes);
            }
        }
        _ => {}
    }
}

fn resolve_function_arguments(arguments: &mut FunctionArguments, scopes: &[Scope]) {
    match arguments {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                let arg_expr = match arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => arg,
                };
                match arg_expr {
                    FunctionArgExpr::Expr(expr) => resolve_expr(expr, scopes),
                    FunctionArgExpr::QualifiedWildcard(name) => {
                        resolve_object_name(name, scopes);
                    }
                    FunctionArgExpr::Wildcard => {}
                }
            }
        }
        FunctionArguments::Subquery(subquery) => resolve_query(subquery, scopes),
        FunctionArguments::None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_from_aliases;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn resolve(sql: &str) -> String {
        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let Statement::Query(query) = &mut statements[0] else {
            panic!("expected query");
        };
        resolve_from_aliases(query);
        query.to_string()
    }

    #[test]
    fn rewrites_projection_where_and_join_qualifiers() {
        let resolved = resolve(
            "SELECT t.id, o.name FROM tickets AS t \
             JOIN owners AS o ON t.owner_id = o.id WHERE o.name = 'x'",
        );
        assert_eq!(
            resolved,
            "SELECT tickets.id, owners.name FROM tickets \
             JOIN owners ON tickets.owner_id = owners.id WHERE owners.name = 'x'"
        );
    }

    #[test]
    fn correlated_exists_sees_outer_alias() {
        let resolved = resolve(
            "SELECT t.id FROM tickets AS t WHERE NOT EXISTS \
             (SELECT 1 FROM blocks AS b WHERE b.ticket_id = t.id)",
        );
        assert_eq!(
            resolved,
            "SELECT tickets.id FROM tickets WHERE NOT EXISTS \
             (SELECT 1 FROM blocks WHERE blocks.ticket_id = tickets.id)"
        );
    }

    #[test]
    fn inner_alias_shadows_outer() {
        let resolved = resolve(
            "SELECT t.id FROM tickets AS t WHERE EXISTS \
             (SELECT 1 FROM tags AS t WHERE t.id = 1)",
        );
        assert_eq!(
            resolved,
            "SELECT tickets.id FROM tickets WHERE EXISTS \
             (SELECT 1 FROM tags WHERE tags.id = 1)"
        );
    }

    #[test]
    fn qualified_wildcard_and_order_by_resolve() {
        let resolved =
            resolve("SELECT t.* FROM tickets AS t ORDER BY t.created_at DESC LIMIT 10");
        assert_eq!(
            resolved,
            "SELECT tickets.* FROM tickets ORDER BY tickets.created_at DESC LIMIT 10"
        );
    }

    #[test]
    fn derived_table_alias_is_preserved() {
        let resolved = resolve("SELECT x.id FROM (SELECT id FROM tickets) AS x");
        assert_eq!(resolved, "SELECT x.id FROM (SELECT id FROM tickets) AS x");
    }

    #[test]
    fn aggregates_group_by_and_having_resolve() {
        let resolved = resolve(
            "SELECT e.category, COUNT(e.id) FROM events AS e \
             GROUP BY e.category HAVING COUNT(e.id) > 3",
        );
        assert_eq!(
            resolved,
            "SELECT events.category, COUNT(events.id) FROM events \
             GROUP BY events.category HAVING COUNT(events.id) > 3"
        );
    }

    #[test]
    fn cte_reference_alias_resolves_to_cte_name() {
        let resolved = resolve(
            "WITH recent AS (SELECT id FROM tickets) SELECT r.id FROM recent AS r",
        );
        assert_eq!(
            resolved,
            "WITH recent AS (SELECT id FROM tickets) SELECT recent.id FROM recent"
        );
    }
}
