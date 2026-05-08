// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeMap;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, UnaryOperator, Value,
    WildcardAdditionalOptions,
};

use crate::{
    catalog::{Catalog, ColumnSchema, ColumnType},
    parse_select, SqlError,
};

#[derive(Debug, Clone)]
struct QueryShape {
    columns: Vec<ColumnSchema>,
    aliases: BTreeMap<String, Expr>,
}

#[derive(Debug, Clone)]
struct RelationBinding {
    columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Default)]
struct Scope {
    relations: BTreeMap<String, RelationBinding>,
}

#[derive(Debug, Clone, Default)]
struct NormalizeContext {
    ctes: BTreeMap<String, QueryShape>,
}

pub fn parse_and_normalize(sql: &str, catalog: &Catalog) -> Result<Statement, SqlError> {
    let statement = parse_select(sql)?;
    normalize_statement(&statement, catalog)
}

pub fn normalize_statement(
    statement: &Statement,
    catalog: &Catalog,
) -> Result<Statement, SqlError> {
    let Statement::Query(query) = statement else {
        return Err(SqlError::UnsupportedStatement);
    };

    let mut query = query.clone();
    normalize_query(&mut query, catalog, &NormalizeContext::default())?;
    Ok(Statement::Query(query))
}

pub fn validate_statement_against_catalog(
    statement: &Statement,
    catalog: &Catalog,
) -> Result<(), SqlError> {
    normalize_statement(statement, catalog).map(|_| ())
}

fn normalize_query(
    query: &mut Query,
    catalog: &Catalog,
    context: &NormalizeContext,
) -> Result<QueryShape, SqlError> {
    let mut local_context = context.clone();

    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            let mut cte_query = (*cte.query).clone();
            let mut shape = normalize_query(&mut cte_query, catalog, &local_context)?;
            cte.query = Box::new(cte_query);

            if !cte.alias.columns.is_empty() {
                if cte.alias.columns.len() != shape.columns.len() {
                    return Err(SqlError::TypeMismatch(format!(
                        "CTE {} has {} column aliases for {} output columns",
                        cte.alias.name,
                        cte.alias.columns.len(),
                        shape.columns.len()
                    )));
                }
                for (column, alias) in shape.columns.iter_mut().zip(&cte.alias.columns) {
                    column.name.clone_from(&alias.value);
                }
            }

            local_context
                .ctes
                .insert(cte.alias.name.value.clone(), shape);
        }
    }

    let shape = normalize_set_expr(&mut query.body, catalog, &local_context)?;

    if let Some(order_by) = &mut query.order_by {
        for order in &mut order_by.exprs {
            order.expr = normalize_output_expr(&order.expr, &shape)?;
        }
    }

    Ok(shape)
}

fn normalize_set_expr(
    expr: &mut SetExpr,
    catalog: &Catalog,
    context: &NormalizeContext,
) -> Result<QueryShape, SqlError> {
    match expr {
        SetExpr::Select(select) => normalize_select(select, catalog, context),
        SetExpr::Query(query) => normalize_query(query, catalog, context),
        SetExpr::SetOperation { left, right, .. } => {
            let left_shape = normalize_set_expr(left, catalog, context)?;
            let right_shape = normalize_set_expr(right, catalog, context)?;
            validate_set_shapes(&left_shape, &right_shape)?;
            Ok(left_shape)
        }
        SetExpr::Values(_) => Err(SqlError::UnsupportedFeature("VALUES queries")),
        SetExpr::Insert(_) => Err(SqlError::UnsupportedFeature("INSERT in query body")),
        SetExpr::Update(_) => Err(SqlError::UnsupportedFeature("UPDATE in query body")),
        SetExpr::Table(_) => Err(SqlError::UnsupportedFeature("TABLE queries")),
    }
}

fn normalize_select(
    select: &mut Select,
    catalog: &Catalog,
    context: &NormalizeContext,
) -> Result<QueryShape, SqlError> {
    let mut scope = Scope::default();

    for table in &mut select.from {
        bind_table_with_joins(table, catalog, context, &mut scope)?;
    }

    if let Some(selection) = &mut select.selection {
        *selection = normalize_expr(selection, &scope, &BTreeMap::new())?;
        let ty = infer_expr_type(selection, &scope)?;
        if !matches!(ty, ColumnType::Bool | ColumnType::Unknown) {
            return Err(SqlError::TypeMismatch(format!(
                "WHERE expression must be boolean, got {ty:?}"
            )));
        }
    }

    let (projection, shape) = normalize_projection(&select.projection, &scope)?;
    select.projection = projection;

    if let GroupByExpr::Expressions(expressions, _) = &mut select.group_by {
        for expression in expressions {
            *expression = normalize_expr(expression, &scope, &shape.aliases)?;
        }
    }

    Ok(shape)
}

fn bind_table_with_joins(
    table: &mut TableWithJoins,
    catalog: &Catalog,
    context: &NormalizeContext,
    scope: &mut Scope,
) -> Result<(), SqlError> {
    bind_table_factor(&mut table.relation, catalog, context, scope)?;

    for join in &mut table.joins {
        bind_table_factor(&mut join.relation, catalog, context, scope)?;
        match &mut join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(predicate))
            | JoinOperator::LeftOuter(JoinConstraint::On(predicate)) => {
                *predicate = normalize_expr(predicate, scope, &BTreeMap::new())?;
                validate_equi_join(predicate)?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn bind_table_factor(
    table: &mut TableFactor,
    catalog: &Catalog,
    context: &NormalizeContext,
    scope: &mut Scope,
) -> Result<(), SqlError> {
    match table {
        TableFactor::Table { name, alias, .. } => {
            let table_name = catalog_table_name(name);
            let columns = if let Some(cte) = context.ctes.get(&table_name) {
                cte.columns.clone()
            } else {
                catalog.require_table(&table_name)?.columns.clone()
            };
            let qualifier = alias
                .as_ref()
                .map_or_else(|| table_name.clone(), |alias| alias.name.value.clone());
            insert_relation(scope, qualifier, columns)
        }
        TableFactor::Derived {
            lateral: false,
            subquery,
            alias,
        } => {
            let shape = normalize_query(subquery, catalog, context)?;
            let Some(alias) = alias else {
                return Err(SqlError::UnsupportedFeature(
                    "derived tables without aliases",
                ));
            };
            insert_relation(scope, alias.name.value.clone(), shape.columns)
        }
        TableFactor::Derived { lateral: true, .. } => {
            Err(SqlError::UnsupportedFeature("LATERAL derived tables"))
        }
        _ => Err(SqlError::UnsupportedFeature(
            "table functions or special table factors",
        )),
    }
}

fn insert_relation(
    scope: &mut Scope,
    qualifier: String,
    columns: Vec<ColumnSchema>,
) -> Result<(), SqlError> {
    if scope
        .relations
        .insert(qualifier.clone(), RelationBinding { columns })
        .is_some()
    {
        return Err(SqlError::AmbiguousColumn(qualifier));
    }
    Ok(())
}

fn normalize_projection(
    projection: &[SelectItem],
    scope: &Scope,
) -> Result<(Vec<SelectItem>, QueryShape), SqlError> {
    let mut normalized = Vec::new();
    let mut columns = Vec::new();
    let mut aliases = BTreeMap::new();

    for item in projection {
        match item {
            SelectItem::Wildcard(options) if wildcard_options_empty(options) => {
                for (qualifier, binding) in &scope.relations {
                    for column in &binding.columns {
                        let expr = qualified_column(qualifier, &column.name);
                        normalized.push(SelectItem::UnnamedExpr(expr));
                        columns.push(column.clone());
                    }
                }
            }
            SelectItem::QualifiedWildcard(name, options) if wildcard_options_empty(options) => {
                let qualifier = object_name(name);
                let binding = scope
                    .relations
                    .get(&qualifier)
                    .ok_or_else(|| SqlError::UnknownTable(qualifier.clone()))?;
                for column in &binding.columns {
                    let expr = qualified_column(&qualifier, &column.name);
                    normalized.push(SelectItem::UnnamedExpr(expr));
                    columns.push(column.clone());
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                let expr = normalize_expr(expr, scope, &BTreeMap::new())?;
                let ty = infer_expr_type(&expr, scope)?;
                let name = output_name(&expr);
                normalized.push(SelectItem::UnnamedExpr(expr));
                columns.push(ColumnSchema::new(name, ty));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = normalize_expr(expr, scope, &BTreeMap::new())?;
                let ty = infer_expr_type(&expr, scope)?;
                aliases.insert(alias.value.clone(), expr.clone());
                normalized.push(SelectItem::ExprWithAlias {
                    expr,
                    alias: alias.clone(),
                });
                columns.push(ColumnSchema::new(alias.value.clone(), ty));
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(SqlError::UnsupportedFeature("wildcard options"));
            }
        }
    }

    Ok((normalized, QueryShape { columns, aliases }))
}

fn normalize_expr(
    expr: &Expr,
    scope: &Scope,
    aliases: &BTreeMap<String, Expr>,
) -> Result<Expr, SqlError> {
    match expr {
        Expr::Identifier(identifier) => {
            if let Some(alias) = aliases.get(&identifier.value) {
                return Ok(alias.clone());
            }
            resolve_column(scope, None, &identifier.value)
        }
        Expr::CompoundIdentifier(parts) => {
            let [relation, column] = parts.as_slice() else {
                return Err(SqlError::UnsupportedFeature(
                    "multi-part column references beyond relation.column",
                ));
            };
            resolve_column(scope, Some(&relation.value), &column.value)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = normalize_expr(expr, scope, aliases)?;
            let low = normalize_expr(low, scope, aliases)?;
            let high = normalize_expr(high, scope, aliases)?;
            let range = and(
                binary(value.clone(), BinaryOperator::GtEq, low),
                binary(value, BinaryOperator::LtEq, high),
            );
            Ok(if *negated { not(range) } else { range })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if list.is_empty() {
                return Err(SqlError::UnsupportedFeature("empty IN list"));
            }
            let value = normalize_expr(expr, scope, aliases)?;
            let op = if *negated {
                BinaryOperator::NotEq
            } else {
                BinaryOperator::Eq
            };
            let join = if *negated { and } else { or };
            let mut parts = list
                .iter()
                .map(|item| normalize_expr(item, scope, aliases))
                .map(|item| item.map(|item| binary(value.clone(), op.clone(), item)));
            let first = parts.next().expect("empty list rejected above")?;
            parts.try_fold(first, |left, right| right.map(|right| join(left, right)))
        }
        Expr::IsNull(inner) => Ok(binary(
            normalize_expr(inner, scope, aliases)?,
            BinaryOperator::Eq,
            Expr::Value(Value::Null),
        )),
        Expr::IsNotNull(inner) => Ok(binary(
            normalize_expr(inner, scope, aliases)?,
            BinaryOperator::NotEq,
            Expr::Value(Value::Null),
        )),
        Expr::BinaryOp { left, op, right } => {
            let left = normalize_expr(left, scope, aliases)?;
            let right = normalize_expr(right, scope, aliases)?;
            validate_binary_types(&left, op, &right, scope)?;
            Ok(binary(left, op.clone(), right))
        }
        Expr::UnaryOp { op, expr } => {
            let expr = normalize_expr(expr, scope, aliases)?;
            if *op == UnaryOperator::Not {
                let ty = infer_expr_type(&expr, scope)?;
                if !matches!(ty, ColumnType::Bool | ColumnType::Unknown) {
                    return Err(SqlError::TypeMismatch(format!(
                        "NOT expects boolean input, got {ty:?}"
                    )));
                }
            }
            Ok(Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(expr),
            })
        }
        Expr::Nested(inner) => normalize_expr(inner, scope, aliases),
        Expr::Value(_) | Expr::Function(_) => Ok(expr.clone()),
        _ => Ok(expr.clone()),
    }
}

fn normalize_output_expr(expr: &Expr, shape: &QueryShape) -> Result<Expr, SqlError> {
    match expr {
        Expr::Identifier(identifier) => {
            if let Some(alias) = shape.aliases.get(&identifier.value) {
                return Ok(alias.clone());
            }
            if shape
                .columns
                .iter()
                .any(|column| column.name == identifier.value)
            {
                return Ok(expr.clone());
            }
            Err(SqlError::UnknownColumn(identifier.value.clone()))
        }
        _ => Ok(expr.clone()),
    }
}

fn resolve_column(scope: &Scope, qualifier: Option<&str>, column: &str) -> Result<Expr, SqlError> {
    if let Some(qualifier) = qualifier {
        let binding = scope
            .relations
            .get(qualifier)
            .ok_or_else(|| SqlError::UnknownTable(qualifier.to_owned()))?;
        if binding
            .columns
            .iter()
            .any(|candidate| candidate.name == column)
        {
            return Ok(qualified_column(qualifier, column));
        }
        return Err(SqlError::UnknownColumn(format!("{qualifier}.{column}")));
    }

    let mut matches = scope.relations.iter().filter(|(_, binding)| {
        binding
            .columns
            .iter()
            .any(|candidate| candidate.name == column)
    });
    let Some((qualifier, _)) = matches.next() else {
        return Err(SqlError::UnknownColumn(column.to_owned()));
    };
    if matches.next().is_some() {
        return Err(SqlError::AmbiguousColumn(column.to_owned()));
    }

    Ok(qualified_column(qualifier, column))
}

fn infer_expr_type(expr: &Expr, scope: &Scope) -> Result<ColumnType, SqlError> {
    match expr {
        Expr::Value(Value::Boolean(_)) => Ok(ColumnType::Bool),
        Expr::Value(Value::Number(_, _)) => Ok(ColumnType::Int),
        Expr::Value(Value::SingleQuotedString(_))
        | Expr::Value(Value::EscapedStringLiteral(_))
        | Expr::Value(Value::UnicodeStringLiteral(_))
        | Expr::Value(Value::NationalStringLiteral(_))
        | Expr::Value(Value::DoubleQuotedString(_)) => Ok(ColumnType::Text),
        Expr::Value(Value::Null) => Ok(ColumnType::Unknown),
        Expr::Identifier(identifier) => column_type(scope, None, &identifier.value),
        Expr::CompoundIdentifier(parts) => {
            let [relation, column] = parts.as_slice() else {
                return Ok(ColumnType::Unknown);
            };
            column_type(scope, Some(&relation.value), &column.value)
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::And
            | BinaryOperator::Or => Ok(ColumnType::Bool),
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let left = infer_expr_type(left, scope)?;
                let right = infer_expr_type(right, scope)?;
                if left == ColumnType::Float || right == ColumnType::Float {
                    Ok(ColumnType::Float)
                } else {
                    Ok(ColumnType::Int)
                }
            }
            _ => Ok(ColumnType::Unknown),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => Ok(ColumnType::Bool),
        Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_lowercase();
            if matches!(name.as_str(), "count") {
                Ok(ColumnType::Int)
            } else if matches!(name.as_str(), "sum" | "min" | "max" | "avg") {
                Ok(ColumnType::Unknown)
            } else {
                Ok(ColumnType::Unknown)
            }
        }
        _ => Ok(ColumnType::Unknown),
    }
}

fn column_type(
    scope: &Scope,
    qualifier: Option<&str>,
    column: &str,
) -> Result<ColumnType, SqlError> {
    if let Some(qualifier) = qualifier {
        let binding = scope
            .relations
            .get(qualifier)
            .ok_or_else(|| SqlError::UnknownTable(qualifier.to_owned()))?;
        return binding
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
            .map(|column| column.ty)
            .ok_or_else(|| SqlError::UnknownColumn(format!("{qualifier}.{column}")));
    }

    let mut matches = scope.relations.values().filter_map(|binding| {
        binding
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
            .map(|column| column.ty)
    });
    let Some(ty) = matches.next() else {
        return Err(SqlError::UnknownColumn(column.to_owned()));
    };
    if matches.next().is_some() {
        return Err(SqlError::AmbiguousColumn(column.to_owned()));
    }
    Ok(ty)
}

fn validate_binary_types(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    scope: &Scope,
) -> Result<(), SqlError> {
    let left_ty = infer_expr_type(left, scope)?;
    let right_ty = infer_expr_type(right, scope)?;

    match op {
        BinaryOperator::And | BinaryOperator::Or => {
            if matches!(left_ty, ColumnType::Bool | ColumnType::Unknown)
                && matches!(right_ty, ColumnType::Bool | ColumnType::Unknown)
            {
                Ok(())
            } else {
                Err(SqlError::TypeMismatch(format!(
                    "{op:?} expects boolean inputs, got {left_ty:?} and {right_ty:?}"
                )))
            }
        }
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => {
            if left_ty.is_numeric() && right_ty.is_numeric() {
                Ok(())
            } else {
                Err(SqlError::TypeMismatch(format!(
                    "{op:?} expects numeric inputs, got {left_ty:?} and {right_ty:?}"
                )))
            }
        }
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq => {
            if left_ty.is_compatible_with(right_ty) {
                Ok(())
            } else {
                Err(SqlError::TypeMismatch(format!(
                    "{op:?} compares incompatible inputs {left_ty:?} and {right_ty:?}"
                )))
            }
        }
        _ => Ok(()),
    }
}

fn validate_equi_join(expr: &Expr) -> Result<(), SqlError> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if matches!(
            left.as_ref(),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_)
        ) && matches!(
            right.as_ref(),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_)
        ) =>
        {
            Ok(())
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            validate_equi_join(left)?;
            validate_equi_join(right)
        }
        _ => Err(SqlError::UnsupportedFeature("theta joins")),
    }
}

fn validate_set_shapes(left: &QueryShape, right: &QueryShape) -> Result<(), SqlError> {
    if left.columns.len() != right.columns.len() {
        return Err(SqlError::TypeMismatch(format!(
            "set operation column count mismatch: {} vs {}",
            left.columns.len(),
            right.columns.len()
        )));
    }
    for (left, right) in left.columns.iter().zip(&right.columns) {
        if !left.ty.is_compatible_with(right.ty) {
            return Err(SqlError::TypeMismatch(format!(
                "set operation column type mismatch: {} is {:?}, right side is {:?}",
                left.name, left.ty, right.ty
            )));
        }
    }
    Ok(())
}

fn wildcard_options_empty(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
}

fn output_name(expr: &Expr) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map_or_else(|| expr.to_string(), |part| part.value.clone()),
        Expr::Identifier(identifier) => identifier.value.clone(),
        _ => expr.to_string(),
    }
}

fn catalog_table_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map_or_else(|| name.to_string(), |part| part.value.clone())
}

fn object_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map_or_else(|| name.to_string(), |part| part.value.clone())
}

fn qualified_column(qualifier: &str, column: &str) -> Expr {
    Expr::CompoundIdentifier(vec![Ident::new(qualifier), Ident::new(column)])
}

fn binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn and(left: Expr, right: Expr) -> Expr {
    binary(left, BinaryOperator::And, right)
}

fn or(left: Expr, right: Expr) -> Expr {
    binary(left, BinaryOperator::Or, right)
}

fn not(expr: Expr) -> Expr {
    Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(expr),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;

    use super::parse_and_normalize;
    use crate::Catalog;

    #[test]
    fn expands_wildcard_and_resolves_columns() {
        let normalized = parse_and_normalize("SELECT * FROM posts", &Catalog::demo())
            .expect("demo catalog contains posts");

        let Statement::Query(query) = normalized else {
            panic!("expected query");
        };

        assert_eq!(
            query.to_string(),
            "SELECT posts.id, posts.author_id, posts.created_at, posts.title, posts.published FROM posts"
        );
    }

    #[test]
    fn propagates_projection_aliases_to_order_by() {
        let normalized = parse_and_normalize(
            "SELECT created_at AS published_at FROM posts ORDER BY published_at LIMIT 10",
            &Catalog::demo(),
        )
        .expect("alias should normalize");

        assert_eq!(
            normalized.to_string(),
            "SELECT posts.created_at AS published_at FROM posts ORDER BY posts.created_at LIMIT 10"
        );
    }

    #[test]
    fn desugars_predicate_forms() {
        let normalized = parse_and_normalize(
            "SELECT id FROM posts WHERE author_id IN (1, 2) AND created_at IS NOT NULL",
            &Catalog::demo(),
        )
        .expect("predicate should normalize");

        assert!(normalized
            .to_string()
            .contains("posts.author_id = 1 OR posts.author_id = 2"));
        assert!(normalized.to_string().contains("posts.created_at <> NULL"));
    }

    #[test]
    fn rejects_unknown_columns() {
        let err = parse_and_normalize("SELECT missing FROM posts", &Catalog::demo())
            .expect_err("missing column should reject");

        assert!(err.to_string().contains("unknown column"));
    }

    #[test]
    fn rejects_type_mismatches() {
        let err = parse_and_normalize("SELECT id FROM posts WHERE title = 1", &Catalog::demo())
            .expect_err("text and integer comparison should reject");

        assert!(err.to_string().contains("type mismatch"));
    }
}
