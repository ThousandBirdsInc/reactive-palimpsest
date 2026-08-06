// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Statement normalization (alias removal, catalog binding) used by
//! the canonical-form / dedup paths.

use std::collections::{BTreeMap, BTreeSet};

use sqlparser::ast::{
    BinaryOperator, CastKind, DataType, Distinct, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, Query, Select,
    SelectItem, SetExpr, SetOperator, Statement, TableFactor, TableWithJoins, UnaryOperator, Value,
    WildcardAdditionalOptions,
};

use crate::{
    catalog::{Catalog, ColumnSchema, ColumnType},
    parse_select,
    parser::count_relation_references,
    SqlError,
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
    /// Qualifiers inherited from an enclosing query (correlated
    /// subqueries: `EXISTS`, `LATERAL`). These resolve column
    /// references but are excluded from wildcard expansion, and a
    /// same-named relation bound at this level shadows them.
    outer: BTreeSet<String>,
}

impl Scope {
    /// Builds the starting scope for a (sub)query: every relation
    /// visible in `outer` is available for correlation.
    fn nested(outer: &Self) -> Self {
        Self {
            relations: outer.relations.clone(),
            outer: outer.relations.keys().cloned().collect(),
        }
    }

    /// Relations bound at this query level (excluding purely-outer
    /// ones), in stable order.
    fn local_relations(&self) -> impl Iterator<Item = (&String, &RelationBinding)> {
        self.relations
            .iter()
            .filter(|(qualifier, _)| !self.outer.contains(*qualifier))
    }
}

#[derive(Debug, Clone, Default)]
struct NormalizeContext {
    ctes: BTreeMap<String, QueryShape>,
}

/// Parses `sql` and returns a normalized statement (alias removal,
/// catalog-driven type binding) suitable for stable canonical-form
/// comparisons.
///
/// # Errors
/// Surfaces parse, validation, and normalization errors.
pub fn parse_and_normalize(sql: &str, catalog: &Catalog) -> Result<Statement, SqlError> {
    let statement = parse_select(sql)?;
    normalize_statement(&statement, catalog)
}

/// Normalizes an already-parsed `Statement` against `catalog`.
///
/// # Errors
/// [`SqlError::UnsupportedStatement`] on non-`SELECT` input, or any
/// catalog-validation error.
pub fn normalize_statement(
    statement: &Statement,
    catalog: &Catalog,
) -> Result<Statement, SqlError> {
    let Statement::Query(query) = statement else {
        return Err(SqlError::UnsupportedStatement);
    };

    let mut query = query.clone();
    normalize_query(
        &mut query,
        catalog,
        &NormalizeContext::default(),
        &Scope::default(),
    )?;
    Ok(Statement::Query(query))
}

/// Convenience wrapper: normalizes `statement` against `catalog`
/// purely for the side-effect of catalog validation.
///
/// # Errors
/// As [`normalize_statement`].
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
    outer: &Scope,
) -> Result<QueryShape, SqlError> {
    let mut local_context = context.clone();

    if let Some(with) = &mut query.with {
        let recursive = with.recursive;
        for cte in &mut with.cte_tables {
            let name = cte.alias.name.value.clone();
            let mut cte_query = (*cte.query).clone();

            let shape = if recursive && count_relation_references(&cte_query, &name) > 0 {
                // Recursive CTE: derive the output shape from the base
                // (non-recursive) term, register it so the recursive
                // term can resolve the self-reference, then normalize
                // the whole body.
                let SetExpr::SetOperation {
                    op: SetOperator::Union,
                    left,
                    ..
                } = &*cte_query.body
                else {
                    return Err(SqlError::InvalidQuery(format!(
                        "recursive CTE {name} must have the form \
                         'base term UNION [ALL] recursive term'"
                    )));
                };
                let mut base = (**left).clone();
                let mut shape =
                    normalize_set_expr(&mut base, catalog, &local_context, &Scope::default())?;
                apply_cte_column_aliases(&mut shape, cte)?;
                local_context.ctes.insert(name.clone(), shape.clone());

                normalize_query(&mut cte_query, catalog, &local_context, &Scope::default())?;
                shape
            } else {
                let mut shape =
                    normalize_query(&mut cte_query, catalog, &local_context, &Scope::default())?;
                apply_cte_column_aliases(&mut shape, cte)?;
                shape
            };

            *cte.query = cte_query;
            local_context.ctes.insert(name, shape);
        }
    }

    let shape = normalize_set_expr(&mut query.body, catalog, &local_context, outer)?;

    if let Some(order_by) = &mut query.order_by {
        for order in &mut order_by.exprs {
            order.expr = normalize_output_expr(&order.expr, &shape)?;
        }
    }

    Ok(shape)
}

fn apply_cte_column_aliases(
    shape: &mut QueryShape,
    cte: &sqlparser::ast::Cte,
) -> Result<(), SqlError> {
    if cte.alias.columns.is_empty() {
        return Ok(());
    }
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
    Ok(())
}

fn normalize_set_expr(
    expr: &mut SetExpr,
    catalog: &Catalog,
    context: &NormalizeContext,
    outer: &Scope,
) -> Result<QueryShape, SqlError> {
    match expr {
        SetExpr::Select(select) => normalize_select(select, catalog, context, outer),
        SetExpr::Query(query) => normalize_query(query, catalog, context, outer),
        SetExpr::SetOperation { left, right, .. } => {
            let left_shape = normalize_set_expr(left, catalog, context, outer)?;
            let right_shape = normalize_set_expr(right, catalog, context, outer)?;
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
    outer: &Scope,
) -> Result<QueryShape, SqlError> {
    let mut scope = Scope::nested(outer);

    for table in &mut select.from {
        bind_table_with_joins(table, catalog, context, &mut scope)?;
    }

    if let Some(selection) = &mut select.selection {
        *selection = normalize_expr(selection, &scope, &BTreeMap::new(), catalog, context)?;
        let ty = infer_expr_type(selection, &scope)?;
        if !matches!(ty, ColumnType::Bool | ColumnType::Unknown) {
            return Err(SqlError::TypeMismatch(format!(
                "WHERE expression must be boolean, got {ty:?}"
            )));
        }
    }

    let (projection, shape) = normalize_projection(&select.projection, &scope, catalog, context)?;
    select.projection = projection;

    if let Some(Distinct::On(expressions)) = &mut select.distinct {
        for expression in expressions {
            *expression = normalize_expr(expression, &scope, &shape.aliases, catalog, context)?;
        }
    }

    if let GroupByExpr::Expressions(expressions, _) = &mut select.group_by {
        for expression in expressions {
            *expression = normalize_expr(expression, &scope, &shape.aliases, catalog, context)?;
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
                *predicate = normalize_expr(predicate, scope, &BTreeMap::new(), catalog, context)?;
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
            lateral,
            subquery,
            alias,
        } => {
            // LATERAL subqueries see every relation bound so far in
            // this FROM clause; plain derived tables start fresh.
            let subquery_outer = if *lateral {
                scope.clone()
            } else {
                Scope::default()
            };
            let shape = normalize_query(subquery, catalog, context, &subquery_outer)?;
            let Some(alias) = alias else {
                return Err(SqlError::UnsupportedFeature(
                    "derived tables without aliases",
                ));
            };
            insert_relation(scope, alias.name.value.clone(), shape.columns)
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
    // A relation bound at this level shadows a same-named outer
    // relation instead of colliding with it.
    if scope.outer.remove(&qualifier) {
        scope
            .relations
            .insert(qualifier, RelationBinding { columns });
        return Ok(());
    }
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
    catalog: &Catalog,
    context: &NormalizeContext,
) -> Result<(Vec<SelectItem>, QueryShape), SqlError> {
    let mut normalized = Vec::new();
    let mut columns = Vec::new();
    let mut aliases = BTreeMap::new();

    for item in projection {
        match item {
            SelectItem::Wildcard(options) if wildcard_options_empty(options) => {
                for (qualifier, binding) in scope.local_relations() {
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
                let expr = normalize_expr(expr, scope, &BTreeMap::new(), catalog, context)?;
                let ty = infer_expr_type(&expr, scope)?;
                let name = output_name(&expr);
                normalized.push(SelectItem::UnnamedExpr(expr));
                columns.push(ColumnSchema::new(name, ty));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = normalize_expr(expr, scope, &BTreeMap::new(), catalog, context)?;
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
    catalog: &Catalog,
    context: &NormalizeContext,
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
            let value = normalize_expr(expr, scope, aliases, catalog, context)?;
            let low = normalize_expr(low, scope, aliases, catalog, context)?;
            let high = normalize_expr(high, scope, aliases, catalog, context)?;
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
            let value = normalize_expr(expr, scope, aliases, catalog, context)?;
            let op = if *negated {
                BinaryOperator::NotEq
            } else {
                BinaryOperator::Eq
            };
            let join = if *negated { and } else { or };
            let mut parts = list
                .iter()
                .map(|item| normalize_expr(item, scope, aliases, catalog, context))
                .map(|item| item.map(|item| binary(value.clone(), op.clone(), item)));
            let first = parts.next().expect("empty list rejected above")?;
            let normalized =
                parts.try_fold(first, |left, right| right.map(|right| join(left, right)))?;
            if *negated || list.len() == 1 {
                Ok(normalized)
            } else {
                Ok(Expr::Nested(Box::new(normalized)))
            }
        }
        Expr::IsNull(inner) => Ok(binary(
            normalize_expr(inner, scope, aliases, catalog, context)?,
            BinaryOperator::Eq,
            Expr::Value(Value::Null),
        )),
        Expr::IsNotNull(inner) => Ok(binary(
            normalize_expr(inner, scope, aliases, catalog, context)?,
            BinaryOperator::NotEq,
            Expr::Value(Value::Null),
        )),
        Expr::BinaryOp { left, op, right } => {
            let left = normalize_expr(left, scope, aliases, catalog, context)?;
            let right = normalize_expr(right, scope, aliases, catalog, context)?;
            validate_binary_types(&left, op, &right, scope)?;
            Ok(binary(left, op.clone(), right))
        }
        Expr::UnaryOp { op, expr } => {
            let expr = normalize_expr(expr, scope, aliases, catalog, context)?;
            if *op == UnaryOperator::Not {
                let ty = infer_expr_type(&expr, scope)?;
                if !matches!(ty, ColumnType::Bool | ColumnType::Unknown) {
                    return Err(SqlError::TypeMismatch(format!(
                        "NOT expects boolean input, got {ty:?}"
                    )));
                }
            }
            Ok(Expr::UnaryOp {
                op: *op,
                expr: Box::new(expr),
            })
        }
        Expr::Nested(inner) => normalize_expr(inner, scope, aliases, catalog, context),
        Expr::Exists { subquery, negated } => {
            // Correlated EXISTS: the subquery sees this query's scope
            // as its outer scope.
            let mut subquery = (**subquery).clone();
            normalize_query(&mut subquery, catalog, context, scope)?;
            Ok(Expr::Exists {
                subquery: Box::new(subquery),
                negated: *negated,
            })
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => {
            if !matches!(
                compare_op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                return Err(SqlError::UnsupportedFeature(
                    "ANY with a non-comparison operator",
                ));
            }
            let left = normalize_expr(left, scope, aliases, catalog, context)?;
            let right = normalize_expr(right, scope, aliases, catalog, context)?;
            Ok(Expr::AnyOp {
                left: Box::new(left),
                compare_op: compare_op.clone(),
                right: Box::new(right),
                is_some: *is_some,
            })
        }
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if format.is_some() {
                return Err(SqlError::UnsupportedFeature("CAST with FORMAT"));
            }
            if !matches!(kind, CastKind::Cast | CastKind::DoubleColon) {
                return Err(SqlError::UnsupportedFeature("TRY_CAST / SAFE_CAST"));
            }
            let inner = normalize_expr(expr, scope, aliases, catalog, context)?;
            Ok(Expr::Cast {
                kind: kind.clone(),
                expr: Box::new(inner),
                data_type: data_type.clone(),
                format: None,
            })
        }
        Expr::Function(function) => {
            let mut function = function.clone();
            // Allowlist: every function the frontend accepts must be
            // one the engine evaluates — aggregates are folded by the
            // dataflow's aggregate operator, `coalesce`/`cardinality`
            // by the expression evaluator. Anything else is rejected
            // here, at parse time, with a named error rather than
            // being accepted and silently never evaluated.
            let name = function.name.to_string().to_ascii_lowercase();
            if !matches!(
                name.as_str(),
                "count" | "sum" | "min" | "max" | "avg" | "coalesce" | "cardinality"
            ) {
                return Err(SqlError::UnsupportedFunction { name });
            }
            if let FunctionArguments::List(list) = &mut function.args {
                for arg in &mut list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                        *expr = normalize_expr(expr, scope, aliases, catalog, context)?;
                    }
                }
            }
            if function
                .name
                .to_string()
                .eq_ignore_ascii_case("cardinality")
            {
                let arity = match &function.args {
                    FunctionArguments::List(list) => list.args.len(),
                    FunctionArguments::None => 0,
                    FunctionArguments::Subquery(_) => {
                        return Err(SqlError::UnsupportedFeature(
                            "subqueries as function arguments",
                        ));
                    }
                };
                if arity != 1 {
                    return Err(SqlError::TypeMismatch(format!(
                        "cardinality expects exactly 1 argument, got {arity}"
                    )));
                }
            }
            Ok(Expr::Function(function))
        }
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

    // Relations bound at this level take precedence over outer
    // (correlation) relations, mirroring Postgres scoping rules.
    for local_only in [true, false] {
        let mut matches = scope
            .relations
            .iter()
            .filter(|(qualifier, _)| scope.outer.contains(*qualifier) != local_only)
            .filter(|(_, binding)| {
                binding
                    .columns
                    .iter()
                    .any(|candidate| candidate.name == column)
            });
        let Some((qualifier, _)) = matches.next() else {
            continue;
        };
        if matches.next().is_some() {
            return Err(SqlError::AmbiguousColumn(column.to_owned()));
        }
        return Ok(qualified_column(qualifier, column));
    }

    Err(SqlError::UnknownColumn(column.to_owned()))
}

fn infer_expr_type(expr: &Expr, scope: &Scope) -> Result<ColumnType, SqlError> {
    match expr {
        Expr::Value(Value::Boolean(_))
        | Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => Ok(ColumnType::Bool),
        Expr::Value(Value::Number(_, _)) => Ok(ColumnType::Int),
        Expr::Value(
            Value::SingleQuotedString(_)
            | Value::EscapedStringLiteral(_)
            | Value::UnicodeStringLiteral(_)
            | Value::NationalStringLiteral(_)
            | Value::DoubleQuotedString(_),
        ) => Ok(ColumnType::Text),
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
        Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_lowercase();
            // `cardinality(anyarray)` returns int in Postgres.
            if matches!(name.as_str(), "count" | "cardinality") {
                Ok(ColumnType::Int)
            } else {
                Ok(ColumnType::Unknown)
            }
        }
        Expr::Exists { .. } | Expr::AnyOp { .. } => Ok(ColumnType::Bool),
        Expr::Cast { data_type, .. } => Ok(cast_target_type(data_type)),
        Expr::Nested(inner) => infer_expr_type(inner, scope),
        _ => Ok(ColumnType::Unknown),
    }
}

/// Maps a SQL cast target onto the coarse [`ColumnType`] taxonomy.
/// Targets outside the taxonomy (arrays, json, ...) come back as
/// `Unknown`, which stays compatible with everything.
const fn cast_target_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Int2(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::Int4(_)
        | DataType::Integer(_)
        | DataType::Int8(_)
        | DataType::BigInt(_) => ColumnType::Int,
        DataType::Float(_)
        | DataType::Float4
        | DataType::Float8
        | DataType::Real
        | DataType::Double
        | DataType::DoublePrecision
        | DataType::Numeric(_)
        | DataType::Decimal(_) => ColumnType::Float,
        DataType::Text | DataType::Varchar(_) | DataType::Char(_) | DataType::String(_) => {
            ColumnType::Text
        }
        DataType::Bool | DataType::Boolean => ColumnType::Bool,
        DataType::Timestamp(_, _) | DataType::Date => ColumnType::Timestamp,
        _ => ColumnType::Unknown,
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

    for local_only in [true, false] {
        let mut matches = scope
            .relations
            .iter()
            .filter(|(qualifier, _)| scope.outer.contains(*qualifier) != local_only)
            .filter_map(|(_, binding)| {
                binding
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
                    .map(|column| column.ty)
            });
        let Some(ty) = matches.next() else {
            continue;
        };
        if matches.next().is_some() {
            return Err(SqlError::AmbiguousColumn(column.to_owned()));
        }
        return Ok(ty);
    }
    Err(SqlError::UnknownColumn(column.to_owned()))
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
        // `ON TRUE`, used with correlated LATERAL join sources.
        Expr::Value(Value::Boolean(true)) => Ok(()),
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

const fn wildcard_options_empty(options: &WildcardAdditionalOptions) -> bool {
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
    fn normalizes_correlated_exists() {
        let normalized = parse_and_normalize(
            "SELECT id FROM posts
             WHERE EXISTS (SELECT id FROM comments WHERE post_id = posts.id)",
            &Catalog::demo(),
        )
        .expect("correlated EXISTS should normalize");

        let rendered = normalized.to_string();
        assert!(rendered.contains("EXISTS"));
        // Inner column resolves against the subquery's own relation,
        // the correlated reference against the outer one.
        assert!(rendered.contains("comments.post_id = posts.id"));
    }

    #[test]
    fn rejects_unknown_columns_inside_exists() {
        let err = parse_and_normalize(
            "SELECT id FROM posts
             WHERE EXISTS (SELECT missing FROM comments WHERE post_id = posts.id)",
            &Catalog::demo(),
        )
        .expect_err("unknown column inside EXISTS should reject");

        assert!(err.to_string().contains("unknown column"));
    }

    #[test]
    fn normalizes_lateral_subquery_with_outer_scope() {
        let normalized = parse_and_normalize(
            "SELECT posts.id, c.body
             FROM posts
             JOIN LATERAL (SELECT body FROM comments WHERE post_id = posts.id) AS c ON TRUE",
            &Catalog::demo(),
        )
        .expect("correlated LATERAL should normalize");

        assert!(normalized
            .to_string()
            .contains("comments.post_id = posts.id"));
    }

    #[test]
    fn rejects_outer_reference_in_non_lateral_derived_table() {
        let err = parse_and_normalize(
            "SELECT posts.id, c.body
             FROM posts
             JOIN (SELECT body FROM comments WHERE post_id = posts.id) AS c ON TRUE",
            &Catalog::demo(),
        )
        .expect_err("non-LATERAL derived tables cannot see outer relations");

        assert!(err.to_string().contains("unknown table"));
    }

    #[test]
    fn normalizes_distinct_on_expressions() {
        let normalized = parse_and_normalize(
            "SELECT DISTINCT ON (author_id) id, author_id FROM posts
             ORDER BY author_id",
            &Catalog::demo(),
        )
        .expect("DISTINCT ON should normalize");

        assert!(normalized
            .to_string()
            .contains("DISTINCT ON (posts.author_id)"));
    }

    #[test]
    fn normalizes_recursive_cte_with_column_aliases() {
        let normalized = parse_and_normalize(
            "WITH RECURSIVE nums(n) AS (
                SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10
             )
             SELECT n FROM nums",
            &Catalog::demo(),
        )
        .expect("recursive CTE should normalize with the aliased shape visible to the step");

        assert!(normalized.to_string().contains("nums.n + 1"));
    }

    #[test]
    fn infers_cast_types() {
        parse_and_normalize(
            "SELECT id FROM posts WHERE CAST(id AS TEXT) = title",
            &Catalog::demo(),
        )
        .expect("cast to text compares fine against a text column");

        let err = parse_and_normalize(
            "SELECT id FROM posts WHERE id::text = author_id",
            &Catalog::demo(),
        )
        .expect_err("text cast compared against an integer should reject");

        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn normalizes_any_predicates() {
        let normalized = parse_and_normalize(
            "SELECT id FROM posts WHERE author_id = ANY('{1,2}')",
            &Catalog::demo(),
        )
        .expect("ANY over an array literal should normalize");

        assert!(normalized
            .to_string()
            .contains("posts.author_id = ANY('{1,2}')"));
    }

    #[test]
    fn accepts_coalesce_and_rejects_unlisted_functions() {
        parse_and_normalize(
            "SELECT coalesce(title, 'untitled') FROM posts",
            &Catalog::demo(),
        )
        .expect("coalesce is on the evaluable allowlist");

        let err = parse_and_normalize("SELECT upper(title) FROM posts", &Catalog::demo())
            .expect_err("functions the dataflow cannot evaluate are rejected at parse time");
        assert!(
            matches!(&err, crate::SqlError::UnsupportedFunction { name } if name == "upper"),
            "got {err}"
        );

        let err = parse_and_normalize(
            "SELECT id FROM posts WHERE now() > created_at",
            &Catalog::demo(),
        )
        .expect_err("functions in predicates are rejected too");
        assert!(
            matches!(&err, crate::SqlError::UnsupportedFunction { name } if name == "now"),
            "got {err}"
        );
    }

    #[test]
    fn checks_cardinality_arity_and_type() {
        let err = parse_and_normalize(
            "SELECT cardinality(id, author_id) FROM posts",
            &Catalog::demo(),
        )
        .expect_err("cardinality takes exactly one argument");
        assert!(err.to_string().contains("cardinality"));

        // cardinality() returns int, so it composes with arithmetic.
        parse_and_normalize(
            "SELECT id FROM posts WHERE cardinality(title) + 1 > id",
            &Catalog::demo(),
        )
        .expect("cardinality result should type as an integer");
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
