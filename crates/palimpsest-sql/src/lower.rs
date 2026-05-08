// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Distinct, DuplicateTreatment, Expr, Function, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Join, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, TableWithJoins, Value,
};

use crate::{
    mir::{AggExpr, ColumnRef, JoinKind, MirGraph, MirNodeKind, OrderKey, SetQuantifierKind},
    parse_select, SqlError,
};

pub fn parse_and_lower(sql: &str) -> Result<MirGraph, SqlError> {
    let statement = parse_select(sql)?;
    lower_select_statement(&statement)
}

pub fn lower_select_statement(statement: &Statement) -> Result<MirGraph, SqlError> {
    let Statement::Query(query) = statement else {
        return Err(SqlError::UnsupportedStatement);
    };

    lower_query(query)
}

fn lower_query(query: &Query) -> Result<MirGraph, SqlError> {
    let mut context = LowerContext::default();

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let graph = lower_query_with_context(&cte.query, &context)?;
            context.ctes.insert(cte.alias.name.value.clone(), graph);
        }
    }

    lower_query_with_context(query, &context)
}

fn lower_query_with_context(query: &Query, context: &LowerContext) -> Result<MirGraph, SqlError> {
    let mut graph = lower_set_expr(&query.body, context)?;

    if let Some(order_by) = &query.order_by {
        let limit = query
            .limit
            .as_ref()
            .ok_or(SqlError::UnsupportedFeature("ORDER BY without LIMIT"))
            .and_then(literal_usize)?;
        let offset = query
            .offset
            .as_ref()
            .map(|offset| literal_usize(&offset.value))
            .transpose()?
            .unwrap_or(0);
        let order_by = order_by
            .exprs
            .iter()
            .map(|expr| OrderKey {
                expression: expr.expr.to_string(),
                descending: expr.asc == Some(false),
            })
            .collect();
        push_unary(
            &mut graph,
            MirNodeKind::TopK {
                order_by,
                limit,
                offset,
            },
        );
    }

    Ok(graph)
}

#[derive(Debug, Default)]
struct LowerContext {
    ctes: HashMap<String, MirGraph>,
}

fn lower_set_expr(expr: &SetExpr, context: &LowerContext) -> Result<MirGraph, SqlError> {
    match expr {
        SetExpr::Select(select) => lower_select_body(select, context),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => lower_set_operation(*op, *set_quantifier, left, right, context),
        SetExpr::Query(query) => lower_query_with_context(query, context),
        SetExpr::Values(_) => Err(SqlError::UnsupportedFeature("VALUES queries")),
        SetExpr::Insert(_) => Err(SqlError::UnsupportedFeature("INSERT in query body")),
        SetExpr::Update(_) => Err(SqlError::UnsupportedFeature("UPDATE in query body")),
        SetExpr::Table(_) => Err(SqlError::UnsupportedFeature("TABLE queries")),
    }
}

fn lower_set_operation(
    op: SetOperator,
    set_quantifier: SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
    context: &LowerContext,
) -> Result<MirGraph, SqlError> {
    let quantifier = lower_set_quantifier(set_quantifier)?;
    let mut graph = lower_set_expr(left, context)?;
    let left_root = graph.root();
    let right = lower_set_expr(right, context)?;
    let right_root = graph.append_graph(&right);

    let set_op = graph.add_node(match op {
        SetOperator::Union => MirNodeKind::Union { quantifier },
        SetOperator::Except => MirNodeKind::Except { quantifier },
        SetOperator::Intersect => MirNodeKind::Intersect { quantifier },
    });
    graph.add_input(left_root, set_op);
    graph.add_input(right_root, set_op);
    graph.set_root(set_op);
    Ok(graph)
}

const fn lower_set_quantifier(quantifier: SetQuantifier) -> Result<SetQuantifierKind, SqlError> {
    match quantifier {
        SetQuantifier::All => Ok(SetQuantifierKind::All),
        SetQuantifier::None | SetQuantifier::Distinct => Ok(SetQuantifierKind::Distinct),
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName => {
            Err(SqlError::UnsupportedFeature("set operations BY NAME"))
        }
    }
}

fn lower_select_body(select: &Select, context: &LowerContext) -> Result<MirGraph, SqlError> {
    reject_select_features_not_lowered(select)?;

    let mut graph = lower_from(select, context)?;

    if let Some(predicate) = &select.selection {
        push_unary(
            &mut graph,
            MirNodeKind::Filter {
                predicate: canonical_predicate(predicate),
            },
        );
    }

    let group_by = group_by_columns(&select.group_by)?;
    let aggs = aggregate_exprs(&select.projection)?;
    if !group_by.is_empty() || !aggs.is_empty() {
        push_unary(&mut graph, MirNodeKind::Aggregate { group_by, aggs });
    }

    push_unary(
        &mut graph,
        MirNodeKind::Project {
            columns: select.projection.iter().map(select_item_name).collect(),
        },
    );

    if matches!(select.distinct, Some(Distinct::Distinct)) {
        push_unary(&mut graph, MirNodeKind::Distinct);
    }

    Ok(graph)
}

fn reject_select_features_not_lowered(select: &Select) -> Result<(), SqlError> {
    if select.having.is_some() {
        return Err(SqlError::UnsupportedFeature("HAVING"));
    }
    if has_group_by_modifiers(&select.group_by) {
        return Err(SqlError::UnsupportedFeature("GROUP BY modifiers"));
    }
    if select.distinct.is_some() && !matches!(select.distinct, Some(Distinct::Distinct)) {
        return Err(SqlError::UnsupportedFeature("DISTINCT ON"));
    }
    if select.top.is_some() {
        return Err(SqlError::UnsupportedFeature("TOP"));
    }
    if select.into.is_some() {
        return Err(SqlError::UnsupportedFeature("SELECT INTO"));
    }
    if !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return Err(SqlError::UnsupportedFeature("non-standard SELECT clauses"));
    }

    Ok(())
}

fn lower_from(select: &Select, context: &LowerContext) -> Result<MirGraph, SqlError> {
    let [source] = select.from.as_slice() else {
        return Err(SqlError::UnsupportedFeature(
            "MIR lowering for zero or multiple FROM items",
        ));
    };

    lower_table_with_joins(source, context)
}

fn lower_table_with_joins(
    source: &TableWithJoins,
    context: &LowerContext,
) -> Result<MirGraph, SqlError> {
    let mut graph = lower_table_factor(&source.relation, context)?;

    for join in &source.joins {
        lower_join(&mut graph, join, context)?;
    }

    Ok(graph)
}

fn lower_join(graph: &mut MirGraph, join: &Join, context: &LowerContext) -> Result<(), SqlError> {
    let right_graph = lower_table_factor(&join.relation, context)?;
    let right = graph.append_graph(&right_graph);

    let (kind, on) = match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(predicate)) => {
            (JoinKind::Inner, equi_join_columns(predicate)?)
        }
        JoinOperator::LeftOuter(JoinConstraint::On(predicate)) => {
            (JoinKind::Left, equi_join_columns(predicate)?)
        }
        JoinOperator::Inner(
            JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None,
        )
        | JoinOperator::LeftOuter(
            JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None,
        ) => {
            return Err(SqlError::UnsupportedFeature(
                "MIR lowering for non-ON joins",
            ));
        }
        JoinOperator::CrossJoin => {
            return Err(SqlError::UnsupportedFeature("MIR lowering for cross joins"));
        }
        _ => return Err(SqlError::UnsupportedFeature("non-standard joins")),
    };

    let left = graph.root();
    let join = graph.add_node(MirNodeKind::Join { kind, on });
    graph.add_input(left, join);
    graph.add_input(right, join);
    graph.set_root(join);
    Ok(())
}

fn lower_table_factor(table: &TableFactor, context: &LowerContext) -> Result<MirGraph, SqlError> {
    match table {
        TableFactor::Table { name, .. } => {
            let name = name.to_string();
            if let Some(cte) = context.ctes.get(&name) {
                let mut graph = MirGraph::new(MirNodeKind::CteRef { cte: name });
                let cte_root = graph.append_graph(cte);
                graph.add_cte_expansion(cte_root, graph.root());
                Ok(graph)
            } else {
                Ok(MirGraph::new(MirNodeKind::BaseTable {
                    table: name,
                    project: Vec::new(),
                }))
            }
        }
        TableFactor::Derived {
            lateral: false,
            subquery,
            ..
        } => lower_query_with_context(subquery, context),
        TableFactor::Derived { lateral: true, .. } => {
            Err(SqlError::UnsupportedFeature("LATERAL derived tables"))
        }
        _ => Err(SqlError::UnsupportedFeature(
            "table functions or special table factors",
        )),
    }
}

fn equi_join_columns(predicate: &Expr) -> Result<Vec<(ColumnRef, ColumnRef)>, SqlError> {
    match predicate {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            Ok(vec![(column_ref(left)?, column_ref(right)?)])
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut pairs = equi_join_columns(left)?;
            pairs.extend(equi_join_columns(right)?);
            Ok(pairs)
        }
        _ => Err(SqlError::UnsupportedFeature("theta joins")),
    }
}

fn column_ref(expr: &Expr) -> Result<ColumnRef, SqlError> {
    match expr {
        Expr::Identifier(ident) => Ok(ColumnRef {
            relation: None,
            name: ident.value.clone(),
        }),
        Expr::CompoundIdentifier(parts) => {
            let [relation, name] = parts.as_slice() else {
                return Err(SqlError::UnsupportedFeature(
                    "multi-part column references beyond relation.column",
                ));
            };

            Ok(ColumnRef {
                relation: Some(relation.value.clone()),
                name: name.value.clone(),
            })
        }
        _ => Err(SqlError::UnsupportedFeature("non-column join keys")),
    }
}

fn canonical_predicate(expr: &Expr) -> String {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut parts = flatten_and(left);
            parts.extend(flatten_and(right));
            parts.sort();
            parts.join(" AND ")
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            let mut operands = [
                (operand_sort_key(left), canonical_expr(left)),
                (operand_sort_key(right), canonical_expr(right)),
            ];
            operands.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
            format!("{} = {}", operands[0].1, operands[1].1)
        }
        Expr::BinaryOp { left, op, right } => {
            format!("{} {op} {}", canonical_expr(left), canonical_expr(right))
        }
        _ => canonical_expr(expr),
    }
}

fn operand_sort_key(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => format!("0:{expr}"),
        _ => format!("1:{}", canonical_expr(expr)),
    }
}

fn canonical_expr(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => canonical_value(value),
        Expr::UnaryOp { op, expr } => format!("{op} {}", canonical_expr(expr)),
        Expr::Nested(expr) => canonical_expr(expr),
        Expr::BinaryOp { left, op, right } => {
            format!("{} {op} {}", canonical_expr(left), canonical_expr(right))
        }
        _ => expr.to_string(),
    }
}

fn canonical_value(value: &Value) -> String {
    match value {
        Value::Number(value, false) => canonical_number(value),
        Value::SingleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::UnicodeStringLiteral(value)
        | Value::NationalStringLiteral(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Boolean(value) => value.to_string(),
        Value::Null => "NULL".to_owned(),
        _ => value.to_string(),
    }
}

fn canonical_number(value: &str) -> String {
    let value = value.trim_start_matches('+');
    if value.contains(['.', 'e', 'E']) {
        return value.to_ascii_lowercase();
    }

    let negative = value.starts_with('-');
    let digits = if negative { &value[1..] } else { value };
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    if negative && digits != "0" {
        format!("-{digits}")
    } else {
        digits.to_owned()
    }
}

fn flatten_and(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut parts = flatten_and(left);
            parts.extend(flatten_and(right));
            parts
        }
        _ => vec![canonical_predicate(expr)],
    }
}

fn group_by_columns(group_by: &GroupByExpr) -> Result<Vec<ColumnRef>, SqlError> {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => {
            expressions.iter().map(column_ref).collect()
        }
        GroupByExpr::Expressions(_, _) => Err(SqlError::UnsupportedFeature("GROUP BY modifiers")),
        GroupByExpr::All(_) => Err(SqlError::UnsupportedFeature("GROUP BY ALL")),
    }
}

fn aggregate_exprs(projection: &[SelectItem]) -> Result<Vec<AggExpr>, SqlError> {
    projection.iter().try_fold(Vec::new(), |mut aggs, item| {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(function)) => {
                if let Some(agg) = aggregate_expr(function, None)? {
                    aggs.push(agg);
                }
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Function(function),
                alias,
            } => {
                if let Some(agg) = aggregate_expr(function, Some(alias.value.clone()))? {
                    aggs.push(agg);
                }
            }
            SelectItem::UnnamedExpr(_)
            | SelectItem::ExprWithAlias { .. }
            | SelectItem::QualifiedWildcard(_, _)
            | SelectItem::Wildcard(_) => {}
        }

        Ok(aggs)
    })
}

fn aggregate_expr(function: &Function, alias: Option<String>) -> Result<Option<AggExpr>, SqlError> {
    let name = function.name.to_string().to_ascii_lowercase();
    if !matches!(name.as_str(), "count" | "sum" | "min" | "max" | "avg") {
        return Ok(None);
    }

    let mut args = function_args(&function.args)?;
    if matches!(
        function.args,
        FunctionArguments::List(ref args)
            if args.duplicate_treatment == Some(DuplicateTreatment::Distinct)
    ) {
        args.insert(0, "DISTINCT".to_owned());
    }

    Ok(Some(AggExpr {
        function: name,
        args,
        alias,
    }))
}

fn function_args(args: &FunctionArguments) -> Result<Vec<String>, SqlError> {
    match args {
        FunctionArguments::None => Ok(Vec::new()),
        FunctionArguments::Subquery(_) => Err(SqlError::UnsupportedFeature(
            "subqueries in aggregate arguments",
        )),
        FunctionArguments::List(args) => args
            .args
            .iter()
            .map(|arg| match arg {
                sqlparser::ast::FunctionArg::Named { .. } => {
                    Err(SqlError::UnsupportedFeature("named aggregate arguments"))
                }
                sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    Ok(expr.to_string())
                }
                sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name)) => {
                    Ok(format!("{name}.*"))
                }
                sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                    Ok("*".to_owned())
                }
            })
            .collect(),
    }
}

fn select_item_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => expr.to_string(),
        SelectItem::ExprWithAlias { alias, .. } => alias.to_string(),
        SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
        SelectItem::Wildcard(_) => "*".to_owned(),
    }
}

fn literal_usize(expr: &Expr) -> Result<usize, SqlError> {
    match expr {
        Expr::Value(Value::Number(value, false)) => value
            .parse()
            .map_err(|_| SqlError::UnsupportedFeature("non-integer LIMIT/OFFSET")),
        _ => Err(SqlError::UnsupportedFeature("non-literal LIMIT/OFFSET")),
    }
}

fn push_unary(graph: &mut MirGraph, node: MirNodeKind) {
    let previous_root = graph.root();
    let next_root = graph.add_node(node);
    graph.add_input(previous_root, next_root);
    graph.set_root(next_root);
}

fn has_group_by_modifiers(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::Expressions(_, modifiers) | GroupByExpr::All(modifiers) => {
            !modifiers.is_empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        lower::parse_and_lower,
        mir::{
            AggExpr, ColumnRef, JoinKind, MirEdgeKind, MirNodeKind, OrderKey, SetQuantifierKind,
        },
    };

    #[test]
    fn lowers_filter_project_distinct_topk_chain() {
        let graph = parse_and_lower(
            "SELECT DISTINCT id, title AS post_title
             FROM posts
             WHERE author_id = 42
             ORDER BY created_at DESC
             LIMIT 5 OFFSET 10",
        )
        .expect("supported query should lower");

        assert_eq!(graph.node_count(), 5);
        assert!(matches!(
            graph.root_kind(),
            MirNodeKind::TopK {
                order_by,
                limit: 5,
                offset: 10,
            } if order_by == &vec![OrderKey {
                expression: "created_at".to_owned(),
                descending: true,
            }]
        ));
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::BaseTable { table, .. } if table == "posts"
        )));
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate } if predicate == "author_id = 42"
        )));
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Project { columns } if columns == &vec!["id".to_owned(), "post_title".to_owned()]
        )));
        assert!(graph
            .node_kinds()
            .any(|node| matches!(node, MirNodeKind::Distinct)));
    }

    #[test]
    fn lowers_equi_join() {
        let graph = parse_and_lower(
            "SELECT posts.id
             FROM posts JOIN authors ON posts.author_id = authors.id",
        )
        .expect("validated equi-join should lower");

        assert_eq!(graph.node_count(), 4);
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Join {
                kind: JoinKind::Inner,
                on,
            } if on == &vec![(
                ColumnRef {
                    relation: Some("posts".to_owned()),
                    name: "author_id".to_owned(),
                },
                ColumnRef {
                    relation: Some("authors".to_owned()),
                    name: "id".to_owned(),
                },
            )]
        )));
    }

    #[test]
    fn lowers_left_equi_join_with_conjunction() {
        let graph = parse_and_lower(
            "SELECT posts.id
             FROM posts LEFT JOIN comments
               ON posts.id = comments.post_id AND posts.author_id = comments.author_id",
        )
        .expect("validated left equi-join should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Join {
                kind: JoinKind::Left,
                on,
            } if on.len() == 2
        )));
    }

    #[test]
    fn lowers_group_by_aggregate() {
        let graph = parse_and_lower(
            "SELECT author_id, count(*) AS post_count, max(created_at)
             FROM posts
             WHERE author_id = 42
             GROUP BY author_id",
        )
        .expect("basic aggregate query should lower");

        assert_eq!(graph.node_count(), 4);
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Aggregate { group_by, aggs }
                if group_by == &vec![ColumnRef {
                    relation: None,
                    name: "author_id".to_owned(),
                }]
                    && aggs == &vec![
                        AggExpr {
                            function: "count".to_owned(),
                            args: vec!["*".to_owned()],
                            alias: Some("post_count".to_owned()),
                        },
                        AggExpr {
                            function: "max".to_owned(),
                            args: vec!["created_at".to_owned()],
                            alias: None,
                        },
                    ]
        )));
    }

    #[test]
    fn lowers_scalar_aggregate() {
        let graph = parse_and_lower("SELECT count(*) FROM posts")
            .expect("scalar aggregate query should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Aggregate { group_by, aggs }
                if group_by.is_empty() && aggs.len() == 1
        )));
    }

    #[test]
    fn lowers_union_all() {
        let graph = parse_and_lower(
            "SELECT id FROM posts
             UNION ALL
             SELECT id FROM archived_posts",
        )
        .expect("UNION ALL should lower");

        assert_eq!(graph.node_count(), 5);
        assert!(matches!(
            graph.root_kind(),
            MirNodeKind::Union {
                quantifier: SetQuantifierKind::All,
            }
        ));
        assert_eq!(
            graph
                .node_kinds()
                .filter(|node| matches!(node, MirNodeKind::BaseTable { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn lowers_distinct_union() {
        let graph = parse_and_lower(
            "SELECT id FROM posts
             UNION
             SELECT id FROM archived_posts",
        )
        .expect("UNION DISTINCT should lower");

        assert!(matches!(
            graph.root_kind(),
            MirNodeKind::Union {
                quantifier: SetQuantifierKind::Distinct,
            }
        ));
    }

    #[test]
    fn lowers_except_and_intersect() {
        let except = parse_and_lower(
            "SELECT id FROM posts
             EXCEPT
             SELECT id FROM archived_posts",
        )
        .expect("EXCEPT should lower");
        let intersect = parse_and_lower(
            "SELECT id FROM posts
             INTERSECT ALL
             SELECT id FROM archived_posts",
        )
        .expect("INTERSECT ALL should lower");

        assert!(matches!(
            except.root_kind(),
            MirNodeKind::Except {
                quantifier: SetQuantifierKind::Distinct,
            }
        ));
        assert!(matches!(
            intersect.root_kind(),
            MirNodeKind::Intersect {
                quantifier: SetQuantifierKind::All,
            }
        ));
    }

    #[test]
    fn lowers_cte_reference() {
        let graph = parse_and_lower(
            "WITH recent_posts AS (
                SELECT id, author_id FROM posts WHERE author_id = 42
             )
             SELECT id FROM recent_posts",
        )
        .expect("non-recursive CTE should lower");

        assert_eq!(graph.node_count(), 5);
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::CteRef { cte } if cte == "recent_posts"
        )));
        assert!(graph
            .graph()
            .edge_weights()
            .any(|edge| *edge == MirEdgeKind::CteExpansion));
    }

    #[test]
    fn lowers_derived_table() {
        let graph = parse_and_lower(
            "SELECT id
             FROM (
                SELECT id FROM posts WHERE author_id = 42
             ) AS recent_posts",
        )
        .expect("derived table should lower through nested query path");

        assert_eq!(graph.node_count(), 4);
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate } if predicate == "author_id = 42"
        )));
    }
}
