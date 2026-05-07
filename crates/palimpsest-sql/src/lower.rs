// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use sqlparser::ast::{
    BinaryOperator, Distinct, DuplicateTreatment, Expr, Function, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Join, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, Value,
};

use crate::{
    mir::{AggExpr, ColumnRef, JoinKind, MirGraph, MirNodeKind, OrderKey},
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
    if query.with.is_some() {
        return Err(SqlError::UnsupportedFeature("MIR lowering for CTEs"));
    }

    let mut graph = match query.body.as_ref() {
        SetExpr::Select(select) => lower_select_body(select)?,
        SetExpr::SetOperation { .. } => {
            return Err(SqlError::UnsupportedFeature(
                "MIR lowering for set operations",
            ));
        }
        SetExpr::Query(query) => return lower_query(query),
        SetExpr::Values(_) => return Err(SqlError::UnsupportedFeature("VALUES queries")),
        SetExpr::Insert(_) => return Err(SqlError::UnsupportedFeature("INSERT in query body")),
        SetExpr::Update(_) => return Err(SqlError::UnsupportedFeature("UPDATE in query body")),
        SetExpr::Table(_) => return Err(SqlError::UnsupportedFeature("TABLE queries")),
    };

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

fn lower_select_body(select: &Select) -> Result<MirGraph, SqlError> {
    reject_select_features_not_lowered(select)?;

    let mut graph = lower_from(select)?;

    if let Some(predicate) = &select.selection {
        push_unary(
            &mut graph,
            MirNodeKind::Filter {
                predicate: predicate.to_string(),
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

fn lower_from(select: &Select) -> Result<MirGraph, SqlError> {
    let [source] = select.from.as_slice() else {
        return Err(SqlError::UnsupportedFeature(
            "MIR lowering for zero or multiple FROM items",
        ));
    };

    lower_table_with_joins(source)
}

fn lower_table_with_joins(source: &TableWithJoins) -> Result<MirGraph, SqlError> {
    let table = base_table_name(&source.relation)?;
    let mut graph = MirGraph::new(MirNodeKind::BaseTable {
        table,
        project: Vec::new(),
    });

    for join in &source.joins {
        lower_join(&mut graph, join)?;
    }

    Ok(graph)
}

fn lower_join(graph: &mut MirGraph, join: &Join) -> Result<(), SqlError> {
    let table = base_table_name(&join.relation)?;
    let right = graph.add_node(MirNodeKind::BaseTable {
        table,
        project: Vec::new(),
    });

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

fn base_table_name(table: &TableFactor) -> Result<String, SqlError> {
    match table {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        TableFactor::Derived { .. } => Err(SqlError::UnsupportedFeature(
            "MIR lowering for derived tables",
        )),
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
        mir::{AggExpr, ColumnRef, JoinKind, MirNodeKind, OrderKey},
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
}
