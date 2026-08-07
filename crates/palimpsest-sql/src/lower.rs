// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Lowering from sqlparser AST into [`MirGraph`].

use core::ops::ControlFlow;
use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    BinaryOperator, Cte, Distinct, DuplicateTreatment, Expr, Function, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, Join, JoinConstraint, JoinOperator, Query, Select,
    SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, TableWithJoins, Value,
    Visit, Visitor,
};

use crate::{
    limits::{enforce_graph_size, QueryLimits},
    mir::{AggExpr, ColumnRef, JoinKind, MirGraph, MirNodeKind, OrderKey, SetQuantifierKind},
    parser::count_relation_references,
    SqlError,
};

/// Parses `sql` and lowers it into an [`MirGraph`] under the default
/// [`QueryLimits`].
///
/// # Errors
/// Surfaces parse, validation, and size-bound errors.
pub fn parse_and_lower(sql: &str) -> Result<MirGraph, SqlError> {
    parse_and_lower_with_limits(sql, QueryLimits::DEFAULT)
}

/// Parse + lower while enforcing both `max_input_bytes` and
/// `max_mir_nodes` from `limits`.
///
/// # Errors
/// Surfaces [`SqlError::QueryTooLarge`] / [`SqlError::QueryTooComplex`]
/// in addition to the usual parse/lower errors.
pub fn parse_and_lower_with_limits(sql: &str, limits: QueryLimits) -> Result<MirGraph, SqlError> {
    let statement = crate::parser::parse_select_with_limits(sql, limits)?;
    let graph = lower_select_statement(&statement)?;
    enforce_graph_size(graph.node_count(), limits)?;
    Ok(graph)
}

/// Lowers an already-parsed `SELECT` [`Statement`] into an [`MirGraph`].
///
/// Skips the byte-budget check (the input is no longer textual at this
/// point) but still produces graphs that should be size-checked by the
/// caller via [`enforce_graph_size`].
///
/// # Errors
/// [`SqlError::UnsupportedStatement`] on non-`SELECT` input, plus any
/// downstream lowering error.
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
            let name = cte.alias.name.value.clone();
            let graph =
                if with.recursive && count_relation_references(cte.query.as_ref(), &name) > 0 {
                    lower_recursive_cte(cte, &context)?
                } else {
                    lower_query_with_context(&cte.query, &context)?
                };
            context.ctes.insert(name, graph);
        }
    }

    lower_query_with_context(query, &context)
}

/// Lowers a self-referential CTE from a `WITH RECURSIVE` list into a
/// `Fixpoint` node whose first input is the base term and second input
/// the recursive step; self-references inside the step become
/// [`MirNodeKind::RecursiveRef`] leaves.
fn lower_recursive_cte(cte: &Cte, context: &LowerContext) -> Result<MirGraph, SqlError> {
    let name = cte.alias.name.value.clone();
    let SetExpr::SetOperation {
        op: SetOperator::Union,
        set_quantifier,
        left,
        right,
    } = &*cte.query.body
    else {
        return Err(SqlError::InvalidQuery(format!(
            "recursive CTE {name} must have the form 'base term UNION [ALL] recursive term'"
        )));
    };
    let union_all = lower_set_quantifier(*set_quantifier)? == SetQuantifierKind::All;

    let mut graph = lower_set_expr(left, context)?;

    let mut step_context = LowerContext {
        ctes: context.ctes.clone(),
        recursive: context.recursive.clone(),
    };
    step_context.recursive.insert(name.clone());
    let step = lower_set_expr(right, &step_context)?;

    let base_root = graph.root();
    let step_root = graph.append_graph(&step);
    let fixpoint = graph.add_node(MirNodeKind::Fixpoint {
        cte: name,
        union_all,
    });
    graph.add_input(base_root, fixpoint);
    graph.add_input(step_root, fixpoint);
    graph.set_root(fixpoint);

    apply_order_limit(&mut graph, cte.query.as_ref())?;
    Ok(graph)
}

fn lower_query_with_context(query: &Query, context: &LowerContext) -> Result<MirGraph, SqlError> {
    let mut graph = lower_set_expr(&query.body, context)?;
    apply_order_limit(&mut graph, query)?;
    Ok(graph)
}

/// Applies `query`-level ORDER BY / LIMIT / OFFSET on top of `graph`.
///
/// Also finishes `SELECT DISTINCT ON` lowering: when the graph root is
/// a `DistinctOn` node (pushed by [`lower_select_body`] with no order
/// keys yet), the query's ORDER BY keys are validated against the
/// Postgres prefix rule and copied into the node so executors know
/// which row per group survives.
fn apply_order_limit(graph: &mut MirGraph, query: &Query) -> Result<(), SqlError> {
    let Some(order_by) = &query.order_by else {
        return Ok(());
    };

    // ORDER BY without LIMIT plans as a sort over the whole input —
    // represented in the MIR as `TopK` with `usize::MAX` so we
    // don't need a separate node kind. Downstream operators see
    // "ordered, unbounded" and can pick the right physical plan.
    let limit = query
        .limit
        .as_ref()
        .map(literal_usize)
        .transpose()?
        .unwrap_or(usize::MAX);
    let offset = query
        .offset
        .as_ref()
        .map(|offset| literal_usize(&offset.value))
        .transpose()?
        .unwrap_or(0);
    let order_by: Vec<OrderKey> = order_by
        .exprs
        .iter()
        .map(|expr| OrderKey {
            expression: expr.expr.to_string(),
            descending: expr.asc == Some(false),
        })
        .collect();

    // ORDER BY may reference columns the projection dropped
    // (Postgres allows it). When that happens, extend the projection
    // with hidden sort-key columns and re-project the visible columns
    // above the TopK.
    let visible = extend_projection_with_order_keys(graph, &order_by);

    let root = graph.root();
    if let MirNodeKind::DistinctOn {
        on,
        order_by: distinct_order,
    } = graph.node_kind_mut(root)
    {
        // Postgres: "SELECT DISTINCT ON expressions must match initial
        // ORDER BY expressions" — every leading ORDER BY key (up to
        // the DISTINCT ON arity) must be one of the ON expressions.
        for key in order_by.iter().take(on.len()) {
            if !on.contains(&key.expression) {
                return Err(SqlError::InvalidQuery(
                    "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
                        .to_owned(),
                ));
            }
        }
        distinct_order.clone_from(&order_by);
    }

    push_unary(
        graph,
        MirNodeKind::TopK {
            order_by,
            limit,
            offset,
        },
    );
    if let Some(columns) = visible {
        push_unary(graph, MirNodeKind::Project { columns });
    }
    Ok(())
}

/// When an ORDER BY key is not among the projected columns, appends it
/// to the query's output projection as a hidden column (so downstream
/// sort operators can read it) and returns the visible column names to
/// re-project above the sort. `None` when nothing was hidden — or when
/// the projection's output names cannot be recomputed (wildcards,
/// unaliased expressions), in which case the caller keeps today's
/// behaviour.
fn extend_projection_with_order_keys(
    graph: &mut MirGraph,
    order_by: &[OrderKey],
) -> Option<Vec<String>> {
    let root = graph.root();
    let project_node = match graph.node_kind(root) {
        MirNodeKind::Project { .. } => root,
        // `SELECT DISTINCT ON` sits directly above its projection and
        // passes columns through unchanged.
        MirNodeKind::DistinctOn { .. } => {
            let input = *graph.ordered_inputs(root).first()?;
            matches!(graph.node_kind(input), MirNodeKind::Project { .. }).then_some(input)?
        }
        _ => return None,
    };
    let MirNodeKind::Project { columns } = graph.node_kind(project_node).clone() else {
        return None;
    };

    let provided: Vec<Option<String>> = columns.iter().map(|entry| visible_name(entry)).collect();
    let mut missing: Vec<String> = Vec::new();
    for key in order_by {
        let present = columns.contains(&key.expression)
            || provided
                .iter()
                .any(|name| name.as_deref() == Some(key.expression.as_str()));
        if !present && !missing.contains(&key.expression) {
            missing.push(key.expression.clone());
        }
    }
    if missing.is_empty() {
        return None;
    }

    let visible: Option<Vec<String>> = provided.into_iter().collect();
    let visible = visible?;

    let MirNodeKind::Project { columns } = graph.node_kind_mut(project_node) else {
        unreachable!("checked above");
    };
    columns.extend(missing);
    Some(visible)
}

/// Output name of a projection entry, when it can be recomputed from
/// the entry text alone: plain identifiers name themselves, qualified
/// identifiers their trailing segment, and `expr AS alias` entries
/// their alias. `None` for wildcards and unaliased expression
/// entries, whose output names are assigned during dataflow
/// compilation.
fn visible_name(entry: &str) -> Option<String> {
    let is_ident = |part: &str| {
        !part.is_empty()
            && part
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '"')
    };
    if let Some((expr, alias)) = entry.rsplit_once(" AS ") {
        if is_ident(alias) && !expr.trim().is_empty() {
            return Some(alias.to_owned());
        }
    }
    if is_ident(entry) {
        return Some(entry.to_owned());
    }
    if let Some((relation, bare)) = entry.split_once('.') {
        if is_ident(relation) && is_ident(bare) {
            return Some(bare.to_owned());
        }
    }
    None
}

#[derive(Debug, Default)]
struct LowerContext {
    ctes: HashMap<String, MirGraph>,
    /// Names of `WITH RECURSIVE` CTEs currently being lowered: a
    /// `FROM` reference to one of these is the self-reference inside
    /// its own recursive step and lowers to a `RecursiveRef` leaf.
    recursive: HashSet<String>,
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

    let exists_terms = if let Some(selection) = &select.selection {
        let (exists_terms, scalar) = split_exists_terms(selection);
        if let Some(scalar) = &scalar {
            if contains_exists(scalar) {
                return Err(SqlError::UnsupportedFeature(
                    "EXISTS outside top-level AND conjuncts",
                ));
            }
            push_unary(
                &mut graph,
                MirNodeKind::Filter {
                    predicate: canonical_predicate(scalar),
                },
            );
        }
        exists_terms
    } else {
        Vec::new()
    };

    for term in exists_terms {
        let (subgraph, on) = lower_exists_subquery(term.subquery, context)?;
        let left = graph.root();
        let right = graph.append_graph(&subgraph);
        let kind = if term.negated {
            JoinKind::Anti
        } else {
            JoinKind::Semi
        };
        let join = graph.add_node(MirNodeKind::Join { kind, on });
        graph.add_input(left, join);
        graph.add_input(right, join);
        graph.set_root(join);
    }

    let group_by = group_by_columns(&select.group_by)?;
    let mut aggs = aggregate_exprs(&select.projection)?;

    // HAVING filters the aggregate's output. Aggregate calls inside
    // the predicate are replaced by references to (possibly hidden)
    // aggregate output columns, so the filter compiles like any other
    // — the projection above then drops the hidden columns.
    let having_predicate = if let Some(having) = &select.having {
        if contains_exists(having) {
            return Err(SqlError::UnsupportedFeature("EXISTS inside HAVING"));
        }
        let rewritten = rewrite_having_aggregates(having, &mut aggs)?;
        Some(canonical_predicate(&rewritten))
    } else {
        None
    };

    if !group_by.is_empty() || !aggs.is_empty() {
        push_unary(&mut graph, MirNodeKind::Aggregate { group_by, aggs });
    } else if having_predicate.is_some() {
        return Err(SqlError::UnsupportedFeature(
            "HAVING without aggregates or GROUP BY",
        ));
    }
    if let Some(predicate) = having_predicate {
        push_unary(&mut graph, MirNodeKind::Filter { predicate });
    }

    push_unary(
        &mut graph,
        MirNodeKind::Project {
            columns: select.projection.iter().map(select_item_name).collect(),
        },
    );

    match &select.distinct {
        Some(Distinct::Distinct) => push_unary(&mut graph, MirNodeKind::Distinct),
        Some(Distinct::On(exprs)) => push_unary(
            &mut graph,
            // Order keys are filled in by `apply_order_limit` when
            // this select is the top of a query with an ORDER BY;
            // otherwise the surviving row per group is arbitrary,
            // matching Postgres.
            MirNodeKind::DistinctOn {
                on: exprs.iter().map(ToString::to_string).collect(),
                order_by: Vec::new(),
            },
        ),
        None => {}
    }

    Ok(graph)
}

fn reject_select_features_not_lowered(select: &Select) -> Result<(), SqlError> {
    if has_group_by_modifiers(&select.group_by) {
        return Err(SqlError::UnsupportedFeature("GROUP BY modifiers"));
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
    let lateral = matches!(&join.relation, TableFactor::Derived { lateral: true, .. });
    let (right_graph, correlation) = if let TableFactor::Derived {
        lateral: true,
        subquery,
        ..
    } = &join.relation
    {
        lower_lateral_subquery(subquery, context)?
    } else {
        (lower_table_factor(&join.relation, context)?, Vec::new())
    };
    let right = graph.append_graph(&right_graph);

    let (kind, mut on) = match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(predicate)) => {
            (JoinKind::Inner, join_predicate_columns(predicate, lateral)?)
        }
        JoinOperator::LeftOuter(JoinConstraint::On(predicate)) => {
            (JoinKind::Left, join_predicate_columns(predicate, lateral)?)
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
        // A correlated LATERAL source turns a cross join into an
        // equi-join on the correlation columns.
        JoinOperator::CrossJoin if lateral && !correlation.is_empty() => {
            (JoinKind::Inner, Vec::new())
        }
        JoinOperator::CrossJoin => {
            return Err(SqlError::UnsupportedFeature("MIR lowering for cross joins"));
        }
        _ => return Err(SqlError::UnsupportedFeature("non-standard joins")),
    };

    on.extend(correlation);
    if on.is_empty() {
        return Err(SqlError::UnsupportedFeature(
            "LATERAL join without correlation or equi-join keys",
        ));
    }

    let left = graph.root();
    let join = graph.add_node(MirNodeKind::Join { kind, on });
    graph.add_input(left, join);
    graph.add_input(right, join);
    graph.set_root(join);
    Ok(())
}

/// Equi-join keys from an ON constraint. `ON TRUE` contributes no
/// keys, which is only meaningful for lateral sources (their keys come
/// from the correlated WHERE predicates instead).
fn join_predicate_columns(
    predicate: &Expr,
    lateral: bool,
) -> Result<Vec<(ColumnRef, ColumnRef)>, SqlError> {
    if lateral && matches!(predicate, Expr::Value(Value::Boolean(true))) {
        return Ok(Vec::new());
    }
    equi_join_columns(predicate)
}

fn lower_table_factor(table: &TableFactor, context: &LowerContext) -> Result<MirGraph, SqlError> {
    match table {
        TableFactor::Table { name, .. } => {
            let name = name.to_string();
            if context.recursive.contains(&name) {
                Ok(MirGraph::new(MirNodeKind::RecursiveRef { cte: name }))
            } else if let Some(cte) = context.ctes.get(&name) {
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
        // A LATERAL subquery as the *first* FROM item has nothing to
        // its left to correlate with, so it lowers like a plain
        // derived table. Correlated lateral sources sit in join
        // position and are handled by `lower_join`.
        TableFactor::Derived { subquery, .. } => lower_query_with_context(subquery, context),
        _ => Err(SqlError::UnsupportedFeature(
            "table functions or special table factors",
        )),
    }
}

/// A `[NOT] EXISTS (...)` conjunct found in a WHERE clause.
struct ExistsTerm<'a> {
    subquery: &'a Query,
    negated: bool,
}

/// Splits a WHERE expression into its top-level `[NOT] EXISTS`
/// conjuncts and the remaining scalar predicate.
fn split_exists_terms(expr: &Expr) -> (Vec<ExistsTerm<'_>>, Option<Expr>) {
    fn collect<'a>(expr: &'a Expr, terms: &mut Vec<ExistsTerm<'a>>, rest: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                collect(left, terms, rest);
                collect(right, terms, rest);
            }
            Expr::Exists { subquery, negated } => terms.push(ExistsTerm {
                subquery,
                negated: *negated,
            }),
            Expr::Nested(inner) if matches!(inner.as_ref(), Expr::Exists { .. }) => {
                collect(inner, terms, rest);
            }
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Not,
                expr: inner,
            } => {
                let unwrapped = match inner.as_ref() {
                    Expr::Nested(nested) => nested.as_ref(),
                    other => other,
                };
                if let Expr::Exists { subquery, negated } = unwrapped {
                    terms.push(ExistsTerm {
                        subquery,
                        negated: !*negated,
                    });
                } else {
                    rest.push(expr.clone());
                }
            }
            _ => rest.push(expr.clone()),
        }
    }

    let mut terms = Vec::new();
    let mut rest = Vec::new();
    collect(expr, &mut terms, &mut rest);
    let scalar = rest.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    });
    (terms, scalar)
}

/// Rewrites a HAVING predicate so every aggregate call becomes an
/// identifier naming an aggregate output column. Aggregates that
/// already appear in the projection with an alias reuse it; anything
/// else is appended to `aggs` under a synthetic `__having_N` alias
/// (the projection above the filter drops those hidden columns).
fn rewrite_having_aggregates(expr: &Expr, aggs: &mut Vec<AggExpr>) -> Result<Expr, SqlError> {
    fn rewrite(expr: &Expr, aggs: &mut Vec<AggExpr>, hidden: &mut usize) -> Result<Expr, SqlError> {
        Ok(match expr {
            Expr::Function(function) => {
                let Some(mut agg) = aggregate_expr(function, None)? else {
                    // Non-aggregate call (`coalesce`, ...): keep as-is.
                    return Ok(expr.clone());
                };
                let existing = aggs
                    .iter()
                    .find(|candidate| {
                        candidate.function == agg.function
                            && candidate.args == agg.args
                            && candidate.alias.is_some()
                    })
                    .and_then(|candidate| candidate.alias.clone());
                let alias = if let Some(alias) = existing {
                    alias
                } else {
                    *hidden += 1;
                    let alias = format!("__having_{hidden}");
                    agg.alias = Some(alias.clone());
                    aggs.push(agg);
                    alias
                };
                Expr::Identifier(Ident::new(alias))
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(rewrite(left, aggs, hidden)?),
                op: op.clone(),
                right: Box::new(rewrite(right, aggs, hidden)?),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(rewrite(inner, aggs, hidden)?),
            },
            Expr::Nested(inner) => Expr::Nested(Box::new(rewrite(inner, aggs, hidden)?)),
            Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite(inner, aggs, hidden)?)),
            Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite(inner, aggs, hidden)?)),
            Expr::IsTrue(inner) => Expr::IsTrue(Box::new(rewrite(inner, aggs, hidden)?)),
            Expr::IsFalse(inner) => Expr::IsFalse(Box::new(rewrite(inner, aggs, hidden)?)),
            Expr::Between {
                expr: inner,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(rewrite(inner, aggs, hidden)?),
                negated: *negated,
                low: Box::new(rewrite(low, aggs, hidden)?),
                high: Box::new(rewrite(high, aggs, hidden)?),
            },
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(rewrite(inner, aggs, hidden)?),
                list: list
                    .iter()
                    .map(|element| rewrite(element, aggs, hidden))
                    .collect::<Result<_, _>>()?,
                negated: *negated,
            },
            other => other.clone(),
        })
    }

    let mut hidden = 0;
    rewrite(expr, aggs, &mut hidden)
}

fn contains_exists(expr: &Expr) -> bool {
    struct ExistsFinder;
    impl Visitor for ExistsFinder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if matches!(expr, Expr::Exists { .. }) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    expr.visit(&mut ExistsFinder).is_break()
}

/// Lowers a correlated `EXISTS` subquery into a graph suitable as the
/// right input of a semi/anti join, returning the `(outer, inner)`
/// correlation columns extracted from the subquery's WHERE clause.
///
/// The subquery's SELECT list is irrelevant to EXISTS semantics, so no
/// projection is emitted — the right side keeps its full width, which
/// keeps the correlation columns visible to the join.
fn lower_exists_subquery(
    query: &Query,
    context: &LowerContext,
) -> Result<(MirGraph, Vec<(ColumnRef, ColumnRef)>), SqlError> {
    if query.with.is_some() {
        return Err(SqlError::UnsupportedFeature(
            "WITH inside EXISTS subqueries",
        ));
    }
    if query.limit.is_some() || query.offset.is_some() {
        return Err(SqlError::UnsupportedFeature(
            "LIMIT/OFFSET inside EXISTS subqueries",
        ));
    }
    let SetExpr::Select(select) = &*query.body else {
        return Err(SqlError::UnsupportedFeature(
            "set operations inside EXISTS subqueries",
        ));
    };
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {}
        _ => {
            return Err(SqlError::UnsupportedFeature(
                "GROUP BY inside EXISTS subqueries",
            ));
        }
    }
    if select.having.is_some() {
        return Err(SqlError::UnsupportedFeature(
            "HAVING inside EXISTS subqueries",
        ));
    }

    let inner_names = relation_names(select);
    let mut select = (**select).clone();
    let (pairs, remaining) = split_correlated_predicates(select.selection.take(), &inner_names);
    if pairs.is_empty() {
        return Err(SqlError::UnsupportedFeature(
            "uncorrelated EXISTS subqueries",
        ));
    }

    let mut graph = lower_from(&select, context)?;
    if let Some(predicate) = remaining {
        if contains_exists(&predicate) {
            return Err(SqlError::UnsupportedFeature(
                "EXISTS nested inside EXISTS subqueries",
            ));
        }
        push_unary(
            &mut graph,
            MirNodeKind::Filter {
                predicate: canonical_predicate(&predicate),
            },
        );
    }
    Ok((graph, pairs))
}

/// Lowers a `LATERAL (subquery)` join source, splitting correlated
/// equality predicates out of its WHERE clause into `(outer, inner)`
/// join keys. Inner correlation columns are appended to the
/// subquery's projection (when missing) so the join keys survive it.
fn lower_lateral_subquery(
    query: &Query,
    context: &LowerContext,
) -> Result<(MirGraph, Vec<(ColumnRef, ColumnRef)>), SqlError> {
    if query.with.is_some() {
        return Err(SqlError::UnsupportedFeature(
            "WITH inside LATERAL subqueries",
        ));
    }
    // A per-outer-row LIMIT ("top N per group") has no MIR encoding
    // after decorrelation, so reject rather than silently change
    // semantics to a global limit.
    if query.limit.is_some() || query.offset.is_some() {
        return Err(SqlError::UnsupportedFeature(
            "LIMIT/OFFSET inside LATERAL subqueries",
        ));
    }
    let SetExpr::Select(select) = &*query.body else {
        return Err(SqlError::UnsupportedFeature(
            "set operations inside LATERAL subqueries",
        ));
    };

    let inner_names = relation_names(select);
    let mut query = query.clone();
    let SetExpr::Select(select_mut) = &mut *query.body else {
        unreachable!("checked above");
    };
    let (pairs, remaining) = split_correlated_predicates(select_mut.selection.take(), &inner_names);
    select_mut.selection = remaining;

    for (_, inner) in &pairs {
        let inner_expr = column_ref_expr(inner);
        let already_projected = select_mut.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expr) => expr == &inner_expr,
            _ => false,
        });
        if !already_projected {
            select_mut
                .projection
                .push(SelectItem::UnnamedExpr(inner_expr));
        }
    }

    let graph = lower_query_with_context(&query, context)?;
    Ok((graph, pairs))
}

/// Relation qualifiers bound by a select's own FROM clause (table
/// names or aliases). Used to tell inner column references apart from
/// correlated (outer) ones.
fn relation_names(select: &Select) -> HashSet<String> {
    fn factor_name(factor: &TableFactor, names: &mut HashSet<String>) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let name = alias.as_ref().map_or_else(
                    || {
                        name.0
                            .last()
                            .map_or_else(|| name.to_string(), |part| part.value.clone())
                    },
                    |alias| alias.name.value.clone(),
                );
                names.insert(name);
            }
            TableFactor::Derived {
                alias: Some(alias), ..
            } => {
                names.insert(alias.name.value.clone());
            }
            _ => {}
        }
    }

    let mut names = HashSet::new();
    for table in &select.from {
        factor_name(&table.relation, &mut names);
        for join in &table.joins {
            factor_name(&join.relation, &mut names);
        }
    }
    names
}

/// Partitions a WHERE expression's top-level conjuncts into correlated
/// equality pairs (`outer.col = inner.col`, returned as
/// `(outer, inner)`) and the remaining local predicate. Only
/// qualified column references can correlate; unqualified names are
/// treated as inner.
fn split_correlated_predicates(
    selection: Option<Expr>,
    inner_names: &HashSet<String>,
) -> (Vec<(ColumnRef, ColumnRef)>, Option<Expr>) {
    fn collect(
        expr: Expr,
        inner_names: &HashSet<String>,
        pairs: &mut Vec<(ColumnRef, ColumnRef)>,
        rest: &mut Vec<Expr>,
    ) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                collect(*left, inner_names, pairs, rest);
                collect(*right, inner_names, pairs, rest);
            }
            Expr::BinaryOp {
                ref left,
                op: BinaryOperator::Eq,
                ref right,
            } => {
                if let (Ok(left_ref), Ok(right_ref)) = (column_ref(left), column_ref(right)) {
                    let left_inner = left_ref
                        .relation
                        .as_ref()
                        .is_none_or(|relation| inner_names.contains(relation));
                    let right_inner = right_ref
                        .relation
                        .as_ref()
                        .is_none_or(|relation| inner_names.contains(relation));
                    match (left_inner, right_inner) {
                        (false, true) => {
                            pairs.push((left_ref, right_ref));
                            return;
                        }
                        (true, false) => {
                            pairs.push((right_ref, left_ref));
                            return;
                        }
                        _ => {}
                    }
                }
                rest.push(expr);
            }
            other => rest.push(other),
        }
    }

    let mut pairs = Vec::new();
    let mut rest = Vec::new();
    if let Some(expr) = selection {
        collect(expr, inner_names, &mut pairs, &mut rest);
    }
    let remaining = rest.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    });
    (pairs, remaining)
}

fn column_ref_expr(column: &ColumnRef) -> Expr {
    column.relation.as_ref().map_or_else(
        || Expr::Identifier(Ident::new(column.name.clone())),
        |relation| {
            Expr::CompoundIdentifier(vec![
                Ident::new(relation.clone()),
                Ident::new(column.name.clone()),
            ])
        },
    )
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
        // The parser reads IS [NOT] DISTINCT FROM's right side as a
        // *full* expression, so re-parsing `a IS DISTINCT FROM b AND c`
        // would swallow ` AND c` into the comparison. Self-parenthesize
        // so canonical conjuncts stay reorderable.
        Expr::IsDistinctFrom(a, b) => {
            format!(
                "({} IS DISTINCT FROM {})",
                canonical_expr(a),
                canonical_expr(b)
            )
        }
        Expr::IsNotDistinctFrom(a, b) => {
            format!(
                "({} IS NOT DISTINCT FROM {})",
                canonical_expr(a),
                canonical_expr(b)
            )
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
        // Keep the full `expr AS alias` form: the alias alone would
        // sever the output column from the expression that computes
        // it. Downstream resolvers split on the trailing ` AS `.
        SelectItem::ExprWithAlias { expr, alias } => format!("{expr} AS {alias}"),
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
            MirNodeKind::Project { columns }
                if columns == &vec!["id".to_owned(), "title AS post_title".to_owned()]
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
    fn lowers_having_to_filter_above_aggregate() {
        // Aliased aggregates are referenced by alias; aggregates that
        // appear only in HAVING become hidden output columns.
        let graph = parse_and_lower(
            "SELECT author_id, count(*) AS n
             FROM posts
             GROUP BY author_id
             HAVING count(*) > 5 AND sum(id) > 10",
        )
        .expect("HAVING should lower");

        let aggregate = graph
            .node_kinds()
            .find_map(|node| match node {
                MirNodeKind::Aggregate { aggs, .. } => Some(aggs.clone()),
                _ => None,
            })
            .expect("aggregate node");
        assert_eq!(aggregate.len(), 2, "count(*) plus hidden sum(id)");
        assert_eq!(aggregate[1].alias.as_deref(), Some("__having_1"));

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate }
                if predicate.contains('n') && predicate.contains("__having_1")
        )));
    }

    #[test]
    fn rejects_having_without_aggregates_or_group_by() {
        let err = parse_and_lower("SELECT id FROM posts HAVING true")
            .expect_err("HAVING without aggregates or grouping is rejected");
        assert!(err.to_string().contains("HAVING"));
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
    fn lowers_correlated_exists_to_semi_join() {
        let graph = parse_and_lower(
            "SELECT id FROM posts
             WHERE author_id = 42
               AND EXISTS (
                 SELECT 1 FROM comments
                 WHERE comments.post_id = posts.id AND comments.author_id = 7
               )",
        )
        .expect("correlated EXISTS should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Join {
                kind: JoinKind::Semi,
                on,
            } if on == &vec![(
                ColumnRef {
                    relation: Some("posts".to_owned()),
                    name: "id".to_owned(),
                },
                ColumnRef {
                    relation: Some("comments".to_owned()),
                    name: "post_id".to_owned(),
                },
            )]
        )));
        // The outer scalar predicate and the subquery-local predicate
        // land in separate filters.
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate } if predicate == "author_id = 42"
        )));
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate } if predicate == "comments.author_id = 7"
        )));
    }

    #[test]
    fn lowers_not_exists_to_anti_join() {
        let graph = parse_and_lower(
            "SELECT id FROM posts
             WHERE NOT EXISTS (SELECT 1 FROM comments WHERE comments.post_id = posts.id)",
        )
        .expect("NOT EXISTS should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Join {
                kind: JoinKind::Anti,
                ..
            }
        )));
    }

    #[test]
    fn rejects_uncorrelated_exists() {
        let err = parse_and_lower(
            "SELECT id FROM posts WHERE EXISTS (SELECT 1 FROM comments WHERE comments.id = 5)",
        )
        .expect_err("uncorrelated EXISTS is out of scope");

        assert!(err.to_string().contains("uncorrelated EXISTS"));
    }

    #[test]
    fn rejects_exists_under_or() {
        let err = parse_and_lower(
            "SELECT id FROM posts
             WHERE author_id = 42
                OR EXISTS (SELECT 1 FROM comments WHERE comments.post_id = posts.id)",
        )
        .expect_err("EXISTS under OR is out of scope");

        assert!(err.to_string().contains("top-level AND"));
    }

    #[test]
    fn lowers_distinct_on_with_order_keys() {
        let graph = parse_and_lower(
            "SELECT DISTINCT ON (author_id) id, author_id
             FROM posts
             ORDER BY author_id, created_at DESC
             LIMIT 5",
        )
        .expect("DISTINCT ON should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::DistinctOn { on, order_by }
                if on == &vec!["author_id".to_owned()]
                    && order_by == &vec![
                        OrderKey {
                            expression: "author_id".to_owned(),
                            descending: false,
                        },
                        OrderKey {
                            expression: "created_at".to_owned(),
                            descending: true,
                        },
                    ]
        )));
        // `created_at` is not projected, so the projection grows a
        // hidden sort column and a visible re-projection sits above
        // the TopK.
        assert!(matches!(
            graph.root_kind(),
            MirNodeKind::Project { columns }
                if columns == &vec!["id".to_owned(), "author_id".to_owned()]
        ));
        assert!(graph
            .node_kinds()
            .any(|node| matches!(node, MirNodeKind::TopK { limit: 5, .. })));
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Project { columns }
                if columns
                    == &vec!["id".to_owned(), "author_id".to_owned(), "created_at".to_owned()]
        )));
    }

    #[test]
    fn lowers_hidden_order_keys_for_unprojected_columns() {
        let graph = parse_and_lower("SELECT id FROM posts ORDER BY created_at DESC LIMIT 3")
            .expect("ORDER BY over a non-projected column should lower");

        // Inner projection carries the hidden key; the root
        // re-projects the visible column above the TopK.
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Project { columns }
                if columns == &vec!["id".to_owned(), "created_at".to_owned()]
        )));
        assert!(matches!(
            graph.root_kind(),
            MirNodeKind::Project { columns } if columns == &vec!["id".to_owned()]
        ));
    }

    #[test]
    fn rejects_distinct_on_not_matching_initial_order_by() {
        let err = parse_and_lower(
            "SELECT DISTINCT ON (author_id) id, author_id
             FROM posts
             ORDER BY created_at DESC",
        )
        .expect_err("DISTINCT ON must match initial ORDER BY expressions");

        assert!(err.to_string().contains("initial ORDER BY"));
    }

    #[test]
    fn lowers_distinct_on_without_order_by() {
        let graph = parse_and_lower("SELECT DISTINCT ON (author_id) id, author_id FROM posts")
            .expect("DISTINCT ON without ORDER BY picks an arbitrary row, like Postgres");

        assert!(matches!(
            graph.root_kind(),
            MirNodeKind::DistinctOn { on, order_by }
                if on == &vec!["author_id".to_owned()] && order_by.is_empty()
        ));
    }

    #[test]
    fn lowers_correlated_lateral_join() {
        let graph = parse_and_lower(
            "SELECT posts.id
             FROM posts
             JOIN LATERAL (
                SELECT comments.body FROM comments WHERE comments.post_id = posts.id
             ) AS c ON TRUE",
        )
        .expect("correlated LATERAL join should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Join {
                kind: JoinKind::Inner,
                on,
            } if on == &vec![(
                ColumnRef {
                    relation: Some("posts".to_owned()),
                    name: "id".to_owned(),
                },
                ColumnRef {
                    relation: Some("comments".to_owned()),
                    name: "post_id".to_owned(),
                },
            )]
        )));
        // The correlation column is appended to the subquery's
        // projection so the join key survives it.
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Project { columns }
                if columns == &vec!["comments.body".to_owned(), "comments.post_id".to_owned()]
        )));
    }

    #[test]
    fn lowers_cross_join_lateral_with_correlation() {
        let graph = parse_and_lower(
            "SELECT posts.id
             FROM posts
             CROSS JOIN LATERAL (
                SELECT comments.body FROM comments WHERE comments.post_id = posts.id
             ) AS c",
        )
        .expect("correlated CROSS JOIN LATERAL should lower to an inner join");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Join {
                kind: JoinKind::Inner,
                ..
            }
        )));
    }

    #[test]
    fn rejects_lateral_with_limit() {
        let err = parse_and_lower(
            "SELECT posts.id
             FROM posts
             JOIN LATERAL (
                SELECT comments.body FROM comments
                WHERE comments.post_id = posts.id
                ORDER BY comments.id LIMIT 1
             ) AS c ON TRUE",
        )
        .expect_err("per-row LIMIT inside LATERAL has no MIR encoding");

        assert!(err.to_string().contains("LIMIT/OFFSET inside LATERAL"));
    }

    #[test]
    fn lowers_recursive_cte_to_fixpoint() {
        let graph = parse_and_lower(
            "WITH RECURSIVE reach AS (
                SELECT id, post_id FROM comments WHERE post_id = 1
                UNION
                SELECT comments.id, comments.post_id
                FROM comments JOIN reach ON comments.post_id = reach.id
             )
             SELECT id FROM reach",
        )
        .expect("recursive CTE should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Fixpoint {
                cte,
                union_all: false,
            } if cte == "reach"
        )));
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::RecursiveRef { cte } if cte == "reach"
        )));
        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::CteRef { cte } if cte == "reach"
        )));
    }

    #[test]
    fn lowers_cast_and_any_predicates() {
        let cast = parse_and_lower("SELECT id FROM posts WHERE id::text = 'x'")
            .expect("cast predicate should lower");
        assert!(cast.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate } if predicate.contains("id::TEXT")
        )));

        let any = parse_and_lower("SELECT id FROM posts WHERE id = ANY('{1,2,3}')")
            .expect("ANY predicate should lower");
        assert!(any.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate } if predicate == "id = ANY('{1,2,3}')"
        )));
    }

    #[test]
    fn lowers_cardinality_projection() {
        let graph = parse_and_lower("SELECT cardinality(title) FROM posts")
            .expect("cardinality projection should lower");

        assert!(graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Project { columns } if columns == &vec!["cardinality(title)".to_owned()]
        )));
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
