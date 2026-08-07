// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MIR → dataflow build-plan compiler.
//!
//! [`compile_mir`] walks a `MirGraph` topologically and emits a
//! [`CompiledPlan`] of per-node "recipes" — boxed closures that read
//! / extract / aggregate values from `Row`s. [`install_plan`]
//! consumes a plan inside a timely scope, wiring the appropriate
//! [`relational`](crate::palimpsest::relational) operator for each
//! node and returning a final `VecCollection<G, Row, isize>` that the
//! caller arranges into a trace for snapshot + diff delivery.
//!
//! The compiler is intentionally permissive about column shapes: every
//! intermediate collection carries `Row` values rather than typed
//! tuples, with per-node closures projecting columns by index. That
//! erases the type-system pressure that comes from compiling SQL into
//! differential's strongly-typed operators, while still passing each
//! operator data of the exact shape it expects (e.g. `(i64, i64)`
//! pairs into `aggregate_i64`).
//!
//! Coverage today: `BaseTable`, `Filter` (boolean predicates, casts),
//! `Project` (column rename / reorder, `*` and `rel.*` wildcards,
//! cast / `coalesce` / `cardinality` expressions), `Join` (inner,
//! left, semi, anti — equi-keys only), `Distinct`, `Union` (`ALL` and
//! `DISTINCT`), `Aggregate` (group-by with `COUNT` / `SUM` / `MIN` /
//! `MAX` / `AVG`), `TopK` (single-column sort), and `CteRef`.
//! `DistinctOn`, `Except`, `Intersect`, `Fixpoint`, `RecursiveRef`,
//! and `Leaf` return [`CompileError::Unsupported`]. The walker is
//! structured so each of those reduces to a single `match` arm +
//! recipe variant when wired.

use std::collections::HashMap;
use std::sync::Arc;

use palimpsest_sql::catalog::ColumnType;
use palimpsest_sql::mir::{
    AggExpr, ColumnRef, JoinKind, MirGraph, MirNodeKind, OrderKey, SetQuantifierKind,
};
use palimpsest_wal::{Datum, TableId};
use petgraph::graph::NodeIndex;
use petgraph::Direction;
use smallvec::SmallVec;
use thiserror::Error;

use crate::operators::Join as _;
use crate::palimpsest::eval::{compile_predicate, compile_typed_scalar, EvalError, ScalarSchema};
use crate::palimpsest::relational::{self, AggregateFunc, AggregateValue, SortDirection};
use crate::palimpsest::wal::Row;
use crate::{lattice::Lattice, VecCollection};

// -----------------------------------------------------------------------------
// Public types
// -----------------------------------------------------------------------------

/// A query compiled from MIR into a plan that can be instantiated into
/// a timely scope. The compiler doesn't itself touch timely — the
/// dataflow host owns that lifecycle.
#[derive(Clone)]
pub struct CompiledPlan {
    /// The MIR the plan was compiled from. Held so the installer can
    /// re-walk edges to find each node's inputs.
    pub graph: MirGraph,
    /// Root node of `graph` — the query's output operator.
    pub root: NodeIndex,
    /// Tables this query reads from, in the order the host should wire
    /// them up. Each entry maps 1:1 to a `BaseTable` MIR node.
    pub inputs: Vec<TableId>,
    /// Per-table schemas captured at compile time. The host uses
    /// these to know how to encode raw `WalUpdate` rows that feed the
    /// dataflow's input handles.
    pub input_schemas: HashMap<TableId, ScalarSchema>,
    /// Output schema of `root` — the rows the query produces.
    pub output_schema: ScalarSchema,
    /// Per-node schemas (intermediate + leaf). Useful for debugging
    /// and for downstream consumers that want to render an explain
    /// plan.
    pub node_schemas: HashMap<NodeIndex, ScalarSchema>,
    /// Per-node compiled recipes. Indexed by the MIR node index.
    pub recipes: HashMap<NodeIndex, NodeRecipe>,
}

/// Compiled per-node payload. Each variant is a fragment of dataflow
/// the installer can lift into a scope. Closures are `Arc`-wrapped so
/// the installer can clone them across multiple instantiations
/// (shared subgraph reuse) without re-compiling the MIR.
#[derive(Clone)]
pub enum NodeRecipe {
    /// Read raw rows from one of the dataflow host's input handles.
    BaseTable {
        /// Table id passed into `install_plan`'s `inputs` map.
        table: TableId,
    },
    /// Drop rows for which the boolean predicate evaluates to false.
    Filter {
        /// Closure that reads from the input row and returns a bool.
        predicate: Arc<dyn Fn(&Row) -> bool + Send + Sync>,
    },
    /// Reorder + rename columns. The closure returns the new row.
    Project {
        /// Closure that reads from the input row and returns the
        /// projected row.
        extract: Arc<dyn Fn(&Row) -> Row + Send + Sync>,
    },
    /// Equi-join of the node's two ordered inputs. Key columns are
    /// row indices resolved at compile time; the runtime key is the
    /// `Vec<Datum>` of those columns. SQL null semantics: a `NULL` in
    /// any key column never matches.
    Join {
        /// Inner / left / semi / anti.
        kind: JoinKind,
        /// Key column indices into the left input's rows.
        left_keys: Vec<usize>,
        /// Key column indices into the right input's rows.
        right_keys: Vec<usize>,
        /// Right input's column count — the null-extension width for
        /// unmatched left rows in a `Left` join.
        right_width: usize,
    },
    /// Bag → set: drop duplicate rows.
    Distinct,
    /// Concatenate the node's two ordered inputs; `all: false` also
    /// deduplicates (`UNION` vs `UNION ALL`).
    Union {
        /// `true` for `UNION ALL` (bag semantics).
        all: bool,
    },
    /// Group-by aggregate. Group key is the value of one column (a
    /// single-column grouping is all we wire today); each aggregate
    /// function reads the same value column. The compiler emits a
    /// `Project` upstream if a richer extraction is needed.
    Aggregate {
        /// Closure that reads the group-by column out of the input
        /// row. Returns a `Datum` so the original column type is
        /// preserved end-to-end — `aggregate_i64`'s key parameter is
        /// generic, so we use the full `Datum` rather than coercing
        /// `Bool` / `Text` group keys onto `i64`.
        group_extract: Arc<dyn Fn(&Row) -> Datum + Send + Sync>,
        /// Closure that reads the aggregate value column. For
        /// `COUNT(*)`-only aggregates this is the constant zero
        /// extractor (the operator only counts diffs).
        value_extract: Arc<dyn Fn(&Row) -> i64 + Send + Sync>,
        /// One entry per aggregate function in projection order.
        funcs: Vec<AggregateFunc>,
    },
    /// Global TopK with a single-column sort key.
    TopK {
        /// Closure that reads the sort key out of each row.
        sort_key_extract: Arc<dyn Fn(&Row) -> i64 + Send + Sync>,
        /// Ascending vs descending order.
        direction: SortDirection,
        /// Limit (max rows retained).
        limit: usize,
        /// Offset (rows skipped from the head of the sorted slice).
        offset: usize,
    },
    /// CTE reference: forwards to another node in the same graph.
    CteRef {
        /// MIR node index of the CTE's root.
        target: NodeIndex,
    },
}

/// Compile-time errors. Runtime evaluation can't fail — every closure
/// returns *some* `Row` — so any rejection lands here.
#[derive(Debug, Error)]
pub enum CompileError {
    /// Operator kind not yet implemented by the compiler.
    #[error("unsupported MIR node: {0}")]
    Unsupported(String),
    /// Expression evaluator rejected an inline string.
    #[error("expression: {0}")]
    Expression(#[from] EvalError),
    /// Identifier references a column / table the schema lookup
    /// doesn't know about.
    #[error("unknown identifier: {0}")]
    Unknown(String),
    /// MIR has a cycle. petgraph's toposort surfaces this.
    #[error("MIR graph has a cycle")]
    Cycle,
    /// Aggregate function unsupported (something other than
    /// COUNT / SUM / MIN / MAX / AVG / COUNT DISTINCT).
    #[error("unsupported aggregate function: {0}")]
    UnsupportedAggregate(String),
    /// Multi-column group-by (one column is the only shape we wire).
    #[error("multi-column GROUP BY not yet supported")]
    MultiColumnGroupBy,
    /// Aggregates over different value columns aren't wired —
    /// `aggregate_i64` only takes one input column per call.
    #[error("aggregate columns disagree: {0}")]
    HeterogeneousAggregateColumns(String),
    /// Multi-column ORDER BY not yet wired.
    #[error("multi-column ORDER BY not yet supported")]
    MultiColumnOrderBy,
}

/// Callback signature the compiler uses to look up a base table's
/// schema. The dataflow host owns the demo's catalog and supplies
/// this when building the plan.
pub trait TableSchemaLookup {
    /// Resolve `table` to its `(table_id, schema)` pair, or `None`
    /// if the table isn't known.
    fn lookup(&self, table: &str) -> Option<(TableId, ScalarSchema)>;
}

impl<F> TableSchemaLookup for F
where
    F: Fn(&str) -> Option<(TableId, ScalarSchema)>,
{
    fn lookup(&self, table: &str) -> Option<(TableId, ScalarSchema)> {
        (self)(table)
    }
}

// -----------------------------------------------------------------------------
// Compile entry point
// -----------------------------------------------------------------------------

/// Walk `graph` and emit a [`CompiledPlan`].
///
/// # Errors
/// Returns [`CompileError`] on cycles, unknown identifiers, or MIR
/// shapes the walker hasn't been taught yet.
pub fn compile_mir<L: TableSchemaLookup>(
    graph: &MirGraph,
    tables: &L,
) -> Result<CompiledPlan, CompileError> {
    let topo = petgraph::algo::toposort(graph.graph(), None).map_err(|_| CompileError::Cycle)?;

    let mut state = CompileState {
        node_schemas: HashMap::new(),
        node_provenance: HashMap::new(),
        recipes: HashMap::new(),
        inputs: Vec::new(),
        input_schemas: HashMap::new(),
    };

    for node in topo {
        compile_node(graph, node, tables, &mut state)?;
    }

    let root = graph.root();
    let output_schema = state.node_schemas.get(&root).cloned().unwrap_or_default();

    Ok(CompiledPlan {
        graph: graph.clone(),
        root,
        inputs: state.inputs,
        input_schemas: state.input_schemas,
        output_schema,
        node_schemas: state.node_schemas,
        recipes: state.recipes,
    })
}

struct CompileState {
    node_schemas: HashMap<NodeIndex, ScalarSchema>,
    /// Per-node, per-column source-relation attribution, aligned with
    /// the node's schema columns. `Some(table)` while a column can
    /// still be traced to one base table; `None` after aggregates,
    /// set ops, or computed projections. Qualified references
    /// (`posts.id`) and `rel.*` wildcards resolve through this — the
    /// flat `ScalarSchema` alone can't disambiguate columns that share
    /// a name across a join's two sides.
    node_provenance: HashMap<NodeIndex, Vec<Option<String>>>,
    recipes: HashMap<NodeIndex, NodeRecipe>,
    inputs: Vec<TableId>,
    input_schemas: HashMap<TableId, ScalarSchema>,
}

fn compile_node<L: TableSchemaLookup>(
    graph: &MirGraph,
    node: NodeIndex,
    tables: &L,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let kind = graph.node_kind(node);
    match kind {
        MirNodeKind::BaseTable { table, project } => {
            compile_base_table(node, table, project, tables, state)
        }
        MirNodeKind::Filter { predicate } => compile_filter(graph, node, predicate, state),
        MirNodeKind::Project { columns } => compile_project(graph, node, columns, state),
        MirNodeKind::Aggregate { group_by, aggs } => {
            compile_aggregate(graph, node, group_by, aggs, state)
        }
        MirNodeKind::TopK {
            order_by,
            limit,
            offset,
        } => compile_topk(graph, node, order_by, *limit, *offset, state),
        MirNodeKind::CteRef { cte } => compile_cte_ref(graph, node, cte, state),
        MirNodeKind::Join { kind, on } => compile_join(graph, node, *kind, on, state),
        MirNodeKind::Distinct => compile_distinct(graph, node, state),
        MirNodeKind::DistinctOn { .. } => Err(CompileError::Unsupported("DistinctOn".to_owned())),
        MirNodeKind::Union { quantifier } => compile_union(graph, node, *quantifier, state),
        MirNodeKind::Except { .. } => Err(CompileError::Unsupported("Except".to_owned())),
        MirNodeKind::Intersect { .. } => Err(CompileError::Unsupported("Intersect".to_owned())),
        MirNodeKind::Fixpoint { .. } => Err(CompileError::Unsupported("Fixpoint".to_owned())),
        MirNodeKind::RecursiveRef { .. } => {
            Err(CompileError::Unsupported("RecursiveRef".to_owned()))
        }
        MirNodeKind::Leaf { .. } => Err(CompileError::Unsupported("Leaf".to_owned())),
    }
}

// -----------------------------------------------------------------------------
// Per-node compile helpers
// -----------------------------------------------------------------------------

fn compile_base_table<L: TableSchemaLookup>(
    node: NodeIndex,
    table: &str,
    project: &[ColumnRef],
    tables: &L,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let (table_id, full_schema) = tables
        .lookup(table)
        .ok_or_else(|| CompileError::Unknown(format!("table {table}")))?;

    // If `project` is empty, expose the table's full schema. Otherwise
    // narrow to the named columns (in MIR order).
    let schema = if project.is_empty() {
        full_schema.clone()
    } else {
        let pairs = project
            .iter()
            .map(|col| {
                full_schema
                    .column_type(&col.name)
                    .ok_or_else(|| CompileError::Unknown(format!("{table}.{}", col.name)))
                    .map(|ty| (col.name.clone(), ty))
            })
            .collect::<Result<Vec<_>, _>>()?;
        ScalarSchema::from_pairs(pairs)
    };

    if !state.input_schemas.contains_key(&table_id) {
        state.inputs.push(table_id);
        state.input_schemas.insert(table_id, full_schema);
    }

    state
        .node_provenance
        .insert(node, vec![Some(table.to_owned()); schema.len()]);
    state.node_schemas.insert(node, schema);
    state
        .recipes
        .insert(node, NodeRecipe::BaseTable { table: table_id });
    Ok(())
}

fn compile_filter(
    graph: &MirGraph,
    node: NodeIndex,
    predicate: &str,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let input_node = single_input(graph, node)?;
    let input_schema = state
        .node_schemas
        .get(&input_node)
        .ok_or_else(|| CompileError::Unknown("filter input schema".to_owned()))?
        .clone();

    let pred = compile_predicate(predicate, &input_schema)?;
    let pred: Arc<dyn Fn(&Row) -> bool + Send + Sync> = Arc::from(pred);

    let provenance = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);
    state.node_provenance.insert(node, provenance);
    state.node_schemas.insert(node, input_schema);
    state
        .recipes
        .insert(node, NodeRecipe::Filter { predicate: pred });
    Ok(())
}

/// One projected output column: either a passthrough of an input
/// column by index, or a compiled scalar expression.
enum OutputColumn {
    Index(usize),
    Scalar(Arc<dyn Fn(&Row) -> Datum + Send + Sync>),
}

fn compile_project(
    graph: &MirGraph,
    node: NodeIndex,
    columns: &[String],
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let input_node = single_input(graph, node)?;
    let input_schema = state
        .node_schemas
        .get(&input_node)
        .ok_or_else(|| CompileError::Unknown("project input schema".to_owned()))?
        .clone();
    let input_prov = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);

    let mut outputs: Vec<OutputColumn> = Vec::with_capacity(columns.len());
    let mut output_pairs = Vec::with_capacity(columns.len());
    let mut output_prov = Vec::with_capacity(columns.len());
    let push_index = |index: usize,
                      name: String,
                      outputs: &mut Vec<OutputColumn>,
                      output_pairs: &mut Vec<(String, ColumnType)>,
                      output_prov: &mut Vec<Option<String>>| {
        let ty = input_schema.columns()[index].1;
        outputs.push(OutputColumn::Index(index));
        output_pairs.push((name, ty));
        output_prov.push(input_prov.get(index).cloned().flatten());
    };

    for entry in columns {
        let entry = entry.trim();

        // `SELECT *` — every input column, in order.
        if entry == "*" {
            for (index, (name, _)) in input_schema.columns().iter().enumerate() {
                push_index(
                    index,
                    name.clone(),
                    &mut outputs,
                    &mut output_pairs,
                    &mut output_prov,
                );
            }
            continue;
        }

        // `rel.*` — the columns provenance attributes to `rel`.
        if let Some(relation) = entry.strip_suffix(".*") {
            let mut matched = false;
            for (index, (name, _)) in input_schema.columns().iter().enumerate() {
                if input_prov.get(index).and_then(Option::as_deref) == Some(relation) {
                    matched = true;
                    push_index(
                        index,
                        name.clone(),
                        &mut outputs,
                        &mut output_pairs,
                        &mut output_prov,
                    );
                }
            }
            if !matched {
                return Err(CompileError::Unknown(format!("project columns {entry}")));
            }
            continue;
        }

        // Bare column name.
        if let Some(index) = input_schema.index_of(entry) {
            push_index(
                index,
                entry.to_owned(),
                &mut outputs,
                &mut output_pairs,
                &mut output_prov,
            );
            continue;
        }

        // Qualified `rel.column`: prefer the provenance-attributed
        // occurrence (a join can carry the same bare name on both
        // sides), fall back to the bare name. The output column is
        // named by the trailing segment, matching Postgres.
        if let Some((relation, bare)) = split_qualified(entry) {
            let attributed =
                input_schema
                    .columns()
                    .iter()
                    .enumerate()
                    .position(|(i, (name, _))| {
                        name == bare
                            && input_prov.get(i).and_then(Option::as_deref) == Some(relation)
                    });
            let index = attributed.or_else(|| input_schema.index_of(bare));
            if let Some(index) = index {
                push_index(
                    index,
                    bare.to_owned(),
                    &mut outputs,
                    &mut output_pairs,
                    &mut output_prov,
                );
                continue;
            }
            return Err(CompileError::Unknown(format!("project column {entry}")));
        }

        // Anything else is a scalar expression (cast, coalesce, ...).
        let (scalar, ty) = compile_typed_scalar(entry, &input_schema)?;
        let name = crate::palimpsest::eval::cast_label(entry).unwrap_or_else(|| entry.to_owned());
        outputs.push(OutputColumn::Scalar(Arc::from(scalar)));
        output_pairs.push((name, ty));
        output_prov.push(None);
    }

    let output_schema = ScalarSchema::from_pairs(output_pairs);
    let outputs_owned = outputs;
    let extract: Arc<dyn Fn(&Row) -> Row + Send + Sync> = Arc::new(move |row: &Row| {
        let mut out: Row = SmallVec::with_capacity(outputs_owned.len());
        for column in &outputs_owned {
            match column {
                OutputColumn::Index(i) => {
                    out.push(row.get(*i).cloned().unwrap_or(Datum::Null));
                }
                OutputColumn::Scalar(scalar) => out.push(scalar(row)),
            }
        }
        out
    });

    state.node_provenance.insert(node, output_prov);
    state.node_schemas.insert(node, output_schema);
    state.recipes.insert(node, NodeRecipe::Project { extract });
    Ok(())
}

/// Splits `rel.column` when both segments are plain identifiers;
/// `None` for anything with operators, calls, casts, or extra dots.
fn split_qualified(entry: &str) -> Option<(&str, &str)> {
    let (relation, bare) = entry.split_once('.')?;
    let is_ident = |part: &str| {
        !part.is_empty()
            && part
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '"')
    };
    (is_ident(relation) && is_ident(bare)).then_some((relation, bare))
}

fn compile_aggregate(
    graph: &MirGraph,
    node: NodeIndex,
    group_by: &[ColumnRef],
    aggs: &[AggExpr],
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let input_node = single_input(graph, node)?;
    let input_schema = state
        .node_schemas
        .get(&input_node)
        .ok_or_else(|| CompileError::Unknown("aggregate input schema".to_owned()))?
        .clone();

    if group_by.len() != 1 {
        return Err(CompileError::MultiColumnGroupBy);
    }
    let group_col = &group_by[0].name;
    let group_idx = input_schema
        .index_of(group_col)
        .ok_or_else(|| CompileError::Unknown(format!("group column {group_col}")))?;
    let group_type = input_schema
        .column_type(group_col)
        .expect("type for known column");

    // Return the raw `Datum` so the operator key preserves the
    // column's original SQL type. `aggregate_i64` is generic over
    // `K`, so passing `Datum` works directly — and the schema we
    // advertise to clients (group_col with its original `group_type`)
    // round-trips end-to-end.
    let group_extract: Arc<dyn Fn(&Row) -> Datum + Send + Sync> =
        Arc::new(move |row: &Row| row.get(group_idx).cloned().unwrap_or(Datum::Null));

    // `aggregate_i64` reads one input column per call, with each
    // `AggregateFunc` evaluating against the same value stream.
    // Enforce that all aggregates either reference the same column
    // or are `COUNT(*)` (which ignores its argument).
    let mut value_column: Option<String> = None;
    let mut funcs = Vec::with_capacity(aggs.len());
    let mut output_pairs = Vec::with_capacity(group_by.len() + aggs.len());
    output_pairs.push((group_col.clone(), group_type));

    for agg in aggs {
        let func = parse_agg_func(&agg.function)?;
        funcs.push(func);

        // Validate the value column. `*` is the wildcard for COUNT.
        let arg_text = agg.args.first().map(String::as_str).unwrap_or("*");
        let arg_col = arg_text.trim();
        if arg_col != "*" && !matches!(func, AggregateFunc::Count) {
            match &value_column {
                None => value_column = Some(arg_col.to_owned()),
                Some(prev) if prev == arg_col => {}
                Some(prev) => {
                    return Err(CompileError::HeterogeneousAggregateColumns(format!(
                        "{prev} vs {arg_col}"
                    )));
                }
            }
        }

        let alias = agg
            .alias
            .clone()
            .unwrap_or_else(|| format!("{}_{}", agg.function.to_lowercase(), output_pairs.len()));
        let output_type = match func {
            AggregateFunc::Avg => ColumnType::Float,
            _ => ColumnType::Int,
        };
        output_pairs.push((alias, output_type));
    }

    // If every agg is COUNT(*), there's no value column — pass a
    // zero extractor; aggregate_i64 only reads diffs in that case.
    let value_extract: Arc<dyn Fn(&Row) -> i64 + Send + Sync> = match value_column {
        None => Arc::new(|_row: &Row| 0),
        Some(col) => {
            let value_idx = input_schema
                .index_of(&col)
                .ok_or_else(|| CompileError::Unknown(format!("aggregate column {col}")))?;
            Arc::new(move |row: &Row| match row.get(value_idx) {
                Some(Datum::I64(v)) => *v,
                Some(Datum::I32(v)) => i64::from(*v),
                Some(Datum::I16(v)) => i64::from(*v),
                _ => 0,
            })
        }
    };

    let output_schema = ScalarSchema::from_pairs(output_pairs);

    state
        .node_provenance
        .insert(node, vec![None; output_schema.len()]);
    state.node_schemas.insert(node, output_schema);
    state.recipes.insert(
        node,
        NodeRecipe::Aggregate {
            group_extract,
            value_extract,
            funcs,
        },
    );
    Ok(())
}

fn compile_join(
    graph: &MirGraph,
    node: NodeIndex,
    kind: JoinKind,
    on: &[(ColumnRef, ColumnRef)],
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let inputs = graph.ordered_inputs(node);
    let [left, right] = inputs.as_slice() else {
        return Err(CompileError::Unsupported(format!(
            "join with {} inputs",
            inputs.len()
        )));
    };
    let left_schema = state
        .node_schemas
        .get(left)
        .ok_or_else(|| CompileError::Unknown("join left schema".to_owned()))?
        .clone();
    let right_schema = state
        .node_schemas
        .get(right)
        .ok_or_else(|| CompileError::Unknown("join right schema".to_owned()))?
        .clone();
    let left_prov = state
        .node_provenance
        .get(left)
        .cloned()
        .unwrap_or_else(|| vec![None; left_schema.len()]);
    let right_prov = state
        .node_provenance
        .get(right)
        .cloned()
        .unwrap_or_else(|| vec![None; right_schema.len()]);

    if on.is_empty() {
        return Err(CompileError::Unsupported(
            "join without equi-keys".to_owned(),
        ));
    }

    // Each ON pair is written in source order, which need not match
    // the join's (left, right) input order — resolve both
    // orientations, qualifier-aware first so `t2.x = t1.y` binds
    // through provenance even when both sides carry an `x` and a `y`.
    let mut left_keys = Vec::with_capacity(on.len());
    let mut right_keys = Vec::with_capacity(on.len());
    for (a, b) in on {
        let orientations = [
            (
                resolve_column(&left_schema, &left_prov, a, true),
                resolve_column(&right_schema, &right_prov, b, true),
            ),
            (
                resolve_column(&left_schema, &left_prov, b, true),
                resolve_column(&right_schema, &right_prov, a, true),
            ),
            (
                resolve_column(&left_schema, &left_prov, a, false),
                resolve_column(&right_schema, &right_prov, b, false),
            ),
            (
                resolve_column(&left_schema, &left_prov, b, false),
                resolve_column(&right_schema, &right_prov, a, false),
            ),
        ];
        let Some((left_idx, right_idx)) = orientations.into_iter().find_map(|pair| match pair {
            (Some(l), Some(r)) => Some((l, r)),
            _ => None,
        }) else {
            return Err(CompileError::Unknown(format!("join key {a:?} = {b:?}")));
        };
        left_keys.push(left_idx);
        right_keys.push(right_idx);
    }

    let right_width = right_schema.len();

    // Inner/left joins emit left ++ right; semi/anti joins emit the
    // left rows untouched.
    let (output_schema, output_prov) = match kind {
        JoinKind::Inner | JoinKind::Left => {
            let mut pairs: Vec<(String, ColumnType)> = left_schema.columns().to_vec();
            pairs.extend(right_schema.columns().iter().cloned());
            let mut prov = left_prov;
            prov.extend(right_prov);
            (ScalarSchema::from_pairs(pairs), prov)
        }
        JoinKind::Semi | JoinKind::Anti => (left_schema, left_prov),
    };

    state.node_provenance.insert(node, output_prov);
    state.node_schemas.insert(node, output_schema);
    state.recipes.insert(
        node,
        NodeRecipe::Join {
            kind,
            left_keys,
            right_keys,
            right_width,
        },
    );
    Ok(())
}

/// Resolves a join-key reference against one input's schema. In
/// `strict` mode a qualified reference must bind through provenance
/// (its qualifier attributed to the column's source table); in lenient
/// mode it falls back to the bare column name, which also covers
/// table aliases the lowering discarded.
fn resolve_column(
    schema: &ScalarSchema,
    provenance: &[Option<String>],
    reference: &ColumnRef,
    strict: bool,
) -> Option<usize> {
    if let Some(relation) = &reference.relation {
        let attributed = schema
            .columns()
            .iter()
            .enumerate()
            .find_map(|(i, (name, _))| {
                (name == &reference.name
                    && provenance.get(i).and_then(Option::as_deref) == Some(relation.as_str()))
                .then_some(i)
            });
        if strict {
            return attributed;
        }
        if attributed.is_some() {
            return attributed;
        }
    } else if strict {
        // An unqualified reference has no qualifier to check; let the
        // lenient pass handle it so qualified bindings win first.
        return None;
    }
    schema.index_of(&reference.name)
}

fn compile_distinct(
    graph: &MirGraph,
    node: NodeIndex,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let input_node = single_input(graph, node)?;
    let input_schema = state
        .node_schemas
        .get(&input_node)
        .ok_or_else(|| CompileError::Unknown("distinct input schema".to_owned()))?
        .clone();
    let provenance = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);

    state.node_provenance.insert(node, provenance);
    state.node_schemas.insert(node, input_schema);
    state.recipes.insert(node, NodeRecipe::Distinct);
    Ok(())
}

fn compile_union(
    graph: &MirGraph,
    node: NodeIndex,
    quantifier: SetQuantifierKind,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let inputs = graph.ordered_inputs(node);
    let [left, right] = inputs.as_slice() else {
        return Err(CompileError::Unsupported(format!(
            "union with {} inputs",
            inputs.len()
        )));
    };
    let left_schema = state
        .node_schemas
        .get(left)
        .ok_or_else(|| CompileError::Unknown("union left schema".to_owned()))?
        .clone();
    let right_schema = state
        .node_schemas
        .get(right)
        .ok_or_else(|| CompileError::Unknown("union right schema".to_owned()))?;

    // Set ops align columns positionally; a width mismatch would make
    // rows of different arity flow through one collection.
    if left_schema.len() != right_schema.len() {
        return Err(CompileError::Unsupported(format!(
            "UNION branches with {} vs {} columns",
            left_schema.len(),
            right_schema.len()
        )));
    }

    // Postgres names set-op output after the left branch. Column
    // provenance does not survive a union.
    state
        .node_provenance
        .insert(node, vec![None; left_schema.len()]);
    state.node_schemas.insert(node, left_schema);
    state.recipes.insert(
        node,
        NodeRecipe::Union {
            all: quantifier == SetQuantifierKind::All,
        },
    );
    Ok(())
}

fn parse_agg_func(name: &str) -> Result<AggregateFunc, CompileError> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Ok(AggregateFunc::Count),
        "sum" => Ok(AggregateFunc::Sum),
        "min" => Ok(AggregateFunc::Min),
        "max" => Ok(AggregateFunc::Max),
        "avg" => Ok(AggregateFunc::Avg),
        other => Err(CompileError::UnsupportedAggregate(other.to_owned())),
    }
}

fn compile_topk(
    graph: &MirGraph,
    node: NodeIndex,
    order_by: &[OrderKey],
    limit: usize,
    offset: usize,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let input_node = single_input(graph, node)?;
    let input_schema = state
        .node_schemas
        .get(&input_node)
        .ok_or_else(|| CompileError::Unknown("topk input schema".to_owned()))?
        .clone();

    if order_by.len() != 1 {
        return Err(CompileError::MultiColumnOrderBy);
    }
    let key = &order_by[0];
    let sort_idx = input_schema
        .index_of(&key.expression)
        .ok_or_else(|| CompileError::Unknown(format!("order column {}", key.expression)))?;

    let sort_key_extract: Arc<dyn Fn(&Row) -> i64 + Send + Sync> =
        Arc::new(move |row: &Row| match row.get(sort_idx) {
            Some(Datum::I64(v)) => *v,
            Some(Datum::I32(v)) => i64::from(*v),
            Some(Datum::I16(v)) => i64::from(*v),
            _ => 0,
        });

    let direction = if key.descending {
        SortDirection::Descending
    } else {
        SortDirection::Ascending
    };

    let provenance = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);
    state.node_provenance.insert(node, provenance);
    state.node_schemas.insert(node, input_schema);
    state.recipes.insert(
        node,
        NodeRecipe::TopK {
            sort_key_extract,
            direction,
            limit,
            offset,
        },
    );
    Ok(())
}

fn compile_cte_ref(
    graph: &MirGraph,
    node: NodeIndex,
    cte: &str,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    // CteExpansion edges point from the CTE's root *into* the
    // CteRef node — i.e. they're incoming edges on this node,
    // sourced at the cte_root. See `palimpsest_sql::lower::lower_table_factor`.
    use petgraph::visit::EdgeRef;
    let target = graph
        .graph()
        .edges_directed(node, Direction::Incoming)
        .find(|edge| {
            matches!(
                edge.weight(),
                palimpsest_sql::mir::MirEdgeKind::CteExpansion
            )
        })
        .map(|edge| edge.source());
    let target = target.ok_or_else(|| CompileError::Unknown(format!("cte {cte}")))?;

    let schema = state
        .node_schemas
        .get(&target)
        .cloned()
        .ok_or_else(|| CompileError::Unknown(format!("cte target schema {cte}")))?;

    let provenance = state
        .node_provenance
        .get(&target)
        .cloned()
        .unwrap_or_else(|| vec![None; schema.len()]);
    state.node_provenance.insert(node, provenance);
    state.node_schemas.insert(node, schema);
    state.recipes.insert(node, NodeRecipe::CteRef { target });
    Ok(())
}

// -----------------------------------------------------------------------------
// Graph helpers
// -----------------------------------------------------------------------------

fn single_input(graph: &MirGraph, node: NodeIndex) -> Result<NodeIndex, CompileError> {
    use petgraph::visit::EdgeRef;
    let mut inputs = graph
        .graph()
        .edges_directed(node, Direction::Incoming)
        .filter(|edge| matches!(edge.weight(), palimpsest_sql::mir::MirEdgeKind::Input(_)))
        .map(|edge| edge.source());
    let first = inputs
        .next()
        .ok_or_else(|| CompileError::Unknown("expected input edge".to_owned()))?;
    if inputs.next().is_some() {
        return Err(CompileError::Unsupported("multi-input node".to_owned()));
    }
    Ok(first)
}

// -----------------------------------------------------------------------------
// Install plan into a scope
// -----------------------------------------------------------------------------

/// Materialize a [`CompiledPlan`] inside the given timely `scope`.
///
/// The caller supplies `inputs` — one `VecCollection<G, Row, isize>`
/// per `TableId` the plan references. The function returns the final
/// `VecCollection<G, Row, isize>` whose rows are the query output;
/// the caller is responsible for arranging it into a trace.
pub fn install_plan<G>(
    plan: &CompiledPlan,
    scope: &mut G,
    inputs: &HashMap<TableId, VecCollection<G, Row, isize>>,
) -> VecCollection<G, Row, isize>
where
    G: timely::dataflow::Scope,
    G::Timestamp: Lattice + Ord,
{
    let mut cache: HashMap<NodeIndex, VecCollection<G, Row, isize>> = HashMap::new();
    install_recursive(plan, scope, inputs, plan.root, &mut cache)
}

fn install_recursive<G>(
    plan: &CompiledPlan,
    scope: &mut G,
    inputs: &HashMap<TableId, VecCollection<G, Row, isize>>,
    node: NodeIndex,
    cache: &mut HashMap<NodeIndex, VecCollection<G, Row, isize>>,
) -> VecCollection<G, Row, isize>
where
    G: timely::dataflow::Scope,
    G::Timestamp: Lattice + Ord,
{
    if let Some(c) = cache.get(&node) {
        return c.clone();
    }

    let recipe = plan
        .recipes
        .get(&node)
        .expect("compile_mir guarantees a recipe per node");
    let collection = match recipe {
        NodeRecipe::BaseTable { table } => inputs
            .get(table)
            .expect("install_plan caller wires every BaseTable input")
            .clone(),
        NodeRecipe::Filter { predicate } => {
            let input_node = single_input(&plan.graph, node).expect("compile_mir validated");
            let input = install_recursive(plan, scope, inputs, input_node, cache);
            let pred = Arc::clone(predicate);
            relational::filter(&input, move |row: &Row| pred(row))
        }
        NodeRecipe::Project { extract } => {
            let input_node = single_input(&plan.graph, node).expect("compile_mir validated");
            let input = install_recursive(plan, scope, inputs, input_node, cache);
            let ext = Arc::clone(extract);
            relational::project(&input, move |row: Row| ext(&row))
        }
        NodeRecipe::Aggregate {
            group_extract,
            value_extract,
            funcs,
        } => {
            let input_node = single_input(&plan.graph, node).expect("compile_mir validated");
            let input = install_recursive(plan, scope, inputs, input_node, cache);

            // Project Row → (group_key, value).
            let ge = Arc::clone(group_extract);
            let ve = Arc::clone(value_extract);
            let projected = relational::project(&input, move |row: Row| (ge(&row), ve(&row)));
            let funcs = funcs.clone();
            let aggregated = relational::aggregate_i64(&projected, funcs);

            // Project (group_key, Vec<AggregateValue>) → Row. The
            // group key keeps its original Datum type, so a Bool
            // group column emits a Bool first column and matches the
            // schema we advertised to clients.
            relational::project(
                &aggregated,
                |(group, aggs): (Datum, Vec<AggregateValue>)| {
                    let mut row: Row = SmallVec::with_capacity(1 + aggs.len());
                    row.push(group);
                    for av in aggs {
                        let datum = match av {
                            AggregateValue::Integer(v) => Datum::I64(saturating_i128_to_i64(v)),
                            AggregateValue::Average { sum, count } => {
                                let avg = if count == 0 {
                                    0.0
                                } else {
                                    sum as f64 / count as f64
                                };
                                Datum::F64(avg.to_bits())
                            }
                        };
                        row.push(datum);
                    }
                    row
                },
            )
        }
        NodeRecipe::TopK {
            sort_key_extract,
            direction,
            limit,
            offset,
        } => {
            let input_node = single_input(&plan.graph, node).expect("compile_mir validated");
            let input = install_recursive(plan, scope, inputs, input_node, cache);

            // Use the input's natural Ord — Row's lexicographic order
            // doesn't always match the desired key. Pre-project to
            // (sort_key, row) so TopK sorts by `sort_key`, then strip
            // the prefix once the slice is selected.
            let extract = Arc::clone(sort_key_extract);
            let with_key = relational::project(&input, move |row: Row| (extract(&row), row));
            let sliced = relational::topk(&with_key, *direction, *limit, *offset);
            relational::project(&sliced, |(_, row): (i64, Row)| row)
        }
        NodeRecipe::Join {
            kind,
            left_keys,
            right_keys,
            right_width,
        } => {
            let join_inputs = plan.graph.ordered_inputs(node);
            let (left_node, right_node) = (join_inputs[0], join_inputs[1]);
            let left = install_recursive(plan, scope, inputs, left_node, cache);
            let right = install_recursive(plan, scope, inputs, right_node, cache);

            let lk = left_keys.clone();
            let rk = right_keys.clone();
            let left_keyed = relational::project(&left, move |row: Row| (key_of(&row, &lk), row));
            let right_keyed = relational::project(&right, move |row: Row| (key_of(&row, &rk), row));
            // SQL equality: a NULL key never matches. Differential's
            // Rust equality would pair NULL with NULL, so strip
            // null-keyed rows from the right side — left rows then
            // simply find no partner (and still surface null-extended
            // / unmatched under Left / Anti).
            let right_keyed = relational::filter(&right_keyed, |(key, _): &(Vec<Datum>, Row)| {
                !key.iter().any(|d| matches!(d, Datum::Null))
            });

            match kind {
                JoinKind::Inner => relational::equi_join(
                    &left_keyed,
                    &right_keyed,
                    |_key, left_row: &Row, right_row: &Row| {
                        let mut out: Row =
                            SmallVec::with_capacity(left_row.len() + right_row.len());
                        out.extend(left_row.iter().cloned());
                        out.extend(right_row.iter().cloned());
                        out
                    },
                ),
                JoinKind::Left => {
                    let width = *right_width;
                    relational::left_join(
                        &left_keyed,
                        &right_keyed,
                        move |_key, left_row: &Row, right_row: Option<&Row>| {
                            let mut out: Row = SmallVec::with_capacity(left_row.len() + width);
                            out.extend(left_row.iter().cloned());
                            match right_row {
                                Some(row) => out.extend(row.iter().cloned()),
                                None => {
                                    out.extend(std::iter::repeat_n(Datum::Null, width));
                                }
                            }
                            out
                        },
                    )
                }
                JoinKind::Semi | JoinKind::Anti => {
                    // Deduplicate the right keys so a left row's
                    // multiplicity is preserved no matter how many
                    // right partners exist.
                    let right_keys_distinct = relational::distinct(&relational::project(
                        &right_keyed,
                        |(key, _row): (Vec<Datum>, Row)| key,
                    ));
                    let joined = if matches!(kind, JoinKind::Semi) {
                        left_keyed.semijoin(&right_keys_distinct)
                    } else {
                        left_keyed.antijoin(&right_keys_distinct)
                    };
                    relational::project(&joined, |(_key, row): (Vec<Datum>, Row)| row)
                }
            }
        }
        NodeRecipe::Distinct => {
            let input_node = single_input(&plan.graph, node).expect("compile_mir validated");
            let input = install_recursive(plan, scope, inputs, input_node, cache);
            relational::distinct(&input)
        }
        NodeRecipe::Union { all } => {
            let union_inputs = plan.graph.ordered_inputs(node);
            let (left_node, right_node) = (union_inputs[0], union_inputs[1]);
            let left = install_recursive(plan, scope, inputs, left_node, cache);
            let right = install_recursive(plan, scope, inputs, right_node, cache);
            if *all {
                relational::union(&left, &right)
            } else {
                relational::union_distinct(&left, &right)
            }
        }
        NodeRecipe::CteRef { target } => install_recursive(plan, scope, inputs, *target, cache),
    };

    cache.insert(node, collection.clone());
    collection
}

/// Join key: the named columns' datums, in key order. `Vec<Datum>`
/// rather than `Row` so the key satisfies the exchange bounds without
/// extra trait plumbing on the inline-storage row type.
fn key_of(row: &Row, indices: &[usize]) -> Vec<Datum> {
    indices
        .iter()
        .map(|&i| row.get(i).cloned().unwrap_or(Datum::Null))
        .collect()
}

fn saturating_i128_to_i64(v: i128) -> i64 {
    if v > i64::MAX as i128 {
        i64::MAX
    } else if v < i64::MIN as i128 {
        i64::MIN
    } else {
        v as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::Input;
    use palimpsest_sql::lower::parse_and_lower;

    fn posts_schema() -> ScalarSchema {
        ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("title".to_owned(), ColumnType::Text),
            ("published".to_owned(), ColumnType::Bool),
        ])
    }

    fn events_schema() -> ScalarSchema {
        ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("category_id".to_owned(), ColumnType::Int),
            ("value".to_owned(), ColumnType::Int),
        ])
    }

    fn lookup(table: &str) -> Option<(TableId, ScalarSchema)> {
        match table {
            "posts" => Some((TableId::new(1), posts_schema())),
            "events" => Some((TableId::new(2), events_schema())),
            _ => None,
        }
    }

    #[test]
    fn compile_simple_select() {
        let graph = parse_and_lower("SELECT id, title, published FROM posts").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(plan.inputs, vec![TableId::new(1)]);
        assert_eq!(plan.output_schema.len(), 3);
    }

    #[test]
    fn compile_filter() {
        let graph =
            parse_and_lower("SELECT id, title, published FROM posts WHERE published = true")
                .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        let recipes_include_filter = plan
            .recipes
            .values()
            .any(|r| matches!(r, NodeRecipe::Filter { .. }));
        assert!(recipes_include_filter);
    }

    #[test]
    fn compile_aggregate_with_cte() {
        let sql = "WITH per_category AS (
            SELECT category_id, COUNT(*) AS n, SUM(value) AS total
            FROM events
            GROUP BY category_id
        )
        SELECT category_id, n, total
        FROM per_category
        ORDER BY total DESC
        LIMIT 8";
        let graph = parse_and_lower(sql).unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(plan.inputs, vec![TableId::new(2)]);
        assert_eq!(plan.output_schema.len(), 3);
        let has_agg = plan
            .recipes
            .values()
            .any(|r| matches!(r, NodeRecipe::Aggregate { .. }));
        let has_topk = plan
            .recipes
            .values()
            .any(|r| matches!(r, NodeRecipe::TopK { .. }));
        assert!(has_agg, "aggregate recipe missing");
        assert!(has_topk, "topk recipe missing");
    }

    fn datum_row(values: Vec<Datum>) -> Row {
        values.into_iter().collect()
    }

    #[test]
    fn aggregate_preserves_bool_group_key_type() {
        // Regression: `GROUP BY <bool column>` used to coerce the
        // group key into an `i64`, then emit `Datum::I64` on output —
        // which produced a `schema/datum mismatch at column 0:
        // schema=Bool, datum=i64` decode failure on the wire.
        let sql = "SELECT published, COUNT(*) AS n
                   FROM posts
                   GROUP BY published";
        let graph = parse_and_lower(sql).unwrap();
        let posts_schema = ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("title".to_owned(), ColumnType::Text),
            ("published".to_owned(), ColumnType::Bool),
        ]);
        let plan = compile_mir(&graph, &|table: &str| match table {
            "posts" => Some((TableId::new(1), posts_schema.clone())),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            plan.output_schema.column_type("published"),
            Some(ColumnType::Bool)
        );
        assert_eq!(plan.output_schema.column_type("n"), Some(ColumnType::Int));

        // Drive the pipeline through timely and verify the emitted
        // rows actually carry `Datum::Bool` at column 0.
        let seed = vec![
            datum_row(vec![
                Datum::I64(1),
                Datum::Text(bytes::Bytes::from_static(b"a")),
                Datum::Bool(true),
            ]),
            datum_row(vec![
                Datum::I64(2),
                Datum::Text(bytes::Bytes::from_static(b"b")),
                Datum::Bool(true),
            ]),
            datum_row(vec![
                Datum::I64(3),
                Datum::Text(bytes::Bytes::from_static(b"c")),
                Datum::Bool(false),
            ]),
        ];

        timely::example(move |scope| {
            let (_, posts) = scope.new_collection_from(seed);
            let mut inputs: HashMap<TableId, VecCollection<_, Row, isize>> = HashMap::new();
            inputs.insert(TableId::new(1), posts);
            let output = install_plan(&plan, scope, &inputs);

            let expected = vec![
                datum_row(vec![Datum::Bool(true), Datum::I64(2)]),
                datum_row(vec![Datum::Bool(false), Datum::I64(1)]),
            ];
            let expected_coll = scope.new_collection_from(expected).1;
            output.assert_eq(&expected_coll);
        });
    }

    #[test]
    fn install_aggregate_pipeline_emits_grouped_rows() {
        let sql = "WITH per_category AS (
            SELECT category_id, COUNT(*) AS n, SUM(value) AS total
            FROM events
            GROUP BY category_id
        )
        SELECT category_id, n, total
        FROM per_category
        ORDER BY total DESC
        LIMIT 8";
        let graph = parse_and_lower(sql).unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();

        let seed: Vec<Row> = vec![
            datum_row(vec![Datum::I64(1), Datum::I64(7), Datum::I64(100)]),
            datum_row(vec![Datum::I64(2), Datum::I64(7), Datum::I64(50)]),
            datum_row(vec![Datum::I64(3), Datum::I64(9), Datum::I64(20)]),
            datum_row(vec![Datum::I64(4), Datum::I64(9), Datum::I64(20)]),
        ];

        let expected: Vec<Row> = vec![
            datum_row(vec![Datum::I64(7), Datum::I64(2), Datum::I64(150)]),
            datum_row(vec![Datum::I64(9), Datum::I64(2), Datum::I64(40)]),
        ];

        timely::example(move |scope| {
            let (_, posts) = scope.new_collection_from(Vec::<Row>::new());
            let (_, events) = scope.new_collection_from(seed);
            let mut inputs: HashMap<TableId, VecCollection<_, Row, isize>> = HashMap::new();
            inputs.insert(TableId::new(1), posts);
            inputs.insert(TableId::new(2), events);

            let output = install_plan(&plan, scope, &inputs);
            let expected = scope.new_collection_from(expected).1;
            output.assert_eq(&expected);
        });
    }

    // -------------------------------------------------------------------------
    // Join / set-op / wildcard / cast coverage
    // -------------------------------------------------------------------------

    const ARTICLES: TableId = TableId::new(10);
    const AUTHORS: TableId = TableId::new(11);
    const COMMENTS: TableId = TableId::new(12);

    fn relational_lookup(table: &str) -> Option<(TableId, ScalarSchema)> {
        match table {
            "articles" => Some((
                ARTICLES,
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("author_id".to_owned(), ColumnType::Int),
                ]),
            )),
            "authors" => Some((
                AUTHORS,
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("name".to_owned(), ColumnType::Text),
                ]),
            )),
            "comments" => Some((
                COMMENTS,
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("article_id".to_owned(), ColumnType::Int),
                ]),
            )),
            _ => None,
        }
    }

    fn text(s: &str) -> Datum {
        Datum::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }

    fn articles_rows() -> Vec<Row> {
        vec![
            datum_row(vec![Datum::I64(1), Datum::I64(42)]),
            datum_row(vec![Datum::I64(2), Datum::I64(7)]),
        ]
    }

    fn authors_rows() -> Vec<Row> {
        vec![datum_row(vec![Datum::I64(42), text("Ada")])]
    }

    fn run_plan(plan: &CompiledPlan, seeds: Vec<(TableId, Vec<Row>)>, expected: Vec<Row>) {
        let plan = plan.clone();
        timely::example(move |scope| {
            let mut inputs: HashMap<TableId, VecCollection<_, Row, isize>> = HashMap::new();
            for table in &plan.inputs {
                let rows = seeds
                    .iter()
                    .find(|(id, _)| id == table)
                    .map(|(_, rows)| rows.clone())
                    .unwrap_or_default();
                inputs.insert(*table, scope.new_collection_from(rows).1);
            }
            let output = install_plan(&plan, scope, &inputs);
            let expected = scope.new_collection_from(expected).1;
            output.assert_eq(&expected);
        });
    }

    #[test]
    fn inner_join_emits_matched_rows() {
        let graph = parse_and_lower(
            "SELECT articles.id, authors.name
             FROM articles JOIN authors ON articles.author_id = authors.id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        let mut inputs = plan.inputs.clone();
        inputs.sort();
        assert_eq!(inputs, vec![ARTICLES, AUTHORS]);
        assert_eq!(plan.output_schema.column_type("id"), Some(ColumnType::Int));
        assert_eq!(
            plan.output_schema.column_type("name"),
            Some(ColumnType::Text)
        );

        run_plan(
            &plan,
            vec![(ARTICLES, articles_rows()), (AUTHORS, authors_rows())],
            vec![datum_row(vec![Datum::I64(1), text("Ada")])],
        );
    }

    #[test]
    fn inner_join_resolves_swapped_on_operands() {
        // ON authors.id = articles.author_id — pair order opposite the
        // join's (left, right) input order.
        let graph = parse_and_lower(
            "SELECT articles.id, authors.name
             FROM articles JOIN authors ON authors.id = articles.author_id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        run_plan(
            &plan,
            vec![(ARTICLES, articles_rows()), (AUTHORS, authors_rows())],
            vec![datum_row(vec![Datum::I64(1), text("Ada")])],
        );
    }

    #[test]
    fn left_join_null_extends_unmatched_rows() {
        let graph = parse_and_lower(
            "SELECT articles.id, authors.name
             FROM articles LEFT JOIN authors ON articles.author_id = authors.id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        run_plan(
            &plan,
            vec![(ARTICLES, articles_rows()), (AUTHORS, authors_rows())],
            vec![
                datum_row(vec![Datum::I64(1), text("Ada")]),
                datum_row(vec![Datum::I64(2), Datum::Null]),
            ],
        );
    }

    #[test]
    fn exists_lowers_to_semi_join_and_emits_left_rows_once() {
        let graph = parse_and_lower(
            "SELECT id FROM articles
             WHERE EXISTS (SELECT 1 FROM comments WHERE comments.article_id = articles.id)",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        // Article 1 has two comments but the semi-join emits it once.
        let comments = vec![
            datum_row(vec![Datum::I64(10), Datum::I64(1)]),
            datum_row(vec![Datum::I64(11), Datum::I64(1)]),
        ];
        run_plan(
            &plan,
            vec![(ARTICLES, articles_rows()), (COMMENTS, comments)],
            vec![datum_row(vec![Datum::I64(1)])],
        );
    }

    #[test]
    fn not_exists_lowers_to_anti_join() {
        let graph = parse_and_lower(
            "SELECT id FROM articles
             WHERE NOT EXISTS (SELECT 1 FROM comments WHERE comments.article_id = articles.id)",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        let comments = vec![datum_row(vec![Datum::I64(10), Datum::I64(1)])];
        run_plan(
            &plan,
            vec![(ARTICLES, articles_rows()), (COMMENTS, comments)],
            vec![datum_row(vec![Datum::I64(2)])],
        );
    }

    #[test]
    fn null_join_keys_never_match() {
        let graph = parse_and_lower(
            "SELECT articles.id, authors.name
             FROM articles JOIN authors ON articles.author_id = authors.id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        let articles = vec![datum_row(vec![Datum::I64(3), Datum::Null])];
        let authors = vec![datum_row(vec![Datum::Null, text("ghost")])];
        run_plan(
            &plan,
            vec![(ARTICLES, articles), (AUTHORS, authors)],
            Vec::new(),
        );
    }

    #[test]
    fn union_all_keeps_duplicates_and_union_dedupes() {
        let seeds = || {
            vec![(
                ARTICLES,
                vec![
                    datum_row(vec![Datum::I64(1), Datum::I64(42)]),
                    datum_row(vec![Datum::I64(1), Datum::I64(42)]),
                ],
            )]
        };

        let all =
            parse_and_lower("SELECT id FROM articles UNION ALL SELECT id FROM articles").unwrap();
        let plan = compile_mir(&all, &relational_lookup).unwrap();
        run_plan(&plan, seeds(), vec![datum_row(vec![Datum::I64(1)]); 4]);

        let distinct =
            parse_and_lower("SELECT id FROM articles UNION SELECT id FROM articles").unwrap();
        let plan = compile_mir(&distinct, &relational_lookup).unwrap();
        run_plan(&plan, seeds(), vec![datum_row(vec![Datum::I64(1)])]);
    }

    #[test]
    fn select_distinct_dedupes_rows() {
        let graph = parse_and_lower("SELECT DISTINCT author_id FROM articles").unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        let articles = vec![
            datum_row(vec![Datum::I64(1), Datum::I64(42)]),
            datum_row(vec![Datum::I64(2), Datum::I64(42)]),
        ];
        run_plan(
            &plan,
            vec![(ARTICLES, articles)],
            vec![datum_row(vec![Datum::I64(42)])],
        );
    }

    #[test]
    fn select_star_projects_every_column() {
        let graph = parse_and_lower("SELECT * FROM posts WHERE published = true").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(plan.output_schema.len(), 3);
        assert_eq!(
            plan.output_schema
                .columns()
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "title", "published"],
        );

        let seed = vec![
            datum_row(vec![Datum::I64(1), text("a"), Datum::Bool(true)]),
            datum_row(vec![Datum::I64(2), text("b"), Datum::Bool(false)]),
        ];
        run_plan(
            &plan,
            vec![(TableId::new(1), seed)],
            vec![datum_row(vec![Datum::I64(1), text("a"), Datum::Bool(true)])],
        );
    }

    #[test]
    fn qualified_star_expands_one_join_side() {
        let graph = parse_and_lower(
            "SELECT articles.*, authors.name
             FROM articles JOIN authors ON articles.author_id = authors.id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        assert_eq!(
            plan.output_schema
                .columns()
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "author_id", "name"],
        );
        run_plan(
            &plan,
            vec![(ARTICLES, articles_rows()), (AUTHORS, authors_rows())],
            vec![datum_row(vec![Datum::I64(1), Datum::I64(42), text("Ada")])],
        );
    }

    #[test]
    fn casts_work_in_projections_and_predicates() {
        let graph = parse_and_lower("SELECT id::text FROM posts WHERE id::text = '1'").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        // Postgres names a simple cast after the inner column, typed
        // by the cast target.
        assert_eq!(plan.output_schema.column_type("id"), Some(ColumnType::Text));

        let seed = vec![
            datum_row(vec![Datum::I64(1), text("a"), Datum::Bool(true)]),
            datum_row(vec![Datum::I64(2), text("b"), Datum::Bool(true)]),
        ];
        run_plan(
            &plan,
            vec![(TableId::new(1), seed)],
            vec![datum_row(vec![text("1")])],
        );
    }

    #[test]
    fn spliced_filter_above_left_join_input_keeps_sides_straight() {
        // Emulates the permission rewriter: splice a Filter directly
        // above the join's *left* base table. `splice_above` removes
        // edges (recycling petgraph edge indices) and adds a node with
        // a higher index than either input — both of which used to
        // scramble join input ordering.
        let mut graph = parse_and_lower(
            "SELECT articles.id, authors.name
             FROM articles JOIN authors ON articles.author_id = authors.id",
        )
        .unwrap();
        let left_base = graph
            .base_table_indices()
            .into_iter()
            .find(|index| {
                matches!(
                    graph.node_kind(*index),
                    MirNodeKind::BaseTable { table, .. } if table == "articles"
                )
            })
            .expect("articles base table");
        graph.splice_above(
            left_base,
            MirNodeKind::Filter {
                predicate: "author_id = 42".to_owned(),
            },
        );

        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        // Article 2 (author 7) is dropped by the spliced filter, and
        // the join still treats articles as the left side.
        run_plan(
            &plan,
            vec![(ARTICLES, articles_rows()), (AUTHORS, authors_rows())],
            vec![datum_row(vec![Datum::I64(1), text("Ada")])],
        );
    }
}
