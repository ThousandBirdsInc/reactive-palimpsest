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
use crate::palimpsest::relational;
use crate::palimpsest::wal::Row;
use crate::{lattice::Lattice, AsCollection, VecCollection};

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
    /// Group-by aggregate over zero or more group columns. Group keys
    /// and aggregate inputs keep their original `Datum` types; each
    /// aggregate function reads its own value expression, typed at
    /// compile time (integer vs float sums, order-based min/max).
    Aggregate {
        /// Closure that reads the group-key datums out of the input
        /// row (empty vector for a global aggregate).
        group_extract: Arc<dyn Fn(&Row) -> Vec<Datum> + Send + Sync>,
        /// One value extractor per aggregate function, in projection
        /// order. `COUNT(*)` uses a constant non-null placeholder so
        /// every row counts.
        value_extracts: Vec<Arc<dyn Fn(&Row) -> Datum + Send + Sync>>,
        /// One entry per aggregate function in projection order.
        funcs: Vec<relational::DatumAggregate>,
        /// For global (no `GROUP BY`) aggregates: the row to emit when
        /// the input is empty — SQL still returns one row there
        /// (`COUNT` = 0, other aggregates NULL). `None` for grouped
        /// aggregates, which produce no rows for no groups.
        empty_default: Option<Row>,
    },
    /// Global TopK ordered by one or more typed sort keys.
    TopK {
        /// Closure that reads the sort-key datums out of each row.
        sort_extract: Arc<dyn Fn(&Row) -> Vec<Datum> + Send + Sync>,
        /// Per-key descending flags, aligned with the extractor.
        descending: Vec<bool>,
        /// Limit (max rows retained).
        limit: usize,
        /// Offset (rows skipped from the head of the sorted slice).
        offset: usize,
    },
    /// Postgres `DISTINCT ON`: first row per `on`-group, ranked by the
    /// sort keys (arbitrary-but-deterministic when no order was given).
    DistinctOn {
        /// Closure that reads the grouping datums out of each row.
        on_extract: Arc<dyn Fn(&Row) -> Vec<Datum> + Send + Sync>,
        /// Closure that reads the ranking datums out of each row.
        sort_extract: Arc<dyn Fn(&Row) -> Vec<Datum> + Send + Sync>,
        /// Per-key descending flags for the ranking keys.
        descending: Vec<bool>,
    },
    /// `EXCEPT [ALL]` / `INTERSECT [ALL]` over the node's two ordered
    /// inputs, with SQL bag semantics per quantifier.
    SetOp {
        /// `true` for INTERSECT, `false` for EXCEPT.
        intersect: bool,
        /// `true` for the `ALL` quantifier (bag counts), `false` for
        /// set semantics.
        all: bool,
    },
    /// `WITH RECURSIVE` fixpoint. Ordered inputs are the base term
    /// then the step term; the step subgraph reads the working table
    /// through a [`Self::RecursiveRef`]. `UNION` recursion iterates
    /// the step over the accumulated distinct set until it stops
    /// changing; `UNION ALL` recursion circulates per-iteration waves
    /// and accumulates them (terminating exactly when the recursion
    /// itself does, like Postgres).
    Fixpoint {
        /// CTE name the step's `RecursiveRef` binds against.
        cte: String,
        /// `true` for `UNION ALL` (bag accumulation) recursion.
        union_all: bool,
    },
    /// Reference to the enclosing fixpoint's working table. Resolved
    /// at install time from the iteration's variable binding.
    RecursiveRef {
        /// CTE name, matching the enclosing [`Self::Fixpoint`].
        cte: String,
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
    // Cycle check up front; the walk itself is a demand-driven DFS so
    // a `RecursiveRef` can pull its fixpoint's *base* subtree (its
    // schema source) before the topological position would reach it.
    petgraph::algo::toposort(graph.graph(), None).map_err(|_| CompileError::Cycle)?;

    let mut state = CompileState {
        node_schemas: HashMap::new(),
        node_provenance: HashMap::new(),
        recipes: HashMap::new(),
        inputs: Vec::new(),
        input_schemas: HashMap::new(),
    };

    for node in graph.graph().node_indices() {
        ensure_compiled(graph, node, tables, &mut state)?;
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

/// Compiles `node` after its inputs (dataflow and CTE-expansion
/// alike), memoized through `state.recipes`. Safe against repeated
/// visits; the acyclicity check in [`compile_mir`] rules out
/// non-termination.
fn ensure_compiled<L: TableSchemaLookup>(
    graph: &MirGraph,
    node: NodeIndex,
    tables: &L,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    if state.recipes.contains_key(&node) {
        return Ok(());
    }
    use petgraph::visit::EdgeRef;
    let dependencies: Vec<NodeIndex> = graph
        .graph()
        .edges_directed(node, Direction::Incoming)
        .map(|edge| edge.source())
        .collect();
    for dependency in dependencies {
        ensure_compiled(graph, dependency, tables, state)?;
    }
    compile_node(graph, node, tables, state)
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
        MirNodeKind::DistinctOn { on, order_by } => {
            compile_distinct_on(graph, node, on, order_by, state)
        }
        MirNodeKind::Union { quantifier } => compile_union(graph, node, *quantifier, state),
        MirNodeKind::Except { quantifier } => {
            compile_set_op(graph, node, false, *quantifier, state)
        }
        MirNodeKind::Intersect { quantifier } => {
            compile_set_op(graph, node, true, *quantifier, state)
        }
        MirNodeKind::Fixpoint { cte, union_all } => {
            compile_fixpoint(graph, node, cte, *union_all, state)
        }
        MirNodeKind::RecursiveRef { cte } => compile_recursive_ref(graph, node, cte, tables, state),
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

    let provenance = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);

    // Attach provenance so qualified references (`tickets.name`) bind
    // to the right occurrence when a join carries colliding names.
    let pred_schema = input_schema.clone().with_provenance(provenance.clone());
    let pred = compile_predicate(predicate, &pred_schema)?;
    let pred: Arc<dyn Fn(&Row) -> bool + Send + Sync> = Arc::from(pred);
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
    // Scalar projection expressions resolve qualified references
    // through provenance, like predicates do.
    let expr_schema = input_schema.clone().with_provenance(input_prov.clone());

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

        // `expr AS alias` — the lowering keeps aliased select items in
        // full so the computing expression survives. Resolve the
        // expression side first (so `title AS id` reads `title`, not
        // the input's `id`); an alias that names an input column
        // directly covers aggregate outputs, which are keyed by alias.
        if let Some((expr_text, alias)) = split_alias(entry) {
            if let Some(index) = resolve_entry_column(&input_schema, &input_prov, expr_text) {
                push_index(
                    index,
                    alias.to_owned(),
                    &mut outputs,
                    &mut output_pairs,
                    &mut output_prov,
                );
                continue;
            }
            if let Some(index) = input_schema.index_of(alias) {
                push_index(
                    index,
                    alias.to_owned(),
                    &mut outputs,
                    &mut output_pairs,
                    &mut output_prov,
                );
                continue;
            }
            let (scalar, ty) = compile_typed_scalar(expr_text, &expr_schema)?;
            outputs.push(OutputColumn::Scalar(Arc::from(scalar)));
            output_pairs.push((alias.to_owned(), ty));
            output_prov.push(None);
            continue;
        }

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

        // Aggregate outputs are named by their display text
        // (`count(*)`); the select item may differ only in case
        // (`COUNT(*)`), so try a case-insensitive match before
        // treating the entry as a fresh expression.
        if let Some(index) = input_schema
            .columns()
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(entry))
        {
            let name = input_schema.columns()[index].0.clone();
            push_index(
                index,
                name,
                &mut outputs,
                &mut output_pairs,
                &mut output_prov,
            );
            continue;
        }

        // Anything else is a scalar expression (cast, coalesce, ...).
        let (scalar, ty) = compile_typed_scalar(entry, &expr_schema)?;
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
    (is_bare_ident(relation) && is_bare_ident(bare)).then_some((relation, bare))
}

fn is_bare_ident(part: &str) -> bool {
    !part.is_empty()
        && part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '"')
}

/// Splits the lowering's `expr AS alias` projection form. The alias is
/// always a plain identifier, and ` AS ` cannot occur at the top level
/// of an expression, so the *last* occurrence is the separator; a
/// last-segment that isn't identifier-shaped (e.g. inside a string
/// literal) means the entry carries no alias.
fn split_alias(entry: &str) -> Option<(&str, &str)> {
    let (expr_text, alias) = entry.rsplit_once(" AS ")?;
    (is_bare_ident(alias) && !expr_text.trim().is_empty()).then_some((expr_text.trim(), alias))
}

/// Resolves a projection entry as an input column: exact name,
/// provenance-qualified `rel.column`, or a case-insensitive match
/// (aggregate outputs are named by their lowercased display text).
fn resolve_entry_column(
    schema: &ScalarSchema,
    provenance: &[Option<String>],
    entry: &str,
) -> Option<usize> {
    if let Some(index) = schema.index_of(entry) {
        return Some(index);
    }
    if let Some((relation, bare)) = split_qualified(entry) {
        let attributed = schema
            .columns()
            .iter()
            .enumerate()
            .position(|(i, (name, _))| {
                name == bare && provenance.get(i).and_then(Option::as_deref) == Some(relation)
            });
        if let Some(index) = attributed.or_else(|| schema.index_of(bare)) {
            return Some(index);
        }
    }
    schema
        .columns()
        .iter()
        .position(|(name, _)| name.eq_ignore_ascii_case(entry))
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
    let input_prov = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);

    // Resolve every group column (zero or more) to a row index; the
    // group key is the vector of those datums, preserving each
    // column's original SQL type end-to-end.
    let mut group_indices = Vec::with_capacity(group_by.len());
    let mut output_pairs = Vec::with_capacity(group_by.len() + aggs.len());
    for group_col in group_by {
        let index = resolve_column(&input_schema, &input_prov, group_col, true)
            .or_else(|| resolve_column(&input_schema, &input_prov, group_col, false))
            .ok_or_else(|| CompileError::Unknown(format!("group column {}", group_col.name)))?;
        let (name, ty) = input_schema.columns()[index].clone();
        group_indices.push(index);
        output_pairs.push((name, ty));
    }
    let group_extract: Arc<dyn Fn(&Row) -> Vec<Datum> + Send + Sync> = {
        let indices = group_indices;
        Arc::new(move |row: &Row| {
            indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Datum::Null))
                .collect()
        })
    };

    // Each aggregate reads its own value expression as a raw datum;
    // the evaluator variant is picked from the SQL function and the
    // argument's inferred type.
    let mut funcs = Vec::with_capacity(aggs.len());
    let mut value_extracts: Vec<Arc<dyn Fn(&Row) -> Datum + Send + Sync>> =
        Vec::with_capacity(aggs.len());
    for agg in aggs {
        let (distinct, arg) = parse_agg_call(agg);
        let (extractor, arg_type): (Arc<dyn Fn(&Row) -> Datum + Send + Sync>, ColumnType) =
            if arg == "*" {
                // COUNT(*): a constant non-null placeholder counts
                // every row.
                (Arc::new(|_row: &Row| Datum::Bool(true)), ColumnType::Bool)
            } else {
                let arg_schema = input_schema.clone().with_provenance(input_prov.clone());
                let (scalar, ty) = compile_typed_scalar(&arg, &arg_schema)?;
                (Arc::from(scalar), ty)
            };
        value_extracts.push(extractor);

        use relational::DatumAggregate;
        let (func, output_type) = match agg.function.to_ascii_lowercase().as_str() {
            "count" => (DatumAggregate::Count { distinct }, ColumnType::Int),
            "sum" => {
                if arg_type == ColumnType::Int {
                    (DatumAggregate::SumInt { distinct }, ColumnType::Int)
                } else {
                    (DatumAggregate::SumFloat { distinct }, ColumnType::Float)
                }
            }
            "avg" => (DatumAggregate::Avg { distinct }, ColumnType::Float),
            "min" => (DatumAggregate::Min, arg_type),
            "max" => (DatumAggregate::Max, arg_type),
            other => return Err(CompileError::UnsupportedAggregate(other.to_owned())),
        };
        funcs.push(func);

        let name = agg
            .alias
            .clone()
            .unwrap_or_else(|| aggregate_display_name(agg));
        output_pairs.push((name, output_type));
    }

    // A global aggregate (no GROUP BY) still returns one row over an
    // empty input: COUNT = 0, everything else NULL.
    let empty_default: Option<Row> = group_by.is_empty().then(|| {
        funcs
            .iter()
            .map(|func| match func {
                relational::DatumAggregate::Count { .. } => Datum::I64(0),
                _ => Datum::Null,
            })
            .collect()
    });

    let output_schema = ScalarSchema::from_pairs(output_pairs);

    state
        .node_provenance
        .insert(node, vec![None; output_schema.len()]);
    state.node_schemas.insert(node, output_schema);
    state.recipes.insert(
        node,
        NodeRecipe::Aggregate {
            group_extract,
            value_extracts,
            funcs,
            empty_default,
        },
    );
    Ok(())
}

/// Splits an [`AggExpr`] into its `DISTINCT` flag and value-argument
/// text. The lowering marks `agg(DISTINCT x)` by prepending a
/// `DISTINCT` sentinel to the argument list.
fn parse_agg_call(agg: &AggExpr) -> (bool, String) {
    let distinct = agg.args.first().is_some_and(|arg| arg == "DISTINCT");
    let arg = agg
        .args
        .get(usize::from(distinct))
        .map(|arg| arg.trim().to_owned())
        .unwrap_or_else(|| "*".to_owned());
    (distinct, arg)
}

/// Output-column name for an unaliased aggregate, matching how the
/// select item renders (`count(*)`, `max(created_at)`) so projections
/// referencing the aggregate by its display text resolve — modulo
/// case, which the projection resolver ignores.
fn aggregate_display_name(agg: &AggExpr) -> String {
    let distinct = agg.args.first().is_some_and(|arg| arg == "DISTINCT");
    let args = &agg.args[usize::from(distinct)..];
    if distinct {
        format!("{}(DISTINCT {})", agg.function, args.join(", "))
    } else {
        format!("{}({})", agg.function, args.join(", "))
    }
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

/// Compiles one key-expression string (an `ORDER BY` key or a
/// `DISTINCT ON` expression) into a datum extractor: plain and
/// qualified column references resolve by index (provenance-aware),
/// anything else compiles through the expression evaluator.
fn compile_key_extractor(
    entry: &str,
    schema: &ScalarSchema,
    provenance: &[Option<String>],
) -> Result<Arc<dyn Fn(&Row) -> Datum + Send + Sync>, CompileError> {
    let entry = entry.trim();
    let index = schema.index_of(entry).or_else(|| {
        split_qualified(entry).and_then(|(relation, bare)| {
            schema
                .columns()
                .iter()
                .enumerate()
                .position(|(i, (name, _))| {
                    name == bare && provenance.get(i).and_then(Option::as_deref) == Some(relation)
                })
                .or_else(|| schema.index_of(bare))
        })
    });
    if let Some(index) = index {
        return Ok(Arc::new(move |row: &Row| {
            row.get(index).cloned().unwrap_or(Datum::Null)
        }));
    }
    let expr_schema = schema.clone().with_provenance(provenance.to_vec());
    let scalar = crate::palimpsest::eval::compile_scalar(entry, &expr_schema)?;
    Ok(Arc::from(scalar))
}

/// Builds a `Vec<Datum>`-keyed extractor from per-key extractors.
fn combine_key_extractors(
    extractors: Vec<Arc<dyn Fn(&Row) -> Datum + Send + Sync>>,
) -> Arc<dyn Fn(&Row) -> Vec<Datum> + Send + Sync> {
    Arc::new(move |row: &Row| extractors.iter().map(|extract| extract(row)).collect())
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
    let provenance = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);

    let mut extractors = Vec::with_capacity(order_by.len());
    let mut descending = Vec::with_capacity(order_by.len());
    for key in order_by {
        extractors.push(compile_key_extractor(
            &key.expression,
            &input_schema,
            &provenance,
        )?);
        descending.push(key.descending);
    }
    let sort_extract = combine_key_extractors(extractors);

    state.node_provenance.insert(node, provenance);
    state.node_schemas.insert(node, input_schema);
    state.recipes.insert(
        node,
        NodeRecipe::TopK {
            sort_extract,
            descending,
            limit,
            offset,
        },
    );
    Ok(())
}

fn compile_distinct_on(
    graph: &MirGraph,
    node: NodeIndex,
    on: &[String],
    order_by: &[OrderKey],
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let input_node = single_input(graph, node)?;
    let input_schema = state
        .node_schemas
        .get(&input_node)
        .ok_or_else(|| CompileError::Unknown("distinct-on input schema".to_owned()))?
        .clone();
    let provenance = state
        .node_provenance
        .get(&input_node)
        .cloned()
        .unwrap_or_else(|| vec![None; input_schema.len()]);

    let mut on_extractors = Vec::with_capacity(on.len());
    for expression in on {
        on_extractors.push(compile_key_extractor(
            expression,
            &input_schema,
            &provenance,
        )?);
    }
    let mut sort_extractors = Vec::with_capacity(order_by.len());
    let mut descending = Vec::with_capacity(order_by.len());
    for key in order_by {
        sort_extractors.push(compile_key_extractor(
            &key.expression,
            &input_schema,
            &provenance,
        )?);
        descending.push(key.descending);
    }

    state.node_provenance.insert(node, provenance);
    state.node_schemas.insert(node, input_schema);
    state.recipes.insert(
        node,
        NodeRecipe::DistinctOn {
            on_extract: combine_key_extractors(on_extractors),
            sort_extract: combine_key_extractors(sort_extractors),
            descending,
        },
    );
    Ok(())
}

fn compile_set_op(
    graph: &MirGraph,
    node: NodeIndex,
    intersect: bool,
    quantifier: SetQuantifierKind,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let inputs = graph.ordered_inputs(node);
    let [left, right] = inputs.as_slice() else {
        return Err(CompileError::Unsupported(format!(
            "set operation with {} inputs",
            inputs.len()
        )));
    };
    let left_schema = state
        .node_schemas
        .get(left)
        .ok_or_else(|| CompileError::Unknown("set-op left schema".to_owned()))?
        .clone();
    let right_schema = state
        .node_schemas
        .get(right)
        .ok_or_else(|| CompileError::Unknown("set-op right schema".to_owned()))?;
    if left_schema.len() != right_schema.len() {
        return Err(CompileError::Unsupported(format!(
            "set-operation branches with {} vs {} columns",
            left_schema.len(),
            right_schema.len()
        )));
    }

    state
        .node_provenance
        .insert(node, vec![None; left_schema.len()]);
    state.node_schemas.insert(node, left_schema);
    state.recipes.insert(
        node,
        NodeRecipe::SetOp {
            intersect,
            all: quantifier == SetQuantifierKind::All,
        },
    );
    Ok(())
}

fn compile_fixpoint(
    graph: &MirGraph,
    node: NodeIndex,
    cte: &str,
    union_all: bool,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    let inputs = graph.ordered_inputs(node);
    let [base, step] = inputs.as_slice() else {
        return Err(CompileError::Unsupported(format!(
            "fixpoint with {} inputs",
            inputs.len()
        )));
    };

    // The installer runs the step term inside one iteration scope and
    // cannot open another one within it (that would recurse the scope
    // type without bound). Fixpoints nested in the step are fine when
    // they are *loop-invariant* — earlier, fully-lowered recursive
    // CTEs expanded here — because the installer hoists them outside
    // the iteration. What must not appear is a reference to this
    // fixpoint's working table from inside a nested fixpoint (the
    // hoisted computation cannot see it), or a reference to a
    // recursion that isn't in scope. The frontend's
    // single-self-reference rule already excludes both; this walk is
    // a defensive check for hand-built graphs.
    let (top_level, nested_roots) = fixpoint_step_partition(graph, *step);
    for index in &top_level {
        if let MirNodeKind::RecursiveRef { cte: name } = graph.node_kind(*index) {
            if name != cte {
                return Err(CompileError::Unsupported(format!(
                    "recursive CTE {name} referenced outside its fixpoint"
                )));
            }
        }
    }
    let mut pending = nested_roots;
    while let Some(nested) = pending.pop() {
        let (nested_nodes, deeper) = fixpoint_step_partition(graph, nested);
        pending.extend(deeper);
        for index in nested_nodes {
            if matches!(
                graph.node_kind(index),
                MirNodeKind::RecursiveRef { cte: name } if name == cte
            ) {
                return Err(CompileError::Unsupported(format!(
                    "nested recursive CTE references enclosing recursion {cte}"
                )));
            }
        }
    }

    let base_schema = state
        .node_schemas
        .get(base)
        .ok_or_else(|| CompileError::Unknown("fixpoint base schema".to_owned()))?
        .clone();

    state
        .node_provenance
        .insert(node, vec![None; base_schema.len()]);
    state.node_schemas.insert(node, base_schema);
    state.recipes.insert(
        node,
        NodeRecipe::Fixpoint {
            cte: cte.to_owned(),
            union_all,
        },
    );
    Ok(())
}

/// Partitions the subtree feeding `start` (inclusive) into the nodes
/// reachable without crossing a nested `Fixpoint`, and the nested
/// `Fixpoint` roots encountered (which are not traversed). `start`
/// itself is always traversed, so passing a `Fixpoint` walks *its*
/// base and step subtrees.
fn fixpoint_step_partition(graph: &MirGraph, start: NodeIndex) -> (Vec<NodeIndex>, Vec<NodeIndex>) {
    use petgraph::visit::EdgeRef;
    let mut top_level = Vec::new();
    let mut nested = Vec::new();
    let mut stack = vec![start];
    let mut visited = std::collections::BTreeSet::new();
    while let Some(current) = stack.pop() {
        if !visited.insert(current) {
            continue;
        }
        if current != start && matches!(graph.node_kind(current), MirNodeKind::Fixpoint { .. }) {
            nested.push(current);
            continue;
        }
        top_level.push(current);
        stack.extend(
            graph
                .graph()
                .edges_directed(current, Direction::Incoming)
                .map(|edge| edge.source()),
        );
    }
    (top_level, nested)
}

fn compile_recursive_ref<L: TableSchemaLookup>(
    graph: &MirGraph,
    node: NodeIndex,
    cte: &str,
    tables: &L,
    state: &mut CompileState,
) -> Result<(), CompileError> {
    // The working table's schema is the fixpoint's base-term schema.
    // The base subtree is disjoint from this node (the frontend
    // rejects self-references in the base term), so compiling it on
    // demand cannot recurse back here.
    let fixpoint = graph
        .graph()
        .node_indices()
        .find(|index| {
            matches!(
                graph.node_kind(*index),
                MirNodeKind::Fixpoint { cte: name, .. } if name == cte
            )
        })
        .ok_or_else(|| CompileError::Unknown(format!("fixpoint for recursive CTE {cte}")))?;
    let base = *graph
        .ordered_inputs(fixpoint)
        .first()
        .ok_or_else(|| CompileError::Unknown(format!("fixpoint base for {cte}")))?;
    ensure_compiled(graph, base, tables, state)?;
    let schema = state
        .node_schemas
        .get(&base)
        .cloned()
        .ok_or_else(|| CompileError::Unknown(format!("recursive CTE schema {cte}")))?;

    // The working table is a relation named after its CTE, so
    // qualified references (`reach.id`) resolve against it.
    state
        .node_provenance
        .insert(node, vec![Some(cte.to_owned()); schema.len()]);
    state.node_schemas.insert(node, schema);
    state.recipes.insert(
        node,
        NodeRecipe::RecursiveRef {
            cte: cte.to_owned(),
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

    // A CTE reference introduces a fresh relation named after the CTE
    // — `SELECT r.col FROM r` qualifies against `r`, not against the
    // base tables inside it (which are out of scope there, as in SQL).
    state
        .node_provenance
        .insert(node, vec![Some(cte.to_owned()); schema.len()]);
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
        NodeRecipe::CteRef { target } => install_recursive(plan, scope, inputs, *target, cache),
        NodeRecipe::RecursiveRef { cte } => {
            unreachable!("RecursiveRef {cte} outside its fixpoint's step term")
        }
        NodeRecipe::Fixpoint { cte, union_all } => {
            let fix_inputs = plan.graph.ordered_inputs(node);
            let (base_node, step_node) = (fix_inputs[0], fix_inputs[1]);
            let base = install_recursive(plan, scope, inputs, base_node, cache);

            // Nested fixpoints in the step term are loop-invariant
            // (earlier recursive CTEs expanded here) — install them in
            // *this* scope and enter their results into the iteration,
            // pre-seeded into the step installer's cache.
            let (_, nested_roots) = fixpoint_step_partition(&plan.graph, step_node);
            let hoisted: Vec<(NodeIndex, VecCollection<G, Row, isize>)> = nested_roots
                .into_iter()
                .map(|nested| {
                    (
                        nested,
                        install_recursive(plan, scope, inputs, nested, cache),
                    )
                })
                .collect();

            if *union_all {
                // UNION ALL recursion: circulate per-iteration *waves*
                // (W₀ = base, Wᵢ₊₁ = step(Wᵢ)) and accumulate them
                // (A = base + ΣΔ step-output). Converges exactly when
                // the recursion itself terminates — a cyclic
                // recursion runs forever, as it would in Postgres.
                use crate::operators::iterate::Variable;
                use timely::order::Product;
                scope.iterative::<u64, _, _>(|child| {
                    let entered: HashMap<TableId, VecCollection<_, Row, isize>> = inputs
                        .iter()
                        .map(|(table, collection)| (*table, collection.enter(child)))
                        .collect();
                    let base_in = base.enter(child);
                    let summary = Product::new(Default::default(), 1);
                    let wave = Variable::new_from(base_in.clone(), summary.clone());
                    let mut step_cache: HashMap<NodeIndex, VecCollection<_, Row, isize>> = hoisted
                        .iter()
                        .map(|(nested, collection)| (*nested, collection.enter(child)))
                        .collect();
                    let step_out = install_step(
                        plan,
                        child,
                        &entered,
                        step_node,
                        &mut step_cache,
                        (cte, &wave),
                    );
                    let accumulator = Variable::new_from(base_in, summary);
                    let total = accumulator.concat(&step_out).consolidate();
                    let result = accumulator.set(&total);
                    wave.set(&step_out.consolidate());
                    result.leave()
                })
            } else {
                // UNION-distinct recursion: iterate the step over the
                // working table, re-adding the base each round, until
                // the distinct set stops changing. The closing
                // `distinct` consolidates, which is also what
                // guarantees the loop's differences dissipate.
                use crate::operators::iterate::Iterate;
                base.iterate(|working| {
                    let mut child = working.scope();
                    let entered: HashMap<TableId, VecCollection<_, Row, isize>> = inputs
                        .iter()
                        .map(|(table, collection)| (*table, collection.enter(&child)))
                        .collect();
                    let base_in = base.enter(&child);
                    let mut step_cache: HashMap<NodeIndex, VecCollection<_, Row, isize>> = hoisted
                        .iter()
                        .map(|(nested, collection)| (*nested, collection.enter(&child)))
                        .collect();
                    let step_out = install_step(
                        plan,
                        &mut child,
                        &entered,
                        step_node,
                        &mut step_cache,
                        (cte, working),
                    );
                    relational::distinct(&relational::union(&base_in, &step_out))
                })
            }
        }
        dual @ (NodeRecipe::Join { .. } | NodeRecipe::Union { .. } | NodeRecipe::SetOp { .. }) => {
            let op_inputs = plan.graph.ordered_inputs(node);
            let installed: Vec<_> = op_inputs
                .into_iter()
                .map(|input| install_recursive(plan, scope, inputs, input, cache))
                .collect();
            install_op(dual, &installed, scope)
        }
        unary => {
            let input_node = single_input(&plan.graph, node).expect("compile_mir validated");
            let installed = install_recursive(plan, scope, inputs, input_node, cache);
            install_op(unary, &[installed], scope)
        }
    };

    cache.insert(node, collection.clone());
    collection
}

/// Installer used *inside* a fixpoint's iteration scope. Identical to
/// [`install_recursive`] except that `RecursiveRef` resolves to the
/// iteration's working collection and `Fixpoint` is unreachable
/// (compile rejects nested recursions) — which is what keeps the
/// iterative scope type from recursing without bound.
fn install_step<G>(
    plan: &CompiledPlan,
    scope: &mut G,
    inputs: &HashMap<TableId, VecCollection<G, Row, isize>>,
    node: NodeIndex,
    cache: &mut HashMap<NodeIndex, VecCollection<G, Row, isize>>,
    recursive: (&str, &VecCollection<G, Row, isize>),
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
        NodeRecipe::CteRef { target } => {
            install_step(plan, scope, inputs, *target, cache, recursive)
        }
        NodeRecipe::RecursiveRef { cte } => {
            debug_assert_eq!(cte, recursive.0, "compile_fixpoint validated the binding");
            recursive.1.clone()
        }
        NodeRecipe::Fixpoint { .. } => {
            unreachable!("compile_fixpoint rejects nested recursive CTEs")
        }
        dual @ (NodeRecipe::Join { .. } | NodeRecipe::Union { .. } | NodeRecipe::SetOp { .. }) => {
            let op_inputs = plan.graph.ordered_inputs(node);
            let installed: Vec<_> = op_inputs
                .into_iter()
                .map(|input| install_step(plan, scope, inputs, input, cache, recursive))
                .collect();
            install_op(dual, &installed, scope)
        }
        unary => {
            let input_node = single_input(&plan.graph, node).expect("compile_mir validated");
            let installed = install_step(plan, scope, inputs, input_node, cache, recursive);
            install_op(unary, &[installed], scope)
        }
    };

    cache.insert(node, collection.clone());
    collection
}

/// Ordering over `(sort_keys, row)` pairs: componentwise SQL datum
/// comparison with per-key direction, tie-broken by the row's natural
/// order for determinism.
fn sort_key_comparator(
    descending: Vec<bool>,
) -> impl Fn(&(Vec<Datum>, Row), &(Vec<Datum>, Row)) -> std::cmp::Ordering {
    use crate::palimpsest::eval::compare_datums_sort;
    move |a, b| {
        for (index, desc) in descending.iter().enumerate() {
            let left = a.0.get(index).unwrap_or(&Datum::Null);
            let right = b.0.get(index).unwrap_or(&Datum::Null);
            let ordering = compare_datums_sort(left, right);
            let ordering = if *desc { ordering.reverse() } else { ordering };
            if ordering != std::cmp::Ordering::Equal {
                return ordering;
            }
        }
        a.1.cmp(&b.1)
    }
}

/// Wires one non-source recipe over its already-installed inputs.
/// Shared between the outer installer and the fixpoint-step installer.
#[allow(clippy::too_many_lines)]
fn install_op<G>(
    recipe: &NodeRecipe,
    ins: &[VecCollection<G, Row, isize>],
    scope: &mut G,
) -> VecCollection<G, Row, isize>
where
    G: timely::dataflow::Scope,
    G::Timestamp: Lattice + Ord,
{
    match recipe {
        NodeRecipe::Filter { predicate } => {
            let pred = Arc::clone(predicate);
            relational::filter(&ins[0], move |row: &Row| pred(row))
        }
        NodeRecipe::Project { extract } => {
            let ext = Arc::clone(extract);
            relational::project(&ins[0], move |row: Row| ext(&row))
        }
        NodeRecipe::Aggregate {
            group_extract,
            value_extracts,
            funcs,
            empty_default,
        } => {
            // Project Row → (group_keys, per-aggregate value datums).
            let ge = Arc::clone(group_extract);
            let ves = value_extracts.clone();
            let keyed = relational::project(&ins[0], move |row: Row| {
                let values: Vec<Datum> = ves.iter().map(|ve| ve(&row)).collect();
                (ge(&row), values)
            });
            let aggregated = relational::aggregate_datums(&keyed, funcs.clone());

            // Project (group_keys, aggregate datums) → Row. Group keys
            // and aggregate results keep their Datum types, so the row
            // matches the schema advertised to clients.
            let result =
                relational::project(&aggregated, |(group, aggs): (Vec<Datum>, Vec<Datum>)| {
                    let mut row: Row = SmallVec::with_capacity(group.len() + aggs.len());
                    row.extend(group);
                    row.extend(aggs);
                    row
                });

            // A global aggregate still returns one row over an empty
            // input. Reduce emits nothing for absent groups, so emit
            // the default row exactly while the (single, unit) group
            // key is absent from the input.
            if let Some(default_row) = empty_default {
                use timely::dataflow::operators::ToStream;
                let default_row = default_row.clone();
                let present = relational::distinct(&relational::project(
                    &keyed,
                    |(key, _values): (Vec<Datum>, Vec<Datum>)| key,
                ));
                let unit: VecCollection<G, Vec<Datum>, isize> = vec![(
                    Vec::new(),
                    <G::Timestamp as timely::progress::Timestamp>::minimum(),
                    1_isize,
                )]
                .to_stream(scope)
                .as_collection();
                let missing =
                    relational::project(&unit, |key: Vec<Datum>| (key, ())).antijoin(&present);
                result.concat(&relational::project(
                    &missing,
                    move |(_key, ()): (Vec<Datum>, ())| default_row.clone(),
                ))
            } else {
                result
            }
        }
        NodeRecipe::TopK {
            sort_extract,
            descending,
            limit,
            offset,
        } => {
            // Pre-project to (sort_keys, row); the comparator orders
            // by the keys, then the slice is selected and the prefix
            // stripped.
            let extract = Arc::clone(sort_extract);
            let with_key = relational::project(&ins[0], move |row: Row| (extract(&row), row));
            let sliced = relational::topk_by(
                &with_key,
                sort_key_comparator(descending.clone()),
                *limit,
                *offset,
            );
            relational::project(&sliced, |(_, row): (Vec<Datum>, Row)| row)
        }
        NodeRecipe::DistinctOn {
            on_extract,
            sort_extract,
            descending,
        } => {
            let on = Arc::clone(on_extract);
            let sort = Arc::clone(sort_extract);
            let keyed = relational::project(&ins[0], move |row: Row| (on(&row), (sort(&row), row)));
            let picked =
                relational::distinct_on_first(&keyed, sort_key_comparator(descending.clone()));
            relational::project(&picked, |(_sort, row): (Vec<Datum>, Row)| row)
        }
        NodeRecipe::Join {
            kind,
            left_keys,
            right_keys,
            right_width,
        } => {
            let lk = left_keys.clone();
            let rk = right_keys.clone();
            let left_keyed = relational::project(&ins[0], move |row: Row| (key_of(&row, &lk), row));
            let right_keyed =
                relational::project(&ins[1], move |row: Row| (key_of(&row, &rk), row));
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
        NodeRecipe::Distinct => relational::distinct(&ins[0]),
        NodeRecipe::Union { all } => {
            if *all {
                relational::union(&ins[0], &ins[1])
            } else {
                relational::union_distinct(&ins[0], &ins[1])
            }
        }
        NodeRecipe::SetOp { intersect, all } => {
            let combine: fn(isize, isize) -> isize = match (intersect, all) {
                // EXCEPT: distinct left rows with no right occurrence.
                (false, false) => |l, r| isize::from(l > 0 && r <= 0),
                // EXCEPT ALL: bag difference.
                (false, true) => |l, r| (l - r).max(0),
                // INTERSECT: distinct rows present on both sides.
                (true, false) => |l, r| isize::from(l > 0 && r > 0),
                // INTERSECT ALL: bag minimum.
                (true, true) => |l, r| l.min(r).max(0),
            };
            relational::bag_set_op(&ins[0], &ins[1], combine)
        }
        NodeRecipe::BaseTable { .. }
        | NodeRecipe::CteRef { .. }
        | NodeRecipe::Fixpoint { .. }
        | NodeRecipe::RecursiveRef { .. } => {
            unreachable!("source recipes are handled by the install drivers")
        }
    }
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
    const EDGES: TableId = TableId::new(13);
    const MEASUREMENTS: TableId = TableId::new(14);

    fn relational_lookup(table: &str) -> Option<(TableId, ScalarSchema)> {
        match table {
            "edges" => Some((
                EDGES,
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("parent".to_owned(), ColumnType::Int),
                ]),
            )),
            "measurements" => Some((
                MEASUREMENTS,
                ScalarSchema::from_pairs([
                    ("id".to_owned(), ColumnType::Int),
                    ("reading".to_owned(), ColumnType::Float),
                    ("label".to_owned(), ColumnType::Text),
                ]),
            )),
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

    fn events_seed() -> Vec<Row> {
        vec![
            datum_row(vec![Datum::I64(1), Datum::I64(7), Datum::I64(10)]),
            datum_row(vec![Datum::I64(2), Datum::I64(7), Datum::I64(10)]),
            datum_row(vec![Datum::I64(3), Datum::I64(7), Datum::I64(20)]),
            datum_row(vec![Datum::I64(4), Datum::I64(9), Datum::I64(5)]),
        ]
    }

    #[test]
    fn distinct_on_keeps_ranked_row_per_group() {
        let graph = parse_and_lower(
            "SELECT DISTINCT ON (author_id) id, author_id
             FROM articles
             ORDER BY author_id, id DESC",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        let articles = vec![
            datum_row(vec![Datum::I64(1), Datum::I64(42)]),
            datum_row(vec![Datum::I64(2), Datum::I64(42)]),
            datum_row(vec![Datum::I64(3), Datum::I64(7)]),
        ];
        // Per author, the highest id survives.
        run_plan(
            &plan,
            vec![(ARTICLES, articles)],
            vec![
                datum_row(vec![Datum::I64(3), Datum::I64(7)]),
                datum_row(vec![Datum::I64(2), Datum::I64(42)]),
            ],
        );
    }

    #[test]
    fn except_and_intersect_follow_bag_semantics() {
        let seeds = || {
            vec![
                (
                    ARTICLES,
                    vec![
                        datum_row(vec![Datum::I64(1), Datum::I64(42)]),
                        datum_row(vec![Datum::I64(1), Datum::I64(42)]),
                        datum_row(vec![Datum::I64(2), Datum::I64(7)]),
                    ],
                ),
                (
                    COMMENTS,
                    vec![datum_row(vec![Datum::I64(10), Datum::I64(1)])],
                ),
            ]
        };
        let compile = |sql: &str| compile_mir(&parse_and_lower(sql).unwrap(), &relational_lookup);

        // EXCEPT: distinct left rows absent from the right. Article
        // ids {1, 1, 2} minus comment article_ids {1} → {2}.
        let plan =
            compile("SELECT id FROM articles EXCEPT SELECT article_id FROM comments").unwrap();
        run_plan(&plan, seeds(), vec![datum_row(vec![Datum::I64(2)])]);

        // EXCEPT ALL: bag difference keeps the surviving duplicate.
        let plan =
            compile("SELECT id FROM articles EXCEPT ALL SELECT article_id FROM comments").unwrap();
        run_plan(
            &plan,
            seeds(),
            vec![
                datum_row(vec![Datum::I64(1)]),
                datum_row(vec![Datum::I64(2)]),
            ],
        );

        // INTERSECT: distinct rows on both sides.
        let plan =
            compile("SELECT id FROM articles INTERSECT SELECT article_id FROM comments").unwrap();
        run_plan(&plan, seeds(), vec![datum_row(vec![Datum::I64(1)])]);

        // INTERSECT ALL: bag minimum (1 occurrence on the right).
        let plan = compile("SELECT id FROM articles INTERSECT ALL SELECT article_id FROM comments")
            .unwrap();
        run_plan(&plan, seeds(), vec![datum_row(vec![Datum::I64(1)])]);
    }

    #[test]
    fn recursive_cte_reaches_fixpoint() {
        let graph = parse_and_lower(
            "WITH RECURSIVE reach AS (
                SELECT id, parent FROM edges WHERE parent = 1
                UNION
                SELECT edges.id, edges.parent
                FROM edges JOIN reach ON edges.parent = reach.id
             )
             SELECT id FROM reach",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        let edges = vec![
            datum_row(vec![Datum::I64(2), Datum::I64(1)]),
            datum_row(vec![Datum::I64(3), Datum::I64(2)]),
            datum_row(vec![Datum::I64(4), Datum::I64(3)]),
            datum_row(vec![Datum::I64(9), Datum::I64(8)]),
        ];
        run_plan(
            &plan,
            vec![(EDGES, edges)],
            vec![
                datum_row(vec![Datum::I64(2)]),
                datum_row(vec![Datum::I64(3)]),
                datum_row(vec![Datum::I64(4)]),
            ],
        );
    }

    #[test]
    fn recursive_union_all_accumulates_per_iteration_waves() {
        let graph = parse_and_lower(
            "WITH RECURSIVE reach AS (
                SELECT id, parent FROM edges WHERE parent = 1
                UNION ALL
                SELECT edges.id, edges.parent
                FROM edges JOIN reach ON edges.parent = reach.id
             )
             SELECT id FROM reach",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        // Two roots point at 1, and node 3 hangs off node 2 — the
        // second wave re-derives it once per parent occurrence, and
        // UNION ALL keeps every derivation.
        let edges = vec![
            datum_row(vec![Datum::I64(2), Datum::I64(1)]),
            datum_row(vec![Datum::I64(3), Datum::I64(2)]),
            datum_row(vec![Datum::I64(9), Datum::I64(8)]),
        ];
        run_plan(
            &plan,
            vec![(EDGES, edges)],
            vec![
                datum_row(vec![Datum::I64(2)]),
                datum_row(vec![Datum::I64(3)]),
            ],
        );
    }

    #[test]
    fn multi_column_group_by_keys_groups_by_all_columns() {
        let graph = parse_and_lower(
            "SELECT category_id, value, count(*) AS n
             FROM events
             GROUP BY category_id, value",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![
                datum_row(vec![Datum::I64(7), Datum::I64(10), Datum::I64(2)]),
                datum_row(vec![Datum::I64(7), Datum::I64(20), Datum::I64(1)]),
                datum_row(vec![Datum::I64(9), Datum::I64(5), Datum::I64(1)]),
            ],
        );
    }

    #[test]
    fn scalar_aggregate_emits_one_row_even_for_empty_input() {
        let graph =
            parse_and_lower("SELECT count(*) AS n, sum(value) AS total FROM events").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();

        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![datum_row(vec![Datum::I64(4), Datum::I64(45)])],
        );

        // SQL: aggregates over an empty input still return one row —
        // COUNT is 0 and SUM is NULL.
        run_plan(
            &plan,
            vec![(TableId::new(2), Vec::new())],
            vec![datum_row(vec![Datum::I64(0), Datum::Null])],
        );
    }

    #[test]
    fn aggregates_read_their_own_value_columns() {
        let graph = parse_and_lower(
            "SELECT category_id, sum(value) AS total, min(id) AS first_id
             FROM events
             GROUP BY category_id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![
                datum_row(vec![Datum::I64(7), Datum::I64(40), Datum::I64(1)]),
                datum_row(vec![Datum::I64(9), Datum::I64(5), Datum::I64(4)]),
            ],
        );
    }

    #[test]
    fn count_distinct_counts_distinct_values() {
        let graph = parse_and_lower(
            "SELECT category_id, count(DISTINCT value) AS variants
             FROM events
             GROUP BY category_id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![
                datum_row(vec![Datum::I64(7), Datum::I64(2)]),
                datum_row(vec![Datum::I64(9), Datum::I64(1)]),
            ],
        );
    }

    #[test]
    fn unaliased_aggregates_resolve_in_the_projection() {
        let graph = parse_and_lower(
            "SELECT category_id, COUNT(*)
             FROM events
             GROUP BY category_id",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(
            plan.output_schema.column_type("count(*)"),
            Some(ColumnType::Int)
        );
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![
                datum_row(vec![Datum::I64(7), Datum::I64(3)]),
                datum_row(vec![Datum::I64(9), Datum::I64(1)]),
            ],
        );
    }

    #[test]
    fn having_filters_aggregate_groups() {
        // Aliased aggregate referenced by HAVING.
        let graph = parse_and_lower(
            "SELECT category_id, count(*) AS n
             FROM events
             GROUP BY category_id
             HAVING count(*) > 1",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![datum_row(vec![Datum::I64(7), Datum::I64(3)])],
        );

        // Aggregate that appears only in HAVING — computed as a hidden
        // column and dropped by the projection.
        let graph = parse_and_lower(
            "SELECT category_id
             FROM events
             GROUP BY category_id
             HAVING sum(value) > 25",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(plan.output_schema.len(), 1);
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![datum_row(vec![Datum::I64(7)])],
        );
    }

    fn float(value: f64) -> Datum {
        Datum::F64(value.to_bits())
    }

    #[test]
    fn aggregates_handle_float_and_text_inputs() {
        let graph = parse_and_lower(
            "SELECT min(label) AS first_label, max(reading) AS peak,
                    sum(reading) AS total, avg(reading) AS mean
             FROM measurements",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup).unwrap();
        assert_eq!(
            plan.output_schema.column_type("first_label"),
            Some(ColumnType::Text)
        );
        assert_eq!(
            plan.output_schema.column_type("peak"),
            Some(ColumnType::Float)
        );
        assert_eq!(
            plan.output_schema.column_type("total"),
            Some(ColumnType::Float)
        );

        let rows = vec![
            datum_row(vec![Datum::I64(1), float(1.5), text("b")]),
            datum_row(vec![Datum::I64(2), float(2.5), text("a")]),
        ];
        run_plan(
            &plan,
            vec![(MEASUREMENTS, rows)],
            vec![datum_row(vec![
                text("a"),
                float(2.5),
                float(4.0),
                float(2.0),
            ])],
        );
    }

    #[test]
    fn sum_distinct_sums_each_value_once() {
        let graph = parse_and_lower("SELECT sum(DISTINCT value) AS total FROM events").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        // events values: 10, 10, 20, 5 → distinct sum 35.
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![datum_row(vec![Datum::I64(35)])],
        );
    }

    #[test]
    fn count_of_a_column_skips_nulls() {
        let graph = parse_and_lower("SELECT count(title) AS n FROM posts").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        let rows = vec![
            datum_row(vec![Datum::I64(1), text("a"), Datum::Bool(true)]),
            datum_row(vec![Datum::I64(2), Datum::Null, Datum::Bool(true)]),
        ];
        run_plan(
            &plan,
            vec![(TableId::new(1), rows)],
            vec![datum_row(vec![Datum::I64(1)])],
        );
    }

    #[test]
    fn null_literal_projection_compiles() {
        let graph = parse_and_lower("SELECT id, NULL FROM posts").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(plan.output_schema.len(), 2);
        let rows = vec![datum_row(vec![Datum::I64(1), text("a"), Datum::Bool(true)])];
        run_plan(
            &plan,
            vec![(TableId::new(1), rows)],
            vec![datum_row(vec![Datum::I64(1), Datum::Null])],
        );
    }

    #[test]
    fn order_by_non_projected_column_uses_hidden_key() {
        let graph = parse_and_lower("SELECT id FROM posts ORDER BY title DESC LIMIT 1").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(plan.output_schema.len(), 1, "hidden key is dropped");
        let rows = vec![
            datum_row(vec![Datum::I64(1), text("alpha"), Datum::Bool(true)]),
            datum_row(vec![Datum::I64(2), text("zeta"), Datum::Bool(true)]),
        ];
        run_plan(
            &plan,
            vec![(TableId::new(1), rows)],
            vec![datum_row(vec![Datum::I64(2)])],
        );
    }

    #[test]
    fn loop_invariant_nested_recursion_is_hoisted() {
        let graph = parse_and_lower(
            "WITH RECURSIVE r1 AS (
                SELECT id, parent FROM edges WHERE parent = 1
                UNION
                SELECT edges.id, edges.parent
                FROM edges JOIN r1 ON edges.parent = r1.id
             ), r2 AS (
                SELECT id, parent FROM edges WHERE parent = 9
                UNION
                SELECT r1.id, r1.parent
                FROM r1 JOIN r2 ON r1.id = r2.parent
             )
             SELECT id FROM r2",
        )
        .unwrap();
        let plan = compile_mir(&graph, &relational_lookup)
            .expect("earlier recursive CTE used in a later step must compile");
        let edges = vec![
            datum_row(vec![Datum::I64(2), Datum::I64(1)]),
            datum_row(vec![Datum::I64(3), Datum::I64(2)]),
            datum_row(vec![Datum::I64(9), Datum::I64(3)]),
            datum_row(vec![Datum::I64(10), Datum::I64(9)]),
        ];
        // r1 = reachable from 1: {(2,1), (3,2), (9,3)}. r2 starts at
        // (10,9) and walks parents through r1: 9, 3, 2.
        run_plan(
            &plan,
            vec![(EDGES, edges)],
            vec![
                datum_row(vec![Datum::I64(10)]),
                datum_row(vec![Datum::I64(9)]),
                datum_row(vec![Datum::I64(3)]),
                datum_row(vec![Datum::I64(2)]),
            ],
        );
    }

    #[test]
    fn multi_column_order_by_slices_with_mixed_directions() {
        let graph = parse_and_lower(
            "SELECT id, category_id FROM events
             ORDER BY category_id ASC, id DESC
             LIMIT 2",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        // Sorted: (3,7), (2,7), (1,7), (4,9) — the first two survive.
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![
                datum_row(vec![Datum::I64(3), Datum::I64(7)]),
                datum_row(vec![Datum::I64(2), Datum::I64(7)]),
            ],
        );
    }

    #[test]
    fn order_by_text_key_sorts_lexicographically() {
        let graph =
            parse_and_lower("SELECT id, title FROM posts ORDER BY title DESC LIMIT 1").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        let seed = vec![
            datum_row(vec![Datum::I64(1), text("alpha"), Datum::Bool(true)]),
            datum_row(vec![Datum::I64(2), text("zeta"), Datum::Bool(true)]),
        ];
        run_plan(
            &plan,
            vec![(TableId::new(1), seed)],
            vec![datum_row(vec![Datum::I64(2), text("zeta")])],
        );
    }

    #[test]
    fn like_case_concat_and_distinctness_evaluate() {
        let graph = parse_and_lower(
            "SELECT id,
                    CASE WHEN published THEN 'live' ELSE 'draft' END AS state,
                    title || '!' AS shout
             FROM posts
             WHERE title LIKE 'a%'
               AND title NOT ILIKE '%ZZZ%'
               AND id IS DISTINCT FROM 99",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        assert_eq!(
            plan.output_schema.column_type("state"),
            Some(ColumnType::Text)
        );
        assert_eq!(
            plan.output_schema.column_type("shout"),
            Some(ColumnType::Text)
        );

        let rows = vec![
            datum_row(vec![Datum::I64(1), text("alpha"), Datum::Bool(true)]),
            datum_row(vec![Datum::I64(2), text("beta"), Datum::Bool(false)]),
            datum_row(vec![Datum::I64(3), text("azzz"), Datum::Bool(true)]),
        ];
        run_plan(
            &plan,
            vec![(TableId::new(1), rows)],
            vec![datum_row(vec![Datum::I64(1), text("live"), text("alpha!")])],
        );
    }

    #[test]
    fn any_over_postgres_array_literal_evaluates() {
        let graph =
            parse_and_lower("SELECT id FROM events WHERE category_id = ANY('{7, 11}')").unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![
                datum_row(vec![Datum::I64(1)]),
                datum_row(vec![Datum::I64(2)]),
                datum_row(vec![Datum::I64(3)]),
            ],
        );
    }

    #[test]
    fn in_between_and_arithmetic_evaluate_in_predicates() {
        let graph = parse_and_lower(
            "SELECT id FROM events
             WHERE category_id IN (7, 9)
               AND value BETWEEN 10 AND 20
               AND id + 1 = 3",
        )
        .unwrap();
        let plan = compile_mir(&graph, &lookup).unwrap();
        run_plan(
            &plan,
            vec![(TableId::new(2), events_seed())],
            vec![datum_row(vec![Datum::I64(2)])],
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
