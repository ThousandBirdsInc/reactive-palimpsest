// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reference relational executor used as the source of truth for
//! conformance and property tests.

#![allow(missing_docs)]

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use palimpsest_sql::mir::{
    AggExpr, ColumnRef, JoinKind, MirEdgeKind, MirGraph, MirNodeKind, OrderKey, SetQuantifierKind,
};
use petgraph::{graph::NodeIndex, visit::EdgeRef, Direction};

use crate::wal::{Catalog, LogicalEvent, TableId, Tuple};

pub type PrimaryKey = String;
pub type Row = Tuple;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetDiff {
    pub missing: Vec<Row>,
    pub extra: Vec<Row>,
}

#[derive(Debug, Clone)]
pub struct ReferenceExecutor {
    schema: Arc<Catalog>,
    tables: HashMap<TableId, BTreeMap<PrimaryKey, Row>>,
}

impl ReferenceExecutor {
    #[must_use]
    pub fn new(schema: Arc<Catalog>) -> Self {
        Self {
            schema,
            tables: HashMap::new(),
        }
    }

    #[must_use]
    pub const fn with_tables(
        schema: Arc<Catalog>,
        tables: HashMap<TableId, BTreeMap<PrimaryKey, Row>>,
    ) -> Self {
        Self { schema, tables }
    }

    #[must_use]
    pub fn schema(&self) -> &Catalog {
        &self.schema
    }

    #[must_use]
    pub const fn tables(&self) -> &HashMap<TableId, BTreeMap<PrimaryKey, Row>> {
        &self.tables
    }

    #[must_use]
    pub fn table(&self, table: TableId) -> Option<&BTreeMap<PrimaryKey, Row>> {
        self.tables.get(&table)
    }

    pub fn table_mut(&mut self, table: TableId) -> &mut BTreeMap<PrimaryKey, Row> {
        self.tables.entry(table).or_default()
    }

    pub fn apply(&mut self, events: &[LogicalEvent]) {
        for event in events {
            match event {
                LogicalEvent::Insert { table, new } => {
                    self.table_mut(*table).insert(primary_key(new), new.clone());
                }
                LogicalEvent::Update { table, old, new } => {
                    if let Some(old) = old {
                        self.table_mut(*table).remove(&primary_key(old));
                    }
                    self.table_mut(*table).insert(primary_key(new), new.clone());
                }
                LogicalEvent::Delete { table, old } => {
                    self.table_mut(*table).remove(&primary_key(old));
                }
                LogicalEvent::RelationChange { table, .. } => {
                    self.table_mut(*table);
                }
                LogicalEvent::Begin { .. }
                | LogicalEvent::Commit
                | LogicalEvent::Truncate { .. }
                | LogicalEvent::Origin { .. }
                | LogicalEvent::StreamStart { .. }
                | LogicalEvent::StreamStop
                | LogicalEvent::StreamCommit { .. }
                | LogicalEvent::StreamAbort { .. }
                | LogicalEvent::BeginPrepare { .. }
                | LogicalEvent::Prepare { .. }
                | LogicalEvent::CommitPrepared { .. }
                | LogicalEvent::RollbackPrepared { .. }
                | LogicalEvent::Keepalive => {}
            }
        }
    }

    #[must_use]
    pub fn execute(&self, graph: &MirGraph) -> Vec<Row> {
        self.execute_records(graph, graph.root(), &RecursiveEnv::new())
            .into_iter()
            .map(|record| record.values)
            .collect()
    }

    fn execute_records(
        &self,
        graph: &MirGraph,
        node: NodeIndex,
        env: &RecursiveEnv,
    ) -> Vec<Record> {
        match &graph.graph()[node] {
            MirNodeKind::BaseTable { table, .. } => self.scan_table(table),
            MirNodeKind::Filter { predicate } => self
                .single_input(graph, node, env)
                .into_iter()
                .filter(|record| eval_predicate(predicate, record) == Truth::True)
                .collect(),
            MirNodeKind::Project { columns } => self
                .single_input(graph, node, env)
                .into_iter()
                .map(|record| project_record(&record, columns))
                .collect(),
            MirNodeKind::Join { kind, on } => {
                let [left, right] = self.two_inputs(graph, node, env);
                join_records(left, &right, *kind, on)
            }
            MirNodeKind::Aggregate { group_by, aggs } => {
                aggregate_records(self.single_input(graph, node, env), group_by, aggs)
            }
            MirNodeKind::Distinct => distinct_records(self.single_input(graph, node, env)),
            MirNodeKind::DistinctOn { on, order_by } => {
                let mut records = self.single_input(graph, node, env);
                sort_records(&mut records, order_by);
                let mut seen = std::collections::BTreeSet::new();
                records
                    .into_iter()
                    .filter(|record| {
                        let key: Vec<Option<String>> =
                            on.iter().map(|expr| value_for_expr(record, expr)).collect();
                        seen.insert(key)
                    })
                    .collect()
            }
            MirNodeKind::Union { quantifier } => {
                let [left, right] = self.two_inputs(graph, node, env);
                union_records(left, &right, *quantifier)
            }
            MirNodeKind::Except { quantifier } => {
                let [left, right] = self.two_inputs(graph, node, env);
                except_records(left, &right, *quantifier)
            }
            MirNodeKind::Intersect { quantifier } => {
                let [left, right] = self.two_inputs(graph, node, env);
                intersect_records(left, &right, *quantifier)
            }
            MirNodeKind::TopK {
                order_by,
                limit,
                offset,
            } => {
                let mut records = self.single_input(graph, node, env);
                sort_records(&mut records, order_by);
                records.into_iter().skip(*offset).take(*limit).collect()
            }
            MirNodeKind::CteRef { .. } => self.cte_expansion_input(graph, node, env),
            MirNodeKind::Fixpoint { cte, union_all } => {
                let inputs = graph.ordered_inputs(node);
                let [base, step] = inputs.as_slice() else {
                    return Vec::new();
                };
                self.execute_fixpoint(graph, *base, *step, cte, *union_all, env)
            }
            MirNodeKind::RecursiveRef { cte } => env.get(cte).cloned().unwrap_or_default(),
            MirNodeKind::Leaf { .. } => Vec::new(),
        }
    }

    /// Semi-naive evaluation of a `WITH RECURSIVE` fixpoint: seed with
    /// the base term, then repeatedly run the step term over the
    /// previous iteration's rows until it produces nothing new (or a
    /// safety cap trips for non-terminating `UNION ALL` recursions).
    fn execute_fixpoint(
        &self,
        graph: &MirGraph,
        base: NodeIndex,
        step: NodeIndex,
        cte: &str,
        union_all: bool,
        env: &RecursiveEnv,
    ) -> Vec<Record> {
        const MAX_ITERATIONS: usize = 4096;

        let mut working = self.execute_records(graph, base, env);
        if !union_all {
            working = distinct_records(working);
        }
        let mut seen = multiset_records(&working)
            .into_keys()
            .collect::<std::collections::BTreeSet<_>>();
        let mut output = working.clone();

        for _ in 0..MAX_ITERATIONS {
            if working.is_empty() {
                break;
            }
            let mut step_env = env.clone();
            step_env.insert(cte.to_owned(), working);
            let mut produced = self.execute_records(graph, step, &step_env);
            if !union_all {
                produced = distinct_records(produced);
                produced.retain(|record| seen.insert(record.values.clone()));
            }
            output.extend(produced.clone());
            working = produced;
        }

        output
    }

    fn scan_table(&self, table_name: &str) -> Vec<Record> {
        let Some(table_id) = self.schema.table_id(table_name) else {
            return Vec::new();
        };
        let columns = self
            .schema
            .table(table_id)
            .map(|table| {
                table
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        self.table(table_id)
            .into_iter()
            .flat_map(BTreeMap::values)
            .map(|row| Record::from_table(table_name, &columns, row.clone()))
            .collect()
    }

    fn single_input(&self, graph: &MirGraph, node: NodeIndex, env: &RecursiveEnv) -> Vec<Record> {
        let inputs = graph.ordered_inputs(node);
        let [input] = inputs.as_slice() else {
            return Vec::new();
        };
        self.execute_records(graph, *input, env)
    }

    fn cte_expansion_input(
        &self,
        graph: &MirGraph,
        node: NodeIndex,
        env: &RecursiveEnv,
    ) -> Vec<Record> {
        let inputs: Vec<NodeIndex> = graph
            .graph()
            .edges_directed(node, Direction::Incoming)
            .filter(|edge| matches!(edge.weight(), MirEdgeKind::CteExpansion))
            .map(|edge| edge.source())
            .collect();
        let [input] = inputs.as_slice() else {
            return Vec::new();
        };
        self.execute_records(graph, *input, env)
    }

    fn two_inputs(
        &self,
        graph: &MirGraph,
        node: NodeIndex,
        env: &RecursiveEnv,
    ) -> [Vec<Record>; 2] {
        let inputs = graph.ordered_inputs(node);
        let [left, right] = inputs.as_slice() else {
            return [Vec::new(), Vec::new()];
        };
        [
            self.execute_records(graph, *left, env),
            self.execute_records(graph, *right, env),
        ]
    }
}

/// Working-table bindings for in-flight `Fixpoint` evaluations, keyed
/// by CTE name. `RecursiveRef` nodes resolve against this.
type RecursiveEnv = BTreeMap<String, Vec<Record>>;

fn sort_records(records: &mut [Record], order_by: &[OrderKey]) {
    records.sort_by(|left, right| {
        order_by
            .iter()
            .map(|key| {
                let ordering = cmp_values(
                    value_for_expr(left, &key.expression).as_deref(),
                    value_for_expr(right, &key.expression).as_deref(),
                );
                if key.descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            })
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
}

fn primary_key(row: &Row) -> PrimaryKey {
    row.first().cloned().unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    values: Row,
    attrs: BTreeMap<String, Option<String>>,
}

impl Record {
    fn from_table(table: &str, columns: &[String], values: Row) -> Self {
        let mut attrs = BTreeMap::new();
        for (index, value) in values.iter().enumerate() {
            let column = columns
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("c{}", index + 1));
            attrs.insert(column.clone(), Some(value.clone()));
            attrs.insert(format!("{table}.{column}"), Some(value.clone()));
        }
        Self { values, attrs }
    }

    fn nulls_for(attrs: impl IntoIterator<Item = String>) -> Self {
        Self {
            values: Vec::new(),
            attrs: attrs.into_iter().map(|attr| (attr, None)).collect(),
        }
    }
}

pub fn assert_set_eq(left: &[Row], right: &[Row]) -> Result<(), SetDiff> {
    let left = multiset(left);
    let right = multiset(right);
    if left == right {
        return Ok(());
    }

    let mut missing = Vec::new();
    let mut extra = Vec::new();
    for (row, expected) in &right {
        let actual = left.get(row).copied().unwrap_or(0);
        missing.extend(std::iter::repeat_n(
            row.clone(),
            expected.saturating_sub(actual),
        ));
    }
    for (row, actual) in &left {
        let expected = right.get(row).copied().unwrap_or(0);
        extra.extend(std::iter::repeat_n(
            row.clone(),
            actual.saturating_sub(expected),
        ));
    }
    Err(SetDiff { missing, extra })
}

#[must_use]
pub fn fixture_line_count_within_budget(contents: &str, max_lines: usize) -> bool {
    contents.lines().count() <= max_lines
}

fn project_record(record: &Record, columns: &[String]) -> Record {
    let values = columns
        .iter()
        .map(|column| value_for_expr(record, column).unwrap_or_default())
        .collect::<Vec<_>>();
    let mut attrs = BTreeMap::new();
    for (column, value) in columns.iter().zip(&values) {
        // A projection starts a fresh (anonymous) relation: a
        // projected `relation.column` is addressable downstream only
        // by its output name (the trailing segment), matching SQL.
        // Keeping source-qualified keys would leak stale bindings into
        // later joins against the same base relation (e.g. recursive
        // steps).
        let name = column
            .rsplit_once('.')
            .map_or(column.as_str(), |(_, bare)| bare);
        attrs.insert(name.to_owned(), Some(value.clone()));
    }
    Record { values, attrs }
}

fn join_records(
    left: Vec<Record>,
    right: &[Record],
    kind: JoinKind,
    on: &[(ColumnRef, ColumnRef)],
) -> Vec<Record> {
    let right_attrs = right
        .iter()
        .flat_map(|record| record.attrs.keys().cloned())
        .collect::<Vec<_>>();
    let null_right = Record::nulls_for(right_attrs);
    let mut joined = Vec::new();

    for left_record in left {
        let mut matched = false;
        for right_record in right {
            let merged = merge_records(&left_record, right_record);
            if on
                .iter()
                .all(|(left_key, right_key)| eval_join_key(&merged, left_key, right_key))
            {
                matched = true;
                match kind {
                    JoinKind::Inner | JoinKind::Left => joined.push(merged),
                    // Semi/anti joins emit at most one copy of the
                    // left record, untouched by right-side columns.
                    JoinKind::Semi | JoinKind::Anti => break,
                }
            }
        }
        match kind {
            JoinKind::Left if !matched => joined.push(merge_records(&left_record, &null_right)),
            JoinKind::Semi if matched => joined.push(left_record),
            JoinKind::Anti if !matched => joined.push(left_record),
            _ => {}
        }
    }

    joined
}

fn aggregate_records(
    records: Vec<Record>,
    group_by: &[ColumnRef],
    aggs: &[AggExpr],
) -> Vec<Record> {
    let mut groups = BTreeMap::<Vec<Option<String>>, Vec<Record>>::new();
    for record in records {
        let key = group_by
            .iter()
            .map(|column| value_for_column(&record, column))
            .collect();
        groups.entry(key).or_default().push(record);
    }

    groups
        .into_iter()
        .map(|(key, rows)| {
            // Expose named attributes so downstream operators (a
            // HAVING filter, projections referencing aggregate output
            // columns) can resolve values by name.
            let mut attrs = BTreeMap::new();
            let mut values = Row::new();
            for (column, value) in group_by.iter().zip(&key) {
                values.push(value.clone().unwrap_or_default());
                attrs.insert(column.name.clone(), value.clone());
                if let Some(relation) = &column.relation {
                    attrs.insert(format!("{relation}.{}", column.name), value.clone());
                }
            }
            for agg in aggs {
                let value = eval_agg(agg, &rows);
                attrs.insert(aggregate_output_name(agg), Some(value.clone()));
                values.push(value);
            }
            Record { values, attrs }
        })
        .collect()
}

/// Output-column name for an aggregate: its alias when present,
/// otherwise its display text (`count(*)`), matching the dataflow
/// compiler's naming.
fn aggregate_output_name(agg: &AggExpr) -> String {
    if let Some(alias) = &agg.alias {
        return alias.clone();
    }
    let distinct = agg.args.first().is_some_and(|arg| arg == "DISTINCT");
    let args = &agg.args[usize::from(distinct)..];
    if distinct {
        format!("{}(DISTINCT {})", agg.function, args.join(", "))
    } else {
        format!("{}({})", agg.function, args.join(", "))
    }
}

fn distinct_records(records: Vec<Record>) -> Vec<Record> {
    let mut seen = BTreeMap::<Row, Record>::new();
    for record in records {
        seen.entry(record.values.clone()).or_insert(record);
    }
    seen.into_values().collect()
}

fn union_records(
    left: Vec<Record>,
    right: &[Record],
    quantifier: SetQuantifierKind,
) -> Vec<Record> {
    let records = left
        .into_iter()
        .chain(right.iter().cloned())
        .collect::<Vec<_>>();
    if quantifier == SetQuantifierKind::Distinct {
        distinct_records(records)
    } else {
        records
    }
}

fn except_records(
    left: Vec<Record>,
    right: &[Record],
    quantifier: SetQuantifierKind,
) -> Vec<Record> {
    let right_counts = multiset_records(right);
    let mut used = BTreeMap::<Row, usize>::new();
    let mut output = Vec::new();
    for record in left {
        let key = record.values.clone();
        let consumed = used.entry(key.clone()).or_default();
        let available = right_counts.get(&key).copied().unwrap_or(0);
        if *consumed >= available {
            output.push(record);
        } else {
            *consumed += 1;
        }
    }
    if quantifier == SetQuantifierKind::Distinct {
        distinct_records(output)
    } else {
        output
    }
}

fn intersect_records(
    left: Vec<Record>,
    right: &[Record],
    quantifier: SetQuantifierKind,
) -> Vec<Record> {
    let right_counts = multiset_records(right);
    let mut used = BTreeMap::<Row, usize>::new();
    let mut output = Vec::new();
    for record in left {
        let key = record.values.clone();
        let consumed = used.entry(key.clone()).or_default();
        let available = right_counts.get(&key).copied().unwrap_or(0);
        if *consumed < available {
            *consumed += 1;
            output.push(record);
        }
    }
    if quantifier == SetQuantifierKind::Distinct {
        distinct_records(output)
    } else {
        output
    }
}

fn merge_records(left: &Record, right: &Record) -> Record {
    let mut values = left.values.clone();
    values.extend(right.values.clone());
    let mut attrs = left.attrs.clone();
    attrs.extend(right.attrs.clone());
    Record { values, attrs }
}

fn eval_join_key(record: &Record, left: &ColumnRef, right: &ColumnRef) -> bool {
    value_for_column(record, left).is_some()
        && value_for_column(record, left) == value_for_column(record, right)
}

fn eval_predicate(predicate: &str, record: &Record) -> Truth {
    predicate
        .split(" AND ")
        .map(|part| eval_atom(part, record))
        .fold(Truth::True, and_truth)
}

fn eval_atom(atom: &str, record: &Record) -> Truth {
    if let Some(expr) = atom.strip_suffix(" IS NULL") {
        return if value_for_expr(record, expr).is_none() {
            Truth::True
        } else {
            Truth::False
        };
    }
    for op in [" >= ", " <= ", " <> ", " != ", " = ", " > ", " < "] {
        if let Some((left, right)) = atom.split_once(op) {
            return compare_atom(
                value_for_expr(record, left),
                value_for_expr(record, right),
                op.trim(),
            );
        }
    }
    Truth::Unknown
}

fn compare_atom(left: Option<String>, right: Option<String>, op: &str) -> Truth {
    let (Some(left), Some(right)) = (left, right) else {
        return Truth::Unknown;
    };
    let ordering = cmp_values(Some(&left), Some(&right));
    let matched = match op {
        "=" => ordering == Ordering::Equal,
        "!=" | "<>" => ordering != Ordering::Equal,
        ">" => ordering == Ordering::Greater,
        "<" => ordering == Ordering::Less,
        ">=" => matches!(ordering, Ordering::Greater | Ordering::Equal),
        "<=" => matches!(ordering, Ordering::Less | Ordering::Equal),
        _ => false,
    };
    Truth::from(matched)
}

const fn and_truth(left: Truth, right: Truth) -> Truth {
    match (left, right) {
        (Truth::False, _) | (_, Truth::False) => Truth::False,
        (Truth::Unknown, _) | (_, Truth::Unknown) => Truth::Unknown,
        (Truth::True, Truth::True) => Truth::True,
    }
}

impl From<bool> for Truth {
    fn from(value: bool) -> Self {
        if value {
            Self::True
        } else {
            Self::False
        }
    }
}

fn value_for_column(record: &Record, column: &ColumnRef) -> Option<String> {
    if let Some(relation) = &column.relation {
        let key = format!("{relation}.{}", column.name);
        if let Some(value) = record.attrs.get(&key) {
            return value.clone();
        }
    }
    // Fall back to the bare column name: rows that flowed through a
    // `Project` (CTE outputs, derived tables, recursive working
    // tables) only carry unqualified attributes.
    record.attrs.get(&column.name).cloned().flatten()
}

fn value_for_expr(record: &Record, expr: &str) -> Option<String> {
    let expr = expr.trim();
    if let Some(value) = record.attrs.get(expr) {
        return value.clone();
    }
    if expr.eq_ignore_ascii_case("NULL") {
        return None;
    }
    if let Some(value) = expr
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    {
        return Some(value.replace("''", "'"));
    }
    // `alias.column` where the record only carries the bare output
    // name (e.g. rows that flowed through a Project).
    if let Some((qualifier, bare)) = expr.rsplit_once('.') {
        if !qualifier.is_empty() {
            if let Some(value) = record.attrs.get(bare) {
                return value.clone();
            }
        }
    }
    Some(expr.to_owned()).filter(|value| !value.is_empty())
}

fn cmp_values(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => match (left.parse::<i128>(), right.parse::<i128>()) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            _ => left.cmp(right),
        },
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn eval_agg(agg: &AggExpr, rows: &[Record]) -> String {
    let values = rows
        .iter()
        .filter_map(|record| agg.args.first().and_then(|arg| value_for_expr(record, arg)))
        .collect::<Vec<_>>();
    match agg.function.as_str() {
        "count" if agg.args.first().is_some_and(|arg| arg == "*") => rows.len().to_string(),
        "count" => values.len().to_string(),
        "sum" => values
            .iter()
            .filter_map(|value| value.parse::<i128>().ok())
            .sum::<i128>()
            .to_string(),
        "min" => values.iter().min().cloned().unwrap_or_default(),
        "max" => values.iter().max().cloned().unwrap_or_default(),
        "avg" => {
            let ints = values
                .iter()
                .filter_map(|value| value.parse::<i128>().ok())
                .collect::<Vec<_>>();
            if ints.is_empty() {
                String::new()
            } else {
                (ints.iter().sum::<i128>() / i128::try_from(ints.len()).unwrap_or(1)).to_string()
            }
        }
        _ => String::new(),
    }
}

fn multiset(rows: &[Row]) -> BTreeMap<Row, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row.clone()).or_default() += 1;
    }
    counts
}

fn multiset_records(records: &[Record]) -> BTreeMap<Row, usize> {
    let mut counts = BTreeMap::new();
    for record in records {
        *counts.entry(record.values.clone()).or_default() += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use palimpsest_sql::mir::{ColumnRef, JoinKind, MirGraph, MirNodeKind, SetQuantifierKind};

    use super::{
        assert_set_eq, fixture_line_count_within_budget, PrimaryKey, ReferenceExecutor, Row,
    };
    use crate::wal::{Catalog, ColumnDef, LogicalEvent, TableDef, TableId};

    #[test]
    fn starts_with_schema_and_empty_tables() {
        let table = TableId::new(7);
        let executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        assert!(executor.schema().contains(table));
        assert!(executor.tables().is_empty());
    }

    #[test]
    fn stores_rows_by_table_and_primary_key() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.table_mut(table).insert(
            "post-1".to_owned(),
            vec!["post-1".to_owned(), "hello".to_owned()],
        );

        assert_eq!(
            executor.table(table).and_then(|rows| rows.get("post-1")),
            Some(&vec!["post-1".to_owned(), "hello".to_owned()])
        );
    }

    #[test]
    fn accepts_seed_tables() {
        let table = TableId::new(7);
        let mut rows = BTreeMap::<PrimaryKey, Row>::new();
        rows.insert("post-1".to_owned(), vec!["post-1".to_owned()]);

        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::new([table])),
            std::iter::once((table, rows)).collect(),
        );

        assert_eq!(executor.table(table).map(BTreeMap::len), Some(1));
    }

    #[test]
    fn applies_insert_update_and_delete_events() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.apply(&[
            LogicalEvent::Begin { xid: 1 },
            LogicalEvent::Insert {
                table,
                new: vec!["post-1".to_owned(), "draft".to_owned()],
            },
            LogicalEvent::Update {
                table,
                old: Some(vec!["post-1".to_owned(), "draft".to_owned()]),
                new: vec!["post-1".to_owned(), "published".to_owned()],
            },
            LogicalEvent::Commit,
        ]);

        assert_eq!(
            executor.table(table).and_then(|rows| rows.get("post-1")),
            Some(&vec!["post-1".to_owned(), "published".to_owned()])
        );

        executor.apply(&[LogicalEvent::Delete {
            table,
            old: vec!["post-1".to_owned(), "published".to_owned()],
        }]);

        assert!(executor.table(table).is_some_and(BTreeMap::is_empty));
    }

    #[test]
    fn applies_update_without_old_tuple_as_upsert() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.apply(&[LogicalEvent::Update {
            table,
            old: None,
            new: vec!["post-1".to_owned(), "published".to_owned()],
        }]);

        assert_eq!(executor.table(table).map(BTreeMap::len), Some(1));
    }

    #[test]
    fn relation_change_creates_table_slot() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.apply(&[LogicalEvent::RelationChange {
            table,
            new_columns: Vec::new(),
        }]);

        assert!(executor.table(table).is_some());
    }

    #[test]
    fn executes_filter_project_mir() {
        let posts = TableId::new(7);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([table(
                posts,
                "posts",
                &["id", "author_id", "title"],
            )])),
            std::iter::once((
                posts,
                rows([
                    ["1", "42", "hello"].as_slice(),
                    ["2", "7", "draft"].as_slice(),
                ]),
            ))
            .collect(),
        );
        let mut graph = MirGraph::new(MirNodeKind::BaseTable {
            table: "posts".to_owned(),
            project: Vec::new(),
        });
        let base = graph.root();
        let filter = graph.add_node(MirNodeKind::Filter {
            predicate: "author_id = 42".to_owned(),
        });
        graph.add_input(base, filter);
        let project = graph.add_node(MirNodeKind::Project {
            columns: vec!["id".to_owned(), "title".to_owned()],
        });
        graph.add_input(filter, project);
        graph.set_root(project);

        assert_eq!(
            executor.execute(&graph),
            [vec!["1".to_owned(), "hello".to_owned()]]
        );
    }

    #[test]
    fn executes_join_left_join_and_null_predicate() {
        let posts = TableId::new(7);
        let authors = TableId::new(8);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([
                table(posts, "posts", &["id", "author_id"]),
                table(authors, "authors", &["id", "name"]),
            ])),
            [
                (
                    posts,
                    rows([["1", "42"].as_slice(), ["2", "99"].as_slice()]),
                ),
                (authors, rows([["42", "Ada"].as_slice()])),
            ]
            .into_iter()
            .collect(),
        );

        let mut graph = MirGraph::new(MirNodeKind::BaseTable {
            table: "posts".to_owned(),
            project: Vec::new(),
        });
        let posts_node = graph.root();
        let authors_node = graph.add_node(MirNodeKind::BaseTable {
            table: "authors".to_owned(),
            project: Vec::new(),
        });
        let join = graph.add_node(MirNodeKind::Join {
            kind: JoinKind::Left,
            on: vec![(column("posts", "author_id"), column("authors", "id"))],
        });
        graph.add_input(posts_node, join);
        graph.add_input(authors_node, join);
        let filter = graph.add_node(MirNodeKind::Filter {
            predicate: "authors.name IS NULL".to_owned(),
        });
        graph.add_input(join, filter);
        let project = graph.add_node(MirNodeKind::Project {
            columns: vec!["posts.id".to_owned()],
        });
        graph.add_input(filter, project);
        graph.set_root(project);

        assert_eq!(executor.execute(&graph), [vec!["2".to_owned()]]);
    }

    #[test]
    fn executes_set_operations_and_aggregate() {
        let posts = TableId::new(7);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([table(
                posts,
                "posts",
                &["id", "author_id"],
            )])),
            std::iter::once((
                posts,
                rows([
                    ["1", "42"].as_slice(),
                    ["2", "42"].as_slice(),
                    ["3", "7"].as_slice(),
                ]),
            ))
            .collect(),
        );

        let mut aggregate = MirGraph::new(MirNodeKind::BaseTable {
            table: "posts".to_owned(),
            project: Vec::new(),
        });
        let base = aggregate.root();
        let agg = aggregate.add_node(MirNodeKind::Aggregate {
            group_by: vec![column("", "author_id")],
            aggs: vec![palimpsest_sql::mir::AggExpr {
                function: "count".to_owned(),
                args: vec!["*".to_owned()],
                alias: None,
            }],
        });
        aggregate.add_input(base, agg);
        aggregate.set_root(agg);

        assert_set_eq(
            &executor.execute(&aggregate),
            &[
                vec!["42".to_owned(), "2".to_owned()],
                vec!["7".to_owned(), "1".to_owned()],
            ],
        )
        .expect("aggregate rows should match as a set");

        let union = set_graph("posts", SetQuantifierKind::Distinct);
        assert_eq!(executor.execute(&union), [vec!["1".to_owned()]]);
    }

    #[test]
    fn executes_semi_and_anti_joins_from_exists_lowering() {
        let posts = TableId::new(7);
        let comments = TableId::new(8);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([
                table(posts, "posts", &["id", "author_id"]),
                table(comments, "comments", &["id", "post_id"]),
            ])),
            [
                (posts, rows([["1", "42"].as_slice(), ["2", "7"].as_slice()])),
                (
                    comments,
                    rows([["10", "1"].as_slice(), ["11", "1"].as_slice()]),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let semi = palimpsest_sql::lower::parse_and_lower(
            "SELECT id FROM posts
             WHERE EXISTS (SELECT 1 FROM comments WHERE comments.post_id = posts.id)",
        )
        .expect("correlated EXISTS should lower");
        // Post 1 has two matching comments but a semi-join emits it once.
        assert_eq!(executor.execute(&semi), [vec!["1".to_owned()]]);

        let anti = palimpsest_sql::lower::parse_and_lower(
            "SELECT id FROM posts
             WHERE NOT EXISTS (SELECT 1 FROM comments WHERE comments.post_id = posts.id)",
        )
        .expect("correlated NOT EXISTS should lower");
        assert_eq!(executor.execute(&anti), [vec!["2".to_owned()]]);
    }

    #[test]
    fn executes_distinct_on_keeping_first_row_per_group() {
        let posts = TableId::new(7);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([table(
                posts,
                "posts",
                &["id", "author_id"],
            )])),
            std::iter::once((
                posts,
                rows([
                    ["1", "42"].as_slice(),
                    ["2", "42"].as_slice(),
                    ["3", "7"].as_slice(),
                ]),
            ))
            .collect(),
        );

        let graph = palimpsest_sql::lower::parse_and_lower(
            "SELECT DISTINCT ON (author_id) id, author_id FROM posts
             ORDER BY author_id, id DESC",
        )
        .expect("DISTINCT ON should lower");

        // Per author the row with the highest id wins; the final sort
        // is by author_id ascending.
        assert_eq!(
            executor.execute(&graph),
            [
                vec!["3".to_owned(), "7".to_owned()],
                vec!["2".to_owned(), "42".to_owned()],
            ]
        );
    }

    #[test]
    fn executes_recursive_cte_to_fixpoint() {
        let edges = TableId::new(9);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([table(
                edges,
                "edges",
                &["id", "parent"],
            )])),
            std::iter::once((
                edges,
                rows([
                    ["2", "1"].as_slice(),
                    ["3", "2"].as_slice(),
                    ["4", "3"].as_slice(),
                    ["9", "8"].as_slice(),
                ]),
            ))
            .collect(),
        );

        let graph = palimpsest_sql::lower::parse_and_lower(
            "WITH RECURSIVE reach AS (
                SELECT id, parent FROM edges WHERE parent = 1
                UNION
                SELECT edges.id, edges.parent FROM edges JOIN reach ON edges.parent = reach.id
             )
             SELECT id FROM reach ORDER BY id",
        )
        .expect("recursive CTE should lower");

        assert_eq!(
            executor.execute(&graph),
            [
                vec!["2".to_owned()],
                vec!["3".to_owned()],
                vec!["4".to_owned()],
            ]
        );
    }

    #[test]
    fn recursive_union_all_terminates_when_step_drains() {
        let edges = TableId::new(9);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([table(
                edges,
                "edges",
                &["id", "parent"],
            )])),
            std::iter::once((edges, rows([["2", "1"].as_slice(), ["3", "2"].as_slice()])))
                .collect(),
        );

        let graph = palimpsest_sql::lower::parse_and_lower(
            "WITH RECURSIVE reach AS (
                SELECT id, parent FROM edges WHERE parent = 1
                UNION ALL
                SELECT edges.id, edges.parent FROM edges JOIN reach ON edges.parent = reach.id
             )
             SELECT id FROM reach ORDER BY id",
        )
        .expect("recursive CTE should lower");

        assert_eq!(
            executor.execute(&graph),
            [vec!["2".to_owned()], vec!["3".to_owned()]]
        );
    }

    #[test]
    fn executes_correlated_lateral_join() {
        let posts = TableId::new(7);
        let comments = TableId::new(8);
        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::with_tables([
                table(posts, "posts", &["id", "author_id"]),
                table(comments, "comments", &["id", "post_id", "body"]),
            ])),
            [
                (posts, rows([["1", "42"].as_slice(), ["2", "7"].as_slice()])),
                (
                    comments,
                    rows([
                        ["10", "1", "first"].as_slice(),
                        ["11", "2", "second"].as_slice(),
                    ]),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let graph = palimpsest_sql::lower::parse_and_lower(
            "SELECT posts.id, c.body
             FROM posts
             JOIN LATERAL (
                SELECT body, comments.post_id FROM comments WHERE comments.post_id = posts.id
             ) AS c ON TRUE",
        )
        .expect("correlated LATERAL join should lower");

        assert_set_eq(
            &executor.execute(&graph),
            &[
                vec!["1".to_owned(), "first".to_owned()],
                vec!["2".to_owned(), "second".to_owned()],
            ],
        )
        .expect("lateral join rows should match");
    }

    #[test]
    fn set_equality_and_fixture_budget_helpers_report_failures() {
        assert!(assert_set_eq(&[vec!["a".to_owned()]], &[vec!["a".to_owned()]]).is_ok());
        assert!(assert_set_eq(&[vec!["a".to_owned()]], &[vec!["b".to_owned()]]).is_err());
        assert!(fixture_line_count_within_budget("one\ntwo\n", 2));
        assert!(!fixture_line_count_within_budget("one\ntwo\nthree\n", 2));
    }

    fn table(id: TableId, name: &str, columns: &[&str]) -> TableDef {
        TableDef::new(
            id,
            name,
            columns
                .iter()
                .map(|name| ColumnDef {
                    name: (*name).to_owned(),
                    type_oid: 25,
                    nullable: false,
                })
                .collect(),
        )
    }

    fn rows<const N: usize>(rows: [&[&str]; N]) -> BTreeMap<PrimaryKey, Row> {
        rows.into_iter()
            .map(|row| {
                let row = row.iter().map(|value| (*value).to_owned()).collect::<Row>();
                (row[0].clone(), row)
            })
            .collect()
    }

    fn column(relation: &str, name: &str) -> ColumnRef {
        ColumnRef {
            relation: (!relation.is_empty()).then(|| relation.to_owned()),
            name: name.to_owned(),
        }
    }

    fn set_graph(table: &str, quantifier: SetQuantifierKind) -> MirGraph {
        let mut graph = MirGraph::new(MirNodeKind::BaseTable {
            table: table.to_owned(),
            project: Vec::new(),
        });
        let left_base = graph.root();
        let left_filter = graph.add_node(MirNodeKind::Filter {
            predicate: "id = 1".to_owned(),
        });
        graph.add_input(left_base, left_filter);
        let left = graph.add_node(MirNodeKind::Project {
            columns: vec!["id".to_owned()],
        });
        graph.add_input(left_filter, left);

        let right_base = graph.add_node(MirNodeKind::BaseTable {
            table: table.to_owned(),
            project: Vec::new(),
        });
        let right_filter = graph.add_node(MirNodeKind::Filter {
            predicate: "id = 1".to_owned(),
        });
        graph.add_input(right_base, right_filter);
        let right = graph.add_node(MirNodeKind::Project {
            columns: vec!["id".to_owned()],
        });
        graph.add_input(right_filter, right);

        let union = graph.add_node(MirNodeKind::Union { quantifier });
        graph.add_input(left, union);
        graph.add_input(right, union);
        graph.set_root(union);
        graph
    }
}
