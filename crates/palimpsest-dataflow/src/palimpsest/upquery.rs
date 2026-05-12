//! Upquery resolver.
//!
//! When a partial-materialization arrangement misses a key, the runtime
//! issues an *upquery* against the upstream Postgres so the consumer can
//! be served. The resolver walks an MIR plan backward, finds every base
//! table that contributes to the requested rows, and emits a
//! `SELECT … WHERE pk IN (…)` statement per base table.
//!
//! This module owns only the planning step. The actual SQL execution is
//! the responsibility of the gateway/postgres layer.

use std::collections::BTreeMap;

use palimpsest_sql::mir::{ColumnRef, MirEdgeKind, MirGraph, MirNodeKind};
use petgraph::{graph::NodeIndex, visit::EdgeRef, Direction};

/// One Postgres upquery directive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpqueryRequest {
    /// Postgres relation name.
    pub table: String,
    /// Primary-key column for the relation.
    pub primary_key: String,
    /// Primary-key values to fetch.
    pub keys: Vec<String>,
}

impl UpqueryRequest {
    /// Returns the SQL `SELECT * FROM table WHERE pk IN (…)` form, with values
    /// quoted using single quotes (and embedded quotes escaped per SQL rules).
    #[must_use]
    pub fn to_sql(&self) -> String {
        let placeholders = self
            .keys
            .iter()
            .map(|key| format!("'{}'", key.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT * FROM {table} WHERE {pk} IN ({values})",
            table = self.table,
            pk = self.primary_key,
            values = placeholders
        )
    }
}

/// Plan produced by walking an MIR backward from the requested node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpqueryPlan {
    /// One entry per contributing base table.
    pub requests: Vec<UpqueryRequest>,
}

impl UpqueryPlan {
    /// Returns true when no base tables were reached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    /// Number of distinct base tables in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.requests.len()
    }
}

/// Maps a relation name to its primary-key column.
pub trait PrimaryKeyResolver {
    /// Returns the primary-key column for `table`, if known.
    fn primary_key(&self, table: &str) -> Option<&str>;
}

/// Static `(table, primary_key)` lookup driven by a `BTreeMap`.
#[derive(Debug, Clone, Default)]
pub struct StaticPrimaryKeys {
    keys: BTreeMap<String, String>,
}

impl StaticPrimaryKeys {
    /// Creates an empty resolver.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            keys: BTreeMap::new(),
        }
    }

    /// Inserts or updates a `(table, primary_key)` mapping.
    pub fn insert(&mut self, table: impl Into<String>, primary_key: impl Into<String>) {
        self.keys.insert(table.into(), primary_key.into());
    }

    /// Builds a resolver from an iterator of `(table, primary_key)` pairs.
    #[must_use]
    pub fn from_iter<I, S, P>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (S, P)>,
        S: Into<String>,
        P: Into<String>,
    {
        let mut resolver = Self::new();
        for (table, key) in pairs {
            resolver.insert(table, key);
        }
        resolver
    }
}

impl PrimaryKeyResolver for StaticPrimaryKeys {
    fn primary_key(&self, table: &str) -> Option<&str> {
        self.keys.get(table).map(String::as_str)
    }
}

/// Walks the MIR backwards from its root and records the base tables that
/// must be queried for `requested_keys`.
///
/// The same primary-key set is forwarded to every base table; callers that
/// need per-base filtering should build a tailored plan up front (e.g., by
/// resolving join keys to per-table identifiers in the SQL frontend).
#[must_use]
pub fn plan_upquery<R>(graph: &MirGraph, requested_keys: &[String], primary_keys: &R) -> UpqueryPlan
where
    R: PrimaryKeyResolver + ?Sized,
{
    let mut requests: BTreeMap<String, UpqueryRequest> = BTreeMap::new();
    let mut stack = vec![graph.root()];
    let mut visited = std::collections::BTreeSet::new();

    while let Some(node) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }

        match &graph.graph()[node] {
            MirNodeKind::BaseTable { table, .. } => {
                let Some(primary_key) = primary_keys.primary_key(table) else {
                    continue;
                };
                requests
                    .entry(table.clone())
                    .or_insert_with(|| UpqueryRequest {
                        table: table.clone(),
                        primary_key: primary_key.to_owned(),
                        keys: requested_keys.to_vec(),
                    });
            }
            MirNodeKind::CteRef { .. } => {
                stack.extend(input_nodes(graph, node, MirEdgeKind::CteExpansion));
            }
            _ => {
                stack.extend(input_nodes(graph, node, MirEdgeKind::Input));
            }
        }
    }

    UpqueryPlan {
        requests: requests.into_values().collect(),
    }
}

/// Walks an MIR backward from `root` and returns every base table referenced.
#[must_use]
pub fn base_tables(graph: &MirGraph) -> Vec<String> {
    let mut tables = Vec::new();
    let mut stack = vec![graph.root()];
    let mut visited = std::collections::BTreeSet::new();

    while let Some(node) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }
        match &graph.graph()[node] {
            MirNodeKind::BaseTable { table, .. } => tables.push(table.clone()),
            MirNodeKind::CteRef { .. } => {
                stack.extend(input_nodes(graph, node, MirEdgeKind::CteExpansion));
            }
            _ => {
                stack.extend(input_nodes(graph, node, MirEdgeKind::Input));
            }
        }
    }

    tables.sort();
    tables.dedup();
    tables
}

/// Returns every `(relation, column)` pair the MIR projects for `table`.
///
/// Useful for narrowing upqueries to only the columns the consumer reads.
#[must_use]
pub fn referenced_columns(graph: &MirGraph, table: &str) -> Vec<ColumnRef> {
    let mut columns = Vec::new();
    for node in graph.graph().node_weights() {
        if let MirNodeKind::BaseTable {
            table: name,
            project,
        } = node
        {
            if name == table {
                columns.extend(project.iter().cloned());
            }
        }
    }
    columns.sort_by(|left, right| {
        left.relation
            .cmp(&right.relation)
            .then_with(|| left.name.cmp(&right.name))
    });
    columns.dedup();
    columns
}

fn input_nodes(graph: &MirGraph, node: NodeIndex, edge: MirEdgeKind) -> Vec<NodeIndex> {
    graph
        .graph()
        .edges_directed(node, Direction::Incoming)
        .filter(|candidate| *candidate.weight() == edge)
        .map(|candidate| candidate.source())
        .collect()
}

#[cfg(test)]
mod tests {
    use palimpsest_sql::mir::{ColumnRef, JoinKind, MirGraph, MirNodeKind};

    use super::{base_tables, plan_upquery, referenced_columns, StaticPrimaryKeys, UpqueryRequest};

    fn join_graph() -> MirGraph {
        let mut graph = MirGraph::new(MirNodeKind::BaseTable {
            table: "posts".to_owned(),
            project: vec![ColumnRef {
                relation: Some("posts".to_owned()),
                name: "id".to_owned(),
            }],
        });
        let posts = graph.root();
        let authors = graph.add_node(MirNodeKind::BaseTable {
            table: "authors".to_owned(),
            project: vec![ColumnRef {
                relation: Some("authors".to_owned()),
                name: "id".to_owned(),
            }],
        });
        let join = graph.add_node(MirNodeKind::Join {
            kind: JoinKind::Inner,
            on: vec![(
                ColumnRef {
                    relation: Some("posts".to_owned()),
                    name: "author_id".to_owned(),
                },
                ColumnRef {
                    relation: Some("authors".to_owned()),
                    name: "id".to_owned(),
                },
            )],
        });
        graph.add_input(posts, join);
        graph.add_input(authors, join);
        graph.set_root(join);
        graph
    }

    #[test]
    fn plan_upquery_emits_one_request_per_base_table() {
        let graph = join_graph();
        let primary_keys = StaticPrimaryKeys::from_iter([("posts", "id"), ("authors", "id")]);

        let plan = plan_upquery(&graph, &["7".to_owned(), "9".to_owned()], &primary_keys);
        assert_eq!(plan.len(), 2);

        let mut requests = plan.requests;
        requests.sort_by(|left, right| left.table.cmp(&right.table));
        assert_eq!(
            requests,
            [
                UpqueryRequest {
                    table: "authors".to_owned(),
                    primary_key: "id".to_owned(),
                    keys: vec!["7".to_owned(), "9".to_owned()],
                },
                UpqueryRequest {
                    table: "posts".to_owned(),
                    primary_key: "id".to_owned(),
                    keys: vec!["7".to_owned(), "9".to_owned()],
                },
            ]
        );
    }

    #[test]
    fn plan_upquery_skips_tables_with_unknown_primary_key() {
        let graph = join_graph();
        let primary_keys = StaticPrimaryKeys::from_iter([("posts", "id")]);

        let plan = plan_upquery(&graph, &["1".to_owned()], &primary_keys);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.requests[0].table, "posts");
    }

    #[test]
    fn upquery_to_sql_quotes_values_and_escapes_inner_quotes() {
        let request = UpqueryRequest {
            table: "posts".to_owned(),
            primary_key: "id".to_owned(),
            keys: vec!["a".to_owned(), "b'c".to_owned()],
        };
        assert_eq!(
            request.to_sql(),
            "SELECT * FROM posts WHERE id IN ('a', 'b''c')"
        );
    }

    #[test]
    fn base_tables_walks_through_filter_and_join() {
        let graph = join_graph();
        assert_eq!(base_tables(&graph), ["authors", "posts"]);
    }

    #[test]
    fn referenced_columns_collects_projected_columns_for_base_table() {
        let graph = join_graph();
        assert_eq!(
            referenced_columns(&graph, "posts"),
            [ColumnRef {
                relation: Some("posts".to_owned()),
                name: "id".to_owned(),
            }]
        );
    }
}
