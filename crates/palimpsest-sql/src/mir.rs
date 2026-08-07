// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Mid-level IR.
//!
//! Each variant of [`MirNode`] corresponds to a relational operator
//! the dataflow engine knows how to instantiate. Internal — consumers
//! should treat the graph as opaque and only inspect it through the
//! helpers re-exported from the crate root.

#![allow(missing_docs)]

use std::collections::HashMap;

use petgraph::{graph::NodeIndex, visit::EdgeRef, Direction, Graph};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    /// Left semi-join: emits each left row once if at least one right
    /// row matches. Produced by decorrelated `EXISTS` subqueries.
    Semi,
    /// Left anti-join: emits each left row once if no right row
    /// matches. Produced by decorrelated `NOT EXISTS` subqueries.
    Anti,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetQuantifierKind {
    Distinct,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub relation: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    pub expression: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggExpr {
    pub function: String,
    pub args: Vec<String>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MirNodeKind {
    BaseTable {
        table: String,
        project: Vec<ColumnRef>,
    },
    Filter {
        predicate: String,
    },
    Project {
        columns: Vec<String>,
    },
    Join {
        kind: JoinKind,
        on: Vec<(ColumnRef, ColumnRef)>,
    },
    Aggregate {
        group_by: Vec<ColumnRef>,
        aggs: Vec<AggExpr>,
    },
    Distinct,
    /// Postgres `SELECT DISTINCT ON (exprs) ...`: keeps the first row
    /// of each `on`-group. When `order_by` is non-empty rows are
    /// ranked by those keys before picking; otherwise the surviving
    /// row per group is arbitrary (matching Postgres).
    DistinctOn {
        on: Vec<String>,
        order_by: Vec<OrderKey>,
    },
    Union {
        quantifier: SetQuantifierKind,
    },
    Except {
        quantifier: SetQuantifierKind,
    },
    Intersect {
        quantifier: SetQuantifierKind,
    },
    TopK {
        order_by: Vec<OrderKey>,
        limit: usize,
        offset: usize,
    },
    CteRef {
        cte: String,
    },
    /// Fixpoint for a `WITH RECURSIVE` CTE. Takes exactly two `Input`
    /// edges: the base (non-recursive) term first, then the recursive
    /// step term. The step subgraph reads the previous iteration's
    /// rows through a [`MirNodeKind::RecursiveRef`] naming the same
    /// CTE. `union_all` distinguishes `UNION ALL` (bag semantics)
    /// from `UNION` (rows deduplicated across iterations).
    Fixpoint {
        cte: String,
        union_all: bool,
    },
    /// Reference to the enclosing [`MirNodeKind::Fixpoint`]'s working
    /// table. Has no input edges — executors resolve it by CTE name
    /// against the innermost fixpoint being evaluated.
    RecursiveRef {
        cte: String,
    },
    Leaf {
        name: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirEdgeKind {
    /// Dataflow input edge. The payload is the input's position on the
    /// consuming node (`0` = first/left input), assigned by
    /// [`MirGraph::add_input`] in call order. Positions — not petgraph
    /// edge indices, which get recycled by [`MirGraph::splice_above`]'s
    /// edge removal — are what executors must use to tell a join's
    /// left side from its right (and a fixpoint's base term from its
    /// step term).
    Input(u8),
    CteExpansion,
}

#[derive(Debug, Clone)]
pub struct MirGraph {
    graph: Graph<MirNodeKind, MirEdgeKind>,
    root: NodeIndex,
}

impl MirGraph {
    #[must_use]
    pub fn new(root: MirNodeKind) -> Self {
        let mut graph = Graph::new();
        let root = graph.add_node(root);
        Self { graph, root }
    }

    #[must_use]
    pub const fn root(&self) -> NodeIndex {
        self.root
    }

    #[must_use]
    pub const fn graph(&self) -> &Graph<MirNodeKind, MirEdgeKind> {
        &self.graph
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    #[must_use]
    pub fn root_kind(&self) -> &MirNodeKind {
        &self.graph[self.root]
    }

    pub fn node_kinds(&self) -> impl Iterator<Item = &MirNodeKind> {
        self.graph.node_weights()
    }

    pub fn set_root(&mut self, root: NodeIndex) {
        self.root = root;
    }

    pub fn add_input(&mut self, from: NodeIndex, to: NodeIndex) {
        let ordinal = self
            .graph
            .edges_directed(to, Direction::Incoming)
            .filter(|edge| matches!(edge.weight(), MirEdgeKind::Input(_)))
            .count();
        self.graph.add_edge(
            from,
            to,
            MirEdgeKind::Input(u8::try_from(ordinal).unwrap_or(u8::MAX)),
        );
    }

    pub fn add_cte_expansion(&mut self, from: NodeIndex, to: NodeIndex) {
        self.graph.add_edge(from, to, MirEdgeKind::CteExpansion);
    }

    pub fn add_node(&mut self, node: MirNodeKind) -> NodeIndex {
        self.graph.add_node(node)
    }

    pub fn append_graph(&mut self, other: &Self) -> NodeIndex {
        let mut node_map = HashMap::with_capacity(other.graph.node_count());

        for source in other.graph.node_indices() {
            let target = self.graph.add_node(other.graph[source].clone());
            node_map.insert(source, target);
        }

        for edge in other.graph.edge_references() {
            self.graph.add_edge(
                node_map[&edge.source()],
                node_map[&edge.target()],
                *edge.weight(),
            );
        }

        node_map[&other.root]
    }

    pub(crate) const fn from_graph(
        graph: Graph<MirNodeKind, MirEdgeKind>,
        root: NodeIndex,
    ) -> Self {
        Self { graph, root }
    }

    /// Inserts `kind` immediately above `target`, redirecting every
    /// outgoing edge from `target` (i.e. each consumer that read from
    /// `target`) to read from the newly-spliced node instead. The new
    /// node receives a single incoming `Input` edge from `target`.
    ///
    /// If `target` was the graph root, the spliced node becomes the new
    /// root.
    ///
    /// Returns the index of the newly-inserted node.
    pub fn splice_above(&mut self, target: NodeIndex, kind: MirNodeKind) -> NodeIndex {
        let inserted = self.graph.add_node(kind);

        let outgoing: Vec<_> = self
            .graph
            .edges_directed(target, Direction::Outgoing)
            .map(|edge| (edge.id(), edge.target(), *edge.weight()))
            .collect();
        for (edge_id, consumer, weight) in outgoing {
            self.graph.remove_edge(edge_id);
            self.graph.add_edge(inserted, consumer, weight);
        }

        self.graph.add_edge(target, inserted, MirEdgeKind::Input(0));

        if self.root == target {
            self.root = inserted;
        }

        inserted
    }

    /// Returns the node's `Input` sources in position order (`0`
    /// first). This is the only correct way to identify a join's left
    /// vs right input (or a fixpoint's base vs step term): petgraph
    /// edge indices are recycled by [`Self::splice_above`]'s edge
    /// removal, and node indices of spliced filters exceed both
    /// original inputs'.
    #[must_use]
    pub fn ordered_inputs(&self, node: NodeIndex) -> Vec<NodeIndex> {
        let mut inputs: Vec<(u8, NodeIndex)> = self
            .graph
            .edges_directed(node, Direction::Incoming)
            .filter_map(|edge| match edge.weight() {
                MirEdgeKind::Input(ordinal) => Some((*ordinal, edge.source())),
                MirEdgeKind::CteExpansion => None,
            })
            .collect();
        inputs.sort_by_key(|(ordinal, _)| *ordinal);
        inputs.into_iter().map(|(_, source)| source).collect()
    }

    /// Returns every node index whose payload is a `BaseTable`.
    #[must_use]
    pub fn base_table_indices(&self) -> Vec<NodeIndex> {
        self.graph
            .node_indices()
            .filter(|index| matches!(self.graph[*index], MirNodeKind::BaseTable { .. }))
            .collect()
    }

    /// Returns the kind stored at `index`.
    #[must_use]
    pub fn node_kind(&self, index: NodeIndex) -> &MirNodeKind {
        &self.graph[index]
    }

    /// Mutable access to the kind stored at `index`.
    pub fn node_kind_mut(&mut self, index: NodeIndex) -> &mut MirNodeKind {
        &mut self.graph[index]
    }
}

#[cfg(test)]
mod tests {
    use super::{JoinKind, MirGraph, MirNodeKind};

    fn base(table: &str) -> MirNodeKind {
        MirNodeKind::BaseTable {
            table: table.to_owned(),
            project: Vec::new(),
        }
    }

    #[test]
    fn ordered_inputs_survive_splice_above_left_input() {
        // splice_above removes edges, which recycles petgraph edge
        // indices, and adds a node with a higher index than either
        // join input — neither may disturb left/right identity.
        let mut graph = MirGraph::new(base("left"));
        let left = graph.root();
        let right = graph.add_node(base("right"));
        let join = graph.add_node(MirNodeKind::Join {
            kind: JoinKind::Left,
            on: Vec::new(),
        });
        graph.add_input(left, join);
        graph.add_input(right, join);
        graph.set_root(join);

        assert_eq!(graph.ordered_inputs(join), vec![left, right]);

        let filter = graph.splice_above(
            left,
            MirNodeKind::Filter {
                predicate: "visible = true".to_owned(),
            },
        );
        assert_eq!(
            graph.ordered_inputs(join),
            vec![filter, right],
            "spliced filter must take the left slot"
        );
        assert_eq!(graph.ordered_inputs(filter), vec![left]);
    }
}
