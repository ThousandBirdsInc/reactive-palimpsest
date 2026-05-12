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
    Leaf {
        name: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirEdgeKind {
    Input,
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
        self.graph.add_edge(from, to, MirEdgeKind::Input);
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

        self.graph.add_edge(target, inserted, MirEdgeKind::Input);

        if self.root == target {
            self.root = inserted;
        }

        inserted
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
}
