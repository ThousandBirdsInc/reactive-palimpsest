// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use petgraph::{graph::NodeIndex, Graph};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
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
    Union,
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

    pub fn add_node(&mut self, node: MirNodeKind) -> NodeIndex {
        self.graph.add_node(node)
    }
}
