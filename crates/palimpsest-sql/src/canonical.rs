// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
};

use petgraph::{visit::EdgeRef, Direction};

use crate::mir::{MirEdgeKind, MirGraph, MirNodeKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CanonicalKey(u64);

impl CanonicalKey {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[must_use]
pub fn canonical_key(graph: &MirGraph) -> CanonicalKey {
    let mut hasher = DefaultHasher::new();
    canonical_form(graph).hash(&mut hasher);
    CanonicalKey(hasher.finish())
}

#[must_use]
pub fn canonical_form(graph: &MirGraph) -> String {
    let mut stack = HashSet::new();
    canonical_node(graph, graph.root(), &mut stack)
}

fn canonical_node(
    graph: &MirGraph,
    node_index: petgraph::graph::NodeIndex,
    stack: &mut HashSet<petgraph::graph::NodeIndex>,
) -> String {
    assert!(stack.insert(node_index), "MIR graph contains a cycle");

    let mut inputs = graph
        .graph()
        .edges_directed(node_index, Direction::Incoming)
        .map(|edge| {
            let edge_kind = match edge.weight() {
                MirEdgeKind::Input => "input",
                MirEdgeKind::CteExpansion => "cte",
            };
            format!(
                "{edge_kind}:{}",
                canonical_node(graph, edge.source(), stack)
            )
        })
        .collect::<Vec<_>>();

    if matches!(
        graph.graph()[node_index],
        MirNodeKind::Union { .. } | MirNodeKind::Intersect { .. }
    ) {
        inputs.sort();
    }

    let node = canonical_node_kind(&graph.graph()[node_index]);
    stack.remove(&node_index);
    format!("{node}[{}]", inputs.join(","))
}

fn canonical_node_kind(node: &MirNodeKind) -> String {
    match node {
        MirNodeKind::BaseTable { table, project } => {
            format!("base:{table}:{}", canonical_debug(project))
        }
        MirNodeKind::Filter { predicate } => format!("filter:{predicate}"),
        MirNodeKind::Project { columns } => format!("project:{}", columns.join(",")),
        MirNodeKind::Join { kind, on } => {
            format!("join:{kind:?}:{}", canonical_debug(on))
        }
        MirNodeKind::Aggregate { group_by, aggs } => {
            format!(
                "aggregate:{}:{}",
                canonical_debug(group_by),
                canonical_debug(aggs)
            )
        }
        MirNodeKind::Distinct => "distinct".to_owned(),
        MirNodeKind::Union { quantifier } => format!("union:{quantifier:?}"),
        MirNodeKind::Except { quantifier } => format!("except:{quantifier:?}"),
        MirNodeKind::Intersect { quantifier } => format!("intersect:{quantifier:?}"),
        MirNodeKind::TopK {
            order_by,
            limit,
            offset,
        } => format!("topk:{}:{limit}:{offset}", canonical_debug(order_by)),
        MirNodeKind::CteRef { .. } => "cte-ref".to_owned(),
        MirNodeKind::Leaf { name } => format!("leaf:{name}"),
    }
}

fn canonical_debug<T: core::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use crate::{
        canonical::{canonical_form, canonical_key},
        lower::parse_and_lower,
    };

    #[test]
    fn equivalent_filter_conjunctions_have_same_key() {
        let left = parse_and_lower(
            "SELECT id FROM posts
             WHERE author_id = 42 AND id = 7",
        )
        .expect("query should lower");
        let right = parse_and_lower(
            "SELECT id FROM posts
             WHERE id = 7 AND author_id = 42",
        )
        .expect("query should lower");

        assert_eq!(canonical_form(&left), canonical_form(&right));
        assert_eq!(canonical_key(&left), canonical_key(&right));
    }

    #[test]
    fn different_queries_have_different_keys() {
        let left = parse_and_lower("SELECT id FROM posts WHERE author_id = 42")
            .expect("query should lower");
        let right = parse_and_lower("SELECT id FROM posts WHERE author_id = 43")
            .expect("query should lower");

        assert_ne!(canonical_key(&left), canonical_key(&right));
    }

    #[test]
    fn normalized_literals_have_same_key() {
        let left = parse_and_lower("SELECT id FROM posts WHERE author_id = 00042")
            .expect("query should lower");
        let right = parse_and_lower("SELECT id FROM posts WHERE author_id = 42")
            .expect("query should lower");
        let escaped = parse_and_lower("SELECT id FROM posts WHERE title = E'hello'")
            .expect("query should lower");
        let quoted = parse_and_lower("SELECT id FROM posts WHERE title = 'hello'")
            .expect("query should lower");

        assert_eq!(canonical_form(&left), canonical_form(&right));
        assert_eq!(canonical_key(&escaped), canonical_key(&quoted));
    }

    #[test]
    fn cte_names_do_not_affect_canonical_key() {
        let left = parse_and_lower(
            "WITH recent_posts AS (
                SELECT id FROM posts WHERE author_id = 42
             )
             SELECT id FROM recent_posts",
        )
        .expect("query should lower");
        let right = parse_and_lower(
            "WITH visible_posts AS (
                SELECT id FROM posts WHERE author_id = 42
             )
             SELECT id FROM visible_posts",
        )
        .expect("query should lower");

        assert_eq!(canonical_key(&left), canonical_key(&right));
    }
}
