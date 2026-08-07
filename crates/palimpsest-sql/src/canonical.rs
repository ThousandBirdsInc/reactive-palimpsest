// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Canonical-form fingerprint and reused-subgraph collapsing. Internal
//! to MIR processing — exposed for callers that compare/dedupe graphs.

#![allow(missing_docs)]

use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
};

use petgraph::{graph::NodeIndex, visit::EdgeRef, Direction, Graph};

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

#[must_use]
pub fn collapse_reused_subgraphs(graph: &MirGraph) -> MirGraph {
    let mut rebuilt = Graph::new();
    let mut interned = HashMap::new();
    let mut stack = HashSet::new();
    let (root, _) = rebuild_node(graph, graph.root(), &mut rebuilt, &mut interned, &mut stack);
    MirGraph::from_graph(rebuilt, root)
}

fn rebuild_node(
    source: &MirGraph,
    node_index: NodeIndex,
    rebuilt: &mut Graph<MirNodeKind, MirEdgeKind>,
    interned: &mut HashMap<String, NodeIndex>,
    stack: &mut HashSet<NodeIndex>,
) -> (NodeIndex, String) {
    assert!(stack.insert(node_index), "MIR graph contains a cycle");

    let commutative = matches!(
        source.graph()[node_index],
        MirNodeKind::Union { .. } | MirNodeKind::Intersect { .. }
    );
    let mut inputs = source
        .graph()
        .edges_directed(node_index, Direction::Incoming)
        .map(|edge| {
            let (child, child_signature) =
                rebuild_node(source, edge.source(), rebuilt, interned, stack);
            (*edge.weight(), child, child_signature)
        })
        .collect::<Vec<_>>();

    // Sort for a deterministic signature regardless of petgraph edge
    // iteration order. Commutative set ops drop the input ordinal from
    // the edge name so swapped branches still intern identically;
    // order-sensitive nodes keep it, so the ordinal dominates the sort.
    inputs.sort_by(|left, right| {
        (edge_kind_name(left.0, commutative), &left.2)
            .cmp(&(edge_kind_name(right.0, commutative), &right.2))
    });

    let input_signature = inputs
        .iter()
        .map(|(edge, _, child)| format!("{}:{child}", edge_kind_name(*edge, commutative)))
        .collect::<Vec<_>>()
        .join(",");
    let signature = format!(
        "{}[{input_signature}]",
        canonical_node_kind(&source.graph()[node_index])
    );

    if let Some(existing) = interned.get(&signature) {
        stack.remove(&node_index);
        return (*existing, signature);
    }

    let rebuilt_node = rebuilt.add_node(source.graph()[node_index].clone());
    // Edge weights carry the input ordinal, so insertion order no
    // longer matters — executors recover left/right (and base/step)
    // from the ordinal, not from petgraph iteration order.
    for (edge, child, _) in inputs {
        rebuilt.add_edge(child, rebuilt_node, edge);
    }
    interned.insert(signature.clone(), rebuilt_node);
    stack.remove(&node_index);
    (rebuilt_node, signature)
}

fn canonical_node(
    graph: &MirGraph,
    node_index: NodeIndex,
    stack: &mut HashSet<NodeIndex>,
) -> String {
    assert!(stack.insert(node_index), "MIR graph contains a cycle");

    let commutative = matches!(
        graph.graph()[node_index],
        MirNodeKind::Union { .. } | MirNodeKind::Intersect { .. }
    );
    let mut inputs = graph
        .graph()
        .edges_directed(node_index, Direction::Incoming)
        .map(|edge| {
            let edge_kind = edge_kind_name(*edge.weight(), commutative);
            format!(
                "{edge_kind}:{}",
                canonical_node(graph, edge.source(), stack)
            )
        })
        .collect::<Vec<_>>();

    // Deterministic regardless of edge iteration order: ordinals in
    // the edge names keep order-sensitive inputs (join left/right,
    // fixpoint base/step) apart, while commutative set-op inputs sort
    // purely by child signature.
    inputs.sort();

    let node = canonical_node_kind(&graph.graph()[node_index]);
    stack.remove(&node_index);
    format!("{node}[{}]", inputs.join(","))
}

fn edge_kind_name(edge: MirEdgeKind, strip_ordinal: bool) -> String {
    match edge {
        MirEdgeKind::Input(ordinal) => {
            if strip_ordinal {
                "input".to_owned()
            } else {
                format!("input{ordinal:03}")
            }
        }
        MirEdgeKind::CteExpansion => "cte".to_owned(),
    }
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
        MirNodeKind::DistinctOn { on, order_by } => {
            format!("distinct-on:{}:{}", on.join(","), canonical_debug(order_by))
        }
        MirNodeKind::Union { quantifier } => format!("union:{quantifier:?}"),
        MirNodeKind::Except { quantifier } => format!("except:{quantifier:?}"),
        MirNodeKind::Intersect { quantifier } => format!("intersect:{quantifier:?}"),
        MirNodeKind::TopK {
            order_by,
            limit,
            offset,
        } => format!("topk:{}:{limit}:{offset}", canonical_debug(order_by)),
        MirNodeKind::CteRef { .. } => "cte-ref".to_owned(),
        // Recursive nodes keep the CTE name: a `RecursiveRef` has no
        // input edges, so without the name two distinct recursive CTEs
        // would produce byte-identical signatures and
        // `collapse_reused_subgraphs` would wrongly merge their
        // self-references.
        MirNodeKind::Fixpoint { cte, union_all } => format!("fixpoint:{cte}:{union_all}"),
        MirNodeKind::RecursiveRef { cte } => format!("recursive-ref:{cte}"),
        MirNodeKind::Leaf { name } => format!("leaf:{name}"),
    }
}

fn canonical_debug<T: core::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use crate::{
        canonical::{canonical_form, canonical_key, collapse_reused_subgraphs},
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

    #[test]
    fn recursive_ctes_canonicalize_without_cycles() {
        let sql = "WITH RECURSIVE reach AS (
                SELECT id, post_id FROM comments WHERE post_id = 1
                UNION
                SELECT comments.id, comments.post_id
                FROM comments JOIN reach ON comments.post_id = reach.id
             )
             SELECT id FROM reach";
        let graph = parse_and_lower(sql).expect("recursive CTE should lower");
        let other = parse_and_lower(
            "WITH RECURSIVE reach AS (
                SELECT id, post_id FROM comments WHERE post_id = 2
                UNION
                SELECT comments.id, comments.post_id
                FROM comments JOIN reach ON comments.post_id = reach.id
             )
             SELECT id FROM reach",
        )
        .expect("recursive CTE should lower");

        // Canonicalization terminates (the fixpoint graph is acyclic)
        // and distinguishes different recursions.
        assert_eq!(canonical_key(&graph), canonical_key(&graph));
        assert_ne!(canonical_key(&graph), canonical_key(&other));

        let collapsed = collapse_reused_subgraphs(&graph);
        assert_eq!(canonical_key(&graph), canonical_key(&collapsed));
    }

    #[test]
    fn distinct_on_keys_participate_in_canonical_form() {
        let by_author = parse_and_lower(
            "SELECT DISTINCT ON (author_id) id, author_id FROM posts ORDER BY author_id",
        )
        .expect("DISTINCT ON should lower");
        let by_id = parse_and_lower("SELECT DISTINCT ON (id) id, author_id FROM posts ORDER BY id")
            .expect("DISTINCT ON should lower");

        assert_ne!(canonical_key(&by_author), canonical_key(&by_id));
    }

    #[test]
    fn collapse_reused_subgraphs_shares_duplicate_branches() {
        let graph = parse_and_lower(
            "SELECT id FROM posts
             UNION ALL
             SELECT id FROM posts",
        )
        .expect("query should lower");

        let collapsed = collapse_reused_subgraphs(&graph);

        assert!(collapsed.node_count() < graph.node_count());
        assert_eq!(canonical_key(&graph), canonical_key(&collapsed));
    }
}
