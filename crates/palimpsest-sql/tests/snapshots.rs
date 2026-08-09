// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use petgraph::{visit::EdgeRef, Direction};

use palimpsest_sql::{
    canonical::{canonical_form, collapse_reused_subgraphs},
    parse_and_lower, parse_and_normalize, parse_select, Catalog,
};

macro_rules! assert_sql_snapshot {
    ($name:literal, $value:expr) => {
        insta::with_settings!({ snapshot_path => "../snapshots" }, {
            insta::assert_snapshot!($name, $value);
        });
    };
}

const FIXTURE: &str = "
    WITH recent_posts AS (
        SELECT id, author_id, created_at
        FROM posts
        WHERE author_id IN (42, 7) AND created_at IS NOT NULL
    )
    SELECT recent_posts.id AS post_id, authors.name
    FROM recent_posts
    JOIN authors ON recent_posts.author_id = authors.id
    ORDER BY post_id
    LIMIT 10
";

#[test]
fn snapshot_parser_ast() {
    let ast = parse_select(FIXTURE).expect("fixture should parse");

    assert_sql_snapshot!("sql_ast", format!("{ast:#?}"));
}

#[test]
fn snapshot_mir_raw() {
    let mir = parse_and_lower(FIXTURE).expect("fixture should lower");

    assert_sql_snapshot!("mir_raw", format!("{mir:#?}"));
}

#[test]
fn snapshot_mir_canonicalized() {
    let mir = parse_and_lower(FIXTURE).expect("fixture should lower");
    let canonicalized = collapse_reused_subgraphs(&mir);

    assert_sql_snapshot!(
        "mir_canonicalized",
        format!(
            "canonical_form:\n{}\n\ncollapsed_mir:\n{canonicalized:#?}",
            canonical_form(&canonicalized)
        )
    );
}

#[test]
fn snapshot_permission_rewritten_ast() {
    let normalized = parse_and_normalize(FIXTURE, &Catalog::demo())
        .expect("fixture should normalize against demo catalog");

    assert_sql_snapshot!("permission_rewritten_ast", normalized.to_string());
}

#[test]
fn snapshot_build_plan() {
    let mir = parse_and_lower(FIXTURE).expect("fixture should lower");

    assert_sql_snapshot!("build_plan", render_build_plan(&mir));
}

fn render_build_plan(mir: &palimpsest_sql::mir::MirGraph) -> String {
    let mut lines = Vec::new();
    for node in mir.graph().node_indices() {
        let inputs = mir
            .graph()
            .edges_directed(node, Direction::Incoming)
            .map(|edge| format!("{}:{:?}", edge.source().index(), edge.weight()))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "{}: {:?} <- [{}]",
            node.index(),
            mir.graph()[node],
            inputs
        ));
    }
    lines.join("\n")
}
