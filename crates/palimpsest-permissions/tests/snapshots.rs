// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! AST-before / AST-after fixtures for the permission rewriter (\u{00a7}18.6).

use palimpsest_sql::{canonical::canonical_form, lower::parse_and_lower, Catalog, ColumnType};

use palimpsest_permissions::{
    compile_rules, rewrite, Mode, PermissionRule, UserContext, UserContextSchema, UserValue,
};

macro_rules! assert_permissions_snapshot {
    ($name:literal, $value:expr) => {
        insta::with_settings!({ snapshot_path => "snapshots" }, {
            insta::assert_snapshot!($name, $value);
        });
    };
}

fn schema() -> UserContextSchema {
    UserContextSchema::new([
        ("id".to_owned(), ColumnType::Int),
        ("org_id".to_owned(), ColumnType::Int),
        ("is_admin".to_owned(), ColumnType::Bool),
    ])
}

fn alice() -> UserContext {
    UserContext::new([
        ("id".to_owned(), UserValue::Int(42)),
        ("org_id".to_owned(), UserValue::Int(7)),
        ("is_admin".to_owned(), UserValue::Bool(false)),
    ])
}

fn render(rules: &[PermissionRule], query: &str, context: &UserContext) -> String {
    let before = parse_and_lower(query).expect("query lowers");
    let compiled = compile_rules(rules, &Catalog::demo(), &schema()).expect("rules compile");
    let outcome = rewrite(&before, &compiled, context).expect("rewrite succeeds");
    format!(
        "rules:\n{}\n\nquery:\n  {query}\n\ncanonical_before:\n  {}\n\ncanonical_after:\n  {}\n\nstats: {:?}",
        rules
            .iter()
            .map(|rule| format!(
                "  - name={} table={} mode={:?} predicate={}",
                rule.name, rule.table, rule.mode, rule.predicate
            ))
            .collect::<Vec<_>>()
            .join("\n"),
        canonical_form(&before),
        canonical_form(&outcome.graph),
        outcome.stats,
    )
}

#[test]
fn rewrite_single_table_filter() {
    let rules = vec![PermissionRule::new(
        "posts_owner",
        "posts",
        "author_id = $user.id",
    )];
    assert_permissions_snapshot!(
        "rewrite_single_table_filter",
        render(&rules, "SELECT id FROM posts", &alice())
    );
}

#[test]
fn rewrite_composes_two_rules_on_same_table_disjunctively() {
    let rules = vec![
        PermissionRule::new("posts_owner", "posts", "author_id = $user.id"),
        PermissionRule::new("posts_org", "posts", "author_id = $user.org_id"),
    ];
    assert_permissions_snapshot!(
        "rewrite_composes_two_rules_on_same_table_disjunctively",
        render(&rules, "SELECT id FROM posts", &alice())
    );
}

#[test]
fn rewrite_join_filters_each_basetable() {
    let rules = vec![
        PermissionRule::new("posts_owner", "posts", "author_id = $user.id"),
        PermissionRule::new("authors_self", "authors", "id = $user.id"),
    ];
    assert_permissions_snapshot!(
        "rewrite_join_filters_each_basetable",
        render(
            &rules,
            "SELECT posts.id
             FROM posts JOIN authors ON posts.author_id = authors.id",
            &alice(),
        )
    );
}

#[test]
fn rewrite_elides_tautology() {
    let rules = vec![
        PermissionRule::new("everything", "posts", "true"),
        PermissionRule::new("posts_owner", "posts", "author_id = $user.id"),
    ];
    assert_permissions_snapshot!(
        "rewrite_elides_tautology",
        render(&rules, "SELECT id FROM posts", &alice())
    );
}

#[test]
fn rewrite_skips_subscribe_only_rule() {
    let rules =
        vec![PermissionRule::new("p", "posts", "author_id = $user.id").with_mode(Mode::Subscribe)];
    assert_permissions_snapshot!(
        "rewrite_skips_subscribe_only_rule",
        render(&rules, "SELECT id FROM posts", &alice())
    );
}

#[test]
fn rewrite_no_matching_rules_is_identity() {
    let rules = vec![PermissionRule::new(
        "authors_self",
        "authors",
        "id = $user.id",
    )];
    assert_permissions_snapshot!(
        "rewrite_no_matching_rules_is_identity",
        render(&rules, "SELECT id FROM posts", &alice())
    );
}

#[test]
fn canonical_key_shares_when_user_context_matches() {
    let rules = vec![PermissionRule::new(
        "posts_org",
        "posts",
        "author_id = $user.org_id",
    )];
    let compiled = compile_rules(&rules, &Catalog::demo(), &schema()).unwrap();
    let graph = parse_and_lower("SELECT id FROM posts").unwrap();

    let alice = UserContext::new([("org_id".to_owned(), UserValue::Int(7))]);
    let bob = UserContext::new([("org_id".to_owned(), UserValue::Int(7))]);
    let carol = UserContext::new([("org_id".to_owned(), UserValue::Int(99))]);

    let alice_graph = rewrite(&graph, &compiled, &alice).unwrap().graph;
    let bob_graph = rewrite(&graph, &compiled, &bob).unwrap().graph;
    let carol_graph = rewrite(&graph, &compiled, &carol).unwrap().graph;

    assert_eq!(canonical_form(&alice_graph), canonical_form(&bob_graph));
    assert_ne!(canonical_form(&alice_graph), canonical_form(&carol_graph));
}
