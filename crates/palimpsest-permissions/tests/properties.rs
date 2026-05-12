// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Permission rewriter property tests (\u{00a7}15.3 #6 and #7).
//!
//! Soundness — for any (rule, `user_ctx`, row) the row appears in the
//! rewritten output iff the rule predicate evaluates to true under
//! `user_ctx`.
//!
//! Liveness — when `user_ctx` changes such that a previously-rejected
//! row now satisfies the rule, the rewriter exposes that row, and
//! vice-versa.
//!
//! The rewriter operates on MIR strings rather than runtime tuples, so
//! these tests use a tiny evaluator covering the predicate shapes that
//! the v1 surface emits (`<col> = <int-literal>` after substitution).

#![allow(clippy::cast_possible_truncation)]

use palimpsest_permissions::{
    compile_rules, rewrite, PermissionRule, UserContext, UserContextSchema, UserValue,
};
use palimpsest_sql::{
    canonical::canonical_key,
    lower::parse_and_lower,
    mir::{MirGraph, MirNodeKind},
    Catalog, ColumnType,
};
use proptest::prelude::*;

#[derive(Debug, Clone, Copy)]
struct Row {
    id: i64,
    author_id: i64,
}

fn schema() -> UserContextSchema {
    UserContextSchema::new([
        ("id".to_owned(), ColumnType::Int),
        ("org_id".to_owned(), ColumnType::Int),
    ])
}

fn rules_for_test() -> Vec<PermissionRule> {
    vec![PermissionRule::new(
        "posts_owner",
        "posts",
        "author_id = $user.id",
    )]
}

fn collect_filter_predicates(graph: &MirGraph) -> Vec<String> {
    graph
        .node_kinds()
        .filter_map(|node| match node {
            MirNodeKind::Filter { predicate } => Some(predicate.clone()),
            _ => None,
        })
        .collect()
}

/// Tiny evaluator that handles `<col> = <int-literal>` predicates,
/// including the canonical reorder (`<int-literal> = <col>` is also
/// accepted because the lower-pass canonicalizes equality operands).
fn matches(filter: &str, row: Row) -> bool {
    let parts: Vec<&str> = filter.splitn(3, ' ').collect();
    assert_eq!(parts.len(), 3, "test predicate must be `lhs op rhs`");
    let (lhs, op, rhs) = (parts[0], parts[1], parts[2]);
    assert_eq!(op, "=", "test predicate must be equality");
    let column_value = match lhs {
        "id" => row.id,
        "author_id" => row.author_id,
        _ => panic!("unexpected lhs in test predicate: {lhs}"),
    };
    let literal: i64 = rhs
        .trim_matches('(')
        .trim_matches(')')
        .parse()
        .expect("test rhs must parse as integer");
    column_value == literal
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Soundness — every row that the rewritten filter accepts must
    /// satisfy the original rule under the user's context.
    #[test]
    fn permission_soundness(
        user_id in -8_i64..8,
        rows in prop::collection::vec((-8_i64..8, -8_i64..8), 0..16),
    ) {
        let context = UserContext::new([
            ("id".to_owned(), UserValue::Int(user_id)),
            ("org_id".to_owned(), UserValue::Int(0)),
        ]);
        let compiled = compile_rules(&rules_for_test(), &Catalog::demo(), &schema()).unwrap();
        let base = parse_and_lower("SELECT id FROM posts").unwrap();
        let outcome = rewrite(&base, &compiled, &context).unwrap();
        let filters = collect_filter_predicates(&outcome.graph);
        prop_assert_eq!(filters.len(), 1);

        let oracle = |row: Row| row.author_id == user_id;
        for (id, author_id) in rows {
            let row = Row { id, author_id };
            let rewriter_admits = matches(&filters[0], row);
            prop_assert_eq!(rewriter_admits, oracle(row),
                "rewriter and oracle disagreed: filter={} row={:?} user_id={}",
                filters[0], row, user_id);
        }
    }

    /// Liveness — flipping the user_ctx so a previously-rejected row
    /// matches must produce a different filter that admits the row.
    #[test]
    fn permission_liveness(
        author_id in -8_i64..8,
        before_user in -8_i64..8,
        after_user in -8_i64..8,
    ) {
        prop_assume!(before_user != after_user);
        prop_assume!(author_id == before_user || author_id == after_user);

        let row = Row { id: 0, author_id };
        let before = UserContext::new([
            ("id".to_owned(), UserValue::Int(before_user)),
            ("org_id".to_owned(), UserValue::Int(0)),
        ]);
        let after = UserContext::new([
            ("id".to_owned(), UserValue::Int(after_user)),
            ("org_id".to_owned(), UserValue::Int(0)),
        ]);
        let compiled = compile_rules(&rules_for_test(), &Catalog::demo(), &schema()).unwrap();
        let base = parse_and_lower("SELECT id FROM posts").unwrap();

        let before_graph = rewrite(&base, &compiled, &before).unwrap().graph;
        let after_graph = rewrite(&base, &compiled, &after).unwrap().graph;
        let before_filter = collect_filter_predicates(&before_graph).remove(0);
        let after_filter = collect_filter_predicates(&after_graph).remove(0);

        let before_admits = matches(&before_filter, row);
        let after_admits = matches(&after_filter, row);
        prop_assert_ne!(before_admits, after_admits,
            "user-context change must flip row visibility: before={} after={}",
            before_filter, after_filter);
    }

    /// Canonical-key sharing — two users with identical user_ctx values
    /// must produce identical canonical keys; a different value must
    /// produce a different key.
    #[test]
    fn canonical_key_splits_on_user_context(
        org_id in -8_i64..8,
        other_org_id in -8_i64..8,
    ) {
        let compiled = compile_rules(
            &[PermissionRule::new(
                "posts_org",
                "posts",
                "author_id = $user.org_id",
            )],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        let base = parse_and_lower("SELECT id FROM posts").unwrap();

        let alice = UserContext::new([("org_id".to_owned(), UserValue::Int(org_id))]);
        let bob = UserContext::new([("org_id".to_owned(), UserValue::Int(org_id))]);
        let carol = UserContext::new([("org_id".to_owned(), UserValue::Int(other_org_id))]);

        let alice_key = canonical_key(&rewrite(&base, &compiled, &alice).unwrap().graph);
        let bob_key = canonical_key(&rewrite(&base, &compiled, &bob).unwrap().graph);
        let carol_key = canonical_key(&rewrite(&base, &compiled, &carol).unwrap().graph);

        prop_assert_eq!(alice_key, bob_key);
        if org_id != other_org_id {
            prop_assert_ne!(alice_key, carol_key);
        }
    }
}
