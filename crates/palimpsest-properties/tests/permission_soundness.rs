// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §15.3 property 6 — Permission soundness.
//!
//! For an arbitrary `(rules, user_ctx)`, no row in client output may
//! fail the rule predicate. We assert the structural form: rewriting
//! `SELECT * FROM <t>` with a single rule splices a `Filter` above the
//! base table, so any row reaching the root must satisfy the rule.
//!
//! Rule *composition* is pinned here too: multiple rules on one table
//! combine **disjunctively** (Postgres RLS `PERMISSIVE` semantics, per
//! `docs/PERMISSIONS.md` "How rules compose") — a row is visible iff at
//! least one rule admits it.

use palimpsest_dataflow::palimpsest::eval::{compile_predicate, ScalarSchema};
use palimpsest_permissions::{
    compile_rule, compile_rules, rewrite, PermissionRule, UserContext, UserContextSchema, UserValue,
};
use palimpsest_sql::{
    lower::parse_and_lower,
    mir::{MirGraph, MirNodeKind},
    Catalog, ColumnSchema, ColumnType, TableSchema,
};
use palimpsest_wal::Datum;
use proptest::prelude::*;
use smallvec::smallvec;

fn catalog() -> Catalog {
    Catalog::new([TableSchema::new(
        "events",
        vec![
            ColumnSchema::new("id", ColumnType::Int),
            ColumnSchema::new("tenant_id", ColumnType::Int),
            ColumnSchema::new("priority", ColumnType::Int),
        ],
    )])
}

/// Extracts the single spliced permission `Filter` predicate from a
/// rewritten graph.
fn spliced_predicate(graph: &MirGraph) -> String {
    let filters: Vec<_> = graph
        .node_kinds()
        .filter_map(|node| match node {
            MirNodeKind::Filter { predicate } => Some(predicate.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(filters.len(), 1, "expected exactly one spliced filter");
    filters.into_iter().next().expect("one filter")
}

fn events_scalar_schema() -> ScalarSchema {
    ScalarSchema::from_pairs([
        ("id".to_owned(), ColumnType::Int),
        ("tenant_id".to_owned(), ColumnType::Int),
        ("priority".to_owned(), ColumnType::Int),
    ])
}

fn event_row(id: i64, tenant_id: i64, priority: i64) -> palimpsest_dataflow::palimpsest::Row {
    smallvec![Datum::I64(id), Datum::I64(tenant_id), Datum::I64(priority)]
}

/// Two rules on one table, each of which alone admits a *different*
/// row: disjunctive (PERMISSIVE) composition must admit both rows and
/// still exclude rows neither rule admits.
#[test]
fn two_rules_on_one_table_admit_the_union_of_their_rows() {
    let cat = catalog();
    let user_schema = UserContextSchema::new([
        ("tenant".to_owned(), ColumnType::Int),
        ("prio".to_owned(), ColumnType::Int),
    ]);
    let rules = compile_rules(
        &[
            PermissionRule::new("by_tenant", "events", "tenant_id = $user.tenant"),
            PermissionRule::new("by_priority", "events", "priority = $user.prio"),
        ],
        &cat,
        &user_schema,
    )
    .expect("compile");
    let ctx = UserContext::new([
        ("tenant".to_owned(), UserValue::Int(1)),
        ("prio".to_owned(), UserValue::Int(42)),
    ]);

    let graph = parse_and_lower("SELECT id FROM events").unwrap();
    let outcome = rewrite(&graph, &rules, &ctx).expect("rewrite");
    assert_eq!(outcome.stats.filters_inserted, 1);

    let predicate = compile_predicate(&spliced_predicate(&outcome.graph), &events_scalar_schema())
        .expect("predicate compiles in the dataflow evaluator");

    let only_tenant_matches = event_row(1, 1, 0);
    let only_priority_matches = event_row(2, 2, 42);
    let neither_matches = event_row(3, 2, 0);
    assert!(predicate(&only_tenant_matches), "rule 1 alone must admit");
    assert!(predicate(&only_priority_matches), "rule 2 alone must admit");
    assert!(!predicate(&neither_matches), "no rule admits this row");
}

/// The documented predicate `team_id = ANY($user.team_ids)`
/// (docs/PERMISSIONS.md "Scoped read with admin override") must compile
/// and evaluate: a user in N teams sees rows from all N teams and from
/// no others.
#[test]
fn list_valued_user_context_evaluates_membership() {
    let cat = Catalog::new([TableSchema::new(
        "tasks",
        vec![
            ColumnSchema::new("id", ColumnType::Int),
            ColumnSchema::new("team_id", ColumnType::Int),
        ],
    )]);
    let user_schema = UserContextSchema::new([("team_ids".to_owned(), ColumnType::Int)]);
    let rules = compile_rules(
        &[PermissionRule::new(
            "team_read",
            "tasks",
            "team_id = ANY($user.team_ids)",
        )],
        &cat,
        &user_schema,
    )
    .expect("documented ANY($user.*) predicate compiles");

    let ctx = UserContext::new([(
        "team_ids".to_owned(),
        UserValue::List(vec![UserValue::Int(1), UserValue::Int(3)]),
    )]);
    ctx.validate(&user_schema).expect("list context validates");

    let graph = parse_and_lower("SELECT id FROM tasks").unwrap();
    let outcome = rewrite(&graph, &rules, &ctx).expect("rewrite");
    let schema = ScalarSchema::from_pairs([
        ("id".to_owned(), ColumnType::Int),
        ("team_id".to_owned(), ColumnType::Int),
    ]);
    let predicate = compile_predicate(&spliced_predicate(&outcome.graph), &schema)
        .expect("materialized ANY(ARRAY[...]) evaluates in the dataflow");

    let team = |team_id: i64| -> palimpsest_dataflow::palimpsest::Row {
        smallvec![Datum::I64(100 + team_id), Datum::I64(team_id)]
    };
    assert!(predicate(&team(1)), "member team must be visible");
    assert!(predicate(&team(3)), "member team must be visible");
    assert!(!predicate(&team(2)), "non-member team must be hidden");
    assert!(!predicate(&team(4)), "non-member team must be hidden");
}

/// Sharing behaviour for list-valued context: the canonical subgraph
/// key treats a team list as a *set* — order and duplicates don't fork
/// a private dataflow; genuinely different team sets do.
#[test]
fn list_valued_user_context_canonical_key_is_set_semantics() {
    use palimpsest_server::router::canonical_subgraph_key;
    use palimpsest_server::subscription::QueryId;

    let query = QueryId::new("SELECT id FROM tasks");
    let key = |teams: Vec<i64>| {
        let ctx = UserContext::new([(
            "team_ids".to_owned(),
            UserValue::List(teams.into_iter().map(UserValue::Int).collect()),
        )]);
        canonical_subgraph_key(&query, &ctx)
    };

    assert_eq!(key(vec![1, 2]), key(vec![2, 1, 1]));
    assert_ne!(key(vec![1, 2]), key(vec![1, 3]));
}

proptest! {
    /// Pin the composition semantics: for arbitrary thresholds and row
    /// values, a row passes the composed filter iff at least one rule's
    /// predicate holds (disjunction), never only-if-all-hold
    /// (conjunction).
    #[test]
    fn multiple_rules_compose_disjunctively(
        lo in -512_i64..512,
        hi in -512_i64..512,
        priority in -512_i64..512,
    ) {
        let cat = catalog();
        let user_schema = UserContextSchema::new([
            ("lo".to_owned(), ColumnType::Int),
            ("hi".to_owned(), ColumnType::Int),
        ]);
        let rules = compile_rules(
            &[
                PermissionRule::new("below", "events", "priority < $user.lo"),
                PermissionRule::new("above", "events", "priority > $user.hi"),
            ],
            &cat,
            &user_schema,
        )
        .expect("compile");
        let ctx = UserContext::new([
            ("lo".to_owned(), UserValue::Int(lo)),
            ("hi".to_owned(), UserValue::Int(hi)),
        ]);

        let graph = parse_and_lower("SELECT id FROM events").unwrap();
        let outcome = rewrite(&graph, &rules, &ctx).expect("rewrite");
        let predicate =
            compile_predicate(&spliced_predicate(&outcome.graph), &events_scalar_schema())
                .expect("predicate compiles");

        let row = event_row(1, 1, priority);
        let expected = priority < lo || priority > hi;
        prop_assert_eq!(predicate(&row), expected,
            "row with priority {} under rules (< {}) OR (> {})", priority, lo, hi);
    }

    #[test]
    fn rewrite_inserts_filter_for_visibility_rule(predicate_value in 0_i64..1024) {
        let cat = catalog();
        let user_schema = UserContextSchema::default();
        let rule = PermissionRule::new(
            "events_priority",
            "events",
            format!("priority < {predicate_value}"),
        );
        let compiled = compile_rule(&rule, &cat, &user_schema).expect("compile");

        let graph = parse_and_lower("SELECT id FROM events").unwrap();
        let outcome = rewrite(&graph, &[compiled], &UserContext::new(std::iter::empty()))
            .expect("rewrite");

        prop_assert_eq!(outcome.stats.filters_inserted, 1,
            "soundness requires a Filter for every applicable rule");
    }
}
