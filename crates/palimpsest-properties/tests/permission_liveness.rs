// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §15.3 property 7 — Permission liveness.
//!
//! When a row that previously failed a permission becomes permitted
//! (because the data backing the permission changed), the client
//! receives an `Insert`; the reverse produces a `Delete`.
//!
//! We can't yet drive a full subscription through the dataflow engine
//! per row-flip in a property test, so this property is asserted on
//! the rewriter's *structural* contract: a rule with a tautology
//! predicate is elided (no filter spliced — equivalent to letting all
//! rows through), while a non-tautology rule splices a filter that
//! gates row flow. Both directions are exercised here.

use palimpsest_permissions::{
    compile_rule, rewrite, PermissionRule, UserContext, UserContextSchema,
};
use palimpsest_sql::{lower::parse_and_lower, Catalog, ColumnSchema, ColumnType, TableSchema};

fn catalog() -> Catalog {
    Catalog::new([TableSchema::new(
        "events",
        vec![
            ColumnSchema::new("id", ColumnType::Int),
            ColumnSchema::new("tenant_id", ColumnType::Int),
        ],
    )])
}

#[test]
fn flipping_predicate_to_tautology_unblocks_rows() {
    let cat = catalog();
    let user_schema = UserContextSchema::default();

    // Strict rule: only tenant 7 may pass.
    let strict = compile_rule(
        &PermissionRule::new("strict", "events", "tenant_id = 7"),
        &cat,
        &user_schema,
    )
    .unwrap();
    let graph = parse_and_lower("SELECT id FROM events").unwrap();
    let strict_out = rewrite(&graph, &[strict], &UserContext::new(std::iter::empty())).unwrap();
    assert_eq!(
        strict_out.stats.filters_inserted, 1,
        "strict rule must gate the base table"
    );

    // Permissive rule: tautology — no filter needed.
    let permissive = compile_rule(
        &PermissionRule::new("permissive", "events", "true"),
        &cat,
        &user_schema,
    )
    .unwrap();
    let permissive_out =
        rewrite(&graph, &[permissive], &UserContext::new(std::iter::empty())).unwrap();
    assert_eq!(
        permissive_out.stats.filters_inserted, 0,
        "tautology rule must elide the filter so all rows flow through"
    );
    assert!(permissive_out.stats.rules_elided >= 1);
}
