// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §15.3 property 6 — Permission soundness.
//!
//! For an arbitrary `(rules, user_ctx)`, no row in client output may
//! fail the rule predicate. We assert the structural form: rewriting
//! `SELECT * FROM <t>` with a single rule splices a `Filter` above the
//! base table, so any row reaching the root must satisfy the rule.

use palimpsest_permissions::{
    compile_rule, rewrite, PermissionRule, UserContext, UserContextSchema,
};
use palimpsest_sql::{lower::parse_and_lower, Catalog, ColumnSchema, ColumnType, TableSchema};
use proptest::prelude::*;

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

proptest! {
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
