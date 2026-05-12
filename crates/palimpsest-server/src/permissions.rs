// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Permission-filter integration.
//!
//! Per §11.2, permission filters are spliced into the user's MIR
//! immediately above each `BaseTable` reference. This module is the
//! seam between the router and `palimpsest_permissions`: it accepts a
//! freshly-lowered `MirGraph` and the connection's `UserContext`, calls
//! [`palimpsest_permissions::rewrite`], and surfaces the rewritten
//! graph + statistics for build-plan registration.
//!
//! The router applies permissions *before* registering the build plan
//! so the canonical-key under which the plan is stored already reflects
//! `(query, user_ctx)`. Two clients with the same `UserContext` for
//! the same query share the trace; differing user-context values
//! produce distinct keys naturally (covered by §18.6 properties).

use palimpsest_permissions::{rewrite, CompiledRule, RewriteOutcome, UserContext};
use palimpsest_sql::mir::MirGraph;

use crate::error::RouterError;

/// Splices permission filters into `graph` for `user_ctx`.
///
/// Tautologies and rules whose mode does not affect row visibility are
/// elided (see `palimpsest_permissions::rewriter`).
///
/// # Errors
/// Returns [`RouterError::Permission`] when a referenced `$user.<field>`
/// is missing from the supplied context.
pub fn install_permission_filters(
    graph: &MirGraph,
    rules: &[CompiledRule],
    user_ctx: &UserContext,
) -> Result<RewriteOutcome, RouterError> {
    Ok(rewrite(graph, rules, user_ctx)?)
}

#[cfg(test)]
mod tests {
    use palimpsest_permissions::{
        compile_rules, PermissionRule, UserContext, UserContextSchema, UserValue,
    };
    use palimpsest_sql::{lower::parse_and_lower, mir::MirNodeKind, Catalog, ColumnType};

    use super::install_permission_filters;

    fn schema() -> UserContextSchema {
        UserContextSchema::new([("id".to_owned(), ColumnType::Int)])
    }

    #[test]
    fn install_inserts_filter_above_basetable() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules = compile_rules(
            &[PermissionRule::new(
                "posts_owner",
                "posts",
                "author_id = $user.id",
            )],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        let ctx = UserContext::new([("id".to_owned(), UserValue::Int(7))]);

        let outcome = install_permission_filters(&graph, &rules, &ctx).unwrap();
        assert_eq!(outcome.stats.filters_inserted, 1);
        assert!(outcome
            .graph
            .node_kinds()
            .any(|node| matches!(node, MirNodeKind::Filter { .. })));
    }

    #[test]
    fn install_no_rules_is_identity() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let ctx = UserContext::new(std::iter::empty());
        let outcome = install_permission_filters(&graph, &[], &ctx).unwrap();
        assert_eq!(outcome.stats.filters_inserted, 0);
    }
}
