// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Rewrites a user's `MirGraph` to splice permission filters.
//!
//! For every `BaseTable` reference (\u{00a7}11.2) we look up matching
//! rules and chain a `Filter` per rule above the base table.
//! Tautologies (`true`) and rules whose `mode` does not affect row
//! visibility are elided so the unconfigured/default-open case has
//! zero overhead.

use std::collections::BTreeMap;

use palimpsest_sql::mir::{MirGraph, MirNodeKind};

use crate::{compile::CompiledRule, error::PermissionError, rule::Mode, user::UserContext};

/// Result of rewriting a query MIR with a set of compiled rules.
#[derive(Debug, Clone)]
pub struct RewriteOutcome {
    /// MIR graph with permission filters spliced in. When no rule
    /// applied, this is a clone of the input.
    pub graph: MirGraph,
    /// Per-rewrite statistics for tests and instrumentation.
    pub stats: RewriteStats,
}

/// Counters surfaced from a [`rewrite`] call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RewriteStats {
    /// Total `BaseTable` nodes visited.
    pub base_tables_visited: usize,
    /// Filters spliced in.
    pub filters_inserted: usize,
    /// Rules elided because their predicate is `true` or their mode
    /// disables row-visibility filtering.
    pub rules_elided: usize,
}

/// Splices a `Filter` node above every `BaseTable` whose name matches a
/// rule. Rules whose predicate is a tautology, or whose mode does not
/// affect row visibility, are elided.
///
/// The `UserContext` supplies the values bound to each `$user.*`
/// reference; if a referenced field is absent, the rewriter returns
/// [`PermissionError::MissingUserValue`].
pub fn rewrite(
    graph: &MirGraph,
    rules: &[CompiledRule],
    context: &UserContext,
) -> Result<RewriteOutcome, PermissionError> {
    let mut by_table: BTreeMap<&str, Vec<&CompiledRule>> = BTreeMap::new();
    for rule in rules {
        if !rule.mode.affects_visibility() {
            continue;
        }
        if rule.predicate.is_tautology() {
            continue;
        }
        by_table.entry(rule.table.as_str()).or_default().push(rule);
    }

    let mut output = graph.clone();
    let mut stats = RewriteStats::default();

    for index in output.base_table_indices() {
        stats.base_tables_visited += 1;
        let MirNodeKind::BaseTable { table, .. } = output.node_kind(index).clone() else {
            continue;
        };

        let Some(rules_for_table) = by_table.get(table.as_str()) else {
            continue;
        };

        let mut current = index;
        for rule in rules_for_table {
            let materialized = rule.predicate.materialize(context).map_err(|err| {
                if let PermissionError::MissingUserValue(field) = err {
                    PermissionError::MissingUserValue(field)
                } else {
                    err
                }
            })?;
            current = output.splice_above(
                current,
                MirNodeKind::Filter {
                    predicate: materialized,
                },
            );
            stats.filters_inserted += 1;
        }
    }

    stats.rules_elided = rules.len().saturating_sub(rules_in_play(rules));

    Ok(RewriteOutcome {
        graph: output,
        stats,
    })
}

fn rules_in_play(rules: &[CompiledRule]) -> usize {
    rules
        .iter()
        .filter(|rule| rule.mode.affects_visibility() && !rule.predicate.is_tautology())
        .count()
}

/// Returns true when the rule should participate in row-visibility
/// rewriting.
#[must_use]
pub const fn rule_affects_rewriter(mode: Mode) -> bool {
    mode.affects_visibility()
}

#[cfg(test)]
mod tests {
    use palimpsest_sql::{
        lower::parse_and_lower,
        mir::{MirEdgeKind, MirNodeKind},
        Catalog, ColumnType,
    };

    use super::rewrite;
    use crate::{
        compile::compile_rules,
        rule::{Mode, PermissionRule},
        user::{UserContext, UserContextSchema, UserValue},
    };

    fn schema() -> UserContextSchema {
        UserContextSchema::new([
            ("id".to_owned(), ColumnType::Int),
            ("org_id".to_owned(), ColumnType::Int),
        ])
    }

    fn alice() -> UserContext {
        UserContext::new([
            ("id".to_owned(), UserValue::Int(1)),
            ("org_id".to_owned(), UserValue::Int(7)),
        ])
    }

    #[test]
    fn rewrite_inserts_filter_above_basetable() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules = compile_rules(
            &[PermissionRule::new(
                "posts_in_org",
                "posts",
                "author_id = $user.id",
            )],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();

        let outcome = rewrite(&graph, &rules, &alice()).unwrap();
        assert_eq!(outcome.stats.base_tables_visited, 1);
        assert_eq!(outcome.stats.filters_inserted, 1);
        assert!(outcome.graph.node_kinds().any(|node| matches!(
            node,
            MirNodeKind::Filter { predicate } if predicate.contains('1')
        )));
    }

    #[test]
    fn rewrite_elides_tautology() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules = compile_rules(
            &[PermissionRule::new("everything", "posts", "true")],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();

        let outcome = rewrite(&graph, &rules, &alice()).unwrap();
        assert_eq!(outcome.stats.filters_inserted, 0);
        assert_eq!(outcome.stats.rules_elided, 1);
    }

    #[test]
    fn rewrite_skips_subscribe_only_rules() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules =
            compile_rules(
                &[PermissionRule::new("p", "posts", "author_id = $user.id")
                    .with_mode(Mode::Subscribe)],
                &Catalog::demo(),
                &schema(),
            )
            .unwrap();

        let outcome = rewrite(&graph, &rules, &alice()).unwrap();
        assert_eq!(outcome.stats.filters_inserted, 0);
    }

    #[test]
    fn rewrite_no_rules_for_table_is_identity() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules = compile_rules(
            &[PermissionRule::new(
                "authors_only",
                "authors",
                "id = $user.id",
            )],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();

        let outcome = rewrite(&graph, &rules, &alice()).unwrap();
        assert_eq!(outcome.stats.filters_inserted, 0);
    }

    #[test]
    fn rewrite_handles_join_with_two_basetables() {
        let graph = parse_and_lower(
            "SELECT posts.id
             FROM posts JOIN authors ON posts.author_id = authors.id",
        )
        .unwrap();
        let rules = compile_rules(
            &[
                PermissionRule::new("posts_owner", "posts", "author_id = $user.id"),
                PermissionRule::new("authors_self", "authors", "id = $user.id"),
            ],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();

        let outcome = rewrite(&graph, &rules, &alice()).unwrap();
        assert_eq!(outcome.stats.base_tables_visited, 2);
        assert_eq!(outcome.stats.filters_inserted, 2);
    }

    #[test]
    fn rewrite_chains_multiple_rules_for_same_table() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules = compile_rules(
            &[
                PermissionRule::new("owner", "posts", "author_id = $user.id"),
                PermissionRule::new("org", "posts", "author_id = $user.org_id"),
            ],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();

        let outcome = rewrite(&graph, &rules, &alice()).unwrap();
        assert_eq!(outcome.stats.filters_inserted, 2);
        let filter_count = outcome
            .graph
            .node_kinds()
            .filter(|node| matches!(node, MirNodeKind::Filter { .. }))
            .count();
        assert_eq!(filter_count, 2);

        let input_edge_count = outcome
            .graph
            .graph()
            .edge_weights()
            .filter(|edge| matches!(edge, MirEdgeKind::Input))
            .count();
        assert!(input_edge_count >= 3);
    }

    #[test]
    fn rewrite_reports_missing_user_value() {
        let graph = parse_and_lower("SELECT id FROM posts").unwrap();
        let rules = compile_rules(
            &[PermissionRule::new(
                "p",
                "posts",
                "author_id = $user.org_id",
            )],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();

        let context = UserContext::new([("id".to_owned(), UserValue::Int(1))]);
        let err = rewrite(&graph, &rules, &context).unwrap_err();
        assert!(err.to_string().contains("org_id"));
    }
}
