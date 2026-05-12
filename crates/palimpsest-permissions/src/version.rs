// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Rule-version tracking primitive that supports the v1.5 hot-reload
//! story (\u{00a7}18.6).
//!
//! v1 ships with restart-only rule edits, but the subscription router
//! still needs a stable identifier to compare against when checking
//! whether a subscription's compiled rule set is still current. When
//! v1.5 introduces hot-reload, the router will diff
//! [`RuleVersion`] values and issue `Resync` for any subscription whose
//! rules changed.
//!
//! The tracker stores per-rule `(name -> version)` so the router can
//! mark only the affected subscriptions dirty rather than resyncing
//! every subscription on any edit.

use std::collections::BTreeMap;

use crate::compile::CompiledRule;

/// Monotonic version counter assigned to each (re)load of a rule.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuleVersion(pub u64);

impl RuleVersion {
    /// Returns the next version number.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Per-rule version tracker.
///
/// `Default` produces an empty tracker at version 0. Each `install`
/// call advances the global epoch; rules whose canonical predicate
/// changed receive the new epoch, untouched rules keep their previous
/// version. Removed rules are reported via `removed_since`.
#[derive(Debug, Clone, Default)]
pub struct RuleVersionTracker {
    epoch: RuleVersion,
    versions: BTreeMap<String, RuleEntry>,
}

#[derive(Debug, Clone)]
struct RuleEntry {
    version: RuleVersion,
    canonical: String,
}

/// Summary of changes from one `install` call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleVersionDelta {
    /// Rule names with new or modified predicates after this install.
    pub changed: Vec<String>,
    /// Rule names removed by this install.
    pub removed: Vec<String>,
    /// Epoch assigned to changed rules.
    pub epoch: RuleVersion,
}

impl RuleVersionTracker {
    /// Returns the current global epoch.
    #[must_use]
    pub const fn epoch(&self) -> RuleVersion {
        self.epoch
    }

    /// Returns the version recorded for `name`, if any.
    #[must_use]
    pub fn version(&self, name: &str) -> Option<RuleVersion> {
        self.versions.get(name).map(|entry| entry.version)
    }

    /// Number of rules currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.versions.len()
    }

    /// True when no rules are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Replaces the tracker contents with `rules`. Rules whose
    /// canonical predicate is unchanged keep their existing version;
    /// new or modified rules receive the new epoch.
    pub fn install(&mut self, rules: &[CompiledRule]) -> RuleVersionDelta {
        let new_epoch = self.epoch.next();
        let mut next: BTreeMap<String, RuleEntry> = BTreeMap::new();
        let mut delta = RuleVersionDelta {
            changed: Vec::new(),
            removed: Vec::new(),
            epoch: new_epoch,
        };

        for rule in rules {
            let canonical = rule.predicate.canonical.clone();
            match self.versions.get(&rule.name) {
                Some(existing) if existing.canonical == canonical => {
                    next.insert(
                        rule.name.clone(),
                        RuleEntry {
                            version: existing.version,
                            canonical,
                        },
                    );
                }
                _ => {
                    next.insert(
                        rule.name.clone(),
                        RuleEntry {
                            version: new_epoch,
                            canonical,
                        },
                    );
                    delta.changed.push(rule.name.clone());
                }
            }
        }

        for name in self.versions.keys() {
            if !next.contains_key(name) {
                delta.removed.push(name.clone());
            }
        }

        self.versions = next;
        if !delta.changed.is_empty() || !delta.removed.is_empty() {
            self.epoch = new_epoch;
        }
        delta
    }
}

#[cfg(test)]
mod tests {
    use palimpsest_sql::{Catalog, ColumnType};

    use super::RuleVersionTracker;
    use crate::{compile::compile_rules, rule::PermissionRule, user::UserContextSchema};

    fn schema() -> UserContextSchema {
        UserContextSchema::new([("id".to_owned(), ColumnType::Int)])
    }

    #[test]
    fn install_assigns_epoch_to_new_rules() {
        let rules = compile_rules(
            &[PermissionRule::new("p", "posts", "author_id = $user.id")],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        let mut tracker = RuleVersionTracker::default();
        let delta = tracker.install(&rules);
        assert_eq!(delta.changed, vec!["p".to_owned()]);
        assert_eq!(tracker.version("p"), Some(delta.epoch));
    }

    #[test]
    fn install_keeps_version_for_unchanged_rule() {
        let rules = compile_rules(
            &[PermissionRule::new("p", "posts", "author_id = $user.id")],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        let mut tracker = RuleVersionTracker::default();
        let first = tracker.install(&rules);
        let second = tracker.install(&rules);
        assert!(second.changed.is_empty());
        assert_eq!(tracker.version("p"), Some(first.epoch));
    }

    #[test]
    fn install_records_modified_predicate_as_changed() {
        let mut tracker = RuleVersionTracker::default();
        let v1 = compile_rules(
            &[PermissionRule::new("p", "posts", "author_id = $user.id")],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        tracker.install(&v1);

        let v2 = compile_rules(
            &[PermissionRule::new("p", "posts", "author_id = 99")],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        let delta = tracker.install(&v2);
        assert_eq!(delta.changed, vec!["p".to_owned()]);
    }

    #[test]
    fn install_reports_removed_rules() {
        let mut tracker = RuleVersionTracker::default();
        let rules = compile_rules(
            &[
                PermissionRule::new("a", "posts", "id = 1"),
                PermissionRule::new("b", "posts", "id = 2"),
            ],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        tracker.install(&rules);

        let trimmed = compile_rules(
            &[PermissionRule::new("a", "posts", "id = 1")],
            &Catalog::demo(),
            &schema(),
        )
        .unwrap();
        let delta = tracker.install(&trimmed);
        assert_eq!(delta.removed, vec!["b".to_owned()]);
    }
}
