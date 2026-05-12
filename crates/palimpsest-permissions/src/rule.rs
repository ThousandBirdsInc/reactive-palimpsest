// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Rule data types: surface DSL inputs, no compilation.

use serde::{Deserialize, Serialize};

/// When a rule applies. Defaults to `Both` so configurations are
/// safe-by-default (a rule that hides rows from results also blocks
/// subscriptions to those rows).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Filters rows from query output, but does not affect subscribe-time
    /// authorization.
    RowVisibility,
    /// Blocks subscription to keys that the rule excludes; previously
    /// observed rows still appear.
    Subscribe,
    /// Both `RowVisibility` and `Subscribe`. Default.
    #[default]
    Both,
}

impl Mode {
    /// Returns true when the rule should rewrite the user's query graph
    /// (i.e. inject a row-visibility filter).
    #[must_use]
    pub const fn affects_visibility(self) -> bool {
        matches!(self, Self::RowVisibility | Self::Both)
    }

    /// Returns true when the rule should be consulted at subscribe time.
    #[must_use]
    pub const fn affects_subscribe(self) -> bool {
        matches!(self, Self::Subscribe | Self::Both)
    }
}

/// User-facing permission rule, as it appears in configuration.
///
/// `predicate` is a SQL boolean expression (no `WHERE` keyword) that may
/// reference columns of `table` and `$user.<field>` placeholders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRule {
    /// Stable rule name.
    pub name: String,
    /// Target table.
    pub table: String,
    /// SQL boolean expression evaluated against rows of `table`.
    pub predicate: String,
    /// Whether the rule gates row visibility, subscribe-time auth, or both.
    #[serde(default)]
    pub mode: Mode,
}

impl PermissionRule {
    /// Convenience constructor for tests and inline configuration.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        table: impl Into<String>,
        predicate: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            predicate: predicate.into(),
            mode: Mode::default(),
        }
    }

    /// Builder-style mode override.
    #[must_use]
    pub const fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }
}
