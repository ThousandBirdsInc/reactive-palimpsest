// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Errors raised by the permission DSL: loading, compilation, and
//! enforcement.

use thiserror::Error;

use palimpsest_sql::SqlError;

/// Errors raised while loading, compiling, or applying permission rules.
#[derive(Debug, Error)]
pub enum PermissionError {
    /// Rule references a `$user.*` field that the schema does not declare.
    #[error("rule {rule:?} references unknown user-context field: {field}")]
    UnknownUserField {
        /// Offending rule name.
        rule: String,
        /// Field referenced by the rule.
        field: String,
    },

    /// Rule references a column that the catalog does not declare for `table`.
    #[error("rule {rule:?} references unknown column {column} on table {table}")]
    UnknownColumn {
        /// Offending rule name.
        rule: String,
        /// Target table.
        table: String,
        /// Missing column.
        column: String,
    },

    /// Rule targets a table that the catalog does not contain.
    #[error("rule {rule:?} targets unknown table: {table}")]
    UnknownTable {
        /// Offending rule name.
        rule: String,
        /// Missing table name.
        table: String,
    },

    /// Two rules with the same `name` were loaded.
    #[error("duplicate permission rule name: {0}")]
    DuplicateRuleName(String),

    /// Two rules over the same table whose predicates render to the same
    /// canonical form. The loader rejects ambiguous configurations early so
    /// the rewriter never sees them.
    #[error(
        "ambiguous rules {first:?} and {second:?} on table {table} share predicate {predicate}"
    )]
    AmbiguousRule {
        /// First rule name.
        first: String,
        /// Second rule name.
        second: String,
        /// Table both rules target.
        table: String,
        /// Shared canonical predicate text.
        predicate: String,
    },

    /// `$user.<field>` reference malformed (missing `.<field>` after `$user`).
    #[error("malformed user reference in rule {rule:?}: {fragment}")]
    MalformedUserReference {
        /// Offending rule name.
        rule: String,
        /// The malformed text fragment.
        fragment: String,
    },

    /// `UserContext` did not provide a value for a field referenced by the rule.
    #[error("missing user-context value for field {0}")]
    MissingUserValue(String),

    /// Predicate depth exceeded the configured ceiling. Stops a malicious
    /// or sloppy operator from shipping rule predicates that pathologically
    /// nest boolean operators.
    #[error("rule {rule:?} predicate is {depth} levels deep, exceeding limit of {limit}")]
    PredicateTooDeep {
        /// Offending rule name.
        rule: String,
        /// Observed AST depth.
        depth: usize,
        /// Configured limit.
        limit: usize,
    },

    /// `UserContext` value is the right type but malformed (e.g. a
    /// `Uuid` value whose text does not parse as a UUID).
    #[error("user-context field {field:?} has invalid value: {reason}")]
    InvalidUserValue {
        /// Offending field, or empty when the value was rejected before
        /// it was bound to a field.
        field: String,
        /// Human-readable rejection reason.
        reason: String,
    },

    /// `UserContext` value disagrees with the declared schema type.
    #[error("user-context field {field} has type {actual:?}, expected {expected:?}")]
    UserValueTypeMismatch {
        /// Mismatched field.
        field: String,
        /// Type the schema declared.
        expected: palimpsest_sql::ColumnType,
        /// Type the supplied value carried.
        actual: palimpsest_sql::ColumnType,
    },

    /// TOML deserialization failure during config loading.
    #[error("invalid permissions config: {0}")]
    InvalidConfig(String),

    /// Underlying SQL parser/lower error.
    #[error(transparent)]
    Sql(#[from] SqlError),
}
