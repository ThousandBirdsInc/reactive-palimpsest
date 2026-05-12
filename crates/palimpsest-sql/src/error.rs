// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use thiserror::Error;

/// Errors surfaced by the SQL parsing, lowering, and validation passes.
#[derive(Debug, Error)]
pub enum SqlError {
    /// More than one statement was supplied where exactly one was
    /// expected (subscriptions accept a single `SELECT`).
    #[error("expected exactly one SQL statement, got {0}")]
    StatementCount(usize),

    /// The supplied statement was not a `SELECT`.
    #[error("only SELECT queries are supported")]
    UnsupportedStatement,

    /// The supplied query uses a SQL feature outside the v1 surface.
    #[error("unsupported SQL feature: {0}")]
    UnsupportedFeature(&'static str),

    /// Referenced table is not in the catalog.
    #[error("unknown table: {0}")]
    UnknownTable(String),

    /// Referenced column is not present on the resolved table.
    #[error("unknown column: {0}")]
    UnknownColumn(String),

    /// Column name resolves to multiple sources without a qualifier.
    #[error("ambiguous column reference: {0}")]
    AmbiguousColumn(String),

    /// Expression failed type-checking against the catalog.
    #[error("SQL type mismatch: {0}")]
    TypeMismatch(String),

    /// Parser input exceeded the configured byte budget. Surfaces as a
    /// 4xx-equivalent on the gRPC wire so a runaway client cannot
    /// exhaust the parser by submitting megabyte-scale SQL strings.
    #[error("SQL input is {len} bytes, limit is {limit}")]
    QueryTooLarge {
        /// Length of the rejected input.
        len: usize,
        /// Configured byte budget.
        limit: usize,
    },

    /// Lowered MIR exceeded the configured node-count budget. Same
    /// rationale as `QueryTooLarge`, but for cases where the SQL itself
    /// is short yet expands (set ops, joins, CTEs) into a large graph.
    #[error("MIR has {nodes} nodes, limit is {limit}")]
    QueryTooComplex {
        /// Node count of the lowered graph.
        nodes: usize,
        /// Configured node budget.
        limit: usize,
    },

    /// Wrapped underlying parser failure.
    #[error(transparent)]
    Parse(#[from] sqlparser::parser::ParserError),
}
