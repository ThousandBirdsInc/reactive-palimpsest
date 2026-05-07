// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("expected exactly one SQL statement, got {0}")]
    StatementCount(usize),

    #[error("only SELECT queries are supported")]
    UnsupportedStatement,

    #[error("unsupported SQL feature: {0}")]
    UnsupportedFeature(&'static str),

    #[error(transparent)]
    Parse(#[from] sqlparser::parser::ParserError),
}
