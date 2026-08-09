// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQL parser, validation, and MIR scaffolding for Palimpsest.

#![warn(missing_docs)]

pub mod alias;
pub mod canonical;
pub mod catalog;
mod error;
pub mod limits;
pub mod lower;
pub mod mir;
pub mod normalize;
pub mod parser;
pub mod prepared;
mod properties;

pub use catalog::{Catalog, ColumnSchema, ColumnType, TableSchema};
pub use error::SqlError;
pub use limits::{enforce_graph_size, enforce_input_size, QueryLimits};
pub use lower::{lower_select_statement, parse_and_lower, parse_and_lower_with_limits};
pub use normalize::{normalize_statement, parse_and_normalize, validate_statement_against_catalog};
pub use parser::{is_user_placeholder, parse_select, parse_select_with_limits, validate_query};
pub use prepared::{
    BindError, BoundQuery, ParamDecl, ParamSpec, ParamValue, PreparedCardinality, PreparedQuery,
    QueryRegistry, RegisterError,
};
