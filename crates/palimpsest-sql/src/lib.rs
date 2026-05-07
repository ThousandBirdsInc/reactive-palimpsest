// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "SQL parser, validation, and MIR scaffolding for Palimpsest."]

pub mod canonical;
mod error;
pub mod lower;
pub mod mir;
pub mod parser;
mod properties;

pub use error::SqlError;
pub use lower::{lower_select_statement, parse_and_lower};
pub use parser::{parse_select, validate_query};
