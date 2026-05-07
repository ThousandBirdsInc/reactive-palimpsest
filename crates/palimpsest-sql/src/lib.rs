// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "SQL parser, validation, and MIR scaffolding for Palimpsest."]

mod error;
pub mod mir;
pub mod parser;

pub use error::SqlError;
pub use parser::{parse_select, validate_query};
