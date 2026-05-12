// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "Permission-rule DSL, compilation, and MIR rewriter (\u{00a7}11, \u{00a7}18.6)."]
#![warn(missing_docs)]

pub mod compile;
pub mod config;
mod error;
pub mod rewriter;
pub mod rule;
pub mod user;
pub mod version;

pub use compile::{compile_rule, compile_rules, CompiledPredicate, CompiledRule};
pub use config::{load_config, parse_config, Config, RuleConfig, UserContextField};
pub use error::PermissionError;
pub use rewriter::{rewrite, RewriteOutcome, RewriteStats};
pub use rule::{Mode, PermissionRule};
pub use user::{UserContext, UserContextSchema, UserValue};
pub use version::{RuleVersion, RuleVersionTracker};
