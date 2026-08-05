// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! TOML configuration loader for permission rules.
//!
//! ```toml
//! [[user_context]]
//! name = "id"
//! type = "int"
//!
//! [[user_context]]
//! name = "org_id"
//! type = "int"
//!
//! # Also supported: "uuid", "jsonb", and "enum" fields.
//! [[user_context]]
//! name = "tenant_id"
//! type = "uuid"
//!
//! [[rule]]
//! name = "posts_in_org"
//! table = "posts"
//! mode = "both"
//! predicate = "author_id = $user.id"
//! ```

use std::path::Path;

use palimpsest_sql::{Catalog, ColumnType};
use serde::{Deserialize, Serialize};

use crate::{
    compile::{compile_rules, CompiledRule},
    error::PermissionError,
    rule::{Mode, PermissionRule},
    user::UserContextSchema,
};

/// Field declaration inside a `[[user_context]]` block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserContextField {
    /// Field name (referenced as `$user.<name>` from predicates).
    pub name: String,
    /// Declared type.
    #[serde(rename = "type")]
    pub ty: UserContextFieldType,
}

/// Subset of [`ColumnType`] that the configuration surface accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserContextFieldType {
    /// Boolean.
    Bool,
    /// Signed integer.
    Int,
    /// Floating-point.
    Float,
    /// UTF-8 text.
    Text,
    /// ISO-8601 timestamp.
    Timestamp,
    /// RFC 4122 UUID.
    Uuid,
    /// JSON document (`jsonb`).
    Jsonb,
    /// Enum label, compared as text.
    Enum,
}

impl UserContextFieldType {
    /// Maps the config-surface type onto the catalog's [`ColumnType`].
    #[must_use]
    pub const fn to_column_type(self) -> ColumnType {
        match self {
            Self::Bool => ColumnType::Bool,
            Self::Int => ColumnType::Int,
            Self::Float => ColumnType::Float,
            Self::Text => ColumnType::Text,
            Self::Timestamp => ColumnType::Timestamp,
            Self::Uuid => ColumnType::Uuid,
            Self::Jsonb => ColumnType::Jsonb,
            Self::Enum => ColumnType::Enum,
        }
    }
}

/// On-disk representation of a permission rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleConfig {
    /// Stable rule name (must be unique within a config).
    pub name: String,
    /// Target table the rule applies to.
    pub table: String,
    /// SQL fragment evaluated against rows of `table`.
    pub predicate: String,
    /// Whether the rule gates row visibility, write authorization, or both.
    #[serde(default)]
    pub mode: Mode,
}

impl RuleConfig {
    fn into_rule(self) -> PermissionRule {
        PermissionRule {
            name: self.name,
            table: self.table,
            predicate: self.predicate,
            mode: self.mode,
        }
    }
}

/// Parsed configuration document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Declared user-context fields.
    #[serde(default, rename = "user_context")]
    pub user_context: Vec<UserContextField>,
    /// Permission rules.
    #[serde(default, rename = "rule")]
    pub rules: Vec<RuleConfig>,
}

impl Config {
    /// Builds a `UserContextSchema` from the declared `user_context` fields.
    #[must_use]
    pub fn user_context_schema(&self) -> UserContextSchema {
        UserContextSchema::new(
            self.user_context
                .iter()
                .map(|field| (field.name.clone(), field.ty.to_column_type())),
        )
    }

    /// Returns the rules in their on-disk order.
    #[must_use]
    pub fn rules(&self) -> Vec<PermissionRule> {
        self.rules
            .iter()
            .cloned()
            .map(RuleConfig::into_rule)
            .collect()
    }

    /// Compiles every rule against `catalog`, validating the user-context
    /// schema embedded in the config.
    ///
    /// # Errors
    /// Returns the first [`PermissionError`] from compilation.
    pub fn compile(&self, catalog: &Catalog) -> Result<Vec<CompiledRule>, PermissionError> {
        let schema = self.user_context_schema();
        compile_rules(&self.rules(), catalog, &schema)
    }
}

/// Parse a configuration document from raw TOML text.
///
/// # Errors
/// Returns [`PermissionError::InvalidConfig`] on TOML parse failure.
pub fn parse_config(input: &str) -> Result<Config, PermissionError> {
    toml::from_str(input).map_err(|err| PermissionError::InvalidConfig(err.to_string()))
}

/// Reads `path` from disk and parses it as a permissions configuration.
///
/// # Errors
/// Returns [`PermissionError::InvalidConfig`] on filesystem or parse failure.
pub fn load_config(path: impl AsRef<Path>) -> Result<Config, PermissionError> {
    let content = std::fs::read_to_string(path.as_ref())
        .map_err(|err| PermissionError::InvalidConfig(err.to_string()))?;
    parse_config(&content)
}

#[cfg(test)]
mod tests {
    use palimpsest_sql::Catalog;

    use super::{parse_config, Config};
    use crate::rule::Mode;

    const SAMPLE: &str = r#"
        [[user_context]]
        name = "id"
        type = "int"

        [[user_context]]
        name = "org_id"
        type = "int"

        [[rule]]
        name = "posts_in_org"
        table = "posts"
        predicate = "author_id = $user.id"

        [[rule]]
        name = "authors_self"
        table = "authors"
        mode = "row_visibility"
        predicate = "id = $user.id"
    "#;

    #[test]
    fn parses_sample_config() {
        let config: Config = parse_config(SAMPLE).expect("config parses");
        assert_eq!(config.user_context.len(), 2);
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].mode, Mode::Both);
        assert_eq!(config.rules[1].mode, Mode::RowVisibility);
    }

    #[test]
    fn parses_uuid_jsonb_and_enum_field_types() {
        use palimpsest_sql::ColumnType;

        let config: Config = parse_config(
            r#"
            [[user_context]]
            name = "tenant_id"
            type = "uuid"

            [[user_context]]
            name = "prefs"
            type = "jsonb"

            [[user_context]]
            name = "role"
            type = "enum"
        "#,
        )
        .expect("config parses");
        let schema = config.user_context_schema();
        assert_eq!(schema.field("tenant_id"), Some(ColumnType::Uuid));
        assert_eq!(schema.field("prefs"), Some(ColumnType::Jsonb));
        assert_eq!(schema.field("role"), Some(ColumnType::Enum));
    }

    #[test]
    fn compiles_against_catalog() {
        let config: Config = parse_config(SAMPLE).expect("config parses");
        let compiled = config.compile(&Catalog::demo()).expect("compiles cleanly");
        assert_eq!(compiled.len(), 2);
    }

    #[test]
    fn rejects_unknown_table() {
        let bad = r#"
            [[rule]]
            name = "x"
            table = "not_in_catalog"
            predicate = "true"
        "#;
        let config = parse_config(bad).expect("parses TOML");
        let err = config.compile(&Catalog::demo()).unwrap_err();
        assert!(err.to_string().contains("not_in_catalog"));
    }

    #[test]
    fn rejects_unknown_user_field() {
        let bad = r#"
            [[user_context]]
            name = "id"
            type = "int"

            [[rule]]
            name = "x"
            table = "posts"
            predicate = "author_id = $user.bogus"
        "#;
        let config = parse_config(bad).expect("parses TOML");
        let err = config.compile(&Catalog::demo()).unwrap_err();
        assert!(err.to_string().contains("bogus"));
    }

    #[test]
    fn rejects_ambiguous_rules() {
        let bad = r#"
            [[user_context]]
            name = "id"
            type = "int"

            [[rule]]
            name = "a"
            table = "posts"
            predicate = "author_id = $user.id"

            [[rule]]
            name = "b"
            table = "posts"
            predicate = "author_id = $user.id"
        "#;
        let config = parse_config(bad).expect("parses TOML");
        let err = config.compile(&Catalog::demo()).unwrap_err();
        assert!(err.to_string().contains("ambiguous"));
    }
}
