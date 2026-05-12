// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Typed `UserContext` carried alongside each subscription.
//!
//! Permission predicates may reference fields like `$user.org_id` or
//! `$user.is_admin`. Operators declare the schema of those fields up
//! front; subscribers attach a `UserContext` whose values are validated
//! against that schema before any predicate is evaluated.

use std::collections::BTreeMap;

use palimpsest_sql::ColumnType;
use serde::{Deserialize, Serialize};

use crate::error::PermissionError;

/// Concrete value bound to a `$user.*` field at subscribe time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum UserValue {
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 text.
    Text(String),
    /// ISO-8601 timestamp, opaque to the rewriter (compared as text).
    Timestamp(String),
    /// SQL `NULL`. Always validates against any declared field type.
    Null,
}

impl UserValue {
    /// Returns the catalog type that this value satisfies.
    #[must_use]
    pub const fn column_type(&self) -> ColumnType {
        match self {
            Self::Bool(_) => ColumnType::Bool,
            Self::Int(_) => ColumnType::Int,
            Self::Float(_) => ColumnType::Float,
            Self::Text(_) => ColumnType::Text,
            Self::Timestamp(_) => ColumnType::Timestamp,
            Self::Null => ColumnType::Unknown,
        }
    }

    /// Renders the value as a SQL literal suitable for splicing into a
    /// canonicalized predicate string.
    #[must_use]
    pub fn to_sql_literal(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            Self::Float(value) => format!("{value}"),
            Self::Text(value) | Self::Timestamp(value) => {
                format!("'{}'", value.replace('\'', "''"))
            }
            Self::Null => "NULL".to_owned(),
        }
    }

    /// Stable string used for canonical-key hashing.
    ///
    /// Includes a type tag so `Int(0)` and `Bool(false)` never collide.
    #[must_use]
    pub fn canonical_repr(&self) -> String {
        match self {
            Self::Bool(value) => format!("bool:{value}"),
            Self::Int(value) => format!("int:{value}"),
            Self::Float(value) => format!("float:{value}"),
            Self::Text(value) => format!("text:{value}"),
            Self::Timestamp(value) => format!("ts:{value}"),
            Self::Null => "null".to_owned(),
        }
    }
}

/// Declared shape of a `UserContext`: ordered field name → type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserContextSchema {
    fields: BTreeMap<String, ColumnType>,
}

impl UserContextSchema {
    /// Builds a schema from `(name, type)` pairs.
    #[must_use]
    pub fn new(fields: impl IntoIterator<Item = (String, ColumnType)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
        }
    }

    /// Returns the type declared for `field`, if any.
    #[must_use]
    pub fn field(&self, field: &str) -> Option<ColumnType> {
        self.fields.get(field).copied()
    }

    /// Iterates over `(field_name, type)` pairs in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, ColumnType)> {
        self.fields.iter().map(|(name, ty)| (name.as_str(), *ty))
    }

    /// Returns true when the schema declares `field`.
    #[must_use]
    pub fn contains(&self, field: &str) -> bool {
        self.fields.contains_key(field)
    }

    /// Number of declared fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// True when the schema has no declared fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Inserts (or replaces) a field declaration.
    pub fn insert(&mut self, name: impl Into<String>, ty: ColumnType) {
        self.fields.insert(name.into(), ty);
    }
}

/// Concrete `UserContext` attached to a subscription.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserContext {
    values: BTreeMap<String, UserValue>,
}

impl UserContext {
    /// Builds a context from `(field, value)` pairs. No validation is
    /// performed; use [`UserContext::validate`] to check against a schema.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = (String, UserValue)>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }

    /// Returns the value bound to `field`, if any.
    #[must_use]
    pub fn get(&self, field: &str) -> Option<&UserValue> {
        self.values.get(field)
    }

    /// Inserts (or replaces) a field value.
    pub fn insert(&mut self, field: impl Into<String>, value: UserValue) {
        self.values.insert(field.into(), value);
    }

    /// Iterates over `(field, value)` pairs in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &UserValue)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    /// Number of bound fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// True when no fields are bound.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Validates each value against the schema's declared type. `Null` is
    /// permitted regardless of the declared type.
    pub fn validate(&self, schema: &UserContextSchema) -> Result<(), PermissionError> {
        for (field, value) in &self.values {
            let Some(expected) = schema.field(field) else {
                continue;
            };
            let actual = value.column_type();
            if matches!(value, UserValue::Null) {
                continue;
            }
            if !expected.is_compatible_with(actual) {
                return Err(PermissionError::UserValueTypeMismatch {
                    field: field.clone(),
                    expected,
                    actual,
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnType, UserContext, UserContextSchema, UserValue};

    #[test]
    fn schema_round_trip() {
        let mut schema = UserContextSchema::default();
        schema.insert("org_id", ColumnType::Int);
        schema.insert("is_admin", ColumnType::Bool);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.field("org_id"), Some(ColumnType::Int));
        assert!(schema.contains("is_admin"));
    }

    #[test]
    fn validate_rejects_type_mismatch() {
        let schema = UserContextSchema::new([
            ("org_id".to_owned(), ColumnType::Int),
            ("is_admin".to_owned(), ColumnType::Bool),
        ]);
        let context = UserContext::new([("org_id".to_owned(), UserValue::Text("nope".to_owned()))]);
        let err = context.validate(&schema).unwrap_err();
        assert!(err.to_string().contains("org_id"));
    }

    #[test]
    fn validate_accepts_null_and_compatible_numerics() {
        let schema = UserContextSchema::new([
            ("org_id".to_owned(), ColumnType::Int),
            ("balance".to_owned(), ColumnType::Float),
        ]);
        let context = UserContext::new([
            ("org_id".to_owned(), UserValue::Null),
            ("balance".to_owned(), UserValue::Int(7)),
        ]);
        context.validate(&schema).expect("compatible types");
    }

    #[test]
    fn sql_literal_escapes_single_quotes() {
        assert_eq!(
            UserValue::Text("o'brien".to_owned()).to_sql_literal(),
            "'o''brien'"
        );
        assert_eq!(UserValue::Int(-5).to_sql_literal(), "-5");
        assert_eq!(UserValue::Bool(true).to_sql_literal(), "true");
        assert_eq!(UserValue::Null.to_sql_literal(), "NULL");
    }

    #[test]
    fn canonical_repr_distinguishes_types() {
        assert_ne!(
            UserValue::Bool(false).canonical_repr(),
            UserValue::Int(0).canonical_repr(),
        );
    }
}
