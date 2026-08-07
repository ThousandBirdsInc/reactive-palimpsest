// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Logical catalog the parser/lower passes consult to resolve table
//! and column references and to type-check expressions.

use std::collections::BTreeMap;

use crate::SqlError;

/// Coarse SQL type taxonomy used during validation and lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnType {
    /// Boolean.
    Bool,
    /// Signed integer.
    Int,
    /// Floating-point.
    Float,
    /// UTF-8 text.
    Text,
    /// Timestamp (with or without timezone).
    Timestamp,
    /// RFC 4122 UUID.
    Uuid,
    /// Postgres `jsonb` (and `json`) document.
    Jsonb,
    /// Postgres enum label. The taxonomy is coarse on purpose: all enum
    /// types collapse into one variant and labels are compared as text.
    Enum,
    /// Type couldn't be inferred yet — treat as compatible with anything.
    Unknown,
}

impl ColumnType {
    /// Maps a SQL cast target onto the engine's coarse type taxonomy.
    /// `None` for types the expression evaluator can't produce — the
    /// parser rejects those casts up front, and the evaluator uses the
    /// same mapping to build the conversion.
    #[must_use]
    pub const fn from_cast_target(data_type: &sqlparser::ast::DataType) -> Option<Self> {
        use sqlparser::ast::DataType;
        Some(match data_type {
            DataType::Text
            | DataType::String(_)
            | DataType::Varchar(_)
            | DataType::CharVarying(_)
            | DataType::CharacterVarying(_)
            | DataType::Char(_)
            | DataType::Character(_) => Self::Text,
            DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int2(_)
            | DataType::Int(_)
            | DataType::Int4(_)
            | DataType::Integer(_)
            | DataType::BigInt(_)
            | DataType::Int8(_) => Self::Int,
            DataType::Real
            | DataType::Float4
            | DataType::Float8
            | DataType::Float(_)
            | DataType::Double
            | DataType::DoublePrecision => Self::Float,
            DataType::Bool | DataType::Boolean => Self::Bool,
            DataType::Uuid => Self::Uuid,
            DataType::JSON | DataType::JSONB => Self::Jsonb,
            _ => return None,
        })
    }

    /// True for [`Self::Int`] and [`Self::Float`].
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        matches!(self, Self::Int | Self::Float)
    }

    /// True for types whose values are written and compared as text.
    /// Quoted SQL literals type as [`Self::Text`], so uuid and enum
    /// columns must accept comparisons against text operands.
    #[must_use]
    pub const fn is_textual(self) -> bool {
        matches!(self, Self::Text | Self::Uuid | Self::Enum)
    }

    /// Whether two column types are interchangeable in a comparison or
    /// arithmetic context. `Unknown` is compatible with everything;
    /// numerics promote to each other; textual types (text, uuid, enum)
    /// compare with each other. `Jsonb` compares with itself and with
    /// `Text`, because a jsonb constant can only be written as a quoted
    /// literal (which types as `Text`).
    #[must_use]
    pub fn is_compatible_with(self, other: Self) -> bool {
        matches!((self, other), (Self::Unknown, _) | (_, Self::Unknown))
            || self == other
            || (self.is_numeric() && other.is_numeric())
            || (self.is_textual() && other.is_textual())
            || matches!(
                (self, other),
                (Self::Jsonb, Self::Text) | (Self::Text, Self::Jsonb)
            )
    }
}

/// Single column entry in a [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    /// Column name (case-sensitive).
    pub name: String,
    /// Column type.
    pub ty: ColumnType,
}

impl ColumnSchema {
    /// Builds a column schema.
    #[must_use]
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// Schema of a single relation tracked in the [`Catalog`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    /// Table name (case-sensitive).
    pub name: String,
    /// Ordered column list.
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Builds a table schema.
    #[must_use]
    pub fn new(name: impl Into<String>, columns: Vec<ColumnSchema>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    /// Looks a column up by name.
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }
}

/// Logical catalog: a set of [`TableSchema`]s keyed by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    tables: BTreeMap<String, TableSchema>,
}

impl Catalog {
    /// Builds a catalog from an iterator of table schemas.
    #[must_use]
    pub fn new(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self {
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone(), table))
                .collect(),
        }
    }

    /// Looks up a table by name; `None` if absent.
    #[must_use]
    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    /// Looks up a table by name, returning [`SqlError::UnknownTable`]
    /// if absent.
    ///
    /// # Errors
    /// `SqlError::UnknownTable` when the table is not in the catalog.
    pub fn require_table(&self, name: &str) -> Result<&TableSchema, SqlError> {
        self.table(name)
            .ok_or_else(|| SqlError::UnknownTable(name.to_owned()))
    }

    /// Iterates over table schemas in stable (alphabetical) order.
    pub fn tables(&self) -> impl Iterator<Item = &TableSchema> {
        self.tables.values()
    }

    /// Built-in demo catalog used by examples and integration tests.
    #[must_use]
    pub fn demo() -> Self {
        let post_columns = vec![
            ColumnSchema::new("id", ColumnType::Int),
            ColumnSchema::new("author_id", ColumnType::Int),
            ColumnSchema::new("created_at", ColumnType::Timestamp),
            ColumnSchema::new("title", ColumnType::Text),
            ColumnSchema::new("published", ColumnType::Bool),
        ];

        Self::new([
            TableSchema::new("posts", post_columns.clone()),
            TableSchema::new("archived_posts", post_columns),
            TableSchema::new(
                "authors",
                vec![
                    ColumnSchema::new("id", ColumnType::Int),
                    ColumnSchema::new("name", ColumnType::Text),
                ],
            ),
            TableSchema::new(
                "comments",
                vec![
                    ColumnSchema::new("id", ColumnType::Int),
                    ColumnSchema::new("post_id", ColumnType::Int),
                    ColumnSchema::new("author_id", ColumnType::Int),
                    ColumnSchema::new("body", ColumnType::Text),
                ],
            ),
        ])
    }
}
