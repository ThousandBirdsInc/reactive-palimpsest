// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeMap;

use crate::SqlError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnType {
    Bool,
    Int,
    Float,
    Text,
    Timestamp,
    Unknown,
}

impl ColumnType {
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        matches!(self, Self::Int | Self::Float)
    }

    #[must_use]
    pub fn is_compatible_with(self, other: Self) -> bool {
        matches!((self, other), (Self::Unknown, _) | (_, Self::Unknown))
            || self == other
            || (self.is_numeric() && other.is_numeric())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub ty: ColumnType,
}

impl ColumnSchema {
    #[must_use]
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    #[must_use]
    pub fn new(name: impl Into<String>, columns: Vec<ColumnSchema>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    tables: BTreeMap<String, TableSchema>,
}

impl Catalog {
    #[must_use]
    pub fn new(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self {
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone(), table))
                .collect(),
        }
    }

    #[must_use]
    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    pub fn require_table(&self, name: &str) -> Result<&TableSchema, SqlError> {
        self.table(name)
            .ok_or_else(|| SqlError::UnknownTable(name.to_owned()))
    }

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
