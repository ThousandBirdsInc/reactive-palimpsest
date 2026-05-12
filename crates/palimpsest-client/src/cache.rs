// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Client-side primary-key cache (§18.10).
//!
//! Each subscription gets its own cache, keyed on the columns named in
//! `Schema.primary_key_columns`. Diff application is row-by-row:
//!
//! * `Initial` / `Insert` — upsert by PK.
//! * `Update` — replace by PK.
//! * `Delete` — remove by PK.
//!
//! Callers can borrow the live row map via [`LocalCache::rows`] or the
//! flattened iterator via [`LocalCache::iter`].

use std::collections::HashMap;

use palimpsest_proto::palimpsest::sync::v1::{DiffOp, Schema};
use palimpsest_proto::wire::{WireDatum, WireRow};

/// Per-subscription PK-keyed row cache.
#[derive(Debug, Default, Clone)]
pub struct LocalCache {
    pk_columns: Vec<usize>,
    rows: HashMap<PrimaryKey, WireRow>,
}

/// Hashable key formed from the row's primary-key columns.
pub type PrimaryKey = Vec<WireDatum>;

impl LocalCache {
    /// Build an empty cache for `schema`.
    #[must_use]
    pub fn for_schema(schema: &Schema) -> Self {
        Self {
            pk_columns: schema
                .primary_key_columns
                .iter()
                .map(|c| usize::try_from(*c).unwrap_or(0))
                .collect(),
            rows: HashMap::new(),
        }
    }

    /// Pick out the primary key for a row.
    fn key(&self, row: &WireRow) -> PrimaryKey {
        self.pk_columns
            .iter()
            .filter_map(|idx| row.get(*idx).cloned())
            .collect()
    }

    /// Apply a decoded diff to the cache.
    pub fn apply(&mut self, op: DiffOp, rows: &[WireRow]) {
        match op {
            DiffOp::Initial | DiffOp::Insert | DiffOp::Update => {
                for row in rows {
                    self.rows.insert(self.key(row), row.clone());
                }
            }
            DiffOp::Delete => {
                for row in rows {
                    self.rows.remove(&self.key(row));
                }
            }
            DiffOp::Unspecified => {
                // No-op: server should never send this.
            }
        }
    }

    /// Number of rows currently materialized.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when no rows are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Borrow the entire keyed map.
    #[must_use]
    pub const fn rows(&self) -> &HashMap<PrimaryKey, WireRow> {
        &self.rows
    }

    /// Iterate `(pk, row)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&PrimaryKey, &WireRow)> {
        self.rows.iter()
    }

    /// Look up a row by its primary key.
    #[must_use]
    pub fn get(&self, key: &PrimaryKey) -> Option<&WireRow> {
        self.rows.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::LocalCache;
    use palimpsest_proto::palimpsest::sync::v1::{Column, DatumType, DiffOp, Schema};
    use palimpsest_proto::wire::WireDatum;

    fn schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "id".into(),
                    r#type: DatumType::I64.into(),
                    nullable: false,
                },
                Column {
                    name: "title".into(),
                    r#type: DatumType::Text.into(),
                    nullable: true,
                },
            ],
            primary_key_columns: vec![0],
        }
    }

    #[test]
    fn initial_then_update_replaces_in_place() {
        let mut cache = LocalCache::for_schema(&schema());
        let row1 = vec![WireDatum::I64(1), WireDatum::Text(b"a".to_vec())];
        let row2 = vec![WireDatum::I64(1), WireDatum::Text(b"b".to_vec())];
        cache.apply(DiffOp::Initial, &[row1]);
        assert_eq!(cache.len(), 1);
        cache.apply(DiffOp::Update, std::slice::from_ref(&row2));
        assert_eq!(cache.len(), 1);
        let stored = cache.get(&vec![WireDatum::I64(1)]).unwrap();
        assert_eq!(stored, &row2);
    }

    #[test]
    fn delete_removes_by_pk() {
        let mut cache = LocalCache::for_schema(&schema());
        cache.apply(
            DiffOp::Insert,
            &[vec![WireDatum::I64(7), WireDatum::Text(b"x".to_vec())]],
        );
        cache.apply(
            DiffOp::Delete,
            &[vec![
                WireDatum::I64(7),
                WireDatum::Text(b"ignored".to_vec()),
            ]],
        );
        assert!(cache.is_empty());
    }

    #[test]
    fn unspecified_is_noop() {
        let mut cache = LocalCache::for_schema(&schema());
        cache.apply(
            DiffOp::Unspecified,
            &[vec![WireDatum::I64(1), WireDatum::Null]],
        );
        assert!(cache.is_empty());
    }
}
