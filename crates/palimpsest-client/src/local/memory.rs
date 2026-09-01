// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Dependency-free in-memory [`LocalDatabase`] for native apps and
//! tests.
//!
//! Sync writes (`ensure_table` / `replace_rows` / `apply_writes` /
//! `get_row`) are fully supported. Arbitrary SQL is not — this store
//! has no SQL engine — with one convenience exception: a plain
//! `SELECT * FROM t` works so demos and tests can read a mirror back
//! without wiring up a real engine. Browser deployments should use
//! [`SqlLocalDatabase`](super::sql::SqlLocalDatabase) over a
//! pgrust-style WASM Postgres instead.

use std::collections::HashMap;
use std::sync::Mutex;

use palimpsest_proto::wire::{WireDatum, WireRow};

use crate::cache::PrimaryKey;

use super::db::{DbFuture, LocalDatabase, LocalDbError, LocalWrite, TableSpec};

#[derive(Debug, Default)]
struct MemTable {
    rows: HashMap<PrimaryKey, WireRow>,
}

/// In-memory table store keyed the same way mirrors are.
#[derive(Debug, Default)]
pub struct MemoryDatabase {
    tables: Mutex<HashMap<String, MemTable>>,
}

impl MemoryDatabase {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot every row of `table`, sorted by key for deterministic
    /// assertions. Empty when the table doesn't exist.
    #[must_use]
    pub fn snapshot(&self, table: &str) -> Vec<WireRow> {
        let tables = self.tables.lock().expect("memory db poisoned");
        let Some(mem) = tables.get(table) else {
            return Vec::new();
        };
        let mut rows: Vec<(String, WireRow)> = mem
            .rows
            .iter()
            .map(|(k, v)| (format!("{k:?}"), v.clone()))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows.into_iter().map(|(_, row)| row).collect()
    }
}

impl LocalDatabase for MemoryDatabase {
    fn ensure_table<'a>(&'a self, spec: &'a TableSpec) -> DbFuture<'a, Result<(), LocalDbError>> {
        Box::pin(async move {
            self.tables
                .lock()
                .expect("memory db poisoned")
                .entry(spec.name.clone())
                .or_default();
            Ok(())
        })
    }

    fn replace_rows<'a>(
        &'a self,
        spec: &'a TableSpec,
        rows: Vec<WireRow>,
    ) -> DbFuture<'a, Result<(), LocalDbError>> {
        Box::pin(async move {
            let mut tables = self.tables.lock().expect("memory db poisoned");
            let table = tables.entry(spec.name.clone()).or_default();
            table.rows = rows
                .into_iter()
                .map(|row| (spec.key_of(&row), row))
                .collect();
            Ok(())
        })
    }

    fn apply_writes<'a>(
        &'a self,
        spec: &'a TableSpec,
        writes: Vec<LocalWrite>,
    ) -> DbFuture<'a, Result<(), LocalDbError>> {
        Box::pin(async move {
            let mut tables = self.tables.lock().expect("memory db poisoned");
            let table = tables.entry(spec.name.clone()).or_default();
            for write in writes {
                match write {
                    LocalWrite::Upsert { row } => {
                        table.rows.insert(spec.key_of(&row), row);
                    }
                    LocalWrite::Delete { key } => {
                        table.rows.remove(&key);
                    }
                }
            }
            Ok(())
        })
    }

    fn get_row<'a>(
        &'a self,
        spec: &'a TableSpec,
        key: &'a PrimaryKey,
    ) -> DbFuture<'a, Result<Option<WireRow>, LocalDbError>> {
        Box::pin(async move {
            let tables = self.tables.lock().expect("memory db poisoned");
            Ok(tables
                .get(&spec.name)
                .and_then(|table| table.rows.get(key).cloned()))
        })
    }

    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: Vec<WireDatum>,
    ) -> DbFuture<'a, Result<Vec<WireRow>, LocalDbError>> {
        Box::pin(async move {
            if !params.is_empty() {
                return Err(LocalDbError::Unsupported(
                    "MemoryDatabase cannot bind query parameters".to_owned(),
                ));
            }
            match parse_select_star(sql) {
                Some(table) => Ok(self.snapshot(&table)),
                None => Err(LocalDbError::Unsupported(format!(
                    "MemoryDatabase only supports `SELECT * FROM <table>`, got: {sql}"
                ))),
            }
        })
    }
}

/// Accepts exactly `SELECT * FROM <ident>` (case-insensitive keywords,
/// optional trailing semicolon, optional double-quoted identifier).
fn parse_select_star(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut parts = trimmed.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("select") {
        return None;
    }
    if parts.next()? != "*" {
        return None;
    }
    if !parts.next()?.eq_ignore_ascii_case("from") {
        return None;
    }
    let table = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let unquoted = table
        .strip_prefix('"')
        .and_then(|t| t.strip_suffix('"'))
        .map_or(table, |t| t);
    Some(unquoted.replace("\"\"", "\""))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use palimpsest_proto::palimpsest::sync::v1::{Column, DatumType, Schema};
    use palimpsest_proto::wire::WireDatum;

    use super::super::db::{LocalDatabase, LocalWrite, TableSpec};
    use super::MemoryDatabase;

    fn spec() -> TableSpec {
        TableSpec::new(
            "posts",
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
            },
        )
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let db = MemoryDatabase::new();
        let spec = spec();
        db.ensure_table(&spec).await.unwrap();
        db.apply_writes(
            &spec,
            vec![LocalWrite::Upsert {
                row: vec![WireDatum::I64(1), WireDatum::Text(b"a".to_vec())],
            }],
        )
        .await
        .unwrap();
        let got = db.get_row(&spec, &vec![WireDatum::I64(1)]).await.unwrap();
        assert_eq!(
            got,
            Some(vec![WireDatum::I64(1), WireDatum::Text(b"a".to_vec())])
        );
        let rows = db.query("SELECT * FROM posts;", Vec::new()).await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn replace_swaps_contents() {
        let db = MemoryDatabase::new();
        let spec = spec();
        db.replace_rows(&spec, vec![vec![WireDatum::I64(1), WireDatum::Null]])
            .await
            .unwrap();
        db.replace_rows(&spec, vec![vec![WireDatum::I64(2), WireDatum::Null]])
            .await
            .unwrap();
        let rows = db.snapshot("posts");
        assert_eq!(rows, vec![vec![WireDatum::I64(2), WireDatum::Null]]);
    }

    #[tokio::test]
    async fn arbitrary_sql_is_unsupported() {
        let db = MemoryDatabase::new();
        let err = db
            .query("SELECT id FROM posts WHERE id = 1", Vec::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("only supports"));
    }
}
