// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Postgres-dialect adapter: implements [`LocalDatabase`] on top of any
//! engine that can execute parameterized SQL — a pgrust/pglite-style
//! Postgres WASM build in the browser, or an embedded Postgres natively.
//!
//! The embedding side only supplies a [`SqlExecutor`] (one `exec`
//! method); everything else — DDL derived from the wire schema,
//! `INSERT ... ON CONFLICT` upserts, batched transactions — is
//! generated here so every backend applies changes identically.

// On wasm the executor futures (JS driver calls) are not `Send`; they
// run on the single-threaded JS event loop.
#![allow(clippy::future_not_send)]

use std::collections::HashSet;

use palimpsest_proto::palimpsest::sync::v1::{DatumType, Schema};
use palimpsest_proto::wire::{WireDatum, WireRow};

use crate::cache::PrimaryKey;

use super::db::{DbFuture, LocalDatabase, LocalDbError, LocalWrite, MaybeSendSync, TableSpec};

/// Minimal contract a SQL engine must satisfy to back a replica.
///
/// * Statements arrive one at a time and must run on a single logical
///   connection/session, in call order — `BEGIN`/`COMMIT` framing is
///   issued by the adapter around batches.
/// * `params` bind to `$1..$n` placeholders in `sql`.
/// * `expect` is a decoding hint: when `Some`, returned rows should be
///   coerced to the given column types (in schema order). When `None`
///   the executor returns its best-effort native typing.
pub trait SqlExecutor: MaybeSendSync {
    /// Execute one statement, returning any result rows.
    fn exec(
        &self,
        sql: String,
        params: Vec<WireDatum>,
        expect: Option<Schema>,
    ) -> DbFuture<'_, Result<Vec<WireRow>, LocalDbError>>;
}

/// [`LocalDatabase`] built from a [`SqlExecutor`].
pub struct SqlLocalDatabase<E> {
    executor: E,
}

impl<E: SqlExecutor> SqlLocalDatabase<E> {
    /// Wrap an executor.
    pub const fn new(executor: E) -> Self {
        Self { executor }
    }

    /// Borrow the wrapped executor.
    pub const fn executor(&self) -> &E {
        &self.executor
    }

    /// Run a list of statements inside one transaction, rolling back on
    /// the first failure.
    async fn run_transaction(&self, statements: Vec<Statement>) -> Result<(), LocalDbError> {
        self.executor
            .exec("BEGIN".to_owned(), Vec::new(), None)
            .await?;
        for statement in statements {
            if let Err(err) = self
                .executor
                .exec(statement.sql, statement.params, None)
                .await
            {
                // Best-effort rollback; the original error is what the
                // caller needs to see.
                let _ = self
                    .executor
                    .exec("ROLLBACK".to_owned(), Vec::new(), None)
                    .await;
                return Err(err);
            }
        }
        self.executor
            .exec("COMMIT".to_owned(), Vec::new(), None)
            .await?;
        Ok(())
    }
}

/// One generated statement plus its bind parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// Postgres-dialect SQL with `$1..$n` placeholders.
    pub sql: String,
    /// Values bound to the placeholders, in order.
    pub params: Vec<WireDatum>,
}

impl<E: SqlExecutor> LocalDatabase for SqlLocalDatabase<E> {
    fn ensure_table<'a>(&'a self, spec: &'a TableSpec) -> DbFuture<'a, Result<(), LocalDbError>> {
        Box::pin(async move {
            self.executor
                .exec(create_table_sql(spec), Vec::new(), None)
                .await?;
            Ok(())
        })
    }

    fn replace_rows<'a>(
        &'a self,
        spec: &'a TableSpec,
        rows: Vec<WireRow>,
    ) -> DbFuture<'a, Result<(), LocalDbError>> {
        Box::pin(async move {
            let mut statements = vec![Statement {
                sql: format!("DELETE FROM {}", quote_ident(&spec.name)),
                params: Vec::new(),
            }];
            for row in rows {
                statements.push(upsert_statement(spec, row));
            }
            self.run_transaction(statements).await
        })
    }

    fn apply_writes<'a>(
        &'a self,
        spec: &'a TableSpec,
        writes: Vec<LocalWrite>,
    ) -> DbFuture<'a, Result<(), LocalDbError>> {
        Box::pin(async move {
            let statements: Vec<Statement> = writes
                .into_iter()
                .map(|write| match write {
                    LocalWrite::Upsert { row } => upsert_statement(spec, row),
                    LocalWrite::Delete { key } => delete_statement(spec, &key),
                })
                .collect();
            if statements.is_empty() {
                return Ok(());
            }
            self.run_transaction(statements).await
        })
    }

    fn get_row<'a>(
        &'a self,
        spec: &'a TableSpec,
        key: &'a PrimaryKey,
    ) -> DbFuture<'a, Result<Option<WireRow>, LocalDbError>> {
        Box::pin(async move {
            let statement = select_by_key_statement(spec, key);
            let rows = self
                .executor
                .exec(statement.sql, statement.params, Some(spec.schema.clone()))
                .await?;
            Ok(rows.into_iter().next())
        })
    }

    fn query<'a>(
        &'a self,
        sql: &'a str,
        params: Vec<WireDatum>,
    ) -> DbFuture<'a, Result<Vec<WireRow>, LocalDbError>> {
        Box::pin(async move { self.executor.exec(sql.to_owned(), params, None).await })
    }
}

/// Quote a SQL identifier (double quotes, embedded quotes doubled).
#[must_use]
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Map a wire datum type to the Postgres column type used for the
/// mirrored table.
#[must_use]
pub const fn column_type_sql(datum_type: DatumType) -> &'static str {
    match datum_type {
        DatumType::Bool => "boolean",
        DatumType::I16 => "smallint",
        DatumType::I32 => "integer",
        DatumType::I64 => "bigint",
        DatumType::F32 => "real",
        DatumType::F64 => "double precision",
        DatumType::Numeric => "numeric",
        DatumType::Bytea => "bytea",
        DatumType::Date => "date",
        DatumType::Time => "time",
        DatumType::Timestamp => "timestamp",
        DatumType::TimestampTz => "timestamptz",
        DatumType::Interval => "interval",
        DatumType::Uuid => "uuid",
        DatumType::Json => "json",
        // The wire schema does not carry array element types, so array
        // columns are mirrored as jsonb documents.
        DatumType::Jsonb | DatumType::Array => "jsonb",
        DatumType::Text | DatumType::Unspecified | DatumType::Null => "text",
    }
}

/// `CREATE TABLE IF NOT EXISTS` for a mirror table.
#[must_use]
pub fn create_table_sql(spec: &TableSpec) -> String {
    let mut columns: Vec<String> = spec
        .schema
        .columns
        .iter()
        .map(|col| {
            let datum_type = DatumType::try_from(col.r#type).unwrap_or(DatumType::Unspecified);
            let mut def = format!("{} {}", quote_ident(&col.name), column_type_sql(datum_type));
            if !col.nullable {
                def.push_str(" NOT NULL");
            }
            def
        })
        .collect();
    if !spec.schema.primary_key_columns.is_empty() {
        let pk_cols: Vec<String> = spec
            .schema
            .primary_key_columns
            .iter()
            .filter_map(|idx| spec.schema.columns.get(usize::try_from(*idx).unwrap_or(0)))
            .map(|col| quote_ident(&col.name))
            .collect();
        columns.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(&spec.name),
        columns.join(", ")
    )
}

/// Upsert one full row.
///
/// With a primary key this is `INSERT ... ON CONFLICT (pk) DO UPDATE`;
/// without one it degrades to a plain insert (row identity is the
/// whole row, so a "replace" of an identical row is a no-op handled by
/// delete-then-insert upstream).
#[must_use]
pub fn upsert_statement(spec: &TableSpec, row: WireRow) -> Statement {
    let names: Vec<String> = spec
        .schema
        .columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect();
    let placeholders: Vec<String> = (1..=names.len()).map(|i| format!("${i}")).collect();
    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(&spec.name),
        names.join(", "),
        placeholders.join(", ")
    );
    if !spec.schema.primary_key_columns.is_empty() {
        let pk_set: HashSet<usize> = spec.pk_indices().into_iter().collect();
        let pk_cols: Vec<String> = spec
            .pk_indices()
            .iter()
            .filter_map(|idx| spec.schema.columns.get(*idx))
            .map(|col| quote_ident(&col.name))
            .collect();
        let non_pk: Vec<String> = spec
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(idx, _)| !pk_set.contains(idx))
            .map(|(_, col)| {
                let name = quote_ident(&col.name);
                format!("{name} = EXCLUDED.{name}")
            })
            .collect();
        if non_pk.is_empty() {
            sql.push_str(&format!(" ON CONFLICT ({}) DO NOTHING", pk_cols.join(", ")));
        } else {
            sql.push_str(&format!(
                " ON CONFLICT ({}) DO UPDATE SET {}",
                pk_cols.join(", "),
                non_pk.join(", ")
            ));
        }
    }
    Statement { sql, params: row }
}

/// Delete a row by identity key. Uses `IS NOT DISTINCT FROM` so the
/// no-primary-key fallback (whole-row identity, possibly with NULLs)
/// still matches.
#[must_use]
pub fn delete_statement(spec: &TableSpec, key: &PrimaryKey) -> Statement {
    let (sql, params) = key_predicate(spec, key);
    Statement {
        sql: format!("DELETE FROM {} WHERE {}", quote_ident(&spec.name), sql),
        params,
    }
}

/// Select a single row by identity key, projecting columns in schema
/// order so decoding is positional.
#[must_use]
pub fn select_by_key_statement(spec: &TableSpec, key: &PrimaryKey) -> Statement {
    let names: Vec<String> = spec
        .schema
        .columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect();
    let (predicate, params) = key_predicate(spec, key);
    Statement {
        sql: format!(
            "SELECT {} FROM {} WHERE {}",
            names.join(", "),
            quote_ident(&spec.name),
            predicate
        ),
        params,
    }
}

fn key_predicate(spec: &TableSpec, key: &PrimaryKey) -> (String, Vec<WireDatum>) {
    let clauses: Vec<String> = spec
        .pk_indices()
        .iter()
        .enumerate()
        .filter_map(|(param_idx, col_idx)| {
            spec.schema.columns.get(*col_idx).map(|col| {
                format!(
                    "{} IS NOT DISTINCT FROM ${}",
                    quote_ident(&col.name),
                    param_idx + 1
                )
            })
        })
        .collect();
    (clauses.join(" AND "), key.clone())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::sync::Mutex;

    use palimpsest_proto::palimpsest::sync::v1::{Column, DatumType, Schema};
    use palimpsest_proto::wire::WireDatum;

    use super::super::db::{LocalDatabase, LocalWrite, TableSpec};
    use super::{
        create_table_sql, delete_statement, upsert_statement, SqlExecutor, SqlLocalDatabase,
        Statement,
    };
    use crate::local::db::{DbFuture, LocalDbError};

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

    #[test]
    fn create_table_includes_pk_and_nullability() {
        assert_eq!(
            create_table_sql(&spec()),
            "CREATE TABLE IF NOT EXISTS \"posts\" (\"id\" bigint NOT NULL, \
             \"title\" text, PRIMARY KEY (\"id\"))"
        );
    }

    #[test]
    fn upsert_uses_on_conflict_update() {
        let statement = upsert_statement(
            &spec(),
            vec![WireDatum::I64(1), WireDatum::Text(b"a".to_vec())],
        );
        assert_eq!(
            statement.sql,
            "INSERT INTO \"posts\" (\"id\", \"title\") VALUES ($1, $2) \
             ON CONFLICT (\"id\") DO UPDATE SET \"title\" = EXCLUDED.\"title\""
        );
        assert_eq!(statement.params.len(), 2);
    }

    #[test]
    fn delete_matches_null_safely() {
        let statement = delete_statement(&spec(), &vec![WireDatum::I64(9)]);
        assert_eq!(
            statement.sql,
            "DELETE FROM \"posts\" WHERE \"id\" IS NOT DISTINCT FROM $1"
        );
        assert_eq!(statement.params, vec![WireDatum::I64(9)]);
    }

    /// Records every statement (and optionally fails ones containing a
    /// marker) so tests can assert transaction framing.
    struct Recorder(Mutex<Vec<Statement>>, Option<&'static str>);

    impl SqlExecutor for Recorder {
        fn exec(
            &self,
            sql: String,
            params: Vec<WireDatum>,
            _expect: Option<Schema>,
        ) -> DbFuture<'_, Result<Vec<Vec<WireDatum>>, LocalDbError>> {
            let fail = self.1.is_some_and(|f| sql.contains(f));
            self.0.lock().unwrap().push(Statement { sql, params });
            Box::pin(async move {
                if fail {
                    Err(LocalDbError::Execution("boom".into()))
                } else {
                    Ok(Vec::new())
                }
            })
        }
    }

    #[tokio::test]
    async fn apply_writes_wraps_in_transaction() {
        let db = SqlLocalDatabase::new(Recorder(Mutex::new(Vec::new()), None));
        db.apply_writes(
            &spec(),
            vec![
                LocalWrite::Upsert {
                    row: vec![WireDatum::I64(1), WireDatum::Null],
                },
                LocalWrite::Delete {
                    key: vec![WireDatum::I64(2)],
                },
            ],
        )
        .await
        .unwrap();
        let log = db.executor().0.lock().unwrap();
        let sqls: Vec<&str> = log.iter().map(|s| s.sql.as_str()).collect();
        assert_eq!(sqls[0], "BEGIN");
        assert!(sqls[1].starts_with("INSERT INTO \"posts\""));
        assert!(sqls[2].starts_with("DELETE FROM \"posts\""));
        assert_eq!(sqls[3], "COMMIT");
    }

    #[tokio::test]
    async fn failed_statement_rolls_back() {
        let db = SqlLocalDatabase::new(Recorder(Mutex::new(Vec::new()), Some("DELETE")));
        let err = db
            .apply_writes(
                &spec(),
                vec![LocalWrite::Delete {
                    key: vec![WireDatum::I64(2)],
                }],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, LocalDbError::Execution(_)));
        let log = db.executor().0.lock().unwrap();
        assert_eq!(log.last().unwrap().sql, "ROLLBACK");
    }
}
