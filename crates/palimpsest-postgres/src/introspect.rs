// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `pg_catalog` introspection.
//!
//! Everything the engine needs to know about a streamed table —
//! columns, types, nullability, primary key, replica identity — is
//! read from the live catalog. Enums decode as their labels, domains
//! resolve to their base type, and arrays carry their element type.
//! Nothing here is supplied by the adopter, so nothing here can drift
//! against the database silently.

use palimpsest_sql::ColumnType;
use palimpsest_wal::{
    stock_postgres_16_type, ColumnDef, DatumType, RelationSchema, ReplicaIdentity, TableId,
};
use tokio_postgres::Client;

use crate::error::PostgresRuntimeError;

/// One introspected column.
#[derive(Debug, Clone)]
pub struct IntrospectedColumn {
    /// Column name.
    pub name: String,
    /// Postgres type OID as reported by `pg_attribute`.
    pub type_oid: u32,
    /// Decoder type for WAL / snapshot values.
    pub datum_type: DatumType,
    /// Coarse SQL-layer type for planning and validation.
    pub column_type: ColumnType,
    /// Whether the column admits NULL.
    pub nullable: bool,
    /// Whether the column is part of the primary key.
    pub primary_key: bool,
}

/// One introspected table.
#[derive(Debug, Clone)]
pub struct IntrospectedTable {
    /// `pg_class` OID — used verbatim as the engine [`TableId`].
    pub id: TableId,
    /// Schema (namespace) the table lives in.
    pub namespace: String,
    /// Table name.
    pub name: String,
    /// Replica identity currently configured (`d`/`n`/`f`/`i`).
    pub replica_identity: ReplicaIdentity,
    /// Columns in attribute order.
    pub columns: Vec<IntrospectedColumn>,
}

impl IntrospectedTable {
    /// The SQL-layer schema (name → coarse type, in column order).
    #[must_use]
    pub fn scalar_schema(&self) -> palimpsest_dataflow::palimpsest::eval::ScalarSchema {
        palimpsest_dataflow::palimpsest::eval::ScalarSchema::from_pairs(
            self.columns
                .iter()
                .map(|column| (column.name.clone(), column.column_type)),
        )
    }

    /// The WAL-catalog schema used to decode pgoutput tuples.
    #[must_use]
    pub fn relation_schema(&self) -> RelationSchema {
        RelationSchema::new(
            self.id,
            self.namespace.clone(),
            self.name.clone(),
            self.replica_identity,
            self.columns
                .iter()
                .map(|column| ColumnDef {
                    name: column.name.clone(),
                    type_oid: column.type_oid,
                    datum_type: column.datum_type.clone(),
                    nullable: column.nullable,
                    key: column.primary_key,
                })
                .collect(),
        )
    }

    /// Positions of the primary-key columns, in column order.
    #[must_use]
    pub fn primary_key_positions(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.primary_key.then_some(index))
            .collect()
    }
}

/// Builds the SQL-layer [`palimpsest_sql::Catalog`] from introspected
/// tables, so named-query registration validates against the live
/// database schema instead of a hand-maintained (or demo) catalog.
#[must_use]
pub fn sql_catalog(tables: &[IntrospectedTable]) -> palimpsest_sql::Catalog {
    palimpsest_sql::Catalog::new(tables.iter().flat_map(|table| {
        // Register under both the bare and schema-qualified name so
        // queries can write either.
        let columns: Vec<_> = table
            .columns
            .iter()
            .map(|column| {
                palimpsest_sql::ColumnSchema::new(column.name.clone(), column.column_type)
            })
            .collect();
        [
            table.name.clone(),
            format!("{}.{}", table.namespace, table.name),
        ]
        .map(|name| palimpsest_sql::TableSchema::new(name, columns.clone()))
    }))
}

const COLUMN_SQL: &str = "\
SELECT c.oid AS table_oid,
       n.nspname AS namespace,
       c.relname AS table_name,
       c.relreplident::text AS replica_identity,
       a.attname AS column_name,
       a.attnum::int AS attnum,
       NOT a.attnotnull AS nullable,
       COALESCE(i.indisprimary AND a.attnum = ANY(i.indkey), false) AS primary_key,
       t.oid AS type_oid,
       t.typtype::text AS typtype,
       t.typcategory::text AS typcategory,
       bt.oid AS base_type_oid,
       bt.typtype::text AS base_typtype,
       et.oid AS elem_type_oid,
       et.typtype::text AS elem_typtype
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_type bt ON t.typtype = 'd' AND bt.oid = t.typbasetype
LEFT JOIN pg_type et ON t.typcategory = 'A' AND et.oid = t.typelem
LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
WHERE c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
  AND n.nspname = $1 AND c.relname = $2
ORDER BY a.attnum";

/// Every non-system table, as `namespace.relation`, in catalog order.
const ALL_TABLES_SQL: &str = "\
SELECT n.nspname AS namespace, c.relname AS table_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%'
  AND n.nspname NOT LIKE 'pg_temp%'
ORDER BY n.nspname, c.relname";

/// Introspects every user table in the database.
///
/// The CLI uses this to build the SQL catalog *before* the query
/// registry exists — registration needs a catalog, and the streamed
/// table set is then derived from the registered queries. Tables in
/// the `public` schema are also reachable by their bare name.
///
/// # Errors
/// [`PostgresRuntimeError::Query`] on catalog failures.
pub async fn introspect_all_tables(
    client: &Client,
) -> Result<Vec<IntrospectedTable>, PostgresRuntimeError> {
    let rows = client
        .query(ALL_TABLES_SQL, &[])
        .await
        .map_err(|source| PostgresRuntimeError::query("table enumeration", source))?;
    let names: Vec<String> = rows
        .iter()
        .map(|row| {
            let namespace: String = row.get("namespace");
            let name: String = row.get("table_name");
            format!("{namespace}.{name}")
        })
        .collect();
    introspect_tables(client, &names).await
}

/// Splits a registered table reference into `(namespace, relation)`,
/// defaulting the namespace to `public`.
fn split_table_name(table: &str) -> (&str, &str) {
    table
        .split_once('.')
        .map_or(("public", table), |(namespace, relation)| {
            (namespace, relation)
        })
}

/// Introspects every table in `tables` from `pg_catalog`.
///
/// # Errors
/// [`PostgresRuntimeError::MissingTable`] when a registered table does
/// not exist; [`PostgresRuntimeError::Query`] on catalog failures.
pub async fn introspect_tables(
    client: &Client,
    tables: &[String],
) -> Result<Vec<IntrospectedTable>, PostgresRuntimeError> {
    let mut introspected = Vec::with_capacity(tables.len());
    for table in tables {
        let (namespace, relation) = split_table_name(table);
        let rows = client
            .query(COLUMN_SQL, &[&namespace, &relation])
            .await
            .map_err(|source| PostgresRuntimeError::query("catalog introspection", source))?;
        if rows.is_empty() {
            return Err(PostgresRuntimeError::MissingTable {
                table: table.clone(),
            });
        }

        let table_oid: u32 = rows[0].get("table_oid");
        let namespace: String = rows[0].get("namespace");
        let name: String = rows[0].get("table_name");
        let replica_identity: String = rows[0].get("replica_identity");
        let replica_identity =
            ReplicaIdentity::try_from(replica_identity.bytes().next().unwrap_or(b'd'))
                .unwrap_or(ReplicaIdentity::Default);

        let mut columns = Vec::with_capacity(rows.len());
        for row in &rows {
            let type_oid: u32 = row.get("type_oid");
            let typtype: String = row.get("typtype");
            let typcategory: String = row.get("typcategory");
            let base_type_oid: Option<u32> = row.get("base_type_oid");
            let base_typtype: Option<String> = row.get("base_typtype");
            let elem_type_oid: Option<u32> = row.get("elem_type_oid");
            let elem_typtype: Option<String> = row.get("elem_typtype");

            let (datum_type, column_type) = map_type(
                type_oid,
                &typtype,
                &typcategory,
                base_type_oid,
                base_typtype.as_deref(),
                elem_type_oid,
                elem_typtype.as_deref(),
            );

            columns.push(IntrospectedColumn {
                name: row.get("column_name"),
                type_oid,
                datum_type,
                column_type,
                nullable: row.get("nullable"),
                primary_key: row.get("primary_key"),
            });
        }

        introspected.push(IntrospectedTable {
            id: TableId::new(table_oid),
            namespace,
            name,
            replica_identity,
            columns,
        });
    }
    Ok(introspected)
}

/// Maps one `pg_type` row onto the engine's decoder + planner types.
/// Total: anything unrecognized decodes as text (pgoutput ships every
/// value in text form, so text is always faithful).
fn map_type(
    type_oid: u32,
    typtype: &str,
    typcategory: &str,
    base_type_oid: Option<u32>,
    base_typtype: Option<&str>,
    elem_type_oid: Option<u32>,
    elem_typtype: Option<&str>,
) -> (DatumType, ColumnType) {
    // Stock scalar / array OIDs.
    if let Some(datum_type) = stock_postgres_16_type(type_oid) {
        let column_type = column_type_of(&datum_type);
        return (datum_type, column_type);
    }
    // Enums decode as their labels.
    if typtype == "e" {
        return (DatumType::Text, ColumnType::Enum);
    }
    // Domains resolve to their base type (one level; a domain over a
    // domain falls through to text).
    if typtype == "d" {
        if let Some(base_oid) = base_type_oid {
            if let Some(datum_type) = stock_postgres_16_type(base_oid) {
                let column_type = column_type_of(&datum_type);
                return (datum_type, column_type);
            }
            if base_typtype == Some("e") {
                return (DatumType::Text, ColumnType::Enum);
            }
        }
        return (DatumType::Text, ColumnType::Text);
    }
    // Arrays of user-defined types: element enums (and anything else
    // unrecognized) decode element-wise as text.
    if typcategory == "A" {
        let element = elem_type_oid
            .and_then(stock_postgres_16_type)
            .unwrap_or_else(|| {
                // Element enums decode as their labels; anything else
                // unrecognized falls back to text too, loudly.
                if elem_typtype != Some("e") {
                    tracing::warn!(
                        type_oid,
                        elem_type_oid,
                        "array element type not recognized; decoding elements as text"
                    );
                }
                DatumType::Text
            });
        return (DatumType::Array(Box::new(element)), ColumnType::Array);
    }
    tracing::warn!(
        type_oid,
        typtype,
        typcategory,
        "type not recognized; decoding as text"
    );
    (DatumType::Text, ColumnType::Text)
}

/// Coarse SQL-layer type for a decoder type.
const fn column_type_of(datum_type: &DatumType) -> ColumnType {
    match datum_type {
        DatumType::Bool => ColumnType::Bool,
        DatumType::I16 | DatumType::I32 | DatumType::I64 => ColumnType::Int,
        DatumType::F32 | DatumType::F64 => ColumnType::Float,
        DatumType::Numeric => ColumnType::Numeric,
        DatumType::Text => ColumnType::Text,
        DatumType::Bytea => ColumnType::Bytea,
        DatumType::Date => ColumnType::Date,
        DatumType::Time => ColumnType::Time,
        DatumType::Timestamp => ColumnType::Timestamp,
        DatumType::TimestampTz => ColumnType::TimestampTz,
        DatumType::Interval => ColumnType::Interval,
        DatumType::Uuid => ColumnType::Uuid,
        DatumType::Json | DatumType::Jsonb => ColumnType::Jsonb,
        DatumType::Array(_) => ColumnType::Array,
    }
}

#[cfg(test)]
mod tests {
    use super::{map_type, split_table_name};
    use palimpsest_sql::ColumnType;
    use palimpsest_wal::{DatumType, INT8_OID, TEXT_OID, TIMESTAMPTZ_OID, UUID_OID};

    #[test]
    fn splits_schema_qualified_names() {
        assert_eq!(split_table_name("tickets"), ("public", "tickets"));
        assert_eq!(split_table_name("app.tickets"), ("app", "tickets"));
    }

    #[test]
    fn maps_stock_types() {
        assert_eq!(
            map_type(INT8_OID, "b", "N", None, None, None, None),
            (DatumType::I64, ColumnType::Int)
        );
        assert_eq!(
            map_type(TIMESTAMPTZ_OID, "b", "D", None, None, None, None),
            (DatumType::TimestampTz, ColumnType::TimestampTz)
        );
        assert_eq!(
            map_type(UUID_OID, "b", "U", None, None, None, None),
            (DatumType::Uuid, ColumnType::Uuid)
        );
    }

    #[test]
    fn maps_enums_domains_and_arrays_of_enums() {
        // Enum: user-defined OID, typtype 'e'.
        assert_eq!(
            map_type(543_210, "e", "E", None, None, None, None),
            (DatumType::Text, ColumnType::Enum)
        );
        // Domain over int8.
        assert_eq!(
            map_type(543_211, "d", "N", Some(INT8_OID), Some("b"), None, None),
            (DatumType::I64, ColumnType::Int)
        );
        // Domain over an enum.
        assert_eq!(
            map_type(543_212, "d", "E", Some(543_210), Some("e"), None, None),
            (DatumType::Text, ColumnType::Enum)
        );
        // Array of a user-defined enum.
        assert_eq!(
            map_type(543_213, "b", "A", None, None, Some(543_210), Some("e")),
            (
                DatumType::Array(Box::new(DatumType::Text)),
                ColumnType::Array
            )
        );
        // Array of text (stock OID path).
        assert_eq!(
            map_type(
                palimpsest_wal::TEXT_ARRAY_OID,
                "b",
                "A",
                None,
                None,
                Some(TEXT_OID),
                Some("b")
            ),
            (
                DatumType::Array(Box::new(DatumType::Text)),
                ColumnType::Array
            )
        );
    }
}
