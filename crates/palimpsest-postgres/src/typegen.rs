// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! TypeScript row-type generation.
//!
//! Closes the client half of the "resolve types yourself" principle:
//! given a registered query and the introspected catalog, the server
//! can describe its own result shape, so clients *generate* row types
//! instead of hand-writing them (and hand-writing the micros-to-Date
//! decoders that went with them).
//!
//! Emitted types match what the wire actually carries after the datum
//! surface was widened: `timestamptz` arrives as a `Date`, `bigint`
//! as a JS `bigint`, arrays as arrays. Nothing here is transcribed by
//! an adopter, so nothing here can drift against the database.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use palimpsest_sql::prepared::{ParamSpec, ParamValue, PreparedQuery, QueryRegistry};
use palimpsest_sql::ColumnType;

use crate::introspect::IntrospectedTable;

/// The TypeScript type a [`ColumnType`] surfaces as in the JS client.
///
/// Mirrors `datum_to_js` in `palimpsest-client-js`: temporal datums
/// become `Date`, 64-bit integers become `bigint`, intervals become a
/// structured object, arrays become arrays.
const fn ts_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Bool => "boolean",
        // I64 crosses the wire as a JS bigint (I16/I32 widen to it in
        // the compiled output schema, so bigint is the safe surface).
        ColumnType::Int => "bigint",
        ColumnType::Float => "number",
        // Numeric keeps its lossless decimal text form.
        ColumnType::Numeric => "string",
        ColumnType::Timestamp | ColumnType::TimestampTz | ColumnType::Date => "Date",
        // `time` has no natural Date form; micros since midnight.
        ColumnType::Time => "number",
        ColumnType::Interval => "{ months: number; days: number; micros: bigint }",
        ColumnType::Bytea => "Uint8Array",
        ColumnType::Jsonb => "unknown",
        ColumnType::Array => "unknown[]",
        ColumnType::Uuid | ColumnType::Text | ColumnType::Enum | ColumnType::Unknown => "string",
    }
}

/// The TypeScript type accepted for a query parameter.
const fn param_ts_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Bool => "boolean",
        ColumnType::Int => "number | bigint",
        ColumnType::Float | ColumnType::Numeric => "number",
        // Temporal and uuid params bind as their text forms, which is
        // what the wire `VarValue` carries.
        _ => "string",
    }
}

/// Escapes a TypeScript object-literal key, quoting it when it is not
/// a plain identifier.
fn ts_key(name: &str) -> String {
    let plain = !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    if plain {
        name.to_owned()
    } else {
        format!("{name:?}")
    }
}

/// Upper-camel-cases a registered query name for use as a type name.
fn type_name(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let mut capitalize = true;
    for ch in query.chars() {
        if ch == '_' || ch == '-' || ch == ' ' {
            capitalize = true;
            continue;
        }
        if capitalize {
            out.extend(ch.to_uppercase());
            capitalize = false;
        } else {
            out.push(ch);
        }
    }
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, 'Q');
    }
    out
}

/// Representative value for one parameter. The row *shape* does not
/// depend on parameter values, so any well-typed value will do.
fn placeholder(spec: &ParamSpec) -> ParamValue {
    let scalar = match spec.ty {
        ColumnType::Bool => ParamValue::Bool(true),
        ColumnType::Int => ParamValue::Int(0),
        ColumnType::Float | ColumnType::Numeric => ParamValue::Float(0.0),
        ColumnType::Uuid => ParamValue::Text("00000000-0000-0000-0000-000000000000".to_owned()),
        ColumnType::Timestamp | ColumnType::TimestampTz => {
            ParamValue::Text("2000-01-01 00:00:00".to_owned())
        }
        ColumnType::Date => ParamValue::Text("2000-01-01".to_owned()),
        ColumnType::Time => ParamValue::Text("00:00:00".to_owned()),
        ColumnType::Jsonb => ParamValue::Text("{}".to_owned()),
        _ => ParamValue::Text(String::new()),
    };
    if spec.list {
        ParamValue::List(vec![scalar])
    } else {
        scalar
    }
}

/// Resolves one registered query's output columns from the
/// introspected catalog, via the same compile path the dataflow uses.
fn output_columns(
    registry: &QueryRegistry,
    query: &PreparedQuery,
    tables: &BTreeMap<String, &IntrospectedTable>,
) -> Result<Vec<(String, ColumnType)>, String> {
    let params = query
        .params
        .iter()
        .map(|spec| (spec.name.clone(), placeholder(spec)))
        .collect();
    let bound = registry
        .bind(&query.name, &params)
        .map_err(|err| format!("query '{}': {err}", query.name))?;

    let lookup = |table: &str| {
        tables
            .get(table)
            .map(|table| (table.id, table.scalar_schema()))
    };
    let plan = palimpsest_dataflow::palimpsest::compile_mir::compile_mir(&bound.graph, &lookup)
        .map_err(|err| format!("query '{}': {err}", query.name))?;
    Ok(plan
        .output_schema
        .columns()
        .iter()
        .map(|(name, column_type)| (name.clone(), *column_type))
        .collect())
}

/// Generates a TypeScript module declaring a row type and a parameter
/// type for every registered query, plus a `QueryTypes` map keyed by
/// query name.
///
/// # Errors
/// Returns a message naming the query that could not be described
/// (an unknown table, or a template the compiler refuses).
pub fn typescript_module(
    registry: &QueryRegistry,
    tables: &[IntrospectedTable],
) -> Result<String, String> {
    let by_name: BTreeMap<String, &IntrospectedTable> = tables
        .iter()
        .flat_map(|table| {
            [
                (table.name.clone(), table),
                (format!("{}.{}", table.namespace, table.name), table),
            ]
        })
        .collect();

    let mut out = String::new();
    out.push_str(
        "// Code generated by `palimpsest typegen` from the live Postgres\n\
         // catalog and the registered query set. DO NOT EDIT.\n\
         //\n\
         // Regenerate after changing db/queries or the database schema.\n\n",
    );

    let mut entries: Vec<(String, String, String)> = Vec::new();
    for query in registry.queries() {
        let columns = output_columns(registry, query, &by_name)?;
        let row_type = format!("{}Row", type_name(&query.name));
        let params_type = format!("{}Params", type_name(&query.name));

        let _ = writeln!(out, "/** Result row of the `{}` query. */", query.name);
        let _ = writeln!(out, "export interface {row_type} {{");
        for (name, column_type) in &columns {
            let _ = writeln!(out, "  {}: {};", ts_key(name), ts_type(*column_type));
        }
        let _ = writeln!(out, "}}\n");

        let _ = writeln!(out, "/** Parameters of the `{}` query. */", query.name);
        if query.params.is_empty() {
            let _ = writeln!(out, "export type {params_type} = Record<string, never>;\n");
        } else {
            let _ = writeln!(out, "export interface {params_type} {{");
            for spec in &query.params {
                let base = param_ts_type(spec.ty);
                let ty = if spec.list {
                    format!("Array<{base}>")
                } else {
                    base.to_owned()
                };
                let _ = writeln!(out, "  {}: {ty};", ts_key(&spec.name));
            }
            let _ = writeln!(out, "}}\n");
        }

        entries.push((query.name.clone(), row_type, params_type));
    }

    out.push_str("/** Every registered query, keyed by its wire name. */\n");
    out.push_str("export interface QueryTypes {\n");
    for (name, row_type, params_type) in &entries {
        let _ = writeln!(
            out,
            "  {}: {{ row: {row_type}; params: {params_type} }};",
            ts_key(name)
        );
    }
    out.push_str("}\n\n");
    out.push_str("export type QueryName = keyof QueryTypes;\n");
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{ts_key, ts_type, type_name, typescript_module};
    use crate::introspect::{IntrospectedColumn, IntrospectedTable};
    use palimpsest_sql::prepared::QueryRegistry;
    use palimpsest_sql::{Catalog, ColumnSchema, ColumnType, TableSchema};
    use palimpsest_wal::{DatumType, ReplicaIdentity, TableId};

    fn column(
        name: &str,
        datum_type: DatumType,
        column_type: ColumnType,
        primary_key: bool,
    ) -> IntrospectedColumn {
        IntrospectedColumn {
            name: name.to_owned(),
            type_oid: 0,
            datum_type,
            column_type,
            nullable: !primary_key,
            primary_key,
        }
    }

    fn tickets() -> IntrospectedTable {
        IntrospectedTable {
            id: TableId::new(77),
            namespace: "public".to_owned(),
            name: "tickets".to_owned(),
            replica_identity: ReplicaIdentity::Full,
            columns: vec![
                column("id", DatumType::I64, ColumnType::Int, true),
                column("title", DatumType::Text, ColumnType::Text, false),
                column(
                    "created_at",
                    DatumType::TimestampTz,
                    ColumnType::TimestampTz,
                    false,
                ),
                column("status", DatumType::Text, ColumnType::Enum, false),
                column(
                    "tags",
                    DatumType::Array(Box::new(DatumType::Text)),
                    ColumnType::Array,
                    false,
                ),
            ],
        }
    }

    fn catalog() -> Catalog {
        Catalog::new([TableSchema::new(
            "tickets",
            vec![
                ColumnSchema::new("id", ColumnType::Int),
                ColumnSchema::new("title", ColumnType::Text),
                ColumnSchema::new("created_at", ColumnType::TimestampTz),
                ColumnSchema::new("status", ColumnType::Enum),
                ColumnSchema::new("tags", ColumnType::Array),
            ],
        )])
    }

    #[test]
    fn maps_wire_types_onto_typescript() {
        assert_eq!(ts_type(ColumnType::Int), "bigint");
        assert_eq!(ts_type(ColumnType::TimestampTz), "Date");
        assert_eq!(ts_type(ColumnType::Date), "Date");
        assert_eq!(ts_type(ColumnType::Array), "unknown[]");
        assert_eq!(ts_type(ColumnType::Uuid), "string");
    }

    #[test]
    fn quotes_non_identifier_keys_and_camel_cases_names() {
        assert_eq!(ts_key("created_at"), "created_at");
        assert_eq!(ts_key("count(*)"), "\"count(*)\"");
        assert_eq!(type_name("board_tickets"), "BoardTickets");
        assert_eq!(type_name("BoardTickets"), "BoardTickets");
    }

    #[test]
    fn generates_row_and_param_types_from_the_catalog() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(
                "-- name: BoardTickets :many\n\
                 SELECT id, title, created_at, status, tags FROM tickets WHERE id = $1;\n",
                "queries.sql",
                &catalog(),
            )
            .expect("register");

        let module = typescript_module(&registry, &[tickets()]).expect("generate");

        // Timestamps cross as Date objects — no hand-written
        // micros-to-Date decoder, which is the whole point.
        assert!(
            module.contains("created_at: Date;"),
            "timestamptz must generate as Date:\n{module}"
        );
        assert!(module.contains("id: bigint;"), "{module}");
        assert!(module.contains("tags: unknown[];"), "{module}");
        assert!(module.contains("status: string;"), "{module}");
        assert!(
            module.contains("export interface BoardTicketsRow"),
            "{module}"
        );
        assert!(
            module.contains("BoardTickets: { row: BoardTicketsRow; params: BoardTicketsParams };"),
            "{module}"
        );
        assert!(module.contains("export type QueryName = keyof QueryTypes;"));
    }

    #[test]
    fn aggregate_output_columns_are_described() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(
                "-- name: TicketCounts :many\n\
                 SELECT status, COUNT(*) AS n FROM tickets GROUP BY status;\n",
                "queries.sql",
                &catalog(),
            )
            .expect("register");
        let module = typescript_module(&registry, &[tickets()]).expect("generate");
        assert!(module.contains("status: string;"), "{module}");
        assert!(module.contains("n: bigint;"), "{module}");
    }

    #[test]
    fn unknown_table_is_reported_not_silently_skipped() {
        let mut registry = QueryRegistry::new();
        registry
            .register(
                "Orphan",
                "SELECT id FROM tickets WHERE id = $1",
                &[palimpsest_sql::prepared::ParamDecl::new(
                    "id",
                    ColumnType::Int,
                )],
            )
            .expect("register");
        // Generate against an empty catalog: the table cannot resolve.
        let err = typescript_module(&registry, &[]).expect_err("must report the missing table");
        assert!(err.contains("Orphan"), "{err}");
    }
}
