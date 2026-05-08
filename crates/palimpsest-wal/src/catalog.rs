// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeMap;

use crate::{stock_postgres_16_type, ColumnDef, Result, TableId, WalError};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    relations: BTreeMap<TableId, RelationSchema>,
}

impl Catalog {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            relations: BTreeMap::new(),
        }
    }

    pub fn upsert_relation(&mut self, relation: RelationSchema) {
        self.relations.insert(relation.table, relation);
    }

    pub fn remove_relation(&mut self, table: TableId) -> Option<RelationSchema> {
        self.relations.remove(&table)
    }

    #[must_use]
    pub fn relation(&self, table: TableId) -> Option<&RelationSchema> {
        self.relations.get(&table)
    }

    pub fn relations(&self) -> impl Iterator<Item = &RelationSchema> {
        self.relations.values()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationSchema {
    pub table: TableId,
    pub namespace: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<ColumnDef>,
}

impl RelationSchema {
    pub fn new(
        table: TableId,
        namespace: impl Into<String>,
        name: impl Into<String>,
        replica_identity: ReplicaIdentity,
        columns: Vec<ColumnDef>,
    ) -> Self {
        Self {
            table,
            namespace: namespace.into(),
            name: name.into(),
            replica_identity,
            columns,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

impl TryFrom<u8> for ReplicaIdentity {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            b'd' => Ok(Self::Default),
            b'n' => Ok(Self::Nothing),
            b'f' => Ok(Self::Full),
            b'i' => Ok(Self::Index),
            _ => Err(WalError::Malformed("unknown replica identity")),
        }
    }
}

#[allow(clippy::redundant_pub_crate)]
pub(crate) fn column_from_pgoutput(flags: u8, name: String, type_oid: u32) -> Result<ColumnDef> {
    let datum_type =
        stock_postgres_16_type(type_oid).ok_or(WalError::UnsupportedTypeOid(type_oid))?;
    Ok(ColumnDef {
        name,
        type_oid,
        datum_type,
        nullable: true,
        key: flags & 1 == 1,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogProbeRow {
    pub table_oid: u32,
    pub namespace: String,
    pub table_name: String,
    pub replica_identity: ReplicaIdentity,
    pub column_name: String,
    pub type_oid: u32,
    pub attnum: i16,
    pub nullable: bool,
    pub primary_key: bool,
}

#[must_use]
pub const fn catalog_probe_sql() -> &'static str {
    "SELECT c.oid AS table_oid, n.nspname AS namespace, c.relname AS table_name, \
     c.relreplident AS replica_identity, a.attname AS column_name, a.atttypid AS type_oid, \
     a.attnum, NOT a.attnotnull AS nullable, i.indisprimary AND a.attnum = ANY(i.indkey) AS primary_key \
     FROM pg_class c \
     JOIN pg_namespace n ON n.oid = c.relnamespace \
     JOIN pg_attribute a ON a.attrelid = c.oid \
     LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
     WHERE c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped \
     ORDER BY n.nspname, c.relname, a.attnum"
}

pub fn load_catalog_from_probe_rows(
    rows: impl IntoIterator<Item = CatalogProbeRow>,
) -> Result<Catalog> {
    let mut grouped: BTreeMap<TableId, ProbeRelation> = BTreeMap::new();

    for row in rows {
        let table = TableId::new(row.table_oid);
        let column = ColumnDef {
            name: row.column_name,
            type_oid: row.type_oid,
            datum_type: stock_postgres_16_type(row.type_oid)
                .ok_or(WalError::UnsupportedTypeOid(row.type_oid))?,
            nullable: row.nullable,
            key: row.primary_key,
        };

        grouped
            .entry(table)
            .or_insert_with(|| {
                ProbeRelation::new(table, row.namespace, row.table_name, row.replica_identity)
            })
            .columns
            .push((row.attnum, column));
    }

    let mut catalog = Catalog::new();
    for (_, mut relation) in grouped {
        relation.columns.sort_by_key(|(attnum, _)| *attnum);
        catalog.upsert_relation(RelationSchema::new(
            relation.table,
            relation.namespace,
            relation.name,
            relation.replica_identity,
            relation
                .columns
                .into_iter()
                .map(|(_, column)| column)
                .collect(),
        ));
    }

    Ok(catalog)
}

struct ProbeRelation {
    table: TableId,
    namespace: String,
    name: String,
    replica_identity: ReplicaIdentity,
    columns: Vec<(i16, ColumnDef)>,
}

impl ProbeRelation {
    const fn new(
        table: TableId,
        namespace: String,
        name: String,
        replica_identity: ReplicaIdentity,
    ) -> Self {
        Self {
            table,
            namespace,
            name,
            replica_identity,
            columns: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        catalog_probe_sql, load_catalog_from_probe_rows, CatalogProbeRow, ReplicaIdentity,
    };
    use crate::{DatumType, INT4_OID, TEXT_OID};

    #[test]
    fn probe_sql_mentions_required_catalog_tables() {
        let sql = catalog_probe_sql();
        assert!(sql.contains("pg_class"));
        assert!(sql.contains("pg_attribute"));
        assert!(sql.contains("pg_index"));
        assert!(sql.contains("pg_namespace"));
    }

    #[test]
    fn loads_catalog_from_probe_rows_in_attribute_order() {
        let catalog = load_catalog_from_probe_rows([
            row("body", TEXT_OID, 2, false),
            row("id", INT4_OID, 1, true),
        ])
        .unwrap();

        let relation = catalog
            .relations()
            .next()
            .expect("probe rows should create a relation");
        assert_eq!(relation.namespace, "public");
        assert_eq!(relation.name, "posts");
        assert_eq!(relation.columns[0].name, "id");
        assert_eq!(relation.columns[0].datum_type, DatumType::I32);
        assert!(relation.columns[0].key);
        assert_eq!(relation.columns[1].name, "body");
    }

    fn row(name: &str, type_oid: u32, attnum: i16, primary_key: bool) -> CatalogProbeRow {
        CatalogProbeRow {
            table_oid: 42,
            namespace: "public".to_owned(),
            table_name: "posts".to_owned(),
            replica_identity: ReplicaIdentity::Default,
            column_name: name.to_owned(),
            type_oid,
            attnum,
            nullable: !primary_key,
            primary_key,
        }
    }
}
