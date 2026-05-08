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

    #[must_use]
    pub fn relation(&self, table: TableId) -> Option<&RelationSchema> {
        self.relations.get(&table)
    }

    #[must_use]
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
