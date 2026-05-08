// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::wal::{Catalog, TableId, Tuple};

pub type PrimaryKey = String;
pub type Row = Tuple;

#[derive(Debug, Clone)]
pub struct ReferenceExecutor {
    schema: Arc<Catalog>,
    tables: HashMap<TableId, BTreeMap<PrimaryKey, Row>>,
}

impl ReferenceExecutor {
    #[must_use]
    pub fn new(schema: Arc<Catalog>) -> Self {
        Self {
            schema,
            tables: HashMap::new(),
        }
    }

    #[must_use]
    pub const fn with_tables(
        schema: Arc<Catalog>,
        tables: HashMap<TableId, BTreeMap<PrimaryKey, Row>>,
    ) -> Self {
        Self { schema, tables }
    }

    #[must_use]
    pub fn schema(&self) -> &Catalog {
        &self.schema
    }

    #[must_use]
    pub const fn tables(&self) -> &HashMap<TableId, BTreeMap<PrimaryKey, Row>> {
        &self.tables
    }

    #[must_use]
    pub fn table(&self, table: TableId) -> Option<&BTreeMap<PrimaryKey, Row>> {
        self.tables.get(&table)
    }

    pub fn table_mut(&mut self, table: TableId) -> &mut BTreeMap<PrimaryKey, Row> {
        self.tables.entry(table).or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::{PrimaryKey, ReferenceExecutor, Row};
    use crate::wal::{Catalog, TableId};

    #[test]
    fn starts_with_schema_and_empty_tables() {
        let table = TableId::new(7);
        let executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        assert!(executor.schema().contains(table));
        assert!(executor.tables().is_empty());
    }

    #[test]
    fn stores_rows_by_table_and_primary_key() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.table_mut(table).insert(
            "post-1".to_owned(),
            vec!["post-1".to_owned(), "hello".to_owned()],
        );

        assert_eq!(
            executor.table(table).and_then(|rows| rows.get("post-1")),
            Some(&vec!["post-1".to_owned(), "hello".to_owned()])
        );
    }

    #[test]
    fn accepts_seed_tables() {
        let table = TableId::new(7);
        let mut rows = BTreeMap::<PrimaryKey, Row>::new();
        rows.insert("post-1".to_owned(), vec!["post-1".to_owned()]);

        let executor = ReferenceExecutor::with_tables(
            Arc::new(Catalog::new([table])),
            std::iter::once((table, rows)).collect(),
        );

        assert_eq!(executor.table(table).map(BTreeMap::len), Some(1));
    }
}
