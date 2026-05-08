// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::wal::{Catalog, LogicalEvent, TableId, Tuple};

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

    pub fn apply(&mut self, events: &[LogicalEvent]) {
        for event in events {
            match event {
                LogicalEvent::Insert { table, new } => {
                    self.table_mut(*table).insert(primary_key(new), new.clone());
                }
                LogicalEvent::Update { table, old, new } => {
                    if let Some(old) = old {
                        self.table_mut(*table).remove(&primary_key(old));
                    }
                    self.table_mut(*table).insert(primary_key(new), new.clone());
                }
                LogicalEvent::Delete { table, old } => {
                    self.table_mut(*table).remove(&primary_key(old));
                }
                LogicalEvent::RelationChange { table, .. } => {
                    self.table_mut(*table);
                }
                LogicalEvent::Begin { .. }
                | LogicalEvent::Commit
                | LogicalEvent::Truncate { .. }
                | LogicalEvent::Origin { .. }
                | LogicalEvent::StreamStart { .. }
                | LogicalEvent::StreamStop
                | LogicalEvent::StreamCommit { .. }
                | LogicalEvent::StreamAbort { .. }
                | LogicalEvent::BeginPrepare { .. }
                | LogicalEvent::Prepare { .. }
                | LogicalEvent::CommitPrepared { .. }
                | LogicalEvent::RollbackPrepared { .. }
                | LogicalEvent::Keepalive => {}
            }
        }
    }
}

fn primary_key(row: &Row) -> PrimaryKey {
    row.first().cloned().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::{PrimaryKey, ReferenceExecutor, Row};
    use crate::wal::{Catalog, LogicalEvent, TableId};

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

    #[test]
    fn applies_insert_update_and_delete_events() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.apply(&[
            LogicalEvent::Begin { xid: 1 },
            LogicalEvent::Insert {
                table,
                new: vec!["post-1".to_owned(), "draft".to_owned()],
            },
            LogicalEvent::Update {
                table,
                old: Some(vec!["post-1".to_owned(), "draft".to_owned()]),
                new: vec!["post-1".to_owned(), "published".to_owned()],
            },
            LogicalEvent::Commit,
        ]);

        assert_eq!(
            executor.table(table).and_then(|rows| rows.get("post-1")),
            Some(&vec!["post-1".to_owned(), "published".to_owned()])
        );

        executor.apply(&[LogicalEvent::Delete {
            table,
            old: vec!["post-1".to_owned(), "published".to_owned()],
        }]);

        assert!(executor.table(table).is_some_and(BTreeMap::is_empty));
    }

    #[test]
    fn applies_update_without_old_tuple_as_upsert() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.apply(&[LogicalEvent::Update {
            table,
            old: None,
            new: vec!["post-1".to_owned(), "published".to_owned()],
        }]);

        assert_eq!(executor.table(table).map(BTreeMap::len), Some(1));
    }

    #[test]
    fn relation_change_creates_table_slot() {
        let table = TableId::new(7);
        let mut executor = ReferenceExecutor::new(Arc::new(Catalog::new([table])));

        executor.apply(&[LogicalEvent::RelationChange {
            table,
            new_columns: Vec::new(),
        }]);

        assert!(executor.table(table).is_some());
    }
}
