// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashSet;

use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

impl Lsn {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn advance(&mut self, by: u64) {
        self.0 = self.0.saturating_add(by);
    }
}

impl Default for Lsn {
    fn default() -> Self {
        Self(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(u32);

impl TableId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub type_oid: u32,
    pub nullable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    tables: HashSet<TableId>,
}

impl Catalog {
    #[must_use]
    pub fn new(tables: impl IntoIterator<Item = TableId>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn contains(&self, table: TableId) -> bool {
        self.tables.contains(&table)
    }
}

pub type Tuple = Vec<String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalEvent {
    Begin {
        xid: u32,
    },
    Insert {
        table: TableId,
        new: Tuple,
    },
    Update {
        table: TableId,
        old: Option<Tuple>,
        new: Tuple,
    },
    Delete {
        table: TableId,
        old: Tuple,
    },
    Commit,
    RelationChange {
        table: TableId,
        new_columns: Vec<ColumnDef>,
    },
    Keepalive,
}

#[derive(Debug, Clone)]
pub struct WalGenerator {
    catalog: Catalog,
    next_lsn: Lsn,
    relation_emitted: HashSet<TableId>,
}

impl Default for WalGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl WalGenerator {
    #[must_use]
    pub fn new() -> Self {
        Self::with_catalog(Catalog::default())
    }

    #[must_use]
    pub fn with_catalog(catalog: Catalog) -> Self {
        Self {
            catalog,
            next_lsn: Lsn::default(),
            relation_emitted: HashSet::new(),
        }
    }

    #[must_use]
    pub const fn current_lsn(&self) -> Lsn {
        self.next_lsn
    }

    pub fn skip_lsn(&mut self, by: u64) {
        self.next_lsn.advance(by);
    }

    pub fn encode(&mut self, events: &[LogicalEvent]) -> Vec<Bytes> {
        let mut frames = Vec::new();

        for event in events {
            if let Some(table) = event.table_id() {
                debug_assert!(
                    self.catalog.tables.is_empty() || self.catalog.contains(table),
                    "logical event references table missing from test catalog"
                );

                if self.relation_emitted.insert(table) {
                    frames.push(Self::encode_relation(table));
                }
            }

            frames.push(self.encode_event(event));
            self.next_lsn.advance(1);
        }

        frames
    }

    fn encode_relation(table: TableId) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5);
        bytes.put_u8(b'R');
        bytes.put_u32(table.get());
        bytes.freeze()
    }

    fn encode_event(&self, event: &LogicalEvent) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u64(self.next_lsn.get());

        match event {
            LogicalEvent::Begin { xid } => {
                bytes.put_u8(b'B');
                bytes.put_u32(*xid);
            }
            LogicalEvent::Insert { table, new } => {
                bytes.put_u8(b'I');
                bytes.put_u32(table.get());
                put_tuple(&mut bytes, new);
            }
            LogicalEvent::Update { table, old, new } => {
                bytes.put_u8(b'U');
                bytes.put_u32(table.get());
                put_optional_tuple(&mut bytes, old.as_ref());
                put_tuple(&mut bytes, new);
            }
            LogicalEvent::Delete { table, old } => {
                bytes.put_u8(b'D');
                bytes.put_u32(table.get());
                put_tuple(&mut bytes, old);
            }
            LogicalEvent::Commit => {
                bytes.put_u8(b'C');
            }
            LogicalEvent::RelationChange { table, new_columns } => {
                bytes.put_u8(b'S');
                bytes.put_u32(table.get());
                bytes.put_u16(u16::try_from(new_columns.len()).unwrap_or(u16::MAX));
            }
            LogicalEvent::Keepalive => {
                bytes.put_u8(b'K');
            }
        }

        bytes.freeze()
    }
}

impl LogicalEvent {
    #[must_use]
    pub const fn table_id(&self) -> Option<TableId> {
        match self {
            Self::Insert { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. }
            | Self::RelationChange { table, .. } => Some(*table),
            Self::Begin { .. } | Self::Commit | Self::Keepalive => None,
        }
    }
}

fn put_optional_tuple(bytes: &mut BytesMut, tuple: Option<&Tuple>) {
    match tuple {
        Some(tuple) => {
            bytes.put_u8(1);
            put_tuple(bytes, tuple);
        }
        None => bytes.put_u8(0),
    }
}

fn put_tuple(bytes: &mut BytesMut, tuple: &Tuple) {
    bytes.put_u16(u16::try_from(tuple.len()).unwrap_or(u16::MAX));
    for value in tuple {
        bytes.put_u16(u16::try_from(value.len()).unwrap_or(u16::MAX));
        bytes.put_slice(value.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::{Catalog, LogicalEvent, TableId, WalGenerator};

    #[test]
    fn auto_emits_relation_before_first_row_for_table() {
        let table = TableId::new(42);
        let mut generator = WalGenerator::new();

        let frames = generator.encode(&[
            LogicalEvent::Insert {
                table,
                new: vec!["1".to_owned()],
            },
            LogicalEvent::Delete {
                table,
                old: vec!["1".to_owned()],
            },
        ]);

        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0][0], b'R');
        assert_eq!(frames[1][8], b'I');
        assert_eq!(frames[2][8], b'D');
    }

    #[test]
    fn advances_lsn_for_logical_events_only() {
        let mut generator = WalGenerator::new();
        generator.skip_lsn(10);

        generator.encode(&[
            LogicalEvent::Begin { xid: 7 },
            LogicalEvent::Commit,
            LogicalEvent::Keepalive,
        ]);

        assert_eq!(generator.current_lsn().get(), 14);
    }

    #[test]
    fn accepts_explicit_catalog() {
        let table = TableId::new(7);
        let mut generator = WalGenerator::with_catalog(Catalog::new([table]));

        let frames = generator.encode(&[LogicalEvent::Insert {
            table,
            new: vec!["1".to_owned()],
        }]);

        assert_eq!(frames.len(), 2);
    }
}
