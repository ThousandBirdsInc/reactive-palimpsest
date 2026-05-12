// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Synthetic WAL event generator used by the Postgres-free test harness.

#![allow(missing_docs)]

use std::collections::{BTreeMap, HashSet};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableDef {
    #[must_use]
    pub fn new(id: TableId, name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        Self {
            id,
            name: name.into(),
            columns,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TruncateOpts {
    pub cascade: bool,
    pub restart_identity: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    tables: HashSet<TableId>,
    table_defs: BTreeMap<TableId, TableDef>,
    names: BTreeMap<String, TableId>,
}

impl Catalog {
    #[must_use]
    pub fn new(tables: impl IntoIterator<Item = TableId>) -> Self {
        Self::with_tables(
            tables
                .into_iter()
                .map(|table| TableDef::new(table, format!("table_{}", table.get()), Vec::new())),
        )
    }

    #[must_use]
    pub fn with_tables(tables: impl IntoIterator<Item = TableDef>) -> Self {
        let mut catalog = Self::default();
        for table in tables {
            catalog.names.insert(table.name.clone(), table.id);
            catalog.tables.insert(table.id);
            catalog.table_defs.insert(table.id, table);
        }
        catalog
    }

    #[must_use]
    pub fn contains(&self, table: TableId) -> bool {
        self.tables.contains(&table)
    }

    #[must_use]
    pub fn table_id(&self, name: &str) -> Option<TableId> {
        self.names.get(name).copied()
    }

    #[must_use]
    pub fn table(&self, table: TableId) -> Option<&TableDef> {
        self.table_defs.get(&table)
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
    Truncate {
        tables: Vec<TableId>,
        options: TruncateOpts,
    },
    Origin {
        lsn: Lsn,
        name: String,
    },
    StreamStart {
        xid: u32,
        first_segment: bool,
    },
    StreamStop,
    StreamCommit {
        xid: u32,
    },
    StreamAbort {
        xid: u32,
        subxid: u32,
    },
    BeginPrepare {
        xid: u32,
        gid: String,
    },
    Prepare {
        xid: u32,
        gid: String,
    },
    CommitPrepared {
        xid: u32,
        gid: String,
    },
    RollbackPrepared {
        xid: u32,
        gid: String,
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

    pub fn encode_pgoutput(&mut self, events: &[LogicalEvent]) -> Vec<Bytes> {
        let mut frames = Vec::new();

        for event in events {
            if let Some((table, column_count)) = event.relation_shape() {
                debug_assert!(
                    self.catalog.tables.is_empty() || self.catalog.contains(table),
                    "logical event references table missing from test catalog"
                );

                if self.relation_emitted.insert(table) {
                    frames.push(Self::encode_pgoutput_relation(table, column_count));
                }
            }

            if let Some(frame) = self.encode_pgoutput_event(event) {
                frames.push(frame);
                self.next_lsn.advance(1);
            }
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
                for column in new_columns {
                    put_string(&mut bytes, &column.name);
                    bytes.put_u32(column.type_oid);
                    bytes.put_u8(u8::from(column.nullable));
                }
            }
            LogicalEvent::Truncate { tables, options } => {
                bytes.put_u8(b'T');
                bytes.put_u32(u32::try_from(tables.len()).unwrap_or(u32::MAX));
                bytes.put_u8(options.flags());
                for table in tables {
                    bytes.put_u32(table.get());
                }
            }
            LogicalEvent::Origin { lsn, name } => {
                bytes.put_u8(b'O');
                bytes.put_u64(lsn.get());
                put_string(&mut bytes, name);
            }
            LogicalEvent::StreamStart { xid, first_segment } => {
                bytes.put_u8(b'Y');
                bytes.put_u32(*xid);
                bytes.put_u8(u8::from(*first_segment));
            }
            LogicalEvent::StreamStop => {
                bytes.put_u8(b'E');
            }
            LogicalEvent::StreamCommit { xid } => {
                bytes.put_u8(b'c');
                bytes.put_u32(*xid);
            }
            LogicalEvent::StreamAbort { xid, subxid } => {
                bytes.put_u8(b'A');
                bytes.put_u32(*xid);
                bytes.put_u32(*subxid);
            }
            LogicalEvent::BeginPrepare { xid, gid } => {
                bytes.put_u8(b'b');
                bytes.put_u32(*xid);
                put_string(&mut bytes, gid);
            }
            LogicalEvent::Prepare { xid, gid } => {
                bytes.put_u8(b'P');
                bytes.put_u32(*xid);
                put_string(&mut bytes, gid);
            }
            LogicalEvent::CommitPrepared { xid, gid } => {
                bytes.put_u8(b'p');
                bytes.put_u32(*xid);
                put_string(&mut bytes, gid);
            }
            LogicalEvent::RollbackPrepared { xid, gid } => {
                bytes.put_u8(b'r');
                bytes.put_u32(*xid);
                put_string(&mut bytes, gid);
            }
            LogicalEvent::Keepalive => {
                bytes.put_u8(b'K');
            }
        }

        bytes.freeze()
    }

    fn encode_pgoutput_relation(table: TableId, column_count: usize) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'R');
        bytes.put_u32(table.get());
        put_cstr(&mut bytes, "public");
        put_cstr(&mut bytes, &format!("table_{}", table.get()));
        bytes.put_u8(b'd');
        bytes.put_u16(u16::try_from(column_count).unwrap_or(u16::MAX));
        for index in 0..column_count {
            bytes.put_u8(u8::from(index == 0));
            put_cstr(&mut bytes, &format!("c{}", index + 1));
            bytes.put_u32(25);
            bytes.put_u32(u32::MAX);
        }
        bytes.freeze()
    }

    #[allow(clippy::too_many_lines)]
    fn encode_pgoutput_event(&self, event: &LogicalEvent) -> Option<Bytes> {
        let mut bytes = BytesMut::new();

        match event {
            LogicalEvent::Begin { xid } => {
                bytes.put_u8(b'B');
                bytes.put_u64(self.next_lsn.get());
                bytes.put_u64(0);
                bytes.put_u32(*xid);
            }
            LogicalEvent::Insert { table, new } => {
                bytes.put_u8(b'I');
                bytes.put_u32(table.get());
                bytes.put_u8(b'N');
                put_pgoutput_tuple(&mut bytes, new);
            }
            LogicalEvent::Update { table, old, new } => {
                bytes.put_u8(b'U');
                bytes.put_u32(table.get());
                if let Some(old) = old {
                    bytes.put_u8(b'O');
                    put_pgoutput_tuple(&mut bytes, old);
                }
                bytes.put_u8(b'N');
                put_pgoutput_tuple(&mut bytes, new);
            }
            LogicalEvent::Delete { table, old } => {
                bytes.put_u8(b'D');
                bytes.put_u32(table.get());
                bytes.put_u8(b'O');
                put_pgoutput_tuple(&mut bytes, old);
            }
            LogicalEvent::Commit => {
                bytes.put_u8(b'C');
                bytes.put_u8(0);
                bytes.put_u64(self.next_lsn.get());
                bytes.put_u64(self.next_lsn.get().saturating_add(1));
                bytes.put_u64(0);
            }
            LogicalEvent::RelationChange { table, new_columns } => {
                bytes.put_u8(b'R');
                bytes.put_u32(table.get());
                put_cstr(&mut bytes, "public");
                put_cstr(&mut bytes, &format!("table_{}", table.get()));
                bytes.put_u8(b'd');
                bytes.put_u16(u16::try_from(new_columns.len()).unwrap_or(u16::MAX));
                for (index, column) in new_columns.iter().enumerate() {
                    bytes.put_u8(u8::from(index == 0));
                    put_cstr(&mut bytes, &column.name);
                    bytes.put_u32(column.type_oid);
                    bytes.put_u32(u32::MAX);
                }
            }
            LogicalEvent::Truncate { tables, options } => {
                bytes.put_u8(b'T');
                bytes.put_u32(u32::try_from(tables.len()).unwrap_or(u32::MAX));
                bytes.put_u8(options.flags());
                for table in tables {
                    bytes.put_u32(table.get());
                }
            }
            LogicalEvent::Origin { lsn, name } => {
                bytes.put_u8(b'O');
                bytes.put_u64(lsn.get());
                put_cstr(&mut bytes, name);
            }
            LogicalEvent::StreamStart { xid, first_segment } => {
                bytes.put_u8(b'S');
                bytes.put_u32(*xid);
                bytes.put_u8(u8::from(*first_segment));
            }
            LogicalEvent::StreamStop => {
                bytes.put_u8(b'E');
            }
            LogicalEvent::StreamCommit { xid } => {
                bytes.put_u8(b'c');
                bytes.put_u32(*xid);
                bytes.put_u8(0);
                bytes.put_u64(self.next_lsn.get());
                bytes.put_u64(self.next_lsn.get().saturating_add(1));
                bytes.put_u64(0);
            }
            LogicalEvent::StreamAbort { xid, subxid } => {
                bytes.put_u8(b'A');
                bytes.put_u32(*xid);
                bytes.put_u32(*subxid);
            }
            LogicalEvent::BeginPrepare { xid, gid } => {
                bytes.put_u8(b'b');
                put_pgoutput_prepare(&mut bytes, self.next_lsn, *xid, gid);
            }
            LogicalEvent::Prepare { xid, gid } => {
                bytes.put_u8(b'P');
                put_pgoutput_prepare(&mut bytes, self.next_lsn, *xid, gid);
            }
            LogicalEvent::CommitPrepared { xid, gid } => {
                bytes.put_u8(b'K');
                put_pgoutput_prepare(&mut bytes, self.next_lsn, *xid, gid);
            }
            LogicalEvent::RollbackPrepared { xid, gid } => {
                bytes.put_u8(b'r');
                put_pgoutput_prepare(&mut bytes, self.next_lsn, *xid, gid);
            }
            LogicalEvent::Keepalive => return None,
        }

        Some(bytes.freeze())
    }
}

impl TruncateOpts {
    fn flags(self) -> u8 {
        u8::from(self.cascade) | (u8::from(self.restart_identity) << 1)
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
            Self::Begin { .. }
            | Self::Commit
            | Self::Truncate { .. }
            | Self::Origin { .. }
            | Self::StreamStart { .. }
            | Self::StreamStop
            | Self::StreamCommit { .. }
            | Self::StreamAbort { .. }
            | Self::BeginPrepare { .. }
            | Self::Prepare { .. }
            | Self::CommitPrepared { .. }
            | Self::RollbackPrepared { .. }
            | Self::Keepalive => None,
        }
    }

    #[must_use]
    pub fn relation_shape(&self) -> Option<(TableId, usize)> {
        match self {
            Self::Insert { table, new } | Self::Update { table, new, .. } => {
                Some((*table, new.len()))
            }
            Self::Delete { table, old } => Some((*table, old.len())),
            Self::RelationChange { table, new_columns } => Some((*table, new_columns.len())),
            Self::Begin { .. }
            | Self::Commit
            | Self::Truncate { .. }
            | Self::Origin { .. }
            | Self::StreamStart { .. }
            | Self::StreamStop
            | Self::StreamCommit { .. }
            | Self::StreamAbort { .. }
            | Self::BeginPrepare { .. }
            | Self::Prepare { .. }
            | Self::CommitPrepared { .. }
            | Self::RollbackPrepared { .. }
            | Self::Keepalive => None,
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
        put_string(bytes, value);
    }
}

fn put_string(bytes: &mut BytesMut, value: &str) {
    bytes.put_u16(u16::try_from(value.len()).unwrap_or(u16::MAX));
    bytes.put_slice(value.as_bytes());
}

fn put_cstr(bytes: &mut BytesMut, value: &str) {
    bytes.put_slice(value.as_bytes());
    bytes.put_u8(0);
}

fn put_pgoutput_tuple(bytes: &mut BytesMut, tuple: &Tuple) {
    bytes.put_u16(u16::try_from(tuple.len()).unwrap_or(u16::MAX));
    for value in tuple {
        bytes.put_u8(b't');
        bytes.put_u32(u32::try_from(value.len()).unwrap_or(u32::MAX));
        bytes.put_slice(value.as_bytes());
    }
}

fn put_pgoutput_prepare(bytes: &mut BytesMut, lsn: Lsn, xid: u32, gid: &str) {
    bytes.put_u8(0);
    bytes.put_u64(lsn.get());
    bytes.put_u64(lsn.get().saturating_add(1));
    bytes.put_u64(0);
    bytes.put_u32(xid);
    put_cstr(bytes, gid);
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};
    use proptest::{option, prelude::*};

    use super::{
        Catalog, ColumnDef, LogicalEvent, Lsn, TableId, TruncateOpts, Tuple, WalGenerator,
    };

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

    #[test]
    fn pgoutput_encoder_covers_supported_message_tags() {
        let table = TableId::new(7);
        let mut generator = WalGenerator::new();
        let frames = generator.encode_pgoutput(&[
            LogicalEvent::Begin { xid: 1 },
            LogicalEvent::Insert {
                table,
                new: vec!["1".to_owned()],
            },
            LogicalEvent::Update {
                table,
                old: Some(vec!["1".to_owned()]),
                new: vec!["2".to_owned()],
            },
            LogicalEvent::Delete {
                table,
                old: vec!["2".to_owned()],
            },
            LogicalEvent::Commit,
            LogicalEvent::Truncate {
                tables: vec![table],
                options: TruncateOpts {
                    cascade: true,
                    restart_identity: true,
                },
            },
            LogicalEvent::Origin {
                lsn: Lsn::new(99),
                name: "origin".to_owned(),
            },
            LogicalEvent::StreamStart {
                xid: 2,
                first_segment: true,
            },
            LogicalEvent::StreamStop,
            LogicalEvent::StreamCommit { xid: 2 },
            LogicalEvent::StreamAbort { xid: 2, subxid: 3 },
            LogicalEvent::BeginPrepare {
                xid: 4,
                gid: "gid-begin".to_owned(),
            },
            LogicalEvent::Prepare {
                xid: 5,
                gid: "gid-prepare".to_owned(),
            },
            LogicalEvent::CommitPrepared {
                xid: 6,
                gid: "gid-commit".to_owned(),
            },
            LogicalEvent::RollbackPrepared {
                xid: 7,
                gid: "gid-rollback".to_owned(),
            },
            LogicalEvent::Keepalive,
        ]);

        let tags = frames.iter().map(|frame| frame[0]).collect::<Vec<_>>();
        assert_eq!(
            tags,
            vec![
                b'B', b'R', b'I', b'U', b'D', b'C', b'T', b'O', b'S', b'E', b'c', b'A', b'b', b'P',
                b'K', b'r'
            ]
        );
        assert_eq!(generator.current_lsn().get(), 16);
    }

    proptest! {
        #[test]
        fn generated_frames_round_trip(events in logical_events()) {
            let mut generator = WalGenerator::new();
            let frames = generator.encode(&events);

            let decoded = decode_frames(&frames)?;

            prop_assert_eq!(decoded, events);
        }
    }

    fn logical_events() -> impl Strategy<Value = Vec<LogicalEvent>> {
        prop::collection::vec(logical_event(), 0..32)
    }

    fn logical_event() -> impl Strategy<Value = LogicalEvent> {
        prop_oneof![
            (0_u32..1_000_000).prop_map(|xid| LogicalEvent::Begin { xid }),
            (table_id(), tuple()).prop_map(|(table, new)| LogicalEvent::Insert { table, new }),
            (table_id(), option::of(tuple()), tuple())
                .prop_map(|(table, old, new)| LogicalEvent::Update { table, old, new }),
            (table_id(), tuple()).prop_map(|(table, old)| LogicalEvent::Delete { table, old }),
            Just(LogicalEvent::Commit),
            (table_id(), prop::collection::vec(column_def(), 0..8)).prop_map(
                |(table, new_columns)| LogicalEvent::RelationChange { table, new_columns }
            ),
            (
                prop::collection::vec(table_id(), 0..8),
                any::<bool>(),
                any::<bool>()
            )
                .prop_map(|(tables, cascade, restart_identity)| {
                    LogicalEvent::Truncate {
                        tables,
                        options: TruncateOpts {
                            cascade,
                            restart_identity,
                        },
                    }
                }),
            (0_u64..1_000_000, "[a-z][a-z0-9_]{0,15}").prop_map(|(lsn, name)| {
                LogicalEvent::Origin {
                    lsn: Lsn::new(lsn),
                    name,
                }
            }),
            (0_u32..1_000_000, any::<bool>())
                .prop_map(|(xid, first_segment)| LogicalEvent::StreamStart { xid, first_segment }),
            Just(LogicalEvent::StreamStop),
            (0_u32..1_000_000).prop_map(|xid| LogicalEvent::StreamCommit { xid }),
            (0_u32..1_000_000, 0_u32..1_000_000)
                .prop_map(|(xid, subxid)| LogicalEvent::StreamAbort { xid, subxid }),
            (0_u32..1_000_000, "[a-z][a-z0-9_]{0,15}")
                .prop_map(|(xid, gid)| LogicalEvent::BeginPrepare { xid, gid }),
            (0_u32..1_000_000, "[a-z][a-z0-9_]{0,15}")
                .prop_map(|(xid, gid)| LogicalEvent::Prepare { xid, gid }),
            (0_u32..1_000_000, "[a-z][a-z0-9_]{0,15}")
                .prop_map(|(xid, gid)| LogicalEvent::CommitPrepared { xid, gid }),
            (0_u32..1_000_000, "[a-z][a-z0-9_]{0,15}")
                .prop_map(|(xid, gid)| LogicalEvent::RollbackPrepared { xid, gid }),
            Just(LogicalEvent::Keepalive),
        ]
    }

    fn table_id() -> impl Strategy<Value = TableId> {
        (1_u32..256).prop_map(TableId::new)
    }

    fn tuple() -> impl Strategy<Value = Tuple> {
        prop::collection::vec("[a-z0-9_]{0,16}", 0..8)
    }

    fn column_def() -> impl Strategy<Value = ColumnDef> {
        ("[a-z][a-z0-9_]{0,15}", 1_u32..10_000, any::<bool>()).prop_map(
            |(name, type_oid, nullable)| ColumnDef {
                name,
                type_oid,
                nullable,
            },
        )
    }

    #[allow(clippy::too_many_lines)]
    fn decode_frames(frames: &[Bytes]) -> Result<Vec<LogicalEvent>, TestCaseError> {
        let mut events = Vec::new();

        for frame in frames {
            if frame.len() == 5 && frame[0] == b'R' {
                continue;
            }

            let mut bytes = frame.clone();
            ensure_remaining(&bytes, 9)?;
            let _lsn = bytes.get_u64();

            match bytes.get_u8() {
                b'B' => {
                    ensure_remaining(&bytes, 4)?;
                    events.push(LogicalEvent::Begin {
                        xid: bytes.get_u32(),
                    });
                }
                b'I' => {
                    ensure_remaining(&bytes, 4)?;
                    let table = TableId::new(bytes.get_u32());
                    let new = get_tuple(&mut bytes)?;
                    events.push(LogicalEvent::Insert { table, new });
                }
                b'U' => {
                    ensure_remaining(&bytes, 5)?;
                    let table = TableId::new(bytes.get_u32());
                    let old = get_optional_tuple(&mut bytes)?;
                    let new = get_tuple(&mut bytes)?;
                    events.push(LogicalEvent::Update { table, old, new });
                }
                b'D' => {
                    ensure_remaining(&bytes, 4)?;
                    let table = TableId::new(bytes.get_u32());
                    let old = get_tuple(&mut bytes)?;
                    events.push(LogicalEvent::Delete { table, old });
                }
                b'C' => events.push(LogicalEvent::Commit),
                b'S' => {
                    ensure_remaining(&bytes, 6)?;
                    let table = TableId::new(bytes.get_u32());
                    let column_count = bytes.get_u16();
                    let mut new_columns = Vec::with_capacity(usize::from(column_count));
                    for _ in 0..column_count {
                        new_columns.push(get_column_def(&mut bytes)?);
                    }
                    events.push(LogicalEvent::RelationChange { table, new_columns });
                }
                b'T' => {
                    ensure_remaining(&bytes, 5)?;
                    let table_count = bytes.get_u32();
                    let flags = bytes.get_u8();
                    let mut tables = Vec::with_capacity(usize::try_from(table_count).unwrap_or(0));
                    for _ in 0..table_count {
                        ensure_remaining(&bytes, 4)?;
                        tables.push(TableId::new(bytes.get_u32()));
                    }
                    events.push(LogicalEvent::Truncate {
                        tables,
                        options: TruncateOpts {
                            cascade: flags & 1 == 1,
                            restart_identity: flags & 2 == 2,
                        },
                    });
                }
                b'O' => {
                    ensure_remaining(&bytes, 8)?;
                    let lsn = Lsn::new(bytes.get_u64());
                    let name = get_string(&mut bytes)?;
                    events.push(LogicalEvent::Origin { lsn, name });
                }
                b'Y' => {
                    ensure_remaining(&bytes, 5)?;
                    let xid = bytes.get_u32();
                    let first_segment = match bytes.get_u8() {
                        0 => false,
                        1 => true,
                        tag => {
                            return Err(TestCaseError::fail(format!(
                                "unexpected stream-start flag {tag}"
                            )));
                        }
                    };
                    events.push(LogicalEvent::StreamStart { xid, first_segment });
                }
                b'E' => events.push(LogicalEvent::StreamStop),
                b'c' => {
                    ensure_remaining(&bytes, 4)?;
                    events.push(LogicalEvent::StreamCommit {
                        xid: bytes.get_u32(),
                    });
                }
                b'A' => {
                    ensure_remaining(&bytes, 8)?;
                    events.push(LogicalEvent::StreamAbort {
                        xid: bytes.get_u32(),
                        subxid: bytes.get_u32(),
                    });
                }
                b'b' => {
                    ensure_remaining(&bytes, 4)?;
                    let xid = bytes.get_u32();
                    let gid = get_string(&mut bytes)?;
                    events.push(LogicalEvent::BeginPrepare { xid, gid });
                }
                b'P' => {
                    ensure_remaining(&bytes, 4)?;
                    let xid = bytes.get_u32();
                    let gid = get_string(&mut bytes)?;
                    events.push(LogicalEvent::Prepare { xid, gid });
                }
                b'p' => {
                    ensure_remaining(&bytes, 4)?;
                    let xid = bytes.get_u32();
                    let gid = get_string(&mut bytes)?;
                    events.push(LogicalEvent::CommitPrepared { xid, gid });
                }
                b'r' => {
                    ensure_remaining(&bytes, 4)?;
                    let xid = bytes.get_u32();
                    let gid = get_string(&mut bytes)?;
                    events.push(LogicalEvent::RollbackPrepared { xid, gid });
                }
                b'K' => events.push(LogicalEvent::Keepalive),
                tag => {
                    return Err(TestCaseError::fail(format!("unexpected frame tag {tag}")));
                }
            }

            prop_assert!(
                !bytes.has_remaining(),
                "frame should not contain trailing bytes"
            );
        }

        Ok(events)
    }

    fn get_optional_tuple(bytes: &mut Bytes) -> Result<Option<Tuple>, TestCaseError> {
        ensure_remaining(bytes, 1)?;
        match bytes.get_u8() {
            0 => Ok(None),
            1 => get_tuple(bytes).map(Some),
            tag => Err(TestCaseError::fail(format!(
                "unexpected optional tuple tag {tag}"
            ))),
        }
    }

    fn get_tuple(bytes: &mut Bytes) -> Result<Tuple, TestCaseError> {
        ensure_remaining(bytes, 2)?;
        let value_count = bytes.get_u16();
        let mut tuple = Vec::with_capacity(usize::from(value_count));
        for _ in 0..value_count {
            tuple.push(get_string(bytes)?);
        }
        Ok(tuple)
    }

    fn get_column_def(bytes: &mut Bytes) -> Result<ColumnDef, TestCaseError> {
        let name = get_string(bytes)?;
        ensure_remaining(bytes, 5)?;
        let type_oid = bytes.get_u32();
        let nullable = match bytes.get_u8() {
            0 => false,
            1 => true,
            tag => {
                return Err(TestCaseError::fail(format!(
                    "unexpected nullable tag {tag}"
                )));
            }
        };

        Ok(ColumnDef {
            name,
            type_oid,
            nullable,
        })
    }

    fn get_string(bytes: &mut Bytes) -> Result<String, TestCaseError> {
        ensure_remaining(bytes, 2)?;
        let len = usize::from(bytes.get_u16());
        ensure_remaining(bytes, len)?;
        let value = bytes.copy_to_bytes(len);
        String::from_utf8(value.to_vec())
            .map_err(|err| TestCaseError::fail(format!("invalid utf8: {err}")))
    }

    fn ensure_remaining(bytes: &Bytes, count: usize) -> Result<(), TestCaseError> {
        prop_assert!(
            bytes.remaining() >= count,
            "frame ended early: need {count} bytes, have {}",
            bytes.remaining()
        );
        Ok(())
    }
}
