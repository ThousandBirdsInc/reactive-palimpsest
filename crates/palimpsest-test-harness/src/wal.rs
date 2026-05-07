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
                for column in new_columns {
                    put_string(&mut bytes, &column.name);
                    bytes.put_u32(column.type_oid);
                    bytes.put_u8(u8::from(column.nullable));
                }
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
        put_string(bytes, value);
    }
}

fn put_string(bytes: &mut BytesMut, value: &str) {
    bytes.put_u16(u16::try_from(value.len()).unwrap_or(u16::MAX));
    bytes.put_slice(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};
    use proptest::{option, prelude::*};

    use super::{Catalog, ColumnDef, LogicalEvent, TableId, Tuple, WalGenerator};

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
