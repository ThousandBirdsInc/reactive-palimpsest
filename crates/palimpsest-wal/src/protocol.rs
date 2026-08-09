// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `pgoutput` logical-replication protocol decoding.
//!
//! Wire-shape types (Begin, Commit, Insert, Update, Delete, Truncate,
//! Origin, etc.) are direct mirrors of the upstream protocol; the
//! variants and fields are documented in the Postgres logical-replication
//! reference rather than re-stated here.

#![allow(missing_docs)]

use bytes::{Buf, Bytes};

use crate::{
    catalog::{column_from_pgoutput, ReplicaIdentity},
    decode_column_value, Catalog, ColumnValue, Lsn, RelationSchema, Result, TableId, Tuple,
    WalError,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedEvent {
    Begin {
        xid: u32,
        commit_lsn: Lsn,
    },
    Row {
        table: TableId,
        op: RowOp,
        old: Option<Tuple>,
        new: Option<Tuple>,
    },
    Commit {
        commit_lsn: Lsn,
        end_lsn: Lsn,
    },
    Schema {
        table: TableId,
        columns: Vec<crate::ColumnDef>,
    },
    Heartbeat {
        lsn: Lsn,
    },
    Reconnect {
        attempt: u32,
    },
    Resync {
        snapshot_lsn: Lsn,
    },
    Truncate(Truncate),
    Origin(Origin),
    /// A user-defined type announced ahead of the relations that use
    /// it. Informational: values still arrive in text form.
    Type(TypeInfo),
    Stream(StreamAction),
    TwoPhase(TwoPhaseAction),
}

/// A `Type` message from the replication stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeInfo {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOp {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    pub tables: Vec<TableId>,
    pub cascade: bool,
    pub restart_identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    pub lsn: Lsn,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamAction {
    Start {
        xid: u32,
        first_segment: bool,
    },
    Stop,
    Commit {
        xid: u32,
        commit_lsn: Lsn,
        end_lsn: Lsn,
    },
    Abort {
        xid: u32,
        subxid: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TwoPhaseAction {
    BeginPrepare { xid: u32, gid: String },
    Prepare { xid: u32, gid: String },
    CommitPrepared { xid: u32, gid: String },
    RollbackPrepared { xid: u32, gid: String },
}

pub fn decode_pgoutput_message(catalog: &mut Catalog, bytes: Bytes) -> Result<DecodedEvent> {
    let mut decoder = Decoder::new(bytes);
    let tag = decoder.u8("message tag")?;
    let event = match tag {
        b'B' => DecodedEvent::Begin {
            commit_lsn: Lsn::new(decoder.u64("begin final lsn")?),
            xid: {
                let _commit_ts = decoder.u64("begin commit timestamp")?;
                decoder.u32("begin xid")?
            },
        },
        b'C' => {
            let _flags = decoder.u8("commit flags")?;
            let commit_lsn = Lsn::new(decoder.u64("commit lsn")?);
            let end_lsn = Lsn::new(decoder.u64("commit end lsn")?);
            let _timestamp = decoder.u64("commit timestamp")?;
            DecodedEvent::Commit {
                commit_lsn,
                end_lsn,
            }
        }
        b'R' => decode_relation(catalog, &mut decoder)?,
        // Type: emitted before a Relation whose columns use a
        // user-defined type (enum, domain, composite, or an array of
        // one). Purely informational for us — values arrive in their
        // text form and the catalog carries the real types — but it
        // must be decoded, not rejected, or a single enum column makes
        // the whole stream undecodable.
        b'Y' => DecodedEvent::Type(TypeInfo {
            oid: decoder.u32("type oid")?,
            namespace: decoder.cstr("type namespace")?,
            name: decoder.cstr("type name")?,
        }),
        b'I' => decode_insert(catalog, &mut decoder)?,
        b'U' => decode_update(catalog, &mut decoder)?,
        b'D' => decode_delete(catalog, &mut decoder)?,
        b'T' => decode_truncate(&mut decoder)?,
        b'O' => DecodedEvent::Origin(Origin {
            lsn: Lsn::new(decoder.u64("origin lsn")?),
            name: decoder.cstr("origin name")?,
        }),
        b'S' => DecodedEvent::Stream(StreamAction::Start {
            xid: decoder.u32("stream xid")?,
            first_segment: decoder.u8("stream flags")? & 1 == 1,
        }),
        b'E' => DecodedEvent::Stream(StreamAction::Stop),
        b'c' => {
            let xid = decoder.u32("stream commit xid")?;
            let _flags = decoder.u8("stream commit flags")?;
            let commit_lsn = Lsn::new(decoder.u64("stream commit lsn")?);
            let end_lsn = Lsn::new(decoder.u64("stream commit end lsn")?);
            let _timestamp = decoder.u64("stream commit timestamp")?;
            DecodedEvent::Stream(StreamAction::Commit {
                xid,
                commit_lsn,
                end_lsn,
            })
        }
        b'A' => DecodedEvent::Stream(StreamAction::Abort {
            xid: decoder.u32("stream abort xid")?,
            subxid: decoder.u32("stream abort subxid")?,
        }),
        b'b' => DecodedEvent::TwoPhase(TwoPhaseAction::BeginPrepare {
            xid: decode_prepare_prefix(&mut decoder)?,
            gid: decoder.cstr("begin prepare gid")?,
        }),
        b'P' => DecodedEvent::TwoPhase(TwoPhaseAction::Prepare {
            xid: decode_prepare_prefix(&mut decoder)?,
            gid: decoder.cstr("prepare gid")?,
        }),
        b'K' => DecodedEvent::TwoPhase(TwoPhaseAction::CommitPrepared {
            xid: decode_prepare_prefix(&mut decoder)?,
            gid: decoder.cstr("commit prepared gid")?,
        }),
        b'r' => DecodedEvent::TwoPhase(TwoPhaseAction::RollbackPrepared {
            xid: decode_prepare_prefix(&mut decoder)?,
            gid: decoder.cstr("rollback prepared gid")?,
        }),
        tag => return Err(WalError::UnsupportedMessage(tag)),
    };
    decoder.finish()?;
    Ok(event)
}

fn decode_relation(catalog: &mut Catalog, decoder: &mut Decoder) -> Result<DecodedEvent> {
    let table = TableId::new(decoder.u32("relation id")?);
    let namespace = decoder.cstr("relation namespace")?;
    let name = decoder.cstr("relation name")?;
    let replica_identity = ReplicaIdentity::try_from(decoder.u8("relation replica identity")?)?;
    let column_count = decoder.u16("relation column count")?;
    let mut columns = Vec::with_capacity(usize::from(column_count));
    for _ in 0..column_count {
        let flags = decoder.u8("relation column flags")?;
        let name = decoder.cstr("relation column name")?;
        let type_oid = decoder.u32("relation column type oid")?;
        let _type_mod = decoder.u32("relation column type modifier")?;
        columns.push(column_from_pgoutput(flags, name, type_oid)?);
    }

    catalog.upsert_relation(RelationSchema::new(
        table,
        namespace,
        name,
        replica_identity,
        columns.clone(),
    ));

    Ok(DecodedEvent::Schema { table, columns })
}

fn decode_insert(catalog: &Catalog, decoder: &mut Decoder) -> Result<DecodedEvent> {
    let table = TableId::new(decoder.u32("insert relation id")?);
    decoder.expect_tag(b'N', "insert new tuple")?;
    let new = decode_tuple(catalog, table, decoder)?;
    Ok(DecodedEvent::Row {
        table,
        op: RowOp::Insert,
        old: None,
        new: Some(new),
    })
}

fn decode_update(catalog: &Catalog, decoder: &mut Decoder) -> Result<DecodedEvent> {
    let table = TableId::new(decoder.u32("update relation id")?);
    let marker = decoder.u8("update tuple marker")?;
    let old = match marker {
        b'K' | b'O' => {
            let old = decode_tuple(catalog, table, decoder)?;
            decoder.expect_tag(b'N', "update new tuple")?;
            Some(old)
        }
        b'N' => None,
        _ => return Err(WalError::Malformed("unexpected update tuple marker")),
    };
    let new = decode_tuple(catalog, table, decoder)?;
    Ok(DecodedEvent::Row {
        table,
        op: RowOp::Update,
        old,
        new: Some(new),
    })
}

fn decode_delete(catalog: &Catalog, decoder: &mut Decoder) -> Result<DecodedEvent> {
    let table = TableId::new(decoder.u32("delete relation id")?);
    match decoder.u8("delete tuple marker")? {
        b'K' | b'O' => {
            let old = decode_tuple(catalog, table, decoder)?;
            Ok(DecodedEvent::Row {
                table,
                op: RowOp::Delete,
                old: Some(old),
                new: None,
            })
        }
        _ => Err(WalError::Malformed("unexpected delete tuple marker")),
    }
}

fn decode_truncate(decoder: &mut Decoder) -> Result<DecodedEvent> {
    let relation_count = decoder.u32("truncate relation count")?;
    let options = decoder.u8("truncate options")?;
    let mut tables = Vec::with_capacity(usize::try_from(relation_count).unwrap_or(usize::MAX));
    for _ in 0..relation_count {
        tables.push(TableId::new(decoder.u32("truncate relation id")?));
    }
    Ok(DecodedEvent::Truncate(Truncate {
        tables,
        cascade: options & 1 == 1,
        restart_identity: options & 2 == 2,
    }))
}

fn decode_tuple(catalog: &Catalog, table: TableId, decoder: &mut Decoder) -> Result<Tuple> {
    let relation = catalog
        .relation(table)
        .ok_or(WalError::UnknownRelation(table))?;
    let value_count = usize::from(decoder.u16("tuple column count")?);
    if relation.columns.len() != value_count {
        return Err(WalError::TupleArity {
            table,
            expected: relation.columns.len(),
            actual: value_count,
        });
    }

    let mut tuple = Tuple::with_capacity(value_count);
    for column in &relation.columns {
        let value = match decoder.u8("tuple column kind")? {
            b'n' => ColumnValue::Null,
            b'u' => ColumnValue::Unchanged,
            b't' => ColumnValue::Text(decoder.bytes_i32("tuple text column")?),
            b'b' => ColumnValue::Binary(decoder.bytes_i32("tuple binary column")?),
            _ => return Err(WalError::Malformed("unknown tuple column kind")),
        };
        tuple.push(decode_column_value(&column.datum_type, value)?);
    }
    Ok(tuple)
}

fn decode_prepare_prefix(decoder: &mut Decoder) -> Result<u32> {
    let _flags = decoder.u8("two-phase flags")?;
    let _prepare_lsn = decoder.u64("two-phase prepare lsn")?;
    let _end_lsn = decoder.u64("two-phase end lsn")?;
    let _timestamp = decoder.u64("two-phase timestamp")?;
    decoder.u32("two-phase xid")
}

struct Decoder {
    bytes: Bytes,
}

impl Decoder {
    const fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    fn finish(&self) -> Result<()> {
        if self.bytes.has_remaining() {
            return Err(WalError::Malformed("trailing bytes"));
        }
        Ok(())
    }

    fn expect_tag(&mut self, expected: u8, context: &'static str) -> Result<()> {
        let actual = self.u8(context)?;
        if actual == expected {
            Ok(())
        } else {
            Err(WalError::Malformed(context))
        }
    }

    fn u8(&mut self, context: &'static str) -> Result<u8> {
        if self.bytes.remaining() < 1 {
            return Err(WalError::UnexpectedEof(context));
        }
        Ok(self.bytes.get_u8())
    }

    fn u16(&mut self, context: &'static str) -> Result<u16> {
        if self.bytes.remaining() < 2 {
            return Err(WalError::UnexpectedEof(context));
        }
        Ok(self.bytes.get_u16())
    }

    fn u32(&mut self, context: &'static str) -> Result<u32> {
        if self.bytes.remaining() < 4 {
            return Err(WalError::UnexpectedEof(context));
        }
        Ok(self.bytes.get_u32())
    }

    fn u64(&mut self, context: &'static str) -> Result<u64> {
        if self.bytes.remaining() < 8 {
            return Err(WalError::UnexpectedEof(context));
        }
        Ok(self.bytes.get_u64())
    }

    fn bytes_i32(&mut self, context: &'static str) -> Result<Bytes> {
        let len = usize::try_from(self.u32(context)?)
            .map_err(|_| WalError::Malformed("negative tuple value length"))?;
        if self.bytes.remaining() < len {
            return Err(WalError::UnexpectedEof(context));
        }
        Ok(self.bytes.split_to(len))
    }

    fn cstr(&mut self, context: &'static str) -> Result<String> {
        let Some(position) = self.bytes.iter().position(|byte| *byte == 0) else {
            return Err(WalError::UnexpectedEof(context));
        };
        let value = self.bytes.split_to(position);
        self.bytes.advance(1);
        Ok(std::str::from_utf8(&value)?.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use super::{
        decode_pgoutput_message, DecodedEvent, Origin, RowOp, StreamAction, Truncate,
        TwoPhaseAction, TypeInfo,
    };
    use crate::{Catalog, Datum, Lsn, TableId, Tuple, WalError, BOOL_OID, INT4_OID, TEXT_OID};

    #[test]
    fn decodes_type_message_for_user_defined_types() {
        // Postgres emits a Type message before the Relation whenever a
        // column uses a user-defined type (an enum, say). Rejecting it
        // made a single enum column undecodable for the whole stream.
        let mut catalog = Catalog::new();
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'Y');
        bytes.put_u32(543_210);
        put_cstr(&mut bytes, "public");
        put_cstr(&mut bytes, "ticket_status");

        let event = decode_pgoutput_message(&mut catalog, bytes.freeze()).expect("decodes");
        assert_eq!(
            event,
            DecodedEvent::Type(TypeInfo {
                oid: 543_210,
                namespace: "public".to_owned(),
                name: "ticket_status".to_owned(),
            })
        );
    }

    #[test]
    fn decodes_relation_and_insert_tuple() {
        let mut catalog = Catalog::new();
        let table = TableId::new(42);
        let schema = relation(
            table,
            &[("id", INT4_OID), ("body", TEXT_OID), ("live", BOOL_OID)],
        );

        assert!(matches!(
            decode_pgoutput_message(&mut catalog, schema).unwrap(),
            DecodedEvent::Schema { table: decoded, .. } if decoded == table
        ));

        let row = insert(
            table,
            &[field_text("7"), field_text("hello"), field_text("t")],
        );
        let event = decode_pgoutput_message(&mut catalog, row).unwrap();
        assert_eq!(
            event,
            DecodedEvent::Row {
                table,
                op: RowOp::Insert,
                old: None,
                new: Some(Tuple::from_vec(vec![
                    Datum::I32(7),
                    Datum::Text(Bytes::from_static(b"hello")),
                    Datum::Bool(true),
                ])),
            }
        );
    }

    #[test]
    fn surfaces_toast_unchanged_marker() {
        let mut catalog = Catalog::new();
        let table = TableId::new(7);
        decode_pgoutput_message(&mut catalog, relation(table, &[("body", TEXT_OID)])).unwrap();

        let event =
            decode_pgoutput_message(&mut catalog, update(table, None, &[field_unchanged()]))
                .unwrap();
        assert_eq!(
            event,
            DecodedEvent::Row {
                table,
                op: RowOp::Update,
                old: None,
                new: Some(Tuple::from_vec(vec![Datum::Unchanged])),
            }
        );
    }

    #[test]
    fn decodes_commit_and_stream_commit() {
        let mut catalog = Catalog::new();
        assert_eq!(
            decode_pgoutput_message(&mut catalog, commit(10, 11)).unwrap(),
            DecodedEvent::Commit {
                commit_lsn: Lsn::new(10),
                end_lsn: Lsn::new(11),
            }
        );

        let mut bytes = BytesMut::new();
        bytes.put_u8(b'c');
        bytes.put_u32(9);
        bytes.put_u8(0);
        bytes.put_u64(20);
        bytes.put_u64(21);
        bytes.put_u64(0);
        assert_eq!(
            decode_pgoutput_message(&mut catalog, bytes.freeze()).unwrap(),
            DecodedEvent::Stream(StreamAction::Commit {
                xid: 9,
                commit_lsn: Lsn::new(20),
                end_lsn: Lsn::new(21),
            })
        );
    }

    #[test]
    fn decodes_control_and_rare_message_variants() {
        let mut catalog = Catalog::new();

        assert_eq!(
            decode_pgoutput_message(&mut catalog, begin(55, 99)).unwrap(),
            DecodedEvent::Begin {
                xid: 55,
                commit_lsn: Lsn::new(99),
            }
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, truncate(&[TableId::new(1), TableId::new(2)]))
                .unwrap(),
            DecodedEvent::Truncate(Truncate {
                tables: vec![TableId::new(1), TableId::new(2)],
                cascade: true,
                restart_identity: true,
            })
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, origin()).unwrap(),
            DecodedEvent::Origin(Origin {
                lsn: Lsn::new(88),
                name: "origin-a".to_owned(),
            })
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, stream_start()).unwrap(),
            DecodedEvent::Stream(StreamAction::Start {
                xid: 7,
                first_segment: true,
            })
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, Bytes::from_static(b"E")).unwrap(),
            DecodedEvent::Stream(StreamAction::Stop)
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, stream_abort()).unwrap(),
            DecodedEvent::Stream(StreamAction::Abort { xid: 7, subxid: 8 })
        );
    }

    #[test]
    fn decodes_two_phase_commit_variants() {
        let mut catalog = Catalog::new();

        assert_eq!(
            decode_pgoutput_message(&mut catalog, two_phase(b'b', 11, "gid-a")).unwrap(),
            DecodedEvent::TwoPhase(TwoPhaseAction::BeginPrepare {
                xid: 11,
                gid: "gid-a".to_owned(),
            })
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, two_phase(b'P', 12, "gid-b")).unwrap(),
            DecodedEvent::TwoPhase(TwoPhaseAction::Prepare {
                xid: 12,
                gid: "gid-b".to_owned(),
            })
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, two_phase(b'K', 13, "gid-c")).unwrap(),
            DecodedEvent::TwoPhase(TwoPhaseAction::CommitPrepared {
                xid: 13,
                gid: "gid-c".to_owned(),
            })
        );
        assert_eq!(
            decode_pgoutput_message(&mut catalog, two_phase(b'r', 14, "gid-d")).unwrap(),
            DecodedEvent::TwoPhase(TwoPhaseAction::RollbackPrepared {
                xid: 14,
                gid: "gid-d".to_owned(),
            })
        );
    }

    #[test]
    fn explicitly_rejects_logical_message_variant() {
        // `M` (logical decoding message) carries no row data and has
        // no meaning for the engine, so it stays rejected. `Y` (Type)
        // used to be rejected too, but Postgres emits one before any
        // Relation using a user-defined type — see
        // `decodes_type_message_for_user_defined_types`.
        let mut catalog = Catalog::new();
        assert!(matches!(
            decode_pgoutput_message(&mut catalog, Bytes::from_static(b"M")),
            Err(WalError::UnsupportedMessage(b'M'))
        ));
    }

    #[test]
    fn truncated_type_message_is_rejected_not_silently_accepted() {
        let mut catalog = Catalog::new();
        assert!(decode_pgoutput_message(&mut catalog, Bytes::from_static(b"Y")).is_err());
    }

    fn relation(table: TableId, columns: &[(&str, u32)]) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'R');
        bytes.put_u32(table.get());
        put_cstr(&mut bytes, "public");
        put_cstr(&mut bytes, "posts");
        bytes.put_u8(b'd');
        bytes.put_u16(u16::try_from(columns.len()).unwrap());
        for (index, (name, oid)) in columns.iter().enumerate() {
            bytes.put_u8(u8::from(index == 0));
            put_cstr(&mut bytes, name);
            bytes.put_u32(*oid);
            bytes.put_u32(u32::MAX);
        }
        bytes.freeze()
    }

    fn begin(xid: u32, lsn: u64) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'B');
        bytes.put_u64(lsn);
        bytes.put_u64(0);
        bytes.put_u32(xid);
        bytes.freeze()
    }

    fn insert(table: TableId, fields: &[Field]) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'I');
        bytes.put_u32(table.get());
        bytes.put_u8(b'N');
        tuple(&mut bytes, fields);
        bytes.freeze()
    }

    fn update(table: TableId, old: Option<&[Field]>, new: &[Field]) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'U');
        bytes.put_u32(table.get());
        if let Some(old) = old {
            bytes.put_u8(b'O');
            tuple(&mut bytes, old);
        }
        bytes.put_u8(b'N');
        tuple(&mut bytes, new);
        bytes.freeze()
    }

    fn commit(commit_lsn: u64, end_lsn: u64) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'C');
        bytes.put_u8(0);
        bytes.put_u64(commit_lsn);
        bytes.put_u64(end_lsn);
        bytes.put_u64(0);
        bytes.freeze()
    }

    fn truncate(tables: &[TableId]) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'T');
        bytes.put_u32(u32::try_from(tables.len()).unwrap());
        bytes.put_u8(3);
        for table in tables {
            bytes.put_u32(table.get());
        }
        bytes.freeze()
    }

    fn origin() -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'O');
        bytes.put_u64(88);
        put_cstr(&mut bytes, "origin-a");
        bytes.freeze()
    }

    fn stream_start() -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'S');
        bytes.put_u32(7);
        bytes.put_u8(1);
        bytes.freeze()
    }

    fn stream_abort() -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'A');
        bytes.put_u32(7);
        bytes.put_u32(8);
        bytes.freeze()
    }

    fn two_phase(tag: u8, xid: u32, gid: &str) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(tag);
        bytes.put_u8(0);
        bytes.put_u64(1);
        bytes.put_u64(2);
        bytes.put_u64(3);
        bytes.put_u32(xid);
        put_cstr(&mut bytes, gid);
        bytes.freeze()
    }

    fn tuple(bytes: &mut BytesMut, fields: &[Field]) {
        bytes.put_u16(u16::try_from(fields.len()).unwrap());
        for field in fields {
            match field {
                Field::Text(value) => {
                    bytes.put_u8(b't');
                    bytes.put_u32(u32::try_from(value.len()).unwrap());
                    bytes.put_slice(value.as_bytes());
                }
                Field::Unchanged => bytes.put_u8(b'u'),
            }
        }
    }

    fn put_cstr(bytes: &mut BytesMut, value: &str) {
        bytes.put_slice(value.as_bytes());
        bytes.put_u8(0);
    }

    fn field_text(value: &'static str) -> Field {
        Field::Text(value)
    }

    const fn field_unchanged() -> Field {
        Field::Unchanged
    }

    enum Field {
        Text(&'static str),
        Unchanged,
    }
}
