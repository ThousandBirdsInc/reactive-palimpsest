// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use bytes::Bytes;
use palimpsest_test_harness::wal::{LogicalEvent, TableId, WalGenerator};
use palimpsest_wal::{decode_pgoutput_message, Catalog, Datum, DecodedEvent, RowOp, Tuple};

#[test]
fn wal_generator_pgoutput_frames_decode_through_production_decoder() {
    let table = TableId::new(42);
    let mut generator = WalGenerator::new();
    let frames = generator.encode_pgoutput(&[
        LogicalEvent::Begin { xid: 7 },
        LogicalEvent::Insert {
            table,
            new: vec!["1".to_owned(), "hello".to_owned()],
        },
        LogicalEvent::Update {
            table,
            old: Some(vec!["1".to_owned(), "hello".to_owned()]),
            new: vec!["1".to_owned(), "goodbye".to_owned()],
        },
        LogicalEvent::Delete {
            table,
            old: vec!["1".to_owned(), "goodbye".to_owned()],
        },
        LogicalEvent::Commit,
    ]);

    let mut catalog = Catalog::new();
    let decoded = frames
        .into_iter()
        .map(|frame| decode_pgoutput_message(&mut catalog, frame))
        .collect::<palimpsest_wal::Result<Vec<_>>>()
        .unwrap();

    assert!(matches!(decoded[0], DecodedEvent::Begin { xid: 7, .. }));
    assert!(matches!(decoded[1], DecodedEvent::Schema { .. }));
    assert_eq!(
        decoded[2],
        DecodedEvent::Row {
            table: palimpsest_wal::TableId::new(table.get()),
            op: RowOp::Insert,
            old: None,
            new: Some(tuple(["1", "hello"])),
        }
    );
    assert_eq!(
        decoded[3],
        DecodedEvent::Row {
            table: palimpsest_wal::TableId::new(table.get()),
            op: RowOp::Update,
            old: Some(tuple(["1", "hello"])),
            new: Some(tuple(["1", "goodbye"])),
        }
    );
    assert_eq!(
        decoded[4],
        DecodedEvent::Row {
            table: palimpsest_wal::TableId::new(table.get()),
            op: RowOp::Delete,
            old: Some(tuple(["1", "goodbye"])),
            new: None,
        }
    );
    assert!(matches!(decoded[5], DecodedEvent::Commit { .. }));
}

fn tuple(values: [&'static str; 2]) -> Tuple {
    Tuple::from_vec(
        values
            .into_iter()
            .map(|value| Datum::Text(Bytes::from_static(value.as_bytes())))
            .collect(),
    )
}
