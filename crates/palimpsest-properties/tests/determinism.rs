// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §15.3 property 3 — Determinism.
//!
//! Two runs of the same WAL trace must produce byte-identical wire
//! output. We approximate this with reference-executor + WAL encoder:
//! re-encoding the same `LogicalEvent` slice through two independent
//! `WalGenerator`s must yield identical pgoutput frames, and applying
//! the events through two independent `ReferenceExecutor`s must yield
//! identical state.

use std::sync::Arc;

use palimpsest_test_harness::{
    Catalog, ColumnDef, LogicalEvent, ReferenceExecutor, TableDef, TableId, WalGenerator,
};
use proptest::prelude::*;

const T: TableId = TableId::new(1);

fn schema() -> Catalog {
    Catalog::with_tables([TableDef::new(
        T,
        "users",
        vec![
            ColumnDef {
                name: "id".into(),
                type_oid: 23,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                type_oid: 25,
                nullable: true,
            },
        ],
    )])
}

fn arb_event() -> impl Strategy<Value = LogicalEvent> {
    (0_i64..32, "[a-z]{1,8}").prop_map(|(id, name)| LogicalEvent::Insert {
        table: T,
        new: vec![id.to_string(), name],
    })
}

proptest! {
    #[test]
    fn wal_encode_is_deterministic(events in proptest::collection::vec(arb_event(), 0..16)) {
        let mut a = WalGenerator::with_catalog(schema());
        let mut b = WalGenerator::with_catalog(schema());
        let frames_a = a.encode_pgoutput(&events);
        let frames_b = b.encode_pgoutput(&events);
        prop_assert_eq!(frames_a, frames_b);
    }

    #[test]
    fn reference_apply_is_deterministic(events in proptest::collection::vec(arb_event(), 0..16)) {
        let catalog = Arc::new(schema());
        let mut a = ReferenceExecutor::new(Arc::clone(&catalog));
        let mut b = ReferenceExecutor::new(catalog);
        a.apply(&events);
        b.apply(&events);
        prop_assert_eq!(a.tables(), b.tables());
    }
}
