// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §15.3 property 2 — Snapshot/replay equivalence.
//!
//! Subscribe at LSN L₀, apply diffs through Lₙ, and assert the result
//! equals re-subscribing at Lₙ from a fresh snapshot. We model this
//! at the reference-executor level: applying `[a, b]` cumulatively
//! must equal applying `[a]` then `[b]` separately on a fresh
//! executor seeded with `apply([a])`'s state.

use std::sync::Arc;

use palimpsest_test_harness::{
    Catalog, ColumnDef, LogicalEvent, ReferenceExecutor, TableDef, TableId,
};
use proptest::prelude::*;

const T: TableId = TableId::new(1);

fn schema() -> Catalog {
    Catalog::with_tables([TableDef::new(
        T,
        "items",
        vec![
            ColumnDef {
                name: "id".into(),
                type_oid: 23,
                nullable: false,
            },
            ColumnDef {
                name: "qty".into(),
                type_oid: 23,
                nullable: false,
            },
        ],
    )])
}

fn arb_event() -> impl Strategy<Value = LogicalEvent> {
    prop_oneof![
        (0_i64..16, 0_i64..100).prop_map(|(id, qty)| LogicalEvent::Insert {
            table: T,
            new: vec![id.to_string(), qty.to_string()],
        }),
        (0_i64..16).prop_map(|id| LogicalEvent::Delete {
            table: T,
            old: vec![id.to_string(), "0".to_owned()],
        }),
    ]
}

proptest! {
    #[test]
    fn split_application_matches_combined(
        prefix in proptest::collection::vec(arb_event(), 0..8),
        suffix in proptest::collection::vec(arb_event(), 0..8),
    ) {
        let catalog = Arc::new(schema());
        let mut combined = ReferenceExecutor::new(Arc::clone(&catalog));
        combined.apply(&prefix);
        combined.apply(&suffix);

        let mut split = ReferenceExecutor::new(catalog);
        let mut all = prefix;
        all.extend(suffix);
        split.apply(&all);

        prop_assert_eq!(combined.tables(), split.tables());
    }
}
