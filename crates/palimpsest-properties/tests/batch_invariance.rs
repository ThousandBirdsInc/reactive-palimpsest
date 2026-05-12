// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(clippy::cast_possible_truncation)]

//! §15.3 property 4 — Batch invariance.
//!
//! A WAL trace replayed in any partition consistent with commit order
//! produces the same final state. We assert this on the reference
//! executor: shuffling rows *within* a transaction (between
//! `Begin`/`Commit`) must not change the final state, since per-row
//! commutativity holds for inserts/deletes against distinct PKs.

use std::sync::Arc;

use palimpsest_test_harness::{
    Catalog, ColumnDef, LogicalEvent, ReferenceExecutor, TableDef, TableId,
};
use proptest::prelude::*;

const T: TableId = TableId::new(1);

fn schema() -> Catalog {
    Catalog::with_tables([TableDef::new(
        T,
        "kv",
        vec![
            ColumnDef {
                name: "k".into(),
                type_oid: 25,
                nullable: false,
            },
            ColumnDef {
                name: "v".into(),
                type_oid: 23,
                nullable: false,
            },
        ],
    )])
}

fn arb_distinct_inserts() -> impl Strategy<Value = Vec<LogicalEvent>> {
    proptest::collection::hash_set(0_i64..64, 0..16).prop_map(|keys| {
        keys.into_iter()
            .enumerate()
            .map(|(idx, k)| LogicalEvent::Insert {
                table: T,
                new: vec![k.to_string(), idx.to_string()],
            })
            .collect()
    })
}

proptest! {
    #[test]
    fn within_txn_partition_order_is_invariant(
        inserts in arb_distinct_inserts(),
        seed in any::<u64>(),
    ) {
        let mut shuffled = inserts.clone();
        let mut state = seed;
        // Fisher-Yates with a reproducible LCG so shrinking is stable.
        for i in (1..shuffled.len()).rev() {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            let j = (state as usize) % (i + 1);
            shuffled.swap(i, j);
        }

        let mut original_events: Vec<LogicalEvent> = vec![LogicalEvent::Begin { xid: 1 }];
        original_events.extend(inserts);
        original_events.push(LogicalEvent::Commit);
        let mut shuffled_events: Vec<LogicalEvent> = vec![LogicalEvent::Begin { xid: 1 }];
        shuffled_events.extend(shuffled);
        shuffled_events.push(LogicalEvent::Commit);

        let catalog = Arc::new(schema());
        let mut a = ReferenceExecutor::new(Arc::clone(&catalog));
        let mut b = ReferenceExecutor::new(catalog);
        a.apply(&original_events);
        b.apply(&shuffled_events);
        prop_assert_eq!(a.tables(), b.tables());
    }
}
