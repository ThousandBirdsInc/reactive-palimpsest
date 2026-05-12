// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §15.3 property 1 — Oracle equivalence.
//!
//! For an arbitrary `(query, wal_trace)`, the materialized result of
//! the live subscription must equal the reference executor's result
//! of `query`. The harness's `ReferenceExecutor` is the oracle; the
//! "engine side" here is the harness's own `apply` + `execute` path,
//! which is the closest analogue we have until an end-to-end plan
//! evaluator lands. Operator-level oracle parity is asserted in
//! `palimpsest-dataflow/tests/operator_oracle.rs`; this property is
//! the SQL → MIR → result side.

use std::sync::Arc;

use palimpsest_sql::lower::parse_and_lower;
use palimpsest_test_harness::{
    Catalog as HarnessCatalog, ColumnDef as HarnessColumn, LogicalEvent, ReferenceExecutor,
    TableDef, TableId,
};
use proptest::prelude::*;

const POSTS: TableId = TableId::new(1);

fn schema() -> HarnessCatalog {
    HarnessCatalog::with_tables([TableDef::new(
        POSTS,
        "posts",
        vec![
            HarnessColumn {
                name: "id".into(),
                type_oid: 23,
                nullable: false,
            },
            HarnessColumn {
                name: "author_id".into(),
                type_oid: 23,
                nullable: false,
            },
            HarnessColumn {
                name: "score".into(),
                type_oid: 23,
                nullable: false,
            },
        ],
    )])
}

fn arb_trace() -> impl Strategy<Value = Vec<LogicalEvent>> {
    proptest::collection::vec(
        (0_i64..32, 0_i64..8, 0_i64..200).prop_map(|(id, author, score)| LogicalEvent::Insert {
            table: POSTS,
            new: vec![id.to_string(), author.to_string(), score.to_string()],
        }),
        0..16,
    )
}

proptest! {
    #[test]
    fn project_id_matches_oracle(trace in arb_trace()) {
        let catalog = Arc::new(schema());
        let mut reference = ReferenceExecutor::new(Arc::clone(&catalog));
        reference.apply(&trace);

        let graph = parse_and_lower("SELECT id FROM posts")
            .expect("lower must succeed for a fixed query");
        let actual = reference.execute(&graph);

        // Naive oracle: scan every row inserted, keeping the latest
        // insert per primary key (column 0).
        let mut expected = std::collections::BTreeMap::new();
        for event in &trace {
            if let LogicalEvent::Insert { new, .. } = event {
                expected.insert(new[0].clone(), new[0].clone());
            }
        }

        let mut actual_ids: Vec<String> = actual.into_iter().map(|row| row[0].clone()).collect();
        actual_ids.sort();
        let mut expected_ids: Vec<String> = expected.values().cloned().collect();
        expected_ids.sort();
        prop_assert_eq!(actual_ids, expected_ids);
    }
}
