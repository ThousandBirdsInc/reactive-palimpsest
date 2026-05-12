// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end test harness wiring: spins up a mock Postgres, drives
//! WAL events, and produces an executable subscription pipeline.

#![allow(missing_docs)]

use std::{io, sync::Arc};

use crate::{
    reference::{assert_set_eq, Row},
    Catalog, LogicalEvent, MockPostgres, ReferenceExecutor, WalGenerator,
};

#[derive(Debug)]
pub struct TestHarness {
    schema: Arc<Catalog>,
    wal: WalGenerator,
    reference: ReferenceExecutor,
    mock_pg: MockPostgres,
}

impl TestHarness {
    pub fn new(schema: Catalog) -> io::Result<Self> {
        let schema = Arc::new(schema);
        Ok(Self {
            wal: WalGenerator::with_catalog((*schema).clone()),
            reference: ReferenceExecutor::new(Arc::clone(&schema)),
            mock_pg: MockPostgres::bind()?,
            schema,
        })
    }

    #[must_use]
    pub fn schema(&self) -> &Catalog {
        &self.schema
    }

    #[must_use]
    pub const fn wal(&self) -> &WalGenerator {
        &self.wal
    }

    pub fn wal_mut(&mut self) -> &mut WalGenerator {
        &mut self.wal
    }

    #[must_use]
    pub const fn reference(&self) -> &ReferenceExecutor {
        &self.reference
    }

    pub fn reference_mut(&mut self) -> &mut ReferenceExecutor {
        &mut self.reference
    }

    #[must_use]
    pub const fn mock_pg(&self) -> &MockPostgres {
        &self.mock_pg
    }

    pub fn mock_pg_mut(&mut self) -> &mut MockPostgres {
        &mut self.mock_pg
    }

    pub fn drive(&mut self, events: &[LogicalEvent]) {
        let frames = self.wal.encode_pgoutput(events);
        self.mock_pg.push_wal(frames);
        self.reference.apply(events);
    }

    pub fn assert_snapshot(&self, actual: &[Row], expected: &[Row]) -> Result<(), crate::SetDiff> {
        assert_set_eq(actual, expected)
    }
}

#[derive(Debug, Clone)]
pub struct HarnessBuilder {
    schema: Catalog,
}

impl HarnessBuilder {
    #[must_use]
    pub const fn new(schema: Catalog) -> Self {
        Self { schema }
    }

    pub fn build(self) -> io::Result<TestHarness> {
        TestHarness::new(self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::HarnessBuilder;
    use crate::{Catalog, ColumnDef, LogicalEvent, TableDef, TableId};

    #[test]
    fn builder_composes_wal_mock_postgres_and_reference_executor() {
        let table = TableId::new(7);
        let mut harness = HarnessBuilder::new(Catalog::with_tables([TableDef::new(
            table,
            "posts",
            vec![ColumnDef {
                name: "id".to_owned(),
                type_oid: 25,
                nullable: false,
            }],
        )]))
        .build()
        .expect("harness should bind mock postgres");

        harness.drive(&[LogicalEvent::Insert {
            table,
            new: vec!["post-1".to_owned()],
        }]);

        assert_eq!(
            harness
                .reference()
                .table(table)
                .and_then(|rows| rows.get("post-1")),
            Some(&vec!["post-1".to_owned()])
        );
        assert_ne!(harness.mock_pg().local_addr().port(), 0);
    }
}
