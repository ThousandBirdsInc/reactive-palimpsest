// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "Postgres-free test harness pieces for Palimpsest."]
#![warn(missing_docs)]

pub mod harness;
pub mod mock_postgres;
pub mod reference;
pub mod wal;

pub use harness::{HarnessBuilder, TestHarness};
pub use mock_postgres::{Fault, MockPostgres, Startup};
pub use reference::{
    assert_set_eq, fixture_line_count_within_budget, PrimaryKey, ReferenceExecutor, Row, SetDiff,
    Truth,
};
pub use wal::{
    Catalog, ColumnDef, LogicalEvent, Lsn, TableDef, TableId, TruncateOpts, Tuple, WalGenerator,
};
