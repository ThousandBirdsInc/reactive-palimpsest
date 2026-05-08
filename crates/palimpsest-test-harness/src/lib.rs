// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "Postgres-free test harness pieces for Palimpsest."]

pub mod mock_postgres;
pub mod wal;

pub use mock_postgres::{Fault, MockPostgres, Startup};
pub use wal::{Catalog, ColumnDef, LogicalEvent, Lsn, TableId, Tuple, WalGenerator};
