// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod catalog;
mod datum;
mod error;
mod protocol;
mod source;
mod spill;
mod types;

pub use catalog::{
    catalog_probe_sql, load_catalog_from_probe_rows, Catalog, CatalogProbeRow, RelationSchema,
    ReplicaIdentity,
};
pub use datum::{
    decode_column_value, stock_postgres_16_type, BigDecimal, ColumnValue, Date, Datum, DatumType,
    Interval, Time, Timestamp, TimestampTz, Uuid, BOOL_ARRAY_OID, BOOL_OID, BYTEA_ARRAY_OID,
    BYTEA_OID, DATE_OID, FLOAT4_ARRAY_OID, FLOAT4_OID, FLOAT8_ARRAY_OID, FLOAT8_OID,
    INT2_ARRAY_OID, INT2_OID, INT4_ARRAY_OID, INT4_OID, INT8_ARRAY_OID, INT8_OID, INTERVAL_OID,
    JSONB_ARRAY_OID, JSONB_OID, JSON_ARRAY_OID, JSON_OID, NUMERIC_ARRAY_OID, NUMERIC_OID,
    TEXT_ARRAY_OID, TEXT_OID, TIMESTAMPTZ_OID, TIMESTAMP_OID, TIME_OID, UUID_ARRAY_OID, UUID_OID,
};
pub use error::{Result, WalError};
pub use protocol::{
    decode_pgoutput_message, DecodedEvent, Origin, RowOp, StreamAction, Truncate, TwoPhaseAction,
    TypeInfo,
};
pub use source::{ReconnectBackoff, RestartLsnStore, WalConfig, WalSource};
pub use spill::{BackpressureQueue, StreamSpillBuffer};
pub use types::{ColumnDef, Lsn, TableId, Tuple};
