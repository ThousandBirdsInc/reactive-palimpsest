// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Manual `Row` codec for the wire protocol (§13, §18.9).
//!
//! Diff payloads are referenced by `schema_id` from the matching
//! [`Accepted`](crate::palimpsest::sync::v1::Accepted) message. The
//! payload itself is a bincode-encoded `Vec<Vec<WireDatum>>` — that
//! choice keeps the row-major layout small for the steady state (one
//! datum per column per row), avoids per-row reallocations, and stays
//! byte-stable as long as the [`WireDatum`] variant order is preserved.
//!
//! The codec lives in `palimpsest-proto` rather than `palimpsest-server`
//! so future clients (`palimpsest-client`, WASM browser SDKs) can decode
//! diffs without pulling in WAL/dataflow internals.
//!
//! # Wire-format invariants
//!
//! * Bincode default config (little-endian, varint disabled, no
//!   length-limit). Adding configuration knobs is a breaking change.
//! * [`WireDatum`] variants are append-only. New variants must be added
//!   to the end so existing tags remain stable.
//! * The `bytes` field on
//!   [`Diff`](crate::palimpsest::sync::v1::Diff) is encoded by
//!   [`encode_rows`] and decoded by [`decode_rows`] / [`decode_diff`].
//!
//! See `VERSIONING.md` for the full additive-only policy.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::palimpsest::sync::v1::{
    DatumType, Diff, DiffOp, RowChange as ProtoRowChange, Schema,
    TransactionUpdate as ProtoTransactionUpdate,
};

/// Wire-safe representation of one column value.
///
/// **Adding variants:** append only. Re-ordering this enum changes the
/// bincode tag for every variant and is a wire-incompatible change —
/// see `VERSIONING.md`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WireDatum {
    /// Boolean value.
    Bool(bool),
    /// 16-bit signed integer.
    I16(i16),
    /// 32-bit signed integer.
    I32(i32),
    /// 64-bit signed integer.
    I64(i64),
    /// 32-bit IEEE 754 float, encoded as the raw `u32` bit-pattern so
    /// `NaN` payloads round-trip exactly.
    F32(u32),
    /// 64-bit IEEE 754 float, encoded as the raw `u64` bit-pattern.
    F64(u64),
    /// Numeric value, lossless decimal text representation.
    Numeric(String),
    /// UTF-8 text value.
    Text(Vec<u8>),
    /// Opaque byte payload (`bytea`).
    Bytea(Vec<u8>),
    /// JSON document (UTF-8).
    Json(Vec<u8>),
    /// JSONB document (server-defined bytes).
    Jsonb(Vec<u8>),
    /// 16-byte UUID.
    Uuid([u8; 16]),
    /// SQL `NULL`.
    Null,
    /// Postgres `date`: days since the Unix epoch (1970-01-01).
    Date(i32),
    /// Postgres `time`: microseconds since midnight.
    Time(i64),
    /// Postgres `timestamp` (no timezone): microseconds since the
    /// Unix epoch.
    Timestamp(i64),
    /// Postgres `timestamptz`: microseconds since the Unix epoch,
    /// UTC-normalized.
    TimestampTz(i64),
    /// Postgres `interval`, kept in its native three-field form so
    /// calendar-aware arithmetic stays possible client-side.
    Interval {
        /// Whole months.
        months: i32,
        /// Whole days.
        days: i32,
        /// Sub-day microseconds.
        micros: i64,
    },
    /// Postgres array value: element datums in array order.
    Array(Vec<Self>),
}

/// One wire-encoded row — a flat list of column values matching the
/// associated schema's column order.
pub type WireRow = Vec<WireDatum>;

/// Decoded row-level change from a transaction envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireRowChange {
    /// Operation kind for this row.
    pub op: DiffOp,
    /// Pre-image, present for deletes and updates.
    pub old: Option<WireRow>,
    /// Post-image, present for inserts and updates.
    pub new: Option<WireRow>,
}

/// Failures that can occur encoding or decoding a row payload.
#[derive(Debug, Error)]
pub enum CodecError {
    /// Bincode failure during encode.
    #[error("bincode encode failed: {0}")]
    Encode(String),
    /// Bincode failure during decode.
    #[error("bincode decode failed: {0}")]
    Decode(String),
    /// Decoded row width does not match the registered schema.
    #[error("row arity mismatch: expected {expected}, got {actual}")]
    Arity {
        /// Column count from the schema.
        expected: usize,
        /// Column count present in the payload.
        actual: usize,
    },
    /// A transaction row-change payload did not contain exactly one row.
    #[error("row count mismatch: expected {expected}, got {actual}")]
    RowCount {
        /// Expected row count.
        expected: usize,
        /// Actual decoded row count.
        actual: usize,
    },
    /// Decoded datum variant is incompatible with the schema's
    /// declared column type.
    #[error("schema/datum mismatch at column {column}: schema={schema:?}, datum={datum}")]
    SchemaMismatch {
        /// Column index.
        column: usize,
        /// Declared column type.
        schema: DatumType,
        /// Wire-tag of the offending datum.
        datum: &'static str,
    },
    /// The diff referenced a `schema_id` not present in the registry.
    #[error("schema id {0} not registered")]
    UnknownSchema(u64),
}

/// Encodes a slice of [`WireRow`]s into the byte payload that goes onto
/// `Diff::rows`.
///
/// # Errors
/// Returns [`CodecError::Encode`] on bincode failure.
pub fn encode_rows(rows: &[WireRow]) -> Result<Vec<u8>, CodecError> {
    bincode::serialize(rows).map_err(|err| CodecError::Encode(err.to_string()))
}

/// Decodes a row payload back into a list of [`WireRow`]s.
///
/// # Errors
/// Returns [`CodecError::Decode`] on bincode failure.
pub fn decode_rows(bytes: &[u8]) -> Result<Vec<WireRow>, CodecError> {
    bincode::deserialize(bytes).map_err(|err| CodecError::Decode(err.to_string()))
}

/// Decodes a [`Diff`]'s row payload, validating it against the
/// [`Schema`] previously announced for `diff.schema_id`.
///
/// # Errors
/// * [`CodecError::Decode`] — bincode decode failed.
/// * [`CodecError::Arity`] — a row's column count differs from the
///   schema.
/// * [`CodecError::SchemaMismatch`] — a column's wire variant does not
///   line up with the declared [`DatumType`].
pub fn decode_diff(diff: &Diff, schema: &Schema) -> Result<Vec<WireRow>, CodecError> {
    let rows = decode_rows(&diff.rows)?;
    for row in &rows {
        validate_row(row, schema)?;
    }
    Ok(rows)
}

/// Decodes and validates every row change in a transaction update.
///
/// # Errors
/// Returns the same validation failures as [`decode_diff`] for any
/// encoded old/new row payload.
pub fn decode_transaction_update(
    update: &ProtoTransactionUpdate,
    schema: &Schema,
) -> Result<Vec<WireRowChange>, CodecError> {
    update
        .changes
        .iter()
        .map(|change| decode_row_change(change, schema))
        .collect()
}

fn decode_row_change(
    change: &ProtoRowChange,
    schema: &Schema,
) -> Result<WireRowChange, CodecError> {
    let op = DiffOp::try_from(change.op).unwrap_or(DiffOp::Unspecified);
    Ok(WireRowChange {
        op,
        old: decode_optional_single_row(change.old_row.as_deref(), schema)?,
        new: decode_optional_single_row(change.new_row.as_deref(), schema)?,
    })
}

fn decode_optional_single_row(
    bytes: Option<&[u8]>,
    schema: &Schema,
) -> Result<Option<WireRow>, CodecError> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let mut rows = decode_rows(bytes)?;
    for row in &rows {
        validate_row(row, schema)?;
    }
    if rows.len() != 1 {
        return Err(CodecError::RowCount {
            expected: 1,
            actual: rows.len(),
        });
    }
    Ok(rows.pop())
}

fn validate_row(row: &WireRow, schema: &Schema) -> Result<(), CodecError> {
    if row.len() != schema.columns.len() {
        return Err(CodecError::Arity {
            expected: schema.columns.len(),
            actual: row.len(),
        });
    }
    for (idx, (datum, column)) in row.iter().zip(schema.columns.iter()).enumerate() {
        let declared = DatumType::try_from(column.r#type).unwrap_or(DatumType::Unspecified);
        if !is_compatible(datum, declared, column.nullable) {
            return Err(CodecError::SchemaMismatch {
                column: idx,
                schema: declared,
                datum: datum_tag(datum),
            });
        }
    }
    Ok(())
}

const fn is_compatible(datum: &WireDatum, declared: DatumType, nullable: bool) -> bool {
    if matches!(datum, WireDatum::Null) {
        return nullable;
    }
    matches!(
        (datum, declared),
        (WireDatum::Bool(_), DatumType::Bool)
            | (WireDatum::I16(_), DatumType::I16)
            | (WireDatum::I32(_), DatumType::I32)
            | (WireDatum::I64(_), DatumType::I64)
            | (WireDatum::F32(_), DatumType::F32)
            | (WireDatum::F64(_), DatumType::F64)
            | (WireDatum::Numeric(_), DatumType::Numeric)
            | (WireDatum::Text(_), DatumType::Text)
            | (WireDatum::Bytea(_), DatumType::Bytea)
            | (WireDatum::Json(_), DatumType::Json)
            | (WireDatum::Jsonb(_), DatumType::Jsonb)
            | (WireDatum::Uuid(_), DatumType::Uuid)
            | (WireDatum::Date(_), DatumType::Date)
            | (WireDatum::Time(_), DatumType::Time)
            | (WireDatum::Timestamp(_), DatumType::Timestamp)
            | (WireDatum::TimestampTz(_), DatumType::TimestampTz)
            | (WireDatum::Interval { .. }, DatumType::Interval)
            // Array element types are not declared per-column on the
            // wire; the container variant is what the schema checks.
            | (WireDatum::Array(_), DatumType::Array)
            // Tolerate a server that hasn't advertised a strong schema:
            // a permissive `Unspecified` accepts anything.
            | (_, DatumType::Unspecified)
    )
}

const fn datum_tag(datum: &WireDatum) -> &'static str {
    match datum {
        WireDatum::Bool(_) => "bool",
        WireDatum::I16(_) => "i16",
        WireDatum::I32(_) => "i32",
        WireDatum::I64(_) => "i64",
        WireDatum::F32(_) => "f32",
        WireDatum::F64(_) => "f64",
        WireDatum::Numeric(_) => "numeric",
        WireDatum::Text(_) => "text",
        WireDatum::Bytea(_) => "bytea",
        WireDatum::Json(_) => "json",
        WireDatum::Jsonb(_) => "jsonb",
        WireDatum::Uuid(_) => "uuid",
        WireDatum::Null => "null",
        WireDatum::Date(_) => "date",
        WireDatum::Time(_) => "time",
        WireDatum::Timestamp(_) => "timestamp",
        WireDatum::TimestampTz(_) => "timestamptz",
        WireDatum::Interval { .. } => "interval",
        WireDatum::Array(_) => "array",
    }
}

/// Maps `schema_id` (announced via `Accepted`) to the [`Schema`] that
/// should be used to decode subsequent `Diff`s.
///
/// Clients populate the registry from each `Accepted`; encoder-side
/// callers (the server) populate it directly when allocating new
/// schemas.
#[derive(Debug, Default, Clone)]
pub struct SchemaRegistry {
    schemas: BTreeMap<u64, Schema>,
}

impl SchemaRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `schema` under `schema_id`.
    ///
    /// Re-registering a previously-seen id overwrites the entry — the
    /// server is the source of truth.
    pub fn register(&mut self, schema_id: u64, schema: Schema) {
        self.schemas.insert(schema_id, schema);
    }

    /// Removes a schema from the registry.
    pub fn remove(&mut self, schema_id: u64) -> Option<Schema> {
        self.schemas.remove(&schema_id)
    }

    /// Looks up a previously-registered schema.
    #[must_use]
    pub fn get(&self, schema_id: u64) -> Option<&Schema> {
        self.schemas.get(&schema_id)
    }

    /// Decodes a [`Diff`] using the schema already registered under
    /// `diff.schema_id`.
    ///
    /// # Errors
    /// * [`CodecError::UnknownSchema`] if the schema id has not been
    ///   registered.
    /// * Anything [`decode_diff`] can return.
    pub fn decode(&self, diff: &Diff) -> Result<Vec<WireRow>, CodecError> {
        let schema = self
            .get(diff.schema_id)
            .ok_or(CodecError::UnknownSchema(diff.schema_id))?;
        decode_diff(diff, schema)
    }

    /// Decodes a [`ProtoTransactionUpdate`] using its registered schema.
    ///
    /// # Errors
    /// * [`CodecError::UnknownSchema`] if the schema id has not been
    ///   registered.
    /// * Anything [`decode_transaction_update`] can return.
    pub fn decode_transaction(
        &self,
        update: &ProtoTransactionUpdate,
    ) -> Result<Vec<WireRowChange>, CodecError> {
        let schema = self
            .get(update.schema_id)
            .ok_or(CodecError::UnknownSchema(update.schema_id))?;
        decode_transaction_update(update, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_diff, decode_rows, decode_transaction_update, encode_rows, CodecError,
        SchemaRegistry, WireDatum,
    };
    use crate::palimpsest::sync::v1::{
        Column, DatumType, Diff, DiffOp, RowChange, Schema, TransactionUpdate,
    };

    fn posts_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "id".into(),
                    r#type: DatumType::I64.into(),
                    nullable: false,
                },
                Column {
                    name: "title".into(),
                    r#type: DatumType::Text.into(),
                    nullable: true,
                },
            ],
            primary_key_columns: vec![0],
        }
    }

    #[test]
    fn round_trips_text_and_int_rows() {
        let rows = vec![
            vec![WireDatum::I64(1), WireDatum::Text(b"hello".to_vec())],
            vec![WireDatum::I64(2), WireDatum::Null],
        ];
        let bytes = encode_rows(&rows).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn decode_diff_validates_schema_arity() {
        let bad = vec![vec![WireDatum::I64(1)]];
        let bytes = encode_rows(&bad).unwrap();
        let diff = Diff {
            subscription_id: "posts".into(),
            lsn: 1,
            op: DiffOp::Insert.into(),
            schema_id: 7,
            rows: bytes,
        };
        let err = decode_diff(&diff, &posts_schema()).unwrap_err();
        assert!(matches!(
            err,
            CodecError::Arity {
                expected: 2,
                actual: 1
            }
        ));
    }

    #[test]
    fn decode_diff_rejects_type_mismatch() {
        let rows = vec![vec![WireDatum::I64(1), WireDatum::I32(0)]];
        let bytes = encode_rows(&rows).unwrap();
        let diff = Diff {
            subscription_id: "posts".into(),
            lsn: 1,
            op: DiffOp::Insert.into(),
            schema_id: 7,
            rows: bytes,
        };
        let err = decode_diff(&diff, &posts_schema()).unwrap_err();
        assert!(matches!(
            err,
            CodecError::SchemaMismatch {
                column: 1,
                schema: DatumType::Text,
                datum: "i32"
            }
        ));
    }

    #[test]
    fn decode_diff_accepts_null_for_nullable_column() {
        let rows = vec![vec![WireDatum::I64(1), WireDatum::Null]];
        let bytes = encode_rows(&rows).unwrap();
        let diff = Diff {
            subscription_id: "posts".into(),
            lsn: 1,
            op: DiffOp::Insert.into(),
            schema_id: 7,
            rows: bytes,
        };
        let decoded = decode_diff(&diff, &posts_schema()).unwrap();
        assert_eq!(decoded.len(), 1);
    }

    #[test]
    fn decode_diff_rejects_null_for_non_nullable_column() {
        let rows = vec![vec![WireDatum::Null, WireDatum::Text(b"x".to_vec())]];
        let bytes = encode_rows(&rows).unwrap();
        let diff = Diff {
            subscription_id: "posts".into(),
            lsn: 1,
            op: DiffOp::Insert.into(),
            schema_id: 7,
            rows: bytes,
        };
        let err = decode_diff(&diff, &posts_schema()).unwrap_err();
        assert!(matches!(
            err,
            CodecError::SchemaMismatch {
                column: 0,
                schema: DatumType::I64,
                datum: "null"
            }
        ));
    }

    #[test]
    fn registry_decodes_known_schema_and_rejects_unknown() {
        let mut reg = SchemaRegistry::new();
        reg.register(7, posts_schema());

        let rows = vec![vec![WireDatum::I64(42), WireDatum::Text(b"a".to_vec())]];
        let bytes = encode_rows(&rows).unwrap();
        let diff = Diff {
            subscription_id: "posts".into(),
            lsn: 1,
            op: DiffOp::Insert.into(),
            schema_id: 7,
            rows: bytes,
        };
        let decoded = reg.decode(&diff).unwrap();
        assert_eq!(decoded, rows);

        let stale = Diff {
            schema_id: 99,
            ..diff
        };
        let err = reg.decode(&stale).unwrap_err();
        assert!(matches!(err, CodecError::UnknownSchema(99)));
    }

    #[test]
    fn transaction_update_preserves_per_row_ops() {
        let old = vec![WireDatum::I64(1), WireDatum::Text(b"old".to_vec())];
        let new = vec![WireDatum::I64(1), WireDatum::Text(b"new".to_vec())];
        let update = TransactionUpdate {
            subscription_id: "posts".into(),
            commit_lsn: 10,
            begin_lsn: Some(8),
            end_lsn: Some(11),
            transaction_id: Some(99),
            schema_id: 7,
            chunk_index: 0,
            chunk_count: 1,
            changes: vec![
                RowChange {
                    op: DiffOp::Update.into(),
                    old_row: Some(encode_rows(std::slice::from_ref(&old)).unwrap()),
                    new_row: Some(encode_rows(std::slice::from_ref(&new)).unwrap()),
                },
                RowChange {
                    op: DiffOp::Delete.into(),
                    old_row: Some(encode_rows(std::slice::from_ref(&old)).unwrap()),
                    new_row: None,
                },
            ],
        };

        let decoded = decode_transaction_update(&update, &posts_schema()).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].op, DiffOp::Update);
        assert_eq!(decoded[0].old.as_ref(), Some(&old));
        assert_eq!(decoded[0].new.as_ref(), Some(&new));
        assert_eq!(decoded[1].op, DiffOp::Delete);
        assert_eq!(decoded[1].old.as_ref(), Some(&old));
        assert!(decoded[1].new.is_none());
    }

    #[test]
    fn float_round_trips_via_bit_pattern() {
        let rows = vec![vec![
            WireDatum::F32(f32::to_bits(0.25)),
            WireDatum::F64(f64::to_bits(1.5)),
        ]];
        let bytes = encode_rows(&rows).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn temporal_and_array_datums_round_trip() {
        let rows = vec![vec![
            WireDatum::Date(19_723),
            WireDatum::Time(43_200_000_000),
            WireDatum::Timestamp(1_700_000_000_000_000),
            WireDatum::TimestampTz(1_700_000_000_000_000),
            WireDatum::Interval {
                months: 1,
                days: 2,
                micros: 3_000_000,
            },
            WireDatum::Array(vec![
                WireDatum::Text(b"a".to_vec()),
                WireDatum::Null,
                WireDatum::Array(vec![WireDatum::I32(1)]),
            ]),
        ]];
        let bytes = encode_rows(&rows).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn temporal_datums_validate_against_declared_schema() {
        let schema = Schema {
            columns: vec![
                Column {
                    name: "created_at".into(),
                    r#type: DatumType::TimestampTz.into(),
                    nullable: false,
                },
                Column {
                    name: "tags".into(),
                    r#type: DatumType::Array.into(),
                    nullable: true,
                },
            ],
            primary_key_columns: vec![0],
        };
        let rows = vec![vec![
            WireDatum::TimestampTz(1_700_000_000_000_000),
            WireDatum::Array(vec![WireDatum::Text(b"bug".to_vec())]),
        ]];
        let bytes = encode_rows(&rows).unwrap();
        let diff = Diff {
            subscription_id: "tickets".into(),
            lsn: 1,
            op: DiffOp::Insert.into(),
            schema_id: 3,
            rows: bytes,
        };
        let decoded = decode_diff(&diff, &schema).unwrap();
        assert_eq!(decoded, rows);

        // A timestamp datum on a timestamptz column is still a
        // mismatch — the variants are distinct on purpose.
        let bad = encode_rows(&[vec![
            WireDatum::Timestamp(0),
            WireDatum::Null,
        ]])
        .unwrap();
        let diff = Diff {
            rows: bad,
            ..diff
        };
        let err = decode_diff(&diff, &schema).unwrap_err();
        assert!(matches!(
            err,
            CodecError::SchemaMismatch {
                column: 0,
                schema: DatumType::TimestampTz,
                datum: "timestamp"
            }
        ));
    }
}
