// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Manual `Row` codec for the wire protocol (§13, §18.9).
//!
//! Diff payloads are referenced by `schema_id` from the matching
//! [`Accepted`](crate::palimpsest::sync::v1::Accepted) message. The
//! payload itself is a `Vec<Vec<WireDatum>>` in the bincode 1.x
//! default layout — that choice keeps the row-major layout small for
//! the steady state (one datum per column per row), avoids per-row
//! reallocations, and stays byte-stable as long as the [`WireDatum`]
//! variant order is preserved.
//!
//! The encoder and decoder are written by hand (see [`codec`]) rather
//! than going through serde + bincode: the layout is a dozen rules, and
//! the derived deserializer dragged `serde`, `bincode`, and ~20 KB of
//! `core::fmt` float formatting into the browser bundle. The serde
//! derives on [`WireDatum`] remain available behind the default-on
//! `serde` feature, and the test suite checks every encoding against
//! `bincode` as the golden reference.
//!
//! The codec lives in `palimpsest-proto` rather than `palimpsest-server`
//! so clients (`palimpsest-client`, the WASM browser SDK) can decode
//! diffs without pulling in WAL/dataflow internals.
//!
//! # Wire-format invariants
//!
//! * Bincode 1.x default config: little-endian, fixed-width integers
//!   (no varint), `u64` length prefixes, `u32` enum tags, no length
//!   limit, trailing bytes tolerated on decode. Changing any of these
//!   is a breaking change.
//! * [`WireDatum`] variants are append-only. New variants must be added
//!   to the end so existing tags remain stable, and [`codec`] must be
//!   extended in the same change.
//! * The `bytes` field on
//!   [`Diff`](crate::palimpsest::sync::v1::Diff) is encoded by
//!   [`encode_rows`] and decoded by [`decode_rows`] / [`decode_diff`].
//!
//! See `VERSIONING.md` for the full additive-only policy.

use std::collections::BTreeMap;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::palimpsest::sync::v1::{
    DatumType, Diff, DiffOp, RowChange as ProtoRowChange, Schema,
    TransactionUpdate as ProtoTransactionUpdate,
};

/// Wire-safe representation of one column value.
///
/// **Adding variants:** append only. Re-ordering this enum changes the
/// wire tag for every variant and is a wire-incompatible change — see
/// `VERSIONING.md`. Every variant must be handled by [`codec`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
    /// Row payload could not be encoded.
    #[error("row encode failed: {0}")]
    Encode(String),
    /// Row payload bytes are malformed.
    #[error("row decode failed: {0}")]
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
/// Returns [`CodecError::Encode`] if the payload exceeds the format's
/// limits (a nesting deeper than [`codec::MAX_DEPTH`]).
pub fn encode_rows(rows: &[WireRow]) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(codec::encoded_len(rows));
    codec::encode_rows_into(rows, &mut out)?;
    Ok(out)
}

/// Decodes a row payload back into a list of [`WireRow`]s.
///
/// # Errors
/// Returns [`CodecError::Decode`] if the bytes are truncated, carry an
/// unknown datum tag, an invalid `bool`, non-UTF-8 `numeric` text, or
/// nest arrays deeper than [`codec::MAX_DEPTH`].
pub fn decode_rows(bytes: &[u8]) -> Result<Vec<WireRow>, CodecError> {
    codec::decode_rows(bytes)
}

/// Hand-written encoder/decoder for the row payload.
///
/// Byte-for-byte the bincode 1.x default layout of
/// `Vec<Vec<WireDatum>>` (what `bincode::serialize` produced before
/// this module existed, and what the serde derives still produce):
///
/// | Rust                    | bytes                                   |
/// |-------------------------|-----------------------------------------|
/// | `Vec<T>` / `String`     | `u64` LE length, then the elements/bytes|
/// | enum variant            | `u32` LE variant index, then its fields |
/// | `bool`                  | one byte, `0` or `1`                    |
/// | `i16`/`i32`/`i64`/`u32`/`u64` | fixed-width LE                    |
/// | `[u8; 16]`              | the 16 bytes, no length prefix          |
/// | struct variant fields   | in declaration order                    |
pub mod codec {
    use super::{CodecError, WireDatum, WireRow};

    /// Maximum nesting of [`WireDatum::Array`] the codec accepts.
    ///
    /// Enforced by the decoder (and refused by the encoder) so a
    /// hostile payload cannot recurse the decoder off the stack. Real
    /// Postgres arrays nest a handful of levels at most.
    pub const MAX_DEPTH: usize = 64;

    // Variant indices — the wire tags. Append-only, see `WireDatum`.
    const TAG_BOOL: u32 = 0;
    const TAG_I16: u32 = 1;
    const TAG_I32: u32 = 2;
    const TAG_I64: u32 = 3;
    const TAG_F32: u32 = 4;
    const TAG_F64: u32 = 5;
    const TAG_NUMERIC: u32 = 6;
    const TAG_TEXT: u32 = 7;
    const TAG_BYTEA: u32 = 8;
    const TAG_JSON: u32 = 9;
    const TAG_JSONB: u32 = 10;
    const TAG_UUID: u32 = 11;
    const TAG_NULL: u32 = 12;
    const TAG_DATE: u32 = 13;
    const TAG_TIME: u32 = 14;
    const TAG_TIMESTAMP: u32 = 15;
    const TAG_TIMESTAMPTZ: u32 = 16;
    const TAG_INTERVAL: u32 = 17;
    const TAG_ARRAY: u32 = 18;

    /// Size of a `u64` length prefix.
    const LEN: usize = 8;
    /// Size of a `u32` variant tag.
    const TAG: usize = 4;

    /// Exact encoded size of `rows`, so the caller can allocate once.
    #[must_use]
    pub fn encoded_len(rows: &[WireRow]) -> usize {
        LEN + rows
            .iter()
            .map(|row| LEN + row.iter().map(datum_len).sum::<usize>())
            .sum::<usize>()
    }

    fn datum_len(datum: &WireDatum) -> usize {
        TAG + match datum {
            WireDatum::Bool(_) => 1,
            WireDatum::I16(_) => 2,
            WireDatum::I32(_) | WireDatum::F32(_) | WireDatum::Date(_) => 4,
            WireDatum::I64(_)
            | WireDatum::F64(_)
            | WireDatum::Time(_)
            | WireDatum::Timestamp(_)
            | WireDatum::TimestampTz(_) => 8,
            WireDatum::Numeric(s) => LEN + s.len(),
            WireDatum::Text(b) | WireDatum::Bytea(b) | WireDatum::Json(b) | WireDatum::Jsonb(b) => {
                LEN + b.len()
            }
            WireDatum::Uuid(_) => 16,
            WireDatum::Null => 0,
            WireDatum::Interval { .. } => 4 + 4 + 8,
            WireDatum::Array(items) => LEN + items.iter().map(datum_len).sum::<usize>(),
        }
    }

    /// Appends the encoding of `rows` to `out`.
    ///
    /// # Errors
    /// [`CodecError::Encode`] if an array nests deeper than
    /// [`MAX_DEPTH`].
    pub fn encode_rows_into(rows: &[WireRow], out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_len(out, rows.len());
        for row in rows {
            put_len(out, row.len());
            for datum in row {
                encode_datum(datum, out, 0)?;
            }
        }
        Ok(())
    }

    fn put_len(out: &mut Vec<u8>, len: usize) {
        out.extend_from_slice(&(len as u64).to_le_bytes());
    }

    fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
        put_len(out, bytes.len());
        out.extend_from_slice(bytes);
    }

    fn encode_datum(datum: &WireDatum, out: &mut Vec<u8>, depth: usize) -> Result<(), CodecError> {
        match datum {
            WireDatum::Bool(b) => {
                out.extend_from_slice(&TAG_BOOL.to_le_bytes());
                out.push(u8::from(*b));
            }
            WireDatum::I16(n) => {
                out.extend_from_slice(&TAG_I16.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            WireDatum::I32(n) => {
                out.extend_from_slice(&TAG_I32.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            WireDatum::I64(n) => {
                out.extend_from_slice(&TAG_I64.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            WireDatum::F32(bits) => {
                out.extend_from_slice(&TAG_F32.to_le_bytes());
                out.extend_from_slice(&bits.to_le_bytes());
            }
            WireDatum::F64(bits) => {
                out.extend_from_slice(&TAG_F64.to_le_bytes());
                out.extend_from_slice(&bits.to_le_bytes());
            }
            WireDatum::Numeric(s) => {
                out.extend_from_slice(&TAG_NUMERIC.to_le_bytes());
                put_bytes(out, s.as_bytes());
            }
            WireDatum::Text(b) => {
                out.extend_from_slice(&TAG_TEXT.to_le_bytes());
                put_bytes(out, b);
            }
            WireDatum::Bytea(b) => {
                out.extend_from_slice(&TAG_BYTEA.to_le_bytes());
                put_bytes(out, b);
            }
            WireDatum::Json(b) => {
                out.extend_from_slice(&TAG_JSON.to_le_bytes());
                put_bytes(out, b);
            }
            WireDatum::Jsonb(b) => {
                out.extend_from_slice(&TAG_JSONB.to_le_bytes());
                put_bytes(out, b);
            }
            WireDatum::Uuid(bytes) => {
                out.extend_from_slice(&TAG_UUID.to_le_bytes());
                out.extend_from_slice(bytes);
            }
            WireDatum::Null => out.extend_from_slice(&TAG_NULL.to_le_bytes()),
            WireDatum::Date(n) => {
                out.extend_from_slice(&TAG_DATE.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            WireDatum::Time(n) => {
                out.extend_from_slice(&TAG_TIME.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            WireDatum::Timestamp(n) => {
                out.extend_from_slice(&TAG_TIMESTAMP.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            WireDatum::TimestampTz(n) => {
                out.extend_from_slice(&TAG_TIMESTAMPTZ.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            WireDatum::Interval {
                months,
                days,
                micros,
            } => {
                out.extend_from_slice(&TAG_INTERVAL.to_le_bytes());
                out.extend_from_slice(&months.to_le_bytes());
                out.extend_from_slice(&days.to_le_bytes());
                out.extend_from_slice(&micros.to_le_bytes());
            }
            WireDatum::Array(items) => {
                if depth >= MAX_DEPTH {
                    return Err(CodecError::Encode(format!(
                        "array nesting exceeds {MAX_DEPTH} levels"
                    )));
                }
                out.extend_from_slice(&TAG_ARRAY.to_le_bytes());
                put_len(out, items.len());
                for item in items {
                    encode_datum(item, out, depth + 1)?;
                }
            }
        }
        Ok(())
    }

    /// Decodes a full row payload. Trailing bytes after the last row
    /// are ignored, matching `bincode::deserialize`.
    pub(super) fn decode_rows(bytes: &[u8]) -> Result<Vec<WireRow>, CodecError> {
        let mut reader = Reader { bytes, pos: 0 };
        let row_count = reader.len_prefix(LEN)?;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let width = reader.len_prefix(TAG)?;
            let mut row = Vec::with_capacity(width);
            for _ in 0..width {
                row.push(reader.datum(0)?);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    struct Reader<'a> {
        bytes: &'a [u8],
        pos: usize,
    }

    impl<'a> Reader<'a> {
        const fn remaining(&self) -> usize {
            self.bytes.len() - self.pos
        }

        fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
            if self.remaining() < n {
                return Err(CodecError::Decode(format!(
                    "unexpected end of payload at byte {} (wanted {n} more)",
                    self.pos
                )));
            }
            let slice = &self.bytes[self.pos..self.pos + n];
            self.pos += n;
            Ok(slice)
        }

        fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
            let mut out = [0u8; N];
            out.copy_from_slice(self.take(N)?);
            Ok(out)
        }

        fn u8(&mut self) -> Result<u8, CodecError> {
            Ok(self.take(1)?[0])
        }

        fn u32(&mut self) -> Result<u32, CodecError> {
            self.array().map(u32::from_le_bytes)
        }

        fn u64(&mut self) -> Result<u64, CodecError> {
            self.array().map(u64::from_le_bytes)
        }

        fn i16(&mut self) -> Result<i16, CodecError> {
            self.array().map(i16::from_le_bytes)
        }

        fn i32(&mut self) -> Result<i32, CodecError> {
            self.array().map(i32::from_le_bytes)
        }

        fn i64(&mut self) -> Result<i64, CodecError> {
            self.array().map(i64::from_le_bytes)
        }

        /// A `u64` element count, rejected up front when the payload
        /// cannot possibly hold that many elements of at least
        /// `min_element_size` bytes each — so a hostile length prefix
        /// never drives a huge allocation.
        fn len_prefix(&mut self, min_element_size: usize) -> Result<usize, CodecError> {
            let len = self.u64()?;
            let fits = usize::try_from(len)
                .ok()
                .and_then(|len| len.checked_mul(min_element_size))
                .is_some_and(|needed| needed <= self.remaining());
            if !fits {
                return Err(CodecError::Decode(format!(
                    "length prefix {len} at byte {} exceeds the payload",
                    self.pos - LEN
                )));
            }
            Ok(len as usize)
        }

        fn bytes(&mut self) -> Result<Vec<u8>, CodecError> {
            let len = self.len_prefix(1)?;
            Ok(self.take(len)?.to_vec())
        }

        fn string(&mut self) -> Result<String, CodecError> {
            let bytes = self.bytes()?;
            String::from_utf8(bytes)
                .map_err(|_| CodecError::Decode("numeric text is not valid UTF-8".to_owned()))
        }

        fn datum(&mut self, depth: usize) -> Result<WireDatum, CodecError> {
            let tag_at = self.pos;
            let datum = match self.u32()? {
                TAG_BOOL => match self.u8()? {
                    0 => WireDatum::Bool(false),
                    1 => WireDatum::Bool(true),
                    other => {
                        return Err(CodecError::Decode(format!(
                            "invalid bool byte {other} at byte {}",
                            self.pos - 1
                        )))
                    }
                },
                TAG_I16 => WireDatum::I16(self.i16()?),
                TAG_I32 => WireDatum::I32(self.i32()?),
                TAG_I64 => WireDatum::I64(self.i64()?),
                TAG_F32 => WireDatum::F32(self.u32()?),
                TAG_F64 => WireDatum::F64(self.u64()?),
                TAG_NUMERIC => WireDatum::Numeric(self.string()?),
                TAG_TEXT => WireDatum::Text(self.bytes()?),
                TAG_BYTEA => WireDatum::Bytea(self.bytes()?),
                TAG_JSON => WireDatum::Json(self.bytes()?),
                TAG_JSONB => WireDatum::Jsonb(self.bytes()?),
                TAG_UUID => WireDatum::Uuid(self.array()?),
                TAG_NULL => WireDatum::Null,
                TAG_DATE => WireDatum::Date(self.i32()?),
                TAG_TIME => WireDatum::Time(self.i64()?),
                TAG_TIMESTAMP => WireDatum::Timestamp(self.i64()?),
                TAG_TIMESTAMPTZ => WireDatum::TimestampTz(self.i64()?),
                TAG_INTERVAL => WireDatum::Interval {
                    months: self.i32()?,
                    days: self.i32()?,
                    micros: self.i64()?,
                },
                TAG_ARRAY => {
                    if depth >= MAX_DEPTH {
                        return Err(CodecError::Decode(format!(
                            "array nesting exceeds {MAX_DEPTH} levels at byte {tag_at}"
                        )));
                    }
                    let count = self.len_prefix(TAG)?;
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        items.push(self.datum(depth + 1)?);
                    }
                    WireDatum::Array(items)
                }
                other => {
                    return Err(CodecError::Decode(format!(
                        "unknown datum tag {other} at byte {tag_at}"
                    )))
                }
            };
            Ok(datum)
        }
    }
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

    /// Every variant at least once, plus edge values, so the
    /// bincode-equivalence tests below exercise each codec arm.
    fn corpus() -> Vec<WireDatum> {
        vec![
            WireDatum::Bool(false),
            WireDatum::Bool(true),
            WireDatum::I16(i16::MIN),
            WireDatum::I16(-1),
            WireDatum::I32(i32::MAX),
            WireDatum::I64(i64::MIN),
            WireDatum::F32(f32::NAN.to_bits()),
            WireDatum::F32(f32::to_bits(-0.0)),
            WireDatum::F64(f64::to_bits(f64::INFINITY)),
            WireDatum::F64(0x7ff8_dead_beef_0001),
            WireDatum::Numeric(String::new()),
            WireDatum::Numeric("-12345.678900".into()),
            WireDatum::Numeric("ünïcödé".into()),
            WireDatum::Text(Vec::new()),
            WireDatum::Text(b"hello \xff world".to_vec()),
            WireDatum::Bytea(vec![0, 1, 2, 255]),
            WireDatum::Json(b"{\"a\":[1,2]}".to_vec()),
            WireDatum::Jsonb(vec![1, 0, 0, 0]),
            WireDatum::Uuid([0u8; 16]),
            WireDatum::Uuid([
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ]),
            WireDatum::Null,
            WireDatum::Date(-1),
            WireDatum::Date(i32::MAX),
            WireDatum::Time(0),
            WireDatum::Timestamp(i64::MAX),
            WireDatum::TimestampTz(-1),
            WireDatum::Interval {
                months: -1,
                days: i32::MIN,
                micros: 42,
            },
            WireDatum::Array(Vec::new()),
            WireDatum::Array(vec![
                WireDatum::Null,
                WireDatum::Array(vec![WireDatum::Array(vec![WireDatum::Bool(true)])]),
                WireDatum::Text(b"x".to_vec()),
            ]),
        ]
    }

    /// Tiny xorshift generator so the equivalence test can sweep
    /// random payloads deterministically without a proptest dep.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn bytes(&mut self) -> Vec<u8> {
            let len = self.below(12) as usize;
            (0..len).map(|_| self.next() as u8).collect()
        }

        fn datum(&mut self, depth: usize) -> WireDatum {
            match self.below(19) {
                0 => WireDatum::Bool(self.below(2) == 1),
                1 => WireDatum::I16(self.next() as i16),
                2 => WireDatum::I32(self.next() as i32),
                3 => WireDatum::I64(self.next() as i64),
                4 => WireDatum::F32(self.next() as u32),
                5 => WireDatum::F64(self.next()),
                6 => WireDatum::Numeric(String::from_utf8_lossy(&self.bytes()).into_owned()),
                7 => WireDatum::Text(self.bytes()),
                8 => WireDatum::Bytea(self.bytes()),
                9 => WireDatum::Json(self.bytes()),
                10 => WireDatum::Jsonb(self.bytes()),
                11 => {
                    let mut uuid = [0u8; 16];
                    for byte in &mut uuid {
                        *byte = self.next() as u8;
                    }
                    WireDatum::Uuid(uuid)
                }
                12 => WireDatum::Null,
                13 => WireDatum::Date(self.next() as i32),
                14 => WireDatum::Time(self.next() as i64),
                15 => WireDatum::Timestamp(self.next() as i64),
                16 => WireDatum::TimestampTz(self.next() as i64),
                17 => WireDatum::Interval {
                    months: self.next() as i32,
                    days: self.next() as i32,
                    micros: self.next() as i64,
                },
                _ if depth < 4 => {
                    let len = self.below(4) as usize;
                    WireDatum::Array((0..len).map(|_| self.datum(depth + 1)).collect())
                }
                _ => WireDatum::Null,
            }
        }

        fn rows(&mut self) -> Vec<Vec<WireDatum>> {
            let rows = self.below(4) as usize;
            let width = self.below(6) as usize;
            (0..rows)
                .map(|_| (0..width).map(|_| self.datum(0)).collect())
                .collect()
        }
    }

    #[test]
    fn encoded_len_matches_actual_encoding() {
        let rows = vec![corpus(), vec![WireDatum::Null], Vec::new()];
        let bytes = encode_rows(&rows).unwrap();
        assert_eq!(bytes.len(), super::codec::encoded_len(&rows));
        assert_eq!(decode_rows(&bytes).unwrap(), rows);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn manual_codec_matches_bincode_on_corpus() {
        let rows = vec![
            corpus(),
            Vec::new(),
            vec![WireDatum::Null, WireDatum::Bool(true)],
        ];
        let manual = encode_rows(&rows).unwrap();
        let golden = bincode::serialize(&rows).unwrap();
        assert_eq!(manual, golden, "encoder diverged from bincode layout");
        let via_bincode: Vec<Vec<WireDatum>> = bincode::deserialize(&manual).unwrap();
        assert_eq!(via_bincode, rows);
        assert_eq!(decode_rows(&golden).unwrap(), rows);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn manual_codec_matches_bincode_on_random_payloads() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..2_000 {
            let rows = rng.rows();
            let manual = encode_rows(&rows).unwrap();
            let golden = bincode::serialize(&rows).unwrap();
            assert_eq!(manual, golden, "encoder diverged for {rows:?}");
            assert_eq!(decode_rows(&golden).unwrap(), rows);
            let via_bincode: Vec<Vec<WireDatum>> = bincode::deserialize(&manual).unwrap();
            assert_eq!(via_bincode, rows);
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn manual_decoder_rejects_what_bincode_rejects() {
        let rows = vec![corpus()];
        let bytes = encode_rows(&rows).unwrap();
        // Truncation at every prefix length.
        for cut in 0..bytes.len() {
            let truncated = &bytes[..cut];
            let golden: Result<Vec<Vec<WireDatum>>, _> = bincode::deserialize(truncated);
            assert!(golden.is_err(), "bincode accepted a {cut}-byte prefix");
            assert!(
                decode_rows(truncated).is_err(),
                "manual decoder accepted a {cut}-byte prefix"
            );
        }
        // Trailing garbage is tolerated by both.
        let mut padded = bytes;
        padded.extend_from_slice(&[0xde, 0xad]);
        let golden: Vec<Vec<WireDatum>> = bincode::deserialize(&padded).unwrap();
        assert_eq!(decode_rows(&padded).unwrap(), golden);
    }

    #[test]
    fn decoder_rejects_malformed_datums() {
        // One row, one datum, bool with byte 2.
        let mut bad_bool = Vec::new();
        bad_bool.extend_from_slice(&1u64.to_le_bytes());
        bad_bool.extend_from_slice(&1u64.to_le_bytes());
        bad_bool.extend_from_slice(&0u32.to_le_bytes());
        bad_bool.push(2);
        assert!(matches!(decode_rows(&bad_bool), Err(CodecError::Decode(_))));

        // Unknown tag 19.
        let mut bad_tag = Vec::new();
        bad_tag.extend_from_slice(&1u64.to_le_bytes());
        bad_tag.extend_from_slice(&1u64.to_le_bytes());
        bad_tag.extend_from_slice(&19u32.to_le_bytes());
        assert!(matches!(decode_rows(&bad_tag), Err(CodecError::Decode(_))));

        // Numeric with invalid UTF-8.
        let bad_numeric = encode_rows(&[vec![WireDatum::Text(vec![0xff])]])
            .unwrap()
            .iter()
            .enumerate()
            .map(|(idx, byte)| if idx == 16 { 6 } else { *byte })
            .collect::<Vec<u8>>();
        assert!(matches!(
            decode_rows(&bad_numeric),
            Err(CodecError::Decode(_))
        ));

        // A length prefix larger than the payload fails fast instead
        // of allocating.
        let mut huge = Vec::new();
        huge.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(decode_rows(&huge), Err(CodecError::Decode(_))));
    }

    #[test]
    fn codec_bounds_array_nesting() {
        let mut deep = WireDatum::Null;
        for _ in 0..super::codec::MAX_DEPTH {
            deep = WireDatum::Array(vec![deep]);
        }
        let bytes = encode_rows(&[vec![deep.clone()]]).unwrap();
        assert_eq!(decode_rows(&bytes).unwrap(), vec![vec![deep.clone()]]);

        let too_deep = WireDatum::Array(vec![deep]);
        assert!(matches!(
            encode_rows(&[vec![too_deep]]),
            Err(CodecError::Encode(_))
        ));

        // Hand-build the same over-deep payload to exercise the decoder's
        // own guard (the encoder refuses to produce it).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&1u64.to_le_bytes());
        for _ in 0..=super::codec::MAX_DEPTH {
            bytes.extend_from_slice(&18u32.to_le_bytes());
            bytes.extend_from_slice(&1u64.to_le_bytes());
        }
        bytes.extend_from_slice(&12u32.to_le_bytes());
        assert!(matches!(decode_rows(&bytes), Err(CodecError::Decode(_))));
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
        let bad = encode_rows(&[vec![WireDatum::Timestamp(0), WireDatum::Null]]).unwrap();
        let diff = Diff { rows: bad, ..diff };
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
