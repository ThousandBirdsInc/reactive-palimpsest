// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! WAL → wire codec shim.
//!
//! The canonical wire codec lives in [`palimpsest_proto::wire`] so
//! transport-agnostic clients (e.g. `palimpsest-client`) can decode
//! diffs without depending on the WAL crate. This module is the thin
//! conversion layer that maps `palimpsest_wal::Datum` onto
//! [`palimpsest_proto::WireDatum`] and reuses the proto encoder for the
//! actual byte payload.

use palimpsest_proto::wire::{self, WireDatum};
use palimpsest_wal::{BigDecimal, Datum};
use thiserror::Error;

use palimpsest_dataflow::palimpsest::Row;

/// Failures producing a wire row from a WAL row.
#[derive(Debug, Error)]
pub enum CodecError {
    /// `Datum::Unchanged` reached the encoder: a TOAST placeholder
    /// that should have been resolved against the mirror upstream.
    #[error("unsupported datum variant for wire format")]
    UnsupportedDatum,
    /// Underlying wire codec failure (encode/decode).
    #[error(transparent)]
    Wire(#[from] wire::CodecError),
}

/// Encodes a row slice into the bincode payload that goes onto
/// `Diff::rows`.
///
/// # Errors
/// * [`CodecError::UnsupportedDatum`] for `Datum::Unchanged` (a TOAST
///   placeholder that must be resolved before encoding).
/// * [`CodecError::Wire`] on bincode failure.
pub fn encode_rows(rows: &[Row]) -> Result<Vec<u8>, CodecError> {
    let mut wire_rows: Vec<wire::WireRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut wire_row: wire::WireRow = Vec::with_capacity(row.len());
        for datum in row {
            wire_row.push(datum_to_wire(datum)?);
        }
        wire_rows.push(wire_row);
    }
    Ok(wire::encode_rows(&wire_rows)?)
}

/// Encodes one row into the same row-list payload used by `Diff::rows`.
///
/// Transaction row changes carry old/new as optional single-row
/// payloads, so this keeps their encoding byte-compatible with the
/// legacy diff codec.
pub fn encode_row(row: &Row) -> Result<Vec<u8>, CodecError> {
    encode_rows(std::slice::from_ref(row))
}

/// Decodes a row payload back into WAL-flavoured rows.
///
/// # Errors
/// Returns [`CodecError::Wire`] on bincode failure.
pub fn decode_rows(bytes: &[u8]) -> Result<Vec<Row>, CodecError> {
    let wire_rows = wire::decode_rows(bytes)?;
    Ok(wire_rows
        .into_iter()
        .map(|wire_row| {
            let mut row = Row::with_capacity(wire_row.len());
            for datum in wire_row {
                row.push(wire_to_datum(&datum));
            }
            row
        })
        .collect())
}

fn datum_to_wire(datum: &Datum) -> Result<WireDatum, CodecError> {
    use palimpsest_wal::{Date, Interval, Time, Timestamp, TimestampTz};
    Ok(match datum {
        Datum::Bool(value) => WireDatum::Bool(*value),
        Datum::I16(value) => WireDatum::I16(*value),
        Datum::I32(value) => WireDatum::I32(*value),
        Datum::I64(value) => WireDatum::I64(*value),
        Datum::F32(bits) => WireDatum::F32(*bits),
        Datum::F64(bits) => WireDatum::F64(*bits),
        Datum::Numeric(value) => WireDatum::Numeric(value.as_str().to_owned()),
        Datum::Text(value) => WireDatum::Text(value.to_vec()),
        Datum::Bytea(value) => WireDatum::Bytea(value.to_vec()),
        Datum::Json(value) => WireDatum::Json(value.to_vec()),
        Datum::Jsonb(value) => WireDatum::Jsonb(value.to_vec()),
        Datum::Uuid(value) => WireDatum::Uuid(value.as_bytes()),
        Datum::Null => WireDatum::Null,
        Datum::Date(Date {
            days_since_unix_epoch,
        }) => WireDatum::Date(*days_since_unix_epoch),
        Datum::Time(Time {
            micros_since_midnight,
        }) => WireDatum::Time(*micros_since_midnight),
        Datum::Timestamp(Timestamp {
            micros_since_unix_epoch,
        }) => WireDatum::Timestamp(*micros_since_unix_epoch),
        Datum::TimestampTz(TimestampTz {
            micros_since_unix_epoch,
        }) => WireDatum::TimestampTz(*micros_since_unix_epoch),
        Datum::Interval(Interval {
            months,
            days,
            micros,
        }) => WireDatum::Interval {
            months: *months,
            days: *days,
            micros: *micros,
        },
        Datum::Array(elements) => WireDatum::Array(
            elements
                .iter()
                .map(datum_to_wire)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        // `Unchanged` is a TOAST placeholder, not a value; it must be
        // resolved against the mirror before rows reach the wire.
        Datum::Unchanged => return Err(CodecError::UnsupportedDatum),
    })
}

fn wire_to_datum(datum: &WireDatum) -> Datum {
    use palimpsest_wal::{Date, Interval, Time, Timestamp, TimestampTz};
    match datum {
        WireDatum::Bool(value) => Datum::Bool(*value),
        WireDatum::I16(value) => Datum::I16(*value),
        WireDatum::I32(value) => Datum::I32(*value),
        WireDatum::I64(value) => Datum::I64(*value),
        WireDatum::F32(bits) => Datum::F32(*bits),
        WireDatum::F64(bits) => Datum::F64(*bits),
        WireDatum::Numeric(value) => Datum::Numeric(BigDecimal::new(value.clone())),
        WireDatum::Text(bytes) => Datum::Text(bytes.clone().into()),
        WireDatum::Bytea(bytes) => Datum::Bytea(bytes.clone().into()),
        WireDatum::Json(bytes) => Datum::Json(bytes.clone().into()),
        WireDatum::Jsonb(bytes) => Datum::Jsonb(bytes.clone().into()),
        WireDatum::Uuid(bytes) => Datum::Uuid(palimpsest_wal::Uuid::from_bytes(*bytes)),
        WireDatum::Null => Datum::Null,
        WireDatum::Date(days) => Datum::Date(Date {
            days_since_unix_epoch: *days,
        }),
        WireDatum::Time(micros) => Datum::Time(Time {
            micros_since_midnight: *micros,
        }),
        WireDatum::Timestamp(micros) => Datum::Timestamp(Timestamp {
            micros_since_unix_epoch: *micros,
        }),
        WireDatum::TimestampTz(micros) => Datum::TimestampTz(TimestampTz {
            micros_since_unix_epoch: *micros,
        }),
        WireDatum::Interval {
            months,
            days,
            micros,
        } => Datum::Interval(Interval {
            months: *months,
            days: *days,
            micros: *micros,
        }),
        WireDatum::Array(elements) => Datum::Array(elements.iter().map(wire_to_datum).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_rows, encode_rows, CodecError};
    use palimpsest_wal::{Datum, Uuid};
    use smallvec::smallvec;

    #[test]
    fn round_trips_numeric_and_text() {
        let row = smallvec![
            Datum::I64(1),
            Datum::Text("hello".into()),
            Datum::Numeric(palimpsest_wal::BigDecimal::new("3.14")),
        ];
        let bytes = encode_rows(std::slice::from_ref(&row)).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], row);
    }

    #[test]
    fn round_trips_uuid_and_null() {
        let uuid = Uuid::from_bytes([1; 16]);
        let row = smallvec![Datum::Uuid(uuid), Datum::Null];
        let bytes = encode_rows(std::slice::from_ref(&row)).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded[0], row);
    }

    #[test]
    fn unsupported_variant_is_reported() {
        let row = smallvec![Datum::Unchanged];
        let err = encode_rows(&[row]).unwrap_err();
        assert!(matches!(err, CodecError::UnsupportedDatum));
    }

    #[test]
    fn float_round_trips_via_bit_pattern() {
        let row = smallvec![
            Datum::F64(f64::to_bits(1.5)),
            Datum::F32(f32::to_bits(0.25))
        ];
        let bytes = encode_rows(std::slice::from_ref(&row)).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded[0], row);
    }

    #[test]
    fn empty_row_list_round_trips() {
        let bytes = encode_rows(&[]).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn from_datum_handles_jsonb() {
        let row = smallvec![Datum::Jsonb(b"{}".to_vec().into())];
        let bytes = encode_rows(std::slice::from_ref(&row)).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded[0], row);
    }

    #[test]
    fn temporal_interval_and_array_datums_round_trip() {
        use palimpsest_wal::{Date, Interval, Time, Timestamp, TimestampTz};
        let row = smallvec![
            Datum::Date(Date {
                days_since_unix_epoch: 19_723
            }),
            Datum::Time(Time {
                micros_since_midnight: 43_200_000_000
            }),
            Datum::Timestamp(Timestamp {
                micros_since_unix_epoch: 1_700_000_000_000_000
            }),
            Datum::TimestampTz(TimestampTz {
                micros_since_unix_epoch: 1_700_000_000_000_000
            }),
            Datum::Interval(Interval {
                months: 1,
                days: 2,
                micros: 3_000_000
            }),
            Datum::Array(vec![
                Datum::Text("a".into()),
                Datum::Null,
                Datum::Array(vec![Datum::I32(7)]),
            ]),
        ];
        let bytes = encode_rows(std::slice::from_ref(&row)).unwrap();
        let decoded = decode_rows(&bytes).unwrap();
        assert_eq!(decoded[0], row);
    }

    #[test]
    fn unchanged_inside_array_is_reported() {
        let row = smallvec![Datum::Array(vec![Datum::Unchanged])];
        let err = encode_rows(&[row]).unwrap_err();
        assert!(matches!(err, CodecError::UnsupportedDatum));
    }
}
