// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use bytes::{Buf, Bytes};

use crate::{Result, WalError};

pub const BOOL_OID: u32 = 16;
pub const BYTEA_OID: u32 = 17;
pub const INT8_OID: u32 = 20;
pub const INT2_OID: u32 = 21;
pub const INT4_OID: u32 = 23;
pub const TEXT_OID: u32 = 25;
pub const FLOAT4_OID: u32 = 700;
pub const FLOAT8_OID: u32 = 701;
pub const UUID_OID: u32 = 2950;
pub const JSON_OID: u32 = 114;
pub const JSONB_OID: u32 = 3802;
pub const DATE_OID: u32 = 1082;
pub const TIME_OID: u32 = 1083;
pub const TIMESTAMP_OID: u32 = 1114;
pub const TIMESTAMPTZ_OID: u32 = 1184;
pub const INTERVAL_OID: u32 = 1186;
pub const NUMERIC_OID: u32 = 1700;

pub const BOOL_ARRAY_OID: u32 = 1000;
pub const BYTEA_ARRAY_OID: u32 = 1001;
pub const INT2_ARRAY_OID: u32 = 1005;
pub const INT4_ARRAY_OID: u32 = 1007;
pub const TEXT_ARRAY_OID: u32 = 1009;
pub const INT8_ARRAY_OID: u32 = 1016;
pub const FLOAT4_ARRAY_OID: u32 = 1021;
pub const FLOAT8_ARRAY_OID: u32 = 1022;
pub const UUID_ARRAY_OID: u32 = 2951;
pub const JSON_ARRAY_OID: u32 = 199;
pub const JSONB_ARRAY_OID: u32 = 3807;
pub const NUMERIC_ARRAY_OID: u32 = 1231;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigDecimal(String);

impl BigDecimal {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    pub days_since_unix_epoch: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time {
    pub micros_since_midnight: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    pub micros_since_unix_epoch: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampTz {
    pub micros_since_unix_epoch: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Uuid([u8; 16]);

impl Uuid {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(u32),
    F64(u64),
    Numeric(BigDecimal),
    Text(Bytes),
    Bytea(Bytes),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    TimestampTz(TimestampTz),
    Interval(Interval),
    Uuid(Uuid),
    Json(Bytes),
    Jsonb(Bytes),
    Array(Vec<Self>),
    Null,
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatumType {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    Numeric,
    Text,
    Bytea,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Json,
    Jsonb,
    Array(Box<Self>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnValue {
    Text(Bytes),
    Binary(Bytes),
    Null,
    Unchanged,
}

#[must_use]
pub fn stock_postgres_16_type(oid: u32) -> Option<DatumType> {
    let datum_type = match oid {
        BOOL_OID => DatumType::Bool,
        BYTEA_OID => DatumType::Bytea,
        INT8_OID => DatumType::I64,
        INT2_OID => DatumType::I16,
        INT4_OID => DatumType::I32,
        TEXT_OID => DatumType::Text,
        FLOAT4_OID => DatumType::F32,
        FLOAT8_OID => DatumType::F64,
        UUID_OID => DatumType::Uuid,
        JSON_OID => DatumType::Json,
        JSONB_OID => DatumType::Jsonb,
        DATE_OID => DatumType::Date,
        TIME_OID => DatumType::Time,
        TIMESTAMP_OID => DatumType::Timestamp,
        TIMESTAMPTZ_OID => DatumType::TimestampTz,
        INTERVAL_OID => DatumType::Interval,
        NUMERIC_OID => DatumType::Numeric,
        BOOL_ARRAY_OID => DatumType::Array(Box::new(DatumType::Bool)),
        BYTEA_ARRAY_OID => DatumType::Array(Box::new(DatumType::Bytea)),
        INT2_ARRAY_OID => DatumType::Array(Box::new(DatumType::I16)),
        INT4_ARRAY_OID => DatumType::Array(Box::new(DatumType::I32)),
        TEXT_ARRAY_OID => DatumType::Array(Box::new(DatumType::Text)),
        INT8_ARRAY_OID => DatumType::Array(Box::new(DatumType::I64)),
        FLOAT4_ARRAY_OID => DatumType::Array(Box::new(DatumType::F32)),
        FLOAT8_ARRAY_OID => DatumType::Array(Box::new(DatumType::F64)),
        UUID_ARRAY_OID => DatumType::Array(Box::new(DatumType::Uuid)),
        JSON_ARRAY_OID => DatumType::Array(Box::new(DatumType::Json)),
        JSONB_ARRAY_OID => DatumType::Array(Box::new(DatumType::Jsonb)),
        NUMERIC_ARRAY_OID => DatumType::Array(Box::new(DatumType::Numeric)),
        _ => return None,
    };

    Some(datum_type)
}

pub fn decode_column_value(datum_type: &DatumType, value: ColumnValue) -> Result<Datum> {
    match value {
        ColumnValue::Null => Ok(Datum::Null),
        ColumnValue::Unchanged => Ok(Datum::Unchanged),
        ColumnValue::Text(bytes) => decode_text(datum_type, &bytes),
        ColumnValue::Binary(bytes) => decode_binary(datum_type, bytes),
    }
}

fn decode_text(datum_type: &DatumType, bytes: &[u8]) -> Result<Datum> {
    let text = std::str::from_utf8(bytes)?;
    match datum_type {
        DatumType::Bool => parse_bool(text).map(Datum::Bool),
        DatumType::I16 => text.parse().map(Datum::I16).map_err(WalError::from),
        DatumType::I32 => text.parse().map(Datum::I32).map_err(WalError::from),
        DatumType::I64 => text.parse().map(Datum::I64).map_err(WalError::from),
        DatumType::F32 => parse_f32_bits(text).map(Datum::F32),
        DatumType::F64 => parse_f64_bits(text).map(Datum::F64),
        DatumType::Numeric => Ok(Datum::Numeric(BigDecimal::new(text))),
        DatumType::Text => Ok(Datum::Text(Bytes::copy_from_slice(bytes))),
        DatumType::Bytea => parse_text_bytea(text).map(Datum::Bytea),
        DatumType::Date => parse_date(text).map(Datum::Date),
        DatumType::Time => parse_time(text).map(Datum::Time),
        DatumType::Timestamp => parse_timestamp(text).map(Datum::Timestamp),
        DatumType::TimestampTz => parse_timestamp_tz(text).map(Datum::TimestampTz),
        DatumType::Interval => parse_interval(text).map(Datum::Interval),
        DatumType::Uuid => parse_uuid(text).map(Datum::Uuid),
        DatumType::Json => Ok(Datum::Json(Bytes::copy_from_slice(bytes))),
        DatumType::Jsonb => Ok(Datum::Jsonb(Bytes::copy_from_slice(bytes))),
        DatumType::Array(inner) => parse_array(inner, text).map(Datum::Array),
    }
}

fn decode_binary(datum_type: &DatumType, mut bytes: Bytes) -> Result<Datum> {
    match datum_type {
        DatumType::Bool if bytes.len() == 1 => Ok(Datum::Bool(bytes[0] != 0)),
        DatumType::I16 if bytes.len() == 2 => Ok(Datum::I16(bytes.get_i16())),
        DatumType::I32 if bytes.len() == 4 => Ok(Datum::I32(bytes.get_i32())),
        DatumType::I64 if bytes.len() == 8 => Ok(Datum::I64(bytes.get_i64())),
        DatumType::F32 if bytes.len() == 4 => Ok(Datum::F32(bytes.get_u32())),
        DatumType::F64 if bytes.len() == 8 => Ok(Datum::F64(bytes.get_u64())),
        DatumType::Date if bytes.len() == 4 => Ok(Datum::Date(Date {
            days_since_unix_epoch: bytes.get_i32() + 10_957,
        })),
        DatumType::Time if bytes.len() == 8 => Ok(Datum::Time(Time {
            micros_since_midnight: bytes.get_i64(),
        })),
        DatumType::Timestamp if bytes.len() == 8 => Ok(Datum::Timestamp(Timestamp {
            micros_since_unix_epoch: bytes.get_i64() + 946_684_800_000_000,
        })),
        DatumType::TimestampTz if bytes.len() == 8 => Ok(Datum::TimestampTz(TimestampTz {
            micros_since_unix_epoch: bytes.get_i64() + 946_684_800_000_000,
        })),
        DatumType::Interval if bytes.len() == 16 => Ok(Datum::Interval(Interval {
            micros: bytes.get_i64(),
            days: bytes.get_i32(),
            months: bytes.get_i32(),
        })),
        DatumType::Uuid if bytes.len() == 16 => {
            let mut uuid = [0; 16];
            uuid.copy_from_slice(&bytes);
            Ok(Datum::Uuid(Uuid::from_bytes(uuid)))
        }
        DatumType::Text => Ok(Datum::Text(bytes)),
        DatumType::Bytea => Ok(Datum::Bytea(bytes)),
        DatumType::Json => Ok(Datum::Json(bytes)),
        DatumType::Jsonb => {
            if bytes.first().copied() == Some(1) {
                Ok(Datum::Jsonb(bytes.slice(1..)))
            } else {
                Err(WalError::Binary(datum_type.clone()))
            }
        }
        DatumType::Numeric | DatumType::Array(_) => Err(WalError::Decode {
            datum_type: datum_type.clone(),
            format: "binary",
        }),
        _ => Err(WalError::Binary(datum_type.clone())),
    }
}

fn parse_bool(text: &str) -> Result<bool> {
    match text {
        "t" | "true" | "1" => Ok(true),
        "f" | "false" | "0" => Ok(false),
        _ => Err(WalError::Bool(text.to_owned())),
    }
}

fn parse_f32_bits(text: &str) -> Result<u32> {
    text.parse::<f32>()
        .map(f32::to_bits)
        .map_err(|_| WalError::Decode {
            datum_type: DatumType::F32,
            format: "text",
        })
}

fn parse_f64_bits(text: &str) -> Result<u64> {
    text.parse::<f64>()
        .map(f64::to_bits)
        .map_err(|_| WalError::Decode {
            datum_type: DatumType::F64,
            format: "text",
        })
}

fn parse_text_bytea(text: &str) -> Result<Bytes> {
    let Some(hex) = text.strip_prefix(r"\x") else {
        return Ok(Bytes::copy_from_slice(text.as_bytes()));
    };

    if hex.len() % 2 != 0 {
        return Err(WalError::Binary(DatumType::Bytea));
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for index in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[index..index + 2], 16)
            .map_err(|_| WalError::Binary(DatumType::Bytea))?;
        bytes.push(byte);
    }
    Ok(Bytes::from(bytes))
}

fn parse_date(text: &str) -> Result<Date> {
    let (year, month, day) = parse_ymd(text).ok_or_else(|| WalError::Date(text.to_owned()))?;
    Ok(Date {
        days_since_unix_epoch: days_from_civil(year, month, day),
    })
}

fn parse_time(text: &str) -> Result<Time> {
    Ok(Time {
        micros_since_midnight: parse_hms_micros(text)
            .ok_or_else(|| WalError::Time(text.to_owned()))?,
    })
}

fn parse_timestamp(text: &str) -> Result<Timestamp> {
    let (date, time) = text
        .split_once(' ')
        .or_else(|| text.split_once('T'))
        .ok_or_else(|| WalError::Timestamp(text.to_owned()))?;
    let date = parse_date(date)?;
    let time = parse_time(time)?;
    Ok(Timestamp {
        micros_since_unix_epoch: i64::from(date.days_since_unix_epoch) * 86_400_000_000
            + time.micros_since_midnight,
    })
}

fn parse_timestamp_tz(text: &str) -> Result<TimestampTz> {
    let trimmed = text.trim_end_matches("+00").trim_end_matches('Z');
    parse_timestamp(trimmed).map(|timestamp| TimestampTz {
        micros_since_unix_epoch: timestamp.micros_since_unix_epoch,
    })
}

fn parse_interval(text: &str) -> Result<Interval> {
    let mut micros = 0_i64;
    let mut parts = text.split_whitespace();
    while let Some(part) = parts.next() {
        if let Ok(value) = part.parse::<i64>() {
            match parts.next() {
                Some(unit) if unit.starts_with("day") => micros += value * 86_400_000_000,
                Some(unit) if unit.starts_with("hour") => micros += value * 3_600_000_000,
                Some(unit) if unit.starts_with("min") => micros += value * 60_000_000,
                Some(unit) if unit.starts_with("sec") => micros += value * 1_000_000,
                _ => return Err(WalError::Interval(text.to_owned())),
            }
        } else if part.contains(':') {
            micros += parse_hms_micros(part).ok_or_else(|| WalError::Interval(text.to_owned()))?;
        } else {
            return Err(WalError::Interval(text.to_owned()));
        }
    }

    Ok(Interval {
        months: 0,
        days: 0,
        micros,
    })
}

fn parse_uuid(text: &str) -> Result<Uuid> {
    let mut compact = String::with_capacity(32);
    for ch in text.chars() {
        if ch != '-' {
            compact.push(ch);
        }
    }
    if compact.len() != 32 {
        return Err(WalError::Uuid(text.to_owned()));
    }

    let mut bytes = [0; 16];
    for index in 0..16 {
        bytes[index] = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16)
            .map_err(|_| WalError::Uuid(text.to_owned()))?;
    }
    Ok(Uuid::from_bytes(bytes))
}

fn parse_array(inner: &DatumType, text: &str) -> Result<Vec<Datum>> {
    let body = text
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .ok_or_else(|| WalError::Decode {
            datum_type: DatumType::Array(Box::new(inner.clone())),
            format: "text",
        })?;

    if body.is_empty() {
        return Ok(Vec::new());
    }

    body.split(',')
        .map(|item| decode_text(inner, item.trim_matches('"').as_bytes()))
        .collect()
}

fn parse_ymd(text: &str) -> Option<(i32, u32, u32)> {
    let mut parts = text.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse().ok()?;
    let day = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((year, month, day))
}

fn parse_hms_micros(text: &str) -> Option<i64> {
    let mut parts = text.split(':');
    let hours = parts.next()?.parse::<i64>().ok()?;
    let minutes = parts.next()?.parse::<i64>().ok()?;
    let seconds_part = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let (seconds, micros) = match seconds_part.split_once('.') {
        Some((seconds, fraction)) => {
            let mut fraction = fraction.as_bytes().to_vec();
            fraction.resize(6, b'0');
            let micros = std::str::from_utf8(&fraction[..6])
                .ok()?
                .parse::<i64>()
                .ok()?;
            (seconds.parse::<i64>().ok()?, micros)
        }
        None => (seconds_part.parse::<i64>().ok()?, 0),
    };

    Some(((hours * 60 + minutes) * 60 + seconds) * 1_000_000 + micros)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i32::try_from(month).expect("month fits i32");
    let day = i32::try_from(day).expect("day fits i32");
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use super::{
        decode_column_value, stock_postgres_16_type, ColumnValue, Date, Datum, DatumType, Time,
        BOOL_OID, INT4_OID, JSONB_OID, TEXT_ARRAY_OID, UUID_OID,
    };

    #[test]
    fn maps_stock_postgres_oids() {
        assert_eq!(stock_postgres_16_type(BOOL_OID), Some(DatumType::Bool));
        assert_eq!(stock_postgres_16_type(INT4_OID), Some(DatumType::I32));
        assert_eq!(stock_postgres_16_type(UUID_OID), Some(DatumType::Uuid));
        assert_eq!(
            stock_postgres_16_type(TEXT_ARRAY_OID),
            Some(DatumType::Array(Box::new(DatumType::Text)))
        );
        assert_eq!(stock_postgres_16_type(999_999), None);
    }

    #[test]
    fn decodes_text_values() {
        assert_eq!(
            decode_column_value(
                &DatumType::Bool,
                ColumnValue::Text(Bytes::from_static(b"t"))
            )
            .unwrap(),
            Datum::Bool(true)
        );
        assert_eq!(
            decode_column_value(
                &DatumType::I32,
                ColumnValue::Text(Bytes::from_static(b"42"))
            )
            .unwrap(),
            Datum::I32(42)
        );
        assert_eq!(
            decode_column_value(
                &DatumType::Bytea,
                ColumnValue::Text(Bytes::from_static(br"\x0001ff"))
            )
            .unwrap(),
            Datum::Bytea(Bytes::from_static(&[0, 1, 255]))
        );
    }

    #[test]
    fn decodes_temporal_text_values() {
        assert_eq!(
            decode_column_value(
                &DatumType::Date,
                ColumnValue::Text(Bytes::from_static(b"1970-01-02"))
            )
            .unwrap(),
            Datum::Date(Date {
                days_since_unix_epoch: 1
            })
        );
        assert_eq!(
            decode_column_value(
                &DatumType::Time,
                ColumnValue::Text(Bytes::from_static(b"00:00:01.000002"))
            )
            .unwrap(),
            Datum::Time(Time {
                micros_since_midnight: 1_000_002
            })
        );
    }

    #[test]
    fn decodes_binary_values() {
        let mut int = BytesMut::new();
        int.put_i32(42);
        assert_eq!(
            decode_column_value(&DatumType::I32, ColumnValue::Binary(int.freeze())).unwrap(),
            Datum::I32(42)
        );

        let jsonb = Bytes::from_static(&[1, b'{', b'}']);
        assert_eq!(
            decode_column_value(&DatumType::Jsonb, ColumnValue::Binary(jsonb)).unwrap(),
            Datum::Jsonb(Bytes::from_static(b"{}"))
        );
    }

    #[test]
    fn preserves_null_and_unchanged_markers() {
        assert_eq!(
            decode_column_value(&DatumType::Text, ColumnValue::Null).unwrap(),
            Datum::Null
        );
        assert_eq!(
            decode_column_value(&DatumType::Text, ColumnValue::Unchanged).unwrap(),
            Datum::Unchanged
        );
    }

    #[test]
    fn decodes_text_arrays() {
        assert_eq!(
            decode_column_value(
                &DatumType::Array(Box::new(DatumType::I32)),
                ColumnValue::Text(Bytes::from_static(b"{1,2,3}"))
            )
            .unwrap(),
            Datum::Array(vec![Datum::I32(1), Datum::I32(2), Datum::I32(3)])
        );
    }

    #[test]
    fn rejects_bad_jsonb_binary_version() {
        assert!(decode_column_value(
            &DatumType::Jsonb,
            ColumnValue::Binary(Bytes::from_static(&[2, b'{', b'}']))
        )
        .is_err());
        assert_eq!(stock_postgres_16_type(JSONB_OID), Some(DatumType::Jsonb));
    }
}
