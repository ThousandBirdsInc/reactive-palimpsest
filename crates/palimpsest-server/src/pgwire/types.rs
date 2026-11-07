// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Mapping between the engine's datums and Postgres wire types.
//!
//! Result columns are encoded by the *declared* column type (the one
//! the `RowDescription` advertised), coercing the datum into it where
//! the engine's representation is narrower (`int4` rows under an
//! `int8` column, `float4` under `float8`). Parameters are decoded by
//! the OID the client declared, falling back to the type the query
//! registry inferred from the column each placeholder is compared
//! against.

use bytes::{Buf, BufMut};
use palimpsest_sql::prepared::{ParamSpec, ParamValue};
use palimpsest_sql::ColumnType;
use palimpsest_wal::{Datum, DatumType};

// Type OIDs from `pg_type`.
pub(super) const BOOL: u32 = 16;
pub(super) const BYTEA: u32 = 17;
pub(super) const INT8: u32 = 20;
pub(super) const INT2: u32 = 21;
pub(super) const INT4: u32 = 23;
pub(super) const TEXT: u32 = 25;
pub(super) const JSON: u32 = 114;
pub(super) const FLOAT4: u32 = 700;
pub(super) const FLOAT8: u32 = 701;
pub(super) const UNKNOWN: u32 = 705;
pub(super) const DATE: u32 = 1082;
pub(super) const TIME: u32 = 1083;
pub(super) const TIMESTAMP: u32 = 1114;
pub(super) const TIMESTAMPTZ: u32 = 1184;
pub(super) const INTERVAL: u32 = 1186;
pub(super) const NUMERIC: u32 = 1700;
pub(super) const UUID: u32 = 2950;
pub(super) const JSONB: u32 = 3802;

/// Days from 1970-01-01 to 2000-01-01 (the Postgres binary epoch).
const PG_EPOCH_DAYS: i32 = 10_957;
/// Microseconds from 1970-01-01 to 2000-01-01.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Postgres array type OID for an element type OID.
pub(super) const fn array_oid(element: u32) -> u32 {
    match element {
        BOOL => 1000,
        BYTEA => 1001,
        INT8 => 1016,
        INT2 => 1005,
        INT4 => 1007,
        JSON => 199,
        FLOAT4 => 1021,
        FLOAT8 => 1022,
        DATE => 1182,
        TIME => 1183,
        TIMESTAMP => 1115,
        TIMESTAMPTZ => 1185,
        INTERVAL => 1187,
        NUMERIC => 1231,
        UUID => 2951,
        JSONB => 3807,
        _ => 1009, // text[]
    }
}

/// Element OID for an array type OID, if `oid` is one.
const fn array_element_oid(oid: u32) -> Option<u32> {
    Some(match oid {
        1000 => BOOL,
        1001 => BYTEA,
        1016 => INT8,
        1005 => INT2,
        1007 => INT4,
        199 => JSON,
        1021 => FLOAT4,
        1022 => FLOAT8,
        1182 => DATE,
        1183 => TIME,
        1115 => TIMESTAMP,
        1185 => TIMESTAMPTZ,
        1187 => INTERVAL,
        1231 => NUMERIC,
        2951 => UUID,
        3807 => JSONB,
        1009 | 1015 | 1014 => TEXT,
        _ => return None,
    })
}

/// `(type oid, typlen)` for a declared column type.
pub(super) fn wire_type(ty: &DatumType) -> (u32, i16) {
    match ty {
        DatumType::Bool => (BOOL, 1),
        DatumType::I16 => (INT2, 2),
        DatumType::I32 => (INT4, 4),
        DatumType::I64 => (INT8, 8),
        DatumType::F32 => (FLOAT4, 4),
        DatumType::F64 => (FLOAT8, 8),
        DatumType::Numeric => (NUMERIC, -1),
        DatumType::Text => (TEXT, -1),
        DatumType::Bytea => (BYTEA, -1),
        DatumType::Date => (DATE, 4),
        DatumType::Time => (TIME, 8),
        DatumType::Timestamp => (TIMESTAMP, 8),
        DatumType::TimestampTz => (TIMESTAMPTZ, 8),
        DatumType::Interval => (INTERVAL, 16),
        DatumType::Uuid => (UUID, 16),
        DatumType::Json => (JSON, -1),
        DatumType::Jsonb => (JSONB, -1),
        DatumType::Array(inner) => (array_oid(wire_type(inner).0), -1),
    }
}

/// Type OID advertised in `ParameterDescription` for an inferred
/// parameter.
pub(super) const fn param_oid(spec: &ParamSpec) -> u32 {
    let element = match spec.ty {
        ColumnType::Bool => BOOL,
        ColumnType::Int => INT8,
        ColumnType::Float => FLOAT8,
        ColumnType::Numeric => NUMERIC,
        ColumnType::Uuid => UUID,
        ColumnType::Date => DATE,
        ColumnType::Time => TIME,
        ColumnType::Timestamp => TIMESTAMP,
        ColumnType::TimestampTz => TIMESTAMPTZ,
        ColumnType::Interval => INTERVAL,
        ColumnType::Jsonb => JSONB,
        ColumnType::Bytea => BYTEA,
        ColumnType::Text | ColumnType::Enum | ColumnType::Array | ColumnType::Unknown => TEXT,
    };
    if spec.list {
        array_oid(element)
    } else {
        element
    }
}

// ---------------------------------------------------------------------
// Result encoding
// ---------------------------------------------------------------------

/// Encodes one result column; `None` is SQL NULL.
pub(super) fn encode_column(datum: &Datum, declared: &DatumType, binary: bool) -> Option<Vec<u8>> {
    if matches!(datum, Datum::Null | Datum::Unchanged) {
        return None;
    }
    Some(if binary {
        encode_binary(datum, declared)
    } else {
        encode_text(datum, declared).into_bytes()
    })
}

/// Text (`format = 0`) rendering, as `psql` would print it.
pub(super) fn encode_text(datum: &Datum, declared: &DatumType) -> String {
    match (declared, datum) {
        (DatumType::Bool, Datum::Bool(b)) => (if *b { "t" } else { "f" }).to_owned(),
        (DatumType::Array(inner), Datum::Array(items)) => {
            let mut out = String::from("{");
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                if matches!(item, Datum::Null | Datum::Unchanged) {
                    out.push_str("NULL");
                } else {
                    push_array_element(&mut out, &encode_text(item, inner));
                }
            }
            out.push('}');
            out
        }
        (_, Datum::Bool(b)) => (if *b { "t" } else { "f" }).to_owned(),
        (_, Datum::I16(v)) => v.to_string(),
        (_, Datum::I32(v)) => v.to_string(),
        (_, Datum::I64(v)) => v.to_string(),
        (_, Datum::F32(bits)) => float_text(f64::from(f32::from_bits(*bits))),
        (_, Datum::F64(bits)) => float_text(f64::from_bits(*bits)),
        (_, Datum::Numeric(n)) => n.as_str().to_owned(),
        (_, Datum::Text(bytes) | Datum::Json(bytes) | Datum::Jsonb(bytes)) => {
            String::from_utf8_lossy(bytes).into_owned()
        }
        (_, Datum::Bytea(bytes)) => {
            let mut out = String::with_capacity(2 + bytes.len() * 2);
            out.push_str("\\x");
            for byte in bytes {
                out.push_str(&format!("{byte:02x}"));
            }
            out
        }
        (_, Datum::Date(d)) => date_text(d.days_since_unix_epoch),
        (_, Datum::Time(t)) => clock_text(t.micros_since_midnight),
        (_, Datum::Timestamp(ts)) => timestamp_text(ts.micros_since_unix_epoch, false),
        (_, Datum::TimestampTz(ts)) => timestamp_text(ts.micros_since_unix_epoch, true),
        (_, Datum::Interval(iv)) => interval_text(iv.months, iv.days, iv.micros),
        (_, Datum::Uuid(u)) => u.to_string(),
        (_, Datum::Array(items)) => {
            // Declared scalar but the datum is an array: render the
            // elements by their own types.
            let mut out = String::from("{");
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                if matches!(item, Datum::Null | Datum::Unchanged) {
                    out.push_str("NULL");
                } else {
                    push_array_element(&mut out, &encode_text(item, &self_type(item)));
                }
            }
            out.push('}');
            out
        }
        (_, Datum::Null | Datum::Unchanged) => String::new(),
    }
}

/// Quotes an array element the way Postgres' array output does.
fn push_array_element(out: &mut String, text: &str) {
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("null")
        || text
            .chars()
            .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_whitespace());
    if !needs_quotes {
        out.push_str(text);
        return;
    }
    out.push('"');
    for c in text.chars() {
        if matches!(c, '"' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}

/// The declared type a datum would carry if the column were typed
/// exactly like it.
fn self_type(datum: &Datum) -> DatumType {
    match datum {
        Datum::Bool(_) => DatumType::Bool,
        Datum::I16(_) => DatumType::I16,
        Datum::I32(_) => DatumType::I32,
        Datum::I64(_) => DatumType::I64,
        Datum::F32(_) => DatumType::F32,
        Datum::F64(_) => DatumType::F64,
        Datum::Numeric(_) => DatumType::Numeric,
        Datum::Bytea(_) => DatumType::Bytea,
        Datum::Date(_) => DatumType::Date,
        Datum::Time(_) => DatumType::Time,
        Datum::Timestamp(_) => DatumType::Timestamp,
        Datum::TimestampTz(_) => DatumType::TimestampTz,
        Datum::Interval(_) => DatumType::Interval,
        Datum::Uuid(_) => DatumType::Uuid,
        Datum::Json(_) => DatumType::Json,
        Datum::Jsonb(_) => DatumType::Jsonb,
        Datum::Array(items) => {
            DatumType::Array(Box::new(items.first().map_or(DatumType::Text, self_type)))
        }
        Datum::Text(_) | Datum::Null | Datum::Unchanged => DatumType::Text,
    }
}

fn float_text(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() {
        (if value > 0.0 { "Infinity" } else { "-Infinity" }).to_owned()
    } else {
        value.to_string()
    }
}

/// Binary (`format = 1`) encoding for the declared type. Values that
/// cannot be coerced into the declared type fall back to the text
/// rendering of the datum, which is what a client decoding `text`
/// expects and the least surprising thing for everything else.
fn encode_binary(datum: &Datum, declared: &DatumType) -> Vec<u8> {
    let mut out = Vec::new();
    match declared {
        DatumType::Bool => match datum {
            Datum::Bool(b) => out.put_u8(u8::from(*b)),
            _ => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::I16 => match datum_as_i64(datum).and_then(|v| i16::try_from(v).ok()) {
            Some(v) => out.put_i16(v),
            None => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::I32 => match datum_as_i64(datum).and_then(|v| i32::try_from(v).ok()) {
            Some(v) => out.put_i32(v),
            None => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::I64 => match datum_as_i64(datum) {
            Some(v) => out.put_i64(v),
            None => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::F32 => match datum_as_f64(datum) {
            Some(v) => out.put_f32(v as f32),
            None => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::F64 => match datum_as_f64(datum) {
            Some(v) => out.put_f64(v),
            None => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::Numeric => {
            let text = encode_text(datum, declared);
            match numeric_binary(&text) {
                Some(bytes) => return bytes,
                None => return text.into_bytes(),
            }
        }
        DatumType::Text => return encode_text(datum, declared).into_bytes(),
        DatumType::Bytea => match datum {
            Datum::Bytea(bytes) | Datum::Text(bytes) => out.put_slice(bytes),
            _ => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::Date => match (datum, datum_micros(datum)) {
            (Datum::Date(d), _) => out.put_i32(d.days_since_unix_epoch.wrapping_sub(PG_EPOCH_DAYS)),
            (_, Some(micros)) => {
                let days = i32::try_from(micros.div_euclid(86_400_000_000)).unwrap_or(i32::MAX);
                out.put_i32(days.wrapping_sub(PG_EPOCH_DAYS));
            }
            _ => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::Time => match datum {
            Datum::Time(t) => out.put_i64(t.micros_since_midnight),
            _ => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::Timestamp | DatumType::TimestampTz => match datum_micros(datum) {
            Some(micros) => out.put_i64(micros.wrapping_sub(PG_EPOCH_MICROS)),
            None => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::Interval => match datum {
            Datum::Interval(iv) => {
                out.put_i64(iv.micros);
                out.put_i32(iv.days);
                out.put_i32(iv.months);
            }
            _ => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::Uuid => match datum {
            Datum::Uuid(u) => out.put_slice(&u.as_bytes()),
            Datum::Text(bytes) => match std::str::from_utf8(bytes)
                .ok()
                .and_then(palimpsest_wal::Uuid::parse_text)
            {
                Some(u) => out.put_slice(&u.as_bytes()),
                None => return encode_text(datum, declared).into_bytes(),
            },
            _ => return encode_text(datum, declared).into_bytes(),
        },
        DatumType::Json => return encode_text(datum, declared).into_bytes(),
        DatumType::Jsonb => {
            out.put_u8(1);
            out.extend_from_slice(encode_text(datum, declared).as_bytes());
        }
        DatumType::Array(inner) => match datum {
            Datum::Array(items) => {
                let (element_oid, _) = wire_type(inner);
                let has_null = items
                    .iter()
                    .any(|item| matches!(item, Datum::Null | Datum::Unchanged));
                if items.is_empty() {
                    out.put_i32(0); // ndims
                    out.put_i32(0); // has null
                    out.put_u32(element_oid);
                    return out;
                }
                out.put_i32(1);
                out.put_i32(i32::from(has_null));
                out.put_u32(element_oid);
                out.put_i32(i32::try_from(items.len()).unwrap_or(i32::MAX));
                out.put_i32(1); // lower bound
                for item in items {
                    match encode_column(item, inner, true) {
                        None => out.put_i32(-1),
                        Some(bytes) => {
                            out.put_i32(i32::try_from(bytes.len()).unwrap_or(i32::MAX));
                            out.put_slice(&bytes);
                        }
                    }
                }
            }
            _ => return encode_text(datum, declared).into_bytes(),
        },
    }
    out
}

/// Microseconds since the Unix epoch for any temporal datum that has
/// an absolute instant.
const fn datum_micros(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::Timestamp(ts) => Some(ts.micros_since_unix_epoch),
        Datum::TimestampTz(ts) => Some(ts.micros_since_unix_epoch),
        Datum::Date(d) => Some(d.days_since_unix_epoch as i64 * 86_400_000_000),
        _ => None,
    }
}

const fn datum_as_i64(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::I16(v) => Some(*v as i64),
        Datum::I32(v) => Some(*v as i64),
        Datum::I64(v) => Some(*v),
        Datum::Bool(b) => Some(*b as i64),
        _ => None,
    }
}

fn datum_as_f64(datum: &Datum) -> Option<f64> {
    match datum {
        Datum::F32(bits) => Some(f64::from(f32::from_bits(*bits))),
        Datum::F64(bits) => Some(f64::from_bits(*bits)),
        Datum::I16(v) => Some(f64::from(*v)),
        Datum::I32(v) => Some(f64::from(*v)),
        // Lossy above 2^53 by design: float8 output of a bigint column.
        #[allow(clippy::cast_precision_loss)]
        Datum::I64(v) => Some(*v as f64),
        Datum::Numeric(n) => n.as_str().trim().parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------
// Temporal text rendering
// ---------------------------------------------------------------------

/// Proleptic Gregorian `(year, month, day)` for days since 1970-01-01
/// (Howard Hinnant's `civil_from_days`).
const fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub(super) fn date_text(days: i32) -> String {
    let (y, m, d) = civil_from_days(i64::from(days));
    if y <= 0 {
        format!("{:04}-{m:02}-{d:02} BC", 1 - y)
    } else {
        format!("{y:04}-{m:02}-{d:02}")
    }
}

fn clock_text(micros_of_day: i64) -> String {
    let secs = micros_of_day.div_euclid(1_000_000);
    let frac = micros_of_day.rem_euclid(1_000_000);
    let mut out = format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    );
    if frac != 0 {
        let digits = format!("{frac:06}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
    out
}

pub(super) fn timestamp_text(micros_since_unix_epoch: i64, with_zone: bool) -> String {
    let days = micros_since_unix_epoch.div_euclid(86_400_000_000);
    let of_day = micros_since_unix_epoch.rem_euclid(86_400_000_000);
    let (y, m, d) = civil_from_days(days);
    let mut out = if y <= 0 {
        format!("{:04}-{m:02}-{d:02} {}", 1 - y, clock_text(of_day))
    } else {
        format!("{y:04}-{m:02}-{d:02} {}", clock_text(of_day))
    };
    if with_zone {
        out.push_str("+00");
    }
    if y <= 0 {
        out.push_str(" BC");
    }
    out
}

pub(super) fn interval_text(months: i32, days: i32, micros: i64) -> String {
    let mut parts: Vec<String> = Vec::new();
    let years = months / 12;
    let months = months % 12;
    if years != 0 {
        parts.push(format!(
            "{years} year{}",
            if years.abs() == 1 { "" } else { "s" }
        ));
    }
    if months != 0 {
        parts.push(format!(
            "{months} mon{}",
            if months.abs() == 1 { "" } else { "s" }
        ));
    }
    if days != 0 {
        parts.push(format!(
            "{days} day{}",
            if days.abs() == 1 { "" } else { "s" }
        ));
    }
    if micros != 0 || parts.is_empty() {
        let sign = if micros < 0 { "-" } else { "" };
        parts.push(format!("{sign}{}", clock_text(micros.abs())));
    }
    parts.join(" ")
}

// ---------------------------------------------------------------------
// Numeric binary format
// ---------------------------------------------------------------------

/// Encodes a decimal string in Postgres' binary `numeric` layout:
/// `ndigits`, `weight`, `sign`, `dscale`, then base-10000 digits.
pub(super) fn numeric_binary(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("nan") {
        let mut out = Vec::with_capacity(8);
        out.put_i16(0);
        out.put_i16(0);
        out.put_u16(0xC000);
        out.put_u16(0);
        return Some(out);
    }
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let plain = expand_scientific(unsigned)?;
    let (int_part, frac_part) = plain.split_once('.').unwrap_or((&plain, ""));
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let int_part = int_part.trim_start_matches('0');
    let dscale = u16::try_from(frac_part.len()).ok()?;

    // Base-10000 groups: integer part padded on the left, fraction
    // padded on the right.
    let mut digits: Vec<i16> = Vec::new();
    let int_pad = (4 - int_part.len() % 4) % 4;
    let padded_int = format!("{}{}", "0".repeat(int_pad), int_part);
    for chunk in padded_int.as_bytes().chunks(4) {
        digits.push(std::str::from_utf8(chunk).ok()?.parse().ok()?);
    }
    let int_groups = digits.len();
    let frac_pad = (4 - frac_part.len() % 4) % 4;
    let padded_frac = format!("{}{}", frac_part, "0".repeat(frac_pad));
    for chunk in padded_frac.as_bytes().chunks(4) {
        digits.push(std::str::from_utf8(chunk).ok()?.parse().ok()?);
    }

    // Weight is the base-10000 exponent of the first digit group.
    let mut weight = i64::try_from(int_groups).ok()? - 1;
    // Strip leading zero groups (only possible when the integer part
    // is empty) and trailing zero groups.
    while digits.first() == Some(&0) && digits.len() > 1 {
        digits.remove(0);
        weight -= 1;
    }
    while digits.last() == Some(&0) && digits.len() > 1 {
        digits.pop();
    }
    if digits == [0] {
        digits.clear();
        weight = 0;
    }

    let mut out = Vec::with_capacity(8 + digits.len() * 2);
    out.put_i16(i16::try_from(digits.len()).ok()?);
    out.put_i16(i16::try_from(weight).ok()?);
    out.put_u16(if negative { 0x4000 } else { 0 });
    out.put_u16(dscale);
    for digit in digits {
        out.put_i16(digit);
    }
    Some(out)
}

/// Rewrites `1.5E+3`-style input as a plain decimal string.
fn expand_scientific(text: &str) -> Option<String> {
    let Some((mantissa, exponent)) = text.split_once(['e', 'E']) else {
        return Some(text.to_owned());
    };
    let exponent: i32 = exponent.parse().ok()?;
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits = format!("{int_part}{frac_part}");
    let mut point = i64::try_from(int_part.len()).ok()? + i64::from(exponent);
    while point < 0 {
        digits.insert(0, '0');
        point += 1;
    }
    let point = usize::try_from(point).ok()?;
    while digits.len() < point {
        digits.push('0');
    }
    let (int_part, frac_part) = digits.split_at(point);
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    if frac_part.is_empty() {
        Some(int_part.to_owned())
    } else {
        Some(format!("{int_part}.{frac_part}"))
    }
}

/// Decodes Postgres' binary `numeric` layout back into decimal text.
fn numeric_text_from_binary(mut bytes: &[u8]) -> Option<String> {
    if bytes.len() < 8 {
        return None;
    }
    let ndigits = usize::try_from(bytes.get_i16()).ok()?;
    let weight = i64::from(bytes.get_i16());
    let sign = bytes.get_u16();
    let dscale = usize::from(bytes.get_u16());
    if sign == 0xC000 {
        return Some("NaN".to_owned());
    }
    if bytes.len() < ndigits * 2 {
        return None;
    }
    let digits: Vec<i16> = (0..ndigits).map(|_| bytes.get_i16()).collect();
    // Build the integer digit string: groups at positions 0..=weight
    // are integer groups, the rest are fractional.
    let mut integer = String::new();
    let mut fraction = String::new();
    for (index, group) in digits.iter().enumerate() {
        let position = weight - i64::try_from(index).ok()?;
        if position >= 0 {
            if integer.is_empty() {
                integer.push_str(&group.to_string());
            } else {
                integer.push_str(&format!("{group:04}"));
            }
        } else {
            fraction.push_str(&format!("{group:04}"));
        }
    }
    // Missing trailing integer groups are zeros.
    let present_int_groups = digits
        .iter()
        .enumerate()
        .filter(|(index, _)| weight - i64::try_from(*index).unwrap_or(i64::MAX) >= 0)
        .count();
    if weight >= 0 {
        let expected = usize::try_from(weight + 1).ok()?;
        for _ in present_int_groups..expected {
            integer.push_str("0000");
        }
    }
    // Leading fractional zero groups for weight < -1.
    if weight < -1 {
        let missing = usize::try_from(-weight - 1).ok()?;
        fraction.insert_str(0, &"0000".repeat(missing));
    }
    if integer.is_empty() {
        integer.push('0');
    }
    fraction.truncate(dscale.max(fraction.trim_end_matches('0').len()));
    while fraction.len() < dscale {
        fraction.push('0');
    }
    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }
    out.push_str(&integer);
    if !fraction.is_empty() {
        out.push('.');
        out.push_str(&fraction);
    }
    Some(out)
}

// ---------------------------------------------------------------------
// Parameter decoding
// ---------------------------------------------------------------------

/// Decodes one bound parameter into the registry's value type.
///
/// `declared_oid` is what the client sent in `Parse` (`0` = infer);
/// the inferred `spec` type is the fallback. Decoding by the client's
/// declared type matters because that is the type it encoded with.
pub(super) fn decode_param(
    raw: Option<&[u8]>,
    binary: bool,
    declared_oid: u32,
    spec: &ParamSpec,
) -> Result<ParamValue, String> {
    let Some(raw) = raw else {
        return Ok(ParamValue::Null);
    };
    let oid = if declared_oid == 0 || declared_oid == UNKNOWN {
        param_oid(spec)
    } else {
        declared_oid
    };
    if let Some(element) = array_element_oid(oid) {
        return if binary {
            decode_array_binary(raw, element)
        } else {
            decode_array_text(raw, element)
        };
    }
    decode_scalar(raw, binary, oid)
}

fn decode_scalar(raw: &[u8], binary: bool, oid: u32) -> Result<ParamValue, String> {
    if !binary {
        let text = std::str::from_utf8(raw)
            .map_err(|_| "parameter is not valid UTF-8".to_owned())?
            .to_owned();
        return Ok(match oid {
            BOOL => match text.trim().to_ascii_lowercase().as_str() {
                "t" | "true" | "yes" | "on" | "1" => ParamValue::Bool(true),
                "f" | "false" | "no" | "off" | "0" => ParamValue::Bool(false),
                other => return Err(format!("invalid boolean '{other}'")),
            },
            INT2 | INT4 | INT8 => ParamValue::Int(
                text.trim()
                    .parse()
                    .map_err(|_| format!("invalid integer '{text}'"))?,
            ),
            FLOAT4 | FLOAT8 => ParamValue::Float(
                text.trim()
                    .parse()
                    .map_err(|_| format!("invalid float '{text}'"))?,
            ),
            _ => ParamValue::Text(text),
        });
    }
    let mut cursor = raw;
    let need = |len: usize| -> Result<(), String> {
        if raw.len() < len {
            Err(format!(
                "binary parameter too short for its type (oid {oid})"
            ))
        } else {
            Ok(())
        }
    };
    Ok(match oid {
        BOOL => {
            need(1)?;
            ParamValue::Bool(cursor.get_u8() != 0)
        }
        INT2 => {
            need(2)?;
            ParamValue::Int(i64::from(cursor.get_i16()))
        }
        INT4 => {
            need(4)?;
            ParamValue::Int(i64::from(cursor.get_i32()))
        }
        INT8 => {
            need(8)?;
            ParamValue::Int(cursor.get_i64())
        }
        FLOAT4 => {
            need(4)?;
            ParamValue::Float(f64::from(cursor.get_f32()))
        }
        FLOAT8 => {
            need(8)?;
            ParamValue::Float(cursor.get_f64())
        }
        NUMERIC => ParamValue::Text(
            numeric_text_from_binary(raw).ok_or_else(|| "malformed binary numeric".to_owned())?,
        ),
        UUID => {
            need(16)?;
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            ParamValue::Text(palimpsest_wal::Uuid::from_bytes(bytes).to_string())
        }
        DATE => {
            need(4)?;
            ParamValue::Text(date_text(cursor.get_i32().wrapping_add(PG_EPOCH_DAYS)))
        }
        TIME => {
            need(8)?;
            ParamValue::Text(clock_text(cursor.get_i64()))
        }
        TIMESTAMP => {
            need(8)?;
            ParamValue::Text(timestamp_text(
                cursor.get_i64().wrapping_add(PG_EPOCH_MICROS),
                false,
            ))
        }
        TIMESTAMPTZ => {
            need(8)?;
            ParamValue::Text(timestamp_text(
                cursor.get_i64().wrapping_add(PG_EPOCH_MICROS),
                true,
            ))
        }
        INTERVAL => {
            need(16)?;
            let micros = cursor.get_i64();
            let days = cursor.get_i32();
            let months = cursor.get_i32();
            ParamValue::Text(interval_text(months, days, micros))
        }
        JSONB => {
            need(1)?;
            let text = std::str::from_utf8(&raw[1..])
                .map_err(|_| "jsonb parameter is not valid UTF-8".to_owned())?;
            ParamValue::Text(text.to_owned())
        }
        _ => ParamValue::Text(
            std::str::from_utf8(raw)
                .map_err(|_| "parameter is not valid UTF-8".to_owned())?
                .to_owned(),
        ),
    })
}

fn decode_array_binary(raw: &[u8], element: u32) -> Result<ParamValue, String> {
    let mut cursor = raw;
    if cursor.remaining() < 12 {
        return Err("malformed binary array".to_owned());
    }
    let ndims = cursor.get_i32();
    let _has_null = cursor.get_i32();
    let _element_oid = cursor.get_u32();
    if ndims == 0 {
        return Ok(ParamValue::List(Vec::new()));
    }
    if ndims != 1 {
        return Err("only one-dimensional array parameters are supported".to_owned());
    }
    if cursor.remaining() < 8 {
        return Err("malformed binary array".to_owned());
    }
    let len = usize::try_from(cursor.get_i32()).map_err(|_| "negative array length".to_owned())?;
    let _lower_bound = cursor.get_i32();
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        if cursor.remaining() < 4 {
            return Err("malformed binary array".to_owned());
        }
        let item_len = cursor.get_i32();
        if item_len < 0 {
            items.push(ParamValue::Null);
            continue;
        }
        let item_len = item_len as usize;
        if cursor.remaining() < item_len {
            return Err("malformed binary array".to_owned());
        }
        items.push(decode_scalar(&cursor[..item_len], true, element)?);
        cursor.advance(item_len);
    }
    Ok(ParamValue::List(items))
}

/// Parses Postgres array text input (`{1,2,"a b",NULL}`), one
/// dimension only.
fn decode_array_text(raw: &[u8], element: u32) -> Result<ParamValue, String> {
    let text = std::str::from_utf8(raw).map_err(|_| "array is not valid UTF-8".to_owned())?;
    let body = text
        .trim()
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| "array literal must be enclosed in braces".to_owned())?;
    let mut items = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut chars = body.chars();
    let mut pending = false;
    while let Some(c) = chars.next() {
        pending = true;
        match c {
            '"' => {
                quoted = !quoted;
                was_quoted = true;
            }
            '\\' if quoted => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            ',' if !quoted => {
                items.push(array_text_item(&current, was_quoted, element)?);
                current.clear();
                was_quoted = false;
            }
            _ => current.push(c),
        }
    }
    if pending {
        items.push(array_text_item(&current, was_quoted, element)?);
    }
    Ok(ParamValue::List(items))
}

fn array_text_item(text: &str, was_quoted: bool, element: u32) -> Result<ParamValue, String> {
    let trimmed = if was_quoted { text } else { text.trim() };
    if !was_quoted && trimmed.eq_ignore_ascii_case("null") {
        return Ok(ParamValue::Null);
    }
    decode_scalar(trimmed.as_bytes(), false, element)
}

#[cfg(test)]
mod tests {
    use super::*;
    use palimpsest_wal::{BigDecimal, Date, Interval, Time, Timestamp};

    /// Days since 1970-01-01 for a proleptic Gregorian date
    /// (`days_from_civil`).
    fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
        let y = if month <= 2 { year - 1 } else { year };
        let era = y.div_euclid(400);
        let yoe = y.rem_euclid(400);
        let mp = i64::from(if month > 2 { month - 3 } else { month + 9 });
        let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }

    fn spec(ty: ColumnType, list: bool) -> ParamSpec {
        ParamSpec {
            position: 1,
            name: "p".to_owned(),
            ty,
            list,
            nullable: true,
        }
    }

    #[test]
    fn dates_and_timestamps_render_like_postgres() {
        assert_eq!(date_text(0), "1970-01-01");
        assert_eq!(date_text(PG_EPOCH_DAYS), "2000-01-01");
        assert_eq!(date_text(-1), "1969-12-31");
        assert_eq!(
            timestamp_text(PG_EPOCH_MICROS + 3_661_500_000, false),
            "2000-01-01 01:01:01.5"
        );
        assert_eq!(timestamp_text(0, true), "1970-01-01 00:00:00+00");
        assert_eq!(clock_text(45_296_000_001), "12:34:56.000001");
        assert_eq!(
            interval_text(14, 3, 5_000_000),
            "1 year 2 mons 3 days 00:00:05"
        );
        assert_eq!(interval_text(0, 0, 0), "00:00:00");
        assert_eq!(interval_text(0, 0, -90_000_000), "-00:01:30");
        // Round trip through the civil-date helpers.
        for days in [-1_000_000_i64, -1, 0, 1, 10_957, 2_932_896] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
    }

    #[test]
    fn binary_encoding_widens_narrow_integers_and_floats() {
        assert_eq!(
            encode_column(&Datum::I32(7), &DatumType::I64, true),
            Some(7_i64.to_be_bytes().to_vec())
        );
        assert_eq!(
            encode_column(&Datum::F32(1.5_f32.to_bits()), &DatumType::F64, true),
            Some(1.5_f64.to_be_bytes().to_vec())
        );
        assert_eq!(encode_column(&Datum::Null, &DatumType::I64, true), None);
        assert_eq!(
            encode_column(
                &Datum::Date(Date {
                    days_since_unix_epoch: PG_EPOCH_DAYS
                }),
                &DatumType::Date,
                true
            ),
            Some(0_i32.to_be_bytes().to_vec())
        );
        assert_eq!(
            encode_column(
                &Datum::Timestamp(Timestamp {
                    micros_since_unix_epoch: PG_EPOCH_MICROS + 5
                }),
                &DatumType::Timestamp,
                true
            ),
            Some(5_i64.to_be_bytes().to_vec())
        );
        let interval = Datum::Interval(Interval {
            months: 1,
            days: 2,
            micros: 3,
        });
        let mut expected = Vec::new();
        expected.put_i64(3);
        expected.put_i32(2);
        expected.put_i32(1);
        assert_eq!(
            encode_column(&interval, &DatumType::Interval, true),
            Some(expected)
        );
        assert_eq!(
            encode_column(
                &Datum::Time(Time {
                    micros_since_midnight: 1
                }),
                &DatumType::Time,
                true
            ),
            Some(1_i64.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn text_encoding_matches_psql_output() {
        assert_eq!(encode_text(&Datum::Bool(true), &DatumType::Bool), "t");
        assert_eq!(
            encode_text(&Datum::Bytea(vec![0xde, 0xad].into()), &DatumType::Bytea),
            "\\xdead"
        );
        assert_eq!(
            encode_text(&Datum::F64(0.1_f64.to_bits()), &DatumType::F64),
            "0.1"
        );
        assert_eq!(
            encode_text(&Datum::F64(f64::NAN.to_bits()), &DatumType::F64),
            "NaN"
        );
        let array = Datum::Array(vec![
            Datum::Text("a b".into()),
            Datum::Null,
            Datum::Text("plain".into()),
            Datum::Text(String::new().into()),
        ]);
        assert_eq!(
            encode_text(&array, &DatumType::Array(Box::new(DatumType::Text))),
            "{\"a b\",NULL,plain,\"\"}"
        );
    }

    #[test]
    fn numeric_binary_round_trips() {
        for text in [
            "0",
            "1",
            "-1",
            "12345.678",
            "0.0001",
            "10000",
            "123456789.123456789",
            "1.5E+3",
            "-0.5",
        ] {
            let bytes = numeric_binary(text).unwrap_or_else(|| panic!("encode {text}"));
            let back = numeric_text_from_binary(&bytes).unwrap_or_else(|| panic!("decode {text}"));
            let expected = expand_scientific(text.trim_start_matches('-')).unwrap();
            let expected = if text.starts_with('-') && expected != "0" {
                format!("-{expected}")
            } else {
                expected
            };
            assert_eq!(back, expected, "round trip of {text}");
        }
        // Layout check: 12345.678 → digits [1, 2345, 6780], weight 1, dscale 3.
        let bytes = numeric_binary("12345.678").unwrap();
        let mut cursor = &bytes[..];
        assert_eq!(cursor.get_i16(), 3);
        assert_eq!(cursor.get_i16(), 1);
        assert_eq!(cursor.get_u16(), 0);
        assert_eq!(cursor.get_u16(), 3);
        assert_eq!(
            [cursor.get_i16(), cursor.get_i16(), cursor.get_i16()],
            [1, 2345, 6780]
        );
        // Numeric datums render through the declared type.
        let datum = Datum::Numeric(BigDecimal::new("42.10".to_owned()));
        assert_eq!(encode_text(&datum, &DatumType::Numeric), "42.10");
    }

    #[test]
    fn params_decode_by_client_declared_type_or_inferred_fallback() {
        let int_spec = spec(ColumnType::Int, false);
        // Client declared nothing: inferred int8, binary.
        assert_eq!(
            decode_param(Some(&42_i64.to_be_bytes()), true, 0, &int_spec).unwrap(),
            ParamValue::Int(42)
        );
        // Client declared int4: decode four bytes even though the column is int8.
        assert_eq!(
            decode_param(Some(&7_i32.to_be_bytes()), true, INT4, &int_spec).unwrap(),
            ParamValue::Int(7)
        );
        // Text format.
        assert_eq!(
            decode_param(Some(b"-3"), false, 0, &int_spec).unwrap(),
            ParamValue::Int(-3)
        );
        assert_eq!(
            decode_param(None, true, 0, &int_spec).unwrap(),
            ParamValue::Null
        );
        // Text array for an `= ANY($1)` int parameter.
        let list_spec = spec(ColumnType::Int, true);
        assert_eq!(
            decode_param(Some(b"{1,2,NULL}"), false, 0, &list_spec).unwrap(),
            ParamValue::List(vec![
                ParamValue::Int(1),
                ParamValue::Int(2),
                ParamValue::Null
            ])
        );
        // Binary uuid becomes hyphenated text for the literal renderer.
        let uuid_spec = spec(ColumnType::Uuid, false);
        let bytes = [0x12_u8; 16];
        assert_eq!(
            decode_param(Some(&bytes), true, 0, &uuid_spec).unwrap(),
            ParamValue::Text("12121212-1212-1212-1212-121212121212".to_owned())
        );
        // Binary timestamptz at the Postgres epoch.
        let ts_spec = spec(ColumnType::TimestampTz, false);
        assert_eq!(
            decode_param(Some(&0_i64.to_be_bytes()), true, 0, &ts_spec).unwrap(),
            ParamValue::Text("2000-01-01 00:00:00+00".to_owned())
        );
        // Binary int8[] array.
        let mut array = Vec::new();
        array.put_i32(1);
        array.put_i32(0);
        array.put_u32(INT8);
        array.put_i32(2);
        array.put_i32(1);
        array.put_i32(8);
        array.put_i64(5);
        array.put_i32(8);
        array.put_i64(6);
        assert_eq!(
            decode_param(Some(&array), true, array_oid(INT8), &list_spec).unwrap(),
            ParamValue::List(vec![ParamValue::Int(5), ParamValue::Int(6)])
        );
    }

    #[test]
    fn wire_types_cover_every_datum_type() {
        assert_eq!(wire_type(&DatumType::I64), (INT8, 8));
        assert_eq!(
            wire_type(&DatumType::Array(Box::new(DatumType::Text))),
            (1009, -1)
        );
        assert_eq!(
            wire_type(&DatumType::Array(Box::new(DatumType::I64))),
            (1016, -1)
        );
        assert_eq!(param_oid(&spec(ColumnType::Int, true)), 1016);
        assert_eq!(param_oid(&spec(ColumnType::Uuid, false)), UUID);
    }
}
