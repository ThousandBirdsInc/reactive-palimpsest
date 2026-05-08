// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{io, num::ParseIntError, str::Utf8Error};

use thiserror::Error;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("unsupported Postgres type OID {0}")]
    UnsupportedTypeOid(u32),
    #[error("cannot decode {format} value for {datum_type:?}")]
    Decode {
        datum_type: crate::DatumType,
        format: &'static str,
    },
    #[error("invalid UTF-8 in WAL payload: {0}")]
    Utf8(#[from] Utf8Error),
    #[error("invalid integer in WAL payload: {0}")]
    Int(#[from] ParseIntError),
    #[error("invalid bool literal {0:?}")]
    Bool(String),
    #[error("invalid binary payload for {0:?}")]
    Binary(crate::DatumType),
    #[error("invalid date literal {0:?}")]
    Date(String),
    #[error("invalid time literal {0:?}")]
    Time(String),
    #[error("invalid timestamp literal {0:?}")]
    Timestamp(String),
    #[error("invalid interval literal {0:?}")]
    Interval(String),
    #[error("invalid uuid literal {0:?}")]
    Uuid(String),
    #[error("unexpected end of WAL message while reading {0}")]
    UnexpectedEof(&'static str),
    #[error("malformed pgoutput message: {0}")]
    Malformed(&'static str),
    #[error("relation {0:?} is not in the WAL catalog")]
    UnknownRelation(crate::TableId),
    #[error("tuple for relation {table:?} has {actual} columns; expected {expected}")]
    TupleArity {
        table: crate::TableId,
        expected: usize,
        actual: usize,
    },
    #[error("unsupported pgoutput message tag {0}")]
    UnsupportedMessage(u8),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}
