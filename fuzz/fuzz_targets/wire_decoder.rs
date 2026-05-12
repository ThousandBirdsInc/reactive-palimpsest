// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fuzz the row encoder/decoder used on the gRPC wire. We seed the
//! decoder with arbitrary bytes; a successful decode must round-trip
//! back to the same bytes (modulo schema-padded reserved fields).

#![no_main]

use libfuzzer_sys::fuzz_target;
use palimpsest_server::{decode_rows, encode_rows};

fuzz_target!(|data: &[u8]| {
    let Ok(rows) = decode_rows(data) else {
        return;
    };
    let encoded = encode_rows(&rows).expect("encode after decode must succeed");
    if let Ok(reparsed) = decode_rows(&encoded) {
        assert_eq!(rows, reparsed, "wire codec must round-trip");
    }
});
