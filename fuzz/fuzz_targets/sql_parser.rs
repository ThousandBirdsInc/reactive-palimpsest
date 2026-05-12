// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fuzz `parse_select` and the lowering pipeline. Invariant: neither
//! step may panic on arbitrary UTF-8 input. Parse failures and lower
//! failures are returned as typed errors.

#![no_main]

use libfuzzer_sys::fuzz_target;
use palimpsest_sql::{parse_and_lower, parse_select};

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let _ = parse_select(sql);
    let _ = parse_and_lower(sql);
});
