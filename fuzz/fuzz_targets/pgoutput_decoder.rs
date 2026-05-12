// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fuzz the pgoutput message decoder against arbitrary byte input.
//! The invariant is simple: `decode_pgoutput_message` must never
//! panic on any input. Decode errors are fine — they're typed.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use palimpsest_wal::{decode_pgoutput_message, Catalog};

fuzz_target!(|data: &[u8]| {
    let mut catalog = Catalog::default();
    let _ = decode_pgoutput_message(&mut catalog, Bytes::copy_from_slice(data));
});
