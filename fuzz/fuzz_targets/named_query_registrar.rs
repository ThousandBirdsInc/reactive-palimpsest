// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fuzz the sqlc-format registrar. Invariant: registration must never
//! panic on arbitrary UTF-8 input — every failure is a typed
//! `RegisterError`. Registration internally dummy-binds each accepted
//! template (render → validate → lower), so this also exercises the
//! literal-substitution and lowering pipeline behind `bind`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use palimpsest_sql::{prepared::QueryRegistry, Catalog};

fuzz_target!(|data: &[u8]| {
    let Ok(source) = std::str::from_utf8(data) else {
        return;
    };
    let mut registry = QueryRegistry::new();
    let _ = registry.register_sqlc_source(source, "fuzz", &Catalog::demo());
});
