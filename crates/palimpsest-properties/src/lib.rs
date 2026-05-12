// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Test-only crate. Property tests live under `tests/` so each
//! property in §15.3 can be invoked individually:
//!
//! ```bash
//! cargo test -p palimpsest-properties --test oracle_equivalence
//! cargo test -p palimpsest-properties             # all properties
//! ```
//!
//! The library itself is intentionally empty — we publish nothing.
