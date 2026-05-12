// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Empty library entry — `cargo-fuzz` requires the package to be a
//! library crate so target bins can depend on it. We don't actually
//! need shared code; each fuzz target is a self-contained binary.
