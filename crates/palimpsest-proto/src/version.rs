// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Wire-protocol version identifiers (§18.9).
//!
//! See `VERSIONING.md` for the full additive-only policy and rules
//! around when a major bump is required.

/// Current major version of the wire protocol. Major bumps force every
/// connected client to disconnect and re-subscribe.
///
/// Bumping this constant **must** be paired with a new `vN/` proto
/// package and a new top-level Rust module (e.g.
/// `crate::palimpsest::sync::v2`).
pub const WIRE_PROTOCOL_VERSION: &str = "v1";

/// Fully-qualified protobuf package the server speaks.
///
/// Clients may compare this against
/// [`Accepted`](crate::palimpsest::sync::v1::Accepted)-time metadata
/// before sending traffic; mismatches indicate the proxy/server is
/// running an incompatible major version.
pub const WIRE_PROTOCOL_PACKAGE: &str = "palimpsest.sync.v1";
