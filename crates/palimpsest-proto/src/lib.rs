// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "Generated protobuf and gRPC bindings for Palimpsest, plus a \
          manual `Row` codec (§18.9) and the wire-protocol versioning \
          policy (`VERSIONING.md`)."]
#![warn(missing_docs)]

/// Generated `palimpsest.*` protobuf bindings.
pub mod palimpsest {
    /// `palimpsest.sync.*` namespace.
    pub mod sync {
        /// `palimpsest.sync.v1` — current wire version.
        pub mod v1 {
            #![allow(clippy::all, clippy::nursery, clippy::pedantic, missing_docs)]

            tonic::include_proto!("palimpsest.sync.v1");
        }
    }
}

pub mod version;
pub mod wire;

pub use version::{WIRE_PROTOCOL_PACKAGE, WIRE_PROTOCOL_VERSION};
pub use wire::{
    decode_diff, decode_rows, encode_rows, CodecError, SchemaRegistry, WireDatum, WireRow,
};
