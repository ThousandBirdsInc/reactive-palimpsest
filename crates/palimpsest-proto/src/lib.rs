// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![doc = "Generated protobuf and gRPC bindings for Palimpsest."]

pub mod palimpsest {
    pub mod sync {
        pub mod v1 {
            #![allow(clippy::all, clippy::nursery, clippy::pedantic)]

            tonic::include_proto!("palimpsest.sync.v1");
        }
    }
}
