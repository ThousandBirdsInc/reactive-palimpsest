// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `build_transport(false)` skips the convenience `connect(...)`
    // helpers that pull `tonic::transport::Channel`, so the same
    // generated code compiles on both native and
    // `wasm32-unknown-unknown`. Native callers construct a `Channel`
    // themselves; wasm callers use `tonic-web-wasm-client::Client`.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(false)
        .compile_protos(&["proto/palimpsest/sync/v1/sync.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/palimpsest/sync/v1/sync.proto");
    Ok(())
}
