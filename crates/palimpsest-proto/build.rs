// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The browser client never speaks gRPC directly — it carries the
    // same `ClientMessage`/`ServerMessage` frames over a WebSocket and
    // decodes them with `prost` alone. So on `wasm32` we only generate
    // the message types: no `SyncEngineClient`/`SyncEngineServer`
    // stubs, which means no `tonic` (and no `http`, `tower`, `base64`,
    // …) in the wasm dependency closure at all.
    let wasm = std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("wasm32");

    // `build_transport(false)` skips the convenience `connect(...)`
    // helpers that pull `tonic::transport::Channel`, so native callers
    // construct a `Channel` themselves.
    tonic_build::configure()
        .build_server(!wasm)
        .build_client(!wasm)
        .build_transport(false)
        .compile_protos(&["proto/palimpsest/sync/v1/sync.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/palimpsest/sync/v1/sync.proto");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ARCH");
    Ok(())
}
