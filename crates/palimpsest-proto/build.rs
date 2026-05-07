// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/palimpsest/sync/v1/sync.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/palimpsest/sync/v1/sync.proto");
    Ok(())
}
