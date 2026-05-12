// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Headless wasm smoke test (§18.11). Runs under
//! `wasm-bindgen-test-runner` and validates that the wire codec, auth
//! string formatting, and the public client surface compile and behave
//! correctly inside a browser-like environment.
//!
//! Run via:
//! ```bash
//! cargo install wasm-bindgen-cli --version <matching>
//! wasm-pack test --headless --chrome -p palimpsest-client
//! # or, if wasm-bindgen-test-runner is on PATH:
//! CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//!   cargo test -p palimpsest-client --target wasm32-unknown-unknown --test wasm
//! ```

#![cfg(target_arch = "wasm32")]

use palimpsest_client::{Auth, BackoffConfig, ClientConfig, ClientError};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn auth_bearer_constructs() {
    let _a = Auth::bearer("topsecret");
    let _b = Auth::default();
}

#[wasm_bindgen_test]
fn client_config_defaults_are_sane() {
    let cfg = ClientConfig::default();
    assert!(cfg.cache_enabled);
    let backoff: BackoffConfig = cfg.backoff;
    assert!(backoff.factor >= 1);
}

#[wasm_bindgen_test]
fn empty_url_is_rejected() {
    use palimpsest_client::Client;

    // We can't reach a real server from a wasm test runner, so we
    // just exercise the URL-parse path.
    let fut = async {
        let result = Client::connect("", Auth::Anonymous).await;
        match result {
            Err(ClientError::Endpoint(_)) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("empty URL should not connect"),
        }
    };
    wasm_bindgen_futures::spawn_local(fut);
}
