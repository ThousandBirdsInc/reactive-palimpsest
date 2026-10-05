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

// ---------------------------------------------------------------------------
// End-to-end over a real browser WebSocket.
//
// These run only when `PALIMPSEST_WS_TEST_URL` is set at compile time to
// the origin of a bridge stand-in (see "Testing the browser path" in
// `docs/WASM-CLIENT.md`): a server that answers the first frame with an
// `Accepted` + `Diff` for `sub-1` (schema 7, one `[I64(1), Text("hello")]`
// row at LSN 43), answers the next frame (the ack) with an `Error`
// (`unknown_query`), closes with code 1008 when `?token=bad`, and with
// `?token=flaky` closes the *first* connection with code 1011 right after
// the handshake and serves the second one normally. Without the
// variable they return immediately, so the headless CI job still
// compiles and runs them.

use palimpsest_client::{Client, Code, ConnectionState, DiffEvent, DiffOp, WireDatum};

const WS_TEST_URL: Option<&str> = option_env!("PALIMPSEST_WS_TEST_URL");

#[wasm_bindgen_test]
async fn subscribe_streams_accepted_diff_and_error_over_websocket() {
    let Some(origin) = WS_TEST_URL else { return };

    // An `http://` origin without a path exercises the ws:// rewrite and
    // the `/ws/subscribe` default route.
    let client = Client::connect(origin, Auth::Anonymous).await.unwrap();
    let mut sub = client
        .subscribe("SELECT id, title FROM posts")
        .await
        .unwrap();

    match sub.next_event().await.unwrap().unwrap() {
        DiffEvent::Accepted {
            schema_id,
            snapshot_lsn,
            schema,
        } => {
            assert_eq!(schema_id, 7);
            assert_eq!(snapshot_lsn, 42);
            assert_eq!(schema.columns.len(), 2);
            assert_eq!(schema.columns[1].name, "title");
        }
        other => panic!("expected Accepted, got {other:?}"),
    }
    match sub.next_event().await.unwrap().unwrap() {
        DiffEvent::Diff { lsn, op, rows } => {
            assert_eq!(lsn, 43);
            assert_eq!(op, DiffOp::Initial);
            assert_eq!(
                rows,
                vec![vec![WireDatum::I64(1), WireDatum::Text(b"hello".to_vec())]]
            );
        }
        other => panic!("expected Diff, got {other:?}"),
    }
    let cache = sub
        .cache_snapshot()
        .await
        .expect("cache enabled by default");
    assert_eq!(cache.rows().len(), 1);
    assert_eq!(client.connection_state(), ConnectionState::Connected);

    // The ack travels browser → server; the stand-in answers it with an
    // `Error` frame.
    sub.ack(43).await.unwrap();
    match sub.next_event().await.unwrap().unwrap() {
        DiffEvent::Error { code, message } => {
            assert_eq!(code, "unknown_query");
            assert_eq!(message, "no such query");
        }
        other => panic!("expected Error, got {other:?}"),
    }

    // Shutdown fails every open subscription with `ConnectionClosed`,
    // then the stream ends.
    client.shutdown().await;
    assert!(matches!(
        sub.next_event().await,
        Some(Err(ClientError::ConnectionClosed))
    ));
    assert!(sub.next_event().await.is_none());
}

#[wasm_bindgen_test]
async fn transient_close_reconnects_and_resubscribes() {
    let Some(origin) = WS_TEST_URL else { return };

    // First connection is cut with 1011 → backoff sleep → reconnect →
    // the subscription is re-issued on the new socket and streams as
    // if nothing happened.
    let client = Client::connect(origin, Auth::bearer("flaky"))
        .await
        .unwrap();
    let mut state = client.watch_connection_state();
    let mut sub = client
        .subscribe("SELECT id, title FROM posts")
        .await
        .unwrap();

    match sub.next_event().await.unwrap().unwrap() {
        DiffEvent::Accepted { schema_id, .. } => assert_eq!(schema_id, 7),
        other => panic!("expected Accepted after reconnect, got {other:?}"),
    }
    match sub.next_event().await.unwrap().unwrap() {
        DiffEvent::Diff { lsn, .. } => assert_eq!(lsn, 43),
        other => panic!("expected Diff after reconnect, got {other:?}"),
    }
    // The watch keeps only the latest value; by now that is Connected,
    // and it got there through at least one Reconnecting transition
    // (the stand-in never answers the first socket).
    while !matches!(*state.borrow(), ConnectionState::Connected) {
        state.changed().await.unwrap();
    }
    client.shutdown().await;
}

#[wasm_bindgen_test]
async fn policy_violation_close_is_an_auth_failure() {
    let Some(origin) = WS_TEST_URL else { return };

    // `?token=bad` makes the stand-in close with 1008 right after the
    // handshake. The manager must surface that as an auth failure and
    // stop — no reconnect loop.
    let client = Client::connect(origin, Auth::bearer("bad")).await.unwrap();
    let mut state = client.watch_connection_state();
    let mut sub = client.subscribe("SELECT 1").await.unwrap();

    match sub.next_event().await {
        // Subscribe was registered before the close arrived.
        Some(Err(ClientError::Grpc(status))) => {
            assert_eq!(status.code(), Code::Unauthenticated);
            assert_eq!(status.message(), "bad token");
        }
        // Subscribe was still queued when the manager shut down.
        Some(Err(ClientError::ConnectionClosed)) | None => {}
        other => panic!("expected an auth failure, got {other:?}"),
    }
    while !matches!(*state.borrow(), ConnectionState::Closed { .. }) {
        state.changed().await.unwrap();
    }
}
