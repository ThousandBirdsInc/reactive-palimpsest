// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end tests driving `palimpsest-client` against an in-process
//! `palimpsest-server` (§18.10 — "Tests: drive against
//! `palimpsest-server` in same process").
//!
//! Native-only: `palimpsest-server` pulls `axum`, `hyper`, and
//! tokio's multi-threaded runtime, none of which compile for
//! `wasm32-unknown-unknown`.

#![cfg(not(target_arch = "wasm32"))]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use jsonwebtoken::{encode, EncodingKey, Header};
use palimpsest_client::{Auth, Client, ClientConfig, DiffEvent};
use palimpsest_server::{
    AnonymousAuthenticator, EmptyWalRuntime, JwtAuthConfig, JwtAuthenticator, Palimpsest,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct ServerHarness {
    grpc_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl ServerHarness {
    async fn start_anonymous() -> Self {
        Self::start_with(|builder| builder.with_auth(AnonymousAuthenticator)).await
    }

    async fn start_jwt(secret: &str) -> Self {
        let secret = secret.to_owned();
        Self::start_with(move |builder| {
            builder.with_auth(JwtAuthenticator::new(JwtAuthConfig {
                secret,
                issuer: None,
                audience: None,
                claim_to_field: std::collections::BTreeMap::new(),
            }))
        })
        .await
    }

    async fn start_with<F>(configure: F) -> Self
    where
        F: FnOnce(palimpsest_server::PalimpsestBuilder) -> palimpsest_server::PalimpsestBuilder,
    {
        let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind grpc");
        grpc_listener
            .set_nonblocking(true)
            .expect("set nonblocking");
        let grpc_addr = grpc_listener.local_addr().expect("grpc addr");
        drop(grpc_listener);

        let builder = Palimpsest::builder()
            .with_wal(EmptyWalRuntime::default())
            .with_grpc_addr(grpc_addr)
            .with_metrics_addr(None);
        let server = configure(builder).build().expect("build server");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let _ = server
                .serve(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        wait_for_port(grpc_addr).await;
        Self {
            grpc_addr,
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.grpc_addr)
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        }
    }
}

async fn wait_for_port(addr: SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up at {addr}");
}

async fn next_event_within(
    sub: &mut palimpsest_client::Subscription,
    timeout: Duration,
) -> DiffEvent {
    tokio::time::timeout(timeout, sub.next_event())
        .await
        .expect("event timeout")
        .expect("stream closed")
        .expect("event err")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_subscribe_returns_accepted_event() {
    let harness = ServerHarness::start_anonymous().await;
    let client = Client::connect(harness.url(), Auth::Anonymous)
        .await
        .expect("connect");

    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("subscribe");
    let event = next_event_within(&mut sub, Duration::from_secs(2)).await;
    let DiffEvent::Accepted {
        schema_id, schema, ..
    } = event
    else {
        panic!("expected Accepted, got {event:?}");
    };
    assert!(schema_id > 0);
    assert!(!schema.columns.is_empty());

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_default_on_seeds_after_accepted() {
    let harness = ServerHarness::start_anonymous().await;
    let client = Client::connect(harness.url(), Auth::Anonymous)
        .await
        .expect("connect");

    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("subscribe");
    let _ = next_event_within(&mut sub, Duration::from_secs(2)).await;

    let snapshot = sub.cache_snapshot().await.expect("cache enabled");
    // EmptyWalRuntime emits no rows, so the cache is empty after the
    // initial `Accepted` — but it MUST exist (cache enabled).
    assert!(snapshot.is_empty());

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_disabled_returns_none() {
    let harness = ServerHarness::start_anonymous().await;
    let client = Client::connect_with(
        harness.url(),
        Auth::Anonymous,
        ClientConfig {
            cache_enabled: false,
            ..ClientConfig::default()
        },
    )
    .await
    .expect("connect");

    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("subscribe");
    let _ = next_event_within(&mut sub, Duration::from_secs(2)).await;
    assert!(sub.cache_snapshot().await.is_none());

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jwt_token_is_accepted_on_handshake() {
    let harness = ServerHarness::start_jwt("topsecret").await;
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    let token = encode(
        &Header::default(),
        &serde_json::json!({ "sub": "alice", "exp": exp }),
        &EncodingKey::from_secret(b"topsecret"),
    )
    .unwrap();

    let client = Client::connect(harness.url(), Auth::bearer(token))
        .await
        .expect("connect");
    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("subscribe");
    let event = next_event_within(&mut sub, Duration::from_secs(2)).await;
    assert!(matches!(event, DiffEvent::Accepted { .. }));

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jwt_missing_token_closes_subscription_stream() {
    let harness = ServerHarness::start_jwt("topsecret").await;
    let client = Client::connect(harness.url(), Auth::Anonymous)
        .await
        .expect("connect");

    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("subscribe queued");
    // The handshake fails with Unauthenticated → manager shuts the
    // connection down. The subscription's stream emits a
    // ConnectionClosed error (or simply ends).
    let result = tokio::time::timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("timed out waiting for closure");
    match result {
        None | Some(Err(_)) => {}
        Some(Ok(other)) => panic!("expected closure, got {other:?}"),
    }

    client.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_and_unsubscribe_complete_without_error() {
    let harness = ServerHarness::start_anonymous().await;
    let client = Client::connect(harness.url(), Auth::Anonymous)
        .await
        .expect("connect");

    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("subscribe");
    let _ = next_event_within(&mut sub, Duration::from_secs(2)).await;

    sub.update(HashMap::new()).await.expect("update");
    sub.ack(99).await.expect("ack");

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_subscribes_when_server_comes_up_late() {
    // The client connects to a port no one is listening on, queues a
    // subscribe, and only after a small delay does the server come up.
    // The reconnect loop must dial successfully on a later attempt and
    // deliver the queued subscription's `Accepted` — this exercises
    // both backoff and the resubscribe code path.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    listener.set_nonblocking(true).expect("set nonblocking");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    let client = Client::connect_with(
        format!("http://{addr}"),
        Auth::Anonymous,
        ClientConfig {
            backoff: palimpsest_client::BackoffConfig {
                initial: Duration::from_millis(20),
                max: Duration::from_millis(100),
                factor: 2,
            },
            ..ClientConfig::default()
        },
    )
    .await
    .expect("connect");

    let mut sub = client
        .subscribe("SELECT id FROM posts")
        .await
        .expect("queue subscribe");

    // Server lights up on the same port a moment later. Because no
    // active connection has held that port yet, OS-level TIME_WAIT
    // does not interfere.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let server = Palimpsest::builder()
        .with_wal(EmptyWalRuntime::default())
        .with_auth(AnonymousAuthenticator)
        .with_grpc_addr(addr)
        .with_metrics_addr(None)
        .build()
        .expect("build server");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let _ = server
            .serve(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    wait_for_port(addr).await;

    let event = next_event_within(&mut sub, Duration::from_secs(5)).await;
    assert!(
        matches!(event, DiffEvent::Accepted { .. }),
        "expected Accepted after late server bringup, got {event:?}",
    );

    sub.unsubscribe().await.expect("unsubscribe");
    client.shutdown().await;
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
}
