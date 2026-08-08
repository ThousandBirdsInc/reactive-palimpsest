// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Auth integration matrix and per-connection / per-IP limit checks
//! (§18.15.1 / §18.15.2 / §18.15.3 / §18.15.5).
//!
//! These tests run the full gRPC stack on ephemeral ports and assert
//! the wire-level outcome the client actually sees:
//!
//! * Auth matrix — missing token, expired token, wrong audience,
//!   malformed token. Each yields `Status::unauthenticated` *before*
//!   the inbound stream ever produces a `ClientMessage`.
//! * Subscribe rate / cap — server returns an `Error` `ServerMessage`
//!   with the documented code (`rate_limited` /
//!   `connection_saturated`) and does not break the stream.
//!
//! The reconnect-tracker test only exercises the rejection signature
//! (`Status::resource_exhausted`); confirming the *per-IP* nature
//! requires multi-IP plumbing and is covered by the unit tests in
//! `security::tests`.

#![allow(clippy::significant_drop_tightening)]

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{encode, EncodingKey, Header};
use palimpsest_proto::palimpsest::sync::v1 as proto;
use palimpsest_proto::palimpsest::sync::v1::sync_engine_client::SyncEngineClient;
use palimpsest_server::{
    EmptyWalRuntime, JwtAuthConfig, JwtAuthenticator, Palimpsest, SecurityLimits,
    SlidingWindowSpec, TokenBucketSpec,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

struct Harness {
    grpc_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl Harness {
    async fn start_with<F>(configure: F) -> Self
    where
        F: FnOnce(palimpsest_server::PalimpsestBuilder) -> palimpsest_server::PalimpsestBuilder,
    {
        let bind: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let listener = std::net::TcpListener::bind(bind).expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let grpc_addr = listener.local_addr().expect("addr");
        drop(listener);

        let builder = Palimpsest::builder()
            .with_wal(EmptyWalRuntime::default())
            .with_grpc_addr(grpc_addr)
            .with_metrics_addr(None);
        let server = configure(builder).build().expect("build");

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

    async fn channel(&self) -> Channel {
        Endpoint::from_shared(format!("http://{}", self.grpc_addr))
            .expect("endpoint")
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .expect("connect")
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

fn token_with(secret: &str, claims: &serde_json::Value) -> String {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("encode")
}

fn subscribe_message(id: &str, sql: &str) -> proto::ClientMessage {
    proto::ClientMessage {
        kind: Some(proto::client_message::Kind::Subscribe(
            proto::SubscribeRequest {
                client_subscription_id: id.to_owned(),
                sql: sql.to_owned(),
                vars: std::collections::HashMap::default(),
                resume_lsn: None,
                query_name: String::new(),
            },
        )),
    }
}

async fn jwt_harness_with<F>(configure: F) -> Harness
where
    F: FnOnce(JwtAuthConfig) -> JwtAuthConfig,
{
    let config = configure(JwtAuthConfig {
        secret: Some("topsecret".to_owned()),
        ..JwtAuthConfig::default()
    });
    let auth = JwtAuthenticator::from_config(config)
        .await
        .expect("jwt config");
    Harness::start_with(move |builder| builder.with_auth(auth)).await
}

async fn assert_unauthenticated(harness: &Harness, token: Option<&str>) {
    let mut client = SyncEngineClient::new(harness.channel().await);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(subscribe_message("posts", "SELECT id FROM posts"))
        .await
        .unwrap();
    drop(tx);
    let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    if let Some(token) = token {
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
    }
    let err = client.subscribe(request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_matrix_missing_token_is_unauthenticated() {
    let harness = jwt_harness_with(|config| config).await;
    assert_unauthenticated(&harness, None).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_matrix_expired_token_is_unauthenticated() {
    let harness = jwt_harness_with(|config| config).await;
    let token = token_with(
        "topsecret",
        &serde_json::json!({ "sub": "alice", "exp": now_secs() - 600 }),
    );
    assert_unauthenticated(&harness, Some(&token)).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_matrix_wrong_audience_is_unauthenticated() {
    let harness = jwt_harness_with(|mut config| {
        config.audience = Some("expected-aud".to_owned());
        config
    })
    .await;
    let token = token_with(
        "topsecret",
        &serde_json::json!({
            "sub": "alice",
            "aud": "other-aud",
            "exp": now_secs() + 60,
        }),
    );
    assert_unauthenticated(&harness, Some(&token)).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_matrix_malformed_token_is_unauthenticated() {
    let harness = jwt_harness_with(|config| config).await;
    assert_unauthenticated(&harness, Some("not-a-real-jwt")).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_rate_limit_returns_error_message() {
    let limits = SecurityLimits {
        max_subscriptions_per_connection: 16,
        subscribe_rate: TokenBucketSpec {
            burst: 1,
            refill_per_sec: 0.0,
        },
        reconnect_rate: SlidingWindowSpec {
            max_attempts: 1024,
            window: Duration::from_secs(60),
        },
    };
    let harness = Harness::start_with(move |builder| builder.with_security_limits(limits)).await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("first", "SELECT id FROM posts"))
        .await
        .unwrap();
    tx.send(subscribe_message("second", "SELECT id FROM posts"))
        .await
        .unwrap();
    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe")
        .into_inner();

    // Admission decisions are taken in arrival order, but Accepted vs
    // Error are emitted by independent tasks (Accepted goes through
    // the heavy snapshot path, Error returns immediately), so the two
    // can arrive in either order on the multiplexed bidi stream.
    // Match by subscription_id instead of position.
    let (a, b) = (
        next_message(&mut response).await,
        next_message(&mut response).await,
    );
    let (accepted, rejected) = match (&a.kind, &b.kind) {
        (Some(proto::server_message::Kind::Accepted(_)), _) => (a, b),
        (_, Some(proto::server_message::Kind::Accepted(_))) => (b, a),
        _ => panic!("expected one Accepted and one Error, got {a:?}, {b:?}"),
    };
    let proto::server_message::Kind::Accepted(acc) = accepted.kind.expect("accepted kind") else {
        unreachable!()
    };
    assert_eq!(acc.subscription_id, "first");
    let proto::server_message::Kind::Error(err) = rejected.kind.expect("rejected kind") else {
        panic!("expected Error message");
    };
    assert_eq!(err.code, "rate_limited");
    assert_eq!(err.subscription_id, "second");

    drop(tx);
    while response.next().await.is_some() {}
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_cap_returns_connection_saturated() {
    let limits = SecurityLimits {
        max_subscriptions_per_connection: 1,
        subscribe_rate: TokenBucketSpec {
            burst: 1024,
            refill_per_sec: 1024.0,
        },
        reconnect_rate: SlidingWindowSpec {
            max_attempts: 1024,
            window: Duration::from_secs(60),
        },
    };
    let harness = Harness::start_with(move |builder| builder.with_security_limits(limits)).await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("first", "SELECT id FROM posts"))
        .await
        .unwrap();
    tx.send(subscribe_message("second", "SELECT id FROM posts"))
        .await
        .unwrap();
    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe")
        .into_inner();

    // Same ordering caveat as `subscribe_rate_limit_returns_error_message`:
    // admission is in arrival order, but the two responses can arrive
    // in either order. Match by subscription_id.
    let (a, b) = (
        next_message(&mut response).await,
        next_message(&mut response).await,
    );
    let (accepted, rejected) = match (&a.kind, &b.kind) {
        (Some(proto::server_message::Kind::Accepted(_)), _) => (a, b),
        (_, Some(proto::server_message::Kind::Accepted(_))) => (b, a),
        _ => panic!("expected one Accepted and one Error, got {a:?}, {b:?}"),
    };
    let proto::server_message::Kind::Accepted(acc) = accepted.kind.expect("accepted kind") else {
        unreachable!()
    };
    assert_eq!(acc.subscription_id, "first");
    let proto::server_message::Kind::Error(err) = rejected.kind.expect("rejected kind") else {
        panic!("expected Error message");
    };
    assert_eq!(err.code, "connection_saturated");
    assert_eq!(err.subscription_id, "second");

    drop(tx);
    while response.next().await.is_some() {}
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_too_large_is_rejected_at_parse_time() {
    let harness = Harness::start_with(|builder| builder).await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let huge =
        "SELECT id FROM posts WHERE id = 1 OR ".to_string() + &"1=1 OR ".repeat(20_000) + "1=1";
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    tx.send(subscribe_message("oversize", &huge)).await.unwrap();
    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe")
        .into_inner();
    let message = next_message(&mut response).await;
    let proto::server_message::Kind::Error(err) = message.kind.expect("kind") else {
        panic!("expected Error");
    };
    assert_eq!(err.code, "query_too_large");

    drop(tx);
    while response.next().await.is_some() {}
    harness.shutdown().await;
}

async fn next_message(stream: &mut tonic::Streaming<proto::ServerMessage>) -> proto::ServerMessage {
    tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("recv timeout")
        .expect("stream closed")
        .expect("status")
}
