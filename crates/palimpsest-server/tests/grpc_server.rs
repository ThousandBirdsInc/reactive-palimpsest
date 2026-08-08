// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end gRPC tests for the embedded `Palimpsest` server.
//!
//! Each test spins up the full stack (tonic `SyncEngine` plus
//! `tonic-health` and the axum metrics sidecar) on ephemeral local
//! ports, drives a tonic client, and asserts on the server-side
//! metrics counters.

#![allow(clippy::significant_drop_tightening)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use jsonwebtoken::{encode, EncodingKey, Header};
use palimpsest_proto::palimpsest::sync::v1 as proto;
use palimpsest_proto::palimpsest::sync::v1::sync_engine_client::SyncEngineClient;
use palimpsest_server::{
    AnonymousAuthenticator, EmptyWalRuntime, JwtAuthConfig, JwtAuthenticator, Palimpsest,
    PalimpsestHandle,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

struct ServerHarness {
    handle: PalimpsestHandle,
    grpc_addr: SocketAddr,
    metrics_addr: Option<SocketAddr>,
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
                claim_to_field: BTreeMap::new(),
            }))
        })
        .await
    }

    async fn start_with<F>(configure: F) -> Self
    where
        F: FnOnce(palimpsest_server::PalimpsestBuilder) -> palimpsest_server::PalimpsestBuilder,
    {
        let grpc_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let metrics_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");

        // Bind ephemeral ports up front so we know the addresses.
        let grpc_listener = std::net::TcpListener::bind(grpc_addr).expect("bind grpc");
        grpc_listener
            .set_nonblocking(true)
            .expect("set nonblocking");
        let grpc_addr = grpc_listener.local_addr().expect("grpc addr");
        drop(grpc_listener);

        let metrics_listener = std::net::TcpListener::bind(metrics_addr).expect("bind metrics");
        metrics_listener
            .set_nonblocking(true)
            .expect("set nonblocking");
        let metrics_addr = metrics_listener.local_addr().expect("metrics addr");
        drop(metrics_listener);

        let builder = Palimpsest::builder()
            .with_wal(EmptyWalRuntime::default())
            .with_grpc_addr(grpc_addr)
            .with_metrics_addr(Some(metrics_addr));
        let server = configure(builder).build().expect("build");
        let handle = server.handle();

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let _ = server
                .serve(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Poll until the gRPC port is accepting; this races the
        // background task's bind.
        wait_for_port(grpc_addr).await;
        wait_for_port(metrics_addr).await;

        Self {
            handle,
            grpc_addr,
            metrics_addr: Some(metrics_addr),
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    fn endpoint(&self) -> Endpoint {
        Endpoint::from_shared(format!("http://{}", self.grpc_addr))
            .expect("endpoint")
            .connect_timeout(Duration::from_secs(2))
    }

    async fn channel(&self) -> Channel {
        self.endpoint().connect().await.expect("connect")
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

fn subscribe_message(client_subscription_id: &str, sql: &str) -> proto::ClientMessage {
    proto::ClientMessage {
        kind: Some(proto::client_message::Kind::Subscribe(
            proto::SubscribeRequest {
                client_subscription_id: client_subscription_id.to_owned(),
                sql: sql.to_owned(),
                vars: std::collections::HashMap::default(),
                resume_lsn: None,
                query_name: String::new(),
            },
        )),
    }
}

fn unsubscribe_message(client_subscription_id: &str) -> proto::ClientMessage {
    proto::ClientMessage {
        kind: Some(proto::client_message::Kind::Unsubscribe(
            proto::UnsubscribeRequest {
                subscription_id: client_subscription_id.to_owned(),
            },
        )),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anonymous_subscribe_returns_accepted_and_increments_metrics() {
    let harness = ServerHarness::start_anonymous().await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("posts", "SELECT id FROM posts"))
        .await
        .expect("send");

    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe")
        .into_inner();
    let message = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .expect("recv timeout")
        .expect("stream closed")
        .expect("status");

    let kind = message.kind.expect("kind");
    let proto::server_message::Kind::Accepted(accepted) = kind else {
        panic!("expected Accepted, got {kind:?}");
    };
    assert_eq!(accepted.subscription_id, "posts");
    assert!(accepted.schema.is_some());

    // Drop the sender → server-side disconnect path runs.
    drop(tx);
    let _ = response.next().await;

    let snapshot = harness.handle.metrics.snapshot();
    assert!(snapshot.subscriptions_total >= 1);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsubscribe_releases_subscription_and_drops_metrics_count() {
    let harness = ServerHarness::start_anonymous().await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("posts", "SELECT id FROM posts"))
        .await
        .unwrap();
    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe")
        .into_inner();
    let _accepted = response.next().await.unwrap().unwrap();

    tx.send(unsubscribe_message("posts")).await.unwrap();
    drop(tx);
    while response.next().await.is_some() {}

    let snapshot = harness.handle.metrics.snapshot();
    assert_eq!(snapshot.subscriptions_in_flight, 0);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jwt_required_when_authenticator_is_jwt() {
    let harness = ServerHarness::start_jwt("topsecret").await;
    let mut client = SyncEngineClient::new(harness.channel().await);

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(subscribe_message("posts", "SELECT id FROM posts"))
        .await
        .unwrap();
    drop(tx);

    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let err = client.subscribe(request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jwt_accepted_when_token_is_valid() {
    let harness = ServerHarness::start_jwt("topsecret").await;
    let mut client = SyncEngineClient::new(harness.channel().await);

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

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(subscribe_message("posts", "SELECT id FROM posts"))
        .await
        .unwrap();
    let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );

    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe")
        .into_inner();
    let message = tokio::time::timeout(Duration::from_secs(2), response.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        message.kind,
        Some(proto::server_message::Kind::Accepted(_))
    ));

    drop(tx);
    while response.next().await.is_some() {}
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_endpoint_reports_serving() {
    let harness = ServerHarness::start_anonymous().await;
    let mut client = HealthClient::new(harness.channel().await);
    let response = client
        .check(Request::new(HealthCheckRequest {
            service: String::new(),
        }))
        .await
        .expect("health check");
    assert_eq!(
        response.into_inner().status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
    );
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_returns_prometheus_text() {
    let harness = ServerHarness::start_anonymous().await;
    let metrics_addr = harness.metrics_addr.expect("metrics");

    // Drive a subscribe so counters move.
    let mut client = SyncEngineClient::new(harness.channel().await);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(subscribe_message("posts", "SELECT id FROM posts"))
        .await
        .unwrap();
    let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut response = client
        .subscribe(request)
        .await
        .expect("subscribe")
        .into_inner();
    let _ = response.next().await;
    drop(tx);

    // Hit /metrics over a raw TCP connection.
    let body = fetch_metrics(metrics_addr).await;
    assert!(body.contains("palimpsest_subscriptions_total"));
    assert!(body.contains("palimpsest_diffs_sent_total"));

    harness.shutdown().await;
}

async fn fetch_metrics(addr: SocketAddr) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let request = format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let raw = String::from_utf8_lossy(&buf).into_owned();
    let body_start = raw.find("\r\n\r\n").unwrap_or(0);
    raw[body_start..].to_owned()
}
