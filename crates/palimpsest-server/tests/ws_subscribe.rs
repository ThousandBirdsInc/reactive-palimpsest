// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end coverage for the native `/ws/subscribe` transport: the
//! server serves gRPC and the browser WebSocket bridge from one
//! listener, translates `?token=` into `authorization: Bearer`, and
//! pumps protobuf frames in both directions.

use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use palimpsest_proto::palimpsest::sync::v1::{
    client_message, server_message, ClientMessage, ServerMessage, SubscribeRequest,
};
use palimpsest_server::{EmptyWalRuntime, JwtAuthConfig, JwtAuthenticator, Palimpsest};
use prost::Message as ProstMessage;
use tokio_tungstenite::tungstenite::Message as WsMessage;

fn free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("pick port")
        .port()
}

fn subscribe_frame(sql: &str) -> WsMessage {
    let message = ClientMessage {
        kind: Some(client_message::Kind::Subscribe(SubscribeRequest {
            client_subscription_id: "c1".to_owned(),
            sql: sql.to_owned(),
            vars: std::collections::HashMap::new(),
            resume_lsn: None,
            query_name: String::new(),
        })),
    };
    let mut buf = Vec::with_capacity(message.encoded_len());
    message.encode(&mut buf).expect("encode");
    WsMessage::Binary(buf)
}

async fn serve(server: Palimpsest) -> tokio::sync::oneshot::Sender<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = server
            .serve(async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    // Give the listener a moment to come up.
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown_tx
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_subscribe_accepts_and_streams_server_messages() {
    let port = free_port();
    let server = Palimpsest::builder()
        .with_wal(EmptyWalRuntime::default())
        .with_grpc_addr(format!("127.0.0.1:{port}").parse().expect("addr"))
        .with_metrics_addr(None)
        .build()
        .expect("build");
    let _shutdown = serve(server).await;

    let url = format!("ws://127.0.0.1:{port}/ws/subscribe");
    let (mut socket, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("ws connect");

    socket
        .send(subscribe_frame("SELECT id FROM posts"))
        .await
        .expect("send subscribe");

    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("response within deadline")
        .expect("stream open")
        .expect("frame");
    let WsMessage::Binary(bytes) = frame else {
        panic!("expected binary ServerMessage frame, got {frame:?}");
    };
    let message = ServerMessage::decode(bytes.as_slice()).expect("decodes as ServerMessage");
    match message.kind {
        Some(server_message::Kind::Accepted(accepted)) => {
            assert!(!accepted.subscription_id.is_empty());
        }
        other => panic!("expected Accepted, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ws_rejects_bad_token_with_policy_violation_close() {
    let port = free_port();
    let server = Palimpsest::builder()
        .with_wal(EmptyWalRuntime::default())
        .with_auth(
            JwtAuthenticator::from_config(JwtAuthConfig {
                secret: Some("topsecret".to_owned()),
                ..JwtAuthConfig::default()
            })
            .await
            .expect("auth config"),
        )
        .with_grpc_addr(format!("127.0.0.1:{port}").parse().expect("addr"))
        .with_metrics_addr(None)
        .build()
        .expect("build");
    let _shutdown = serve(server).await;

    let url = format!("ws://127.0.0.1:{port}/ws/subscribe?token=not-a-jwt");
    let (mut socket, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("ws connect");
    socket
        .send(subscribe_frame("SELECT id FROM posts"))
        .await
        .expect("send subscribe");

    let deadline = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = socket.next().await {
            match frame {
                Ok(WsMessage::Close(Some(close))) => {
                    assert_eq!(u16::from(close.code), 1008, "policy-violation close code");
                    return;
                }
                Ok(WsMessage::Close(None)) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await;
    deadline.expect("auth rejection surfaces as a close frame");
}
