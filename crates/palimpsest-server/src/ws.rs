// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Native browser transport: `/ws/subscribe`.
//!
//! The WASM client assumes a WebSocket endpoint (it rewrites
//! `http` → `ws` and appends `/ws/subscribe` when the server URL has
//! no path), and gRPC-Web cannot carry the bidirectional Subscribe
//! stream outside Chromium. This module serves that endpoint from the
//! server itself, on the same listener as gRPC, so adopters no longer
//! write their own bridge.
//!
//! Wire format (identical to the demo bridge this replaces):
//!
//! * browser → server: WS binary frame = prost-encoded
//!   `palimpsest.sync.v1.ClientMessage`
//! * server → browser: WS binary frame = prost-encoded
//!   `palimpsest.sync.v1.ServerMessage`
//!
//! Browsers cannot set headers on a WebSocket handshake, so the
//! bearer token arrives as `?token=<jwt>` and is re-presented as
//! `authorization: Bearer` metadata on the in-process subscribe —
//! protocol plumbing the server owns, not adopter policy.

use std::net::SocketAddr;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use palimpsest_proto::palimpsest::sync::v1::sync_engine_client::SyncEngineClient;
use palimpsest_proto::palimpsest::sync::v1::{ClientMessage, ServerMessage};
use prost::Message as ProstMessage;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, warn};

/// Bound on the upstream outbound channel. The browser only sends
/// control messages, so traffic is light.
const OUTBOUND_CAPACITY: usize = 64;

/// WS close code for auth rejections (RFC 6455 §7.4), so client
/// reconnect logic can tell "fix your token" apart from a transient
/// network failure.
const CLOSE_POLICY_VIOLATION: u16 = 1008;

/// State injected into the WS handler: the loopback address of this
/// server's own gRPC listener.
#[derive(Clone)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct WsState {
    pub(crate) grpc_addr: SocketAddr,
}

/// Query parameters accepted on the upgrade URL.
#[derive(Deserialize, Default)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct WsQuery {
    #[serde(default)]
    token: Option<String>,
}

/// Builds the `/ws/subscribe` router to merge into the main listener.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn router(state: WsState) -> axum::Router {
    axum::Router::new()
        .route("/ws/subscribe", axum::routing::get(ws_subscribe))
        .with_state(state)
}

async fn ws_subscribe(
    State(state): State<WsState>,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle(state, query.token, socket))
}

async fn handle(state: WsState, token: Option<String>, socket: WebSocket) {
    let url = format!("http://{}", state.grpc_addr);
    let endpoint = match tonic::transport::Endpoint::from_shared(url) {
        Ok(endpoint) => endpoint,
        Err(err) => {
            warn!(error = %err, "ws bridge: invalid loopback endpoint");
            return;
        }
    };
    let channel = match endpoint.connect().await {
        Ok(channel) => channel,
        Err(err) => {
            warn!(error = %err, "ws bridge: loopback gRPC dial failed");
            return;
        }
    };
    if let Err(err) = run(SyncEngineClient::new(channel), token, socket).await {
        debug!(error = %err, "ws bridge: session ended");
    }
}

async fn run(
    mut client: SyncEngineClient<Channel>,
    token: Option<String>,
    socket: WebSocket,
) -> Result<(), BridgeError> {
    let (mut ws_sink, mut ws_stream) = socket.split();
    let (out_tx, out_rx) = mpsc::channel::<ClientMessage>(OUTBOUND_CAPACITY);

    // Re-present the `?token=` bearer token as gRPC metadata so the
    // configured Authenticator verifies it exactly as it would a
    // native gRPC client's header.
    let mut request = tonic::Request::new(ReceiverStream::new(out_rx));
    if let Some(token) = token {
        let Ok(value) = format!("Bearer {token}").parse() else {
            let close = CloseFrame {
                code: CLOSE_POLICY_VIOLATION,
                reason: "invalid token".into(),
            };
            let _ = ws_sink.send(Message::Close(Some(close))).await;
            return Err(BridgeError::InvalidToken);
        };
        request.metadata_mut().insert("authorization", value);
    }

    let response = match client.subscribe(request).await {
        Ok(response) => response,
        Err(status) => {
            if matches!(
                status.code(),
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
            ) {
                let close = CloseFrame {
                    code: CLOSE_POLICY_VIOLATION,
                    reason: status.message().to_owned().into(),
                };
                let _ = ws_sink.send(Message::Close(Some(close))).await;
            }
            return Err(BridgeError::GrpcCall(status));
        }
    };
    let mut grpc_stream = response.into_inner();

    // browser → engine: each WS binary frame is one ClientMessage.
    let browser_to_grpc = async {
        while let Some(message) = ws_stream.next().await {
            match message.map_err(BridgeError::WsRecv)? {
                Message::Binary(bytes) => {
                    let client_message = ClientMessage::decode(bytes.as_ref())
                        .map_err(BridgeError::DecodeClientMessage)?;
                    if out_tx.send(client_message).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Text(_) => return Err(BridgeError::UnexpectedTextFrame),
            }
        }
        Ok::<_, BridgeError>(())
    };

    // engine → browser: each ServerMessage is one WS binary frame.
    let grpc_to_browser = async {
        while let Some(message) = grpc_stream.next().await {
            let server_message: ServerMessage = message.map_err(BridgeError::GrpcRecv)?;
            let mut buf = Vec::with_capacity(server_message.encoded_len());
            server_message
                .encode(&mut buf)
                .map_err(BridgeError::EncodeServerMessage)?;
            ws_sink
                .send(Message::Binary(buf))
                .await
                .map_err(BridgeError::WsSend)?;
        }
        Ok::<_, BridgeError>(())
    };

    tokio::select! {
        result = browser_to_grpc => result?,
        result = grpc_to_browser => result?,
    }
    Ok(())
}

#[derive(Debug)]
enum BridgeError {
    GrpcCall(tonic::Status),
    GrpcRecv(tonic::Status),
    WsRecv(axum::Error),
    WsSend(axum::Error),
    DecodeClientMessage(prost::DecodeError),
    EncodeServerMessage(prost::EncodeError),
    UnexpectedTextFrame,
    InvalidToken,
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GrpcCall(status) => write!(f, "grpc subscribe call: {status}"),
            Self::GrpcRecv(status) => write!(f, "grpc recv: {status}"),
            Self::WsRecv(err) => write!(f, "ws recv: {err}"),
            Self::WsSend(err) => write!(f, "ws send: {err}"),
            Self::DecodeClientMessage(err) => write!(f, "decode ClientMessage: {err}"),
            Self::EncodeServerMessage(err) => write!(f, "encode ServerMessage: {err}"),
            Self::UnexpectedTextFrame => write!(f, "unexpected ws text frame"),
            Self::InvalidToken => write!(f, "token rejected by metadata encoding"),
        }
    }
}

impl std::error::Error for BridgeError {}
