//! WebSocket ↔ gRPC bridge for the browser.
//!
//! gRPC-Web in browsers does not support bidirectional streaming
//! (it relies on fetch upload streams, which only Chromium ships).
//! Firefox/Safari therefore cannot drive the Subscribe RPC directly.
//!
//! This handler accepts a WebSocket connection from the browser, opens
//! an in-process tonic gRPC client to `127.0.0.1:50051`, drives a
//! Subscribe bidi stream, and pumps protobuf-encoded `ClientMessage` /
//! `ServerMessage` frames between the two halves. Each WS binary
//! frame is one wire message.
//!
//! Wire format:
//!   * browser → server: WS binary frame = `prost`-encoded
//!     `palimpsest.sync.v1.ClientMessage`
//!   * server → browser: WS binary frame = `prost`-encoded
//!     `palimpsest.sync.v1.ServerMessage`

use std::net::SocketAddr;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use palimpsest_proto::palimpsest::sync::v1::sync_engine_client::SyncEngineClient;
use palimpsest_proto::palimpsest::sync::v1::{ClientMessage, ServerMessage};
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

/// Bound on the upstream gRPC outbound channel. Small but non-trivial
/// so a momentarily-stuck Subscribe doesn't starve writes; the browser
/// only sends control messages so traffic is light.
const OUTBOUND_CAPACITY: usize = 64;

/// Shared state injected into the WS handler. Holds the loopback gRPC
/// address so we don't pay DNS / TLS to talk to ourselves.
#[derive(Clone)]
pub struct WsState {
    pub grpc_addr: SocketAddr,
}

/// Axum handler: upgrade an HTTP request to a WebSocket, then run the
/// bridge for the lifetime of the connection.
pub async fn ws_subscribe(
    State(state): State<WsState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle(state, socket))
}

async fn handle(state: WsState, socket: WebSocket) {
    let url = format!("http://{}", state.grpc_addr);
    let endpoint = match tonic::transport::Endpoint::from_shared(url.clone()) {
        Ok(e) => e,
        Err(err) => {
            warn!(?err, %url, "ws bridge: invalid gRPC endpoint");
            return;
        }
    };
    let channel = match endpoint.connect().await {
        Ok(c) => c,
        Err(err) => {
            warn!(?err, %url, "ws bridge: gRPC dial failed");
            return;
        }
    };
    let client = SyncEngineClient::new(channel);

    if let Err(err) = run(client, socket).await {
        debug!(?err, "ws bridge: session ended");
    }
}

async fn run(
    mut client: SyncEngineClient<Channel>,
    socket: WebSocket,
) -> Result<(), BridgeError> {
    let (mut ws_sink, mut ws_stream) = socket.split();
    let (out_tx, out_rx) = mpsc::channel::<ClientMessage>(OUTBOUND_CAPACITY);

    let response = client
        .subscribe(tonic::Request::new(ReceiverStream::new(out_rx)))
        .await
        .map_err(BridgeError::GrpcCall)?;
    let mut grpc_stream = response.into_inner();
    info!("ws bridge: subscribe stream opened");

    // Pump browser → upstream: decode each WS binary frame as a
    // ClientMessage and forward it to the gRPC outbound channel.
    let browser_to_grpc = async {
        while let Some(msg) = ws_stream.next().await {
            match msg.map_err(BridgeError::WsRecv)? {
                Message::Binary(bytes) => {
                    let cm = ClientMessage::decode(bytes.as_ref())
                        .map_err(BridgeError::DecodeClientMessage)?;
                    if out_tx.send(cm).await.is_err() {
                        // Upstream gRPC stream closed.
                        break;
                    }
                }
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => {
                    // axum handles ping/pong automatically.
                }
                Message::Text(_) => {
                    // Reject text frames so misuses fail loudly instead
                    // of silently dropping data on the floor.
                    return Err(BridgeError::UnexpectedTextFrame);
                }
            }
        }
        Ok::<_, BridgeError>(())
    };

    // Pump upstream → browser: encode each ServerMessage as protobuf
    // and ship it as a WS binary frame.
    let grpc_to_browser = async {
        while let Some(msg) = grpc_stream.next().await {
            let server_msg: ServerMessage = msg.map_err(BridgeError::GrpcRecv)?;
            let mut buf = Vec::with_capacity(server_msg.encoded_len());
            server_msg
                .encode(&mut buf)
                .map_err(BridgeError::EncodeServerMessage)?;
            ws_sink
                .send(Message::Binary(buf))
                .await
                .map_err(BridgeError::WsSend)?;
        }
        Ok::<_, BridgeError>(())
    };

    // Whichever direction terminates first cancels the other via the
    // tokio::select! drop.
    tokio::select! {
        res = browser_to_grpc => res?,
        res = grpc_to_browser => res?,
    }
    info!("ws bridge: subscribe stream closed");
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
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GrpcCall(s) => write!(f, "grpc subscribe call: {s}"),
            Self::GrpcRecv(s) => write!(f, "grpc recv: {s}"),
            Self::WsRecv(e) => write!(f, "ws recv: {e}"),
            Self::WsSend(e) => write!(f, "ws send: {e}"),
            Self::DecodeClientMessage(e) => write!(f, "decode ClientMessage: {e}"),
            Self::EncodeServerMessage(e) => write!(f, "encode ServerMessage: {e}"),
            Self::UnexpectedTextFrame => write!(f, "unexpected ws text frame"),
        }
    }
}

impl std::error::Error for BridgeError {}
