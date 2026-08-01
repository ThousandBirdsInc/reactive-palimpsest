// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Transport abstraction for the bidi `Subscribe` RPC.
//!
//! Native uses `tonic::transport::Endpoint`/`Channel` (HTTP/2 gRPC).
//!
//! `wasm32-unknown-unknown` uses a WebSocket carrying protobuf-encoded
//! `ClientMessage`/`ServerMessage` frames. gRPC-Web cannot drive a bidi
//! stream in browsers (it requires fetch upload streaming, which only
//! Chromium ships), so the server-side stack pairs this client with a
//! WS-to-gRPC bridge that re-presents the same protocol.
//!
//! Both targets expose a single transport entry point —
//! [`open_subscribe`] — that returns an [`mpsc::Receiver`] of decoded
//! [`ServerMessage`]s and consumes [`ClientMessage`]s through a paired
//! [`mpsc::Receiver`] supplied by the caller. The connection manager
//! (see `connection.rs`) is target-agnostic above this line.

#![allow(
    clippy::redundant_pub_crate,
    // The wasm path constructs JS types under `wasm_bindgen_futures::spawn_local`,
    // which doesn't require Send. The native path returns a Tokio-backed
    // channel that *is* Send. Keeping a single shared signature.
    clippy::future_not_send,
)]

use palimpsest_proto::palimpsest::sync::v1::{ClientMessage, ServerMessage};
use tokio::sync::mpsc;

use crate::auth::Auth;
use crate::error::ClientError;

/// Bounded depth for the inbound `ServerMessage` channel handed back to
/// the connection manager. Should comfortably hold one connection's
/// worth of in-flight events without becoming a backpressure stall on
/// the consumer.
const INBOUND_CAPACITY: usize = 256;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Endpoint = tonic::transport::Endpoint;

#[cfg(target_arch = "wasm32")]
pub(crate) type Endpoint = String;

/// Errors from a single `open_subscribe` attempt. The connection
/// manager uses [`OpenError::Auth`] as a "hard stop" signal — every
/// other variant triggers reconnect with backoff.
pub(crate) enum OpenError {
    /// Server rejected the handshake with `Unauthenticated` /
    /// `PermissionDenied`. The manager should shut down rather than
    /// loop reconnecting against a credential we know is bad.
    ///
    /// Only the native transport produces this today; the WS bridge
    /// currently accepts anonymous connections.
    // Boxed: `tonic::Status` is ~176 bytes; keeping it inline would trip
    // clippy's `result_large_err` on every `Result<_, OpenError>`.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    Auth(Box<tonic::Status>),
    /// Anything else — dial failure, transient gRPC error, WS handshake
    /// rejection, etc. The manager reconnects.
    Transient,
}

/// Parse a user-facing URL into the platform-specific [`Endpoint`].
#[allow(clippy::result_large_err)]
pub(crate) fn parse_endpoint(url: &str) -> Result<Endpoint, ClientError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tonic::transport::Endpoint::from_shared(url.to_owned())
            .map_err(|err| ClientError::Endpoint(format!("{url}: {err}")))
    }
    #[cfg(target_arch = "wasm32")]
    {
        if url.is_empty() {
            return Err(ClientError::Endpoint("empty url".into()));
        }
        Ok(url.to_owned())
    }
}

/// Open the bidi `Subscribe` stream. Returns an [`mpsc::Receiver`] of
/// decoded server messages; the manager pushes outbound messages into
/// the supplied `outbound_rx` channel (consumed by the transport).
///
/// Implementations spawn a background task per direction; both halves
/// terminate together when either side closes.
pub(crate) async fn open_subscribe(
    endpoint: &Endpoint,
    auth: &Auth,
    outbound_rx: mpsc::Receiver<ClientMessage>,
) -> Result<mpsc::Receiver<Result<ServerMessage, tonic::Status>>, OpenError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::open_subscribe(endpoint, auth, outbound_rx, INBOUND_CAPACITY).await
    }
    #[cfg(target_arch = "wasm32")]
    {
        wasm::open_subscribe(endpoint, auth, outbound_rx, INBOUND_CAPACITY).await
    }
}

// -----------------------------------------------------------------------------
// Native (tonic) implementation
// -----------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use futures::StreamExt;
    use palimpsest_proto::palimpsest::sync::v1::sync_engine_client::SyncEngineClient;
    use palimpsest_proto::palimpsest::sync::v1::{ClientMessage, ServerMessage};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Code, Request};
    use tracing::warn;

    use super::{Endpoint, OpenError};
    use crate::auth::Auth;

    pub(super) async fn open_subscribe(
        endpoint: &Endpoint,
        auth: &Auth,
        outbound_rx: mpsc::Receiver<ClientMessage>,
        inbound_capacity: usize,
    ) -> Result<mpsc::Receiver<Result<ServerMessage, tonic::Status>>, OpenError> {
        let channel = endpoint.connect().await.map_err(|err| {
            warn!(?err, "connect failed");
            OpenError::Transient
        })?;
        let mut client = SyncEngineClient::new(channel);

        let mut request = Request::new(ReceiverStream::new(outbound_rx));
        if let Err(err) = auth.apply(request.metadata_mut()) {
            warn!(?err, "auth header rejected");
            return Err(OpenError::Auth(Box::new(tonic::Status::unauthenticated(
                err.to_string(),
            ))));
        }

        let mut stream = match client.subscribe(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                if matches!(
                    status.code(),
                    Code::Unauthenticated | Code::PermissionDenied
                ) {
                    return Err(OpenError::Auth(Box::new(status)));
                }
                warn!(?status, "subscribe handshake failed");
                return Err(OpenError::Transient);
            }
        };

        let (inbound_tx, inbound_rx) = mpsc::channel(inbound_capacity);
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                if inbound_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });
        Ok(inbound_rx)
    }
}

// -----------------------------------------------------------------------------
// Wasm (WebSocket) implementation
// -----------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
mod wasm {
    use futures::{SinkExt, StreamExt};
    use gloo_net::websocket::futures::WebSocket;
    use gloo_net::websocket::{Message as WsMessage, WebSocketError};
    use palimpsest_proto::palimpsest::sync::v1::{ClientMessage, ServerMessage};
    use prost::Message as _;
    use tokio::sync::mpsc;
    use tracing::warn;
    use wasm_bindgen_futures::spawn_local;

    use super::{Endpoint, OpenError};
    use crate::auth::Auth;

    /// WS close code the demo's bridge uses to signal auth rejection.
    /// Mirrors RFC 6455 §7.4 "Policy Violation".
    const CLOSE_POLICY_VIOLATION: u16 = 1008;

    // `async` is kept for parity with the native `open_subscribe` (which does
    // await); both are selected by `#[cfg]` and awaited at the same call site.
    // The browser path delegates its awaits to `spawn_local` tasks.
    #[allow(clippy::unused_async)]
    pub(super) async fn open_subscribe(
        endpoint: &Endpoint,
        auth: &Auth,
        mut outbound_rx: mpsc::Receiver<ClientMessage>,
        inbound_capacity: usize,
    ) -> Result<mpsc::Receiver<Result<ServerMessage, tonic::Status>>, OpenError> {
        let url = normalize_ws_url(endpoint, auth);
        let ws = WebSocket::open(&url).map_err(|err| {
            warn!(?err, %url, "ws open failed");
            OpenError::Transient
        })?;
        let (mut ws_sink, mut ws_stream) = ws.split();

        let (inbound_tx, inbound_rx) =
            mpsc::channel::<Result<ServerMessage, tonic::Status>>(inbound_capacity);

        // browser → server: drain outbound_rx, encode ClientMessage,
        // ship as a binary WS frame.
        spawn_local(async move {
            while let Some(msg) = outbound_rx.recv().await {
                let mut buf = Vec::with_capacity(msg.encoded_len());
                if msg.encode(&mut buf).is_err() {
                    break;
                }
                if ws_sink.send(WsMessage::Bytes(buf)).await.is_err() {
                    break;
                }
            }
            // Outbound channel closed → close the WS gracefully.
            let _ = ws_sink.close().await;
        });

        // server → browser: read binary frames, decode ServerMessage,
        // forward into the inbound channel as `Ok(_)`. Decode failures
        // are surfaced as `tonic::Status::data_loss` so the connection
        // manager treats them like a normal stream error. Auth-related
        // close frames (code 1008) are surfaced as `Unauthenticated`
        // so `OpenError::Auth` propagates up and the manager stops
        // reconnecting instead of hot-looping against the bridge.
        spawn_local(async move {
            while let Some(item) = ws_stream.next().await {
                let res = match item {
                    Ok(WsMessage::Bytes(bytes)) => {
                        ServerMessage::decode(bytes.as_slice()).map_err(|err| {
                            tonic::Status::data_loss(format!("decode ServerMessage: {err}"))
                        })
                    }
                    Ok(WsMessage::Text(_)) => {
                        Err(tonic::Status::data_loss("unexpected ws text frame"))
                    }
                    Err(err) => Err(map_ws_error_to_status(&err)),
                };
                let stop = res.is_err();
                if inbound_tx.send(res).await.is_err() {
                    break;
                }
                if stop {
                    break;
                }
            }
        });

        Ok(inbound_rx)
    }

    /// Map a `gloo_net::websocket::WebSocketError` onto a
    /// `tonic::Status` whose code drives the connection manager's
    /// retry vs. shutdown decision.
    fn map_ws_error_to_status(err: &WebSocketError) -> tonic::Status {
        match err {
            WebSocketError::ConnectionClose(close_event)
                if close_event.code == CLOSE_POLICY_VIOLATION =>
            {
                tonic::Status::unauthenticated(close_event.reason.clone())
            }
            other => tonic::Status::unavailable(format!("ws recv: {other:?}")),
        }
    }

    /// Map a user-supplied URL + auth onto a valid `ws://`/`wss://`
    /// URL.
    ///
    /// * `http://`/`https://` → swapped to `ws://`/`wss://` so callers
    ///   can pass the same origin they use for REST.
    /// * URLs without a path get `/ws/subscribe` appended — that's the
    ///   route the demo's nginx proxies to the WS bridge.
    /// * A bearer token from `auth` is appended as `?token=<jwt>` (or
    ///   `&token=…` if the URL already carries a query). Browsers
    ///   cannot set `Authorization` headers on a WS handshake, so the
    ///   server-side bridge reads the token off the URL and re-presents
    ///   it as gRPC metadata to the inner `SyncEngine`.
    /// * Anything already `ws(s)://` passes through unchanged.
    fn normalize_ws_url(url: &str, auth: &Auth) -> String {
        let mut out = if let Some(rest) = url.strip_prefix("http://") {
            format!("ws://{rest}")
        } else if let Some(rest) = url.strip_prefix("https://") {
            format!("wss://{rest}")
        } else {
            url.to_owned()
        };
        if !path_present(&out) {
            out.push_str("/ws/subscribe");
        }
        if let Some(token) = bearer_token(auth) {
            let separator = if out.contains('?') { '&' } else { '?' };
            out.push(separator);
            out.push_str("token=");
            out.push_str(&url_encode(token));
        }
        out
    }

    /// True if the URL has anything after the authority component —
    /// i.e. a `/path`. Used as a cheap "did the caller already specify
    /// a route?" check.
    fn path_present(url: &str) -> bool {
        // Skip the scheme://
        let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
        after_scheme.contains('/')
    }

    /// Extract the bearer token from `auth`, if any. Returns `None`
    /// for `Anonymous` and for `Raw` headers that aren't of the form
    /// `Bearer …`.
    fn bearer_token(auth: &Auth) -> Option<&str> {
        match auth {
            Auth::Anonymous => None,
            Auth::Bearer(token) => Some(token.as_str()),
            Auth::Raw(value) => value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer ")),
        }
    }

    /// Minimal percent-encoding for the token query parameter. JWTs
    /// only use URL-safe base64 (`A-Z a-z 0-9 - _`) plus `.`, all of
    /// which are unreserved per RFC 3986, so a token never actually
    /// needs encoding — but we still escape any non-unreserved byte
    /// defensively in case the caller passes something exotic.
    fn url_encode(input: &str) -> String {
        let mut out = String::with_capacity(input.len());
        for byte in input.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(byte as char);
                }
                _ => out.push_str(&format!("%{byte:02X}")),
            }
        }
        out
    }
}
