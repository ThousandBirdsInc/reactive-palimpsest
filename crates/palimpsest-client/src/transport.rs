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
//! WS-to-gRPC bridge that re-presents the same protocol. The socket is
//! driven straight through `web_sys` — four event callbacks and a
//! channel — rather than a WebSocket framework crate, keeping the
//! browser bundle's dependency closure small.
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
use crate::status::Status;

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
    Auth(Box<Status>),
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
) -> Result<mpsc::Receiver<Result<ServerMessage, Status>>, OpenError> {
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
    use palimpsest_proto::palimpsest::sync::v1::sync_engine_client::SyncEngineClient;
    use palimpsest_proto::palimpsest::sync::v1::{ClientMessage, ServerMessage};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::StreamExt;
    use tonic::{Code, Request};

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
    use std::cell::Cell;
    use std::rc::Rc;

    use palimpsest_proto::palimpsest::sync::v1::{ClientMessage, ServerMessage};
    use prost::Message as _;
    use tokio::sync::{mpsc, watch};
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::spawn_local;
    use web_sys::{BinaryType, CloseEvent, MessageEvent, WebSocket};

    use super::{Endpoint, OpenError};
    use crate::auth::Auth;
    use crate::status::Status;

    /// WS close code the demo's bridge uses to signal auth rejection.
    /// Mirrors RFC 6455 §7.4 "Policy Violation".
    const CLOSE_POLICY_VIOLATION: u16 = 1008;

    /// `WebSocket.readyState` values.
    const CONNECTING: u16 = 0;
    const OPEN: u16 = 1;

    /// Socket lifecycle as observed by the outbound pump.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Phase {
        Connecting,
        Open,
        Closed,
    }

    /// Frames the browser hands us, in event order. Errors terminate
    /// the stream: the forwarder stops after relaying one.
    enum Frame {
        Message(Result<ServerMessage, Status>),
        Error(Status),
    }

    // `async` is kept for parity with the native `open_subscribe` (which does
    // await); both are selected by `#[cfg]` and awaited at the same call site.
    // The browser path delegates its awaits to `spawn_local` tasks.
    #[allow(clippy::unused_async)]
    pub(super) async fn open_subscribe(
        endpoint: &Endpoint,
        auth: &Auth,
        mut outbound_rx: mpsc::Receiver<ClientMessage>,
        inbound_capacity: usize,
    ) -> Result<mpsc::Receiver<Result<ServerMessage, Status>>, OpenError> {
        let url = normalize_ws_url(endpoint, auth);
        let socket = Socket::open(&url).map_err(|err| {
            warn!(?err, %url, "ws open failed");
            OpenError::Transient
        })?;
        let Socket {
            ws,
            mut phase_rx,
            mut frames_rx,
            listeners,
        } = socket;
        // Both pumps keep the listeners alive; the last one to finish
        // drops them and closes the socket (see `Listeners::drop`).
        let listeners = Rc::new(listeners);

        let (inbound_tx, inbound_rx) =
            mpsc::channel::<Result<ServerMessage, Status>>(inbound_capacity);

        // browser → server: wait for the socket to open, then drain
        // outbound_rx, encode ClientMessage, ship as a binary frame.
        // Outbound channel closed → close the WS gracefully.
        let send_ws = ws;
        let send_listeners = Rc::clone(&listeners);
        spawn_local(async move {
            let _keep_alive = send_listeners;
            while *phase_rx.borrow() == Phase::Connecting {
                if phase_rx.changed().await.is_err() {
                    return;
                }
            }
            if *phase_rx.borrow() == Phase::Closed {
                return;
            }
            while let Some(msg) = outbound_rx.recv().await {
                let mut buf = Vec::with_capacity(msg.encoded_len());
                if msg.encode(&mut buf).is_err() {
                    break;
                }
                if send_ws.send_with_u8_array(&buf).is_err() {
                    break;
                }
            }
            let _ = send_ws.close();
        });

        // server → browser: forward decoded frames into the bounded
        // inbound channel as `Ok(_)`. Decode failures are surfaced as
        // `Status::data_loss` so the connection manager treats them
        // like a normal stream error. Auth-related close frames (code
        // 1008) are surfaced as `Unauthenticated` so `OpenError::Auth`
        // propagates up and the manager stops reconnecting instead of
        // hot-looping against the bridge.
        spawn_local(async move {
            let _keep_alive = listeners;
            while let Some(frame) = frames_rx.recv().await {
                let res = match frame {
                    Frame::Message(res) => res,
                    Frame::Error(status) => Err(status),
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

    /// A `web_sys::WebSocket` with its event listeners attached: open
    /// and close/error transitions feed `phase_rx`, every frame feeds
    /// `frames_rx`.
    struct Socket {
        ws: WebSocket,
        phase_rx: watch::Receiver<Phase>,
        frames_rx: mpsc::UnboundedReceiver<Frame>,
        listeners: Listeners,
    }

    impl Socket {
        fn open(url: &str) -> Result<Self, JsValue> {
            let ws = WebSocket::new(url)?;
            // ArrayBuffer (not Blob) so frames can be read synchronously
            // inside the event callback and stay in delivery order.
            ws.set_binary_type(BinaryType::Arraybuffer);

            let (phase_tx, phase_rx) = watch::channel(Phase::Connecting);
            let (frames_tx, frames_rx) = mpsc::unbounded_channel();
            // Set once the close event has fired so a later error event
            // (browsers fire `error` *before* `close` on failure) never
            // surfaces after the close status.
            let closed = Rc::new(Cell::new(false));

            let on_open = {
                let phase_tx = phase_tx.clone();
                Closure::<dyn FnMut()>::new(move || {
                    let _ = phase_tx.send(Phase::Open);
                })
            };
            let on_message = {
                let frames_tx = frames_tx.clone();
                Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                    let _ = frames_tx.send(Frame::Message(decode_frame(&event.data())));
                })
            };
            let on_error = {
                let frames_tx = frames_tx.clone();
                let closed = Rc::clone(&closed);
                Closure::<dyn FnMut(JsValue)>::new(move |_event: JsValue| {
                    if closed.get() {
                        return;
                    }
                    // The forwarder stops at the first error, so the
                    // close event's status (which may carry the
                    // server's reason) wins when both arrive.
                    let _ = frames_tx.send(Frame::Error(Status::unavailable(
                        "ws recv: WebSocket connection failed",
                    )));
                })
            };
            let on_close = {
                Closure::<dyn FnMut(CloseEvent)>::new(move |event: CloseEvent| {
                    closed.set(true);
                    let _ = frames_tx.send(Frame::Error(close_status(&event)));
                    let _ = phase_tx.send(Phase::Closed);
                })
            };

            ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
            ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
            ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
            ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

            Ok(Self {
                ws: ws.clone(),
                phase_rx,
                frames_rx,
                listeners: Listeners {
                    ws,
                    _on_open: on_open,
                    _on_message: on_message,
                    _on_error: on_error,
                    _on_close: on_close,
                },
            })
        }
    }

    /// Owns the JS callbacks for the socket's lifetime. Dropping it
    /// detaches them *before* they are freed (so the browser can never
    /// call into a released closure) and closes the socket if it is
    /// still open.
    struct Listeners {
        ws: WebSocket,
        _on_open: Closure<dyn FnMut()>,
        _on_message: Closure<dyn FnMut(MessageEvent)>,
        _on_error: Closure<dyn FnMut(JsValue)>,
        _on_close: Closure<dyn FnMut(CloseEvent)>,
    }

    impl Drop for Listeners {
        fn drop(&mut self) {
            self.ws.set_onopen(None);
            self.ws.set_onmessage(None);
            self.ws.set_onerror(None);
            self.ws.set_onclose(None);
            if matches!(self.ws.ready_state(), CONNECTING | OPEN) {
                let _ = self.ws.close();
            }
        }
    }

    /// Decode one binary frame into a `ServerMessage`.
    fn decode_frame(data: &JsValue) -> Result<ServerMessage, Status> {
        let Some(buffer) = data.dyn_ref::<js_sys::ArrayBuffer>() else {
            return Err(Status::data_loss("unexpected ws text frame"));
        };
        let bytes = js_sys::Uint8Array::new(buffer).to_vec();
        ServerMessage::decode(bytes.as_slice())
            .map_err(|err| Status::data_loss(format!("decode ServerMessage: {err}")))
    }

    /// Map a close event onto a [`Status`] whose code drives the
    /// connection manager's retry vs. shutdown decision.
    fn close_status(event: &CloseEvent) -> Status {
        let code = event.code();
        let reason = event.reason();
        if code == CLOSE_POLICY_VIOLATION {
            Status::unauthenticated(reason)
        } else {
            Status::unavailable(format!(
                "ws recv: WebSocket Closed: code: {code}, reason: {reason}"
            ))
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
