// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Client-side error type.

use thiserror::Error;

use palimpsest_proto::wire;

/// All failures the client surface can produce.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Could not parse the destination URL.
    #[error("invalid endpoint: {0}")]
    Endpoint(String),
    /// Transport-level failure (connect, TLS, etc.). Native-only —
    /// `tonic::transport::Error` is gated on the `transport` feature
    /// which doesn't compile for `wasm32-unknown-unknown`.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// gRPC stream returned a non-OK status.
    #[error("grpc status: {0}")]
    Grpc(#[from] tonic::Status),
    /// The connection is closing or has shut down.
    #[error("connection closed")]
    ConnectionClosed,
    /// Wire codec failure decoding a `Diff`.
    #[error("codec: {0}")]
    Codec(#[from] wire::CodecError),
    /// The server sent a `ServerMessage` whose `kind` was unset.
    #[error("server sent message with no kind")]
    EmptyServerMessage,
    /// The server sent a `Diff` for a subscription that hasn't been
    /// `Accepted` yet (i.e. no schema registered).
    #[error("diff for unaccepted subscription `{0}`")]
    UnaccceptedSubscription(String),
    /// User-supplied SQL or vars failed validation client-side.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}
