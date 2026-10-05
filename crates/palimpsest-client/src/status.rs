// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The stream-level status type the connection manager reasons about.
//!
//! Natively this *is* [`tonic::Status`] — the bidi `Subscribe` RPC
//! yields real gRPC statuses and users can match on every code tonic
//! knows. On `wasm32-unknown-unknown` the transport is a WebSocket
//! and the only thing the manager needs from a status is "is this an
//! auth failure or a transient error, and what did the server say?",
//! so the browser build carries a two-field struct instead of pulling
//! `tonic` (and with it `http`, `tower`, `base64`, `percent-encoding`,
//! …) into the bundle for one error type.

#[cfg(not(target_arch = "wasm32"))]
pub use tonic::{Code, Status};

#[cfg(target_arch = "wasm32")]
pub use wasm::{Code, Status};

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::fmt;

    /// gRPC status codes the browser transport can produce. Mirrors
    /// the numbering of `tonic::Code` for the subset the WebSocket
    /// bridge surfaces.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[repr(i32)]
    pub enum Code {
        /// Catch-all for failures the bridge did not classify.
        Unknown = 2,
        /// The caller does not have permission for the subscription.
        PermissionDenied = 7,
        /// The stream failed in a way the manager should retry.
        Unavailable = 14,
        /// A frame could not be decoded.
        DataLoss = 15,
        /// The server rejected the bearer token.
        Unauthenticated = 16,
    }

    impl Code {
        /// Human-readable name, matching `tonic::Code::description`.
        #[must_use]
        pub const fn description(self) -> &'static str {
            match self {
                Self::Unknown => "Unknown error",
                Self::PermissionDenied => {
                    "The caller does not have permission to execute the specified operation"
                }
                Self::Unavailable => "The service is currently unavailable",
                Self::DataLoss => "Unrecoverable data loss or corruption",
                Self::Unauthenticated => {
                    "The request does not have valid authentication credentials"
                }
            }
        }
    }

    impl fmt::Display for Code {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.description())
        }
    }

    /// Lightweight stand-in for `tonic::Status` on the WebSocket
    /// transport: a [`Code`] plus the server-supplied message.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Status {
        code: Code,
        message: String,
    }

    impl Status {
        /// Build a status from a code and message.
        #[must_use]
        pub fn new(code: Code, message: impl Into<String>) -> Self {
            Self {
                code,
                message: message.into(),
            }
        }

        /// `Code::Unknown`.
        #[must_use]
        pub fn unknown(message: impl Into<String>) -> Self {
            Self::new(Code::Unknown, message)
        }

        /// `Code::PermissionDenied`.
        #[must_use]
        pub fn permission_denied(message: impl Into<String>) -> Self {
            Self::new(Code::PermissionDenied, message)
        }

        /// `Code::Unavailable`.
        #[must_use]
        pub fn unavailable(message: impl Into<String>) -> Self {
            Self::new(Code::Unavailable, message)
        }

        /// `Code::DataLoss`.
        #[must_use]
        pub fn data_loss(message: impl Into<String>) -> Self {
            Self::new(Code::DataLoss, message)
        }

        /// `Code::Unauthenticated`.
        #[must_use]
        pub fn unauthenticated(message: impl Into<String>) -> Self {
            Self::new(Code::Unauthenticated, message)
        }

        /// The status code.
        #[must_use]
        pub const fn code(&self) -> Code {
            self.code
        }

        /// The server-supplied (or transport-supplied) message.
        #[must_use]
        pub fn message(&self) -> &str {
            &self.message
        }
    }

    impl fmt::Display for Status {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "status: {:?}, message: {:?}", self.code, self.message)
        }
    }

    impl std::error::Error for Status {}
}
