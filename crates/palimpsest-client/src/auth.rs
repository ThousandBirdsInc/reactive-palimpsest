// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Authentication strategies the client can present to the server.

#![allow(clippy::redundant_pub_crate)]

#[cfg(not(target_arch = "wasm32"))]
use tonic::metadata::{MetadataMap, MetadataValue};

/// How the client authenticates the bidi `Subscribe` stream.
#[derive(Debug, Clone, Default)]
pub enum Auth {
    /// No `Authorization` header — server must accept anonymous calls.
    #[default]
    Anonymous,
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// Pre-formed `Authorization` header value (e.g. `"Basic ..."`).
    Raw(String),
}

impl Auth {
    /// Convenience for the bearer variant.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer(token.into())
    }

    /// Inserts the appropriate `Authorization` header into `metadata`.
    /// Anonymous is a no-op.
    ///
    /// Native-only: the wasm build uses a WebSocket transport that
    /// cannot set per-request headers (the token travels in the URL
    /// instead), and it does not link `tonic` at all.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn apply(&self, metadata: &mut MetadataMap) -> Result<(), AuthApplyError> {
        let header = match self {
            Self::Anonymous => return Ok(()),
            Self::Bearer(token) => format!("Bearer {token}"),
            Self::Raw(value) => value.clone(),
        };
        let value =
            MetadataValue::try_from(header).map_err(|err| AuthApplyError(err.to_string()))?;
        metadata.insert("authorization", value);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
#[error("auth header rejected by tonic: {0}")]
pub(crate) struct AuthApplyError(String);
