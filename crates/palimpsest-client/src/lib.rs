// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Native Rust client for the Palimpsest sync engine (§18.10).
//!
//! ```no_run
//! use palimpsest_client::{Auth, Client};
//!
//! # async fn run() -> Result<(), palimpsest_client::ClientError> {
//! let client = Client::connect("http://127.0.0.1:50051", Auth::Anonymous).await?;
//! let mut sub = client.subscribe("SELECT id FROM posts").await?;
//! while let Some(event) = sub.next_event().await {
//!     println!("event: {event:?}");
//! }
//! # Ok(()) }
//! ```
//!
//! Each [`Client`] owns one bidi `Subscribe` RPC; every subscription
//! issued through it multiplexes onto that single stream. The internal
//! connection manager handles reconnect-with-backoff transparently and
//! resubscribes with the user's last acked LSN as `resume_lsn`.
//!
//! The same crate also compiles for `wasm32-unknown-unknown` (§18.11).
//! On wasm the transport switches to `tonic-web-wasm-client` and the
//! manager runs on the `wasm-bindgen-futures` event loop instead of
//! Tokio.

#![warn(missing_docs)]

mod auth;
mod cache;
mod connection;
mod error;
mod reconnect;
mod runtime;
mod subscription;
mod transport;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

use connection::{Command, ConnectionInbox, ConnectionTask};
use runtime::TaskHandle;

pub use auth::Auth;
pub use cache::{LocalCache, PrimaryKey};
pub use error::ClientError;
pub use palimpsest_proto::palimpsest::sync::v1::{
    var_value, DatumType, DiffOp, ResyncReason, VarValue,
};
pub use palimpsest_proto::wire::{WireDatum, WireRow};
pub use reconnect::BackoffConfig;
pub use subscription::{DiffEvent, Subscription};

/// Optional construction-time knobs.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Reconnect backoff schedule.
    pub backoff: BackoffConfig,
    /// Whether each subscription should maintain a primary-key cache
    /// (default `true`).
    pub cache_enabled: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            backoff: BackoffConfig::default(),
            cache_enabled: true,
        }
    }
}

/// Native Rust client. Cheap to clone — wraps a single shared
/// connection manager.
#[derive(Clone)]
pub struct Client {
    inbox: ConnectionInbox,
    config: Arc<ClientConfig>,
    next_subscription_id: Arc<AtomicU64>,
    /// Owned by the first cloned [`Client`]; dropped on shutdown.
    join: Arc<Mutex<Option<TaskHandle>>>,
}

impl Client {
    /// Connect to a Palimpsest server.
    ///
    /// `url` should be a `http(s)://host:port` URL — TLS is gated on
    /// the scheme.
    ///
    /// # Errors
    /// * [`ClientError::Endpoint`] if `url` cannot be parsed.
    /// * Native: [`ClientError::Transport`] surfaces dial-time TLS
    ///   failures (rare — the manager defers to background reconnect).
    pub async fn connect(url: impl AsRef<str>, auth: Auth) -> Result<Self, ClientError> {
        Self::connect_with(url, auth, ClientConfig::default()).await
    }

    /// Connect with non-default [`ClientConfig`].
    ///
    /// `async` purely so the public surface lines up with
    /// [`Self::connect`] — the actual handshake happens lazily inside
    /// the manager task.
    ///
    /// # Errors
    /// See [`Self::connect`].
    #[allow(clippy::unused_async)]
    pub async fn connect_with(
        url: impl AsRef<str>,
        auth: Auth,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        let endpoint = transport::parse_endpoint(url.as_ref())?;
        let (inbox, join) = ConnectionTask::spawn(endpoint, auth, config.backoff.clone());
        Ok(Self {
            inbox,
            config: Arc::new(config),
            next_subscription_id: Arc::new(AtomicU64::new(1)),
            join: Arc::new(Mutex::new(Some(join))),
        })
    }

    /// Subscribe to a SQL query with no variables.
    ///
    /// # Errors
    /// [`ClientError::ConnectionClosed`] if the manager has shut down.
    pub async fn subscribe(&self, sql: impl Into<String>) -> Result<Subscription, ClientError> {
        self.subscribe_with(sql, HashMap::new()).await
    }

    /// Subscribe with explicit variable bindings.
    ///
    /// # Errors
    /// [`ClientError::ConnectionClosed`] if the manager has shut down.
    pub async fn subscribe_with(
        &self,
        sql: impl Into<String>,
        vars: HashMap<String, VarValue>,
    ) -> Result<Subscription, ClientError> {
        let id = format!(
            "sub-{}",
            self.next_subscription_id.fetch_add(1, Ordering::Relaxed)
        );
        let (events_tx, events_rx) = mpsc::channel(256);
        let cache = if self.config.cache_enabled {
            Some(Arc::new(Mutex::new(LocalCache::default())))
        } else {
            None
        };
        self.inbox
            .send(Command::Subscribe {
                subscription_id: id.clone(),
                sql: sql.into(),
                vars,
                events_tx,
                cache: cache.clone(),
            })
            .await?;
        Ok(Subscription {
            id,
            inbox: self.inbox.clone(),
            events: events_rx,
            cache,
        })
    }

    /// Initiate graceful shutdown — closes the bidi stream and resolves
    /// every outstanding [`Subscription`] stream.
    pub async fn shutdown(self) {
        connection::request_shutdown(&self.inbox).await;
        let join_handle = self.join.lock().await.take();
        if let Some(handle) = join_handle {
            handle.join().await;
        }
    }
}
