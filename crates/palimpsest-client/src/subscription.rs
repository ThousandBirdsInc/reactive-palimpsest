// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Public `Subscription` handle and the [`DiffEvent`] enum that is
//! streamed back to user code.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::{mpsc, Mutex};

use palimpsest_proto::palimpsest::sync::v1::{DiffOp, ResyncReason, Schema};
use palimpsest_proto::wire::{WireRow, WireRowChange};

use crate::cache::LocalCache;
use crate::connection::{Command, ConnectionInbox};
use crate::error::ClientError;

/// Friendly typed event delivered on a subscription stream.
#[derive(Debug)]
pub enum DiffEvent {
    /// Server confirmed the subscription. Emitted exactly once per
    /// session (and again on reconnect).
    Accepted {
        /// Wire-side schema id (also pre-registered in the codec).
        schema_id: u64,
        /// Server snapshot LSN at subscription time.
        snapshot_lsn: u64,
        /// Column schema for subsequent diffs.
        schema: Schema,
    },
    /// Decoded diff payload.
    Diff {
        /// LSN this diff is delivered at.
        lsn: u64,
        /// The kind of change.
        op: DiffOp,
        /// Decoded rows.
        rows: Vec<WireRow>,
    },
    /// Decoded transaction envelope applied as one cache mutation.
    Transaction {
        /// Commit LSN for the complete transaction.
        commit_lsn: u64,
        /// Begin marker LSN, when supplied by the server.
        begin_lsn: Option<u64>,
        /// End marker LSN, when supplied by the server.
        end_lsn: Option<u64>,
        /// `PostgreSQL` transaction id, when supplied by the server.
        transaction_id: Option<u32>,
        /// Per-row changes in this transaction.
        changes: Vec<WireRowChange>,
    },
    /// Server signalled a forced resync.
    Resync {
        /// Why the resync was forced.
        reason: ResyncReason,
        /// Free-form server-side detail.
        message: String,
    },
    /// Server-side per-subscription error.
    Error {
        /// Error code (server-defined).
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

/// User-facing handle to one active subscription.
///
/// `Subscription` impls [`Stream`] yielding `Result<DiffEvent>`. When
/// the underlying connection is reconnecting, the stream stays open and
/// the handle is automatically re-subscribed with `resume_lsn`.
pub struct Subscription {
    pub(crate) id: String,
    pub(crate) inbox: ConnectionInbox,
    pub(crate) events: mpsc::Receiver<Result<DiffEvent, ClientError>>,
    pub(crate) cache: Option<Arc<Mutex<LocalCache>>>,
}

impl Subscription {
    /// Server-assigned subscription id (echoes the client-supplied id).
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Pull the next event off the stream, returning `None` when the
    /// stream has been closed by `unsubscribe` or by client shutdown.
    pub async fn next_event(&mut self) -> Option<Result<DiffEvent, ClientError>> {
        self.events.recv().await
    }

    /// Re-issue the query with new variable bindings (server replays
    /// the result set as a fresh `Initial`/diff sequence).
    ///
    /// # Errors
    /// [`ClientError::ConnectionClosed`] if the manager has shut down.
    pub async fn update(
        &self,
        vars: std::collections::HashMap<String, palimpsest_proto::palimpsest::sync::v1::VarValue>,
    ) -> Result<(), ClientError> {
        self.inbox
            .send(Command::Update {
                subscription_id: self.id.clone(),
                vars,
            })
            .await
    }

    /// Acknowledge that LSN `lsn` has been durably applied.
    ///
    /// # Errors
    /// [`ClientError::ConnectionClosed`] if the manager has shut down.
    pub async fn ack(&self, lsn: u64) -> Result<(), ClientError> {
        self.inbox
            .send(Command::Ack {
                subscription_id: self.id.clone(),
                lsn,
            })
            .await
    }

    /// Tear down the subscription on the server. After this returns
    /// the stream will eventually emit `None`.
    ///
    /// # Errors
    /// [`ClientError::ConnectionClosed`] if the manager has shut down.
    pub async fn unsubscribe(self) -> Result<(), ClientError> {
        self.inbox
            .send(Command::Unsubscribe {
                subscription_id: self.id.clone(),
            })
            .await
    }

    /// Snapshot of the current cache state, if caching is enabled.
    pub async fn cache_snapshot(&self) -> Option<LocalCache> {
        match self.cache.as_ref() {
            Some(cache) => Some(cache.lock().await.clone()),
            None => None,
        }
    }
}

impl Stream for Subscription {
    type Item = Result<DiffEvent, ClientError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.events.poll_recv(cx)
    }
}
