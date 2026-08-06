// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Top-level error returned from the subscription router.

use thiserror::Error;

use crate::{codec::CodecError, subscription::SubscriptionId};

/// Anything that can go wrong while routing a subscription.
#[derive(Debug, Error)]
pub enum RouterError {
    /// A `subscribe` request reused a `(connection, client_subscription_id)`
    /// pair that already had an active subscription.
    #[error("client subscription id already in use for this connection")]
    DuplicateClientSubscriptionId,
    /// The subscription id is unknown — either it has never existed or
    /// it was already torn down.
    #[error("unknown subscription id {0:?}")]
    UnknownSubscription(SubscriptionId),
    /// Resume LSN is older than the trace's current compaction frontier.
    #[error("resume lsn falls outside compaction window")]
    ResumeOutOfWindow,
    /// Initial-snapshot fetch failed (the upstream snapshot provider
    /// surfaced an error).
    #[error("snapshot provider error: {0}")]
    Snapshot(String),
    /// Permission compilation or rewrite failed.
    #[error(transparent)]
    Permission(#[from] palimpsest_permissions::PermissionError),
    /// Row-visibility rules apply to one of the query's tables, but the
    /// query has no compiled dataflow plan — the only execution path
    /// left is the raw pass-through, which cannot enforce the filters.
    /// The subscribe fails closed instead of serving unfiltered rows.
    #[error(
        "row-visibility rules apply to table {table:?} but the query has no compiled dataflow \
         plan; refusing to serve unfiltered rows"
    )]
    PermissionUnenforceable {
        /// First rule-guarded table the query reads.
        table: String,
    },
    /// Diff codec failure (bincode encode/decode).
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Channel backpressure threshold tripped — caller should issue
    /// `Resync`.
    #[error("subscription channel saturated; resync required")]
    ChannelSaturated,
}
