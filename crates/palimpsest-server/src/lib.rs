// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Palimpsest server runtime.
//!
//! This crate hosts the **subscription router** described in §10 and
//! §18.7 of `DESIGN.md`. The router is intentionally transport-agnostic:
//! it takes a freshly-built dataflow (via [`palimpsest_dataflow`]),
//! manages per-subscription state (channel, cursor, watermark, resume
//! token), applies permission rewriting (via
//! [`palimpsest_permissions`]) and surfaces the resulting diff stream
//! through a small set of typed APIs that a gRPC frontend (§18.8) can
//! adapt onto `tonic`.
//!
//! The subscription model is the `LiveGraph` default: per-subscription
//! at-least-once + in-order delivery (§14.2), client-driven LSN acks
//! that contribute to a global compaction frontier (§9.4), and a
//! `Resync` escape hatch when the bounded channel saturates or when the
//! requested `resume_lsn` falls outside the compaction window.

#![warn(missing_docs)]

pub mod ack;
pub mod auth;
pub mod backpressure;
pub mod codec;
pub mod cursor;
pub mod diff;
pub mod embed;
pub mod error;
pub mod grpc;
pub mod metrics;
pub mod metrics_endpoint;
pub mod permissions;
pub mod registry;
pub mod resume;
pub mod router;
pub mod security;
pub mod snapshot;
pub mod subscription;
pub mod tracing_setup;
pub mod wal_runtime;

pub use ack::{AckOutcome, AckTracker};
pub use auth::{
    AnonymousAuthenticator, AuthError, Authenticator, DynAuthenticator, JwtAuthConfig,
    JwtAuthenticator,
};
pub use backpressure::{BackpressureOutcome, BackpressurePolicy, BoundedDiffChannel};
pub use codec::{decode_rows, encode_row, encode_rows, CodecError};
pub use cursor::{batch_by_lsn, LsnBatch, QueryTransactionDelta, RawDiff, TraceCursor, VecCursor};
pub use diff::{DiffEvent, DiffOp, DiffPayload, ResyncReason, RowChange};
pub use embed::{Palimpsest, PalimpsestBuilder, PalimpsestHandle, ServerConfig};
pub use error::RouterError;
pub use grpc::SyncEngineService;
pub use metrics::{MetricsSnapshot, ResyncReasonSnapshot, RouterMetrics};
pub use metrics_endpoint::{
    build_router as build_metrics_router, build_router_with as build_metrics_router_with,
    render_prometheus, HealthConfig,
};
pub use permissions::install_permission_filters;
pub use registry::SubscriptionRegistry;
pub use resume::{CompactionWindow, ResumeDecision};
pub use router::{
    ResumeRequest, RouterConfig, SubscribeRequest, SubscribeResponse, SubscriptionRouter,
};
pub use security::{
    ConnectionLimiter, LimitDecision, ReconnectTracker, SecurityLimits, SlidingWindowSpec,
    TokenBucketSpec,
};
pub use snapshot::{snapshot_to_seed_updates, SnapshotBatch, SnapshotProvider, SnapshotTableRows};
pub use subscription::{
    ClientSubscriptionId, ColumnSpec, ConnectionId, QueryId, SchemaDefinition, SchemaId,
    Subscription, SubscriptionId, SubscriptionState,
};
pub use wal_runtime::{EmptyWalRuntime, WalRuntime};
