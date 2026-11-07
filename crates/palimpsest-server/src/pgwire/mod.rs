// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Postgres wire-protocol front end.
//!
//! Lets any standard Postgres driver (`psql`, `tokio-postgres`, JDBC,
//! psycopg, ...) connect to the server and run `SELECT`s against the
//! same live queries the gRPC transport serves. Each statement is
//! answered from the materialized view of that query: the first
//! execution on a connection subscribes (compiling the plan and
//! taking the snapshot exactly as a gRPC subscribe would), and the
//! subscription is kept as an *anchor* for the rest of the connection
//! so the shared dataflow plan stays registered and its cursor pump
//! keeps folding WAL diffs into the cached view. Later executions of
//! the same statement read that view directly, which is what makes
//! the server usable as a high-performance materialized view for
//! other backend services.
//!
//! Scope: read-only. Startup, cleartext password authentication (the
//! password is the same bearer token gRPC clients send; anonymous
//! servers skip the prompt), the simple query cycle, and the extended
//! query cycle with text and binary formats. Permission rules apply
//! per user exactly as over gRPC. TLS is not terminated here; see
//! `docs/TLS.md` for putting a proxy in front of the listener.

mod protocol;
mod session;
mod types;

use std::io;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::grpc::QueryCore;

/// Accepts connections on `listener` until `shutdown` resolves, then
/// tells every live session to finish (they answer in-flight work,
/// send an `admin_shutdown` error and close).
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn serve_until<F>(
    listener: TcpListener,
    core: QueryCore,
    shutdown: F,
) -> io::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => {
                let _ = shutdown_tx.send(true);
                return Ok(());
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let _ = stream.set_nodelay(true);
                    debug!(%peer, "pgwire: connection accepted");
                    tokio::spawn(session::serve_connection(
                        core.clone(),
                        stream,
                        peer,
                        shutdown_rx.clone(),
                    ));
                }
                Err(err) => {
                    // Accept failures are usually fd exhaustion; back
                    // off instead of spinning.
                    warn!(%err, "pgwire: accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        }
    }
}
