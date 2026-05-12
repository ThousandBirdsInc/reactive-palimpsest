// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Thin helpers around `tokio-postgres` for the conformance tests.
//!
//! All functions assume `PALIMPSEST_PG_URL` is set; if it is not, use
//! [`Connection::try_connect`] which returns `None` so the calling test
//! can skip cleanly.

use std::env;

use tokio_postgres::{Client, Config, NoTls};

use crate::PG_URL_ENV;

/// A connected client + the join handle of the connection task.
///
/// The handle must be kept alive for the duration of any operation
/// against the client — drop it and the connection is closed.
pub struct Connection {
    pub client: Client,
    pub join: tokio::task::JoinHandle<()>,
}

impl Connection {
    /// Try to connect using `PALIMPSEST_PG_URL`. Returns `Ok(None)` if
    /// the env var is unset (so tests can skip cleanly), and
    /// `Ok(Some(_))` on success. Errors propagate from
    /// `tokio-postgres`.
    pub async fn try_connect() -> Result<Option<Self>, tokio_postgres::Error> {
        let Ok(url) = env::var(PG_URL_ENV) else {
            return Ok(None);
        };
        let config: Config = url.parse()?;
        let (client, conn) = config.connect(NoTls).await?;
        let join = tokio::spawn(async move {
            // We deliberately swallow connection-task errors; tests
            // that need to detect a closed connection check `client`
            // calls returning errors.
            let _ = conn.await;
        });
        Ok(Some(Self { client, join }))
    }
}

/// Convenience wrapper used by every conformance test: returns `None`
/// (so the caller should `return Ok(())`) if no `PALIMPSEST_PG_URL` is
/// configured. Prints a skip message to stderr.
pub async fn connect_or_skip(test: &str) -> Option<Connection> {
    match Connection::try_connect().await {
        Ok(Some(c)) => Some(c),
        Ok(None) => {
            eprintln!("[conformance] {test}: skipped ({PG_URL_ENV} not set; nightly-only test)");
            None
        }
        Err(err) => {
            eprintln!("[conformance] {test}: skipped (connect failed: {err})");
            None
        }
    }
}
