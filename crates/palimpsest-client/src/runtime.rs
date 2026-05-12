// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Tiny shim around the things we need a runtime for: spawning the
//! connection task and sleeping during reconnect-backoff.
//!
//! On native (`cfg(not(target_arch = "wasm32"))`) we delegate to the
//! ambient Tokio runtime. On `wasm32-unknown-unknown` we use
//! `wasm_bindgen_futures::spawn_local` and `gloo_timers` — which means
//! the manager future is not required to be `Send`. The bound on
//! [`spawn`] differs accordingly.

#![allow(
    clippy::redundant_pub_crate,
    // The native impls are async; the wasm impls are not. Keep the
    // signatures uniform so callers don't need cfg.
    clippy::unused_async,
    // The wasm spawn intentionally accepts `!Send` futures because
    // `wasm_bindgen_futures::spawn_local` runs on the JS event loop.
    clippy::future_not_send,
)]

use std::future::Future;
use std::time::Duration;

/// A platform-erased "task is running" handle. On native, awaiting it
/// joins the Tokio task; on wasm, joining is a no-op (`spawn_local`
/// hands the future to the JS event loop and never returns a handle).
pub(crate) struct TaskHandle {
    #[cfg(not(target_arch = "wasm32"))]
    inner: Option<tokio::task::JoinHandle<()>>,
}

impl TaskHandle {
    pub(crate) async fn join(mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(handle) = self.inner.take() {
            let _ = handle.await;
        }
        #[cfg(target_arch = "wasm32")]
        {
            // Nothing to await — `spawn_local` doesn't return a join handle.
            let _ = &mut self;
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn<F>(future: F) -> TaskHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    TaskHandle {
        inner: Some(tokio::spawn(future)),
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(future: F) -> TaskHandle
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
    TaskHandle {}
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn sleep(duration: Duration) {
    let ms = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(ms).await;
}
