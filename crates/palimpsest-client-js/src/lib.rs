// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `wasm-bindgen` shim around [`palimpsest_client`] for use from JS
//! (§18.11). The native crate already compiles for
//! `wasm32-unknown-unknown` — this crate exposes a JS-friendly surface
//! on top of it.
//!
//! ```js
//! import init, { Client } from "./pkg/palimpsest_client_js.js";
//! await init();
//! const c = await Client.connect("https://api/grpc");
//! const sub = await c.subscribe("SELECT id FROM posts");
//! sub.on_diff((event) => console.log(event));
//! ```
//!
//! The whole module is gated on `cfg(target_arch = "wasm32")`. On
//! native, this crate is empty — building it just produces an `rlib`
//! with no symbols, which is what `cargo doc` and the workspace-wide
//! test runs expect.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

#[cfg(all(target_arch = "wasm32", feature = "wee_alloc"))]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc<'_> = wee_alloc::WeeAlloc::INIT;

#[cfg(target_arch = "wasm32")]
mod bindings;
#[cfg(target_arch = "wasm32")]
mod local;

#[cfg(target_arch = "wasm32")]
pub use bindings::*;
#[cfg(target_arch = "wasm32")]
pub use local::LocalReplica;
