//! PostgreSQL connection backends.
//!
//! This vendored Palimpsest fork keeps the pure Rust implementation only:
//!
//! - **`rustls-tls`**: Pure-Rust implementation using `rustls` with the
//!   `aws-lc-rs` crypto backend for hardware-accelerated TLS (AES-NI, AVX2).
//!   Requires `cmake` + C compiler at build time.
//!
//! It exposes the same public types: `PgReplicationConnection` and `PgResult`.

#[cfg(not(feature = "rustls-tls"))]
compile_error!(
    "The vendored Palimpsest pg-walstream fork requires the `rustls-tls` feature."
);

// ── rustls-tls backend ──────────────────────────────────────────────────────

#[cfg(feature = "rustls-tls")]
pub(crate) mod native;

#[cfg(feature = "rustls-tls")]
pub use native::{NativeConnection as PgReplicationConnection, NativePgResult as PgResult};
