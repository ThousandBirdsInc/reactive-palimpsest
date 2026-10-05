// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Crate-internal logging macros.
//!
//! With the `tracing` feature (on by default, and what every native
//! build uses) these forward verbatim to the `tracing` macros of the
//! same name. Without it they expand to nothing — the field
//! expressions are still type-checked inside an `if false` block so
//! that call sites compile identically, but no `tracing-core`
//! callsite registry, dispatcher, or `Debug`/`Display` formatting
//! code is linked. The browser bundle (`palimpsest-client-js`) builds
//! this way: nothing in a browser installs a subscriber, so the
//! callsites were pure dead weight there.
//!
//! The macros are textually scoped (`#[macro_use] mod log;` is the
//! first module in `lib.rs`) rather than `pub(crate) use`d: a
//! path-based `warn` collides with the built-in `#[warn]` attribute.

#![allow(unused_macros)]

#[cfg(feature = "tracing")]
macro_rules! debug {
    ($($arg:tt)*) => { ::tracing::debug!($($arg)*) };
}

#[cfg(feature = "tracing")]
macro_rules! info {
    ($($arg:tt)*) => { ::tracing::info!($($arg)*) };
}

#[cfg(feature = "tracing")]
macro_rules! warn {
    ($($arg:tt)*) => { ::tracing::warn!($($arg)*) };
}

#[cfg(not(feature = "tracing"))]
macro_rules! debug {
    ($($arg:tt)*) => { $crate::log::noop_event!($($arg)*) };
}

#[cfg(not(feature = "tracing"))]
macro_rules! info {
    ($($arg:tt)*) => { $crate::log::noop_event!($($arg)*) };
}

#[cfg(not(feature = "tracing"))]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::log::noop_event!($($arg)*) };
}

/// Walks the subset of `tracing`'s field syntax this crate uses
/// (`?expr`, `%expr`, `name = expr`, `name = ?expr`, `name = %expr`,
/// bare `expr`, and a trailing format string with arguments) and
/// "uses" every expression so the no-op build raises no
/// unused-variable warnings. Nothing is evaluated at runtime.
#[cfg(not(feature = "tracing"))]
macro_rules! noop_event {
    () => {};
    ($lit:literal $(, $($args:tt)*)?) => {
        if false {
            let _ = ::core::format_args!($lit $(, $($args)*)?);
        }
    };
    (? $e:expr $(, $($rest:tt)*)?) => {
        if false { let _ = &$e; }
        $crate::log::noop_event!($($($rest)*)?)
    };
    (% $e:expr $(, $($rest:tt)*)?) => {
        if false { let _ = &$e; }
        $crate::log::noop_event!($($($rest)*)?)
    };
    ($name:ident = ? $e:expr $(, $($rest:tt)*)?) => {
        if false { let _ = &$e; }
        $crate::log::noop_event!($($($rest)*)?)
    };
    ($name:ident = % $e:expr $(, $($rest:tt)*)?) => {
        if false { let _ = &$e; }
        $crate::log::noop_event!($($($rest)*)?)
    };
    ($name:ident = $e:expr $(, $($rest:tt)*)?) => {
        if false { let _ = &$e; }
        $crate::log::noop_event!($($($rest)*)?)
    };
    ($e:expr $(, $($rest:tt)*)?) => {
        if false { let _ = &$e; }
        $crate::log::noop_event!($($($rest)*)?)
    };
}

#[cfg(not(feature = "tracing"))]
pub(crate) use noop_event;
