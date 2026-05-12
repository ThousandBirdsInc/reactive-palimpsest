// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-connection and per-IP resource bounds (§18.15.2, §18.15.3, §18.15.5).
//!
//! Three independent limiters:
//!
//! * [`SecurityLimits::max_subscriptions_per_connection`] — caps the
//!   total live subscriptions any one gRPC stream can hold open.
//! * [`SecurityLimits::subscribe_rate`] — token-bucket on
//!   `Subscribe` `ClientMessage`s, applied per-connection. Smooths
//!   over bursty clients without rejecting brief spikes.
//! * [`SecurityLimits::reconnect_rate`] — sliding-window on inbound
//!   `Subscribe` RPCs per remote IP. Trips at the gRPC layer before
//!   the per-connection limiter, so a noisy IP cannot `DoS` the
//!   service simply by hammering reconnects.
//!
//! The limiters are intentionally simple:
//!
//! * Token-bucket arithmetic is monotonic-clock (`Instant`) to dodge
//!   wall-clock drift.
//! * The reconnect tracker is sharded by IP into a `BTreeMap`. v1 keeps
//!   it process-local; if/when we scale out, the same shape lifts onto
//!   a Redis-backed counter without API changes.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Tunables surfaced through [`crate::ServerConfig`]. Defaults are sane
/// for a single-tenant production deployment; tighten them for shared
/// hosting or untrusted clients.
#[derive(Debug, Clone, Copy)]
pub struct SecurityLimits {
    /// Hard cap on subscriptions held by a single gRPC connection.
    /// Subscribes beyond this respond with an `Error` `ServerMessage`.
    pub max_subscriptions_per_connection: usize,
    /// Token-bucket spec for `Subscribe` `ClientMessage`s within a
    /// single gRPC connection. `burst` tokens accumulate at
    /// `refill_per_sec` per second.
    pub subscribe_rate: TokenBucketSpec,
    /// Sliding-window spec for inbound gRPC connection attempts per
    /// remote IP. Tripping closes the inbound stream with
    /// `Status::resource_exhausted`.
    pub reconnect_rate: SlidingWindowSpec,
}

impl SecurityLimits {
    /// Default for tests / dev: large enough that conformance suites
    /// never trip but small enough to demonstrate the limiter is wired
    /// up.
    pub const DEFAULT: Self = Self {
        max_subscriptions_per_connection: 256,
        subscribe_rate: TokenBucketSpec {
            burst: 32,
            refill_per_sec: 8.0,
        },
        reconnect_rate: SlidingWindowSpec {
            max_attempts: 60,
            window: Duration::from_secs(60),
        },
    };
}

impl Default for SecurityLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Token-bucket parameters: `burst` tokens, refilling at
/// `refill_per_sec`. A request consumes one token; if none are
/// available the limiter rejects.
#[derive(Debug, Clone, Copy)]
pub struct TokenBucketSpec {
    /// Bucket capacity (max burst).
    pub burst: u32,
    /// Tokens added per second (in steady state).
    pub refill_per_sec: f64,
}

/// Sliding-window parameters: at most `max_attempts` events in any
/// rolling `window`.
#[derive(Debug, Clone, Copy)]
pub struct SlidingWindowSpec {
    /// Maximum events permitted within `window`.
    pub max_attempts: u32,
    /// Window length over which `max_attempts` is enforced.
    pub window: Duration,
}

/// Reason a limiter rejected a request — used to label `Resync`/error
/// responses without leaking implementation details to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitDecision {
    /// Within bounds — proceed.
    Allowed,
    /// Connection already holds the maximum permitted subscriptions.
    ConnectionSaturated,
    /// Per-connection rate limiter is empty.
    SubscribeRateExceeded,
    /// Per-IP reconnect window is full.
    ReconnectRateExceeded,
}

/// Per-connection bookkeeping. Holds the live subscription count and
/// the token bucket. Cheap to construct; one per gRPC stream.
pub struct ConnectionLimiter {
    limits: SecurityLimits,
    inner: Mutex<ConnectionLimiterInner>,
}

struct ConnectionLimiterInner {
    active: usize,
    bucket: TokenBucketState,
}

impl ConnectionLimiter {
    /// Builds a limiter primed with a full bucket.
    #[must_use]
    pub fn new(limits: SecurityLimits) -> Self {
        Self::new_at(limits, Instant::now())
    }

    /// Test-friendly constructor: pin the starting timestamp.
    #[must_use]
    pub fn new_at(limits: SecurityLimits, now: Instant) -> Self {
        Self {
            limits,
            inner: Mutex::new(ConnectionLimiterInner {
                active: 0,
                bucket: TokenBucketState::full(limits.subscribe_rate, now),
            }),
        }
    }

    /// Decides whether a fresh `Subscribe` should be admitted, and if
    /// so reserves capacity (bumps the active count, consumes a token).
    /// On rejection, the limiter state is unchanged.
    pub fn admit_subscribe(&self) -> LimitDecision {
        self.admit_subscribe_at(Instant::now())
    }

    /// Test-friendly variant of [`Self::admit_subscribe`] that takes
    /// the current instant explicitly.
    pub fn admit_subscribe_at(&self, now: Instant) -> LimitDecision {
        let mut inner = self.inner.lock().expect("connection limiter lock");
        if inner.active >= self.limits.max_subscriptions_per_connection {
            return LimitDecision::ConnectionSaturated;
        }
        if !inner.bucket.try_consume(self.limits.subscribe_rate, now) {
            return LimitDecision::SubscribeRateExceeded;
        }
        inner.active += 1;
        LimitDecision::Allowed
    }

    /// Releases capacity reserved by [`Self::admit_subscribe`]. Calls
    /// in excess of the matching admit are clamped to zero — the
    /// limiter is permissive on `unsubscribe` because a double-release
    /// is less dangerous than a leaked slot.
    pub fn release(&self) {
        let mut inner = self.inner.lock().expect("connection limiter lock");
        inner.active = inner.active.saturating_sub(1);
    }

    /// Current live subscription count (for tests and metrics).
    #[must_use]
    pub fn active(&self) -> usize {
        self.inner.lock().expect("connection limiter lock").active
    }
}

#[derive(Debug, Clone, Copy)]
struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucketState {
    fn full(spec: TokenBucketSpec, now: Instant) -> Self {
        Self {
            tokens: f64::from(spec.burst),
            last_refill: now,
        }
    }

    fn try_consume(&mut self, spec: TokenBucketSpec, now: Instant) -> bool {
        // Refill: elapsed seconds × refill rate, clamped to burst.
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = elapsed
                .mul_add(spec.refill_per_sec, self.tokens)
                .min(f64::from(spec.burst));
            self.last_refill = now;
        }
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-IP sliding-window tracker. One process-wide instance lives in
/// [`crate::PalimpsestBuilder`] and is consulted at the start of every
/// inbound gRPC `Subscribe`.
pub struct ReconnectTracker {
    spec: SlidingWindowSpec,
    inner: Mutex<BTreeMap<IpAddr, Vec<Instant>>>,
}

impl ReconnectTracker {
    /// Builds an empty tracker.
    #[must_use]
    pub const fn new(spec: SlidingWindowSpec) -> Self {
        Self {
            spec,
            inner: Mutex::new(BTreeMap::new()),
        }
    }

    /// Records an attempt and returns whether it is admissible.
    pub fn admit(&self, ip: IpAddr) -> LimitDecision {
        self.admit_at(ip, Instant::now())
    }

    /// Test-friendly variant accepting an explicit `now`.
    #[allow(clippy::significant_drop_tightening)]
    pub fn admit_at(&self, ip: IpAddr, now: Instant) -> LimitDecision {
        let cutoff = now.checked_sub(self.spec.window).unwrap_or(now);
        let max = self.spec.max_attempts as usize;
        let mut inner = self.inner.lock().expect("reconnect tracker lock");
        let attempts = inner.entry(ip).or_default();
        attempts.retain(|stamp| *stamp >= cutoff);
        if attempts.len() >= max {
            LimitDecision::ReconnectRateExceeded
        } else {
            attempts.push(now);
            LimitDecision::Allowed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionLimiter, LimitDecision, ReconnectTracker, SecurityLimits, SlidingWindowSpec,
        TokenBucketSpec,
    };
    use std::net::IpAddr;
    use std::time::{Duration, Instant};

    fn limits_with_burst(burst: u32, refill: f64) -> SecurityLimits {
        SecurityLimits {
            max_subscriptions_per_connection: 4,
            subscribe_rate: TokenBucketSpec {
                burst,
                refill_per_sec: refill,
            },
            reconnect_rate: SlidingWindowSpec {
                max_attempts: 100,
                window: Duration::from_secs(60),
            },
        }
    }

    #[test]
    fn connection_limiter_caps_active_subscriptions() {
        let limiter = ConnectionLimiter::new(limits_with_burst(100, 1000.0));
        for _ in 0..4 {
            assert_eq!(limiter.admit_subscribe(), LimitDecision::Allowed);
        }
        assert_eq!(
            limiter.admit_subscribe(),
            LimitDecision::ConnectionSaturated
        );
        limiter.release();
        assert_eq!(limiter.admit_subscribe(), LimitDecision::Allowed);
    }

    #[test]
    fn connection_limiter_enforces_subscribe_rate() {
        let now = Instant::now();
        let limiter = ConnectionLimiter::new_at(limits_with_burst(2, 1.0), now);
        assert_eq!(limiter.admit_subscribe_at(now), LimitDecision::Allowed);
        assert_eq!(limiter.admit_subscribe_at(now), LimitDecision::Allowed);
        // Bucket empty after burst; still under cap so the rate trips.
        assert_eq!(
            limiter.admit_subscribe_at(now),
            LimitDecision::SubscribeRateExceeded
        );
        // Two seconds later, two tokens have refilled.
        let later = now + Duration::from_secs(2);
        assert_eq!(limiter.admit_subscribe_at(later), LimitDecision::Allowed);
    }

    #[test]
    fn reconnect_tracker_trips_on_burst_per_ip() {
        let tracker = ReconnectTracker::new(SlidingWindowSpec {
            max_attempts: 3,
            window: Duration::from_secs(60),
        });
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let now = Instant::now();
        for _ in 0..3 {
            assert_eq!(tracker.admit_at(ip, now), LimitDecision::Allowed);
        }
        assert_eq!(
            tracker.admit_at(ip, now),
            LimitDecision::ReconnectRateExceeded
        );
    }

    #[test]
    fn reconnect_tracker_resets_after_window() {
        let tracker = ReconnectTracker::new(SlidingWindowSpec {
            max_attempts: 2,
            window: Duration::from_secs(60),
        });
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let now = Instant::now();
        assert_eq!(tracker.admit_at(ip, now), LimitDecision::Allowed);
        assert_eq!(tracker.admit_at(ip, now), LimitDecision::Allowed);
        assert_eq!(
            tracker.admit_at(ip, now),
            LimitDecision::ReconnectRateExceeded
        );
        let later = now + Duration::from_secs(120);
        assert_eq!(tracker.admit_at(ip, later), LimitDecision::Allowed);
    }
}
