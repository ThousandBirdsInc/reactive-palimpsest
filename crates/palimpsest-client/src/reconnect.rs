// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Exponential backoff helper for reconnect (§18.10).
//!
//! No external `rand`/`backoff` dep — the deterministic doubling
//! schedule with a clamp is sufficient for the policy described in
//! §10/§14, and the test seam below allows callers to drive the
//! scheduler directly.

use std::time::Duration;

/// Backoff policy for the connection manager.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// First sleep on the first failure.
    pub initial: Duration,
    /// Maximum sleep.
    pub max: Duration,
    /// Multiplier applied per attempt.
    pub factor: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(50),
            max: Duration::from_secs(5),
            factor: 2,
        }
    }
}

impl BackoffConfig {
    /// Build a backoff schedule from this policy.
    ///
    /// The returned [`Backoff`] is not an [`Iterator`] — its
    /// [`next_delay`](Backoff::next_delay) method returns indefinitely
    /// (clamped at `max`), so callers control how many delays they
    /// pull from it.
    #[must_use]
    pub const fn schedule(&self) -> Backoff {
        Backoff {
            current: self.initial,
            max: self.max,
            factor: self.factor,
        }
    }
}

/// Mutable counter that doubles on each call until clamped.
#[derive(Debug, Clone)]
pub struct Backoff {
    current: Duration,
    max: Duration,
    factor: u32,
}

impl Backoff {
    /// The next sleep duration. Always returns *at least* `initial`
    /// for the first call.
    pub fn next_delay(&mut self) -> Duration {
        let next = self.current;
        self.current = (self.current * self.factor).min(self.max);
        next
    }

    /// Reset back to the initial value (called after a successful
    /// reconnect).
    pub fn reset(&mut self, initial: Duration) {
        self.current = initial;
    }
}

#[cfg(test)]
mod tests {
    use super::BackoffConfig;
    use std::time::Duration;

    #[test]
    fn doubles_until_clamped() {
        let mut backoff = BackoffConfig {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(80),
            factor: 2,
        }
        .schedule();
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
        assert_eq!(backoff.next_delay(), Duration::from_millis(40));
        assert_eq!(backoff.next_delay(), Duration::from_millis(80));
        assert_eq!(backoff.next_delay(), Duration::from_millis(80));
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut backoff = BackoffConfig::default().schedule();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        backoff.reset(Duration::from_millis(50));
        assert_eq!(backoff.next_delay(), Duration::from_millis(50));
    }
}
