// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Deterministic randomness for reproducible load runs.
//!
//! A tiny `SplitMix64` generator plus a precomputed Zipf sampler.
//! No external RNG crate: load numbers must be comparable across
//! runs, so every scenario derives all randomness from one seed.

/// `SplitMix64` pseudo-random generator.
///
/// Not cryptographic; chosen for speed, tiny state, and excellent
/// statistical behavior for workload synthesis.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Creates a generator from `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// Next raw 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform value in `0..bound` (`0` when `bound == 0`).
    pub fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() % bound as u64) as usize
    }

    /// Uniform value in `low..=high`.
    pub fn between(&mut self, low: usize, high: usize) -> usize {
        low + self.below(high.saturating_sub(low) + 1)
    }

    /// Uniform float in `[0, 1)`.
    pub fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    /// True with probability `pct / 100`.
    pub fn chance_pct(&mut self, pct: u8) -> bool {
        self.below(100) < pct as usize
    }

    /// Derives an independent child generator (for per-task streams).
    pub fn fork(&mut self) -> Self {
        Self::new(self.next_u64())
    }
}

/// Zipf-distributed sampler over `0..n`.
///
/// Real fleets are skewed: a handful of documents/channels absorb
/// most writes and most subscribers. `exponent` around `1.0` gives
/// the classic "hot head, long tail" shape.
#[derive(Debug, Clone)]
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    /// Builds the sampler for `n` ranks with skew `exponent`.
    #[must_use]
    pub fn new(n: usize, exponent: f64) -> Self {
        let mut weights = Vec::with_capacity(n.max(1));
        let mut total = 0.0_f64;
        for rank in 0..n.max(1) {
            let w = 1.0 / ((rank + 1) as f64).powf(exponent);
            total += w;
            weights.push(total);
        }
        for w in &mut weights {
            *w /= total;
        }
        Self { cdf: weights }
    }

    /// Samples one rank in `0..n`.
    pub fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.f64();
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1)
    }

    /// Probability mass of `rank` (used to report expected fan-out).
    #[must_use]
    pub fn mass(&self, rank: usize) -> f64 {
        let hi = self.cdf.get(rank).copied().unwrap_or(1.0);
        let lo = rank
            .checked_sub(1)
            .and_then(|r| self.cdf.get(r).copied())
            .unwrap_or(0.0);
        hi - lo
    }
}

#[cfg(test)]
mod tests {
    use super::{Rng, Zipf};

    #[test]
    fn deterministic_under_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..64 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn zipf_is_skewed_toward_low_ranks() {
        let zipf = Zipf::new(64, 1.1);
        let mut rng = Rng::new(7);
        let mut counts = [0_u32; 64];
        for _ in 0..10_000 {
            counts[zipf.sample(&mut rng)] += 1;
        }
        assert!(counts[0] > counts[32] * 4);
    }
}
