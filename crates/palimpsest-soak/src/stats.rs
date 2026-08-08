// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Latency recording and process-memory sampling.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

/// Bounded-memory latency recorder.
///
/// Records every sample until `cap` is reached, then decimates by
/// doubling the sampling stride — percentile estimates stay stable
/// while memory stays bounded regardless of run length.
#[derive(Debug, Clone)]
pub struct LatencyRecorder {
    samples: Vec<u64>,
    cap: usize,
    stride: u64,
    seen: u64,
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::with_capacity(262_144)
    }
}

impl LatencyRecorder {
    /// Creates a recorder that stores at most `cap` samples.
    #[must_use]
    pub const fn with_capacity(cap: usize) -> Self {
        Self {
            samples: Vec::new(),
            cap,
            stride: 1,
            seen: 0,
        }
    }

    /// Records one latency observation in microseconds.
    pub fn record_us(&mut self, us: u64) {
        self.seen += 1;
        if self.seen % self.stride != 0 {
            return;
        }
        if self.samples.len() >= self.cap {
            let mut keep = Vec::with_capacity(self.cap / 2 + 1);
            for (i, v) in self.samples.iter().enumerate() {
                if i % 2 == 0 {
                    keep.push(*v);
                }
            }
            self.samples = keep;
            self.stride *= 2;
        }
        self.samples.push(us);
    }

    /// Folds another recorder's samples into this one.
    pub fn merge(&mut self, other: &Self) {
        self.seen += other.seen;
        for v in &other.samples {
            if self.samples.len() >= self.cap {
                // Cheap merge decimation: skip alternate donor samples.
                if self.samples.len() % 2 == 0 {
                    continue;
                }
            }
            self.samples.push(*v);
        }
        if self.samples.len() > self.cap * 2 {
            self.samples.truncate(self.cap * 2);
        }
    }

    /// Total observations recorded (before decimation).
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.seen
    }

    /// Summarizes the recorded distribution.
    #[must_use]
    pub fn summary(&self) -> LatencySummary {
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let pick = |p: f64| -> u64 {
            if sorted.is_empty() {
                return 0;
            }
            let idx = ((sorted.len() as f64) * p) as usize;
            sorted[idx.min(sorted.len() - 1)]
        };
        LatencySummary {
            count: self.seen,
            p50_us: pick(0.50),
            p90_us: pick(0.90),
            p99_us: pick(0.99),
            p999_us: pick(0.999),
            max_us: sorted.last().copied().unwrap_or(0),
        }
    }
}

/// Percentile summary of a latency distribution, in microseconds.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct LatencySummary {
    /// Total observations.
    pub count: u64,
    /// Median.
    pub p50_us: u64,
    /// 90th percentile.
    pub p90_us: u64,
    /// 99th percentile.
    pub p99_us: u64,
    /// 99.9th percentile.
    pub p999_us: u64,
    /// Worst observation retained.
    pub max_us: u64,
}

impl fmt::Display for LatencySummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "n={} p50={}us p90={}us p99={}us p99.9={}us max={}us",
            self.count, self.p50_us, self.p90_us, self.p99_us, self.p999_us, self.max_us
        )
    }
}

/// Resident-set size of this process in bytes (Linux; `None`
/// elsewhere or when `/proc` is unavailable).
#[must_use]
pub fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Background peak-RSS tracker.
///
/// Samples `VmRSS` on an interval and retains the high-water mark;
/// dropping the handle stops the sampler.
#[derive(Debug)]
pub struct RssWatcher {
    peak: Arc<AtomicU64>,
    stop: Arc<AtomicU64>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl RssWatcher {
    /// Spawns the sampler on the current runtime.
    #[must_use]
    pub fn spawn(interval: Duration) -> Self {
        let peak = Arc::new(AtomicU64::new(rss_bytes().unwrap_or(0)));
        let stop = Arc::new(AtomicU64::new(0));
        let peak_task = Arc::clone(&peak);
        let stop_task = Arc::clone(&stop);
        let handle = tokio::spawn(async move {
            while stop_task.load(Ordering::Relaxed) == 0 {
                if let Some(rss) = rss_bytes() {
                    peak_task.fetch_max(rss, Ordering::Relaxed);
                }
                tokio::time::sleep(interval).await;
            }
        });
        Self {
            peak,
            stop,
            handle: Some(handle),
        }
    }

    /// Stops sampling and returns the observed peak in bytes.
    pub async fn finish(mut self) -> u64 {
        self.stop.store(1, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        self.peak.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::LatencyRecorder;

    #[test]
    fn percentiles_are_ordered_after_decimation() {
        let mut rec = LatencyRecorder::with_capacity(128);
        for i in 0..10_000_u64 {
            rec.record_us(i);
        }
        let s = rec.summary();
        assert_eq!(s.count, 10_000);
        assert!(s.p50_us <= s.p90_us && s.p90_us <= s.p99_us && s.p99_us <= s.max_us);
        assert!(s.p50_us > 3_000 && s.p50_us < 7_000);
    }
}
