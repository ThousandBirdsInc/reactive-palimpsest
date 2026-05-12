//! Per-operator memory and lookup instrumentation.
//!
//! Operators report their arrangement size (in rows and bytes) and the
//! upquery / TOAST counters surfaced by sibling modules. The collected
//! state is exposed as a snapshot so tests, logging, and future Prometheus
//! exporters share one shape.

use std::collections::BTreeMap;

use crate::palimpsest::toast::ToastStats;

/// One operator's resource footprint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperatorMemory {
    /// Number of records currently materialized in the arrangement.
    pub rows: usize,
    /// Approximate memory footprint of the arrangement in bytes.
    pub bytes: usize,
}

impl OperatorMemory {
    /// Builds a record from `(rows, bytes)` measurements.
    #[must_use]
    pub const fn new(rows: usize, bytes: usize) -> Self {
        Self { rows, bytes }
    }
}

/// Upquery counters tracked per operator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpqueryCounters {
    /// Upqueries that hit the materialized arrangement.
    pub hits: usize,
    /// Upqueries that had to fetch from upstream.
    pub fetches: usize,
    /// Upqueries that found nothing.
    pub misses: usize,
}

/// Snapshot of every counter tracked for a single operator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperatorSnapshot {
    /// Memory footprint.
    pub memory: OperatorMemory,
    /// Upquery counters.
    pub upqueries: UpqueryCounters,
    /// TOAST resolver counters.
    pub toast: ToastStats,
}

/// Registry that aggregates per-operator metrics by canonical operator name.
#[derive(Debug, Clone, Default)]
pub struct Metrics {
    operators: BTreeMap<String, OperatorSnapshot>,
}

impl Metrics {
    /// Creates an empty metrics registry.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operators: BTreeMap::new(),
        }
    }

    /// Reports a fresh memory measurement for `operator`.
    pub fn record_memory(&mut self, operator: impl Into<String>, memory: OperatorMemory) {
        self.operators.entry(operator.into()).or_default().memory = memory;
    }

    /// Reports an arrangement-hit upquery.
    pub fn record_upquery_hit(&mut self, operator: impl Into<String>) {
        let entry = self.operators.entry(operator.into()).or_default();
        entry.upqueries.hits = entry.upqueries.hits.saturating_add(1);
    }

    /// Reports an upquery that had to fetch from upstream.
    pub fn record_upquery_fetch(&mut self, operator: impl Into<String>) {
        let entry = self.operators.entry(operator.into()).or_default();
        entry.upqueries.fetches = entry.upqueries.fetches.saturating_add(1);
    }

    /// Reports an upquery that found nothing.
    pub fn record_upquery_miss(&mut self, operator: impl Into<String>) {
        let entry = self.operators.entry(operator.into()).or_default();
        entry.upqueries.misses = entry.upqueries.misses.saturating_add(1);
    }

    /// Stores the latest `ToastStats` for `operator`.
    pub fn record_toast(&mut self, operator: impl Into<String>, toast: ToastStats) {
        self.operators.entry(operator.into()).or_default().toast = toast;
    }

    /// Returns the snapshot for `operator`, if recorded.
    #[must_use]
    pub fn snapshot(&self, operator: &str) -> Option<OperatorSnapshot> {
        self.operators.get(operator).copied()
    }

    /// Iterates over `(operator, snapshot)` pairs in canonical order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &OperatorSnapshot)> {
        self.operators
            .iter()
            .map(|(name, snapshot)| (name.as_str(), snapshot))
    }

    /// Returns the number of operators tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    /// Returns true when no operators have reported metrics.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    /// Aggregates memory totals across every operator.
    #[must_use]
    pub fn total_memory(&self) -> OperatorMemory {
        self.operators
            .values()
            .map(|snapshot| snapshot.memory)
            .fold(OperatorMemory::default(), |acc, memory| OperatorMemory {
                rows: acc.rows.saturating_add(memory.rows),
                bytes: acc.bytes.saturating_add(memory.bytes),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{Metrics, OperatorMemory};
    use crate::palimpsest::toast::ToastStats;

    #[test]
    fn record_memory_overwrites_previous_measurement() {
        let mut metrics = Metrics::new();
        metrics.record_memory("posts/filter", OperatorMemory::new(10, 256));
        metrics.record_memory("posts/filter", OperatorMemory::new(15, 320));

        assert_eq!(
            metrics.snapshot("posts/filter").unwrap().memory,
            OperatorMemory::new(15, 320)
        );
    }

    #[test]
    fn upquery_counters_are_per_operator_and_increment() {
        let mut metrics = Metrics::new();
        metrics.record_upquery_hit("posts/filter");
        metrics.record_upquery_fetch("posts/filter");
        metrics.record_upquery_miss("posts/filter");
        metrics.record_upquery_hit("authors/filter");

        let posts = metrics.snapshot("posts/filter").unwrap();
        assert_eq!(posts.upqueries.hits, 1);
        assert_eq!(posts.upqueries.fetches, 1);
        assert_eq!(posts.upqueries.misses, 1);

        let authors = metrics.snapshot("authors/filter").unwrap();
        assert_eq!(authors.upqueries.hits, 1);
    }

    #[test]
    fn record_toast_stores_last_snapshot() {
        let mut metrics = Metrics::new();
        metrics.record_toast(
            "posts/filter",
            ToastStats {
                cached_hits: 4,
                point_select_hits: 1,
                misses: 0,
            },
        );

        let stats = metrics.snapshot("posts/filter").unwrap().toast;
        assert_eq!(stats.cached_hits, 4);
        assert_eq!(stats.point_select_hits, 1);
    }

    #[test]
    fn total_memory_sums_across_operators() {
        let mut metrics = Metrics::new();
        metrics.record_memory("a", OperatorMemory::new(1, 10));
        metrics.record_memory("b", OperatorMemory::new(2, 20));
        assert_eq!(metrics.total_memory(), OperatorMemory::new(3, 30));
        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn iter_yields_canonical_order() {
        let mut metrics = Metrics::new();
        metrics.record_memory("b", OperatorMemory::new(1, 10));
        metrics.record_memory("a", OperatorMemory::new(2, 20));

        let names: Vec<_> = metrics.iter().map(|(name, _)| name).collect();
        assert_eq!(names, ["a", "b"]);
    }
}
