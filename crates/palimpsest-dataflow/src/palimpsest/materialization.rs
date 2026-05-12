//! Partial materialization key-set tracking.
//!
//! When an arrangement is materialized partially — only for the subset of
//! keys that have been requested — consumers must know whether a key is
//! already covered. If not, an *upquery* is issued to fetch missing rows
//! from the upstream system before the consumer can serve a result.
//!
//! This module owns the key-set bookkeeping. The actual upquery dispatch
//! lives in `crate::palimpsest::upquery`.

use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};

/// Status of a key against a materialized arrangement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStatus {
    /// The key is fully materialized and the consumer can serve immediately.
    Materialized,
    /// The key is in flight: an upquery has been issued but is not yet complete.
    Pending,
    /// The key is not in the arrangement and no upquery has been issued.
    Missing,
}

/// Outcome of probing a key against the tracker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupOutcome<K>
where
    K: Ord + Clone,
{
    /// The key is materialized; the consumer can read it now.
    Hit(K),
    /// An upquery was just enqueued; the caller must wait for it to land.
    Upquery(K),
    /// The key was already pending — wait, do not re-issue.
    AlreadyPending(K),
}

impl<K> LookupOutcome<K>
where
    K: Ord + Clone,
{
    /// Returns the key, regardless of outcome variant.
    #[must_use]
    pub const fn key(&self) -> &K {
        match self {
            Self::Hit(key) | Self::Upquery(key) | Self::AlreadyPending(key) => key,
        }
    }
}

/// Tracks which keys are materialized (and which are pending an upquery).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedKeys<K>
where
    K: Ord + Clone,
{
    materialized: BTreeSet<K>,
    pending: BTreeSet<K>,
    pending_waiters: BTreeMap<K, usize>,
}

impl<K> Default for MaterializedKeys<K>
where
    K: Ord + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K> MaterializedKeys<K>
where
    K: Ord + Clone,
{
    /// Creates an empty tracker.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            materialized: BTreeSet::new(),
            pending: BTreeSet::new(),
            pending_waiters: BTreeMap::new(),
        }
    }

    /// Reports a key's status without mutating the tracker.
    #[must_use]
    pub fn status(&self, key: &K) -> KeyStatus {
        if self.materialized.contains(key) {
            KeyStatus::Materialized
        } else if self.pending.contains(key) {
            KeyStatus::Pending
        } else {
            KeyStatus::Missing
        }
    }

    /// Probes the tracker for `key`. Missing keys are transitioned to pending and
    /// an `Upquery` outcome is returned so the caller can dispatch the upquery.
    pub fn lookup(&mut self, key: K) -> LookupOutcome<K> {
        if self.materialized.contains(&key) {
            return LookupOutcome::Hit(key);
        }
        if self.pending.contains(&key) {
            *self.pending_waiters.entry(key.clone()).or_insert(0) += 1;
            return LookupOutcome::AlreadyPending(key);
        }
        self.pending.insert(key.clone());
        self.pending_waiters.insert(key.clone(), 1);
        LookupOutcome::Upquery(key)
    }

    /// Marks `key` as fully materialized. Returns the number of waiters that
    /// had been blocked on the upquery (so callers can wake them).
    pub fn mark_materialized(&mut self, key: K) -> usize {
        let waiters = self.pending_waiters.remove(&key).unwrap_or(0);
        self.pending.remove(&key);
        self.materialized.insert(key);
        waiters
    }

    /// Bulk-marks several keys as materialized; returns total waiter count.
    pub fn mark_many_materialized<I>(&mut self, keys: I) -> usize
    where
        I: IntoIterator<Item = K>,
    {
        keys.into_iter()
            .map(|key| self.mark_materialized(key))
            .sum()
    }

    /// Drops `key` from the materialized set (for example, after eviction).
    pub fn forget(&mut self, key: &K) -> bool {
        self.materialized.remove(key)
    }

    /// Aborts a pending upquery for `key`, transitioning it back to `Missing`.
    pub fn fail_pending(&mut self, key: &K) -> usize {
        let waiters = self.pending_waiters.remove(key).unwrap_or(0);
        self.pending.remove(key);
        waiters
    }

    /// Returns the materialized key set.
    #[must_use]
    pub const fn materialized(&self) -> &BTreeSet<K> {
        &self.materialized
    }

    /// Returns the pending key set.
    #[must_use]
    pub const fn pending(&self) -> &BTreeSet<K> {
        &self.pending
    }

    /// Returns the number of materialized keys.
    #[must_use]
    pub fn materialized_len(&self) -> usize {
        self.materialized.len()
    }

    /// Returns the number of pending keys.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Returns true when nothing is materialized or pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.materialized.is_empty() && self.pending.is_empty()
    }
}

/// Result of probing a batch of keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchLookup<K>
where
    K: Ord + Clone,
{
    /// Keys that are already materialized.
    pub hits: Vec<K>,
    /// Keys that need an upquery (caller should issue these).
    pub upqueries: Vec<K>,
    /// Keys that already have an upquery in flight.
    pub already_pending: Vec<K>,
}

impl<K> MaterializedKeys<K>
where
    K: Ord + Clone,
{
    /// Probes a batch of keys, splitting them into hit / upquery / pending sets.
    pub fn lookup_batch<I>(&mut self, keys: I) -> BatchLookup<K>
    where
        I: IntoIterator<Item = K>,
    {
        let mut batch = BatchLookup {
            hits: Vec::new(),
            upqueries: Vec::new(),
            already_pending: Vec::new(),
        };
        for key in keys {
            match self.lookup(key) {
                LookupOutcome::Hit(key) => batch.hits.push(key),
                LookupOutcome::Upquery(key) => batch.upqueries.push(key),
                LookupOutcome::AlreadyPending(key) => batch.already_pending.push(key),
            }
        }
        batch
    }
}

/// Multi-arrangement tracker keyed by arrangement name.
#[derive(Debug, Clone, Default)]
pub struct MaterializationTracker<K>
where
    K: Ord + Clone,
{
    arrangements: BTreeMap<String, MaterializedKeys<K>>,
}

impl<K> MaterializationTracker<K>
where
    K: Ord + Clone,
{
    /// Creates an empty multi-arrangement tracker.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            arrangements: BTreeMap::new(),
        }
    }

    /// Registers a fresh arrangement, returning an error if `name` is taken.
    pub fn register(&mut self, name: impl Into<String>) -> Result<(), ArrangementAlreadyTracked> {
        let name = name.into();
        match self.arrangements.entry(name) {
            Entry::Vacant(slot) => {
                slot.insert(MaterializedKeys::new());
                Ok(())
            }
            Entry::Occupied(slot) => Err(ArrangementAlreadyTracked {
                name: slot.key().clone(),
            }),
        }
    }

    /// Returns a mutable view of an arrangement's tracker.
    #[must_use]
    pub fn arrangement_mut(&mut self, name: &str) -> Option<&mut MaterializedKeys<K>> {
        self.arrangements.get_mut(name)
    }

    /// Returns an immutable view of an arrangement's tracker.
    #[must_use]
    pub fn arrangement(&self, name: &str) -> Option<&MaterializedKeys<K>> {
        self.arrangements.get(name)
    }

    /// Returns the number of arrangements being tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.arrangements.len()
    }

    /// Returns true when no arrangements are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.arrangements.is_empty()
    }

    /// Drops `name` from the tracker.
    pub fn forget_arrangement(&mut self, name: &str) -> Option<MaterializedKeys<K>> {
        self.arrangements.remove(name)
    }
}

/// Error returned when registering two arrangements under the same name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrangementAlreadyTracked {
    /// Name that was already registered.
    pub name: String,
}

impl std::fmt::Display for ArrangementAlreadyTracked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arrangement '{}' is already tracked", self.name)
    }
}

impl std::error::Error for ArrangementAlreadyTracked {}

#[cfg(test)]
mod tests {
    use super::{
        ArrangementAlreadyTracked, BatchLookup, KeyStatus, LookupOutcome, MaterializationTracker,
        MaterializedKeys,
    };

    #[test]
    fn lookup_promotes_missing_keys_to_pending_and_returns_upquery() {
        let mut keys = MaterializedKeys::<u64>::new();

        assert_eq!(keys.lookup(7), LookupOutcome::Upquery(7));
        assert_eq!(keys.status(&7), KeyStatus::Pending);
        assert_eq!(keys.lookup(7), LookupOutcome::AlreadyPending(7));
        assert_eq!(keys.pending_len(), 1);
    }

    #[test]
    fn mark_materialized_returns_waiter_count_and_clears_pending() {
        let mut keys = MaterializedKeys::<u64>::new();

        let _ = keys.lookup(7);
        let _ = keys.lookup(7);
        assert_eq!(keys.mark_materialized(7), 2);
        assert_eq!(keys.status(&7), KeyStatus::Materialized);
        assert_eq!(keys.pending_len(), 0);
    }

    #[test]
    fn fail_pending_resets_state_and_reports_waiters() {
        let mut keys = MaterializedKeys::<u64>::new();

        let _ = keys.lookup(7);
        let _ = keys.lookup(7);
        assert_eq!(keys.fail_pending(&7), 2);
        assert_eq!(keys.status(&7), KeyStatus::Missing);
    }

    #[test]
    fn forget_drops_materialized_keys() {
        let mut keys = MaterializedKeys::<u64>::new();
        let _ = keys.lookup(7);
        keys.mark_materialized(7);
        assert!(keys.forget(&7));
        assert_eq!(keys.status(&7), KeyStatus::Missing);
    }

    #[test]
    fn lookup_batch_splits_by_outcome() {
        let mut keys = MaterializedKeys::<u64>::new();
        let _ = keys.lookup(1);
        keys.mark_materialized(1);
        let _ = keys.lookup(2);

        let BatchLookup {
            hits,
            upqueries,
            already_pending,
        } = keys.lookup_batch([1_u64, 2, 3]);
        assert_eq!(hits, [1]);
        assert_eq!(already_pending, [2]);
        assert_eq!(upqueries, [3]);
    }

    #[test]
    fn bulk_mark_returns_aggregate_waiter_count() {
        let mut keys = MaterializedKeys::<u64>::new();
        let _ = keys.lookup(1);
        let _ = keys.lookup(1);
        let _ = keys.lookup(2);
        assert_eq!(keys.mark_many_materialized([1, 2, 3]), 3);
    }

    #[test]
    fn tracker_routes_lookups_per_arrangement() {
        let mut tracker = MaterializationTracker::<u64>::new();
        tracker.register("posts-by-author").unwrap();
        let posts = tracker.arrangement_mut("posts-by-author").unwrap();
        assert_eq!(posts.lookup(7), LookupOutcome::Upquery(7));
        assert_eq!(
            tracker
                .arrangement("posts-by-author")
                .map(MaterializedKeys::pending_len),
            Some(1)
        );
    }

    #[test]
    fn duplicate_registration_errors() {
        let mut tracker = MaterializationTracker::<u64>::new();
        tracker.register("a").unwrap();
        let err = tracker.register("a").unwrap_err();
        assert_eq!(err, ArrangementAlreadyTracked { name: "a".into() });
    }
}
