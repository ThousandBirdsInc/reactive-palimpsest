//! TOAST-aware value resolution.
//!
//! Postgres logical replication transmits an `Unchanged` placeholder for
//! TOAST-stored columns whose value did not change. Operators that read
//! such a column must first consult their own arrangement (because they
//! may have observed the value earlier) and only fall back to a point
//! select against Postgres if the value is genuinely unknown.
//!
//! This module provides the small lookup interface and a chained
//! resolver that prefers the arrangement before issuing the upquery.

use std::collections::BTreeMap;

use palimpsest_wal::{Datum, TableId};

use crate::palimpsest::wal::Row;

/// Identifier for a table column referenced in a TOAST upquery.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnLocation {
    /// Source table.
    pub table: TableId,
    /// Column index within the table tuple.
    pub column: usize,
}

impl ColumnLocation {
    /// Creates a column location.
    #[must_use]
    pub const fn new(table: TableId, column: usize) -> Self {
        Self { table, column }
    }
}

/// Stable identifier for a row.
///
/// `Datum` does not implement `Ord`/`Hash`, so the cache uses an opaque byte
/// encoding (typically the WAL-encoded primary key). Callers are responsible
/// for producing a consistent encoding across producers and consumers.
pub type RowKey = Vec<u8>;

/// Outcome of a TOAST lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToastOutcome {
    /// The value was served from the arrangement (no upquery needed).
    Cached(Datum),
    /// The value was fetched from the upstream Postgres source.
    PointSelect(Datum),
    /// The column is unknown — neither cached nor recoverable.
    Missing,
}

impl ToastOutcome {
    /// Returns the cached or upqueried value if the lookup succeeded.
    #[must_use]
    pub fn into_datum(self) -> Option<Datum> {
        match self {
            Self::Cached(value) | Self::PointSelect(value) => Some(value),
            Self::Missing => None,
        }
    }
}

/// Source that can answer a TOAST point-select against the upstream
/// Postgres database.
pub trait PointSelect {
    /// Returns the column value for `(location, key)` if available.
    fn point_select(&self, location: &ColumnLocation, key: &RowKey) -> Option<Datum>;
}

/// In-memory cache that mirrors the operator's own arrangement.
///
/// Operators populate this cache as they observe `(table, key, column,
/// value)` triples; the resolver checks it before issuing a point select.
#[derive(Debug, Clone, Default)]
pub struct ArrangementCache {
    rows: BTreeMap<(TableId, RowKey), Row>,
}

impl ArrangementCache {
    /// Creates an empty cache.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rows: BTreeMap::new(),
        }
    }

    /// Inserts (or replaces) the row for `(table, key)`.
    pub fn insert(&mut self, table: TableId, key: RowKey, row: Row) {
        self.rows.insert((table, key), row);
    }

    /// Removes a row from the cache.
    pub fn remove(&mut self, table: TableId, key: &RowKey) -> Option<Row> {
        self.rows.remove(&(table, key.clone()))
    }

    /// Looks up the cached column value for `(location, key)`.
    #[must_use]
    pub fn get(&self, location: &ColumnLocation, key: &RowKey) -> Option<Datum> {
        let row = self.rows.get(&(location.table, key.clone()))?;
        row.get(location.column).cloned()
    }

    /// Number of cached rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true when no rows are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Two-level resolver: arrangement cache first, point-select fallback.
#[derive(Debug, Clone)]
pub struct ToastResolver<S> {
    cache: ArrangementCache,
    source: S,
    cached_hits: usize,
    point_select_hits: usize,
    misses: usize,
}

impl<S> ToastResolver<S>
where
    S: PointSelect,
{
    /// Builds a resolver from a fresh cache and a point-select source.
    pub fn new(source: S) -> Self {
        Self {
            cache: ArrangementCache::new(),
            source,
            cached_hits: 0,
            point_select_hits: 0,
            misses: 0,
        }
    }

    /// Resolves the requested column, preferring the arrangement cache.
    pub fn resolve(&mut self, location: &ColumnLocation, key: &RowKey) -> ToastOutcome {
        if let Some(value) = self.cache.get(location, key) {
            self.cached_hits = self.cached_hits.saturating_add(1);
            return ToastOutcome::Cached(value);
        }
        match self.source.point_select(location, key) {
            Some(value) => {
                self.point_select_hits = self.point_select_hits.saturating_add(1);
                ToastOutcome::PointSelect(value)
            }
            None => {
                self.misses = self.misses.saturating_add(1);
                ToastOutcome::Missing
            }
        }
    }

    /// Cache mutator: insert a freshly-decoded row from the WAL.
    pub fn record_row(&mut self, table: TableId, key: RowKey, row: Row) {
        self.cache.insert(table, key, row);
    }

    /// Cache mutator: drop a row that has been removed.
    pub fn forget_row(&mut self, table: TableId, key: &RowKey) {
        self.cache.remove(table, key);
    }

    /// Returns lookup statistics suitable for `Metrics` reporting.
    #[must_use]
    pub const fn stats(&self) -> ToastStats {
        ToastStats {
            cached_hits: self.cached_hits,
            point_select_hits: self.point_select_hits,
            misses: self.misses,
        }
    }

    /// Returns a reference to the underlying point-select source.
    #[must_use]
    pub const fn source(&self) -> &S {
        &self.source
    }

    /// Returns the cache currently held by the resolver.
    #[must_use]
    pub const fn cache(&self) -> &ArrangementCache {
        &self.cache
    }
}

/// Per-resolver counters surfaced to the metrics layer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ToastStats {
    /// Number of resolutions served from the arrangement cache.
    pub cached_hits: usize,
    /// Number of resolutions served by issuing a point select.
    pub point_select_hits: usize,
    /// Number of resolutions that found neither a cached nor an upstream value.
    pub misses: usize,
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use palimpsest_wal::{Datum, TableId};
    use smallvec::smallvec;

    use super::{
        ArrangementCache, ColumnLocation, PointSelect, RowKey, ToastOutcome, ToastResolver,
    };

    fn key_bytes(value: i32) -> RowKey {
        value.to_be_bytes().to_vec()
    }

    struct FakeSource {
        rows: BTreeMap<(TableId, RowKey), Vec<Datum>>,
        calls: RefCell<usize>,
    }

    impl FakeSource {
        fn new() -> Self {
            Self {
                rows: BTreeMap::new(),
                calls: RefCell::new(0),
            }
        }

        fn insert(&mut self, table: TableId, key: RowKey, row: Vec<Datum>) {
            self.rows.insert((table, key), row);
        }
    }

    impl PointSelect for FakeSource {
        fn point_select(&self, location: &ColumnLocation, key: &RowKey) -> Option<Datum> {
            *self.calls.borrow_mut() += 1;
            self.rows
                .get(&(location.table, key.clone()))
                .and_then(|row: &Vec<Datum>| row.get(location.column).cloned())
        }
    }

    #[test]
    fn cached_hit_does_not_call_point_select() {
        let table = TableId::new(7);
        let mut resolver = ToastResolver::new(FakeSource::new());
        resolver.record_row(
            table,
            key_bytes(1),
            smallvec![Datum::I32(1), Datum::Text("hello".into())],
        );

        let outcome = resolver.resolve(&ColumnLocation::new(table, 1), &key_bytes(1));
        assert!(matches!(outcome, ToastOutcome::Cached(_)));
        assert_eq!(*resolver.source().calls.borrow(), 0);
        assert_eq!(resolver.stats().cached_hits, 1);
    }

    #[test]
    fn point_select_falls_back_when_cache_misses() {
        let table = TableId::new(7);
        let mut source = FakeSource::new();
        source.insert(
            table,
            key_bytes(1),
            vec![Datum::I32(1), Datum::Text("late".into())],
        );
        let mut resolver = ToastResolver::new(source);

        let outcome = resolver.resolve(&ColumnLocation::new(table, 1), &key_bytes(1));
        assert_eq!(outcome.into_datum(), Some(Datum::Text("late".into())));
        assert_eq!(resolver.stats().point_select_hits, 1);
        assert_eq!(*resolver.source().calls.borrow(), 1);
    }

    #[test]
    fn missing_outcome_increments_miss_counter() {
        let table = TableId::new(7);
        let mut resolver = ToastResolver::new(FakeSource::new());
        let outcome = resolver.resolve(&ColumnLocation::new(table, 1), &key_bytes(99));
        assert_eq!(outcome, ToastOutcome::Missing);
        assert_eq!(resolver.stats().misses, 1);
    }

    #[test]
    fn arrangement_cache_supports_remove_and_len() {
        let mut cache = ArrangementCache::new();
        cache.insert(
            TableId::new(7),
            key_bytes(1),
            smallvec![Datum::I32(1), Datum::Text("v".into())],
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.get(&ColumnLocation::new(TableId::new(7), 1), &key_bytes(1)),
            Some(Datum::Text("v".into()))
        );
        assert!(cache.remove(TableId::new(7), &key_bytes(1)).is_some());
        assert!(cache.is_empty());
    }
}
