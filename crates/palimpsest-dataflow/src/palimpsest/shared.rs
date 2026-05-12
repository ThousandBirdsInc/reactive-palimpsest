//! Shared subgraph reference tracking.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Stable identifier for a shared dataflow subgraph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SharedSubgraphId(u64);

impl SharedSubgraphId {
    /// Creates a shared subgraph identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Result of acquiring a shared subgraph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedSubgraphAcquire {
    /// The registry created a new subgraph slot for this key.
    Created {
        /// Shared subgraph identifier.
        id: SharedSubgraphId,
    },
    /// The registry reused an existing subgraph slot.
    Reused {
        /// Shared subgraph identifier.
        id: SharedSubgraphId,
        /// New reference count after the acquire.
        ref_count: usize,
    },
}

impl SharedSubgraphAcquire {
    /// Returns the acquired shared subgraph identifier.
    #[must_use]
    pub const fn id(&self) -> SharedSubgraphId {
        match self {
            Self::Created { id } | Self::Reused { id, .. } => *id,
        }
    }
}

/// Result of releasing a shared subgraph reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedSubgraphRelease {
    /// Other subscribers still hold references.
    StillReferenced {
        /// Shared subgraph identifier.
        id: SharedSubgraphId,
        /// Remaining reference count.
        ref_count: usize,
    },
    /// The last subscriber was released and the runtime should tear down the subgraph.
    Teardown {
        /// Shared subgraph identifier.
        id: SharedSubgraphId,
        /// Canonical key associated with the subgraph.
        key: String,
    },
}

/// Reference-counted registry for canonical shared subgraphs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharedSubgraphRegistry {
    next_id: u64,
    ids_by_key: BTreeMap<String, SharedSubgraphId>,
    entries: BTreeMap<SharedSubgraphId, SharedSubgraphEntry>,
}

impl SharedSubgraphRegistry {
    /// Creates an empty shared subgraph registry.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next_id: 0,
            ids_by_key: BTreeMap::new(),
            entries: BTreeMap::new(),
        }
    }

    /// Acquires a reference to the canonical subgraph for `key`.
    pub fn acquire(&mut self, key: impl Into<String>) -> SharedSubgraphAcquire {
        let key = key.into();
        if let Some(id) = self.ids_by_key.get(&key).copied() {
            let entry = self
                .entries
                .get_mut(&id)
                .expect("shared subgraph key index points at a missing entry");
            entry.ref_count = entry.ref_count.saturating_add(1);
            return SharedSubgraphAcquire::Reused {
                id,
                ref_count: entry.ref_count,
            };
        }

        let id = SharedSubgraphId(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        self.ids_by_key.insert(key.clone(), id);
        self.entries
            .insert(id, SharedSubgraphEntry { key, ref_count: 1 });
        SharedSubgraphAcquire::Created { id }
    }

    /// Releases one reference and reports whether teardown is now required.
    pub fn release(&mut self, id: SharedSubgraphId) -> Option<SharedSubgraphRelease> {
        let entry = self.entries.get_mut(&id)?;
        entry.ref_count = entry.ref_count.saturating_sub(1);

        if entry.ref_count > 0 {
            return Some(SharedSubgraphRelease::StillReferenced {
                id,
                ref_count: entry.ref_count,
            });
        }

        let entry = self.entries.remove(&id)?;
        self.ids_by_key.remove(&entry.key);
        Some(SharedSubgraphRelease::Teardown { id, key: entry.key })
    }

    /// Resolves a canonical key to an active shared subgraph identifier.
    #[must_use]
    pub fn resolve(&self, key: &str) -> Option<SharedSubgraphId> {
        self.ids_by_key.get(key).copied()
    }

    /// Returns the active reference count for `id`.
    #[must_use]
    pub fn ref_count(&self, id: SharedSubgraphId) -> Option<usize> {
        self.entries.get(&id).map(|entry| entry.ref_count)
    }

    /// Returns the number of active shared subgraphs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true when no shared subgraphs are active.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SharedSubgraphEntry {
    key: String,
    ref_count: usize,
}

#[cfg(test)]
mod tests {
    use super::{
        SharedSubgraphAcquire, SharedSubgraphId, SharedSubgraphRegistry, SharedSubgraphRelease,
    };

    #[test]
    fn acquire_reuses_existing_subgraph_by_key() {
        let mut registry = SharedSubgraphRegistry::new();
        let first = registry.acquire("canonical:posts-by-author");
        let id = first.id();

        assert_eq!(first, SharedSubgraphAcquire::Created { id });
        assert_eq!(
            registry.acquire("canonical:posts-by-author"),
            SharedSubgraphAcquire::Reused { id, ref_count: 2 }
        );
        assert_eq!(registry.resolve("canonical:posts-by-author"), Some(id));
        assert_eq!(registry.ref_count(id), Some(2));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn release_reports_teardown_on_last_reference() {
        let mut registry = SharedSubgraphRegistry::new();
        let id = registry.acquire("canonical:recent-posts").id();
        registry.acquire("canonical:recent-posts");

        assert_eq!(
            registry.release(id),
            Some(SharedSubgraphRelease::StillReferenced { id, ref_count: 1 })
        );
        assert_eq!(
            registry.release(id),
            Some(SharedSubgraphRelease::Teardown {
                id,
                key: "canonical:recent-posts".to_owned(),
            })
        );
        assert_eq!(registry.resolve("canonical:recent-posts"), None);
        assert!(registry.is_empty());
    }

    #[test]
    fn release_unknown_subgraph_is_noop() {
        let mut registry = SharedSubgraphRegistry::new();

        assert_eq!(registry.release(SharedSubgraphId::new(99)), None);
    }
}
