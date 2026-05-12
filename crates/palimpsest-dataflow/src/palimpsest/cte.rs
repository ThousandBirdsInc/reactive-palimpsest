//! CteRef resolution against shared dataflow handles.
//!
//! A `CteRegistry` lets the build-plan executor register a single
//! materialization for a CTE and hand cloned `VecCollection` handles to
//! every downstream consumer. Cloning a [`Collection`] just copies the
//! upstream timely stream wiring, so all consumers share one operator
//! subgraph and one arrangement.

use std::collections::{btree_map::Entry, BTreeMap};

use timely::dataflow::Scope;

use crate::VecCollection;

/// Registry that maps CTE names to a single shared collection handle.
///
/// The registry is scope-local: handles are valid for the [`Scope`] used
/// when registering them.
pub struct CteRegistry<G, D, R = isize>
where
    G: Scope,
{
    collections: BTreeMap<String, VecCollection<G, D, R>>,
}

impl<G, D, R> Default for CteRegistry<G, D, R>
where
    G: Scope,
{
    fn default() -> Self {
        Self {
            collections: BTreeMap::new(),
        }
    }
}

impl<G, D, R> std::fmt::Debug for CteRegistry<G, D, R>
where
    G: Scope,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CteRegistry")
            .field("names", &self.collections.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Error returned when a CTE is registered twice with the same name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CteAlreadyRegistered {
    /// Name that was already in the registry.
    pub name: String,
}

impl std::fmt::Display for CteAlreadyRegistered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cte '{}' is already registered", self.name)
    }
}

impl std::error::Error for CteAlreadyRegistered {}

impl<G, D, R> CteRegistry<G, D, R>
where
    G: Scope,
    D: Clone + 'static,
    R: Clone + 'static,
{
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a CTE materialization. Returns an error if `name` is taken.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        collection: VecCollection<G, D, R>,
    ) -> Result<(), CteAlreadyRegistered> {
        let name = name.into();
        match self.collections.entry(name) {
            Entry::Vacant(slot) => {
                slot.insert(collection);
                Ok(())
            }
            Entry::Occupied(slot) => Err(CteAlreadyRegistered {
                name: slot.key().clone(),
            }),
        }
    }

    /// Resolves a CTE reference, cloning the shared handle.
    #[must_use]
    pub fn cte_ref(&self, name: &str) -> Option<VecCollection<G, D, R>> {
        self.collections.get(name).cloned()
    }

    /// Returns the names of registered CTEs in canonical order.
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.collections.keys().map(String::as_str)
    }

    /// Returns the number of registered CTEs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.collections.len()
    }

    /// Returns true when the registry holds no CTEs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.collections.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::CteRegistry;
    use crate::input::Input;

    #[test]
    fn cte_ref_returns_clone_of_registered_collection() {
        timely::example(|scope| {
            let mut registry = CteRegistry::<_, i32, isize>::new();
            let source = scope.new_collection_from(vec![1_i32, 2, 3]).1;

            registry
                .register("recent_posts", source.clone())
                .expect("first register should succeed");

            let handle = registry
                .cte_ref("recent_posts")
                .expect("registered CTE should resolve");
            handle.assert_eq(&source);
        });
    }

    #[test]
    fn cte_ref_returns_none_for_unknown_names() {
        timely::example(|scope| {
            let mut registry = CteRegistry::<_, u8, isize>::new();
            let collection = scope.new_collection_from(vec![0_u8]).1;
            registry.register("known", collection).unwrap();
            assert!(registry.cte_ref("missing").is_none());
        });
    }

    #[test]
    fn duplicate_registration_returns_error() {
        timely::example(|scope| {
            let mut registry = CteRegistry::<_, i32, isize>::new();
            let collection = scope.new_collection_from(vec![1_i32]).1;
            registry.register("x", collection.clone()).unwrap();
            let err = registry
                .register("x", collection)
                .expect_err("duplicate name should error");
            assert_eq!(err.name, "x");
        });
    }

    #[test]
    fn names_iterates_in_canonical_order() {
        timely::example(|scope| {
            let mut registry = CteRegistry::<_, i32, isize>::new();
            let collection = scope.new_collection_from(vec![1_i32]).1;
            registry.register("b", collection.clone()).unwrap();
            registry.register("a", collection).unwrap();

            let names: Vec<_> = registry.names().collect();
            assert_eq!(names, ["a", "b"]);
            assert_eq!(registry.len(), 2);
            assert!(!registry.is_empty());
        });
    }
}
