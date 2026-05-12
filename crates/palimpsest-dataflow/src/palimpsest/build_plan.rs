//! Build-plan executor.
//!
//! A *build plan* is the dataflow assembly step for a query: given a
//! timely scope, the caller installs operators and produces a
//! `(ProbeHandle, TraceHandle)` pair that downstream subsystems use to
//! follow frontier progress and reuse the materialized arrangement. The
//! registry stores those pairs by canonical name so subscription and
//! snapshot code can look them up later.
//!
//! The recommended call shape is:
//!
//! ```ignore
//! let plan = worker.dataflow::<Lsn, _, _>(|scope| {
//!     let collection = scope.new_collection_from(/* ... */).1;
//!     let arranged = collection.arrange_by_self();
//!     let probe = arranged.stream.probe();
//!     RegisteredPlan { probe, trace: arranged.trace }
//! });
//! registry.register("query-name", plan).unwrap();
//! ```
//!
//! Constructing the plan inside the timely closure keeps the scope
//! generics local to the build site, which avoids higher-ranked closure
//! inference problems if the registry is parameterized over many
//! concrete trace types.

use std::collections::{btree_map::Entry, BTreeMap};

use timely::dataflow::operators::probe;

use crate::{operators::arrange::TraceAgent, trace::TraceReader};

/// Probe handle exposed by a registered build plan.
pub type ProbeHandle<T> = probe::Handle<T>;

/// Trace handle exposed by a registered build plan.
pub type TraceHandle<Tr> = TraceAgent<Tr>;

/// Handles emitted by a build plan and stored by the registry.
pub struct RegisteredPlan<T, Tr>
where
    T: timely::progress::Timestamp,
    Tr: TraceReader,
{
    /// Probe used to observe the dataflow's output frontier.
    pub probe: ProbeHandle<T>,
    /// Trace used to read the materialized arrangement.
    pub trace: TraceHandle<Tr>,
}

impl<T, Tr> std::fmt::Debug for RegisteredPlan<T, Tr>
where
    T: timely::progress::Timestamp,
    Tr: TraceReader,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let frontier_len = self.probe.with_frontier(|frontier| frontier.len());
        f.debug_struct("RegisteredPlan")
            .field("frontier_len", &frontier_len)
            .finish_non_exhaustive()
    }
}

impl<T, Tr> Clone for RegisteredPlan<T, Tr>
where
    T: timely::progress::Timestamp,
    Tr: TraceReader,
    TraceHandle<Tr>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            probe: self.probe.clone(),
            trace: self.trace.clone(),
        }
    }
}

/// Trait alias for build closures. The blanket impl makes any `FnOnce`
/// that returns the registered handles a usable build plan.
pub trait BuildPlan<G, Tr>
where
    G: timely::dataflow::Scope,
    Tr: TraceReader<Time = G::Timestamp>,
{
    /// Builds the dataflow inside `scope` and produces the registered handles.
    fn build(self, scope: &mut G) -> RegisteredPlan<G::Timestamp, Tr>;
}

impl<G, Tr, F> BuildPlan<G, Tr> for F
where
    G: timely::dataflow::Scope,
    Tr: TraceReader<Time = G::Timestamp>,
    F: FnOnce(&mut G) -> RegisteredPlan<G::Timestamp, Tr>,
{
    fn build(self, scope: &mut G) -> RegisteredPlan<G::Timestamp, Tr> {
        (self)(scope)
    }
}

/// Error returned when registering two plans under the same name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanAlreadyRegistered {
    /// Name that was already taken.
    pub name: String,
}

impl std::fmt::Display for PlanAlreadyRegistered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "build plan '{}' is already registered", self.name)
    }
}

impl std::error::Error for PlanAlreadyRegistered {}

/// Registry of build-plan handles keyed by canonical query name.
pub struct BuildPlanRegistry<T, Tr>
where
    T: timely::progress::Timestamp,
    Tr: TraceReader,
{
    plans: BTreeMap<String, RegisteredPlan<T, Tr>>,
}

impl<T, Tr> Default for BuildPlanRegistry<T, Tr>
where
    T: timely::progress::Timestamp,
    Tr: TraceReader,
{
    fn default() -> Self {
        Self {
            plans: BTreeMap::new(),
        }
    }
}

impl<T, Tr> std::fmt::Debug for BuildPlanRegistry<T, Tr>
where
    T: timely::progress::Timestamp,
    Tr: TraceReader,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildPlanRegistry")
            .field("names", &self.plans.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl<T, Tr> BuildPlanRegistry<T, Tr>
where
    T: timely::progress::Timestamp,
    Tr: TraceReader<Time = T>,
    TraceHandle<Tr>: Clone,
{
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a pre-built plan under `name`.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        plan: RegisteredPlan<T, Tr>,
    ) -> Result<RegisteredPlan<T, Tr>, PlanAlreadyRegistered> {
        let name = name.into();
        match self.plans.entry(name) {
            Entry::Vacant(slot) => {
                let snapshot = plan.clone();
                slot.insert(plan);
                Ok(snapshot)
            }
            Entry::Occupied(slot) => Err(PlanAlreadyRegistered {
                name: slot.key().clone(),
            }),
        }
    }

    /// Runs `plan` against `scope` and registers the resulting handles.
    ///
    /// Convenience wrapper for the recommended `dataflow(|scope| { ... })`
    /// call shape — see the module docs.
    pub fn build_and_register<G, P>(
        &mut self,
        scope: &mut G,
        name: impl Into<String>,
        plan: P,
    ) -> Result<RegisteredPlan<T, Tr>, PlanAlreadyRegistered>
    where
        G: timely::dataflow::Scope<Timestamp = T>,
        P: BuildPlan<G, Tr>,
    {
        let plan = plan.build(scope);
        self.register(name, plan)
    }

    /// Looks up a previously-registered plan by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<RegisteredPlan<T, Tr>> {
        self.plans.get(name).cloned()
    }

    /// Removes a plan from the registry, returning its handles.
    pub fn remove(&mut self, name: &str) -> Option<RegisteredPlan<T, Tr>> {
        self.plans.remove(name)
    }

    /// Replaces an existing plan with a freshly built one.
    pub fn replace(
        &mut self,
        name: impl Into<String>,
        plan: RegisteredPlan<T, Tr>,
    ) -> RegisteredPlan<T, Tr> {
        let name = name.into();
        let snapshot = plan.clone();
        self.plans.insert(name, plan);
        snapshot
    }

    /// Returns the names of registered plans in canonical order.
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.plans.keys().map(String::as_str)
    }

    /// Returns the number of registered plans.
    #[must_use]
    pub fn len(&self) -> usize {
        self.plans.len()
    }

    /// Returns true when the registry holds no plans.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }

    /// Clears every entry, dropping the held handles.
    pub fn clear(&mut self) {
        self.plans.clear();
    }
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::Probe;

    use super::{BuildPlanRegistry, PlanAlreadyRegistered, RegisteredPlan};
    use crate::{
        input::Input, operators::arrange::ArrangeBySelf, palimpsest::Lsn, trace::TraceReader,
    };

    type TestTrace = crate::trace::implementations::KeySpine<u64, Lsn, isize>;

    fn build_plan<G>(scope: &mut G, seed: Vec<u64>) -> RegisteredPlan<Lsn, TestTrace>
    where
        G: timely::dataflow::Scope<Timestamp = Lsn>,
    {
        let collection = scope.new_collection_from(seed).1;
        let arranged = collection.arrange_by_self();
        let probe = arranged.stream.probe();
        RegisteredPlan {
            probe,
            trace: arranged.trace,
        }
    }

    #[test]
    fn register_stores_handles_under_canonical_name() {
        timely::execute_directly(|worker| {
            let mut registry = BuildPlanRegistry::<Lsn, TestTrace>::new();
            let plan = worker.dataflow::<Lsn, _, _>(|scope| build_plan(scope, vec![1_u64, 2, 3]));

            let registered = registry
                .register("all-posts", plan)
                .expect("first register should succeed");

            assert_eq!(registry.len(), 1);
            assert_eq!(registry.names().collect::<Vec<_>>(), ["all-posts"]);
            let frontier_len = registered.probe.with_frontier(|frontier| frontier.len());
            assert_eq!(frontier_len, 1);
            let mut clone = registered.trace.clone();
            let _ = clone.get_logical_compaction();
        });
    }

    #[test]
    fn duplicate_name_returns_error() {
        timely::execute_directly(|worker| {
            let mut registry = BuildPlanRegistry::<Lsn, TestTrace>::new();
            let first = worker.dataflow::<Lsn, _, _>(|scope| build_plan(scope, vec![1_u64]));
            registry.register("q", first).unwrap();

            let second = worker.dataflow::<Lsn, _, _>(|scope| build_plan(scope, vec![2_u64]));
            let err = registry
                .register("q", second)
                .expect_err("duplicate registration must fail");
            assert_eq!(err, PlanAlreadyRegistered { name: "q".into() });
        });
    }

    #[test]
    fn replace_overwrites_and_clear_drops_all_plans() {
        timely::execute_directly(|worker| {
            let mut registry = BuildPlanRegistry::<Lsn, TestTrace>::new();
            let first = worker.dataflow::<Lsn, _, _>(|scope| build_plan(scope, vec![1_u64]));
            registry.register("q", first).unwrap();

            let replacement = worker.dataflow::<Lsn, _, _>(|scope| build_plan(scope, vec![5_u64]));
            registry.replace("q", replacement);

            assert_eq!(registry.len(), 1);
            assert!(registry.get("q").is_some());
            registry.clear();
            assert!(registry.is_empty());
        });
    }

    #[test]
    fn build_and_register_accepts_a_closure() {
        timely::execute_directly(|worker| {
            let mut registry = BuildPlanRegistry::<Lsn, TestTrace>::new();
            worker.dataflow::<Lsn, _, _>(|scope| {
                registry
                    .build_and_register(scope, "via-closure", |scope: &mut _| {
                        build_plan(scope, vec![10_u64])
                    })
                    .expect("first register via closure");
            });

            assert!(registry.get("via-closure").is_some());
        });
    }
}
