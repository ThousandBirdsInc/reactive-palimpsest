//! Relational operator facade for Palimpsest query execution.

use timely::dataflow::Scope;

use crate::{
    difference::{Multiply, Semigroup},
    hashable::Hashable,
    lattice::Lattice,
    operators::{Join, Threshold},
    Data, ExchangeData, VecCollection,
};

/// Applies a row predicate with differential's `filter` operator.
pub fn filter<G, D, R, F>(input: &VecCollection<G, D, R>, predicate: F) -> VecCollection<G, D, R>
where
    G: Scope,
    D: Clone + 'static,
    R: Clone + 'static,
    F: FnMut(&D) -> bool + 'static,
{
    input.filter(predicate)
}

/// Applies projection logic with differential's `map` operator.
pub fn project<G, D, D2, R, F>(
    input: &VecCollection<G, D, R>,
    projection: F,
) -> VecCollection<G, D2, R>
where
    G: Scope,
    D: Clone + 'static,
    D2: Data,
    R: Clone + 'static,
    F: FnMut(D) -> D2 + 'static,
{
    input.map(projection)
}

/// Applies an equi-join using arrangements keyed by the tuple key.
pub fn equi_join<G, K, V, V2, R, R2, D, F>(
    left: &VecCollection<G, (K, V), R>,
    right: &VecCollection<G, (K, V2), R2>,
    projection: F,
) -> VecCollection<G, D, <R as Multiply<R2>>::Output>
where
    G: Scope<Timestamp: Lattice + Ord>,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    V2: ExchangeData,
    R: ExchangeData + Semigroup + Multiply<R2, Output: Semigroup + 'static>,
    R2: ExchangeData + Semigroup,
    D: Data,
    F: FnMut(&K, &V, &V2) -> D + 'static,
{
    left.join_map(right, projection)
}

/// Applies differential's `distinct` operator.
pub fn distinct<G, D, R>(input: &VecCollection<G, D, R>) -> VecCollection<G, D, isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    D: ExchangeData + Hashable,
    R: ExchangeData + Semigroup,
{
    input.distinct()
}

/// Applies differential's `concat` operator as SQL `UNION ALL`.
pub fn union<G, D, R>(
    left: &VecCollection<G, D, R>,
    right: &VecCollection<G, D, R>,
) -> VecCollection<G, D, R>
where
    G: Scope,
    D: Clone + 'static,
    R: Clone + 'static,
{
    left.concat(right)
}

/// Applies SQL `UNION DISTINCT` as concat followed by distinct.
pub fn union_distinct<G, D, R>(
    left: &VecCollection<G, D, R>,
    right: &VecCollection<G, D, R>,
) -> VecCollection<G, D, isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    D: ExchangeData + Hashable,
    R: ExchangeData + Semigroup,
{
    union(left, right).distinct()
}

#[cfg(test)]
mod tests {
    use crate::input::Input;

    use super::{distinct, equi_join, filter, project, union, union_distinct};

    #[test]
    fn filter_and_project_delegate_to_differential_operators() {
        timely::example(|scope| {
            let input = scope.new_collection_from(0..5).1;
            let actual = project(&filter(&input, |value| value % 2 == 0), |value| value * 10);
            let expected = scope.new_collection_from(vec![0, 20, 40]).1;

            actual.assert_eq(&expected);
        });
    }

    #[test]
    fn equi_join_uses_keyed_arrangements() {
        timely::example(|scope| {
            let left = scope
                .new_collection_from(vec![(1_u64, String::from("a")), (2, String::from("b"))])
                .1;
            let right = scope.new_collection_from(vec![(1_u64, 10), (3, 30)]).1;
            let actual = equi_join(&left, &right, |key, left, right| {
                (*key, format!("{left}:{right}"))
            });
            let expected = scope
                .new_collection_from(vec![(1_u64, String::from("a:10"))])
                .1;

            actual.assert_eq(&expected);
        });
    }

    #[test]
    fn distinct_and_union_delegate_to_differential_operators() {
        timely::example(|scope| {
            let left = scope.new_collection_from(vec![1, 1, 2]).1;
            let right = scope.new_collection_from(vec![2, 3]).1;
            let all = union(&left, &right).consolidate();
            let all_expected = scope.new_collection_from(vec![1, 1, 2, 2, 3]).1;
            let distinct_actual = distinct(&all);
            let distinct_expected = scope.new_collection_from(vec![1, 2, 3]).1;

            all.assert_eq(&all_expected);
            distinct_actual.assert_eq(&distinct_expected);
        });
    }

    #[test]
    fn union_distinct_concats_then_distincts() {
        timely::example(|scope| {
            let left = scope.new_collection_from(vec![1, 2]).1;
            let right = scope.new_collection_from(vec![2, 3]).1;
            let actual = union_distinct(&left, &right);
            let expected = scope.new_collection_from(vec![1, 2, 3]).1;

            actual.assert_eq(&expected);
        });
    }
}
