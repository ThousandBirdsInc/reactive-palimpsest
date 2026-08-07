//! Relational operator facade for Palimpsest query execution.

use timely::dataflow::Scope;

use serde::{Deserialize, Serialize};

use crate::{
    difference::{Multiply, Semigroup},
    hashable::Hashable,
    lattice::Lattice,
    operators::{Join, Reduce, Threshold},
    Data, ExchangeData, VecCollection,
};

/// Numeric aggregate functions supported by the Palimpsest facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AggregateFunc {
    /// Count rows in each group.
    Count,
    /// Sum `i64` values in each group.
    Sum,
    /// Minimum `i64` value in each group.
    Min,
    /// Maximum `i64` value in each group.
    Max,
    /// Exact average represented as sum/count.
    Avg,
    /// Count distinct values in each group.
    CountDistinct,
}

/// Result value for numeric aggregates.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AggregateValue {
    /// Integer result.
    Integer(i128),
    /// Exact average as sum/count.
    Average {
        /// Numerator.
        sum: i128,
        /// Denominator.
        count: i128,
    },
}

/// Sort direction for TopK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Smallest values first.
    Ascending,
    /// Largest values first.
    Descending,
}

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

/// Applies a left join using `join_map` plus a distinct-key antijoin for unmatched rows.
pub fn left_join<G, K, V, V2, D, F>(
    left: &VecCollection<G, (K, V), isize>,
    right: &VecCollection<G, (K, V2), isize>,
    mut projection: F,
) -> VecCollection<G, D, isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    V2: ExchangeData,
    D: Data,
    F: FnMut(&K, &V, Option<&V2>) -> D + Clone + 'static,
{
    let matched = left.join_map(right, {
        let mut projection = projection.clone();
        move |key, left, right| projection(key, left, Some(right))
    });
    let right_keys = right.map(|(key, _value)| key).distinct();
    let unmatched = left
        .antijoin(&right_keys)
        .map(move |(key, value)| projection(&key, &value, None));

    matched.concat(&unmatched)
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

/// Applies grouped numeric aggregates with differential's `reduce` operator.
pub fn aggregate_i64<G, K>(
    input: &VecCollection<G, (K, i64), isize>,
    funcs: Vec<AggregateFunc>,
) -> VecCollection<G, (K, Vec<AggregateValue>), isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    K: ExchangeData + Hashable,
{
    input.reduce(move |_key, values, output| {
        let aggregates = funcs
            .iter()
            .map(|func| evaluate_i64_aggregate(*func, values))
            .collect();
        output.push((aggregates, 1));
    })
}

/// Grouped aggregates where every function reads its own value column:
/// each input row carries one `i64` per aggregate (in function order),
/// and `funcs[i]` is evaluated over column `i` of the value vectors.
pub fn aggregate_multi<G, K>(
    input: &VecCollection<G, (K, Vec<i64>), isize>,
    funcs: Vec<AggregateFunc>,
) -> VecCollection<G, (K, Vec<AggregateValue>), isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    K: ExchangeData + Hashable,
{
    input.reduce(move |_key, values, output| {
        let aggregates = funcs
            .iter()
            .enumerate()
            .map(|(index, func)| {
                // The reduce input is consolidated per value *vector*,
                // so one scalar can appear across several entries.
                // Re-consolidate the single column before evaluating —
                // MIN / MAX / COUNT DISTINCT inspect per-value signs
                // and would otherwise see phantom positives.
                let mut merged = std::collections::BTreeMap::<i64, isize>::new();
                for (row, diff) in values {
                    *merged.entry(*row.get(index).unwrap_or(&0)).or_default() += *diff;
                }
                let merged: Vec<(i64, isize)> =
                    merged.into_iter().filter(|(_, diff)| *diff != 0).collect();
                let column: Vec<(&i64, isize)> =
                    merged.iter().map(|(value, diff)| (value, *diff)).collect();
                evaluate_i64_aggregate(*func, &column)
            })
            .collect();
        output.push((aggregates, 1));
    })
}

fn evaluate_i64_aggregate(func: AggregateFunc, values: &[(&i64, isize)]) -> AggregateValue {
    let positive_values = values
        .iter()
        .filter_map(|(value, diff)| usize::try_from(*diff).ok().map(|count| (**value, count)));

    match func {
        AggregateFunc::Count => AggregateValue::Integer(
            values
                .iter()
                .filter_map(|(_value, diff)| i128::try_from(*diff).ok())
                .sum(),
        ),
        AggregateFunc::Sum => AggregateValue::Integer(
            values
                .iter()
                .map(|(value, diff)| i128::from(**value) * (*diff as i128))
                .sum(),
        ),
        AggregateFunc::Min => AggregateValue::Integer(
            positive_values
                .clone()
                .map(|(value, _count)| i128::from(value))
                .min()
                .unwrap_or_default(),
        ),
        AggregateFunc::Max => AggregateValue::Integer(
            positive_values
                .clone()
                .map(|(value, _count)| i128::from(value))
                .max()
                .unwrap_or_default(),
        ),
        AggregateFunc::Avg => {
            let mut sum = 0_i128;
            let mut count = 0_i128;
            for (value, diff) in values {
                sum += i128::from(**value) * (*diff as i128);
                count += *diff as i128;
            }
            AggregateValue::Average { sum, count }
        }
        AggregateFunc::CountDistinct => {
            AggregateValue::Integer(i128::try_from(positive_values.count()).unwrap_or(i128::MAX))
        }
    }
}

/// Applies global TopK with differential's `reduce` operator.
pub fn topk<G, D>(
    input: &VecCollection<G, D, isize>,
    direction: SortDirection,
    limit: usize,
    offset: usize,
) -> VecCollection<G, D, isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    D: ExchangeData + Hashable,
{
    input
        .map(|value| ((), value))
        .reduce(move |_key, values, output| {
            let mut expanded = Vec::new();
            for (value, diff) in values {
                if let Ok(count) = usize::try_from(*diff) {
                    expanded.extend(std::iter::repeat_with(|| (*value).clone()).take(count));
                }
            }
            if direction == SortDirection::Descending {
                expanded.reverse();
            }
            for value in expanded.into_iter().skip(offset).take(limit) {
                output.push((value, 1));
            }
        })
        .map(|(_key, value)| value)
}

/// Global TopK ordered by a caller-supplied comparator. Like [`topk`]
/// but the sort order is not the value's natural `Ord` — used for
/// multi-column `ORDER BY` with per-key directions and SQL null
/// placement.
pub fn topk_by<G, D, F>(
    input: &VecCollection<G, D, isize>,
    compare: F,
    limit: usize,
    offset: usize,
) -> VecCollection<G, D, isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    D: ExchangeData + Hashable,
    F: Fn(&D, &D) -> std::cmp::Ordering + 'static,
{
    input
        .map(|value| ((), value))
        .reduce(move |_key, values, output| {
            let mut expanded = Vec::new();
            for (value, diff) in values {
                if let Ok(count) = usize::try_from(*diff) {
                    expanded.extend(std::iter::repeat_with(|| (*value).clone()).take(count));
                }
            }
            expanded.sort_by(|left, right| compare(left, right));
            for value in expanded.into_iter().skip(offset).take(limit) {
                output.push((value, 1));
            }
        })
        .map(|(_key, value)| value)
}

/// Keeps the first value per key, ranked by `compare` (ties keep the
/// comparator's first minimum). SQL `SELECT DISTINCT ON (...)`.
pub fn distinct_on_first<G, K, D, F>(
    input: &VecCollection<G, (K, D), isize>,
    compare: F,
) -> VecCollection<G, D, isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    K: ExchangeData + Hashable,
    D: ExchangeData,
    F: Fn(&D, &D) -> std::cmp::Ordering + 'static,
{
    input
        .reduce(move |_key, values, output| {
            let first = values
                .iter()
                .filter(|(_, diff)| *diff > 0)
                .map(|(value, _)| (*value).clone())
                .min_by(|left, right| compare(left, right));
            if let Some(value) = first {
                output.push((value, 1));
            }
        })
        .map(|(_key, value)| value)
}

/// Bag-algebra set operation: for each distinct row, emits it with
/// multiplicity `combine(left_count, right_count)` (non-positive
/// results emit nothing). `EXCEPT [ALL]` and `INTERSECT [ALL]` are
/// thin wrappers over this.
pub fn bag_set_op<G, D, F>(
    left: &VecCollection<G, D, isize>,
    right: &VecCollection<G, D, isize>,
    combine: F,
) -> VecCollection<G, D, isize>
where
    G: Scope<Timestamp: Lattice + Ord>,
    D: ExchangeData + Hashable,
    F: Fn(isize, isize) -> isize + 'static,
{
    let tagged = left
        .map(|row| (row, false))
        .concat(&right.map(|row| (row, true)));
    tagged
        .reduce(move |_row, sides, output| {
            let mut left_count = 0;
            let mut right_count = 0;
            for (is_right, diff) in sides {
                if **is_right {
                    right_count += *diff;
                } else {
                    left_count += *diff;
                }
            }
            let multiplicity = combine(left_count, right_count);
            if multiplicity > 0 {
                output.push(((), multiplicity));
            }
        })
        .map(|(row, ())| row)
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

    use super::{
        aggregate_i64, distinct, equi_join, filter, left_join, project, topk, union,
        union_distinct, AggregateFunc, AggregateValue, SortDirection,
    };

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
    fn left_join_emits_matches_and_null_extended_unmatched_rows() {
        timely::example(|scope| {
            let left = scope
                .new_collection_from(vec![(1_u64, String::from("a")), (2, String::from("b"))])
                .1;
            let right = scope.new_collection_from(vec![(1_u64, 10), (3, 30)]).1;
            let actual = left_join(&left, &right, |key, left, right| {
                (*key, left.clone(), right.copied())
            });
            let expected = scope
                .new_collection_from(vec![
                    (1_u64, String::from("a"), Some(10)),
                    (2, String::from("b"), None),
                ])
                .1;

            actual.assert_eq(&expected);
        });
    }

    #[test]
    fn aggregate_i64_supports_numeric_aggregate_set() {
        timely::example(|scope| {
            let input = scope
                .new_collection_from(vec![(1_u64, 10), (1, 20), (1, 20), (2, 7)])
                .1;
            let actual = aggregate_i64(
                &input,
                vec![
                    AggregateFunc::Count,
                    AggregateFunc::Sum,
                    AggregateFunc::Min,
                    AggregateFunc::Max,
                    AggregateFunc::Avg,
                    AggregateFunc::CountDistinct,
                ],
            );
            let expected = scope
                .new_collection_from(vec![
                    (
                        1_u64,
                        vec![
                            AggregateValue::Integer(3),
                            AggregateValue::Integer(50),
                            AggregateValue::Integer(10),
                            AggregateValue::Integer(20),
                            AggregateValue::Average { sum: 50, count: 3 },
                            AggregateValue::Integer(2),
                        ],
                    ),
                    (
                        2,
                        vec![
                            AggregateValue::Integer(1),
                            AggregateValue::Integer(7),
                            AggregateValue::Integer(7),
                            AggregateValue::Integer(7),
                            AggregateValue::Average { sum: 7, count: 1 },
                            AggregateValue::Integer(1),
                        ],
                    ),
                ])
                .1;

            actual.assert_eq(&expected);
        });
    }

    #[test]
    fn topk_slices_ordered_rows_with_reduce() {
        timely::example(|scope| {
            let input = scope.new_collection_from(vec![5, 1, 3, 2, 4]).1;
            let actual = topk(&input, SortDirection::Descending, 2, 1);
            let expected = scope.new_collection_from(vec![4, 3]).1;

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
