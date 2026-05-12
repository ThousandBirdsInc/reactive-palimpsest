//! Property-based oracle equivalence tests for the relational operator
//! facade (§18.5). Each test generates a small random input, runs a
//! differential dataflow build of one operator, and asserts that the
//! output matches a naive in-memory oracle.

#![allow(clippy::all, clippy::nursery, clippy::pedantic)]

use std::collections::{BTreeMap, BTreeSet, HashMap};

use palimpsest_dataflow::input::Input;
use palimpsest_dataflow::palimpsest::{
    aggregate_i64, distinct, equi_join, filter, left_join, project, topk, union, union_distinct,
    AggregateFunc, AggregateValue, SortDirection,
};
use proptest::collection::vec;
use proptest::prelude::*;

fn multiset<T: Ord>(values: impl IntoIterator<Item = T>) -> BTreeMap<T, isize> {
    let mut map = BTreeMap::new();
    for value in values {
        *map.entry(value).or_insert(0) += 1;
    }
    map
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn filter_matches_naive_oracle(values in vec(0_i64..200, 0..32)) {
        let expected = values
            .iter()
            .copied()
            .filter(|value| value % 2 == 0)
            .collect::<Vec<_>>();

        timely::example(move |scope| {
            let input = scope.new_collection_from(values).1;
            let actual = filter(&input, |value| value % 2 == 0);
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn project_matches_naive_oracle(values in vec(-50_i64..50, 0..32)) {
        let expected = values.iter().copied().map(|value| value * 3).collect::<Vec<_>>();

        timely::example(move |scope| {
            let input = scope.new_collection_from(values).1;
            let actual = project(&input, |value| value * 3);
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn distinct_matches_naive_oracle(values in vec(0_u64..16, 0..32)) {
        let expected: Vec<u64> = values.iter().copied().collect::<BTreeSet<_>>().into_iter().collect();

        timely::example(move |scope| {
            let input = scope.new_collection_from(values).1;
            let actual = distinct(&input);
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn union_matches_naive_oracle(left in vec(0_u64..16, 0..16), right in vec(0_u64..16, 0..16)) {
        let combined = left
            .iter()
            .copied()
            .chain(right.iter().copied())
            .collect::<Vec<_>>();

        timely::example(move |scope| {
            let left_collection = scope.new_collection_from(left).1;
            let right_collection = scope.new_collection_from(right).1;
            let actual = union(&left_collection, &right_collection);
            let expected_collection = scope.new_collection_from(combined).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn union_distinct_matches_naive_oracle(left in vec(0_u64..16, 0..16), right in vec(0_u64..16, 0..16)) {
        let expected: Vec<u64> = left
            .iter()
            .copied()
            .chain(right.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        timely::example(move |scope| {
            let left_collection = scope.new_collection_from(left).1;
            let right_collection = scope.new_collection_from(right).1;
            let actual = union_distinct(&left_collection, &right_collection);
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn equi_join_matches_naive_oracle(
        left in vec((0_u64..8, -10_i64..10), 0..16),
        right in vec((0_u64..8, -10_i64..10), 0..16),
    ) {
        let mut right_index = HashMap::<u64, Vec<i64>>::new();
        for (key, value) in &right {
            right_index.entry(*key).or_default().push(*value);
        }
        let mut expected = Vec::new();
        for (key, left_value) in &left {
            for right_value in right_index.get(key).into_iter().flatten() {
                expected.push((*key, *left_value, *right_value));
            }
        }

        timely::example(move |scope| {
            let left_collection = scope.new_collection_from(left).1;
            let right_collection = scope.new_collection_from(right).1;
            let actual = equi_join(&left_collection, &right_collection, |key, left_value, right_value| {
                (*key, *left_value, *right_value)
            });
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn left_join_matches_naive_oracle(
        left in vec((0_u64..8, 0_u32..32), 0..16),
        right in vec((0_u64..8, 0_u32..32), 0..16),
    ) {
        let mut right_index = HashMap::<u64, Vec<u32>>::new();
        for (key, value) in &right {
            right_index.entry(*key).or_default().push(*value);
        }
        let mut expected = Vec::new();
        for (key, left_value) in &left {
            match right_index.get(key) {
                Some(values) if !values.is_empty() => {
                    for right_value in values {
                        expected.push((*key, *left_value, Some(*right_value)));
                    }
                }
                _ => expected.push((*key, *left_value, None)),
            }
        }

        timely::example(move |scope| {
            let left_collection = scope.new_collection_from(left).1;
            let right_collection = scope.new_collection_from(right).1;
            let actual = left_join(&left_collection, &right_collection, |key, left_value, right_value| {
                (*key, *left_value, right_value.copied())
            });
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn aggregate_count_matches_naive_oracle(values in vec((0_u64..6, -10_i64..10), 0..16)) {
        let mut counts = BTreeMap::<u64, i128>::new();
        for (key, _value) in &values {
            *counts.entry(*key).or_default() += 1;
        }
        let expected = counts
            .into_iter()
            .map(|(key, count)| (key, vec![AggregateValue::Integer(count)]))
            .collect::<Vec<_>>();

        timely::example(move |scope| {
            let input = scope.new_collection_from(values).1;
            let actual = aggregate_i64(&input, vec![AggregateFunc::Count]);
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }

    #[test]
    fn topk_descending_matches_naive_oracle(values in vec(0_i64..256, 0..16)) {
        let direction = SortDirection::Descending;
        let limit = 4_usize;
        let offset = 1_usize;
        let counts = multiset(values.iter().copied());
        let mut expanded: Vec<i64> = counts
            .into_iter()
            .flat_map(|(value, count)| std::iter::repeat(value).take(usize::try_from(count).unwrap_or(0)))
            .collect();
        expanded.sort_by(|left, right| right.cmp(left));
        let expected = expanded.into_iter().skip(offset).take(limit).collect::<Vec<_>>();

        timely::example(move |scope| {
            let input = scope.new_collection_from(values).1;
            let actual = topk(&input, direction, limit, offset);
            let expected_collection = scope.new_collection_from(expected).1;
            actual.assert_eq(&expected_collection);
        });
    }
}
