// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#[cfg(test)]
mod tests {
    use proptest::{option, prelude::*};

    use crate::{canonical::canonical_key, lower::parse_and_lower, parse_select};

    const TABLES: &[&str] = &["posts", "authors", "comments"];
    const COLUMNS: &[&str] = &["id", "author_id", "created_at", "title"];
    const CTE_NAMES: &[&str] = &["recent_posts", "visible_posts", "filtered_posts"];

    proptest! {
        #[test]
        fn parse_unparse_reparse_round_trips(query in supported_select()) {
            let statement = parse_select(&query)?;
            let rendered = statement.to_string();
            let reparsed = parse_select(&rendered)?;

            prop_assert_eq!(statement, reparsed);
        }

        #[test]
        fn alpha_renaming_cte_preserves_canonical_key((left_name, right_name) in distinct_cte_names()) {
            let left = cte_query(&left_name);
            let right = cte_query(&right_name);

            let left = parse_and_lower(&left)?;
            let right = parse_and_lower(&right)?;

            prop_assert_eq!(canonical_key(&left), canonical_key(&right));
        }
    }

    fn supported_select() -> impl Strategy<Value = String> {
        (
            select_list(),
            table_name(),
            option::of(predicate()),
            option::of(order_limit()),
        )
            .prop_map(|(projection, table, predicate, order_limit)| {
                let mut query = format!("SELECT {projection} FROM {table}");
                if let Some(predicate) = predicate {
                    query.push_str(" WHERE ");
                    query.push_str(&predicate);
                }
                if let Some(order_limit) = order_limit {
                    query.push(' ');
                    query.push_str(&order_limit);
                }
                query
            })
    }

    fn select_list() -> impl Strategy<Value = String> {
        prop::collection::vec(column_name(), 1..=3).prop_map(|columns| columns.join(", "))
    }

    fn predicate() -> impl Strategy<Value = String> {
        (column_name(), 0_i64..1000).prop_map(|(column, value)| format!("{column} = {value}"))
    }

    fn order_limit() -> impl Strategy<Value = String> {
        (column_name(), any::<bool>(), 1_u16..100).prop_map(|(column, descending, limit)| {
            let direction = if descending { " DESC" } else { "" };
            format!("ORDER BY {column}{direction} LIMIT {limit}")
        })
    }

    fn table_name() -> impl Strategy<Value = String> {
        prop::sample::select(TABLES).prop_map(str::to_owned)
    }

    fn column_name() -> impl Strategy<Value = String> {
        prop::sample::select(COLUMNS).prop_map(str::to_owned)
    }

    fn distinct_cte_names() -> impl Strategy<Value = (String, String)> {
        (
            prop::sample::select(CTE_NAMES),
            prop::sample::select(CTE_NAMES),
        )
            .prop_filter("CTE names must differ", |(left, right)| left != right)
            .prop_map(|(left, right)| (left.to_owned(), right.to_owned()))
    }

    fn cte_query(name: &str) -> String {
        format!(
            "WITH {name} AS (
                SELECT id FROM posts WHERE author_id = 42
             )
             SELECT id FROM {name}"
        )
    }
}
