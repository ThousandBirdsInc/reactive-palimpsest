// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#[cfg(test)]
mod tests {
    use proptest::{option, prelude::*};

    use crate::parse_select;

    const TABLES: &[&str] = &["posts", "authors", "comments"];
    const COLUMNS: &[&str] = &["id", "author_id", "created_at", "title"];

    proptest! {
        #[test]
        fn parse_unparse_reparse_round_trips(query in supported_select()) {
            let statement = parse_select(&query)?;
            let rendered = statement.to_string();
            let reparsed = parse_select(&rendered)?;

            prop_assert_eq!(statement, reparsed);
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
}
