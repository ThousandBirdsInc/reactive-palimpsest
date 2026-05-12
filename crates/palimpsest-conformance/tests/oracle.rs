// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §18.13.5 — oracle conformance.
//!
//! Replays the property-test corpus on real Postgres and compares the
//! resulting row sets against `ReferenceExecutor`. The corpus lives in
//! `corpus/oracle.sql` so it can be diffed in PR review.
//!
//! Without `--features real-postgres`, this test is a no-op.

#![allow(clippy::missing_const_for_fn)]

#[test]
fn skip_without_real_postgres_feature() {
    #[cfg(not(feature = "real-postgres"))]
    {
        eprintln!("[conformance/oracle] skipped (build with --features real-postgres)");
    }
}

#[cfg(feature = "real-postgres")]
mod with_pg {
    use std::{collections::BTreeSet, sync::Arc};

    use palimpsest_conformance::harness::connect_or_skip;
    use palimpsest_sql::lower::parse_and_lower;
    use palimpsest_test_harness::{
        Catalog, ColumnDef, LogicalEvent, ReferenceExecutor, TableDef, TableId,
    };

    fn schema_setup_sql() -> &'static str {
        // Idempotent: drop+recreate so reruns are deterministic.
        // We use the table name `posts` so the same SQL string the
        // engine sees (`SELECT ... FROM posts`) matches against
        // real-PG. The data shape mirrors the property-test corpus
        // (`palimpsest-properties/tests/oracle_equivalence.rs`).
        "DROP TABLE IF EXISTS posts;
         CREATE TABLE posts (
            id BIGINT PRIMARY KEY,
            author_id BIGINT NOT NULL,
            score BIGINT NOT NULL
         );"
    }

    fn corpus() -> Vec<(i64, i64, i64)> {
        vec![(1, 7, 10), (2, 7, 20), (3, 8, 30), (4, 8, 40), (5, 9, 50)]
    }

    /// Each entry: (engine SQL == real-PG SQL, projected i64 column).
    /// We project a single i64 column per query so the equivalence
    /// check is a clean `BTreeSet<i64>` comparison.
    fn queries() -> Vec<&'static str> {
        vec![
            "SELECT id FROM posts",
            "SELECT id FROM posts WHERE author_id = 7",
            "SELECT id FROM posts WHERE score > 20",
            "SELECT author_id FROM posts",
        ]
    }

    fn build_reference() -> ReferenceExecutor {
        let table_id = TableId::new(1);
        let columns = vec![
            ColumnDef {
                name: "id".into(),
                type_oid: 20,
                nullable: false,
            },
            ColumnDef {
                name: "author_id".into(),
                type_oid: 20,
                nullable: false,
            },
            ColumnDef {
                name: "score".into(),
                type_oid: 20,
                nullable: false,
            },
        ];
        let table = TableDef::new(table_id, "posts", columns);
        let catalog = Arc::new(Catalog::with_tables([table]));
        let mut reference = ReferenceExecutor::new(catalog);
        let mut events = vec![LogicalEvent::Begin { xid: 1 }];
        for (id, author_id, score) in corpus() {
            events.push(LogicalEvent::Insert {
                table: table_id,
                new: vec![id.to_string(), author_id.to_string(), score.to_string()],
            });
        }
        events.push(LogicalEvent::Commit);
        reference.apply(&events);
        reference
    }

    #[tokio::test]
    async fn corpus_matches_reference_executor() {
        let Some(conn) = connect_or_skip("oracle").await else {
            return;
        };

        // Real-PG side.
        conn.client
            .batch_execute(schema_setup_sql())
            .await
            .expect("ddl");
        for (id, author_id, score) in corpus() {
            conn.client
                .execute(
                    "INSERT INTO posts(id, author_id, score) VALUES ($1,$2,$3)",
                    &[&id, &author_id, &score],
                )
                .await
                .expect("insert");
        }

        let reference = build_reference();

        for sql in queries() {
            let pg_rows = conn.client.query(sql, &[]).await.expect("real-pg query");
            let pg_set: BTreeSet<i64> = pg_rows.iter().map(|r| r.get::<_, i64>(0)).collect();

            let graph = parse_and_lower(sql).expect("parse_and_lower");
            let ref_rows = reference.execute(&graph);
            let ref_set: BTreeSet<i64> = ref_rows
                .iter()
                .map(|row| row[0].parse::<i64>().expect("i64"))
                .collect();

            assert_eq!(
                pg_set, ref_set,
                "{sql}: real-PG vs ReferenceExecutor diverged"
            );
        }
    }
}
