// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §18.13.4 — catalog-response conformance.
//!
//! We run a fixed list of `pg_catalog` SELECTs against real Postgres
//! and compare the row shapes against checked-in JSON fixtures under
//! `fixtures/`. To regenerate fixtures, run:
//!
//! ```bash
//! PALIMPSEST_PG_URL=... cargo run -p xtask -- regen-pg-fixtures
//! ```
//!
//! Without `--features real-postgres`, this test is a no-op.

#![allow(clippy::missing_const_for_fn)]

#[test]
fn skip_without_real_postgres_feature() {
    #[cfg(not(feature = "real-postgres"))]
    {
        eprintln!("[conformance/catalog_responses] skipped (build with --features real-postgres)");
    }
}

#[cfg(feature = "real-postgres")]
mod with_pg {
    use std::{fs, path::PathBuf};

    use palimpsest_conformance::harness::connect_or_skip;
    use serde_json::{json, Value};

    /// Each entry: a query name (also the fixture filename stem) and
    /// the SQL we run. We pick deterministic, widely-supported catalog
    /// projections — anything depending on local OIDs/CTIDs is masked
    /// in `normalize`.
    fn corpus() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                "pg_type_core",
                "SELECT typname, typtype, typcategory \
                   FROM pg_catalog.pg_type \
                  WHERE typname IN ('int8','int4','text','bool','timestamptz') \
                  ORDER BY typname",
            ),
            (
                "pg_settings_replication",
                "SELECT name, setting \
                   FROM pg_catalog.pg_settings \
                  WHERE name IN ('wal_level','max_replication_slots','max_wal_senders') \
                  ORDER BY name",
            ),
        ]
    }

    fn fixtures_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
    }

    fn normalize(name: &str, value: &Value) -> Value {
        // `setting` for max_wal_senders/max_replication_slots varies
        // by deployment; mask numeric integer settings to a sentinel.
        if name == "pg_settings_replication" {
            if let Some(arr) = value.as_array() {
                return Value::Array(
                    arr.iter()
                        .map(|row| {
                            let mut obj = row.as_object().cloned().unwrap_or_default();
                            if let Some(name) = obj.get("name").and_then(Value::as_str) {
                                if matches!(name, "max_replication_slots" | "max_wal_senders") {
                                    obj.insert("setting".into(), json!("<int>"));
                                }
                            }
                            Value::Object(obj)
                        })
                        .collect(),
                );
            }
        }
        value.clone()
    }

    #[tokio::test]
    async fn catalog_responses_match_fixtures() {
        let Some(conn) = connect_or_skip("catalog_responses").await else {
            return;
        };
        let dir = fixtures_dir();
        for (name, sql) in corpus() {
            let rows = conn.client.query(sql, &[]).await.expect("query");
            let captured: Vec<Value> = rows
                .iter()
                .map(|row| {
                    let mut obj = serde_json::Map::new();
                    for (i, column) in row.columns().iter().enumerate() {
                        // We only project text-friendly columns;
                        // every column above is `name`/`text`-typed.
                        let v: Option<&str> = row.get(i);
                        obj.insert(column.name().to_owned(), json!(v));
                    }
                    Value::Object(obj)
                })
                .collect();
            let captured = normalize(name, &Value::Array(captured));

            let path = dir.join(format!("{name}.json"));

            // Regenerate mode: write the captured value as the new
            // fixture. Used by `cargo run -p xtask -- regen-pg-fixtures`
            // (sets PALIMPSEST_CONFORMANCE_REGEN=1).
            if std::env::var("PALIMPSEST_CONFORMANCE_REGEN").is_ok() {
                fs::write(
                    &path,
                    serde_json::to_string_pretty(&captured).expect("serialize fixture") + "\n",
                )
                .expect("write fixture");
                eprintln!("regenerated {}", path.display());
                continue;
            }

            let fixture: Value = if path.exists() {
                serde_json::from_str(&fs::read_to_string(&path).expect("read fixture"))
                    .expect("parse fixture")
            } else {
                panic!(
                    "missing fixture {}; regenerate with `cargo run -p xtask -- regen-pg-fixtures`",
                    path.display()
                );
            };
            let fixture = normalize(name, &fixture);
            assert_eq!(
                captured,
                fixture,
                "catalog response {name} drifted from {}",
                path.display()
            );
        }
    }
}
