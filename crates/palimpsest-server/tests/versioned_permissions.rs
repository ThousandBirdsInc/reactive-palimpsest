// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Current-version selection must precede project authorization, including WAL updates.
use std::collections::HashMap;

use palimpsest_dataflow::palimpsest::{
    compile_mir, snapshot_run, Lsn, PersistentHost, Row, ScalarSchema,
};
use palimpsest_permissions::{
    compile_rules, rewrite, LatestBy, PermissionRule, UserContext, UserContextSchema, UserValue,
};
use palimpsest_sql::{lower::parse_and_lower, Catalog, ColumnSchema, ColumnType, TableSchema};
use palimpsest_wal::{Datum, TableId};
use smallvec::smallvec;

const TABLE: TableId = TableId::new(1);
fn columns() -> Vec<ColumnSchema> {
    vec![
        ColumnSchema::new("id", ColumnType::Int),
        ColumnSchema::new("version", ColumnType::Int),
        ColumnSchema::new("project", ColumnType::Jsonb),
    ]
}
fn result_row(id: i64, version: i64) -> Row {
    smallvec![Datum::I64(id), Datum::I64(version)]
}
fn row(id: i64, version: i64, project: &str) -> Row {
    smallvec![
        Datum::I64(id),
        Datum::I64(version),
        Datum::Jsonb(
            serde_json::json!({"id": project})
                .to_string()
                .into_bytes()
                .into()
        )
    ]
}
fn plan(sql: &str, admin: bool) -> palimpsest_dataflow::palimpsest::CompiledPlan {
    let catalog = Catalog::new([TableSchema::new("records", columns())]);
    let schema = UserContextSchema::new([
        ("project_ids".to_owned(), ColumnType::Text),
        ("admin".to_owned(), ColumnType::Bool),
    ]);
    let rule = PermissionRule::new(
        "project_access",
        "records",
        "$user.admin OR project->>'id' = ANY($user.project_ids)",
    )
    .with_latest(LatestBy {
        key: vec!["id".to_owned()],
        version: "version".to_owned(),
    });
    let rules = compile_rules(&[rule], &catalog, &schema).unwrap();
    let user = UserContext::new([
        (
            "project_ids".to_owned(),
            UserValue::List(vec![UserValue::Text("a".to_owned())]),
        ),
        ("admin".to_owned(), UserValue::Bool(admin)),
    ]);
    let graph = rewrite(&parse_and_lower(sql).unwrap(), &rules, &user)
        .unwrap()
        .graph;
    compile_mir(&graph, &|name: &str| {
        (name == "records").then(|| {
            (
                TABLE,
                ScalarSchema::from_pairs(columns().into_iter().map(|c| (c.name, c.ty))),
            )
        })
    })
    .unwrap()
}

#[test]
fn latest_is_selected_before_permissions_and_query_filters_on_every_scan() {
    let rows = vec![row(1, 1, "a"), row(1, 2, "b"), row(2, 1, "a")];
    for sql in [
        "SELECT id, version FROM records WHERE project->>'id' = 'a'",
        "SELECT r.id, r.version FROM records r JOIN records s ON r.id = s.id WHERE s.project->>'id' = 'a'",
        "SELECT id, version FROM (SELECT * FROM records) r WHERE project->>'id' = 'a'",
    ] {
        let result = snapshot_run(&plan(sql, false), HashMap::from([(TABLE, rows.clone())]));
        assert_eq!(result, vec![result_row(2, 1)], "{sql}");
    }
    let result = snapshot_run(
        &plan("SELECT id, version FROM records", true),
        HashMap::from([(TABLE, rows)]),
    );
    assert_eq!(result.len(), 2); // Admins also see only the current relation.
    assert!(result.contains(&result_row(1, 2)));
}

#[test]
fn project_move_retracts_previous_visible_row_and_never_resurrects_history() {
    let plan = plan("SELECT id, version FROM records", false);
    let host = PersistentHost::new();
    let initial = host.register_or_seed(
        "current",
        &plan,
        HashMap::from([(TABLE, vec![row(1, 1, "a")])]),
        Lsn::new(1),
        1,
    );
    assert_eq!(initial, vec![result_row(1, 1)]);
    let deltas = host.push_table_diff("current", TABLE, row(1, 2, "b"), 1, Lsn::new(2));
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].diff, -1);
    assert_eq!(deltas[0].row, initial[0]);
    assert!(host.cached_view("current", 2).unwrap().0.is_empty());
    assert!(host
        .push_table_diff("current", TABLE, row(1, 3, "b"), 1, Lsn::new(3))
        .is_empty());
    let deltas = host.push_table_diff("current", TABLE, row(1, 4, "a"), 1, Lsn::new(4));
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].diff, 1);
    assert_eq!(deltas[0].row, result_row(1, 4));
    host.release("current", 1);
    host.release("current", 2);
}
