// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `wasm-bindgen` surface for the local-first replica.
//!
//! The browser hands over a **database driver** — any object exposing
//! `exec(sql, params) => Promise<{ rows: any[][] }>`, typically a thin
//! adapter over a pgrust/pglite-style Postgres-in-WASM instance — plus
//! an optional **writer** callback for optimistic mutations, and the
//! replica keeps the driver's tables in sync with the server's
//! permissioned subset:
//!
//! ```js
//! const replica = await client.localReplica({
//!   database: { exec: (sql, params) => pg.query(sql, params, { rowMode: "array" }) },
//!   writer: (req) => api.applyMutation(req),
//!   mirrors: ["posts", { name: "my_named_query" }, { sql: "SELECT ...", as: "hot_posts" }],
//! });
//! const { rows } = await replica.query("SELECT * FROM posts WHERE author = $1", [me]);
//! const token = await replica.mutate({ table: "posts", key: { id: 1 }, set: { title: "hi" } });
//! replica.onEvent((e) => { if (e.kind === "applied") refresh(); });
//! ```

#![allow(
    clippy::future_not_send,
    // JsValue trees again — same rationale as `bindings.rs`.
    clippy::significant_drop_tightening,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

use std::collections::HashMap;
use std::sync::Arc;

use js_sys::{Array, BigInt, Function, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{spawn_local, JsFuture};

use palimpsest_client::local::{
    DbFuture, LocalDbError, LocalReplica as RustLocalReplica, MutateError, Mutation, RemoteWriter,
    ReplicaEvent, SqlExecutor, SqlLocalDatabase, TableSpec, TableSyncState, WriteRequest,
};
use palimpsest_client::{DatumType, Schema, WireDatum, WireRow};

use crate::bindings::{datum_to_js, set, Client};

fn js_to_string(value: &JsValue) -> String {
    value
        .as_string()
        .or_else(|| {
            js_sys::Reflect::get(value, &JsValue::from_str("message"))
                .ok()
                .and_then(|m| m.as_string())
        })
        .unwrap_or_else(|| format!("{value:?}"))
}

fn exec_error(value: &JsValue) -> LocalDbError {
    LocalDbError::Execution(js_to_string(value))
}

/// Bridge from the JS driver object to the Rust [`SqlExecutor`] trait.
struct JsSqlExecutor {
    driver: JsValue,
    exec: Function,
}

impl JsSqlExecutor {
    fn new(driver: JsValue) -> Result<Self, JsValue> {
        let exec = Reflect::get(&driver, &JsValue::from_str("exec"))?
            .dyn_into::<Function>()
            .map_err(|_| {
                JsValue::from(JsError::new(
                    "database driver must expose exec(sql, params) => Promise<{rows}>",
                ))
            })?;
        Ok(Self { driver, exec })
    }
}

impl SqlExecutor for JsSqlExecutor {
    fn exec(
        &self,
        sql: String,
        params: Vec<WireDatum>,
        expect: Option<Schema>,
    ) -> DbFuture<'_, Result<Vec<WireRow>, LocalDbError>> {
        let driver = self.driver.clone();
        let exec = self.exec.clone();
        Box::pin(async move {
            let params_js = Array::new();
            for datum in &params {
                params_js.push(&datum_to_js(datum));
            }
            let ret = exec
                .call2(&driver, &JsValue::from_str(&sql), &params_js)
                .map_err(|e| exec_error(&e))?;
            let resolved = match ret.dyn_into::<Promise>() {
                Ok(promise) => JsFuture::from(promise).await.map_err(|e| exec_error(&e))?,
                Err(value) => value,
            };
            rows_from_result(&resolved, expect.as_ref())
        })
    }
}

/// Pull `rows` out of a driver result (`{rows}` object, or a bare
/// array) and convert each cell to a wire datum.
fn rows_from_result(
    result: &JsValue,
    expect: Option<&Schema>,
) -> Result<Vec<WireRow>, LocalDbError> {
    let rows_js = if Array::is_array(result) {
        result.clone()
    } else if result.is_object() {
        Reflect::get(result, &JsValue::from_str("rows")).map_err(|e| exec_error(&e))?
    } else {
        JsValue::UNDEFINED
    };
    if rows_js.is_undefined() || rows_js.is_null() {
        return Ok(Vec::new());
    }
    let rows: Array = rows_js
        .dyn_into()
        .map_err(|_| LocalDbError::Corrupt("driver result `rows` is not an array".to_owned()))?;
    let mut out = Vec::with_capacity(rows.length() as usize);
    for row in rows.iter() {
        let row: Array = row.dyn_into().map_err(|_| {
            LocalDbError::Corrupt(
                "driver must return array rows (e.g. rowMode: \"array\")".to_owned(),
            )
        })?;
        let mut wire_row = Vec::with_capacity(row.length() as usize);
        for (idx, cell) in row.iter().enumerate() {
            let datum_type = expect.and_then(|schema| {
                schema
                    .columns
                    .get(idx)
                    .and_then(|c| DatumType::try_from(c.r#type).ok())
            });
            wire_row.push(
                js_to_datum(&cell, datum_type)
                    .map_err(|err| LocalDbError::Corrupt(format!("column {idx}: {err}")))?,
            );
        }
        out.push(wire_row);
    }
    Ok(out)
}

/// Convert a JS value to a wire datum, guided by the column's declared
/// type when known and by the JS type otherwise.
fn js_to_datum(value: &JsValue, datum_type: Option<DatumType>) -> Result<WireDatum, String> {
    if value.is_null() || value.is_undefined() {
        return Ok(WireDatum::Null);
    }
    let type_err = |expected: &str| format!("expected {expected}, got {value:?}");
    match datum_type {
        Some(DatumType::Bool) => value
            .as_bool()
            .map(WireDatum::Bool)
            .ok_or_else(|| type_err("boolean")),
        Some(DatumType::I16) => js_to_i64(value).map(|n| WireDatum::I16(n as i16)),
        Some(DatumType::I32) => js_to_i64(value).map(|n| WireDatum::I32(n as i32)),
        Some(DatumType::I64) => js_to_i64(value).map(WireDatum::I64),
        Some(DatumType::F32) => value
            .as_f64()
            .map(|f| WireDatum::F32((f as f32).to_bits()))
            .ok_or_else(|| type_err("number")),
        Some(DatumType::F64) => value
            .as_f64()
            .map(|f| WireDatum::F64(f.to_bits()))
            .ok_or_else(|| type_err("number")),
        Some(DatumType::Numeric) => value
            .as_string()
            .or_else(|| value.as_f64().map(|f| f.to_string()))
            .map(WireDatum::Numeric)
            .ok_or_else(|| type_err("string or number")),
        Some(DatumType::Text) => value
            .as_string()
            .map(|s| WireDatum::Text(s.into_bytes()))
            .ok_or_else(|| type_err("string")),
        Some(DatumType::Bytea) => bytes_from_js(value).map(WireDatum::Bytea),
        Some(DatumType::Uuid) => value
            .as_string()
            .ok_or_else(|| type_err("uuid string"))
            .and_then(|s| parse_uuid(&s)),
        Some(DatumType::Date) => {
            js_to_millis(value).map(|ms| WireDatum::Date((ms / 86_400_000.0) as i32))
        }
        Some(DatumType::Time) => value
            .as_f64()
            .map(|micros| WireDatum::Time(micros as i64))
            .ok_or_else(|| type_err("microseconds number")),
        Some(DatumType::Timestamp) => {
            js_to_millis(value).map(|ms| WireDatum::Timestamp((ms * 1000.0) as i64))
        }
        Some(DatumType::TimestampTz) => {
            js_to_millis(value).map(|ms| WireDatum::TimestampTz((ms * 1000.0) as i64))
        }
        Some(DatumType::Interval) => {
            let get = |key: &str| {
                Reflect::get(value, &JsValue::from_str(key))
                    .ok()
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0)
            };
            let micros = Reflect::get(value, &JsValue::from_str("micros"))
                .ok()
                .map_or(Ok(0), |v| {
                    if v.is_undefined() {
                        Ok(0)
                    } else {
                        js_to_i64(&v)
                    }
                })?;
            Ok(WireDatum::Interval {
                months: get("months") as i32,
                days: get("days") as i32,
                micros,
            })
        }
        Some(DatumType::Json | DatumType::Jsonb) => {
            let text = value.as_string().map_or_else(
                || {
                    js_sys::JSON::stringify(value)
                        .map(String::from)
                        .map_err(|e| js_to_string(&e))
                },
                Ok,
            )?;
            let bytes = text.into_bytes();
            Ok(match datum_type {
                Some(DatumType::Jsonb) => WireDatum::Jsonb(bytes),
                _ => WireDatum::Json(bytes),
            })
        }
        Some(DatumType::Array) => {
            let array: Array = value.clone().dyn_into().map_err(|_| type_err("array"))?;
            let mut elements = Vec::with_capacity(array.length() as usize);
            for element in array.iter() {
                elements.push(js_to_datum(&element, None)?);
            }
            Ok(WireDatum::Array(elements))
        }
        Some(DatumType::Null | DatumType::Unspecified) | None => js_to_datum_untyped(value),
    }
}

/// Best-effort conversion when no column type is known.
fn js_to_datum_untyped(value: &JsValue) -> Result<WireDatum, String> {
    if let Some(flag) = value.as_bool() {
        return Ok(WireDatum::Bool(flag));
    }
    if let Some(number) = value.as_f64() {
        if number.fract() == 0.0 && number.abs() < 9_007_199_254_740_992.0 {
            return Ok(WireDatum::I64(number as i64));
        }
        return Ok(WireDatum::F64(number.to_bits()));
    }
    if value.dyn_ref::<BigInt>().is_some() {
        return js_to_i64(value).map(WireDatum::I64);
    }
    if let Some(text) = value.as_string() {
        return Ok(WireDatum::Text(text.into_bytes()));
    }
    if value.dyn_ref::<Uint8Array>().is_some() {
        return bytes_from_js(value).map(WireDatum::Bytea);
    }
    if let Some(date) = value.dyn_ref::<js_sys::Date>() {
        return Ok(WireDatum::TimestampTz((date.get_time() * 1000.0) as i64));
    }
    if Array::is_array(value) {
        let array: Array = value.clone().unchecked_into();
        let mut elements = Vec::with_capacity(array.length() as usize);
        for element in array.iter() {
            elements.push(js_to_datum_untyped(&element)?);
        }
        return Ok(WireDatum::Array(elements));
    }
    if value.is_object() {
        let text = js_sys::JSON::stringify(value)
            .map(String::from)
            .map_err(|e| js_to_string(&e))?;
        return Ok(WireDatum::Json(text.into_bytes()));
    }
    Err(format!("unsupported JS value: {value:?}"))
}

fn js_to_i64(value: &JsValue) -> Result<i64, String> {
    if let Some(number) = value.as_f64() {
        return Ok(number as i64);
    }
    if let Some(bigint) = value.dyn_ref::<BigInt>() {
        return i64::try_from(bigint.clone()).map_err(|_| "bigint out of i64 range".to_owned());
    }
    if let Some(text) = value.as_string() {
        return text.parse().map_err(|_| format!("not an integer: {text}"));
    }
    Err(format!("expected integer, got {value:?}"))
}

fn js_to_millis(value: &JsValue) -> Result<f64, String> {
    if let Some(date) = value.dyn_ref::<js_sys::Date>() {
        return Ok(date.get_time());
    }
    value
        .as_f64()
        .ok_or_else(|| format!("expected Date or epoch-millis number, got {value:?}"))
}

fn bytes_from_js(value: &JsValue) -> Result<Vec<u8>, String> {
    let array: Uint8Array = value
        .clone()
        .dyn_into()
        .map_err(|_| format!("expected Uint8Array, got {value:?}"))?;
    Ok(array.to_vec())
}

fn parse_uuid(text: &str) -> Result<WireDatum, String> {
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(format!("invalid uuid: {text}"));
    }
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| format!("invalid uuid: {text}"))?;
    }
    Ok(WireDatum::Uuid(bytes))
}

/// Writer bridge: forwards each optimistic mutation to a JS callback
/// returning a `Promise<void>` (reject to roll the mutation back).
struct JsRemoteWriter {
    func: Function,
}

impl RemoteWriter for JsRemoteWriter {
    fn write(&self, request: WriteRequest) -> DbFuture<'_, Result<(), String>> {
        let func = self.func.clone();
        Box::pin(async move {
            let payload = write_request_to_js(&request);
            let ret = func
                .call1(&JsValue::NULL, &payload)
                .map_err(|e| js_to_string(&e))?;
            if let Ok(promise) = ret.dyn_into::<Promise>() {
                JsFuture::from(promise)
                    .await
                    .map_err(|e| js_to_string(&e))?;
            }
            Ok(())
        })
    }
}

fn named_values_to_js(values: &[(String, WireDatum)]) -> JsValue {
    let obj = Object::new();
    for (name, value) in values {
        set(&obj, name, &datum_to_js(value));
    }
    obj.into()
}

fn write_request_to_js(request: &WriteRequest) -> JsValue {
    let obj = Object::new();
    set(&obj, "token", &JsValue::from_f64(request.token as f64));
    set(&obj, "table", &JsValue::from_str(request.mutation.table()));
    match &request.mutation {
        Mutation::Insert { values, .. } => {
            set(&obj, "kind", &JsValue::from_str("insert"));
            set(&obj, "values", &named_values_to_js(values));
        }
        Mutation::Update {
            key, set: updates, ..
        } => {
            set(&obj, "kind", &JsValue::from_str("update"));
            set(&obj, "key", &named_values_to_js(key));
            set(&obj, "set", &named_values_to_js(updates));
        }
        Mutation::Delete { key, .. } => {
            set(&obj, "kind", &JsValue::from_str("delete"));
            set(&obj, "key", &named_values_to_js(key));
        }
    }
    obj.into()
}

#[wasm_bindgen]
impl Client {
    /// Start a local-first replica.
    ///
    /// `options`:
    /// * `database` (required) — driver object with
    ///   `exec(sql, params) => Promise<{ rows: any[][] }>` over a
    ///   Postgres-compatible WASM engine (rows in **array** mode).
    /// * `mirrors` (required) — array of mirror specs: a string table
    ///   name, `{ table }`, `{ sql, as }`, or
    ///   `{ name, params?, as? }` for named prepared queries.
    /// * `writer` (optional) — `async (request) => void`; receives
    ///   `{token, table, kind, values?/key?/set?}` for each optimistic
    ///   mutation, and should perform the write through the app's own
    ///   API. Throw/reject to roll the local change back.
    #[wasm_bindgen(js_name = localReplica)]
    pub async fn local_replica(&self, options: JsValue) -> Result<LocalReplica, JsValue> {
        let database = Reflect::get(&options, &JsValue::from_str("database"))?;
        if database.is_undefined() || database.is_null() {
            return Err(JsError::new("localReplica: `database` is required").into());
        }
        let executor = JsSqlExecutor::new(database.clone())?;
        let db = Arc::new(SqlLocalDatabase::new(executor));
        let mut builder = self.rust().local_replica(db);

        let writer = Reflect::get(&options, &JsValue::from_str("writer"))?;
        if let Ok(func) = writer.dyn_into::<Function>() {
            builder = builder.with_writer(Arc::new(JsRemoteWriter { func }));
        }

        let mirrors = Reflect::get(&options, &JsValue::from_str("mirrors"))?;
        let mirrors: Array = mirrors
            .dyn_into()
            .map_err(|_| JsValue::from(JsError::new("localReplica: `mirrors` must be an array")))?;
        if mirrors.length() == 0 {
            return Err(JsError::new("localReplica: `mirrors` must not be empty").into());
        }
        for mirror in mirrors.iter() {
            builder = add_mirror(builder, &mirror)?;
        }

        let inner = builder
            .start()
            .await
            .map_err(|err| JsValue::from(JsError::new(&err.to_string())))?;
        Ok(LocalReplica { inner, database })
    }
}

fn add_mirror(
    builder: palimpsest_client::local::LocalReplicaBuilder,
    mirror: &JsValue,
) -> Result<palimpsest_client::local::LocalReplicaBuilder, JsValue> {
    if let Some(table) = mirror.as_string() {
        return Ok(builder.mirror_table(table));
    }
    let get = |key: &str| -> Option<String> {
        Reflect::get(mirror, &JsValue::from_str(key))
            .ok()
            .and_then(|v| v.as_string())
    };
    if let Some(sql) = get("sql") {
        let Some(local) = get("as") else {
            return Err(
                JsError::new("localReplica: sql mirrors need `as` (the local table name)").into(),
            );
        };
        return Ok(builder.mirror_sql(local, sql));
    }
    if let Some(name) = get("name") {
        let params_js =
            Reflect::get(mirror, &JsValue::from_str("params")).unwrap_or(JsValue::UNDEFINED);
        let mut params = HashMap::new();
        if params_js.is_object() {
            let entries = Object::entries(&params_js.unchecked_into::<Object>());
            for entry in entries.iter() {
                let pair: Array = entry.unchecked_into();
                let key = pair.get(0).as_string().unwrap_or_default();
                let value = crate::bindings::param_to_var(&pair.get(1), &key, true)?;
                params.insert(key, value);
            }
        }
        return Ok(builder.mirror_named_with(name, params));
    }
    if let Some(table) = get("table") {
        return Ok(builder.mirror_table(table));
    }
    Err(JsError::new(
        "localReplica: each mirror must be a table name string, {table}, {sql, as}, or {name, params?}",
    )
    .into())
}

/// Browser handle to a running local-first replica.
#[wasm_bindgen]
pub struct LocalReplica {
    inner: RustLocalReplica,
    /// The JS driver object, kept for zero-copy query passthrough.
    database: JsValue,
}

#[wasm_bindgen]
impl LocalReplica {
    /// Run a read-only SQL query against the local database (the same
    /// Postgres-dialect SQL the server accepts). Sugar for calling the
    /// driver's `exec` directly — results come back exactly as the
    /// driver produced them.
    #[wasm_bindgen]
    pub async fn query(&self, sql: String, params: JsValue) -> Result<JsValue, JsValue> {
        let exec: Function = Reflect::get(&self.database, &JsValue::from_str("exec"))?
            .dyn_into()
            .map_err(|_| JsValue::from(JsError::new("database driver lost its exec method")))?;
        let params = if params.is_undefined() || params.is_null() {
            Array::new().into()
        } else {
            params
        };
        let ret = exec.call2(&self.database, &JsValue::from_str(&sql), &params)?;
        match ret.dyn_into::<Promise>() {
            Ok(promise) => JsFuture::from(promise).await,
            Err(value) => Ok(value),
        }
    }

    /// Apply an optimistic mutation. Accepts one of:
    /// * `{ table, insert: {col: value, ...} }`
    /// * `{ table, key: {pk: value}, set: {col: value, ...} }`
    /// * `{ table, key: {pk: value}, delete: true }`
    ///
    /// Resolves to the mutation token used in `onEvent` notifications.
    #[wasm_bindgen]
    pub async fn mutate(&self, mutation: JsValue) -> Result<f64, JsValue> {
        let table = Reflect::get(&mutation, &JsValue::from_str("table"))?
            .as_string()
            .ok_or_else(|| JsValue::from(JsError::new("mutate: `table` is required")))?;
        let spec = self.inner.table_spec(&table).await.ok_or_else(|| {
            JsValue::from(JsError::new(&format!(
                "mutate: table `{table}` is not mirrored or not yet synced"
            )))
        })?;

        let insert = Reflect::get(&mutation, &JsValue::from_str("insert"))?;
        let key = Reflect::get(&mutation, &JsValue::from_str("key"))?;
        let set_js = Reflect::get(&mutation, &JsValue::from_str("set"))?;
        let delete = Reflect::get(&mutation, &JsValue::from_str("delete"))?;

        let parsed = if insert.is_object() {
            Mutation::Insert {
                table,
                values: named_values_from_js(&insert, &spec)?,
            }
        } else if key.is_object() && set_js.is_object() {
            Mutation::Update {
                table,
                key: named_values_from_js(&key, &spec)?,
                set: named_values_from_js(&set_js, &spec)?,
            }
        } else if key.is_object() && delete.as_bool() == Some(true) {
            Mutation::Delete {
                table,
                key: named_values_from_js(&key, &spec)?,
            }
        } else {
            return Err(JsError::new(
                "mutate: expected {table, insert}, {table, key, set}, or {table, key, delete: true}",
            )
            .into());
        };

        let token = self
            .inner
            .mutate(parsed)
            .await
            .map_err(|err: MutateError| JsValue::from(JsError::new(&err.to_string())))?;
        Ok(token as f64)
    }

    /// Register a callback for replica events (`tableState`, `applied`,
    /// `mutationSettled`, `mutationConflicted`, `mutationFailed`,
    /// `mirrorError`). Only the first registration receives events.
    #[wasm_bindgen(js_name = onEvent)]
    pub fn on_event(&self, callback: Function) {
        let Some(mut events) = self.inner.take_events() else {
            return;
        };
        spawn_local(async move {
            while let Some(event) = events.recv().await {
                let _ = callback.call1(&JsValue::NULL, &replica_event_to_js(&event));
            }
        });
    }

    /// Current sync state per mirrored table:
    /// `{ [table]: { kind: "connecting" | "snapshotting" | "live" | "resyncing" | "errored", lsn?, message? } }`.
    #[wasm_bindgen(js_name = tableStates)]
    #[must_use]
    pub fn table_states(&self) -> JsValue {
        let obj = Object::new();
        for (table, state) in self.inner.table_states() {
            set(&obj, &table, &sync_state_to_js(&state));
        }
        obj.into()
    }

    /// Number of optimistic mutations not yet confirmed for `table`.
    #[wasm_bindgen(js_name = pendingMutations)]
    pub async fn pending_mutations(&self, table: String) -> f64 {
        self.inner.pending_mutations(&table).await as f64
    }

    /// Stop the replica's mirror subscriptions. The underlying client
    /// connection stays open.
    #[wasm_bindgen]
    pub async fn stop(&self) {
        self.inner.clone().stop().await;
    }
}

/// Convert a `{col: value}` object to named wire datums using the
/// mirror's schema for typing.
fn named_values_from_js(
    values: &JsValue,
    spec: &TableSpec,
) -> Result<Vec<(String, WireDatum)>, JsValue> {
    let entries = Object::entries(&values.clone().unchecked_into::<Object>());
    let mut out = Vec::with_capacity(entries.length() as usize);
    for entry in entries.iter() {
        let pair: Array = entry.unchecked_into();
        let name = pair.get(0).as_string().unwrap_or_default();
        let datum_type = spec
            .column_index(&name)
            .and_then(|idx| spec.schema.columns.get(idx))
            .and_then(|c| DatumType::try_from(c.r#type).ok());
        let datum = js_to_datum(&pair.get(1), datum_type)
            .map_err(|err| JsValue::from(JsError::new(&format!("column `{name}`: {err}"))))?;
        out.push((name, datum));
    }
    Ok(out)
}

fn sync_state_to_js(state: &TableSyncState) -> JsValue {
    let obj = Object::new();
    match state {
        TableSyncState::Connecting => set(&obj, "kind", &JsValue::from_str("connecting")),
        TableSyncState::Snapshotting => set(&obj, "kind", &JsValue::from_str("snapshotting")),
        TableSyncState::Live { lsn } => {
            set(&obj, "kind", &JsValue::from_str("live"));
            set(&obj, "lsn", &BigInt::from(*lsn).into());
        }
        TableSyncState::Resyncing => set(&obj, "kind", &JsValue::from_str("resyncing")),
        TableSyncState::Errored { message } => {
            set(&obj, "kind", &JsValue::from_str("errored"));
            set(&obj, "message", &JsValue::from_str(message));
        }
    }
    obj.into()
}

fn replica_event_to_js(event: &ReplicaEvent) -> JsValue {
    let obj = Object::new();
    match event {
        ReplicaEvent::TableState { table, state } => {
            set(&obj, "kind", &JsValue::from_str("tableState"));
            set(&obj, "table", &JsValue::from_str(table));
            set(&obj, "state", &sync_state_to_js(state));
        }
        ReplicaEvent::Applied { table, lsn } => {
            set(&obj, "kind", &JsValue::from_str("applied"));
            set(&obj, "table", &JsValue::from_str(table));
            set(&obj, "lsn", &BigInt::from(*lsn).into());
        }
        ReplicaEvent::MutationSettled { token, table } => {
            set(&obj, "kind", &JsValue::from_str("mutationSettled"));
            set(&obj, "token", &JsValue::from_f64(*token as f64));
            set(&obj, "table", &JsValue::from_str(table));
        }
        ReplicaEvent::MutationConflicted { token, table } => {
            set(&obj, "kind", &JsValue::from_str("mutationConflicted"));
            set(&obj, "token", &JsValue::from_f64(*token as f64));
            set(&obj, "table", &JsValue::from_str(table));
        }
        ReplicaEvent::MutationFailed {
            token,
            table,
            error,
        } => {
            set(&obj, "kind", &JsValue::from_str("mutationFailed"));
            set(&obj, "token", &JsValue::from_f64(*token as f64));
            set(&obj, "table", &JsValue::from_str(table));
            set(&obj, "error", &JsValue::from_str(error));
        }
        ReplicaEvent::MirrorError {
            table,
            code,
            message,
        } => {
            set(&obj, "kind", &JsValue::from_str("mirrorError"));
            set(&obj, "table", &JsValue::from_str(table));
            set(&obj, "code", &JsValue::from_str(code));
            set(&obj, "message", &JsValue::from_str(message));
        }
    }
    obj.into()
}
