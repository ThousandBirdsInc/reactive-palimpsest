// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `wasm-bindgen` exports — only compiled for `wasm32-unknown-unknown`.
//!
//! Surface area:
//! * [`Client::connect`] — async constructor returning a JS Promise.
//! * [`Client::subscribe`] — async, returns a [`Subscription`].
//! * [`Subscription::on_diff`] — register a JS callback.
//! * [`Subscription::update`] — push a new vars map.
//! * [`Subscription::ack`] — ack a server LSN.
//! * [`Subscription::unsubscribe`] — graceful close.
//!
//! Event shape (JS-side):
//! ```ts
//! type Event =
//!   | { kind: "accepted"; schemaId: number; snapshotLsn: bigint;
//!       schema: { columns: { name: string; type: number;
//!                            nullable: boolean }[];
//!                  primaryKeyColumns: number[] } }
//!   | { kind: "diff"; lsn: bigint; op: string; rows: any[][] }
//!   | { kind: "transaction"; commitLsn: bigint; changes:
//!       { op: string; old: any[] | null; new: any[] | null }[] }
//!   | { kind: "resync"; reason: string; message: string }
//!   | { kind: "error"; code: string; message: string };
//! ```

#![allow(
    // We hand-build JsValue trees and never hold MutexGuards across
    // long awaits, but each `lock().await` triggers this lint.
    clippy::significant_drop_tightening,
    // `*schema_id as f64` — schema IDs are bounded well below 2^53.
    clippy::cast_precision_loss,
    // `match str::from_utf8(...) { Ok(s) => ..., Err(_) => ... }` is
    // clearer than `map_or_else` here.
    clippy::option_if_let_else,
    // `js_err` takes `ClientError` by value because it's a one-shot
    // conversion at error-handling sites; the `Display` impl reads
    // the value, but clippy's heuristic doesn't see across the
    // `to_string()` call.
    clippy::needless_pass_by_value,
)]

use std::collections::HashMap;
use std::sync::Arc;

use js_sys::{Array, BigInt, Function, Object, Reflect, Uint8Array};
use palimpsest_client::{
    var_value, Auth, Client as RustClient, ClientError, ConnectionState, DatumType, DiffEvent,
    DiffOp, ResyncReason, Subscription as RustSubscription, VarList, VarValue, WireDatum,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

fn js_err(err: ClientError) -> JsValue {
    JsError::new(&err.to_string()).into()
}

fn to_string_or(value: &JsValue, default: &str) -> String {
    value.as_string().unwrap_or_else(|| default.to_owned())
}

fn auth_from_js(token: &JsValue) -> Auth {
    if token.is_undefined() || token.is_null() {
        Auth::Anonymous
    } else if let Some(s) = token.as_string() {
        Auth::bearer(s)
    } else {
        Auth::Anonymous
    }
}

/// Browser-facing client. Cheap to clone; backed by a single shared
/// connection manager.
#[wasm_bindgen]
pub struct Client {
    inner: RustClient,
}

#[wasm_bindgen]
impl Client {
    /// Connect to a Palimpsest gRPC-Web endpoint.
    ///
    /// `token` may be `null`/`undefined` for anonymous, or a string
    /// bearer token.
    #[wasm_bindgen]
    pub async fn connect(url: String, token: JsValue) -> Result<Self, JsValue> {
        let auth = auth_from_js(&token);
        let inner = RustClient::connect(url, auth).await.map_err(js_err)?;
        Ok(Self { inner })
    }

    /// Subscribe to a SQL query. Optional `vars` is a JS object whose
    /// values are interpreted as strings.
    #[wasm_bindgen]
    pub async fn subscribe(&self, sql: String, vars: JsValue) -> Result<Subscription, JsValue> {
        let vars = parse_vars(&vars)?;
        let sub = self.inner.subscribe_with(sql, vars).await.map_err(js_err)?;
        Ok(Subscription::wrap(sub))
    }

    /// Subscribe to a server-registered named prepared query. The
    /// browser never holds or sends SQL on this path.
    ///
    /// `params` is a plain object keyed by the registered parameter
    /// names (or `$N` positions). Values are converted by JS type:
    /// string → string, boolean → bool, integral number → int, other
    /// number → float, `null` → null, array of the above → list.
    #[wasm_bindgen(js_name = subscribeNamed)]
    pub async fn subscribe_named(
        &self,
        name: String,
        params: JsValue,
    ) -> Result<Subscription, JsValue> {
        let params = parse_params(&params)?;
        let sub = self
            .inner
            .subscribe_named_with(name, params)
            .await
            .map_err(js_err)?;
        Ok(Subscription::wrap(sub))
    }

    /// Tear the connection down.
    #[wasm_bindgen]
    pub async fn shutdown(self) {
        self.inner.shutdown().await;
    }

    /// Register a JS callback that fires on every connection-state
    /// transition. The callback receives a plain object matching the
    /// TypeScript `ConnectionStatus` discriminated union — see
    /// `palimpsest-client-typescript/src/types.ts`.
    ///
    /// Fires once immediately with the current state, then on every
    /// change until the `Client` is dropped or shutdown.
    #[wasm_bindgen(js_name = onConnectionStatus)]
    pub fn on_connection_status(&self, callback: Function) {
        let mut rx = self.inner.watch_connection_state();
        spawn_local(async move {
            // Fire once with the current value so callers don't need to
            // poll `borrow()` themselves.
            let _ = callback.call1(
                &JsValue::NULL,
                &connection_state_to_js(&rx.borrow().clone()),
            );
            while rx.changed().await.is_ok() {
                let snapshot = rx.borrow().clone();
                let _ = callback.call1(&JsValue::NULL, &connection_state_to_js(&snapshot));
                if matches!(snapshot, ConnectionState::Closed { .. }) {
                    return;
                }
            }
        });
    }
}

fn connection_state_to_js(state: &ConnectionState) -> JsValue {
    let obj = Object::new();
    match state {
        ConnectionState::Connecting => {
            set(&obj, "kind", &JsValue::from_str("connecting"));
        }
        ConnectionState::Connected => {
            set(&obj, "kind", &JsValue::from_str("connected"));
        }
        ConnectionState::Reconnecting { attempt, delay_ms } => {
            set(&obj, "kind", &JsValue::from_str("reconnecting"));
            set(&obj, "attempt", &JsValue::from_f64(f64::from(*attempt)));
            // delay_ms fits in f64 — values come from Duration::as_millis()
            // clamped to u64, never larger than the watch's max sleep
            // (5s default).
            set(&obj, "delayMs", &JsValue::from_f64(*delay_ms as f64));
        }
        ConnectionState::Closed { reason } => {
            set(&obj, "kind", &JsValue::from_str("closed"));
            set(&obj, "reason", &JsValue::from_str(reason));
        }
    }
    obj.into()
}

fn parse_vars(value: &JsValue) -> Result<HashMap<String, VarValue>, JsValue> {
    let mut out = HashMap::new();
    if value.is_undefined() || value.is_null() {
        return Ok(out);
    }
    let obj: Object = value
        .clone()
        .dyn_into()
        .map_err(|_| JsError::new("vars must be a plain object").into())
        .map_err(|err: JsValue| err)?;
    let entries = Object::entries(&obj);
    for entry in entries.iter() {
        let pair: Array = entry.dyn_into().map_err(|_| JsValue::from_str("entry"))?;
        let k = pair.get(0).as_string().unwrap_or_default();
        let v = pair.get(1);
        let var = VarValue {
            kind: Some(var_value::Kind::StringValue(to_string_or(&v, ""))),
        };
        out.insert(k, var);
    }
    Ok(out)
}

/// Typed conversion for named-query parameters. Unlike the legacy
/// `parse_vars` (everything stringified), this keeps JS types so the
/// server's bind step can type-check them.
fn parse_params(value: &JsValue) -> Result<HashMap<String, VarValue>, JsValue> {
    let mut out = HashMap::new();
    if value.is_undefined() || value.is_null() {
        return Ok(out);
    }
    let obj: Object = value
        .clone()
        .dyn_into()
        .map_err(|_| JsValue::from(JsError::new("params must be a plain object")))?;
    let entries = Object::entries(&obj);
    for entry in entries.iter() {
        let pair: Array = entry.dyn_into().map_err(|_| JsValue::from_str("entry"))?;
        let key = pair.get(0).as_string().unwrap_or_default();
        let var = param_to_var(&pair.get(1), &key, true)?;
        out.insert(key, var);
    }
    Ok(out)
}

fn param_to_var(value: &JsValue, key: &str, allow_list: bool) -> Result<VarValue, JsValue> {
    let kind = if value.is_null() || value.is_undefined() {
        var_value::Kind::NullValue(true)
    } else if let Some(text) = value.as_string() {
        var_value::Kind::StringValue(text)
    } else if let Some(flag) = value.as_bool() {
        var_value::Kind::BoolValue(flag)
    } else if let Some(number) = value.as_f64() {
        // Integral JS numbers travel as ints so int-typed parameters
        // accept them; everything else is a float.
        #[allow(clippy::cast_possible_truncation)]
        if number.fract() == 0.0 && number.abs() < 9_007_199_254_740_992.0 {
            var_value::Kind::IntValue(number as i64)
        } else {
            var_value::Kind::FloatValue(number)
        }
    } else if Array::is_array(value) {
        if !allow_list {
            return Err(
                JsError::new(&format!("param '{key}': nested arrays are not supported")).into(),
            );
        }
        let array: Array = value.clone().unchecked_into();
        let mut values = Vec::with_capacity(array.length() as usize);
        for element in array.iter() {
            values.push(param_to_var(&element, key, false)?);
        }
        var_value::Kind::ListValue(VarList { values })
    } else {
        return Err(JsError::new(&format!(
            "param '{key}': unsupported value type (expected string, number, boolean, null, or array)"
        ))
        .into());
    };
    Ok(VarValue { kind: Some(kind) })
}

/// One active subscription. Drop the value or call `unsubscribe()` to
/// stop receiving events.
#[wasm_bindgen]
pub struct Subscription {
    /// Wrapped in `Option`/`Arc` because `on_diff` consumes `self.inner`
    /// to spawn the forwarding loop, but the JS handle survives so the
    /// caller can still call `update`/`ack`/`unsubscribe`.
    inner: Arc<tokio::sync::Mutex<Option<RustSubscription>>>,
    handle: SubscriptionHandle,
}

#[derive(Clone)]
struct SubscriptionHandle {
    inner: Arc<tokio::sync::Mutex<Option<RustSubscription>>>,
}

impl Subscription {
    fn wrap(sub: RustSubscription) -> Self {
        let inner = Arc::new(tokio::sync::Mutex::new(Some(sub)));
        Self {
            inner: inner.clone(),
            handle: SubscriptionHandle { inner },
        }
    }
}

#[wasm_bindgen]
impl Subscription {
    /// Register a JS callback that fires for every event (accepted /
    /// diff / resync / error).
    ///
    /// The callback runs on the wasm-bindgen-futures executor — it
    /// must be sync and non-blocking. If the same subscription has
    /// `on_diff` called twice, only the most recent callback receives
    /// events (the prior one is silently dropped).
    #[wasm_bindgen(js_name = onDiff)]
    pub fn on_diff(&self, callback: Function) {
        let handle = self.handle.clone();
        spawn_local(async move {
            loop {
                let event = {
                    let mut guard = handle.inner.lock().await;
                    let Some(sub) = guard.as_mut() else { return };
                    sub.next_event().await
                };
                match event {
                    Some(Ok(event)) => {
                        let _ = callback.call1(&JsValue::NULL, &event_to_js(&event));
                    }
                    Some(Err(err)) => {
                        let payload = error_payload(&err.to_string());
                        let _ = callback.call1(&JsValue::NULL, &payload);
                    }
                    None => return,
                }
            }
        });
    }

    /// Push a new vars map. Triggers a server-side resubscribe.
    #[wasm_bindgen]
    pub async fn update(&self, vars: JsValue) -> Result<(), JsValue> {
        let vars = parse_vars(&vars)?;
        let mut guard = self.inner.lock().await;
        let Some(sub) = guard.as_mut() else {
            return Err(JsError::new("subscription already closed").into());
        };
        sub.update(vars).await.map_err(js_err)
    }

    /// Acknowledge a server LSN. Used by the manager as `resume_lsn`
    /// after reconnect.
    #[wasm_bindgen]
    pub async fn ack(&self, lsn: u64) -> Result<(), JsValue> {
        let mut guard = self.inner.lock().await;
        let Some(sub) = guard.as_mut() else {
            return Err(JsError::new("subscription already closed").into());
        };
        sub.ack(lsn).await.map_err(js_err)
    }

    /// Unsubscribe and drop server-side state.
    #[wasm_bindgen]
    pub async fn unsubscribe(&self) -> Result<(), JsValue> {
        let mut guard = self.inner.lock().await;
        let Some(sub) = guard.take() else {
            return Ok(());
        };
        sub.unsubscribe().await.map_err(js_err)
    }
}

fn event_to_js(event: &DiffEvent) -> JsValue {
    let obj = Object::new();
    match event {
        DiffEvent::Accepted {
            schema_id,
            snapshot_lsn,
            schema,
        } => {
            set(&obj, "kind", &JsValue::from_str("accepted"));
            set(&obj, "schemaId", &JsValue::from_f64(*schema_id as f64));
            set(&obj, "snapshotLsn", &BigInt::from(*snapshot_lsn).into());
            let schema_js = Object::new();
            let columns = Array::new();
            for col in &schema.columns {
                let c = Object::new();
                set(&c, "name", &JsValue::from_str(&col.name));
                set(&c, "type", &JsValue::from_f64(col.r#type.into()));
                set(
                    &c,
                    "typeName",
                    &JsValue::from_str(datum_type_name(col.r#type)),
                );
                set(&c, "nullable", &JsValue::from_bool(col.nullable));
                columns.push(&c);
            }
            set(&schema_js, "columns", &columns);
            let pks = Array::new();
            for c in &schema.primary_key_columns {
                pks.push(&JsValue::from_f64((*c).into()));
            }
            set(&schema_js, "primaryKeyColumns", &pks);
            set(&obj, "schema", &schema_js);
        }
        DiffEvent::Diff { lsn, op, rows } => {
            set(&obj, "kind", &JsValue::from_str("diff"));
            set(&obj, "lsn", &BigInt::from(*lsn).into());
            set(&obj, "op", &JsValue::from_str(diff_op_name(*op)));
            let rows_js = Array::new();
            for row in rows {
                let row_arr = Array::new();
                for d in row {
                    row_arr.push(&datum_to_js(d));
                }
                rows_js.push(&row_arr);
            }
            set(&obj, "rows", &rows_js);
        }
        DiffEvent::Transaction {
            commit_lsn,
            begin_lsn,
            end_lsn,
            transaction_id,
            changes,
        } => {
            set(&obj, "kind", &JsValue::from_str("transaction"));
            set(&obj, "commitLsn", &BigInt::from(*commit_lsn).into());
            if let Some(lsn) = begin_lsn {
                set(&obj, "beginLsn", &BigInt::from(*lsn).into());
            }
            if let Some(lsn) = end_lsn {
                set(&obj, "endLsn", &BigInt::from(*lsn).into());
            }
            if let Some(xid) = transaction_id {
                set(&obj, "transactionId", &JsValue::from_f64(f64::from(*xid)));
            }
            let changes_js = Array::new();
            for change in changes {
                let change_js = Object::new();
                set(
                    &change_js,
                    "op",
                    &JsValue::from_str(diff_op_name(change.op)),
                );
                set(
                    &change_js,
                    "old",
                    &change
                        .old
                        .as_ref()
                        .map_or(JsValue::NULL, |row| row_to_js(row)),
                );
                set(
                    &change_js,
                    "new",
                    &change
                        .new
                        .as_ref()
                        .map_or(JsValue::NULL, |row| row_to_js(row)),
                );
                changes_js.push(&change_js);
            }
            set(&obj, "changes", &changes_js);
        }
        DiffEvent::Resync { reason, message } => {
            set(&obj, "kind", &JsValue::from_str("resync"));
            set(
                &obj,
                "reason",
                &JsValue::from_str(resync_reason_name(*reason)),
            );
            set(&obj, "message", &JsValue::from_str(message));
        }
        DiffEvent::Error { code, message } => {
            set(&obj, "kind", &JsValue::from_str("error"));
            set(&obj, "code", &JsValue::from_str(code));
            set(&obj, "message", &JsValue::from_str(message));
        }
    }
    obj.into()
}

fn row_to_js(row: &[WireDatum]) -> JsValue {
    let row_arr = Array::new();
    for datum in row {
        row_arr.push(&datum_to_js(datum));
    }
    row_arr.into()
}

fn error_payload(message: &str) -> JsValue {
    let obj = Object::new();
    set(&obj, "kind", &JsValue::from_str("error"));
    set(&obj, "code", &JsValue::from_str("client"));
    set(&obj, "message", &JsValue::from_str(message));
    obj.into()
}

fn set(obj: &Object, key: &str, value: &JsValue) {
    let _ = Reflect::set(obj, &JsValue::from_str(key), value);
}

fn datum_to_js(d: &WireDatum) -> JsValue {
    match d {
        WireDatum::Bool(b) => JsValue::from_bool(*b),
        WireDatum::I16(n) => JsValue::from_f64(f64::from(*n)),
        WireDatum::I32(n) => JsValue::from_f64(f64::from(*n)),
        WireDatum::I64(n) => BigInt::from(*n).into(),
        WireDatum::F32(bits) => JsValue::from_f64(f64::from(f32::from_bits(*bits))),
        WireDatum::F64(bits) => JsValue::from_f64(f64::from_bits(*bits)),
        WireDatum::Numeric(s) => JsValue::from_str(s),
        WireDatum::Text(bytes) | WireDatum::Json(bytes) | WireDatum::Jsonb(bytes) => {
            match std::str::from_utf8(bytes) {
                Ok(s) => JsValue::from_str(s),
                Err(_) => bytea_to_js(bytes),
            }
        }
        WireDatum::Bytea(bytes) => bytea_to_js(bytes),
        WireDatum::Uuid(b) => JsValue::from_str(&format_uuid(b)),
        WireDatum::Null => JsValue::NULL,
    }
}

fn bytea_to_js(bytes: &[u8]) -> JsValue {
    let arr = Uint8Array::new_with_length(u32::try_from(bytes.len()).unwrap_or(u32::MAX));
    arr.copy_from(bytes);
    arr.into()
}

fn format_uuid(b: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3],
        b[4], b[5],
        b[6], b[7],
        b[8], b[9],
        b[10], b[11], b[12], b[13], b[14], b[15],
    )
}

fn datum_type_name(t: i32) -> &'static str {
    DatumType::try_from(t)
        .unwrap_or(DatumType::Unspecified)
        .as_str_name()
}

fn diff_op_name(op: DiffOp) -> &'static str {
    op.as_str_name()
}

fn resync_reason_name(r: ResyncReason) -> &'static str {
    r.as_str_name()
}
