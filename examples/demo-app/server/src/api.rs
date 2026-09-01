//! Axum HTTP routes for the tracker's write API.
//!
//! Writes go directly to Postgres via tokio-postgres. The
//! logical-replication consumer in `db.rs` picks the change up after
//! a ~100ms poll and propagates it into the in-memory mirror — which
//! is what subscribers + read endpoints read from.
//!
//! | Method | Path                | Body                                            |
//! | ------ | ------------------- | ----------------------------------------------- |
//! | GET    | `/api/health`       | -                                               |
//! | GET    | `/api/users`        | -                                               |
//! | GET    | `/api/token`        | `?user=<id>`                                    |
//! | GET    | `/api/issues`       | -                                               |
//! | POST   | `/api/issues`       | `{ "title": …, "id"?, "status"?, "priority"?, "assignee"?, "project"?, "estimate"? }` |
//! | PATCH  | `/api/issues/:id`   | any of `{ "title", "status", "priority", "assignee", "estimate" }` |
//! | DELETE | `/api/issues/:id`   | -                                               |
//! | POST   | `/api/simulate`     | `{ "events": … }` — synthetic team activity     |
//! | GET    | `/api/permissions`  | -                                               |
//! | PUT    | `/api/permissions`  | `{ "toml": "..." }`                             |

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;

use crate::auth::{lookup, mint_token, USERS};
use crate::db::{today_epoch_day, DemoRng, PROJECTS, STATUSES};
use crate::permissions::{PermissionsState, DEFAULT_PERMISSIONS_TOML};
use crate::state::{Issue, IssueStore};
use crate::ws::{ws_subscribe, WsState};

#[derive(Clone)]
pub struct AppState {
    /// Postgres write client — every mutation is a SQL statement
    /// against this. The replication consumer turns the resulting
    /// WAL records back into in-memory mirror updates.
    pub pg: Arc<Client>,
    /// Read-side mirror of `issues`. Read endpoints + Palimpsest
    /// snapshots both use this; it lags Postgres by the consumer
    /// poll interval (~100ms).
    pub store: Arc<IssueStore>,
    /// Permissions-DSL playground: applied TOML source + the handle
    /// that hot-swaps compiled rules onto the running `SyncEngine`.
    pub permissions: Arc<PermissionsState>,
    /// Deterministic PRNG driving the activity simulator.
    pub sim_rng: Arc<Mutex<DemoRng>>,
}

pub fn router(state: AppState, grpc_addr: SocketAddr) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api = Router::new()
        .route("/api/health", get(health))
        .route("/api/users", get(list_users))
        .route("/api/token", get(issue_token))
        .route("/api/issues", get(list_issues).post(create_issue))
        .route("/api/issues/:id", patch(update_issue).delete(delete_issue))
        .route("/api/simulate", axum::routing::post(simulate))
        .route(
            "/api/permissions",
            get(get_permissions).put(put_permissions),
        )
        .with_state(state);

    let ws = Router::new()
        .route("/ws/subscribe", get(ws_subscribe))
        .with_state(WsState { grpc_addr });

    api.merge(ws).layer(cors)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn list_users() -> Json<Value> {
    let users: Vec<Value> = USERS
        .iter()
        .map(|u| {
            json!({
                "id": u.id,
                "display_name": u.display_name,
                "is_admin": u.is_admin,
            })
        })
        .collect();
    Json(json!({ "users": users }))
}

#[derive(Deserialize)]
struct TokenQuery {
    user: String,
}

async fn issue_token(Query(q): Query<TokenQuery>) -> Result<Json<Value>, StatusCode> {
    let user = lookup(&q.user).ok_or(StatusCode::NOT_FOUND)?;
    let token = mint_token(user).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({
        "token": token,
        "user": {
            "id": user.id,
            "display_name": user.display_name,
            "is_admin": user.is_admin,
        },
    })))
}

async fn list_issues(State(state): State<AppState>) -> Json<Vec<Issue>> {
    Json(state.store.snapshot())
}

fn valid_status(status: &str) -> bool {
    STATUSES.contains(&status)
}

fn valid_assignee(assignee: &str) -> bool {
    assignee.is_empty() || lookup(assignee).is_some()
}

const ISSUE_RETURNING: &str = "RETURNING id, title, status, priority, assignee, project, \
     estimate, created_day, completed_day, cycle_days";

fn issue_from_pg_row(row: &tokio_postgres::Row) -> Issue {
    Issue {
        id: row.get::<_, i64>(0),
        title: row.get::<_, String>(1),
        status: row.get::<_, String>(2),
        priority: row.get::<_, i64>(3),
        assignee: row.get::<_, String>(4),
        project: row.get::<_, String>(5),
        estimate: row.get::<_, i64>(6),
        created_day: row.get::<_, i64>(7),
        completed_day: row.get::<_, i64>(8),
        cycle_days: row.get::<_, i64>(9),
    }
}

#[derive(Deserialize)]
struct CreateIssue {
    title: String,
    /// Optional client-chosen id. The local-first page assigns ids
    /// client-side so its optimistic insert and the WAL row share a
    /// primary key (that's how the replica settles the optimistic
    /// overlay). Clients use timestamp-scale ids far above the
    /// BIGSERIAL sequence, so the two ranges never collide.
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    assignee: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    estimate: Option<i64>,
}

async fn create_issue(
    State(state): State<AppState>,
    Json(body): Json<CreateIssue>,
) -> Result<(StatusCode, Json<Issue>), StatusCode> {
    let title = body.title.trim();
    if title.is_empty() || title.len() > 300 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let status = body.status.unwrap_or_else(|| "todo".to_owned());
    let priority = body.priority.unwrap_or(2);
    let assignee = body.assignee.unwrap_or_default();
    let project = body.project.unwrap_or_else(|| PROJECTS[0].to_owned());
    let estimate = body.estimate.unwrap_or(3);
    if !valid_status(&status)
        || !valid_assignee(&assignee)
        || !PROJECTS.contains(&project.as_str())
        || !(0..=4).contains(&priority)
        || !(1..=21).contains(&estimate)
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let today = today_epoch_day();
    let (completed_day, cycle_days): (i64, i64) =
        if status == "done" { (today, 0) } else { (0, 0) };

    let insert = match body.id {
        Some(id) => {
            state
                .pg
                .query_one(
                    &format!(
                        "INSERT INTO issues (id, title, status, priority, assignee, project, \
                         estimate, created_day, completed_day, cycle_days)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) {ISSUE_RETURNING}"
                    ),
                    &[
                        &id,
                        &title,
                        &status,
                        &priority,
                        &assignee,
                        &project,
                        &estimate,
                        &today,
                        &completed_day,
                        &cycle_days,
                    ],
                )
                .await
        }
        None => {
            state
                .pg
                .query_one(
                    &format!(
                        "INSERT INTO issues (title, status, priority, assignee, project, \
                         estimate, created_day, completed_day, cycle_days)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) {ISSUE_RETURNING}"
                    ),
                    &[
                        &title,
                        &status,
                        &priority,
                        &assignee,
                        &project,
                        &estimate,
                        &today,
                        &completed_day,
                        &cycle_days,
                    ],
                )
                .await
        }
    };
    let row = insert.map_err(|err| {
        warn!(?err, "create_issue failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok((StatusCode::CREATED, Json(issue_from_pg_row(&row))))
}

#[derive(Deserialize)]
struct UpdateIssue {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    assignee: Option<String>,
    #[serde(default)]
    estimate: Option<i64>,
}

/// Partial update. Status transitions maintain the denormalized
/// analytics columns: moving *into* `done` stamps `completed_day` and
/// `cycle_days`; moving out of `done` clears them.
async fn update_issue(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<UpdateIssue>,
) -> Result<Json<Issue>, StatusCode> {
    let mut sets: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Send + Sync>> = Vec::new();

    if let Some(title) = body.title {
        let title = title.trim().to_owned();
        if title.is_empty() || title.len() > 300 {
            return Err(StatusCode::BAD_REQUEST);
        }
        params.push(Box::new(title));
        sets.push(format!("title = ${}", params.len()));
    }
    if let Some(priority) = body.priority {
        if !(0..=4).contains(&priority) {
            return Err(StatusCode::BAD_REQUEST);
        }
        params.push(Box::new(priority));
        sets.push(format!("priority = ${}", params.len()));
    }
    if let Some(assignee) = body.assignee {
        if !valid_assignee(&assignee) {
            return Err(StatusCode::BAD_REQUEST);
        }
        params.push(Box::new(assignee));
        sets.push(format!("assignee = ${}", params.len()));
    }
    if let Some(estimate) = body.estimate {
        if !(1..=21).contains(&estimate) {
            return Err(StatusCode::BAD_REQUEST);
        }
        params.push(Box::new(estimate));
        sets.push(format!("estimate = ${}", params.len()));
    }
    if let Some(status) = body.status {
        if !valid_status(&status) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let today = today_epoch_day();
        params.push(Box::new(status.clone()));
        sets.push(format!("status = ${}", params.len()));
        if status == "done" {
            // GREATEST guards seeded rows whose created_day is today.
            params.push(Box::new(today));
            sets.push(format!("completed_day = ${}", params.len()));
            params.push(Box::new(today));
            sets.push(format!(
                "cycle_days = GREATEST(0, ${} - created_day)",
                params.len()
            ));
        } else {
            sets.push("completed_day = 0".to_owned());
            sets.push("cycle_days = 0".to_owned());
        }
    }
    if sets.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    params.push(Box::new(id));
    let sql = format!(
        "UPDATE issues SET {} WHERE id = ${} {ISSUE_RETURNING}",
        sets.join(", "),
        params.len(),
    );
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = state
        .pg
        .query_opt(&sql, &param_refs)
        .await
        .map_err(|err| {
            warn!(?err, "update_issue failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(issue_from_pg_row(&row)))
}

async fn delete_issue(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    match state
        .pg
        .execute("DELETE FROM issues WHERE id = $1", &[&id])
        .await
    {
        Ok(0) => StatusCode::NOT_FOUND,
        Ok(_) => StatusCode::NO_CONTENT,
        Err(err) => {
            warn!(?err, "delete_issue failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

// ---------------------------------------------------------------------------
// Activity simulator
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SimulateRequest {
    /// How many synthetic team events to apply. Capped server-side.
    events: usize,
}

/// Hard cap on one simulate call so a curious user can't hang the
/// demo with a huge request.
const SIMULATE_CAP: usize = 200;

/// Board-order successor for the progress action.
fn next_status(status: &str) -> Option<&'static str> {
    match status {
        "backlog" => Some("todo"),
        "todo" => Some("in_progress"),
        "in_progress" => Some("in_review"),
        "in_review" => Some("done"),
        _ => None,
    }
}

/// Apply `events` synthetic team events: mostly progressing existing
/// issues across the board, plus a trickle of new issues, triages,
/// reassignments, and the occasional cancellation. Each event is one
/// SQL statement (= one WAL transaction = one live diff batch).
async fn simulate(
    State(state): State<AppState>,
    Json(body): Json<SimulateRequest>,
) -> Result<Json<Value>, StatusCode> {
    if body.events == 0 || body.events > SIMULATE_CAP {
        return Err(StatusCode::BAD_REQUEST);
    }
    let today = today_epoch_day();
    let mut created = 0_u32;
    let mut progressed = 0_u32;
    let mut triaged = 0_u32;
    let mut cancelled = 0_u32;

    for _ in 0..body.events {
        // Sample the action + all its random inputs while holding the
        // RNG lock, then release it before awaiting Postgres.
        let action = {
            let mut rng = state.sim_rng.lock().expect("sim rng poisoned");
            plan_event(&mut rng)
        };
        match action {
            SimEvent::Create {
                title,
                project,
                priority,
                estimate,
                assignee,
            } => {
                let done = state
                    .pg
                    .execute(
                        "INSERT INTO issues (title, status, priority, assignee, project, \
                         estimate, created_day)
                         VALUES ($1, 'todo', $2, $3, $4, $5, $6)",
                        &[&title, &priority, &assignee, &project, &estimate, &today],
                    )
                    .await;
                if log_sim_result("create", done) {
                    created += 1;
                }
            }
            SimEvent::Progress { from } => {
                let Some(to) = next_status(&from) else {
                    continue;
                };
                let done = if to == "done" {
                    state
                        .pg
                        .execute(
                            "UPDATE issues
                             SET status = 'done', completed_day = $2,
                                 cycle_days = GREATEST(0, $2 - created_day)
                             WHERE id = (SELECT id FROM issues WHERE status = $1
                                         ORDER BY random() LIMIT 1)",
                            &[&from, &today],
                        )
                        .await
                } else {
                    state
                        .pg
                        .execute(
                            "UPDATE issues SET status = $2
                             WHERE id = (SELECT id FROM issues WHERE status = $1
                                         ORDER BY random() LIMIT 1)",
                            &[&from, &to],
                        )
                        .await
                };
                if log_sim_result("progress", done) {
                    progressed += 1;
                }
            }
            SimEvent::Triage { priority, assignee } => {
                let done = state
                    .pg
                    .execute(
                        "UPDATE issues SET priority = $1, assignee = $2
                         WHERE id = (SELECT id FROM issues
                                     WHERE status = 'backlog' OR status = 'todo'
                                     ORDER BY random() LIMIT 1)",
                        &[&priority, &assignee],
                    )
                    .await;
                if log_sim_result("triage", done) {
                    triaged += 1;
                }
            }
            SimEvent::Cancel => {
                let done = state
                    .pg
                    .execute(
                        "UPDATE issues
                         SET status = 'cancelled', completed_day = 0, cycle_days = 0
                         WHERE id = (SELECT id FROM issues WHERE status = 'backlog'
                                     ORDER BY random() LIMIT 1)",
                        &[],
                    )
                    .await;
                if log_sim_result("cancel", done) {
                    cancelled += 1;
                }
            }
        }
    }

    Ok(Json(json!({
        "created": created,
        "progressed": progressed,
        "triaged": triaged,
        "cancelled": cancelled,
    })))
}

fn log_sim_result(action: &str, result: Result<u64, tokio_postgres::Error>) -> bool {
    match result {
        Ok(n) => n > 0,
        Err(err) => {
            warn!(?err, action, "simulator write failed");
            false
        }
    }
}

enum SimEvent {
    Create {
        title: String,
        project: String,
        priority: i64,
        estimate: i64,
        assignee: String,
    },
    Progress {
        from: String,
    },
    Triage {
        priority: i64,
        assignee: String,
    },
    Cancel,
}

const SIM_TITLES: &[&str] = &[
    "Chase p99 regression in cursor pump",
    "Backfill missing replica acks metric",
    "Review TopK memory bound",
    "Update TLS runbook screenshots",
    "Spike: partial snapshot resume",
    "Fix flaky reconnect conformance case",
    "Tune diff coalescing window",
    "Document permission-rule precedence",
    "Upgrade wasm-bindgen pin",
    "Add soak scenario for hot resubscribe",
    "Triage saturation alert from loadsuite",
    "Prototype column-level grants",
];

fn plan_event(rng: &mut DemoRng) -> SimEvent {
    // 0 create, 1 progress, 2 triage, 3 cancel
    match rng.weighted(&[24, 55, 15, 6]) {
        0 => SimEvent::Create {
            title: SIM_TITLES[rng.below(SIM_TITLES.len())].to_owned(),
            project: PROJECTS[rng.weighted(&[30, 24, 22, 16, 8])].to_owned(),
            priority: i64::try_from(rng.weighted(&[8, 22, 38, 24, 8])).unwrap_or(2),
            estimate: [1_i64, 2, 3, 5, 8][rng.below(5)],
            assignee: if rng.below(3) == 0 {
                String::new()
            } else {
                USERS[rng.below(USERS.len())].id.to_owned()
            },
        },
        1 => {
            // Progress pressure is highest late in the pipeline so
            // simulated work actually completes.
            let from = ["backlog", "todo", "in_progress", "in_review"][rng.weighted(&[2, 3, 4, 5])];
            SimEvent::Progress {
                from: from.to_owned(),
            }
        }
        2 => SimEvent::Triage {
            priority: i64::try_from(rng.weighted(&[4, 18, 36, 30, 12])).unwrap_or(2),
            assignee: USERS[rng.below(USERS.len())].id.to_owned(),
        },
        _ => SimEvent::Cancel,
    }
}

// ---------------------------------------------------------------------------
// Permissions playground
// ---------------------------------------------------------------------------

/// Currently-applied permissions DSL source, its rule summaries, and
/// the boot default (so the editor's reset button doesn't hardcode
/// the TOML client-side).
async fn get_permissions(State(state): State<AppState>) -> Json<Value> {
    let (toml, rules) = state.permissions.snapshot();
    Json(json!({
        "toml": toml,
        "default_toml": DEFAULT_PERMISSIONS_TOML,
        "rules": rules,
    }))
}

#[derive(Deserialize)]
struct PutPermissionsRequest {
    toml: String,
}

/// Compile the submitted DSL and hot-swap it onto the running server.
/// Parse/compile failures return 422 with the compiler's message and
/// leave the active rule set untouched.
async fn put_permissions(
    State(state): State<AppState>,
    Json(body): Json<PutPermissionsRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.permissions.apply(&body.toml) {
        Ok(rules) => Ok(Json(json!({ "rules": rules }))),
        Err(err) => {
            warn!(%err, "permissions update rejected");
            Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": err.to_string() })),
            ))
        }
    }
}
