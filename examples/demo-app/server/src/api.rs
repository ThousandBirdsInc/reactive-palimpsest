//! Axum HTTP routes for the demo's write API.
//!
//! Writes go directly to Postgres via tokio-postgres. The
//! logical-replication consumer in `db.rs` picks the change up after
//! a ~100ms poll and propagates it into the in-memory mirror — which
//! is what subscribers + read endpoints read from.
//!
//! | Method | Path                         | Body                                  |
//! | ------ | ---------------------------- | ------------------------------------- |
//! | GET    | `/api/health`                | -                                     |
//! | GET    | `/api/posts`                 | -                                     |
//! | POST   | `/api/posts`                 | `{ "title": "...", "published": … }`  |
//! | PATCH  | `/api/posts/:id`             | `{ "published": true \| false }`      |
//! | DELETE | `/api/posts/:id`             | -                                     |
//! | POST   | `/api/events/bulk-add`       | `{ "category_id": …, "count": … }`    |
//! | GET    | `/api/events/stats`          | -                                     |

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_postgres::Client;
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;

use crate::auth::{lookup, mint_token, USERS};
use crate::state::{EventStore, Post, Store};
use crate::ws::{ws_subscribe, WsState};

#[derive(Clone)]
pub struct AppState {
    /// Postgres write client — every mutation is a SQL statement
    /// against this. The replication consumer turns the resulting
    /// WAL records back into in-memory mirror updates.
    pub pg: Arc<Client>,
    /// Read-side mirror of `posts`. Read endpoints + Palimpsest
    /// snapshots both use this; it lags Postgres by the consumer
    /// poll interval (~100ms).
    pub store: Arc<Store>,
    /// Read-side mirror of `events`. Same lag semantics as `store`.
    pub events: Arc<EventStore>,
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
        .route("/api/posts", get(list_posts).post(create_post))
        .route("/api/posts/:id", patch(update_post).delete(delete_post))
        .route("/api/events/bulk-add", axum::routing::post(bulk_add_events))
        .route("/api/events/stats", get(event_stats))
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

async fn list_posts(State(state): State<AppState>) -> Json<Vec<Post>> {
    Json(state.store.snapshot())
}

#[derive(Deserialize)]
struct CreatePost {
    title: String,
    #[serde(default)]
    published: bool,
}

async fn create_post(
    State(state): State<AppState>,
    Json(body): Json<CreatePost>,
) -> Result<(StatusCode, Json<Post>), StatusCode> {
    let row = state
        .pg
        .query_one(
            "INSERT INTO posts (title, published) VALUES ($1, $2)
             RETURNING id, title, published",
            &[&body.title, &body.published],
        )
        .await
        .map_err(|err| {
            warn!(?err, "create_post failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let post = Post {
        id: row.get::<_, i64>(0),
        title: row.get::<_, String>(1),
        published: row.get::<_, bool>(2),
    };
    Ok((StatusCode::CREATED, Json(post)))
}

#[derive(Deserialize)]
struct UpdatePost {
    published: bool,
}

async fn update_post(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<UpdatePost>,
) -> Result<Json<Post>, StatusCode> {
    let row = state
        .pg
        .query_opt(
            "UPDATE posts SET published = $1 WHERE id = $2
             RETURNING id, title, published",
            &[&body.published, &id],
        )
        .await
        .map_err(|err| {
            warn!(?err, "update_post failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(Post {
        id: row.get::<_, i64>(0),
        title: row.get::<_, String>(1),
        published: row.get::<_, bool>(2),
    }))
}

async fn delete_post(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    match state
        .pg
        .execute("DELETE FROM posts WHERE id = $1", &[&id])
        .await
    {
        Ok(0) => StatusCode::NOT_FOUND,
        Ok(_) => StatusCode::NO_CONTENT,
        Err(err) => {
            warn!(?err, "delete_post failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[derive(Deserialize)]
struct BulkAddEventsRequest {
    category_id: i64,
    count: usize,
    #[serde(default = "default_base_value")]
    base_value: i64,
}

const fn default_base_value() -> i64 {
    100
}

/// Hard cap on a single bulk-add. Keeps a curious user from hanging
/// the demo with a 10M-row request.
const BULK_ADD_CAP: usize = 50_000;

/// Insert `count` events into one category in a single statement.
/// Postgres groups them into one logical-replication batch, the
/// consumer applies them at one mirror LSN, and subscribers see a
/// single coalesced aggregate update.
async fn bulk_add_events(
    State(state): State<AppState>,
    Json(body): Json<BulkAddEventsRequest>,
) -> Result<Json<Value>, StatusCode> {
    if body.count == 0 || body.count > BULK_ADD_CAP {
        return Err(StatusCode::BAD_REQUEST);
    }
    let count = i64::try_from(body.count).map_err(|_| StatusCode::BAD_REQUEST)?;
    let inserted = state
        .pg
        .execute(
            "INSERT INTO events (category_id, value)
             SELECT $1, $2 + ((g - 1) % 50)
             FROM generate_series(1::bigint, $3::bigint) AS g",
            &[&body.category_id, &body.base_value, &count],
        )
        .await
        .map_err(|err| {
            warn!(?err, "bulk_add_events failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let total_rows = state.events.row_count();
    Ok(Json(json!({
        "inserted": inserted,
        "category_id": body.category_id,
        "total_rows": total_rows,
    })))
}

async fn event_stats(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "total_rows": state.events.row_count() }))
}
