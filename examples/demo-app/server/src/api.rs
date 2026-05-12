//! Axum HTTP routes for the demo's write API.
//!
//! Endpoints (JSON bodies, JSON responses):
//!
//! | Method | Path                         | Body                                  |
//! | ------ | ---------------------------- | ------------------------------------- |
//! | GET    | `/api/health`                | -                                     |
//! | GET    | `/api/posts`                 | -                                     |
//! | POST   | `/api/posts`                 | `{ "title": "...", "published": … }`  |
//! | PATCH  | `/api/posts/:id`             | `{ "published": true \| false }`      |
//! | DELETE | `/api/posts/:id`             | -                                     |

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::cors::{Any, CorsLayer};

use crate::state::{Post, Store};
use crate::ws::{ws_subscribe, WsState};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
}

pub fn router(state: AppState, grpc_addr: SocketAddr) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api = Router::new()
        .route("/api/health", get(health))
        .route("/api/posts", get(list_posts).post(create_post))
        .route("/api/posts/:id", patch(update_post).delete(delete_post))
        .with_state(state);

    let ws = Router::new()
        .route("/ws/subscribe", get(ws_subscribe))
        .with_state(WsState { grpc_addr });

    api.merge(ws).layer(cors)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
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
) -> (StatusCode, Json<Post>) {
    let post = state.store.create(body.title, body.published);
    (StatusCode::CREATED, Json(post))
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
    state
        .store
        .set_published(id, body.published)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_post(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> StatusCode {
    if state.store.delete(id) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}
