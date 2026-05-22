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
//! | POST   | `/api/orders/bulk-add`       | `{ "category_id": …, "count": … }`    |
//! | POST   | `/api/accounts/deposit`      | `{ "actor_user_id": …, "amount_cents": … }` |
//! | POST   | `/api/accounts/withdraw`     | `{ "actor_user_id": …, "amount_cents": … }` |
//! | POST   | `/api/accounts/transfer`     | `{ "actor_user_id": …, "to_user_id": …, "amount_cents": … }` |
//! | GET    | `/api/orders/stats`          | -                                     |

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
use crate::state::{OrderStore, Post, Store};
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
    /// Read-side mirror of `orders`. Same lag semantics as `store`.
    pub orders: Arc<OrderStore>,
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
        .route("/api/orders/bulk-add", axum::routing::post(bulk_add_orders))
        .route("/api/orders/stats", get(order_stats))
        .route("/api/accounts/deposit", axum::routing::post(deposit))
        .route("/api/accounts/withdraw", axum::routing::post(withdraw))
        .route("/api/accounts/transfer", axum::routing::post(transfer))
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
struct BulkAddOrdersRequest {
    category_id: i64,
    count: usize,
    /// Minimum `amount_cents` for the inserted orders. The actual
    /// price is `floor_cents + uniform(0, spread_cents)`, modeling
    /// the per-category distribution the client knows about.
    floor_cents: i64,
    /// Range of cents added on top of `floor_cents`. Larger spread
    /// = more vertical scatter in the bubble chart per category.
    spread_cents: i64,
}

/// Hard cap on a single bulk-add. Keeps a curious user from hanging
/// the demo with a 10M-row request.
const BULK_ADD_CAP: usize = 50_000;

/// Insert `count` orders into one category in a single statement.
/// Postgres groups them into one logical-replication transaction, the
/// consumer applies them at one mirror LSN, and subscribers see a
/// single coalesced aggregate update.
async fn bulk_add_orders(
    State(state): State<AppState>,
    Json(body): Json<BulkAddOrdersRequest>,
) -> Result<Json<Value>, StatusCode> {
    if body.count == 0 || body.count > BULK_ADD_CAP {
        return Err(StatusCode::BAD_REQUEST);
    }
    if body.spread_cents < 0 || body.floor_cents < 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let count = i64::try_from(body.count).map_err(|_| StatusCode::BAD_REQUEST)?;
    let inserted = state
        .pg
        .execute(
            // Cast each placeholder so Postgres infers types from
            // the column / generate_series side rather than from the
            // `random() * …` context (which would promote $3 to
            // float8 and reject the bound i64).
            "INSERT INTO orders (category_id, amount_cents)
             SELECT $1::bigint,
                    ($2::bigint + (random() * $3::bigint))::bigint
             FROM generate_series(1::bigint, $4::bigint) AS g",
            &[
                &body.category_id,
                &body.floor_cents,
                &body.spread_cents,
                &count,
            ],
        )
        .await
        .map_err(|err| {
            warn!(?err, "bulk_add_orders failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let total_rows = state.orders.row_count();
    Ok(Json(json!({
        "inserted": inserted,
        "category_id": body.category_id,
        "total_rows": total_rows,
    })))
}

async fn order_stats(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "total_rows": state.orders.row_count() }))
}

#[derive(Deserialize)]
struct AccountAmountRequest {
    actor_user_id: String,
    amount_cents: i64,
}

#[derive(Deserialize)]
struct TransferRequest {
    actor_user_id: String,
    to_user_id: String,
    amount_cents: i64,
}

fn validate_account_write(actor_user_id: &str, amount_cents: i64) -> Result<(), StatusCode> {
    if amount_cents <= 0 || amount_cents > 1_000_000 {
        return Err(StatusCode::BAD_REQUEST);
    }
    if lookup(actor_user_id).is_none() {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

async fn deposit(
    State(state): State<AppState>,
    Json(body): Json<AccountAmountRequest>,
) -> Result<Json<Value>, StatusCode> {
    validate_account_write(&body.actor_user_id, body.amount_cents)?;
    let updated = state
        .pg
        .execute(
            "UPDATE accounts
             SET balance_cents = balance_cents + $2
             WHERE owner_user_id = $1",
            &[&body.actor_user_id, &body.amount_cents],
        )
        .await
        .map_err(|err| {
            warn!(?err, "deposit failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    if updated == 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(json!({ "updated": updated })))
}

async fn withdraw(
    State(state): State<AppState>,
    Json(body): Json<AccountAmountRequest>,
) -> Result<Json<Value>, StatusCode> {
    validate_account_write(&body.actor_user_id, body.amount_cents)?;
    let updated = state
        .pg
        .execute(
            "UPDATE accounts
             SET balance_cents = balance_cents - $2
             WHERE owner_user_id = $1 AND balance_cents >= $2",
            &[&body.actor_user_id, &body.amount_cents],
        )
        .await
        .map_err(|err| {
            warn!(?err, "withdraw failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    if updated == 0 {
        return Err(StatusCode::CONFLICT);
    }
    Ok(Json(json!({ "updated": updated })))
}

async fn transfer(
    State(state): State<AppState>,
    Json(body): Json<TransferRequest>,
) -> Result<Json<Value>, StatusCode> {
    validate_account_write(&body.actor_user_id, body.amount_cents)?;
    if body.actor_user_id == body.to_user_id || lookup(&body.to_user_id).is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let row = state
        .pg
        .query_one(
            "WITH recipient AS (
                 SELECT id FROM accounts WHERE owner_user_id = $2
             ),
             debit AS (
                 UPDATE accounts
                 SET balance_cents = balance_cents - $3
                 WHERE owner_user_id = $1
                   AND balance_cents >= $3
                   AND EXISTS (SELECT 1 FROM recipient)
                 RETURNING id
             ),
             credit AS (
                 UPDATE accounts
                 SET balance_cents = balance_cents + $3
                 WHERE owner_user_id = $2
                   AND EXISTS (SELECT 1 FROM debit)
                 RETURNING id
             )
             SELECT
                 (SELECT COUNT(*)::bigint FROM debit) AS debited,
                 (SELECT COUNT(*)::bigint FROM credit) AS credited",
            &[&body.actor_user_id, &body.to_user_id, &body.amount_cents],
        )
        .await
        .map_err(|err| {
            warn!(?err, "transfer failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let debited = row.get::<_, i64>(0);
    let credited = row.get::<_, i64>(1);
    if debited != 1 || credited != 1 {
        return Err(StatusCode::CONFLICT);
    }
    Ok(Json(json!({ "debited": debited, "credited": credited })))
}
