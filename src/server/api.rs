/// HTTP API shim – exposes the OmniPaxos KV store to Jepsen (and any other external agent).
///
/// Endpoints
/// ---------
/// GET  /kv/{key}                          → read
/// PUT  /kv/{key}  body: plain-text value  → write (put)
/// POST /kv/{key}/cas  body: JSON {"from": <str|null>, "to": <str>}  → compare-and-swap
///
/// HTTP status codes
/// -----------------
/// 200   – operation succeeded (body carries JSON result)
/// 404   – key not found (for GET)
/// 409   – CAS precondition failed (body carries {"current": <str|null>})
/// 503   – this node is not the leader right now
///         body: {"leader": <node_id|null>}  so clients can redirect
///
/// Jepsen interprets a TCP-reset / timeout as `:info` (indeterminate).
/// A 503 from a follower is a definitive `:fail` – the write never happened.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use omnipaxos_kv::common::{kv::KVCommand, messages::KVResult};

use crate::server::{ApiRequest, ApiResponse};

// ---------------------------------------------------------------------------
// Shared state passed to every Axum handler
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ApiState {
    pub tx: UnboundedSender<ApiRequest>,
    pub cmd_counter: Arc<AtomicUsize>,
}

impl ApiState {
    pub fn new(tx: UnboundedSender<ApiRequest>) -> Self {
        Self {
            tx,
            cmd_counter: Arc::new(AtomicUsize::new(1)),
        }
    }

    fn next_cmd_id(&self) -> usize {
        // Use a large offset so HTTP-issued IDs never clash with TCP-client IDs
        // (TCP clients start from 1 and use small integers).
        0x8000_0000 + self.cmd_counter.fetch_add(1, Ordering::Relaxed)
    }

    async fn submit(&self, kv_cmd: KVCommand) -> Result<KVResult, Option<u64>> {
        let cmd_id = self.next_cmd_id();
        let (respond_to, rx) = oneshot::channel();
        let req = ApiRequest {
            command_id: cmd_id,
            kv_cmd,
            respond_to,
        };
        // If the server task has gone away this is a fatal error; just propagate as not-leader.
        if self.tx.send(req).is_err() {
            return Err(None);
        }
        match rx.await {
            Ok(ApiResponse::Ok(result)) => Ok(result),
            Ok(ApiResponse::NotLeader(leader)) => Err(leader),
            Err(_) => Err(None), // server dropped the sender (crash / shutdown)
        }
    }
}

// ---------------------------------------------------------------------------
// JSON response bodies
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ReadBody {
    value: Option<String>,
}

#[derive(Serialize)]
struct NotLeaderBody {
    leader: Option<u64>,
}

#[derive(Serialize)]
struct CasFailedBody {
    current: Option<String>,
}

#[derive(Deserialize)]
pub struct CasPayload {
    /// The value we expect to be stored currently (null = key absent).
    pub from: Option<String>,
    /// The value to store if the precondition holds.
    pub to: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /kv/:key
async fn handle_get(Path(key): Path<String>, State(state): State<ApiState>) -> Response {
    match state.submit(KVCommand::Get(key)).await {
        Ok(KVResult::Value(val)) => {
            let status = if val.is_some() {
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            };
            (status, Json(ReadBody { value: val })).into_response()
        }
        Ok(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Err(leader) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NotLeaderBody { leader }),
        )
            .into_response(),
    }
}

/// PUT /kv/:key   body = raw UTF-8 value
async fn handle_put(
    Path(key): Path<String>,
    State(state): State<ApiState>,
    body: Bytes,
) -> Response {
    let value = match std::str::from_utf8(&body) {
        Ok(s) => s.to_string(),
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    match state.submit(KVCommand::Put(key, value)).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(leader) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NotLeaderBody { leader }),
        )
            .into_response(),
    }
}

/// POST /kv/:key/cas   body = JSON {"from": <str|null>, "to": <str>}
async fn handle_cas(
    Path(key): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<CasPayload>,
) -> Response {
    match state
        .submit(KVCommand::Cas(key, payload.from, payload.to))
        .await
    {
        Ok(KVResult::CasOk) => StatusCode::OK.into_response(),
        Ok(KVResult::CasFailed(current)) => (
            StatusCode::CONFLICT,
            Json(CasFailedBody { current }),
        )
            .into_response(),
        Ok(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Err(leader) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NotLeaderBody { leader }),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/kv/:key", get(handle_get).put(handle_put))
        .route("/kv/:key/cas", post(handle_cas))
        .with_state(state)
}


