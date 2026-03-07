/// HTTP API shim – exposes the OmniPaxos KV store to Jepsen (and any other external agent).
///
/// Endpoints
/// ---------
/// GET  /kv/{key}                          → read
/// PUT  /kv/{key}  body: plain-text value  → write (put)
///
/// All requests are injected as ClientMessage::Append(client_id=0) directly into the
/// server's client_messages channel, so any node handles them identically to a TCP client.
/// Responses come back as ServerMessage via a shared pending map.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::collections::HashMap;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use tokio::sync::{oneshot, Mutex};
use tokio::sync::mpsc::Sender;

use omnipaxos_kv::common::{kv::{ClientId, CommandId, KVCommand}, messages::{ClientMessage, KVResult, ServerMessage}};

// ---------------------------------------------------------------------------
// Pending map: CommandId → oneshot response sender
// ---------------------------------------------------------------------------

pub type PendingMap = Arc<Mutex<HashMap<CommandId, oneshot::Sender<ServerMessage>>>>;

// ---------------------------------------------------------------------------
// Shared state passed to every Axum handler
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ApiState {
    /// Injects (client_id=0, ClientMessage) straight into the server's client_messages channel.
    pub tx: Sender<(ClientId, ClientMessage)>,
    pub cmd_counter: Arc<AtomicUsize>,
    pub pending: PendingMap,
}

impl ApiState {
    pub fn new(tx: Sender<(ClientId, ClientMessage)>, pending: PendingMap) -> Self {
        Self {
            tx,
            cmd_counter: Arc::new(AtomicUsize::new(1)),
            pending,
        }
    }

    fn next_cmd_id(&self) -> CommandId {
        // Large offset so HTTP-issued IDs never clash with TCP-client IDs.
        0x8000_0000 + self.cmd_counter.fetch_add(1, Ordering::Relaxed)
    }

    async fn submit(&self, kv_cmd: KVCommand) -> Result<KVResult, ()> {
        let cmd_id = self.next_cmd_id();
        let (respond_to, rx) = oneshot::channel();
        self.pending.lock().await.insert(cmd_id, respond_to);
        let msg = ClientMessage::Append(cmd_id, kv_cmd);
        if self.tx.send((0, msg)).await.is_err() {
            self.pending.lock().await.remove(&cmd_id);
            return Err(());
        }
        match rx.await {
            Ok(ServerMessage::Read(_, result)) => Ok(result),
            Ok(ServerMessage::Write(_)) => Ok(KVResult::Value(None)),
            _ => Err(()),
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

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /kv/:key
async fn handle_get(Path(key): Path<String>, State(state): State<ApiState>) -> Response {
    match state.submit(KVCommand::Get(key)).await {
        Ok(KVResult::Value(val)) => {
            let status = if val.is_some() { StatusCode::OK } else { StatusCode::NOT_FOUND };
            (status, Json(ReadBody { value: val })).into_response()
        }
        Ok(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/", get(|| async { "OmniPaxos KV HTTP API" }))
        .route("/kv/:key", get(handle_get).put(handle_put))
        .with_state(state)
}
