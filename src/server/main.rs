use crate::{configs::OmniPaxosKVConfig, server::OmniPaxosServer};
use env_logger;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

mod api;
mod configs;
mod database;
mod network;
mod server;
mod storage;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match OmniPaxosKVConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };

    // --- Dump environment and config to console ---
    // We serialize both the environment variables and the config as JSON
    // and print them to stdout so they appear on the container console.
    {
        let env_map: HashMap<String, String> = std::env::vars().collect();
        let config_json = match serde_json::to_string_pretty(&server_config) {
            Ok(s) => s,
            Err(e) => format!("<failed to serialize config: {}>", e),
        };
        let env_json = match serde_json::to_string_pretty(&env_map) {
            Ok(s) => s,
            Err(e) => format!("<failed to serialize env: {}>", e),
        };

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        log::info!("timestamp_secs: {}", ts);
        log::info!("\n=== ENVIRONMENT ===\n{}\n", env_json);
        log::info!("\n=== CONFIG ===\n{}\n", config_json);
    }

    // Determine HTTP API bind address
    let api_port = server_config
        .local
        .api_port
        .unwrap_or(server_config.local.listen_port + 1000);
    let api_addr: std::net::SocketAddr = format!(
        "{}:{}",
        server_config.local.listen_address, api_port
    )
    .parse()
    .expect("Invalid API listen address");

    // Build the server (establishes cluster connections)
    let mut server = OmniPaxosServer::new(server_config).await;

    // Wire up the API:
    // 1. api_sender injects ClientMessage::Append(client_id=0) into the server's client_messages
    // 2. api_response_tx/rx carries ServerMessage responses back for client_id=0
    let api_sender = server.network.api_sender();
    let (api_response_tx, mut api_response_rx) =
        tokio::sync::mpsc::unbounded_channel::<omnipaxos_kv::common::messages::ServerMessage>();
    server.network.set_api_response_channel(api_response_tx);

    // Pending map shared between the response-dispatch task and the Axum handlers
    let pending: api::PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let pending_dispatch = pending.clone();

    // Spawn a task that receives ServerMessages for client_id=0 and resolves pending futures
    tokio::spawn(async move {
        while let Some(msg) = api_response_rx.recv().await {
            use omnipaxos_kv::common::messages::ServerMessage;
            let cmd_id = match &msg {
                ServerMessage::Read(id, _) => *id,
                ServerMessage::Write(id) => *id,
                ServerMessage::StartSignal(_) => continue,
            };
            if let Some(tx) = pending_dispatch.lock().await.remove(&cmd_id) {
                let _ = tx.send(msg);
            }
        }
    });

    // Spawn the Axum HTTP server
    let api_state = api::ApiState::new(api_sender, pending);
    let router = api::router(api_state);
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(api_addr)
            .await
            .expect("Failed to bind HTTP API");
        log::info!("HTTP API listening on {api_addr}");
        axum::serve(listener, router)
            .await
            .expect("HTTP API server error");
    });

    server.run().await;
}
