use crate::{configs::OmniPaxosKVConfig, server::OmniPaxosServer};
use env_logger;

mod api;
mod configs;
mod database;
mod network;
mod server;
#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match OmniPaxosKVConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };

    // Channel connecting the HTTP API shim → OmniPaxosServer
    let (api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();

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

    // Spawn the Axum HTTP server
    let api_state = api::ApiState::new(api_tx);
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

    let mut server = OmniPaxosServer::new(server_config, api_rx).await;
    server.run().await;
}
