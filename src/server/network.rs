use futures::{SinkExt, StreamExt};
use log::*;
use omnipaxos_kv::common::{
    kv::{ClientId, NodeId},
    messages::*,
    utils::*,
};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::configs::OmniPaxosKVConfig;

pub struct Network {
    node_id: NodeId,
    peers: Vec<NodeId>,
    peer_addresses: Vec<SocketAddr>,
    peer_connections: Vec<Option<PeerConnection>>,
    client_connections: HashMap<ClientId, ClientConnection>,
    max_client_id: Arc<Mutex<ClientId>>,
    batch_size: usize,
    connection_sink: Sender<NewConnection>,
    connection_source: Receiver<NewConnection>,
    reconnecting_peers: HashSet<NodeId>,
    client_message_sender: Sender<(ClientId, ClientMessage)>,
    cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
    pub cluster_messages: Receiver<(NodeId, ClusterMessage)>,
    pub client_messages: Receiver<(ClientId, ClientMessage)>,
    /// Channel for sending ServerMessage responses back to the HTTP API (client_id = 0).
    api_response_tx: Option<tokio::sync::mpsc::UnboundedSender<ServerMessage>>,
}

fn get_addrs(config: OmniPaxosKVConfig) -> (SocketAddr, Vec<SocketAddr>) {
    let listen_address_str = format!(
        "{}:{}",
        config.local.listen_address, config.local.listen_port
    );
    let listen_address = SocketAddr::from_str(&listen_address_str).expect(&format!(
        "{listen_address_str} is an invalid listen address"
    ));
    let node_addresses: Vec<SocketAddr> = config
        .cluster
        .node_addrs
        .into_iter()
        .map(|addr_str| match addr_str.to_socket_addrs() {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(e) => panic!("Address {addr_str} is invalid: {e}"),
        })
        .collect();
    (listen_address, node_addresses)
}

impl Network {
    // Creates a new network with connections other server nodes in the cluster and any clients.
    // Waits until connections to all servers and clients are established before resolving.
    pub async fn new(config: OmniPaxosKVConfig, batch_size: usize) -> Self {
        let (listen_address, node_addresses) = get_addrs(config.clone());
        let id = config.local.server_id;
        let peer_addresses: Vec<(NodeId, SocketAddr)> = config
            .cluster
            .nodes
            .into_iter()
            .zip(node_addresses.into_iter())
            .filter(|(node_id, _addr)| *node_id != id)
            .collect();
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peer_addresses.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(batch_size);
        let (connection_sink, connection_source) = mpsc::channel(30);
        let mut network = Self {
            node_id: id,
            peers: peer_addresses.iter().map(|(id, _)| *id).collect(),
            peer_addresses: peer_addresses.iter().map(|(_, addr)| *addr).collect(),
            peer_connections: cluster_connections,
            client_connections: HashMap::new(),
            max_client_id: Arc::new(Mutex::new(0)),
            batch_size,
            connection_sink,
            connection_source,
            reconnecting_peers: HashSet::new(),
            client_message_sender,
            cluster_message_sender,
            cluster_messages,
            client_messages,
            api_response_tx: None,
        };
        let num_clients = config.local.num_clients;
        network
            .initialize_connections(id, num_clients, peer_addresses, listen_address)
            .await;
        network
    }

    /// Returns a sender that injects (client_id=0, ClientMessage) directly into the
    /// client_messages channel, bypassing TCP entirely.
    pub fn api_sender(&self) -> Sender<(ClientId, ClientMessage)> {
        self.client_message_sender.clone()
    }

    /// Registers a channel that will receive ServerMessage responses for client_id = 0.
    pub fn set_api_response_channel(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
    ) {
        self.api_response_tx = Some(tx);
    }

    async fn initialize_connections(
        &mut self,
        id: NodeId,
        num_clients: usize,
        peers: Vec<(NodeId, SocketAddr)>,
        listen_address: SocketAddr,
    ) {
        self.spawn_connection_listener(self.connection_sink.clone(), listen_address);
        self.spawn_peer_connectors(id, peers);
        while let Some(new_connection) = self.connection_source.recv().await {
            self.register_connection(new_connection);
            let all_clients_connected = self.client_connections.len() >= num_clients;
            let all_cluster_connected = self.peer_connections.iter().all(|c| c.is_some());
            if all_clients_connected && all_cluster_connected {
                break;
            }
        }
    }

    fn register_connection(&mut self, new_connection: NewConnection) {
        match new_connection {
            NewConnection::ToPeer(connection) => {
                let peer_id = connection.peer_id;
                let peer_idx = self.cluster_id_to_idx(peer_id).unwrap();
                if let Some(existing_connection) = self.peer_connections[peer_idx].take() {
                    existing_connection.close();
                }
                self.peer_connections[peer_idx] = Some(connection);
                self.reconnecting_peers.remove(&peer_id);
            }
            NewConnection::ToClient(connection) => {
                if let Some(existing_connection) =
                    self.client_connections.insert(connection.client_id, connection)
                {
                    existing_connection.close();
                }
            }
        }
    }

    pub fn poll_new_connections(&mut self) {
        while let Ok(new_connection) = self.connection_source.try_recv() {
            self.register_connection(new_connection);
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewConnection>,
        listen_address: SocketAddr,
    ) -> tokio::task::JoinHandle<()> {
        let client_sender = self.client_message_sender.clone();
        let cluster_sender = self.cluster_message_sender.clone();
        let max_client_id_handle = self.max_client_id.clone();
        let batch_size = self.batch_size;
        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            client_sender.clone(),
                            cluster_sender.clone(),
                            connection_sender.clone(),
                            max_client_id_handle.clone(),
                            batch_size,
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        client_message_sender: Sender<(ClientId, ClientMessage)>,
        cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
        connection_sender: Sender<NewConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
        batch_size: usize,
    ) {
        // Identify connector's ID and type by handshake
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        let new_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToPeer(PeerConnection::new(
                    node_id,
                    underlying_stream,
                    batch_size,
                    cluster_message_sender,
                ))
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                info!("Identified connection from client {next_client_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToClient(ClientConnection::new(
                    next_client_id,
                    underlying_stream,
                    batch_size,
                    client_message_sender,
                ))
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
                return;
            }
            None => {
                info!("Connection to unidentified source dropped");
                return;
            }
        };
        connection_sender.send(new_connection).await.unwrap();
    }

    fn spawn_peer_connectors(&mut self, my_id: NodeId, peers: Vec<(NodeId, SocketAddr)>) {
        let peers_to_connect_to = peers.into_iter().filter(|(peer_id, _)| *peer_id < my_id);
        for (peer, peer_address) in peers_to_connect_to {
            self.spawn_peer_connector(my_id, peer, peer_address);
        }
    }

    fn spawn_peer_connector(&self, my_id: NodeId, peer: NodeId, peer_address: SocketAddr) {
        let reconnect_delay = Duration::from_secs(1);
        let mut reconnect_interval = tokio::time::interval(reconnect_delay);
        let cluster_sender = self.cluster_message_sender.clone();
        let connection_sender = self.connection_sink.clone();
        let batch_size = self.batch_size;
        tokio::spawn(async move {
            // Establish connection
            let peer_connection = loop {
                reconnect_interval.tick().await;
                match TcpStream::connect(peer_address).await {
                    Ok(connection) => {
                        info!("New connection to node {peer}");
                        connection.set_nodelay(true).unwrap();
                        break connection;
                    }
                    Err(err) => {
                        error!("Establishing connection to node {peer} failed: {err}")
                    }
                }
            };
            // Send handshake
            let mut registration_connection = frame_registration_connection(peer_connection);
            let handshake = RegistrationMessage::NodeRegister(my_id);
            if let Err(err) = registration_connection.send(handshake).await {
                error!("Error sending handshake to {peer}: {err}");
                return;
            }
            let underlying_stream = registration_connection.into_inner().into_inner();
            // Create connection actor
            let peer_actor = PeerConnection::new(peer, underlying_stream, batch_size, cluster_sender);
            let new_connection = NewConnection::ToPeer(peer_actor);
            if let Err(err) = connection_sender.send(new_connection).await {
                warn!("Failed to register connection to node {peer}: {err}");
            }
        });
    }

    fn maybe_reconnect_peer(&mut self, peer: NodeId) {
        if peer >= self.node_id {
            return;
        }
        if self.reconnecting_peers.contains(&peer) {
            return;
        }
        if let Some(idx) = self.cluster_id_to_idx(peer) {
            if self.peer_connections[idx].is_none() {
                self.reconnecting_peers.insert(peer);
                let peer_address = self.peer_addresses[idx];
                self.spawn_peer_connector(self.node_id, peer, peer_address);
            }
        }
    }

    pub fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        self.poll_new_connections();
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.peer_connections[idx] {
                Some(ref mut connection) => {
                    if let Err(err) = connection.send(msg) {
                        warn!("Couldn't send msg to peer {to}: {err}");
                        if let Some(connection) = self.peer_connections[idx].take() {
                            connection.close();
                        }
                        self.maybe_reconnect_peer(to);
                    }
                }
                None => {
                    warn!("Not connected to node {to}");
                    self.maybe_reconnect_peer(to);
                }
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        if to == 0 {
            if let Some(ref tx) = self.api_response_tx {
                if let Err(e) = tx.send(msg) {
                    warn!("Couldn't send response to HTTP API: {e}");
                }
            }
            return;
        }
        match self.client_connections.get_mut(&to) {
            Some(connection) => {
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to client {to}: {err}");
                    self.client_connections.remove(&to);
                }
            }
            None => warn!("Not connected to client {to}"),
        }
    }

    // Removes all client and peer connections and ends their corresponding tasks.
    #[allow(dead_code)]
    pub fn shutdown(&mut self) {
        for (_, client_connection) in self.client_connections.drain() {
            client_connection.close();
        }
        for peer_connection in self.peer_connections.drain(..) {
            if let Some(connection) = peer_connection {
                connection.close();
            }
        }
        for _ in 0..self.peers.len() {
            self.peer_connections.push(None);
        }
        self.reconnecting_peers.clear();
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

enum NewConnection {
    ToPeer(PeerConnection),
    ToClient(ClientConnection),
}

struct PeerConnection {
    peer_id: NodeId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ClusterMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: NodeId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(NodeId, ClusterMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_cluster_connection(connection);
        // Reader Actor
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((peer_id, m)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to node {peer_id}: {err}");
                        break;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to node {peer_id}: {err}");
                    break;
                }
            }
            info!("Connection to node {peer_id} closed");
        });
        PeerConnection {
            peer_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ClusterMessage,
    ) -> Result<(), mpsc::error::SendError<ClusterMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

struct ClientConnection {
    client_id: ClientId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ServerMessage>,
}

impl ClientConnection {
    pub fn new(
        client_id: ClientId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(ClientId, ClientMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_servers_connection(connection);
        // Reader Actor
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => incoming_messages.send((client_id, m)).await.unwrap(),
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to client {client_id}: {err}");
                        error!("Killing connection to client {client_id}");
                        return;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to client {client_id}: {err}");
                    error!("Killing connection to client {client_id}");
                    return;
                }
            }
        });
        ClientConnection {
            client_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}
