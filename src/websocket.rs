use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, broadcast};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::interval;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use uuid::Uuid;
use log::{info, warn, error, debug};
use dashmap::DashMap;

pub use tokio_tungstenite::tungstenite;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseCode {
    Normal = 1000,
    GoingAway = 1001,
    ProtocolError = 1002,
    UnsupportedData = 1003,
    NoStatus = 1005,
    Abnormal = 1006,
    InvalidPayload = 1007,
    PolicyViolation = 1008,
    MessageTooLarge = 1009,
    InternalError = 1011,
}

impl From<u16> for CloseCode {
    fn from(code: u16) -> Self {
        match code {
            1000 => CloseCode::Normal,
            1001 => CloseCode::GoingAway,
            1002 => CloseCode::ProtocolError,
            1003 => CloseCode::UnsupportedData,
            1005 => CloseCode::NoStatus,
            1006 => CloseCode::Abnormal,
            1007 => CloseCode::InvalidPayload,
            1008 => CloseCode::PolicyViolation,
            1009 => CloseCode::MessageTooLarge,
            1011 => CloseCode::InternalError,
            _ => CloseCode::Normal,
        }
    }
}

impl From<CloseCode> for u16 {
    fn from(code: CloseCode) -> u16 {
        code as u16
    }
}

#[derive(Debug, Clone)]
pub enum WebSocketMessage {
    Text(Arc<str>),
    Binary(Arc<[u8]>),
    Ping(Arc<[u8]>),
    Pong(Arc<[u8]>),
    Close(Option<CloseCode>, Option<Arc<str>>),
}

impl WebSocketMessage {
    fn into_ws_message(self) -> Message {
        match self {
            WebSocketMessage::Text(text) => Message::Text(text.to_string()),
            WebSocketMessage::Binary(data) => Message::Binary(data.to_vec()),
            WebSocketMessage::Ping(data) => Message::Ping(data.to_vec()),
            WebSocketMessage::Pong(data) => Message::Pong(data.to_vec()),
            WebSocketMessage::Close(code, reason) => {
                let close_frame = code.map(|code| {
                    let code_num: u16 = code.into();
                    tungstenite::protocol::CloseFrame {
                        code: code_num.into(),
                        reason: reason.map(|r| r.to_string()).unwrap_or_default().into(),
                    }
                });
                Message::Close(close_frame)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebSocketSession {
    pub id: Arc<str>,
    pub user_id: Option<Arc<str>>,
    pub remote_addr: Arc<str>,
    pub created_at: Instant,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BroadcastResult {
    pub success_count: usize,
    pub error_count: usize,
}

#[derive(Clone)]
pub struct WebSocketHandlers {
    pub on_connect: Arc<dyn Fn(WebSocketSession) -> bool + Send + Sync>,
    pub on_message: Arc<dyn Fn(WebSocketSession, WebSocketMessage) + Send + Sync>,
    pub on_close: Arc<dyn Fn(WebSocketSession, CloseCode, Arc<str>) + Send + Sync>,
    pub on_error: Arc<dyn Fn(WebSocketSession, Arc<str>) + Send + Sync>,
}

impl Default for WebSocketHandlers {
    fn default() -> Self {
        Self {
            on_connect: Arc::new(|_| true),
            on_message: Arc::new(|_, _| {}),
            on_close: Arc::new(|_, _, _| {}),
            on_error: Arc::new(|_, _| {}),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub max_message_size: usize,
    pub ping_interval: Duration,
    pub connection_timeout: Duration,
    pub max_connections: usize,
    pub send_channel_size: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024,
            ping_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(300),
            max_connections: 10000,
            send_channel_size: 1000,
        }
    }
}

struct ClientConnection {
    session_id: Arc<str>,
    sender: mpsc::Sender<Message>,
    last_activity: AtomicU64,
    is_alive: AtomicBool,
}

impl ClientConnection {
    fn new(session_id: Arc<str>, sender: mpsc::Sender<Message>) -> Self {
        Self {
            session_id,
            sender,
            last_activity: AtomicU64::new(Instant::now().elapsed().as_secs()),
            is_alive: AtomicBool::new(true),
        }
    }

    fn update_activity(&self) {
        self.last_activity.store(Instant::now().elapsed().as_secs(), Ordering::Relaxed);
    }

    fn is_inactive(&self, timeout_secs: u64) -> bool {
        let now = Instant::now().elapsed().as_secs();
        now - self.last_activity.load(Ordering::Relaxed) > timeout_secs
    }
}

struct SessionStore {
    sessions: DashMap<Arc<str>, WebSocketSession>,
    user_sessions: DashMap<Arc<str>, HashSet<Arc<str>>>,
    clients: DashMap<Arc<str>, ClientConnection>,
}

impl SessionStore {
    fn new() -> Self {
        Self {
            sessions: DashMap::new(),
            user_sessions: DashMap::new(),
            clients: DashMap::new(),
        }
    }

    fn add_session(&self, session: WebSocketSession, client: ClientConnection) {
        self.sessions.insert(session.id.clone(), session);
        self.clients.insert(client.session_id.clone(), client);
    }

    fn remove_session(&self, session_id: &str) -> Option<WebSocketSession> {
        if let Some((_, session)) = self.sessions.remove(session_id) {
            self.clients.remove(session_id);
            
            if let Some(user_id) = &session.user_id {
                if let Some(mut sessions) = self.user_sessions.get_mut(user_id) {
                    sessions.value_mut().remove(session_id);
                    if sessions.value().is_empty() {
                        self.user_sessions.remove(user_id);
                    }
                }
            }
            Some(session)
        } else {
            None
        }
    }

    fn attach_user(&self, session_id: &str, user_id: Arc<str>) -> bool {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            if let Some(old_user_id) = session.user_id.take() {
                if let Some(mut sessions) = self.user_sessions.get_mut(&old_user_id) {
                    sessions.value_mut().remove(session_id);
                    if sessions.value().is_empty() {
                        self.user_sessions.remove(&old_user_id);
                    }
                }
            }
            
            session.user_id = Some(user_id.clone());
            self.user_sessions
                .entry(user_id)
                .or_insert_with(HashSet::new)
                .insert(Arc::from(session_id));
            true
        } else {
            false
        }
    }

    fn get_user_sessions(&self, user_id: &str) -> Vec<Arc<str>> {
        self.user_sessions
            .get(user_id)
            .map(|sessions| sessions.value().iter().cloned().collect())
            .unwrap_or_default()
    }

    fn get_all_session_ids(&self) -> Vec<Arc<str>> {
        self.clients.iter().map(|entry| entry.key().clone()).collect()
    }

    fn get_client(&self, session_id: &str) -> Option<ClientConnection> {
        self.clients.get(session_id).map(|entry| {
            ClientConnection {
                session_id: entry.session_id.clone(),
                sender: entry.sender.clone(),
                last_activity: AtomicU64::new(entry.last_activity.load(Ordering::Relaxed)),
                is_alive: AtomicBool::new(entry.is_alive.load(Ordering::Relaxed)),
            }
        })
    }
}

pub struct WebSocketServer {
    config: WebSocketConfig,
    handlers: WebSocketHandlers,
    store: Arc<SessionStore>,
    stats: Arc<ServerStats>,
    shutdown_sender: broadcast::Sender<()>,
}

#[derive(Debug, Default)]
struct ServerStats {
    total_connections: AtomicU64,
    current_connections: AtomicU64,
    total_messages_sent: AtomicU64,
    total_messages_received: AtomicU64,
    total_errors: AtomicU64,
}

impl ServerStats {
    fn snapshot(&self) -> ServerStatsSnapshot {
        ServerStatsSnapshot {
            total_connections: self.total_connections.load(Ordering::Relaxed),
            current_connections: self.current_connections.load(Ordering::Relaxed),
            total_messages_sent: self.total_messages_sent.load(Ordering::Relaxed),
            total_messages_received: self.total_messages_received.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ServerStatsSnapshot {
    pub total_connections: u64,
    pub current_connections: u64,
    pub total_messages_sent: u64,
    pub total_messages_received: u64,
    pub total_errors: u64,
}

impl WebSocketServer {
    pub fn new(config: WebSocketConfig, handlers: WebSocketHandlers) -> Self {
        let (shutdown_sender, _) = broadcast::channel(1);
        
        Self {
            config,
            handlers,
            store: Arc::new(SessionStore::new()),
            stats: Arc::new(ServerStats::default()),
            shutdown_sender,
        }
    }

    pub async fn listen(self: Arc<Self>, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        info!("WebSocket server listening on {}", addr);

        let mut shutdown_receiver = self.shutdown_sender.subscribe();
        
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let remote_addr = addr.to_string();
                            debug!("New connection from {}", remote_addr);
                            
                            if self.get_current_connections() >= self.config.max_connections {
                                warn!("Connection limit reached, rejecting connection from {}", remote_addr);
                                continue;
                            }
                            
                            let server = self.clone();
                            tokio::spawn(async move {
                                server.handle_connection(stream, remote_addr).await;
                            });
                        }
                        Err(e) => {
                            error!("Accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_receiver.recv() => {
                    info!("Shutdown signal received");
                    break;
                }
            }
        }
        
        self.wait_for_connections().await;
        Ok(())
    }

    async fn wait_for_connections(&self) {
        let mut interval = interval(Duration::from_millis(100));
        while self.get_current_connections() > 0 {
            interval.tick().await;
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_sender.send(());
        self.wait_for_connections().await;
    }

    async fn handle_connection(self: Arc<Self>, stream: TcpStream, remote_addr: String) {
        let session_id: Arc<str> = Arc::from(Uuid::new_v4().to_string());
        let session_id_clone = session_id.clone();
        let now = Instant::now();
        
        let session = WebSocketSession {
            id: session_id.clone(),
            user_id: None,
            remote_addr: Arc::from(remote_addr),
            created_at: now,
        };

        if !(self.handlers.on_connect)(session.clone()) {
            debug!("Connection rejected by on_connect handler");
            return;
        }

        let ws_stream = match accept_async(stream).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("WebSocket handshake failed: {}", e);
                return;
            }
        };

        debug!("WebSocket connection established: {}", session_id);

        let (sender, mut receiver) = mpsc::channel(self.config.send_channel_size);
        let client_conn = ClientConnection::new(session_id.clone(), sender.clone());

        self.store.add_session(session.clone(), client_conn);
        self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
        self.stats.current_connections.fetch_add(1, Ordering::Relaxed);

        let (mut ws_sink, mut ws_stream) = ws_stream.split();

        let send_task_session_id = session_id.clone();
        let send_task = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                if let Err(e) = ws_sink.send(message).await {
                    error!("Failed to send message to {}: {}", send_task_session_id, e);
                    break;
                }
            }
        });

        let recv_task = {
            let server = self.clone();
            let session_id = session_id_clone.clone();
            
            tokio::spawn(async move {
                while let Some(result) = ws_stream.next().await {
                    match result {
                        Ok(message) => {
                            if let Some(client) = server.store.get_client(&session_id) {
                                client.update_activity();
                            }
                            
                            let ws_message = match message {
                                Message::Text(text) => WebSocketMessage::Text(Arc::from(text)),
                                Message::Binary(data) => WebSocketMessage::Binary(Arc::from(data)),
                                Message::Ping(data) => WebSocketMessage::Ping(Arc::from(data)),
                                Message::Pong(data) => WebSocketMessage::Pong(Arc::from(data)),
                                Message::Close(frame) => {
                                    let (code, reason) = match frame {
                                        Some(frame) => {
                                            let code_num: u16 = frame.code.into();
                                            let code = CloseCode::from(code_num);
                                            let reason = Arc::from(frame.reason.to_string());
                                            (Some(code), Some(reason))
                                        },
                                        None => (None, None),
                                    };
                                    WebSocketMessage::Close(code, reason)
                                }
                                Message::Frame(_) => continue,
                            };
                            
                            server.stats.total_messages_received.fetch_add(1, Ordering::Relaxed);
                            (server.handlers.on_message)(session.clone(), ws_message);
                        }
                        Err(e) => {
                            error!("Error receiving message from {}: {}", session_id, e);
                            (server.handlers.on_error)(session.clone(), Arc::from(e.to_string()));
                            break;
                        }
                    }
                }
            })
        };

        tokio::select! {
            _ = send_task => {},
            _ = recv_task => {},
        }

        self.remove_client(&session_id).await;
        debug!("WebSocket connection closed: {}", session_id);
    }

    async fn remove_client(&self, session_id: &str) {
        if let Some(removed_session) = self.store.remove_session(session_id) {
            (self.handlers.on_close)(
                removed_session, 
                CloseCode::Normal, 
                Arc::from("Connection closed")
            );
        }
        self.stats.current_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub async fn send_to_session(&self, session_id: &str, message: WebSocketMessage) -> bool {
        if let Some(client) = self.store.get_client(session_id) {
            let ws_message = message.into_ws_message();
            
            if client.sender.try_send(ws_message).is_ok() {
                self.stats.total_messages_sent.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    pub async fn broadcast(&self, message: WebSocketMessage) -> BroadcastResult {
        let mut result = BroadcastResult::default();
        let session_ids = self.store.get_all_session_ids();

        for session_id in session_ids {
            if self.send_to_session(&session_id, message.clone()).await {
                result.success_count += 1;
            } else {
                result.error_count += 1;
            }
        }
        
        result
    }

    pub async fn send_to_user(&self, user_id: &str, message: WebSocketMessage) -> BroadcastResult {
        let mut result = BroadcastResult::default();
        let session_ids = self.store.get_user_sessions(user_id);

        for session_id in session_ids {
            if self.send_to_session(&session_id, message.clone()).await {
                result.success_count += 1;
            } else {
                result.error_count += 1;
            }
        }
        
        result
    }

    pub async fn attach_session_to_user(&self, session_id: &str, user_id: String) -> bool {
        self.store.attach_user(session_id, Arc::from(user_id))
    }

    pub async fn disconnect_session(&self, session_id: &str, code: CloseCode, reason: String) -> bool {
        if self.send_to_session(session_id, WebSocketMessage::Close(Some(code), Some(Arc::from(reason.clone())))).await {
            if let Some(session) = self.store.sessions.get(session_id) {
                (self.handlers.on_close)(session.value().clone(), code, Arc::from(reason));
            }
            true
        } else {
            false
        }
    }

    pub async fn disconnect_user(&self, user_id: &str, code: CloseCode, reason: String) -> BroadcastResult {
        let mut result = BroadcastResult::default();
        let session_ids = self.store.get_user_sessions(user_id);

        for session_id in session_ids {
            if self.disconnect_session(&session_id, code, reason.clone()).await {
                result.success_count += 1;
            } else {
                result.error_count += 1;
            }
        }
        
        result
    }

    pub fn get_stats(&self) -> ServerStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn get_current_connections(&self) -> usize {
        self.stats.current_connections.load(Ordering::Relaxed) as usize
    }

    pub async fn health_check(&self) -> bool {
        let stats = self.get_stats();
        stats.current_connections < self.config.max_connections as u64
    }

    pub async fn start_maintenance_tasks(self: Arc<Self>) {
        let server = self.clone();
        tokio::spawn(async move {
            server.run_maintenance_loop().await;
        });
    }

    async fn run_maintenance_loop(self: Arc<Self>) {
        let mut ping_interval = interval(self.config.ping_interval);
        let mut cleanup_interval = interval(Duration::from_secs(60));
        
        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    self.send_pings().await;
                }
                _ = cleanup_interval.tick() => {
                    self.cleanup_inactive_sessions().await;
                }
            }
        }
    }

    async fn send_pings(&self) {
        let session_ids = self.store.get_all_session_ids();

        for session_id in session_ids {
            if self.send_to_session(&session_id, WebSocketMessage::Ping(Arc::from([]))).await {
                debug!("Sent ping to session: {}", session_id);
            }
        }
    }

    async fn cleanup_inactive_sessions(&self) {
        let timeout_secs = self.config.connection_timeout.as_secs();
        let mut to_remove = Vec::new();

        for entry in self.store.clients.iter() {
            let session_id = entry.key().clone();
            let client = entry.value();
            
            if client.is_inactive(timeout_secs) {
                to_remove.push(session_id);
            }
        }

        for session_id in to_remove {
            warn!("Closing inactive session: {}", session_id);
            self.disconnect_session(&session_id, CloseCode::Normal, "Connection timeout".to_string()).await;
        }
    }
}

impl Clone for WebSocketServer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            handlers: self.handlers.clone(),
            store: self.store.clone(),
            stats: self.stats.clone(),
            shutdown_sender: self.shutdown_sender.clone(),
        }
    }
}

pub struct WebSocketServerBuilder {
    config: WebSocketConfig,
    handlers: WebSocketHandlers,
}

impl Default for WebSocketServerBuilder {
    fn default() -> Self {
        Self {
            config: WebSocketConfig::default(),
            handlers: WebSocketHandlers::default(),
        }
    }
}

impl WebSocketServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(mut self, config: WebSocketConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = size;
        self
    }

    pub fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.config.ping_interval = interval;
        self
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }

    pub fn with_send_channel_size(mut self, size: usize) -> Self {
        self.config.send_channel_size = size;
        self
    }

    pub fn on_connect<F>(mut self, handler: F) -> Self 
    where 
        F: Fn(WebSocketSession) -> bool + Send + Sync + 'static 
    {
        self.handlers.on_connect = Arc::new(handler);
        self
    }

    pub fn on_message<F>(mut self, handler: F) -> Self 
    where 
        F: Fn(WebSocketSession, WebSocketMessage) + Send + Sync + 'static 
    {
        self.handlers.on_message = Arc::new(handler);
        self
    }

    pub fn on_close<F>(mut self, handler: F) -> Self 
    where 
        F: Fn(WebSocketSession, CloseCode, Arc<str>) + Send + Sync + 'static 
    {
        self.handlers.on_close = Arc::new(handler);
        self
    }

    pub fn on_error<F>(mut self, handler: F) -> Self 
    where 
        F: Fn(WebSocketSession, Arc<str>) + Send + Sync + 'static 
    {
        self.handlers.on_error = Arc::new(handler);
        self
    }

    pub fn build(self) -> WebSocketServer {
        WebSocketServer::new(self.config, self.handlers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_websocket_server_creation() {
        let builder = WebSocketServerBuilder::new()
            .with_max_connections(100)
            .on_connect(|session| {
                println!("New connection: {}", session.id);
                true
            })
            .on_message(|session, message| {
                println!("Message from {}: {:?}", session.id, message);
            });

        let server = builder.build();
        assert_eq!(server.get_current_connections(), 0);
    }

    #[tokio::test]
    async fn test_session_management() {
        let server = WebSocketServerBuilder::new().build();
        
        let stats = server.get_stats();
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.current_connections, 0);
    }

    #[tokio::test]
    async fn test_health_check() {
        let server = WebSocketServerBuilder::new().build();
        assert!(server.health_check().await);
    }
}
