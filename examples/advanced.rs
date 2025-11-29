use dewebsocket::{WebSocketServerBuilder, WebSocketMessage, CloseCode};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    let server = Arc::new(WebSocketServerBuilder::new()
        .with_max_connections(1000)
        .with_ping_interval(Duration::from_secs(30))
        .with_connection_timeout(Duration::from_secs(300))
        .on_connect(|session| {
            println!("🔗 New connection: {} from {}", session.id, session.remote_addr);
            true
        })
        .on_message(|session, msg| {
            match msg {
                WebSocketMessage::Text(text) => {
                    println!("📨 {}: {}", session.id, text);
                }
                WebSocketMessage::Binary(data) => {
                    println!("📦 {}: {} bytes", session.id, data.len());
                }
                _ => {}
            }
        })
        .on_close(|session, code, reason| {
            println!("🔒 Connection closed: {} - {:?} - {}", session.id, code, reason);
        })
        .on_error(|session, error| {
            println!("❌ Error for {}: {}", session.id, error);
        })
        .build());

    println!("🚀 Starting WebSocket server on 0.0.0.0:8080");
    
    server.clone().start_maintenance_tasks().await;
    server.listen("0.0.0.0:8080").await?;
    Ok(())
}
