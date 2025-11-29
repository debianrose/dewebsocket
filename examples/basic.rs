use dewebsocket::{WebSocketServerBuilder, WebSocketMessage};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    let server = Arc::new(WebSocketServerBuilder::new()
        .on_message(|session, msg| {
            if let WebSocketMessage::Text(text) = msg {
                println!("📨 {}: {}", session.id, text);
            }
        })
        .build());

    server.clone().start_maintenance_tasks().await;
    server.listen("0.0.0.0:8080").await?;
    Ok(())
}
