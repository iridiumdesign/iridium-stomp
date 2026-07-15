use futures::StreamExt;
use iridium_stomp::{AckMode, Connection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This example expects a STOMP broker on localhost:61613 (e.g. RabbitMQ with stomp plugin).
    // Start a local broker before running: `docker compose up -d`

    let conn = Connection::connect(
        "127.0.0.1:61613",
        "guest",
        "guest",
        Connection::DEFAULT_HEARTBEAT,
    )
    .await?;

    let mut sub = conn.subscribe("/queue/test", AckMode::Auto).await?;

    println!("Listening on /queue/test ...");
    while let Some(frame) = sub.next().await {
        println!("Received: {}", frame);
    }

    conn.close().await?;
    Ok(())
}
