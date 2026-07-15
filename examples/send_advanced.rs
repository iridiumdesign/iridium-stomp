use iridium_stomp::{Connection, Frame};

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

    // Build a SEND frame directly for full control over headers and body format.
    let msg = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("content-type", "application/json")
        .set_body(br#"{"event": "order_placed", "id": 42}"#.to_vec());
    conn.send_frame(msg).await?;

    println!("Message sent");
    conn.close().await?;
    Ok(())
}
