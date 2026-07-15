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

    // Begin a transaction
    let tx_id = "tx-example-1";
    conn.begin(tx_id).await?;
    println!("Transaction {} started", tx_id);

    // Send messages within the transaction
    let msg1 = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("transaction", tx_id)
        .set_body(b"message 1 in transaction".to_vec());

    conn.send_frame(msg1).await?;
    println!("Sent message 1 in transaction");

    let msg2 = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("transaction", tx_id)
        .set_body(b"message 2 in transaction".to_vec());

    conn.send_frame(msg2).await?;
    println!("Sent message 2 in transaction");

    // Commit the transaction (both messages will be delivered atomically)
    conn.commit(tx_id).await?;
    println!("Transaction {} committed", tx_id);

    // Example of aborting a transaction
    let tx_id_2 = "tx-example-2";
    conn.begin(tx_id_2).await?;
    println!("\nTransaction {} started", tx_id_2);

    let msg3 = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("transaction", tx_id_2)
        .set_body(b"this message will be aborted".to_vec());

    conn.send_frame(msg3).await?;
    println!("Sent message in transaction {} (will be aborted)", tx_id_2);

    // Abort the transaction (message will not be delivered)
    conn.abort(tx_id_2).await?;
    println!("Transaction {} aborted", tx_id_2);

    // Close the connection
    conn.close().await?;

    Ok(())
}
