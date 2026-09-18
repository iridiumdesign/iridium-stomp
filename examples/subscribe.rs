use futures::StreamExt;
use iridium_stomp::{AckMode, Connection, SubscriptionOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This example expects a STOMP broker on localhost:61613 (e.g. RabbitMQ with stomp plugin).
    // Start a local broker before running: `docker compose up -d`

    // Connect with a heartbeat request (client_out=10000ms, client_in=10000ms)
    let conn = Connection::connect(
        "127.0.0.1:61613",
        "guest",
        "guest",
        Connection::DEFAULT_HEARTBEAT,
    )
    .await?;

    // Subscribe to a queue using client ack mode (so we must ack messages).
    // `SubscriptionOptions` carries broker-specific headers; durability is
    // requested through them, for example ActiveMQ's durable subscription
    // headers. On brokers where the durable queue is declared
    // administratively, such as RabbitMQ, name it as the destination and no
    // extra headers are needed.
    let opts = SubscriptionOptions::default();

    let mut sub = conn
        .subscribe_with_options("/queue/example-durable", AckMode::Client, opts)
        .await?;

    println!("subscribed id={} dest={}", sub.id(), sub.destination());

    // Use the Subscription as a Stream directly (we implemented Stream for Subscription)
    while let Some(frame) = sub.next().await {
        println!("received frame:\n{}", frame);

        // Acknowledge by frame: STOMP 1.2 wants the MESSAGE's `ack` header in
        // the ACK, 1.1 its `message-id`, and `ack_frame` picks the right one.
        sub.ack_frame(&frame).await?;
        println!(
            "acked message-id={}",
            frame.get_header("message-id").unwrap_or("")
        );
    }

    // Explicitly unsubscribe when done (consumes the subscription)
    sub.unsubscribe().await?;

    // close the connection
    conn.close().await?;
    Ok(())
}
