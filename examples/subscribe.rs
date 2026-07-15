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
    let opts = SubscriptionOptions { headers: vec![] };

    let mut sub = conn
        .subscribe_with_options("/queue/example-durable", AckMode::Client, opts)
        .await?;

    println!("subscribed id={} dest={}", sub.id(), sub.destination());

    // Use the Subscription as a Stream directly (we implemented Stream for Subscription)
    while let Some(frame) = sub.next().await {
        println!("received frame:\n{}", frame);

        // read message-id header for ACK
        let msg_id = frame
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("message-id"))
            .map(|(_, v)| v.to_string());

        if let Some(id) = msg_id {
            // Acknowledge the message via the subscription convenience method
            sub.ack(&id).await?;
            println!("acked message-id={}", id);
        }
    }

    // Explicitly unsubscribe when done (consumes the subscription)
    sub.unsubscribe().await?;

    // close the connection
    conn.close().await?;
    Ok(())
}
