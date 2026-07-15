// multi_subscribe.rs
//
// Connects to a STOMP broker, subscribes to multiple destinations, and prints
// incoming messages. Demonstrates:
//
//   - Merging multiple subscriptions into a single stream with select_all
//   - Per-message ACK with ClientIndividual mode
//   - Monitoring broker ERROR frames on a separate task
//   - Graceful shutdown on Ctrl+C
//
// Start a local broker before running:
//   docker compose up -d
//
// Then run:
//   cargo run --example multi_subscribe

use futures::{StreamExt, stream};
use iridium_stomp::AckMode;
use iridium_stomp::{Connection, ReceivedFrame};
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:61613";
    let login = "guest";
    let pass = "guest";
    let destinations = vec!["/queue/orders", "/queue/notifications"];

    println!("Connecting to {}...", addr);
    let conn = Connection::connect(addr, login, pass, Connection::DEFAULT_HEARTBEAT).await?;
    println!("Connected.");

    // Subscribe to all destinations and merge into a single stream.
    let mut subs = Vec::new();
    for dest in &destinations {
        let sub = conn.subscribe(dest, AckMode::ClientIndividual).await?;
        println!("Subscribed (id={}) -> {}", sub.id(), dest);
        subs.push(sub);
    }
    let mut merged = stream::select_all(subs);

    // Spawn a task to watch for broker ERROR frames. These are not delivered
    // through the subscription stream — they must be read from conn.next_frame().
    let conn_errors = conn.clone();
    tokio::spawn(async move {
        while let Some(received) = conn_errors.next_frame().await {
            if let ReceivedFrame::Error(err) = received {
                if err.frame.get_header("x-abandoned").is_some() {
                    eprintln!(
                        "Subscription abandoned after repeated errors: {}",
                        err.message
                    );
                } else {
                    eprintln!("Broker error: {}", err.message);
                }
            }
        }
    });

    println!("Waiting for messages. Press Ctrl+C to exit.");

    tokio::select! {
        _ = async {
            while let Some(frame) = merged.next().await {
                let dest = frame.headers.iter()
                    .find(|(k, _)| k.to_lowercase() == "destination")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("unknown");
                let body = std::str::from_utf8(&frame.body).unwrap_or("<binary>");
                println!("[{}] {}", dest, body);

                let sub_id = frame.headers.iter()
                    .find(|(k, _)| k.to_lowercase() == "subscription")
                    .map(|(_, v)| v.clone());
                let msg_id = frame.headers.iter()
                    .find(|(k, _)| k.to_lowercase() == "message-id")
                    .map(|(_, v)| v.clone());

                if let (Some(sub_id), Some(msg_id)) = (sub_id, msg_id)
                    && let Err(e) = conn.ack(&sub_id, &msg_id).await
                {
                    eprintln!("ACK failed: {}", e);
                }
            }
        } => {}
        _ = signal::ctrl_c() => {
            println!("Shutting down...");
        }
    }

    conn.close().await?;
    Ok(())
}
