//! Does an ACK take? Sends one message, receives it with `client-individual`
//! ack, prints its `message-id` and `ack` headers, acknowledges it, closes the
//! connection, then subscribes again and reports whether the broker
//! redelivers it. A redelivery means the ACK was ignored (#119).
//!
//! ```sh
//! cargo run --example ack_roundtrip -- 127.0.0.1:61613 guest guest
//! cargo run --example ack_roundtrip -- 127.0.0.1:61614 admin admin --by ack
//! ```
//!
//! `--by` chooses what `ack()` is given: `message-id` (the default, the form
//! the examples used to teach), `ack` (the header STOMP 1.2 wants), or
//! `frame` (`ack_frame`, which picks for itself). Exit code 0 when the message
//! was not redelivered, 1 when it was, 2 when it never arrived at all.

use futures::StreamExt;
use iridium_stomp::{AckMode, Connection, Frame, ReceivedFrame, Subscription};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const USAGE: &str = "usage: ack_roundtrip <addr> <login> <passcode> \
                     [--by message-id|ack|frame] [--dest <destination>]";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    let (addr, login, passcode) = (&args[0], &args[1], &args[2]);
    let mut by = "message-id".to_string();
    let mut dest = "/queue/iridium-ack-roundtrip".to_string();
    let mut rest = args[3..].iter();
    while let Some(flag) = rest.next() {
        match (flag.as_str(), rest.next()) {
            ("--by", Some(v)) if ["message-id", "ack", "frame"].contains(&v.as_str()) => {
                by = v.clone()
            }
            ("--dest", Some(v)) => dest = v.clone(),
            _ => {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
        }
    }

    // A body nobody else will have sent, so leftovers on the queue from an
    // earlier run cannot be mistaken for this one.
    let body = format!(
        "ack-roundtrip {} {}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );

    // Session 1: send, receive, acknowledge, close.
    let conn = Connection::connect(addr, login, passcode, "0,0").await?;
    let mut sub = conn.subscribe(&dest, AckMode::ClientIndividual).await?;
    conn.send_frame_confirmed(
        Frame::new("SEND")
            .header("destination", &dest)
            .set_body(body.clone().into_bytes()),
        Duration::from_secs(5),
    )
    .await?;

    let frame = match wait_for(&mut sub, &body).await {
        Some(f) => f,
        None => {
            eprintln!("the message never arrived on {dest}");
            std::process::exit(2);
        }
    };
    let message_id = frame.get_header("message-id").unwrap_or("<none>");
    let ack = frame.get_header("ack").unwrap_or("<none>");
    println!("message-id: {message_id}");
    println!("ack:        {ack}");
    println!(
        "same:       {}",
        if message_id == ack { "yes" } else { "no" }
    );

    match by.as_str() {
        "message-id" => sub.ack(message_id).await?,
        "ack" => sub.ack(ack).await?,
        _ => sub.ack_frame(&frame).await?,
    }
    println!("acked by:   {by}");

    // Give the broker a moment to object. Classic never does, which is the
    // point of the reconnect below.
    match tokio::time::timeout(Duration::from_secs(1), conn.next_frame()).await {
        Ok(Some(ReceivedFrame::Error(err))) => println!("broker ERROR: {}", err.message),
        _ => println!("broker ERROR: none within 1s"),
    }
    // A confirmed DISCONNECT: the ACK was processed before the RECEIPT.
    conn.close().await?;

    // Session 2: is it still there?
    let conn = Connection::connect(addr, login, passcode, "0,0").await?;
    let mut sub = conn.subscribe(&dest, AckMode::ClientIndividual).await?;
    let outcome = match wait_for(&mut sub, &body).await {
        Some(again) => {
            println!(
                "REDELIVERED after reconnect (redelivered: {}): the ACK was ignored",
                again.get_header("redelivered").unwrap_or("<none>")
            );
            // Do not leave it behind for the next run.
            sub.ack_frame(&again).await?;
            1
        }
        None => {
            println!("not redelivered within 3s: the ACK took");
            0
        }
    };
    conn.close().await?;
    std::process::exit(outcome);
}

/// The next frame carrying `body`, acknowledging and skipping any other
/// message found on the way (leftovers from earlier runs), or `None` after
/// three seconds.
async fn wait_for(sub: &mut Subscription, body: &str) -> Option<Frame> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let frame = tokio::time::timeout_at(deadline, sub.next()).await.ok()??;
        if frame.body == body.as_bytes() {
            return Some(frame);
        }
        let _ = sub.ack_frame(&frame).await;
    }
}
