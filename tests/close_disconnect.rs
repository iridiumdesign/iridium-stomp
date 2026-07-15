//! Coverage for #81: `Connection::close` performs the STOMP 1.2 shutdown
//! sequence - DISCONNECT with a receipt, await the RECEIPT, then close.

use iridium_stomp::{ConnError, ConnectOptions, Connection, Frame};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

/// Records the commands a broker saw, in order.
type Seen = Arc<Mutex<Vec<String>>>;

/// A broker that answers CONNECT, records every frame command it receives, and
/// optionally answers a DISCONNECT with its RECEIPT.
///
/// `answer_disconnect: false` models a broker that goes silent, so `close` has
/// to fall back on its timeout.
fn start_broker(answer_disconnect: bool) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();

    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut buf = [0u8; 4096];

        // CONNECT -> CONNECTED
        if stream.read(&mut buf).is_ok() {
            let _ = stream.write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0");
            let _ = stream.flush();
        }

        loop {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buf[..n]).to_string();
                    // A read may carry more than one frame; they are NUL separated.
                    for raw in text.split('\0').filter(|f| !f.trim().is_empty()) {
                        let command = raw.trim_start().lines().next().unwrap_or("").to_string();
                        seen_clone.lock().unwrap().push(command.clone());

                        if command == "DISCONNECT"
                            && answer_disconnect
                            && let Some(id) = receipt_id_of(raw)
                        {
                            let frame = format!("RECEIPT\nreceipt-id:{}\n\n\0", id);
                            let _ = stream.write_all(frame.as_bytes());
                            let _ = stream.flush();
                        }
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(200));
    });

    (addr, seen)
}

/// Pull the `receipt` header out of a raw frame, the way a broker would.
fn receipt_id_of(frame: &str) -> Option<&str> {
    frame.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case("receipt").then_some(value)
    })
}

fn commands(seen: &Seen) -> Vec<String> {
    seen.lock().unwrap().clone()
}

#[tokio::test]
async fn close_sends_disconnect_carrying_a_receipt_and_awaits_it() {
    let (addr, seen) = start_broker(true);

    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();

    conn.close()
        .await
        .expect("broker confirmed the DISCONNECT, so close should report a clean shutdown");

    // Give the broker thread a moment to have recorded it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        commands(&seen),
        vec!["DISCONNECT"],
        "close must send a DISCONNECT frame, not just drop the socket"
    );
}

#[tokio::test]
async fn close_drains_queued_frames_before_disconnecting() {
    // The reason the CLI could lose a `send` issued just before `quit`: frames
    // are queued for a writer task, and the old close signalled shutdown without
    // waiting for the queue to drain.
    let (addr, seen) = start_broker(true);

    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();

    conn.send_frame(
        Frame::new("SEND")
            .header("destination", "/queue/orders")
            .set_body(b"payload".to_vec()),
    )
    .await
    .unwrap();

    // No await between the send and the close: the frame is still only queued.
    conn.close().await.expect("clean shutdown");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        commands(&seen),
        vec!["SEND", "DISCONNECT"],
        "a frame queued before close must reach the broker, ahead of the DISCONNECT"
    );
}

#[tokio::test]
async fn close_reports_timeout_when_the_broker_never_confirms() {
    let (addr, seen) = start_broker(false);

    let conn = Connection::connect_with_options(
        &addr,
        "guest",
        "guest",
        "0,0",
        ConnectOptions::default().disconnect_timeout(Duration::from_millis(300)),
    )
    .await
    .unwrap();

    let started = Instant::now();
    let result = conn.close().await;
    let elapsed = started.elapsed();

    match result {
        Err(ConnError::ReceiptTimeout(_)) => {}
        other => panic!(
            "expected ReceiptTimeout from a silent broker, got {:?}",
            other
        ),
    }

    assert!(
        elapsed < Duration::from_secs(2),
        "close must give up at the configured timeout rather than hang (took {:?})",
        elapsed
    );

    // It still sent the DISCONNECT; the broker simply never answered.
    assert_eq!(commands(&seen), vec!["DISCONNECT"]);
}

#[tokio::test]
async fn disconnect_timeout_option_is_honoured() {
    let (addr, _seen) = start_broker(false);

    let conn = Connection::connect_with_options(
        &addr,
        "guest",
        "guest",
        "0,0",
        ConnectOptions::default().disconnect_timeout(Duration::from_millis(150)),
    )
    .await
    .unwrap();

    let started = Instant::now();
    let _ = conn.close().await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(150),
        "close returned before the configured timeout ({:?})",
        elapsed
    );
    assert!(
        elapsed < Connection::DEFAULT_DISCONNECT_TIMEOUT,
        "close waited the default rather than the configured timeout ({:?})",
        elapsed
    );
}
