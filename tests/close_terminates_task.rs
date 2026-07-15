//! Regression coverage for #96: `Connection::close` must terminate the
//! background task rather than leaving it reconnecting on a backoff loop.
//!
//! The stubs here deliberately keep accepting connections after the client
//! closes. The stubs used elsewhere accept exactly one, which is why a
//! reconnect after close went unnoticed - it failed against a closed listener
//! and the test process exited before anyone looked.

use iridium_stomp::Connection;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

/// A broker that stays up and counts every connection it accepts. It answers
/// CONNECT with CONNECTED, and answers a DISCONNECT with its RECEIPT as a real
/// broker does, so `close` does not have to wait out its receipt timeout here.
fn start_counting_broker() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_clone = accepts.clone();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            accepts_clone.fetch_add(1, Ordering::SeqCst);
            thread::spawn(move || {
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
                            if !text.starts_with("DISCONNECT") {
                                continue;
                            }
                            if let Some(id) = receipt_id_of(&text) {
                                let frame = format!("RECEIPT\nreceipt-id:{}\n\n\0", id);
                                let _ = stream.write_all(frame.as_bytes());
                                let _ = stream.flush();
                            }
                        }
                    }
                }
            });
        }
    });

    (addr, accepts)
}

/// Pull the `receipt` header out of a raw frame.
fn receipt_id_of(frame: &str) -> Option<&str> {
    frame.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case("receipt").then_some(value)
    })
}

/// Backoff starts at 1s and doubles to 2s for a short-lived connection, so a
/// stray reconnect lands ~2s after close. Wait well past that.
const OBSERVE: Duration = Duration::from_secs(6);

/// The task consumed the shutdown broadcast in its inner select, and the
/// reconnect check then re-read the drained receiver and saw nothing.
#[tokio::test]
async fn close_terminates_the_background_task() {
    let (addr, accepts) = start_counting_broker();

    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the initial connect should be the broker's only session"
    );

    // Give the task a chance to be polled, so it is parked in its select with
    // the shutdown receiver live. This is the ordinary case.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let _ = conn.close().await;
    tokio::time::sleep(OBSERVE).await;

    let total = accepts.load(Ordering::SeqCst);
    assert_eq!(
        total, 1,
        "close() must end the background task, but the broker saw {} sessions",
        total
    );
}

/// The connect handshake happens before the task is spawned, so a close with no
/// await points in between reaches a broadcast with no subscribers. The signal
/// was dropped outright and the task reconnected forever.
#[tokio::test]
async fn close_immediately_after_connect_terminates_the_background_task() {
    let (addr, accepts) = start_counting_broker();

    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the initial connect should be the broker's only session"
    );

    // No await between connect returning and close: the task may not have been
    // polled even once yet.
    let _ = conn.close().await;
    tokio::time::sleep(OBSERVE).await;

    let total = accepts.load(Ordering::SeqCst);
    assert_eq!(
        total, 1,
        "close() must end the background task even when it has not been polled yet, \
         but the broker saw {} sessions",
        total
    );
}

/// `Connection` is `Clone`, and the CLI keeps clones alive for its subscriber
/// and error-monitor tasks. With a clone outstanding, `out_tx` is not dropped by
/// `close`, so the outbound branch of the task's select is never ready and the
/// shutdown branch wins on its own - draining the broadcast before the
/// reconnect check reads it.
#[tokio::test]
async fn close_terminates_the_background_task_with_a_clone_outstanding() {
    let (addr, accepts) = start_counting_broker();

    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();
    assert_eq!(accepts.load(Ordering::SeqCst), 1);

    // Held for the rest of the test, as the CLI holds its clones.
    let _clone = conn.clone();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let _ = conn.close().await;
    tokio::time::sleep(OBSERVE).await;

    let total = accepts.load(Ordering::SeqCst);
    assert_eq!(
        total, 1,
        "close() must end the background task while a clone is alive, \
         but the broker saw {} sessions",
        total
    );
}
