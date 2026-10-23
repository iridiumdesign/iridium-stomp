//! Tests for connection error handling during CONNECT phase.
//!
//! These tests verify that ERROR frames and connection failures during
//! the STOMP handshake are properly reported to the caller.

use iridium_stomp::Connection;
use iridium_stomp::connection::ConnError;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpListener;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// A listener on a free loopback port, bound before any thread is spawned so
/// the client can never dial ahead of it, and the address to dial.
fn listen() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    (listener, addr)
}

/// An address nothing listens on: a free port, bound and released.
fn closed_port() -> String {
    listen().1
}

/// A broker that answers the first CONNECT with an ERROR frame.
fn refusing_broker() -> (String, JoinHandle<()>) {
    let (listener, addr) = listen();
    let broker = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        // What the CONNECT says does not matter; wait for it, then refuse.
        let _ = socket.read(&mut [0u8; 1024]);
        socket
            .write_all(b"ERROR\nmessage:Authentication failed\n\nInvalid credentials\0")
            .unwrap();
    });
    (addr, broker)
}

/// A broker that, for `lifetime`, hangs up on every client once its CONNECT
/// arrives. The listener polls, so the thread sees the deadline even while
/// nobody is dialing.
fn hang_up_broker(lifetime: Duration) -> (String, JoinHandle<()>) {
    let (listener, addr) = listen();
    listener.set_nonblocking(true).unwrap();
    let broker = thread::spawn(move || {
        let stop_at = Instant::now() + lifetime;
        while Instant::now() < stop_at {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    // Some platforms hand back the listener's non-blocking
                    // mode; block so the read really waits for the CONNECT.
                    socket.set_nonblocking(false).unwrap();
                    let _ = socket.read(&mut [0u8; 1024]);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        }
    });
    (addr, broker)
}

/// Test that server sending ERROR frame during CONNECT returns ServerRejected
#[tokio::test]
async fn connect_error_frame_returns_server_rejected() {
    let (addr, server) = refusing_broker();

    // Attempt connection
    let result = Connection::connect(&addr, "user", "wrongpass", "0,0").await;

    // Should get ServerRejected error
    match result {
        Err(ConnError::ServerRejected(err)) => {
            assert_eq!(err.message, "Authentication failed");
            assert_eq!(err.body, Some("Invalid credentials".to_string()));
        }
        Err(other) => panic!("Expected ServerRejected, got: {:?}", other),
        Ok(_) => panic!("Expected error, got successful connection"),
    }

    server.join().unwrap();
}

/// Test that server closing connection before CONNECTED causes a retry
/// (not an immediate failure). Protocol errors during handshake are transient.
#[tokio::test]
async fn connect_closed_before_connected_retries() {
    let (addr, server) = hang_up_broker(Duration::from_secs(2));

    // Should keep retrying, not return an error
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        Connection::connect(&addr, "user", "pass", "0,0"),
    )
    .await;

    assert!(
        result.is_err(),
        "Expected connect to keep retrying when server closes during handshake"
    );

    server.join().unwrap();
}

/// Test that connection refused retries (does not fail immediately).
///
/// With initial connection retry, an unreachable broker causes `connect` to
/// retry with backoff. We verify it does *not* return within a short window,
/// then cancel the attempt.
#[tokio::test]
async fn connect_refused_retries_instead_of_failing() {
    let addr = closed_port();

    // No server listening — connect should retry, not return an error.
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        Connection::connect(&addr, "user", "pass", "0,0"),
    )
    .await;

    // Timeout means connect is still retrying — expected behaviour.
    assert!(
        result.is_err(),
        "Expected connect to keep retrying, but it returned"
    );
}
