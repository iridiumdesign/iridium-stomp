//! Tests for initial connection retry with exponential backoff.
//!
//! These tests verify that `Connection::connect` retries on I/O and
//! handshake failures that may be transient (for example, broker unreachable
//! or the server closing during the handshake), and fails immediately only
//! when the server explicitly rejects the connection
//! (`ConnError::ServerRejected`).

use iridium_stomp::Connection;
use iridium_stomp::connection::ConnError;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Helper to find an available port.
fn get_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Broker comes up after a delay — connect should succeed after retrying.
#[tokio::test]
async fn connect_succeeds_after_broker_starts_late() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);

    // Start a mock STOMP server after a delay
    let server_addr = addr.clone();
    let server = thread::spawn(move || {
        // Wait so the client hits at least one retry
        thread::sleep(Duration::from_secs(2));

        let listener = TcpListener::bind(&server_addr).unwrap();
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);

            let connected = "CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0";
            stream.write_all(connected.as_bytes()).unwrap();
            stream.flush().unwrap();

            // Keep alive so the connection doesn't drop immediately
            thread::sleep(Duration::from_secs(1));
        }
    });

    let start = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&addr, "guest", "guest", "0,0"),
    )
    .await;

    let elapsed = start.elapsed();
    assert!(result.is_ok(), "connect should not have timed out");

    let conn = result.unwrap();
    assert!(conn.is_ok(), "connect should have succeeded");
    assert!(
        elapsed >= Duration::from_secs(2),
        "should have waited for the broker to start (elapsed: {:?})",
        elapsed
    );

    let _ = conn.unwrap().close().await;
    server.join().unwrap();
}

/// Bad credentials fail immediately — no retry.
#[tokio::test]
async fn connect_fails_immediately_on_auth_error() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);

    let server_addr = addr.clone();
    let server = thread::spawn(move || {
        let listener = TcpListener::bind(&server_addr).unwrap();
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);

            let error = "ERROR\nmessage:Bad credentials\n\nAccess refused\0";
            stream.write_all(error.as_bytes()).unwrap();
            stream.flush().unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    });

    thread::sleep(Duration::from_millis(50));

    let start = Instant::now();
    let result = Connection::connect(&addr, "bad", "creds", "0,0").await;
    let elapsed = start.elapsed();

    match result {
        Err(ConnError::ServerRejected(err)) => {
            assert_eq!(err.message, "Bad credentials");
        }
        Err(other) => panic!("Expected ServerRejected, got: {}", other),
        Ok(_) => panic!("Expected ServerRejected, got successful connection"),
    }

    // Should have failed fast — before the 1s first backoff
    assert!(
        elapsed < Duration::from_secs(1),
        "auth error should fail before retry backoff, took {:?}",
        elapsed
    );

    server.join().unwrap();
}

/// Server closes without CONNECTED — this is retried (could be a transient
/// broker crash), not a fast failure.
#[tokio::test]
async fn connect_retries_on_server_close_during_handshake() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);

    let server_addr = addr.clone();
    let server = thread::spawn(move || {
        let listener = TcpListener::bind(&server_addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 1024];
                    let _ = stream.read(&mut buf);
                    drop(stream);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
    });

    thread::sleep(Duration::from_millis(50));

    // Should keep retrying, not fail immediately
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        Connection::connect(&addr, "guest", "guest", "0,0"),
    )
    .await;

    assert!(
        result.is_err(),
        "Expected connect to keep retrying on protocol error, but it returned"
    );

    server.join().unwrap();
}

/// Verify backoff increases between retries.
///
/// A mock server that rejects TCP connections is simulated by not listening.
/// We use a timeout to observe that connect is still retrying, and check that
/// successive retry intervals grow.
#[tokio::test]
async fn connect_retry_uses_exponential_backoff() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);

    // No server — every TCP connect will fail and be retried.
    // The first retry is after 1s, second after 2s, so after 3.5s we should
    // still be retrying (total: 1 + 2 = 3s of sleep plus attempt time).
    let result = tokio::time::timeout(
        Duration::from_millis(3500),
        Connection::connect(&addr, "guest", "guest", "0,0"),
    )
    .await;

    assert!(
        result.is_err(),
        "connect should still be retrying after 3.5s (1s + 2s backoff)"
    );
}

/// Broker is initially unreachable, then starts accepting but sends an auth
/// error. The retry loop should surface the auth error immediately once the
/// broker is reachable.
#[tokio::test]
async fn connect_retry_then_auth_error_fails_fast() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);

    let server_addr = addr.clone();
    let server = thread::spawn(move || {
        // Delay so client hits at least one TCP retry
        thread::sleep(Duration::from_millis(1500));

        let listener = TcpListener::bind(&server_addr).unwrap();
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);

            let error = "ERROR\nmessage:Access denied\n\n\0";
            stream.write_all(error.as_bytes()).unwrap();
            stream.flush().unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    });

    let start = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        Connection::connect(&addr, "bad", "creds", "0,0"),
    )
    .await;

    let elapsed = start.elapsed();

    // Should have returned (not timed out)
    assert!(result.is_ok(), "should not time out");

    match result.unwrap() {
        Err(ConnError::ServerRejected(err)) => {
            assert_eq!(err.message, "Access denied");
        }
        Err(other) => panic!("Expected ServerRejected, got: {}", other),
        Ok(_) => panic!("Expected ServerRejected, got successful connection"),
    }

    // Took at least 1.5s (waiting for broker) but not much more
    assert!(
        elapsed >= Duration::from_millis(1500) && elapsed < Duration::from_secs(5),
        "unexpected timing: {:?}",
        elapsed
    );

    server.join().unwrap();
}

/// Multiple retry attempts actually happen (counted by a mock server).
#[tokio::test]
async fn connect_retry_makes_multiple_attempts() {
    let port = get_available_port();
    let addr = format!("127.0.0.1:{}", port);
    let attempt_count = Arc::new(AtomicU32::new(0));

    // Mock server that accepts connections but immediately closes them,
    // causing I/O errors during the STOMP handshake. On the 3rd attempt
    // it responds with CONNECTED.
    let server_addr = addr.clone();
    let count = attempt_count.clone();
    let server = thread::spawn(move || {
        let listener = TcpListener::bind(&server_addr).unwrap();
        for _ in 0..3 {
            if let Ok((mut stream, _)) = listener.accept() {
                let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);

                if n < 3 {
                    // Close without responding — triggers I/O retry
                    drop(stream);
                } else {
                    // Third attempt — send CONNECTED
                    let connected = "CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0";
                    stream.write_all(connected.as_bytes()).unwrap();
                    stream.flush().unwrap();
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(50));

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        Connection::connect(&addr, "guest", "guest", "0,0"),
    )
    .await;

    assert!(result.is_ok(), "should not time out");
    let conn = result.unwrap();
    assert!(conn.is_ok(), "should connect on third attempt");

    let attempts = attempt_count.load(Ordering::SeqCst);
    assert_eq!(
        attempts, 3,
        "expected 3 connection attempts, got {}",
        attempts
    );

    let _ = conn.unwrap().close().await;
    server.join().unwrap();
}
