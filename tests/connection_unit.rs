//! Unit tests for connection-related types.
//!
//! Note: Full Connection testing requires network I/O and is covered by
//! inline tests in connection.rs and the integration smoke test. This file
//! tests the public types and error handling.

use iridium_stomp::connection::{AckMode, ConnError};
use std::io;

// =============================================================================
// AckMode Tests
// =============================================================================

#[test]
fn ack_mode_debug() {
    assert!(format!("{:?}", AckMode::Auto).contains("Auto"));
    assert!(format!("{:?}", AckMode::Client).contains("Client"));
    assert!(format!("{:?}", AckMode::ClientIndividual).contains("ClientIndividual"));
}

#[test]
fn ack_mode_copy() {
    let mode = AckMode::Client;
    let copied = mode; // AckMode is Copy
    assert_eq!(mode, copied);
}

#[test]
fn ack_mode_eq() {
    assert_eq!(AckMode::Auto, AckMode::Auto);
    assert_eq!(AckMode::Client, AckMode::Client);
    assert_eq!(AckMode::ClientIndividual, AckMode::ClientIndividual);
}

#[test]
fn ack_mode_ne() {
    assert_ne!(AckMode::Auto, AckMode::Client);
    assert_ne!(AckMode::Client, AckMode::ClientIndividual);
    assert_ne!(AckMode::Auto, AckMode::ClientIndividual);
}

// =============================================================================
// ConnError Tests
// =============================================================================

#[test]
fn conn_error_io_display() {
    let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused");
    let conn_err = ConnError::Io(io_err);
    let display = format!("{}", conn_err);
    assert!(display.contains("io error"));
    assert!(display.contains("connection refused"));
}

#[test]
fn conn_error_protocol_display() {
    let conn_err = ConnError::Protocol("invalid frame".to_string());
    let display = format!("{}", conn_err);
    assert!(display.contains("protocol error"));
    assert!(display.contains("invalid frame"));
}

#[test]
fn conn_error_io_from() {
    let io_err = io::Error::new(io::ErrorKind::TimedOut, "timeout");
    let conn_err: ConnError = io_err.into();
    match conn_err {
        ConnError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
        _ => panic!("expected Io variant"),
    }
}

#[test]
fn conn_error_debug() {
    let conn_err = ConnError::Protocol("test error".to_string());
    let debug = format!("{:?}", conn_err);
    assert!(debug.contains("Protocol"));
    assert!(debug.contains("test error"));
}

#[test]
fn conn_error_is_error_trait() {
    // Verify ConnError implements std::error::Error
    fn assert_error<E: std::error::Error>() {}
    assert_error::<ConnError>();
}

#[test]
fn conn_error_receipt_timeout_display() {
    let conn_err = ConnError::ReceiptTimeout("msg-123".to_string());
    let display = format!("{}", conn_err);
    assert!(display.contains("receipt timeout"));
    assert!(display.contains("msg-123"));
}

// =============================================================================
// ConnError::ServerRejected Tests
// =============================================================================

#[test]
fn conn_error_server_rejected_display() {
    use iridium_stomp::{Frame, ServerError};

    let frame = Frame::new("ERROR")
        .header("message", "authentication failed")
        .set_body(b"Invalid credentials".to_vec());
    let server_err = ServerError::from_frame(frame);
    let conn_err = ConnError::ServerRejected(Box::new(server_err));

    let display = format!("{}", conn_err);
    assert!(display.contains("server rejected"));
    assert!(display.contains("authentication failed"));
}

#[test]
fn conn_error_server_rejected_debug() {
    use iridium_stomp::{Frame, ServerError};

    let frame = Frame::new("ERROR").header("message", "access denied");
    let server_err = ServerError::from_frame(frame);
    let conn_err = ConnError::ServerRejected(Box::new(server_err));

    let debug = format!("{:?}", conn_err);
    assert!(debug.contains("ServerRejected"));
    assert!(debug.contains("access denied"));
}

#[test]
fn conn_error_server_rejected_extract_error() {
    use iridium_stomp::{Frame, ServerError};

    let frame = Frame::new("ERROR")
        .header("message", "not authorized")
        .set_body(b"You do not have permission".to_vec());
    let server_err = ServerError::from_frame(frame);
    let conn_err = ConnError::ServerRejected(Box::new(server_err));

    // Extract the inner ServerError via pattern matching
    match conn_err {
        ConnError::ServerRejected(e) => {
            assert_eq!(e.message, "not authorized");
            assert_eq!(e.body, Some("You do not have permission".to_string()));
        }
        _ => panic!("expected ServerRejected variant"),
    }
}

#[test]
fn conn_error_server_rejected_preserves_frame() {
    use iridium_stomp::{Frame, ServerError};

    let frame = Frame::new("ERROR")
        .header("message", "error")
        .header("custom-header", "custom-value");
    let server_err = ServerError::from_frame(frame);
    let conn_err = ConnError::ServerRejected(Box::new(server_err));

    // Verify we can access the original frame through the error
    match conn_err {
        ConnError::ServerRejected(e) => {
            assert_eq!(e.frame.get_header("custom-header"), Some("custom-value"));
        }
        _ => panic!("expected ServerRejected variant"),
    }
}
