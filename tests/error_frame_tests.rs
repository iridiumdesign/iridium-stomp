//! Tests for ERROR frame handling (Issue #35)
//!
//! These tests verify:
//! - ServerError struct creation and fields
//! - ReceivedFrame enum variants and methods
//! - Display and Error trait implementations

use iridium_stomp::{Frame, ReceivedFrame, ServerError};

// ============================================================================
// ServerError tests
// ============================================================================

#[test]
fn server_error_from_frame_basic() {
    let frame = Frame::new("ERROR")
        .header("message", "malformed frame received")
        .header("content-type", "text/plain");

    let err = ServerError::from_frame(frame);

    assert_eq!(err.message, "malformed frame received");
    assert!(err.body.is_none());
    assert!(err.receipt_id.is_none());
}

#[test]
fn server_error_from_frame_with_body() {
    let frame = Frame::new("ERROR")
        .header("message", "authentication failed")
        .set_body(b"Invalid credentials provided".to_vec());

    let err = ServerError::from_frame(frame);

    assert_eq!(err.message, "authentication failed");
    assert_eq!(err.body, Some("Invalid credentials provided".to_string()));
}

#[test]
fn server_error_from_frame_with_receipt_id() {
    let frame = Frame::new("ERROR")
        .header("message", "invalid destination")
        .header("receipt-id", "msg-12345");

    let err = ServerError::from_frame(frame);

    assert_eq!(err.message, "invalid destination");
    assert_eq!(err.receipt_id, Some("msg-12345".to_string()));
}

#[test]
fn server_error_from_frame_no_message_header() {
    let frame = Frame::new("ERROR");

    let err = ServerError::from_frame(frame);

    assert_eq!(err.message, "unknown error");
}

#[test]
fn server_error_from_frame_preserves_original() {
    let frame = Frame::new("ERROR")
        .header("message", "test error")
        .header("custom-header", "custom-value")
        .set_body(b"body content".to_vec());

    let err = ServerError::from_frame(frame);

    // Original frame should be accessible
    assert_eq!(err.frame.command, "ERROR");
    assert_eq!(err.frame.get_header("custom-header"), Some("custom-value"));
}

#[test]
fn server_error_display_basic() {
    let frame = Frame::new("ERROR").header("message", "connection refused");

    let err = ServerError::from_frame(frame);
    let display = format!("{}", err);

    assert!(display.contains("STOMP server error"));
    assert!(display.contains("connection refused"));
}

#[test]
fn server_error_display_with_body() {
    let frame = Frame::new("ERROR")
        .header("message", "protocol error")
        .set_body(b"details here".to_vec());

    let err = ServerError::from_frame(frame);
    let display = format!("{}", err);

    assert!(display.contains("protocol error"));
    assert!(display.contains("details here"));
}

#[test]
fn server_error_debug() {
    let frame = Frame::new("ERROR").header("message", "test");

    let err = ServerError::from_frame(frame);
    let debug = format!("{:?}", err);

    assert!(debug.contains("ServerError"));
    assert!(debug.contains("test"));
}

#[test]
fn server_error_is_error_trait() {
    let frame = Frame::new("ERROR").header("message", "test");
    let err = ServerError::from_frame(frame);

    // Verify it implements std::error::Error
    let _: &dyn std::error::Error = &err;
}

#[test]
fn server_error_clone() {
    let frame = Frame::new("ERROR")
        .header("message", "test")
        .set_body(b"body".to_vec());

    let err1 = ServerError::from_frame(frame);
    let err2 = err1.clone();

    assert_eq!(err1.message, err2.message);
    assert_eq!(err1.body, err2.body);
}

#[test]
fn server_error_eq() {
    let frame1 = Frame::new("ERROR").header("message", "test");
    let frame2 = Frame::new("ERROR").header("message", "test");

    let err1 = ServerError::from_frame(frame1);
    let err2 = ServerError::from_frame(frame2);

    assert_eq!(err1, err2);
}

// ============================================================================
// ReceivedFrame tests
// ============================================================================

#[test]
fn received_frame_is_error() {
    let frame = Frame::new("ERROR").header("message", "test");
    let err = ServerError::from_frame(frame);
    let received = ReceivedFrame::Error(err);

    assert!(received.is_error());
    assert!(!received.is_frame());
}

#[test]
fn received_frame_is_frame() {
    let frame = Frame::new("MESSAGE")
        .header("destination", "/queue/test")
        .set_body(b"hello".to_vec());
    let received = ReceivedFrame::Frame(frame);

    assert!(received.is_frame());
    assert!(!received.is_error());
}

#[test]
fn received_frame_into_frame_success() {
    let frame = Frame::new("MESSAGE").header("destination", "/queue/test");
    let received = ReceivedFrame::Frame(frame);

    let result = received.into_frame();
    assert!(result.is_some());
    assert_eq!(result.unwrap().command, "MESSAGE");
}

#[test]
fn received_frame_into_frame_from_error() {
    let frame = Frame::new("ERROR").header("message", "test");
    let err = ServerError::from_frame(frame);
    let received = ReceivedFrame::Error(err);

    let result = received.into_frame();
    assert!(result.is_none());
}

#[test]
fn received_frame_into_error_success() {
    let frame = Frame::new("ERROR").header("message", "test error");
    let err = ServerError::from_frame(frame);
    let received = ReceivedFrame::Error(err);

    let result = received.into_error();
    assert!(result.is_some());
    assert_eq!(result.unwrap().message, "test error");
}

#[test]
fn received_frame_into_error_from_frame() {
    let frame = Frame::new("MESSAGE").header("destination", "/queue/test");
    let received = ReceivedFrame::Frame(frame);

    let result = received.into_error();
    assert!(result.is_none());
}

#[test]
fn received_frame_debug() {
    let frame = Frame::new("MESSAGE").header("destination", "/queue/test");
    let received = ReceivedFrame::Frame(frame);

    let debug = format!("{:?}", received);
    assert!(debug.contains("Frame"));
    assert!(debug.contains("MESSAGE"));
}

#[test]
fn received_frame_clone() {
    let frame = Frame::new("MESSAGE")
        .header("destination", "/queue/test")
        .set_body(b"data".to_vec());
    let received1 = ReceivedFrame::Frame(frame);
    let received2 = received1.clone();

    assert_eq!(received1, received2);
}

#[test]
fn received_frame_eq_frames() {
    let frame1 = Frame::new("MESSAGE").header("destination", "/queue/test");
    let frame2 = Frame::new("MESSAGE").header("destination", "/queue/test");

    let received1 = ReceivedFrame::Frame(frame1);
    let received2 = ReceivedFrame::Frame(frame2);

    assert_eq!(received1, received2);
}

#[test]
fn received_frame_eq_errors() {
    let frame1 = Frame::new("ERROR").header("message", "test");
    let frame2 = Frame::new("ERROR").header("message", "test");

    let received1 = ReceivedFrame::Error(ServerError::from_frame(frame1));
    let received2 = ReceivedFrame::Error(ServerError::from_frame(frame2));

    assert_eq!(received1, received2);
}

#[test]
fn received_frame_ne_different_types() {
    let msg_frame = Frame::new("MESSAGE").header("destination", "/queue/test");
    let err_frame = Frame::new("ERROR").header("message", "test");

    let received1 = ReceivedFrame::Frame(msg_frame);
    let received2 = ReceivedFrame::Error(ServerError::from_frame(err_frame));

    assert_ne!(received1, received2);
}

// ============================================================================
// Pattern matching tests (ergonomics)
// ============================================================================

#[test]
fn pattern_match_frame() {
    let frame = Frame::new("MESSAGE")
        .header("destination", "/queue/test")
        .set_body(b"hello".to_vec());
    let received = ReceivedFrame::Frame(frame);

    match received {
        ReceivedFrame::Frame(f) => {
            assert_eq!(f.command, "MESSAGE");
            assert_eq!(f.get_header("destination"), Some("/queue/test"));
        }
        other => panic!("Expected Frame, got {:?}", other),
    }
}

#[test]
fn pattern_match_error() {
    let frame = Frame::new("ERROR")
        .header("message", "authentication failed")
        .set_body(b"Bad credentials".to_vec());
    let err = ServerError::from_frame(frame);
    let received = ReceivedFrame::Error(err);

    match received {
        ReceivedFrame::Frame(_) => panic!("Expected Error, got Frame"),
        ReceivedFrame::Error(e) => {
            assert_eq!(e.message, "authentication failed");
            assert_eq!(e.body, Some("Bad credentials".to_string()));
        }
        other => panic!("Expected Error, got {:?}", other),
    }
}

// ============================================================================
// Binary body handling
// ============================================================================

#[test]
fn server_error_binary_body_returns_none() {
    // If the body contains invalid UTF-8, body should be None
    let frame = Frame::new("ERROR")
        .header("message", "binary error")
        .set_body(vec![0xFF, 0xFE, 0x00, 0x01]); // Invalid UTF-8

    let err = ServerError::from_frame(frame);

    assert_eq!(err.message, "binary error");
    assert!(err.body.is_none()); // Can't convert to string
}
