//! Unit tests for the Frame struct.

use iridium_stomp::Frame;

// =============================================================================
// Construction Tests
// =============================================================================

#[test]
fn frame_new_creates_empty() {
    let frame = Frame::new("SEND");
    assert_eq!(frame.command, "SEND");
    assert!(frame.headers.is_empty());
    assert!(frame.body.is_empty());
}

#[test]
fn frame_new_with_str() {
    let frame = Frame::new("CONNECT");
    assert_eq!(frame.command, "CONNECT");
}

#[test]
fn frame_new_with_string() {
    let cmd = String::from("MESSAGE");
    let frame = Frame::new(cmd);
    assert_eq!(frame.command, "MESSAGE");
}

// =============================================================================
// Builder Pattern Tests
// =============================================================================

#[test]
fn frame_header_builder_single() {
    let frame = Frame::new("SEND").header("destination", "/queue/test");
    assert_eq!(frame.headers.len(), 1);
    assert_eq!(
        frame.headers[0],
        ("destination".to_string(), "/queue/test".to_string())
    );
}

#[test]
fn frame_header_builder_multiple() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("content-type", "text/plain")
        .header("custom-header", "custom-value");
    assert_eq!(frame.headers.len(), 3);
    assert_eq!(frame.headers[0].0, "destination");
    assert_eq!(frame.headers[1].0, "content-type");
    assert_eq!(frame.headers[2].0, "custom-header");
}

#[test]
fn frame_header_preserves_order() {
    let frame = Frame::new("SEND")
        .header("z-header", "z")
        .header("a-header", "a")
        .header("m-header", "m");
    assert_eq!(frame.headers[0].0, "z-header");
    assert_eq!(frame.headers[1].0, "a-header");
    assert_eq!(frame.headers[2].0, "m-header");
}

#[test]
fn frame_set_body_bytes() {
    let frame = Frame::new("SEND").set_body(vec![1, 2, 3, 4, 5]);
    assert_eq!(frame.body, vec![1, 2, 3, 4, 5]);
}

#[test]
fn frame_set_body_from_string() {
    let frame = Frame::new("SEND").set_body(b"hello world".to_vec());
    assert_eq!(frame.body, b"hello world");
}

#[test]
fn frame_builder_chain() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("content-type", "application/json")
        .set_body(b"{\"key\": \"value\"}".to_vec());

    assert_eq!(frame.command, "SEND");
    assert_eq!(frame.headers.len(), 2);
    assert_eq!(frame.body, b"{\"key\": \"value\"}");
}

// =============================================================================
// Display Trait Tests
// =============================================================================

#[test]
fn frame_display_command() {
    let frame = Frame::new("SEND");
    let display = format!("{}", frame);
    assert!(display.contains("Command: SEND"));
}

#[test]
fn frame_display_headers() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("content-type", "text/plain");
    let display = format!("{}", frame);
    assert!(display.contains("destination: /queue/test"));
    assert!(display.contains("content-type: text/plain"));
}

#[test]
fn frame_display_body_length() {
    let frame = Frame::new("SEND").set_body(b"hello".to_vec());
    let display = format!("{}", frame);
    assert!(display.contains("Body (5 bytes)"));
}

#[test]
fn frame_display_empty_body() {
    let frame = Frame::new("SEND");
    let display = format!("{}", frame);
    assert!(display.contains("Body (0 bytes)"));
}

// =============================================================================
// Clone Tests
// =============================================================================

#[test]
fn frame_clone() {
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .set_body(b"hello".to_vec());
    let cloned = original.clone();

    assert_eq!(original.command, cloned.command);
    assert_eq!(original.headers, cloned.headers);
    assert_eq!(original.body, cloned.body);
}

#[test]
fn frame_clone_is_independent() {
    let original = Frame::new("SEND").set_body(b"hello".to_vec());
    let mut cloned = original.clone();
    cloned.body.push(b'!');

    // Original should be unchanged
    assert_eq!(original.body, b"hello");
    assert_eq!(cloned.body, b"hello!");
}

// =============================================================================
// Equality Tests
// =============================================================================

#[test]
fn frame_eq_identical() {
    let frame1 = Frame::new("SEND")
        .header("destination", "/queue/test")
        .set_body(b"hello".to_vec());
    let frame2 = Frame::new("SEND")
        .header("destination", "/queue/test")
        .set_body(b"hello".to_vec());
    assert_eq!(frame1, frame2);
}

#[test]
fn frame_ne_different_command() {
    let frame1 = Frame::new("SEND");
    let frame2 = Frame::new("MESSAGE");
    assert_ne!(frame1, frame2);
}

#[test]
fn frame_ne_different_headers() {
    let frame1 = Frame::new("SEND").header("destination", "/queue/a");
    let frame2 = Frame::new("SEND").header("destination", "/queue/b");
    assert_ne!(frame1, frame2);
}

#[test]
fn frame_ne_different_header_count() {
    let frame1 = Frame::new("SEND").header("destination", "/queue/test");
    let frame2 = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("extra", "value");
    assert_ne!(frame1, frame2);
}

#[test]
fn frame_ne_different_body() {
    let frame1 = Frame::new("SEND").set_body(b"hello".to_vec());
    let frame2 = Frame::new("SEND").set_body(b"world".to_vec());
    assert_ne!(frame1, frame2);
}

#[test]
fn frame_eq_empty_frames() {
    let frame1 = Frame::new("HEARTBEAT");
    let frame2 = Frame::new("HEARTBEAT");
    assert_eq!(frame1, frame2);
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn frame_empty_command() {
    let frame = Frame::new("");
    assert_eq!(frame.command, "");
}

#[test]
fn frame_header_empty_key() {
    let frame = Frame::new("SEND").header("", "value");
    assert_eq!(frame.headers[0].0, "");
}

#[test]
fn frame_header_empty_value() {
    let frame = Frame::new("SEND").header("key", "");
    assert_eq!(frame.headers[0].1, "");
}

#[test]
fn frame_body_with_nul_bytes() {
    let frame = Frame::new("SEND").set_body(vec![0, 1, 2, 0, 3, 4, 0]);
    assert_eq!(frame.body.len(), 7);
    assert_eq!(frame.body[0], 0);
    assert_eq!(frame.body[3], 0);
    assert_eq!(frame.body[6], 0);
}

#[test]
fn frame_large_body() {
    let large_body = vec![b'x'; 100_000];
    let frame = Frame::new("SEND").set_body(large_body.clone());
    assert_eq!(frame.body.len(), 100_000);
    assert_eq!(frame.body, large_body);
}

#[test]
fn frame_duplicate_headers() {
    // STOMP allows duplicate headers (first wins on read)
    let frame = Frame::new("SEND")
        .header("custom", "first")
        .header("custom", "second");
    assert_eq!(frame.headers.len(), 2);
    assert_eq!(
        frame.headers[0],
        ("custom".to_string(), "first".to_string())
    );
    assert_eq!(
        frame.headers[1],
        ("custom".to_string(), "second".to_string())
    );
}

#[test]
fn frame_get_header_is_case_insensitive() {
    let frame = Frame::new("MESSAGE")
        .header("Message-Id", "msg-123")
        .header("Subscription", "sub-1");

    assert_eq!(frame.get_header("message-id"), Some("msg-123"));
    assert_eq!(frame.get_header("MESSAGE-ID"), Some("msg-123"));
    assert_eq!(frame.get_header("subscription"), Some("sub-1"));
}

#[test]
fn frame_get_header_returns_first_case_insensitive_match() {
    let frame = Frame::new("MESSAGE")
        .header("Message-Id", "first")
        .header("message-id", "second");

    assert_eq!(frame.get_header("MESSAGE-ID"), Some("first"));
}

#[test]
fn frame_header_special_characters() {
    let frame =
        Frame::new("SEND").header("url", "http://example.com:8080/path?query=value&other=123");
    assert_eq!(
        frame.headers[0].1,
        "http://example.com:8080/path?query=value&other=123"
    );
}
