//! Unit tests for the STOMP frame parser.

use iridium_stomp::parser::parse_frame_slice;

// =============================================================================
// Command Parsing Tests
// =============================================================================

#[test]
fn parse_connect_command() {
    let raw = b"CONNECT\naccept-version:1.2\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"CONNECT");
}

#[test]
fn parse_send_command() {
    let raw = b"SEND\ndestination:/queue/test\n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"SEND");
}

#[test]
fn parse_subscribe_command() {
    let raw = b"SUBSCRIBE\nid:0\ndestination:/queue/test\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"SUBSCRIBE");
}

#[test]
fn parse_message_command() {
    let raw = b"MESSAGE\nmessage-id:1\ndestination:/queue/test\n\nbody\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"MESSAGE");
}

#[test]
fn parse_connected_command() {
    let raw = b"CONNECTED\nversion:1.2\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"CONNECTED");
}

#[test]
fn parse_disconnect_command() {
    let raw = b"DISCONNECT\nreceipt:77\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"DISCONNECT");
}

#[test]
fn parse_ack_command() {
    let raw = b"ACK\nid:12345\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"ACK");
}

#[test]
fn parse_nack_command() {
    let raw = b"NACK\nid:12345\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"NACK");
}

#[test]
fn parse_command_with_crlf() {
    // Note: Parser strips CR from command but \r\n\r\n creates issues
    // The parser handles trailing CR on command and headers, but expects
    // the blank line to be just \n (or \r\n which becomes empty after CR strip)
    // This test validates CR is stripped from command line
    let raw = b"SEND\r\ndestination:/queue/test\n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"SEND");
}

// =============================================================================
// Header Parsing Tests
// =============================================================================

#[test]
fn parse_single_header() {
    let raw = b"SEND\ndestination:/queue/test\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1.len(), 1);
    assert_eq!(result.1[0].0, b"destination");
    assert_eq!(result.1[0].1, b"/queue/test");
}

#[test]
fn parse_multiple_headers() {
    let raw = b"SEND\ndestination:/queue/test\ncontent-type:text/plain\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1.len(), 2);
    assert_eq!(result.1[0].0, b"destination");
    assert_eq!(result.1[1].0, b"content-type");
}

#[test]
fn parse_header_with_colon_in_value() {
    // The colon should only split at the FIRST colon
    let raw = b"SEND\ndestination:tcp://host:1234/queue\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1[0].0, b"destination");
    assert_eq!(result.1[0].1, b"tcp://host:1234/queue");
}

#[test]
fn parse_header_with_empty_value() {
    let raw = b"SEND\ndestination:\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1[0].0, b"destination");
    assert_eq!(result.1[0].1, b"");
}

#[test]
fn parse_header_no_colon_errors() {
    let raw = b"SEND\ndestination-no-colon\n\n\0";
    let result = parse_frame_slice(raw);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("malformed header"));
}

#[test]
fn parse_headers_with_crlf() {
    // Parser strips trailing CR from header lines
    // The blank line separator must be just \n after any CR stripping
    let raw = b"SEND\r\ndestination:/queue/test\r\ncontent-type:text/plain\r\n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1.len(), 2);
    assert_eq!(result.1[0].0, b"destination");
    assert_eq!(result.1[1].0, b"content-type");
}

#[test]
fn parse_many_headers() {
    let raw = b"CONNECT\naccept-version:1.2\nhost:/\nlogin:guest\npasscode:guest\nheart-beat:10000,10000\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1.len(), 5);
}

// =============================================================================
// Content-Length Tests
// =============================================================================

#[test]
fn parse_content_length_zero() {
    let raw = b"SEND\ncontent-length:0\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    // With content-length:0, body should be empty
    assert_eq!(result.2, Some(vec![]));
}

#[test]
fn parse_content_length_valid() {
    let raw = b"SEND\ncontent-length:5\n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(b"hello".to_vec()));
}

#[test]
fn parse_content_length_invalid() {
    let raw = b"SEND\ncontent-length:xyz\n\nhello\0";
    let result = parse_frame_slice(raw);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("invalid content-length"));
}

#[test]
fn parse_content_length_empty() {
    let raw = b"SEND\ncontent-length:\n\nhello\0";
    let result = parse_frame_slice(raw);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("empty content-length"));
}

#[test]
fn parse_content_length_with_whitespace() {
    let raw = b"SEND\ncontent-length: 5 \n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(b"hello".to_vec()));
}

#[test]
fn parse_content_length_case_insensitive() {
    let raw = b"SEND\nContent-Length:5\n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(b"hello".to_vec()));
}

#[test]
fn parse_content_length_mixed_case() {
    let raw = b"SEND\nCONTENT-LENGTH:5\n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(b"hello".to_vec()));
}

#[test]
fn parse_content_length_with_embedded_nul() {
    // Binary body with NUL byte in the middle
    let raw = b"SEND\ncontent-length:6\n\nhel\0lo\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(b"hel\0lo".to_vec()));
}

#[test]
fn parse_content_length_missing_nul_terminator() {
    // Body matches content-length but no NUL after
    let raw = b"SEND\ncontent-length:5\n\nhello";
    let result = parse_frame_slice(raw);
    // Should return Ok(None) - need more bytes (NUL terminator)
    assert!(result.unwrap().is_none());
}

// =============================================================================
// Body Parsing Tests
// =============================================================================

#[test]
fn parse_empty_body() {
    let raw = b"SEND\ndestination:/queue/test\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert!(result.2.is_none()); // Empty body is None
}

#[test]
fn parse_body_nul_terminated() {
    let raw = b"SEND\ndestination:/queue/test\n\nhello world\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(b"hello world".to_vec()));
}

#[test]
fn parse_body_with_trailing_lf() {
    let raw = b"SEND\n\nhello\0\n";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(b"hello".to_vec()));
    // Consumed should include the trailing LF
    assert_eq!(result.3, raw.len());
}

#[test]
fn parse_body_binary_with_content_length() {
    // Binary body with multiple NULs
    let raw = b"SEND\ncontent-length:10\n\n\0\0\0\0\0\0\0\0\0\0\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.2, Some(vec![0u8; 10]));
}

#[test]
fn parse_large_body() {
    let body = vec![b'x'; 10000];
    let mut raw = format!("SEND\ncontent-length:{}\n\n", body.len()).into_bytes();
    raw.extend_from_slice(&body);
    raw.push(0);

    let result = parse_frame_slice(&raw).unwrap().unwrap();
    assert_eq!(result.2.unwrap().len(), 10000);
}

// =============================================================================
// Incomplete Frame Tests (Returns Ok(None))
// =============================================================================

#[test]
fn parse_partial_command() {
    let raw = b"SEN";
    let result = parse_frame_slice(raw);
    assert!(result.unwrap().is_none());
}

#[test]
fn parse_partial_command_with_newline() {
    let raw = b"SEND\n";
    let result = parse_frame_slice(raw);
    assert!(result.unwrap().is_none());
}

#[test]
fn parse_partial_headers() {
    let raw = b"SEND\ndestination:/queue/test";
    let result = parse_frame_slice(raw);
    assert!(result.unwrap().is_none());
}

#[test]
fn parse_partial_headers_no_blank_line() {
    let raw = b"SEND\ndestination:/queue/test\n";
    let result = parse_frame_slice(raw);
    assert!(result.unwrap().is_none());
}

#[test]
fn parse_partial_body() {
    let raw = b"SEND\ncontent-length:10\n\nhello";
    let result = parse_frame_slice(raw);
    assert!(result.unwrap().is_none());
}

#[test]
fn parse_partial_body_nul_terminated() {
    let raw = b"SEND\n\nhello";
    let result = parse_frame_slice(raw);
    // No NUL found, so incomplete
    assert!(result.unwrap().is_none());
}

// =============================================================================
// Consumed Bytes Tests
// =============================================================================

#[test]
fn parse_consumed_bytes_simple() {
    let raw = b"SEND\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.3, raw.len());
}

#[test]
fn parse_consumed_bytes_with_trailing_lf() {
    let raw = b"SEND\n\n\0\n";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.3, raw.len()); // Includes trailing LF
}

#[test]
fn parse_consumed_bytes_multiple_frames_in_buffer() {
    // Two frames in buffer
    let raw = b"SEND\n\n\0SEND\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    // Should only consume first frame: S(1) E(2) N(3) D(4) \n(5) \n(6) \0(7) = 7 bytes
    assert_eq!(result.3, 7);
}

// =============================================================================
// Leading LF (Heartbeat) Handling
// =============================================================================

#[test]
fn parse_skips_leading_lf() {
    // Parser skips leading LFs (heartbeats handled by codec)
    let raw = b"\n\n\nSEND\n\nhello\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"SEND");
    assert_eq!(result.2, Some(b"hello".to_vec()));
}

// =============================================================================
// Error Cases
// =============================================================================

#[test]
fn parse_malformed_header_errors() {
    let raw = b"SEND\nthis line has no colon\n\n\0";
    let result = parse_frame_slice(raw);
    assert!(result.is_err());
}

#[test]
fn parse_content_length_negative() {
    let raw = b"SEND\ncontent-length:-5\n\nhello\0";
    let result = parse_frame_slice(raw);
    assert!(result.is_err());
}

#[test]
fn parse_content_length_overflow() {
    // Very large content-length that would overflow
    let raw = b"SEND\ncontent-length:99999999999999999999\n\nhello\0";
    let result = parse_frame_slice(raw);
    assert!(result.is_err());
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn parse_frame_only_command_and_empty_body() {
    let raw = b"HEARTBEAT\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.0, b"HEARTBEAT");
    assert_eq!(result.1.len(), 0);
    assert!(result.2.is_none());
}

#[test]
fn parse_frame_with_numeric_header_value() {
    let raw = b"SEND\nretries:3\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1[0].1, b"3");
}

#[test]
fn parse_frame_with_special_chars_in_value() {
    let raw = b"SEND\ndestination:/queue/test?foo=bar&baz=qux\n\n\0";
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert_eq!(result.1[0].1, b"/queue/test?foo=bar&baz=qux");
}

#[test]
fn parse_empty_input() {
    let raw = b"";
    let result = parse_frame_slice(raw);
    assert!(result.unwrap().is_none());
}

#[test]
fn parse_only_lf() {
    let raw = b"\n";
    // Parser skips leading LF, then needs more bytes for command
    let result = parse_frame_slice(raw);
    assert!(result.unwrap().is_none());
}

#[test]
fn parse_only_nul() {
    let raw = b"\0";
    // NUL with no content is treated as empty body with empty command
    let result = parse_frame_slice(raw).unwrap().unwrap();
    assert!(result.0.is_empty());
    assert!(result.1.is_empty());
    assert!(result.2.is_none());
}

// =============================================================================
// Denial-of-service guards (malformed content-length)
// =============================================================================

use iridium_stomp::parser::parse_frame_slice_bounded;

#[test]
fn content_length_near_usize_max_errors_without_panicking() {
    // A broker announcing a content-length near usize::MAX must not overflow
    // the `pos + content_len + 1` arithmetic (which panicked the decoder before
    // this guard). It is rejected as a protocol error instead.
    let raw = b"MESSAGE\ncontent-length:18446744073709551615\n\nX\0";
    let result = parse_frame_slice(raw);
    assert!(
        result.is_err(),
        "expected an error, got {:?}",
        result.map(|o| o.map(|t| t.3))
    );
}

#[test]
fn content_length_over_the_bound_is_rejected() {
    // A large-but-non-overflowing content-length must be rejected rather than
    // making the caller buffer unboundedly waiting for bytes that never come.
    let raw = b"MESSAGE\ncontent-length:4000000000\n\nX\0";
    let result = parse_frame_slice_bounded(raw, 1024);
    assert!(
        result.is_err(),
        "expected an error, got {:?}",
        result.map(|o| o.map(|t| t.3))
    );
}

#[test]
fn complete_oversized_nul_body_is_rejected() {
    // A whole NUL-terminated frame with no content-length, larger than the
    // bound, arriving all at once must be rejected — not accepted just because
    // it is complete. Guards the gap where only content-length was bounded.
    let mut raw = b"MESSAGE\ndestination:/q\n\n".to_vec();
    raw.extend_from_slice(&[b'x'; 4096]);
    raw.push(0);
    let result = parse_frame_slice_bounded(&raw, 1024);
    assert!(
        result.is_err(),
        "expected an error, got {:?}",
        result.map(|o| o.map(|t| t.3))
    );
}

#[test]
fn content_length_within_the_bound_still_parses() {
    // The guard must not reject a legitimate frame whose length is under the
    // bound.
    let raw = b"MESSAGE\ncontent-length:5\n\nhello\0";
    let parsed = parse_frame_slice_bounded(raw, 1024)
        .expect("parse error")
        .expect("incomplete");
    assert_eq!(parsed.0, b"MESSAGE");
    assert_eq!(parsed.2.as_deref(), Some(&b"hello"[..]));
}
