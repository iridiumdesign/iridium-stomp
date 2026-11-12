//! Tests for STOMP 1.2 header escaping/unescaping per spec.
//!
//! STOMP 1.2 requires these escape sequences in header names and values:
//! - `\r` → carriage return (0x0d)
//! - `\n` → line feed (0x0a)
//! - `\c` → colon (0x3a)
//! - `\\` → backslash (0x5c)

use bytes::BytesMut;
use iridium_stomp::Frame;
use iridium_stomp::codec::{StompCodec, StompItem};
use tokio_util::codec::{Decoder, Encoder};

// ============================================================================
// Unescape tests (parsing incoming frames)
// ============================================================================

#[test]
fn unescape_backslash() {
    // Frame with escaped backslash in header value
    let raw = b"MESSAGE\nheader:value\\\\with\\\\backslashes\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let item = codec.decode(&mut buf).unwrap().unwrap();
    match item {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("header"), Some("value\\with\\backslashes"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn unescape_newline() {
    // Frame with escaped newline in header value
    let raw = b"MESSAGE\nheader:line1\\nline2\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let item = codec.decode(&mut buf).unwrap().unwrap();
    match item {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("header"), Some("line1\nline2"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn unescape_carriage_return() {
    // Frame with escaped carriage return in header value
    let raw = b"MESSAGE\nheader:before\\rafter\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let item = codec.decode(&mut buf).unwrap().unwrap();
    match item {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("header"), Some("before\rafter"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn unescape_colon() {
    // Frame with escaped colon in header value
    let raw = b"MESSAGE\nheader:key\\cvalue\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let item = codec.decode(&mut buf).unwrap().unwrap();
    match item {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("header"), Some("key:value"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn unescape_multiple_sequences() {
    // Frame with multiple escape sequences
    let raw = b"MESSAGE\nheader:a\\nb\\rc\\\\d\\ce\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let item = codec.decode(&mut buf).unwrap().unwrap();
    match item {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("header"), Some("a\nb\rc\\d:e"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn unescape_header_name() {
    // Escaped characters in header name (unusual but spec allows)
    let raw = b"MESSAGE\nkey\\nname:value\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let item = codec.decode(&mut buf).unwrap().unwrap();
    match item {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("key\nname"), Some("value"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn unescape_invalid_sequence() {
    // Invalid escape sequence should produce error
    let raw = b"MESSAGE\nheader:bad\\xescape\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let result = codec.decode(&mut buf);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid escape"));
}

#[test]
fn unescape_incomplete_sequence() {
    // Incomplete escape at end of header value
    let raw = b"MESSAGE\nheader:trailing\\\n\n\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&raw[..]);

    let result = codec.decode(&mut buf);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("incomplete escape"));
}

// ============================================================================
// Escape tests (encoding outgoing frames)
// ============================================================================

#[test]
fn escape_backslash() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("custom", "path\\to\\file");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(StompItem::Frame(frame), &mut buf).unwrap();

    let encoded = String::from_utf8_lossy(&buf);
    assert!(encoded.contains("custom:path\\\\to\\\\file"));
}

#[test]
fn escape_newline() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("custom", "line1\nline2");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(StompItem::Frame(frame), &mut buf).unwrap();

    let encoded = String::from_utf8_lossy(&buf);
    assert!(encoded.contains("custom:line1\\nline2"));
}

#[test]
fn escape_carriage_return() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("custom", "before\rafter");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(StompItem::Frame(frame), &mut buf).unwrap();

    let encoded = String::from_utf8_lossy(&buf);
    assert!(encoded.contains("custom:before\\rafter"));
}

#[test]
fn escape_colon() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("custom", "key:value");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(StompItem::Frame(frame), &mut buf).unwrap();

    let encoded = String::from_utf8_lossy(&buf);
    assert!(encoded.contains("custom:key\\cvalue"));
}

#[test]
fn escape_multiple_characters() {
    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("custom", "a\nb\rc\\d:e");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(StompItem::Frame(frame), &mut buf).unwrap();

    let encoded = String::from_utf8_lossy(&buf);
    assert!(encoded.contains("custom:a\\nb\\rc\\\\d\\ce"));
}

// ============================================================================
// Round-trip tests (encode then decode)
// ============================================================================

#[test]
fn roundtrip_backslash() {
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("path", "C:\\Users\\test\\file.txt");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("path"), Some("C:\\Users\\test\\file.txt"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn roundtrip_newline() {
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("multiline", "first\nsecond\nthird");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("multiline"), Some("first\nsecond\nthird"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn roundtrip_carriage_return() {
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("windows", "line1\r\nline2");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("windows"), Some("line1\r\nline2"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn roundtrip_colon() {
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("url", "http://example.com:8080/path");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(
                frame.get_header("url"),
                Some("http://example.com:8080/path")
            );
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn roundtrip_all_special_chars() {
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("complex", "path\\to\\file\nkey:value\r\nend");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(
                frame.get_header("complex"),
                Some("path\\to\\file\nkey:value\r\nend")
            );
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn roundtrip_empty_value() {
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("empty", "");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("empty"), Some(""));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn roundtrip_only_special_chars() {
    // Header value that is ONLY special characters
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("special", "\\\n\r:");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("special"), Some("\\\n\r:"));
        }
        _ => panic!("expected frame"),
    }
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn no_escaping_needed() {
    // Normal header value with no special characters should pass through unchanged
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("normal", "just-a-normal-value");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    // Verify no escape sequences in the encoded output
    let encoded = String::from_utf8_lossy(&buf);
    assert!(encoded.contains("normal:just-a-normal-value"));
    assert!(!encoded.contains("\\\\"));
    assert!(!encoded.contains("\\n"));
    assert!(!encoded.contains("\\r"));
    assert!(!encoded.contains("\\c"));

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("normal"), Some("just-a-normal-value"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn consecutive_escapes() {
    // Multiple consecutive escape sequences
    let original = Frame::new("SEND")
        .header("destination", "/queue/test")
        .header("consecutive", "\n\n\n\\\\\\");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(frame.get_header("consecutive"), Some("\n\n\n\\\\\\"));
        }
        _ => panic!("expected frame"),
    }
}

#[test]
fn destination_with_colon() {
    // Destination header containing colon (common in URLs)
    let original = Frame::new("SEND").header("destination", "/queue/http://example.com:8080");

    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec
        .encode(StompItem::Frame(original.clone()), &mut buf)
        .unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    match decoded {
        StompItem::Frame(frame) => {
            assert_eq!(
                frame.get_header("destination"),
                Some("/queue/http://example.com:8080")
            );
        }
        _ => panic!("expected frame"),
    }
}

// ============================================================================
// Handshake frames (CONNECT, STOMP, CONNECTED)
// ============================================================================

fn encode(frame: Frame) -> String {
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(StompItem::Frame(frame), &mut buf).unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

fn decode(raw: &[u8]) -> Frame {
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(raw);
    match codec.decode(&mut buf).unwrap().unwrap() {
        StompItem::Frame(frame) => frame,
        other => panic!("expected frame, got {other:?}"),
    }
}

/// #136: Artemis reads CONNECT header values verbatim, so a colon escaped as
/// `\c` is a wrong passcode there. Unescaped it is accepted by Artemis,
/// RabbitMQ and ActiveMQ Classic alike.
#[test]
fn connect_writes_a_colon_in_a_value_as_it_is() {
    for command in ["CONNECT", "STOMP"] {
        let wire = encode(
            Frame::new(command)
                .header("login", "user")
                .header("passcode", "pa:ss"),
        );
        assert!(wire.contains("\npasscode:pa:ss\n"), "{command}: {wire:?}");
    }
}

/// A backslash has no form every broker accepts; it stays escaped, as before.
/// So do CR and LF, which would otherwise end the header line.
#[test]
fn connect_still_escapes_backslash_and_line_breaks() {
    let wire = encode(Frame::new("CONNECT").header("passcode", "a\\b\nc\rd"));
    assert!(wire.contains("\npasscode:a\\\\b\\nc\\rd\n"), "{wire:?}");
}

/// A colon in a header name can only be told from the separator if escaped.
#[test]
fn connect_still_escapes_a_colon_in_a_header_name() {
    let wire = encode(Frame::new("CONNECT").header("odd:name", "v"));
    assert!(wire.contains("\nodd\\cname:v\n"), "{wire:?}");
}

/// Every other frame escapes the colon, as STOMP 1.2 requires.
#[test]
fn other_frames_still_escape_a_colon_in_a_value() {
    let wire = encode(Frame::new("SEND").header("custom", "a:b"));
    assert!(wire.contains("\ncustom:a\\cb\n"), "{wire:?}");
}

#[test]
fn connect_with_colon_and_backslash_round_trips() {
    let frame = decode(encode(Frame::new("CONNECT").header("passcode", "a:b\\c")).as_bytes());
    assert_eq!(frame.get_header("passcode"), Some("a:b\\c"));
}

/// A broker that does not escape CONNECTED headers may send a backslash that
/// is no escape sequence. That must not fail the handshake.
#[test]
fn connected_value_that_is_not_escaped_is_read_verbatim() {
    let frame = decode(b"CONNECTED\nversion:1.2\nserver:Broker\\x/1.0:beta\n\n\0");
    assert_eq!(frame.get_header("server"), Some("Broker\\x/1.0:beta"));
}

/// A broker that does escape them is still read as before.
#[test]
fn connected_value_that_is_escaped_is_unescaped() {
    let frame = decode(b"CONNECTED\nversion:1.2\nserver:a\\cb\\\\c\n\n\0");
    assert_eq!(frame.get_header("server"), Some("a:b\\c"));
}

/// Leniency is for the handshake only.
#[test]
fn other_frames_still_reject_an_invalid_escape() {
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&b"MESSAGE\nheader:bad\\xescape\n\n\0"[..]);
    assert!(codec.decode(&mut buf).is_err());
}
