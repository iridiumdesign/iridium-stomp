//! Unit tests for heartbeat encoding and decoding in the STOMP codec.

use bytes::BytesMut;
use iridium_stomp::Frame;
use iridium_stomp::codec::{StompCodec, StompItem};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn decode_single_lf_as_heartbeat() {
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&[0x0Au8][..]);
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item, StompItem::Heartbeat);
    assert!(
        buf.is_empty(),
        "buffer should be empty after consuming heartbeat"
    );
}

#[test]
fn decode_multiple_consecutive_heartbeats() {
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&[0x0A, 0x0A, 0x0A][..]);

    // First heartbeat
    let item1 = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item1, StompItem::Heartbeat);
    assert_eq!(buf.len(), 2);

    // Second heartbeat
    let item2 = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item2, StompItem::Heartbeat);
    assert_eq!(buf.len(), 1);

    // Third heartbeat
    let item3 = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item3, StompItem::Heartbeat);
    assert!(buf.is_empty());
}

#[test]
fn decode_heartbeat_before_frame() {
    let mut codec = StompCodec::new();
    // Heartbeat (LF) followed by a SEND frame
    let data = b"\nSEND\ndestination:/queue/test\n\nhello\0";
    let mut buf = BytesMut::from(&data[..]);

    // First decode returns heartbeat
    let item1 = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item1, StompItem::Heartbeat);

    // Second decode returns frame
    let item2 = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    match item2 {
        StompItem::Frame(f) => {
            assert_eq!(f.command, "SEND");
            assert_eq!(f.body, b"hello");
        }
        _ => panic!("expected frame, got {:?}", item2),
    }
}

#[test]
fn decode_heartbeat_after_frame() {
    let mut codec = StompCodec::new();
    // Frame followed by TWO LFs - first is consumed as optional trailing LF,
    // second is a separate heartbeat per STOMP spec
    let data = b"SEND\ndestination:/queue/test\n\nhello\0\n\n";
    let mut buf = BytesMut::from(&data[..]);

    // First decode returns frame (consumes optional trailing LF)
    let item1 = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    match item1 {
        StompItem::Frame(f) => {
            assert_eq!(f.command, "SEND");
        }
        _ => panic!("expected frame, got {:?}", item1),
    }

    // Second decode returns heartbeat
    let item2 = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item2, StompItem::Heartbeat);
}

#[test]
fn encode_heartbeat() {
    let mut codec = StompCodec::new();
    let mut dst = BytesMut::new();
    codec
        .encode(StompItem::Heartbeat, &mut dst)
        .expect("encode failed");
    assert_eq!(&dst[..], &[0x0Au8]);
}

#[test]
fn roundtrip_heartbeat() {
    let mut codec = StompCodec::new();

    // Encode
    let mut encoded = BytesMut::new();
    codec
        .encode(StompItem::Heartbeat, &mut encoded)
        .expect("encode failed");

    // Decode
    let decoded = codec
        .decode(&mut encoded)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(decoded, StompItem::Heartbeat);
    assert!(encoded.is_empty());
}

#[test]
fn interleaved_heartbeats_and_frames() {
    let mut codec = StompCodec::new();
    // HB, Frame (with trailing LF consumed), HB, Frame (with trailing LF consumed), HB
    // Note: The LF after NUL is consumed as optional trailing per STOMP spec
    // So we need extra LFs for actual heartbeats
    let data = b"\nSEND\n\n\0\n\nMESSAGE\nmessage-id:1\n\nbody\0\n\n";
    let mut buf = BytesMut::from(&data[..]);

    // 1. Heartbeat
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item, StompItem::Heartbeat);

    // 2. SEND frame (consumes trailing LF)
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    match &item {
        StompItem::Frame(f) => assert_eq!(f.command, "SEND"),
        _ => panic!("expected SEND frame"),
    }

    // 3. Heartbeat
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item, StompItem::Heartbeat);

    // 4. MESSAGE frame (consumes trailing LF)
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    match &item {
        StompItem::Frame(f) => {
            assert_eq!(f.command, "MESSAGE");
            assert_eq!(f.body, b"body");
        }
        _ => panic!("expected MESSAGE frame"),
    }

    // 5. Heartbeat
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item, StompItem::Heartbeat);

    assert!(buf.is_empty());
}

#[test]
fn heartbeat_does_not_corrupt_subsequent_frame_data() {
    let mut codec = StompCodec::new();
    // This tests that consuming a heartbeat correctly advances the buffer
    // and doesn't leave any partial state
    let data = b"\nCONNECT\naccept-version:1.2\nhost:/\n\n\0";
    let mut buf = BytesMut::from(&data[..]);

    // Heartbeat
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item, StompItem::Heartbeat);

    // Frame should decode correctly with all headers intact
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    match item {
        StompItem::Frame(f) => {
            assert_eq!(f.command, "CONNECT");
            assert_eq!(f.headers.len(), 2);
            assert!(
                f.headers
                    .iter()
                    .any(|(k, v)| k == "accept-version" && v == "1.2")
            );
            assert!(f.headers.iter().any(|(k, v)| k == "host" && v == "/"));
        }
        _ => panic!("expected CONNECT frame"),
    }
}

#[test]
fn encode_heartbeat_multiple_times() {
    let mut codec = StompCodec::new();
    let mut dst = BytesMut::new();

    codec
        .encode(StompItem::Heartbeat, &mut dst)
        .expect("encode failed");
    codec
        .encode(StompItem::Heartbeat, &mut dst)
        .expect("encode failed");
    codec
        .encode(StompItem::Heartbeat, &mut dst)
        .expect("encode failed");

    assert_eq!(&dst[..], &[0x0A, 0x0A, 0x0A]);
}

#[test]
fn encode_frame_then_heartbeat() {
    let mut codec = StompCodec::new();
    let mut dst = BytesMut::new();

    let frame = Frame::new("SEND")
        .header("destination", "/queue/test")
        .set_body(b"hello".to_vec());

    codec
        .encode(StompItem::Frame(frame), &mut dst)
        .expect("encode failed");
    codec
        .encode(StompItem::Heartbeat, &mut dst)
        .expect("encode failed");

    // Verify it ends with NUL then LF
    let len = dst.len();
    assert_eq!(dst[len - 2], 0x00); // NUL terminator
    assert_eq!(dst[len - 1], 0x0A); // Heartbeat LF
}

// =============================================================================
// CRLF heartbeat and frame-size guards
// =============================================================================

#[test]
fn decode_crlf_as_heartbeat() {
    // STOMP 1.2 allows the EOL heartbeat to be CRLF, not only a bare LF.
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&b"\r\n"[..]);
    let item = codec
        .decode(&mut buf)
        .expect("decode failed")
        .expect("no item");
    assert_eq!(item, StompItem::Heartbeat);
    assert!(
        buf.is_empty(),
        "buffer should be empty after CRLF heartbeat"
    );
}

#[test]
fn lone_cr_waits_for_more_bytes() {
    // A bare CR could be the first half of a CRLF heartbeat; the codec must wait
    // rather than consume or misparse it.
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&b"\r"[..]);
    let item = codec.decode(&mut buf).expect("decode failed");
    assert!(item.is_none(), "expected Ok(None) for a lone CR");
    assert_eq!(buf.len(), 1, "buffer should be untouched");
}

#[test]
fn oversized_content_length_frame_errors() {
    // A content-length beyond the codec's bound is a hard error, not a panic and
    // not unbounded buffering.
    let mut codec = StompCodec::with_max_frame_size(1024);
    let mut buf = BytesMut::from(&b"MESSAGE\ncontent-length:4000000000\n\nX\0"[..]);
    let result = codec.decode(&mut buf);
    assert!(
        result.is_err(),
        "expected an error for oversized content-length"
    );
}

#[test]
fn never_terminated_frame_is_bounded() {
    // A frame with no content-length that never sends its NUL must not buffer
    // past the bound. Feed more than the max and confirm the codec errors.
    let mut codec = StompCodec::with_max_frame_size(64);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"MESSAGE\ndestination:/q\n\n");
    buf.extend_from_slice(&[b'x'; 128]); // body, no NUL, exceeds the bound
    let result = codec.decode(&mut buf);
    assert!(
        result.is_err(),
        "expected an error once the buffer exceeds the bound"
    );
}

#[test]
fn content_length_overflow_frame_errors_not_panics() {
    // The end-to-end decode path must reject a usize::MAX-ish content-length.
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&b"MESSAGE\ncontent-length:18446744073709551615\n\nX\0"[..]);
    let result = codec.decode(&mut buf);
    assert!(result.is_err(), "expected an error, not a panic");
}

#[test]
fn complete_oversized_frame_rejected_in_one_read() {
    // A *complete* NUL-terminated frame (no content-length) larger than the
    // bound, delivered whole in a single decode call, must be rejected — the
    // guard cannot depend on the frame being incomplete.
    let mut codec = StompCodec::with_max_frame_size(64);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"MESSAGE\ndestination:/q\n\n");
    buf.extend_from_slice(&[b'x'; 128]);
    buf.extend_from_slice(&[0]); // NUL terminator present: the frame IS complete
    let result = codec.decode(&mut buf);
    assert!(
        result.is_err(),
        "a complete-but-oversized frame must be rejected"
    );
}
