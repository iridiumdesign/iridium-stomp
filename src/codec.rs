use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::frame::Frame;
use crate::parser::{DEFAULT_MAX_FRAME_SIZE, parse_frame_slice_bounded, unescape_header_value};

/// Escape a STOMP 1.2 header value for wire transmission.
///
/// Per STOMP 1.2 spec, the following characters must be escaped:
/// - backslash (0x5c) → `\\`
/// - carriage return (0x0d) → `\r`
/// - line feed (0x0a) → `\n`
/// - colon (0x3a) → `\c` (primarily for header names, but we escape in values too for safety)
fn escape_header_value(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' => result.push_str("\\\\"),
            '\r' => result.push_str("\\r"),
            '\n' => result.push_str("\\n"),
            ':' => result.push_str("\\c"),
            _ => result.push(ch),
        }
    }
    result
}

/// (parser-based implementation uses `src` directly; header parsing is
/// delegated to the `parser` module.)
/// Items produced or consumed by the codec.
///
/// A `StompItem` is either a decoded `Frame` or a `Heartbeat` marker
/// representing a single LF received on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StompItem {
    /// A decoded STOMP frame (command + headers + body)
    Frame(Frame),
    /// A single heartbeat pulse (LF)
    Heartbeat,
}

/// `StompCodec` implements `tokio_util::codec::{Decoder, Encoder}` for the
/// STOMP wire protocol.
///
/// Responsibilities:
/// - Decode incoming bytes into `StompItem::Frame` or `StompItem::Heartbeat`.
/// - Support both NUL-terminated frames and frames using the `content-length`
///   header (STOMP 1.2) for binary bodies containing NUL bytes.
/// - Encode `StompItem` back into bytes for the wire format and emit
///   `content-length` when necessary.
pub struct StompCodec {
    // No internal buffer: we parse directly from the provided `src` buffer.
    /// Largest frame this codec will decode before returning an error, in
    /// bytes. Bounds both an oversized `content-length` and a frame that never
    /// terminates, so neither can exhaust memory.
    max_frame_size: usize,
}

impl StompCodec {
    /// Create a new `StompCodec` bounding frames at
    /// [`DEFAULT_MAX_FRAME_SIZE`].
    pub fn new() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Create a `StompCodec` that rejects any frame larger than
    /// `max_frame_size` bytes.
    pub fn with_max_frame_size(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Default for StompCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for StompCodec {
    type Item = StompItem;
    type Error = io::Error;
    /// Decode bytes from `src` into a `StompItem`.
    ///
    /// Parameters
    /// - `src`: a mutable reference to the read buffer containing bytes from the
    ///   transport. The decoder may consume bytes from this buffer (using
    ///   methods like `advance` or `split_to`) when it successfully decodes a
    ///   frame. If there are not enough bytes to form a complete frame, this
    ///   method should return `Ok(None)` and leave `src` in the same state.
    ///
    /// Returns
    /// - `Ok(Some(StompItem))` when a full item (frame or heartbeat) was
    ///   decoded and bytes were consumed from `src` accordingly.
    /// - `Ok(None)` when more bytes are required to decode a complete item.
    /// - `Err(io::Error)` on protocol or data errors (invalid UTF-8, malformed
    ///   frames, missing NUL after a content-length body, etc.).
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Heartbeat: a STOMP 1.2 EOL, which is a bare LF or a CRLF. We parse
        // directly from `src` (BytesMut is contiguous, so `chunk()` is the whole
        // buffer); there is no separate accumulation buffer.
        match src.chunk().first() {
            Some(b'\n') => {
                src.advance(1);
                return Ok(Some(StompItem::Heartbeat));
            }
            Some(b'\r') => match src.chunk().get(1) {
                Some(b'\n') => {
                    src.advance(2);
                    return Ok(Some(StompItem::Heartbeat));
                }
                // A lone CR so far: wait for the next byte to tell CRLF apart
                // from the (malformed) start of a frame.
                None => return Ok(None),
                // CR followed by something else: let the frame parser judge it.
                Some(_) => {}
            },
            _ => {}
        }

        let chunk = src.chunk();
        match parse_frame_slice_bounded(chunk, self.max_frame_size) {
            Ok(Some((cmd_bytes, headers, body, consumed))) => {
                // advance src by consumed
                src.advance(consumed);

                // build owned Frame
                let command = String::from_utf8(cmd_bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid utf8 in command: {}", e),
                    )
                })?;
                // convert headers Vec<(Vec<u8>,Vec<u8>)> -> Vec<(String,String)>
                // and unescape per STOMP 1.2 spec
                let mut hdrs: Vec<(String, String)> = Vec::new();
                for (k, v) in headers {
                    // Unescape header key
                    let k_unescaped = unescape_header_value(&k).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid escape in header key: {}", e),
                        )
                    })?;
                    let ks = String::from_utf8(k_unescaped).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid utf8 in header key: {}", e),
                        )
                    })?;
                    // Unescape header value
                    let v_unescaped = unescape_header_value(&v).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid escape in header value: {}", e),
                        )
                    })?;
                    let vs = String::from_utf8(v_unescaped).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid utf8 in header value: {}", e),
                        )
                    })?;
                    hdrs.push((ks, vs));
                }

                let body = body.unwrap_or_default();

                let frame = Frame {
                    command,
                    headers: hdrs,
                    body,
                };
                Ok(Some(StompItem::Frame(frame)))
            }
            Ok(None) => {
                // Incomplete frame. If we have already buffered the maximum and
                // still cannot parse a whole frame, the frame is oversized (or
                // never NUL-terminated) — bound it here rather than buffering
                // without limit. The content-length path is bounded inside the
                // parser; this covers the NUL-terminated body that never ends.
                if src.len() > self.max_frame_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("frame exceeds maximum frame size {}", self.max_frame_size),
                    ));
                }
                Ok(None)
            }
            Err(e) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parse error: {}", e),
            )),
        }
    }
}

impl Encoder<StompItem> for StompCodec {
    type Error = io::Error;
    /// Encode a `StompItem` into the provided destination buffer.
    ///
    /// Parameters
    /// - `item`: the `StompItem` to encode. The encoder takes ownership of the
    ///   item (and any contained `Frame`) and may consume/mutate its contents.
    /// - `dst`: destination buffer where encoded bytes should be appended.
    ///   This is the same `BytesMut` provided by the `tokio_util::codec`
    ///   framework (e.g. `Framed`). Do not replace or reassign `dst`; instead
    ///   append bytes into it using `BufMut` methods (`put_u8`,
    ///   `put_slice`, `extend_from_slice`, etc.). After `encode` returns the
    ///   contents of `dst` will be written to the underlying transport.
    ///
    /// Returns
    /// - `Ok(())` on success, or `Err(io::Error)` on encoding-related errors.
    fn encode(&mut self, item: StompItem, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            StompItem::Heartbeat => {
                dst.put_u8(b'\n');
            }
            StompItem::Frame(frame) => {
                dst.extend_from_slice(frame.command.as_bytes());
                dst.put_u8(b'\n');

                let mut headers = frame.headers;
                let has_cl = headers
                    .iter()
                    .any(|(k, _)| k.to_lowercase() == "content-length");
                if !has_cl {
                    let include_cl =
                        frame.body.contains(&0) || std::str::from_utf8(&frame.body).is_err();
                    if include_cl {
                        headers.push(("content-length".to_string(), frame.body.len().to_string()));
                    }
                }

                for (k, v) in headers {
                    // Escape header name and value per STOMP 1.2 spec
                    let escaped_key = escape_header_value(&k);
                    let escaped_val = escape_header_value(&v);
                    dst.extend_from_slice(escaped_key.as_bytes());
                    dst.put_u8(b':');
                    dst.extend_from_slice(escaped_val.as_bytes());
                    dst.put_u8(b'\n');
                }

                dst.put_slice(b"\n");
                dst.extend_from_slice(&frame.body);
                dst.put_u8(0);
            }
        }

        Ok(())
    }
}
