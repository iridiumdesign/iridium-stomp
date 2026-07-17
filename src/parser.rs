// Slice-based STOMP frame parser (produces owned Vecs from input slices)

/// Unescape a STOMP 1.2 header value.
///
/// Per STOMP 1.2 spec, the following escape sequences are supported:
/// - `\r` → carriage return (0x0d)
/// - `\n` → line feed (0x0a)
/// - `\c` → colon (0x3a)
/// - `\\` → backslash (0x5c)
///
/// Returns an error if an invalid escape sequence is encountered.
pub fn unescape_header_value(input: &[u8]) -> Result<Vec<u8>, String> {
    let mut result = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'\\' {
            if i + 1 >= input.len() {
                return Err("incomplete escape sequence at end of header value".to_string());
            }
            match input[i + 1] {
                b'\\' => result.push(b'\\'),
                b'n' => result.push(b'\n'),
                b'r' => result.push(b'\r'),
                b'c' => result.push(b':'),
                other => {
                    return Err(format!(
                        "invalid escape sequence '\\{}' in header value",
                        other as char
                    ));
                }
            }
            i += 2;
        } else {
            result.push(input[i]);
            i += 1;
        }
    }
    Ok(result)
}

/// Minimal helper: extract optional content-length header value from a header list.
///
/// Returns:
/// - Ok(Some(n)) when a valid Content-Length header is present and parsed.
/// - Ok(None) when no Content-Length header is present.
/// - Err(String) when Content-Length is present but not a valid unsigned integer.
type ParseResult =
    Result<Option<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>, usize)>, String>;

fn get_content_length(headers: &[(Vec<u8>, Vec<u8>)]) -> Result<Option<usize>, String> {
    for (k, v) in headers {
        if k.eq_ignore_ascii_case(&b"content-length"[..]) {
            let s =
                std::str::from_utf8(v).map_err(|e| format!("content-length not utf8: {}", e))?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Err("empty content-length".to_string());
            }
            match trimmed.parse::<usize>() {
                Ok(n) => return Ok(Some(n)),
                Err(e) => return Err(format!("invalid content-length '{}': {}", trimmed, e)),
            }
        }
    }
    Ok(None)
}

/// Default upper bound on a single STOMP frame, in bytes (16 MiB).
///
/// A frame larger than this is rejected rather than buffered or allocated. The
/// bound exists so a malicious or buggy broker cannot exhaust client memory —
/// or panic the decoder with an overflowing `content-length` — by announcing an
/// enormous frame. Override per connection with
/// [`ConnectOptions::max_frame_size`](crate::ConnectOptions::max_frame_size).
///
/// [`parse_frame_slice`] uses this value; [`parse_frame_slice_bounded`] takes an
/// explicit one.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Parse a single STOMP frame from a raw byte slice, bounding frame size at
/// [`DEFAULT_MAX_FRAME_SIZE`].
///
/// Returns Ok(Some((command, headers, body, consumed_bytes))) when a full frame
/// was parsed and how many bytes were consumed. Returns Ok(None) when more
/// bytes are required. Returns Err on protocol errors.
pub fn parse_frame_slice(input: &[u8]) -> ParseResult {
    parse_frame_slice_bounded(input, DEFAULT_MAX_FRAME_SIZE)
}

/// Parse a single STOMP frame, rejecting any `content-length` that exceeds
/// `max_frame_size`.
///
/// Identical to [`parse_frame_slice`] except the caller chooses the size bound.
/// Rejecting an oversized `content-length` up front is what keeps the length
/// arithmetic (`pos + content_len + 1`) from overflowing and panicking the
/// decoder — see the `content-length` branch below.
pub fn parse_frame_slice_bounded(input: &[u8], max_frame_size: usize) -> ParseResult {
    let mut pos = 0usize;
    let len = input.len();

    // skip any leading LF heartbeats
    while pos < len && input[pos] == b'\n' {
        // treat a single LF as a heartbeat frame (handled by caller if desired)
        // but we skip leading LFs here; the codec will detect heartbeat earlier
        pos += 1;
    }

    // parse command line: find next LF; if no LF, fall back to NUL-only frame
    let cmd_end_opt = input[pos..].iter().position(|&b| b == b'\n');
    let mut command: Vec<u8>;
    if let Some(cmd_end_rel) = cmd_end_opt {
        command = input[pos..pos + cmd_end_rel].to_vec();
        // strip trailing CR if present
        if command.last() == Some(&b'\r') {
            // remove trailing CR
            command.pop();
        }
        pos += cmd_end_rel + 1;
    } else {
        // No newline found: if there's a NUL in the remaining bytes, treat
        // this as a bare NUL-terminated body with empty command/headers.
        if let Some(nul_rel) = input[pos..].iter().position(|&b| b == 0) {
            if nul_rel > max_frame_size {
                return Err(format!(
                    "frame body {} exceeds maximum frame size {}",
                    nul_rel, max_frame_size
                ));
            }
            let body = input[pos..pos + nul_rel].to_vec();
            pos += nul_rel + 1;
            if pos < len && input[pos] == b'\n' {
                pos += 1;
            }
            let body_opt = if body.is_empty() { None } else { Some(body) };
            return Ok(Some((Vec::new(), Vec::new(), body_opt, pos)));
        }
        return Ok(None);
    }

    // parse headers until an empty line (LF) is found
    let mut headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    loop {
        if pos >= len {
            return Ok(None);
        }
        if input[pos] == b'\n' {
            pos += 1; // consume blank line
            break;
        }
        // find end of header line
        let line_end_rel = match input[pos..].iter().position(|&b| b == b'\n') {
            Some(i) => i,
            None => return Ok(None),
        };
        let mut line = &input[pos..pos + line_end_rel];
        // strip trailing CR
        if !line.is_empty() && line[line.len() - 1] == b'\r' {
            line = &line[..line.len() - 1];
        }
        // find ':' separator
        if let Some(colon) = line.iter().position(|&b| b == b':') {
            let key = line[..colon].to_vec();
            let val = line[colon + 1..].to_vec();
            headers.push((key, val));
        } else {
            return Err(format!(
                "malformed header line: {:?}",
                String::from_utf8_lossy(line)
            ));
        }
        pos += line_end_rel + 1;
    }

    // determine body strategy
    match get_content_length(&headers) {
        Ok(Some(content_len)) => {
            // Reject an oversized length before any arithmetic or allocation.
            // Without this, a broker sending `content-length:<huge>` overflows
            // `pos + content_len + 1` below — panicking the decoder (a remote
            // DoS) — or makes the codec buffer unboundedly waiting for bytes
            // that never come.
            if content_len > max_frame_size {
                return Err(format!(
                    "content-length {} exceeds maximum frame size {}",
                    content_len, max_frame_size
                ));
            }
            // need content_len bytes, plus terminating NUL. Checked so that an
            // enormous caller-supplied `max_frame_size` still cannot overflow.
            let needed = match pos.checked_add(content_len).and_then(|n| n.checked_add(1)) {
                Some(n) => n,
                None => return Err("content-length too large".to_string()),
            };
            if needed > len {
                Ok(None)
            } else {
                let body = input[pos..pos + content_len].to_vec();
                pos += content_len;
                // next must be NUL
                if pos >= len || input[pos] != 0 {
                    Err("missing NUL terminator after content-length body".to_string())
                } else {
                    pos += 1;
                    // optional trailing LF
                    if pos < len && input[pos] == b'\n' {
                        pos += 1;
                    }
                    Ok(Some((command, headers, Some(body), pos)))
                }
            }
        }
        Ok(None) => {
            // NUL-terminated body: find NUL
            match input[pos..].iter().position(|&b| b == 0) {
                Some(nul_rel) => {
                    if nul_rel > max_frame_size {
                        return Err(format!(
                            "frame body {} exceeds maximum frame size {}",
                            nul_rel, max_frame_size
                        ));
                    }
                    let body = input[pos..pos + nul_rel].to_vec();
                    pos += nul_rel + 1;
                    // optional trailing LF
                    if pos < len && input[pos] == b'\n' {
                        pos += 1;
                    }
                    let body_opt = if body.is_empty() { None } else { Some(body) };
                    Ok(Some((command, headers, body_opt, pos)))
                }
                None => Ok(None),
            }
        }
        Err(e) => Err(e),
    }
}
