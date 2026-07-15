use std::fmt;

/// A simple representation of a STOMP frame.
///
/// `Frame` contains the command (e.g. "SEND", "MESSAGE"), an ordered list
/// of headers (key/value pairs) and the raw body bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// STOMP command (e.g. CONNECT, SEND, SUBSCRIBE)
    pub command: String,
    /// Ordered headers as (key, value) pairs
    pub headers: Vec<(String, String)>,
    /// Raw body bytes
    pub body: Vec<u8>,
}

impl Frame {
    /// Create a new frame with the given command and empty headers/body.
    ///
    /// Parameters
    /// - `command`: the STOMP command name (for example, `"SEND"` or
    ///   `"SUBSCRIBE"`). Accepts any type convertible into `String`.
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Add a header (builder style).
    ///
    /// Parameters
    /// - `key`: header name (converted to `String`).
    /// - `value`: header value (converted to `String`).
    ///
    /// Returns the mutated `Frame` allowing builder-style chaining.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Set the frame body (builder style).
    ///
    /// Parameters
    /// - `body`: raw body bytes. Accepts any type convertible into `Vec<u8>`.
    ///
    /// Returns the mutated `Frame` allowing builder-style chaining.
    pub fn set_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    /// Request a receipt for this frame (builder style).
    ///
    /// When sent, the server will respond with a RECEIPT frame containing
    /// the same receipt ID.
    ///
    /// Use this only with `Connection::send_frame`, which sends the frame
    /// as-is. `Connection::send_frame_with_receipt` and
    /// `Connection::send_frame_confirmed` add a generated receipt header of
    /// their own and track it for you.
    ///
    /// Parameters
    /// - `id`: the receipt identifier. Must be unique per connection.
    ///
    /// Returns the mutated `Frame` allowing builder-style chaining.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let frame = Frame::new("SEND")
    ///     .header("destination", "/queue/test")
    ///     .receipt("msg-001")
    ///     .set_body(b"hello".to_vec());
    /// ```
    pub fn receipt(self, id: impl Into<String>) -> Self {
        self.header("receipt", id)
    }

    /// Get the value of a header by name.
    ///
    /// Returns the first header value matching the given key (case-insensitive),
    /// or `None` if no such header exists.
    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Command: {}", self.command)?;
        for (k, v) in &self.headers {
            writeln!(f, "{}: {}", k, v)?;
        }
        writeln!(f, "Body ({} bytes)", self.body.len())
    }
}
