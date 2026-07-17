use futures::{SinkExt, StreamExt, future};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::codec::{StompCodec, StompItem};
use crate::frame::Frame;
use crate::parser::DEFAULT_MAX_FRAME_SIZE;

/// Configuration for STOMP heartbeat intervals.
///
/// Provides a type-safe way to configure heartbeat values instead of using
/// raw strings. The `Display` implementation formats the value as required
/// by the STOMP protocol ("send_ms,receive_ms").
///
/// # Example
///
/// ```
/// use iridium_stomp::Heartbeat;
///
/// // Create a custom heartbeat configuration
/// let hb = Heartbeat::new(5000, 10000);
/// assert_eq!(hb.to_string(), "5000,10000");
///
/// // Use predefined configurations
/// assert_eq!(Heartbeat::disabled().to_string(), "0,0");
/// assert_eq!(Heartbeat::default().to_string(), "10000,10000");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    /// Minimum interval (in milliseconds) between heartbeats the client can send.
    /// A value of 0 means the client cannot send heartbeats.
    pub send_ms: u32,

    /// Minimum interval (in milliseconds) between heartbeats the client wants to receive.
    /// A value of 0 means the client does not want to receive heartbeats.
    pub receive_ms: u32,
}

impl Heartbeat {
    /// Create a new heartbeat configuration with the specified intervals.
    ///
    /// # Arguments
    ///
    /// * `send_ms` - Minimum interval in milliseconds between heartbeats the client can send.
    /// * `receive_ms` - Minimum interval in milliseconds between heartbeats the client wants to receive.
    ///
    /// # Example
    ///
    /// ```
    /// use iridium_stomp::Heartbeat;
    ///
    /// let hb = Heartbeat::new(5000, 10000);
    /// assert_eq!(hb.send_ms, 5000);
    /// assert_eq!(hb.receive_ms, 10000);
    /// ```
    pub fn new(send_ms: u32, receive_ms: u32) -> Self {
        Self {
            send_ms,
            receive_ms,
        }
    }

    /// Create a heartbeat configuration that disables heartbeats entirely.
    ///
    /// This is equivalent to `Heartbeat::new(0, 0)`.
    ///
    /// # Example
    ///
    /// ```
    /// use iridium_stomp::Heartbeat;
    ///
    /// let hb = Heartbeat::disabled();
    /// assert_eq!(hb.send_ms, 0);
    /// assert_eq!(hb.receive_ms, 0);
    /// assert_eq!(hb.to_string(), "0,0");
    /// ```
    pub fn disabled() -> Self {
        Self::new(0, 0)
    }

    /// Create a heartbeat configuration from a Duration for symmetric heartbeats.
    ///
    /// Both send and receive intervals will be set to the same value.
    ///
    /// The maximum supported Duration is approximately 49.7 days (u32::MAX milliseconds,
    /// or 4,294,967,295 ms). If a larger Duration is provided, it will be clamped to
    /// u32::MAX milliseconds to prevent overflow.
    ///
    /// # Example
    ///
    /// ```
    /// use iridium_stomp::Heartbeat;
    /// use std::time::Duration;
    ///
    /// let hb = Heartbeat::from_duration(Duration::from_secs(15));
    /// assert_eq!(hb.send_ms, 15000);
    /// assert_eq!(hb.receive_ms, 15000);
    /// ```
    pub fn from_duration(interval: Duration) -> Self {
        let ms = interval.as_millis().min(u32::MAX as u128) as u32;
        Self::new(ms, ms)
    }
}

impl Default for Heartbeat {
    /// Returns the default heartbeat configuration: 10 seconds for both send and receive.
    fn default() -> Self {
        Self::new(10000, 10000)
    }
}

impl std::fmt::Display for Heartbeat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}", self.send_ms, self.receive_ms)
    }
}

/// Internal subscription entry stored for each destination.
#[derive(Clone)]
pub(crate) struct SubscriptionEntry {
    pub(crate) id: String,
    pub(crate) sender: mpsc::Sender<Frame>,
    pub(crate) ack: String,
    pub(crate) headers: Vec<(String, String)>,
}

/// Alias for the subscription dispatch map: destination -> list of
/// `SubscriptionEntry`.
pub(crate) type Subscriptions = HashMap<String, Vec<SubscriptionEntry>>;

/// Alias for the pending map: subscription_id -> queue of (message-id, Frame).
pub(crate) type PendingMap = HashMap<String, VecDeque<(String, Frame)>>;

/// Internal type for resubscribe snapshot entries: (destination, id, ack, headers)
pub(crate) type ResubEntry = (String, String, String, Vec<(String, String)>);

/// Alias for pending receipt map: receipt-id -> oneshot sender to notify when resolved.
pub(crate) type PendingReceipts = HashMap<String, oneshot::Sender<Result<(), ServerError>>>;

/// Errors returned by `Connection` operations.
#[derive(Error, Debug)]
pub enum ConnError {
    /// I/O-level error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Protocol-level error
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Receipt timeout error
    #[error("receipt timeout: no RECEIPT received for '{0}' within timeout")]
    ReceiptTimeout(String),
    /// Server rejected the connection (e.g., authentication failure).
    ///
    /// This error is returned when the server sends an ERROR frame in response
    /// to the CONNECT frame. Common causes include invalid credentials,
    /// unauthorized access, or broker configuration issues.
    #[error("server rejected connection: {0}")]
    ServerRejected(ServerError),
    /// A frame that requested a receipt was rejected by the broker via
    /// an ERROR frame with a matching receipt-id. The connection may
    /// still be usable depending on broker behavior.
    #[error("frame rejected: {0}")]
    FrameRejected(ServerError),
}

/// Represents an ERROR frame received from the STOMP server.
///
/// STOMP servers send ERROR frames to indicate protocol violations, authentication
/// failures, or other server-side errors. After sending an ERROR frame, the server
/// typically closes the connection.
///
/// # Example
///
/// ```ignore
/// use iridium_stomp::ReceivedFrame;
///
/// while let Some(received) = conn.next_frame().await {
///     match received {
///         ReceivedFrame::Frame(frame) => {
///             // Normal message processing
///         }
///         ReceivedFrame::Error(err) => {
///             eprintln!("Server error: {}", err.message);
///             if let Some(body) = &err.body {
///                 eprintln!("Details: {}", body);
///             }
///             break;
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    /// The error message from the `message` header.
    pub message: String,

    /// The error body, if present. Contains additional error details.
    pub body: Option<String>,

    /// The receipt-id if this error is in response to a specific frame.
    pub receipt_id: Option<String>,

    /// The original ERROR frame for access to additional headers.
    pub frame: Frame,
}

impl ServerError {
    /// Create a `ServerError` from an ERROR frame.
    ///
    /// This is primarily used internally but is public for testing and
    /// advanced use cases where you need to construct a `ServerError` manually.
    pub fn from_frame(frame: Frame) -> Self {
        let message = frame
            .get_header("message")
            .unwrap_or("unknown error")
            .to_string();

        let body = if frame.body.is_empty() {
            None
        } else {
            String::from_utf8(frame.body.clone()).ok()
        };

        let receipt_id = frame.get_header("receipt-id").map(|s| s.to_string());

        Self {
            message,
            body,
            receipt_id,
            frame,
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "STOMP server error: {}", self.message)?;
        if let Some(body) = &self.body {
            write!(f, " - {}", body)?;
        }
        Ok(())
    }
}

impl std::error::Error for ServerError {}

/// The result of receiving a frame from the server.
///
/// STOMP servers can send either normal frames (MESSAGE, RECEIPT, etc.) or
/// ERROR frames indicating a problem. This enum allows callers to handle
/// both cases with pattern matching.
///
/// # Example
///
/// ```ignore
/// use iridium_stomp::ReceivedFrame;
///
/// match conn.next_frame().await {
///     Some(ReceivedFrame::Frame(frame)) => {
///         println!("Got frame: {}", frame.command);
///     }
///     Some(ReceivedFrame::Error(err)) => {
///         eprintln!("Server error: {}", err);
///     }
///     None => {
///         println!("Connection closed");
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceivedFrame {
    /// A normal STOMP frame (MESSAGE, RECEIPT, etc.)
    Frame(Frame),
    /// An ERROR frame from the server
    Error(ServerError),
}

impl ReceivedFrame {
    /// Returns `true` if this is an error frame.
    pub fn is_error(&self) -> bool {
        matches!(self, ReceivedFrame::Error(_))
    }

    /// Returns `true` if this is a normal frame.
    pub fn is_frame(&self) -> bool {
        matches!(self, ReceivedFrame::Frame(_))
    }

    /// Returns the frame if this is a normal frame, or `None` if it's an error.
    pub fn into_frame(self) -> Option<Frame> {
        match self {
            ReceivedFrame::Frame(f) => Some(f),
            ReceivedFrame::Error(_) => None,
        }
    }

    /// Returns the error if this is an error frame, or `None` if it's a normal frame.
    pub fn into_error(self) -> Option<ServerError> {
        match self {
            ReceivedFrame::Frame(_) => None,
            ReceivedFrame::Error(e) => Some(e),
        }
    }
}

/// Subscription acknowledgement modes as defined by STOMP 1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Server considers the message delivered as soon as it is sent (no
    /// explicit acknowledgement required from the client).
    Auto,
    /// Client must send an ACK frame; the ACK is cumulative — it
    /// acknowledges all messages up to and including the specified one.
    Client,
    /// Client must send an ACK frame for each individual message.
    ClientIndividual,
}

impl AckMode {
    fn as_str(&self) -> &'static str {
        match self {
            AckMode::Auto => "auto",
            AckMode::Client => "client",
            AckMode::ClientIndividual => "client-individual",
        }
    }
}

/// Options for customizing the STOMP CONNECT frame.
///
/// Use this struct with `Connection::connect_with_options()` to set custom
/// headers, specify supported STOMP versions, or configure broker-specific
/// options like `client-id` for durable subscriptions.
///
/// # Validation
///
/// This struct performs minimal validation. Values are passed to the broker
/// as-is, and invalid configurations will be rejected by the broker at
/// connection time. Empty strings are technically accepted but may cause
/// broker-specific errors.
///
/// # Custom Headers
///
/// Custom headers added via `header()` cannot override critical STOMP headers
/// (`accept-version`, `host`, `login`, `passcode`, `heart-beat`, `client-id`).
/// Such headers are silently ignored. Use the dedicated builder methods to
/// set these values.
///
/// # Example
///
/// ```ignore
/// use iridium_stomp::{Connection, ConnectOptions};
///
/// let options = ConnectOptions::default()
///     .client_id("my-durable-client")
///     .host("my-vhost")
///     .header("custom-header", "value");
///
/// let conn = Connection::connect_with_options(
///     "localhost:61613",
///     "guest",
///     "guest",
///     Connection::DEFAULT_HEARTBEAT,
///     options,
/// ).await?;
/// ```
#[derive(Clone, Default)]
pub struct ConnectOptions {
    /// STOMP version(s) to accept (e.g., "1.2" or "1.0,1.1,1.2").
    /// Defaults to "1.2" if not set.
    pub accept_version: Option<String>,

    /// Client ID for durable subscriptions (required by ActiveMQ, etc.).
    pub client_id: Option<String>,

    /// Virtual host header value. Defaults to "/" if not set.
    pub host: Option<String>,

    /// Additional custom headers to include in the CONNECT frame.
    /// Note: Headers that would override critical STOMP headers are ignored.
    pub headers: Vec<(String, String)>,

    /// Optional channel to receive heartbeat notifications.
    /// When set, the connection will send a `()` on this channel each time
    /// a heartbeat is received from the server.
    pub heartbeat_tx: Option<mpsc::Sender<()>>,

    /// How long `Connection::close` waits for the broker's RECEIPT after it
    /// sends DISCONNECT. Defaults to
    /// [`Connection::DEFAULT_DISCONNECT_TIMEOUT`] if not set.
    pub disconnect_timeout: Option<Duration>,

    /// Upper bound on the initial connect-and-handshake before
    /// [`Connection::connect_with_options`] gives up. When `None` (the
    /// default), an unreachable broker is retried indefinitely with backoff.
    /// When set, the whole operation is bounded and, on expiry, the last
    /// error encountered is returned.
    pub connect_timeout: Option<Duration>,

    /// Largest inbound frame to accept, in bytes. When `None`, the default is
    /// [`crate::parser::DEFAULT_MAX_FRAME_SIZE`] (16 MiB). A frame larger than
    /// this — whether via an oversized `content-length` or a body that never
    /// terminates — is rejected as a `ConnError::Io`, so a malicious or buggy
    /// broker cannot exhaust client memory or panic the decoder.
    pub max_frame_size: Option<usize>,
}

impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("accept_version", &self.accept_version)
            .field("client_id", &self.client_id)
            .field("host", &self.host)
            .field("headers", &self.headers)
            .field(
                "heartbeat_tx",
                &self.heartbeat_tx.as_ref().map(|_| "Some(...)"),
            )
            .field("disconnect_timeout", &self.disconnect_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_frame_size", &self.max_frame_size)
            .finish()
    }
}

impl ConnectOptions {
    /// Create a new `ConnectOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the STOMP version(s) to accept (builder style).
    ///
    /// Examples: "1.2", "1.1,1.2", "1.0,1.1,1.2"
    pub fn accept_version(mut self, version: impl Into<String>) -> Self {
        self.accept_version = Some(version.into());
        self
    }

    /// Set the client ID for durable subscriptions (builder style).
    ///
    /// Required by some brokers (e.g., ActiveMQ) for durable topic subscriptions.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    /// Set the virtual host (builder style).
    ///
    /// Defaults to "/" if not set.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Add a custom header to the CONNECT frame (builder style).
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Set how long [`Connection::close`] waits for the broker to confirm the
    /// DISCONNECT (builder style).
    ///
    /// Defaults to [`Connection::DEFAULT_DISCONNECT_TIMEOUT`]. The socket is
    /// torn down when the timeout expires regardless, so this bounds how long a
    /// clean shutdown may take against an unresponsive broker; it does not
    /// decide whether the connection closes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let options = ConnectOptions::default()
    ///     .disconnect_timeout(Duration::from_secs(2));
    /// ```
    pub fn disconnect_timeout(mut self, timeout: Duration) -> Self {
        self.disconnect_timeout = Some(timeout);
        self
    }

    /// Bound the initial connect and STOMP handshake (builder style).
    ///
    /// By default [`Connection::connect_with_options`] retries an unreachable
    /// broker indefinitely with exponential backoff, which is the right choice
    /// for a long-lived service that should wait for its broker to come up. It
    /// is the wrong choice for a CLI tool or one-shot script pointed at a
    /// misconfigured address: without a bound it hangs forever. Set this to cap
    /// the whole operation.
    ///
    /// When the timeout expires, `connect_with_options` returns the last error
    /// it encountered — a [`ConnError::Io`], or a [`ConnError::Protocol`] such
    /// as a broker that closed the socket mid-handshake — or a synthesized
    /// [`std::io::ErrorKind::TimedOut`] if no attempt had produced one yet. A
    /// `ConnError::ServerRejected` still fails immediately, before the bound is
    /// consulted, because retrying bad credentials is pointless.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let options = ConnectOptions::default()
    ///     .connect_timeout(Duration::from_secs(60));
    /// ```
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the largest inbound frame to accept, in bytes (builder style).
    ///
    /// Defaults to [`crate::parser::DEFAULT_MAX_FRAME_SIZE`] (16 MiB). A frame
    /// exceeding this — an oversized `content-length`, or a body that never
    /// terminates — is rejected rather than buffered or allocated, so a
    /// malicious or buggy broker cannot exhaust client memory. Raise it if you
    /// legitimately exchange larger messages.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let options = ConnectOptions::default()
    ///     .max_frame_size(64 * 1024 * 1024);
    /// ```
    pub fn max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = Some(max_frame_size);
        self
    }

    /// Set a channel to receive heartbeat notifications (builder style).
    ///
    /// When set, the connection will send a `()` on this channel each time
    /// a heartbeat is received from the server. This is useful for CLI tools
    /// or monitoring applications that want to display heartbeat status.
    ///
    /// # Note
    ///
    /// Notifications are sent using `try_send()` to avoid blocking the
    /// connection's background task. If the channel buffer is full,
    /// notifications will be silently dropped. Use a sufficiently sized
    /// channel buffer (e.g., 16) to avoid missing notifications.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tokio::sync::mpsc;
    /// use iridium_stomp::ConnectOptions;
    ///
    /// let (tx, mut rx) = mpsc::channel(16);
    /// let options = ConnectOptions::default()
    ///     .heartbeat_notify(tx);
    ///
    /// // In another task:
    /// while rx.recv().await.is_some() {
    ///     println!("Heartbeat received!");
    /// }
    /// ```
    pub fn heartbeat_notify(mut self, tx: mpsc::Sender<()>) -> Self {
        self.heartbeat_tx = Some(tx);
        self
    }
}

/// Parse the STOMP `heart-beat` header value (format: "cx,cy").
///
/// Parameters
/// - `header`: header string from the server or client (for example
///   "10000,10000"). The values represent milliseconds.
///
/// Returns a tuple `(cx, cy)` where each value is the heartbeat interval in
/// milliseconds. Missing or invalid fields default to `0`.
pub fn parse_heartbeat_header(header: &str) -> (u64, u64) {
    let mut parts = header.split(',');
    let cx = parts
        .next()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let cy = parts
        .next()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);
    (cx, cy)
}

/// Negotiate heartbeat intervals between client and server.
///
/// Parameters
/// - `client_out`: client's desired outgoing heartbeat interval in
///   milliseconds (how often the client will send heartbeats).
/// - `client_in`: client's desired incoming heartbeat interval in
///   milliseconds (how often the client expects to receive heartbeats).
/// - `server_out`: server's advertised outgoing interval in milliseconds.
/// - `server_in`: server's advertised incoming interval in milliseconds.
///
/// Returns `(outgoing, incoming)` where each element is `Some(Duration)` if
/// heartbeats are enabled in that direction, or `None` if disabled. The
/// negotiated interval uses the STOMP rule of taking the maximum of the
/// corresponding client and server values.
pub fn negotiate_heartbeats(
    client_out: u64,
    client_in: u64,
    server_out: u64,
    server_in: u64,
) -> (Option<Duration>, Option<Duration>) {
    let negotiated_out_ms = std::cmp::max(client_out, server_in);
    let negotiated_in_ms = std::cmp::max(client_in, server_out);

    let outgoing = if negotiated_out_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(negotiated_out_ms))
    };
    let incoming = if negotiated_in_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(negotiated_in_ms))
    };
    (outgoing, incoming)
}

/// Extract the destination from an ERROR frame.
///
/// Tries multiple strategies:
/// 1. Check for a `destination` header (some brokers include it)
/// 2. Parse the error message/body for `/topic/...` or `/queue/...` patterns
///
/// Returns `None` if no destination can be identified.
fn extract_destination_from_error(frame: &Frame) -> Option<String> {
    // Strategy 1: Check for destination header
    if let Some(dest) = frame.get_header("destination") {
        return Some(dest.to_string());
    }

    // Strategy 2: Look for destination pattern in message header or body
    let message = frame.get_header("message").unwrap_or("");
    let body = String::from_utf8_lossy(&frame.body);

    // Combine message and body for searching
    let text = format!("{} {}", message, body);

    // Look for /topic/ or /queue/ patterns
    for prefix in ["/topic/", "/queue/"] {
        if let Some(start) = text.find(prefix) {
            // Extract until whitespace, comma, quote, or end of string
            let rest = &text[start..];
            let end = rest
                .find(|c: char| c.is_whitespace() || c == ',' || c == '"' || c == '\'')
                .unwrap_or(rest.len());
            if end > prefix.len() {
                return Some(rest[..end].to_string());
            }
        }
    }

    None
}

/// Extract subscription ID from an ERROR frame message.
///
/// Looks for patterns like "subscription 1" or "subscription sub-1" in the
/// error message or body. Artemis uses this format for subscription errors.
fn extract_subscription_id_from_error(frame: &Frame) -> Option<String> {
    let message = frame.get_header("message").unwrap_or("");
    let body = String::from_utf8_lossy(&frame.body);
    let text = format!("{} {}", message, body);

    // Look for "subscription X" pattern (Artemis format)
    if let Some(idx) = text.to_lowercase().find("subscription ") {
        let rest = &text[idx + 13..]; // "subscription " is 13 chars
        // Extract the subscription ID (could be numeric or alphanumeric like "sub-1")
        let end = rest
            .find(|c: char| c.is_whitespace() || c == ',' || c == '"' || c == '\'')
            .unwrap_or(rest.len());
        if end > 0 {
            return Some(rest[..end].to_string());
        }
    }

    None
}

/// Look up a destination by subscription ID in the subscriptions map.
async fn lookup_destination_by_sub_id(
    sub_id: &str,
    subscriptions: &Arc<Mutex<Subscriptions>>,
) -> Option<String> {
    let map = subscriptions.lock().await;
    for (dest, entries) in map.iter() {
        for entry in entries {
            if entry.id == sub_id {
                return Some(dest.clone());
            }
        }
    }
    None
}

/// High-level connection object that manages a single TCP/STOMP connection.
///
/// The `Connection` spawns a background task that maintains the TCP transport,
/// sends/receives STOMP frames using `StompCodec`, negotiates heartbeats, and
/// performs simple reconnect logic with exponential backoff.
#[derive(Clone)]
pub struct Connection {
    outbound_tx: mpsc::Sender<StompItem>,
    /// The inbound receiver is shared behind a mutex so the `Connection`
    /// handle may be cloned and callers can call `next_frame` concurrently.
    inbound_rx: Arc<Mutex<mpsc::Receiver<Frame>>>,
    shutdown_tx: broadcast::Sender<()>,
    /// Map of destination -> list of (subscription id, sender) for dispatching
    /// inbound MESSAGE frames to subscribers.
    subscriptions: Arc<Mutex<Subscriptions>>,
    /// Monotonic counter used to allocate subscription ids.
    sub_id_counter: Arc<AtomicU64>,
    /// Pending messages awaiting ACK/NACK from the application.
    ///
    /// Organized by subscription id. For `client` ack mode the ACK is
    /// cumulative: acknowledging message `M` for subscription `S` acknowledges
    /// all messages previously delivered for `S` up to and including `M`.
    /// For `client-individual` the ACK/NACK applies only to the single
    /// message.
    pending: Arc<Mutex<PendingMap>>,
    /// Pending receipt confirmations.
    ///
    /// When a frame is sent with a `receipt` header, the receipt-id is stored
    /// here with a oneshot sender. When the server responds with a RECEIPT
    /// frame, the sender is notified.
    pending_receipts: Arc<Mutex<PendingReceipts>>,
    /// How long `close` waits for the broker's RECEIPT after DISCONNECT.
    disconnect_timeout: Duration,
}

/// A pending receipt confirmation for a frame sent with
/// [`Connection::send_frame_with_receipt`].
///
/// The handle owns the receiving half of the confirmation channel from the
/// moment the frame is queued for sending. A RECEIPT that arrives before
/// [`wait`](ReceiptHandle::wait) is called is therefore buffered in the channel
/// rather than dropped, so the confirmation cannot be lost to a fast broker.
///
/// Handles are independent, so several frames may be sent before any of them is
/// awaited:
///
/// ```ignore
/// let mut handles = Vec::new();
/// for order in orders {
///     handles.push(conn.send_frame_with_receipt(order).await?);
/// }
/// for handle in handles {
///     handle.wait(Duration::from_secs(5)).await?;
/// }
/// ```
#[derive(Debug)]
pub struct ReceiptHandle {
    /// The client-generated receipt id carried by the sent frame.
    receipt_id: String,
    /// Resolves when the broker answers with RECEIPT or a matching ERROR.
    rx: oneshot::Receiver<Result<(), ServerError>>,
    /// Shared registry, used to deregister this receipt if the wait times out.
    pending_receipts: Arc<Mutex<PendingReceipts>>,
}

impl ReceiptHandle {
    /// The receipt id the client generated for this frame.
    ///
    /// This is the value sent in the frame's `receipt` header and echoed by the
    /// broker in the `receipt-id` header of its response.
    pub fn receipt_id(&self) -> &str {
        &self.receipt_id
    }

    /// Wait for the broker to confirm the frame.
    ///
    /// # Parameters
    /// - `timeout`: maximum time to wait for the broker's response.
    ///
    /// # Returns
    /// `Ok(())` if the broker sent a RECEIPT, `Err(ConnError::FrameRejected)`
    /// if it answered with an ERROR carrying this receipt id, or
    /// `Err(ConnError::ReceiptTimeout)` if the timeout expired first.
    pub async fn wait(self, timeout: Duration) -> Result<(), ConnError> {
        let ReceiptHandle {
            receipt_id,
            rx,
            pending_receipts,
        } = self;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(err))) => Err(ConnError::FrameRejected(err)),
            Ok(Err(_)) => {
                // Sender dropped without a response - connection likely gone.
                Err(ConnError::Protocol(
                    "receipt channel closed unexpectedly".into(),
                ))
            }
            Err(_) => {
                let mut receipts = pending_receipts.lock().await;
                receipts.remove(&receipt_id);
                Err(ConnError::ReceiptTimeout(receipt_id))
            }
        }
    }
}

impl Connection {
    /// Heartbeat value that disables heartbeats entirely.
    ///
    /// Use this when you don't want the client or server to send heartbeats.
    /// Note that some brokers may still require heartbeats for long-lived connections.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let conn = Connection::connect(
    ///     "localhost:61613",
    ///     "guest",
    ///     "guest",
    ///     Connection::NO_HEARTBEAT,
    /// ).await?;
    /// ```
    pub const NO_HEARTBEAT: &'static str = "0,0";

    /// Default heartbeat value: 10 seconds for both send and receive.
    ///
    /// This is a reasonable default for most applications. The actual heartbeat
    /// interval will be negotiated with the server (taking the maximum of client
    /// and server preferences).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let conn = Connection::connect(
    ///     "localhost:61613",
    ///     "guest",
    ///     "guest",
    ///     Connection::DEFAULT_HEARTBEAT,
    /// ).await?;
    /// ```
    pub const DEFAULT_HEARTBEAT: &'static str = "10000,10000";

    /// How long [`close`](Connection::close) waits for the broker to confirm the
    /// DISCONNECT before giving up and tearing the socket down anyway.
    ///
    /// Override per connection with
    /// [`ConnectOptions::disconnect_timeout`].
    pub const DEFAULT_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Establish a connection to the STOMP server at `addr` with the given
    /// credentials and heartbeat header string (e.g. "10000,10000").
    ///
    /// This is a convenience wrapper around `connect_with_options()` that uses
    /// default options (STOMP 1.2, host="/", no client-id).
    ///
    /// If the broker is unreachable, this method retries with exponential
    /// backoff (1s → 2s → 4s → … → 30s cap). Authentication errors
    /// (`ConnError::ServerRejected`) fail immediately. See
    /// [`connect_with_options`](Self::connect_with_options) for full details.
    ///
    /// Parameters
    /// - `addr`: TCP address (host:port) of the STOMP server.
    /// - `login`: login username for STOMP `CONNECT`.
    /// - `passcode`: passcode for STOMP `CONNECT`.
    /// - `client_hb`: client's `heart-beat` header value ("cx,cy" in
    ///   milliseconds) that will be sent in the `CONNECT` frame.
    ///
    /// Returns a `Connection` which provides `send`, `send_frame`,
    /// `next_frame`, and `close` helpers. The detailed connection handling
    /// (I/O, heartbeats, reconnects) runs on a background task spawned by
    /// this method.
    pub async fn connect(
        addr: &str,
        login: &str,
        passcode: &str,
        client_hb: &str,
    ) -> Result<Self, ConnError> {
        Self::connect_with_options(addr, login, passcode, client_hb, ConnectOptions::default())
            .await
    }

    /// Establish a connection to the STOMP server with custom options.
    ///
    /// Use this method when you need to set a custom `client-id` (for durable
    /// subscriptions), specify a virtual host, negotiate different STOMP
    /// versions, or add custom CONNECT headers.
    ///
    /// Parameters
    /// - `addr`: TCP address (host:port) of the STOMP server.
    /// - `login`: login username for STOMP `CONNECT`.
    /// - `passcode`: passcode for STOMP `CONNECT`.
    /// - `client_hb`: client's `heart-beat` header value ("cx,cy" in
    ///   milliseconds) that will be sent in the `CONNECT` frame.
    /// - `options`: custom connection options (version, host, client-id, etc.).
    ///
    /// # Connection Behavior
    ///
    /// If the broker is unreachable, the method retries with exponential
    /// backoff (1s → 2s → 4s → … → 30s cap) — the same strategy used for
    /// reconnection after a connection drop. This means your application can
    /// start before the broker is available and will connect once it comes up.
    ///
    /// This retry is unbounded by default. Set
    /// [`ConnectOptions::connect_timeout`] to cap it — useful for CLI tools and
    /// one-shot scripts that must not hang forever on a misconfigured address.
    ///
    /// # Errors
    ///
    /// Returns an error immediately (no retry) if:
    /// - The server rejects the connection, e.g., due to invalid credentials
    ///   (`ConnError::ServerRejected`)
    ///
    /// All other errors (TCP refused, connection closed mid-handshake, I/O
    /// failures) are retried with backoff.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iridium_stomp::{Connection, ConnectOptions};
    ///
    /// // Connect with a client-id for durable subscriptions
    /// let options = ConnectOptions::default()
    ///     .client_id("my-app-instance-1");
    ///
    /// let conn = Connection::connect_with_options(
    ///     "localhost:61613",
    ///     "guest",
    ///     "guest",
    ///     Connection::DEFAULT_HEARTBEAT,
    ///     options,
    /// ).await?;
    /// ```
    pub async fn connect_with_options(
        addr: &str,
        login: &str,
        passcode: &str,
        client_hb: &str,
        options: ConnectOptions,
    ) -> Result<Self, ConnError> {
        let (out_tx, mut out_rx) = mpsc::channel::<StompItem>(32);
        let (in_tx, in_rx) = mpsc::channel::<Frame>(32);
        let subscriptions: Arc<Mutex<Subscriptions>> = Arc::new(Mutex::new(HashMap::new()));
        let sub_id_counter = Arc::new(AtomicU64::new(1));
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = pending.clone();
        let pending_receipts: Arc<Mutex<PendingReceipts>> = Arc::new(Mutex::new(HashMap::new()));
        let pending_receipts_clone = pending_receipts.clone();

        let addr = addr.to_string();
        let login = login.to_string();
        let passcode = passcode.to_string();
        let client_hb = client_hb.to_string();

        // Extract options into owned values for the spawned task
        let accept_version = options.accept_version.unwrap_or_else(|| "1.2".to_string());
        let host = options.host.unwrap_or_else(|| "/".to_string());
        let client_id = options.client_id;
        let custom_headers = options.headers;
        let heartbeat_notify_tx = options.heartbeat_tx;
        let connect_timeout = options.connect_timeout;
        let max_frame_size = options.max_frame_size.unwrap_or(DEFAULT_MAX_FRAME_SIZE);

        // Perform initial connection and STOMP handshake before spawning
        // background task. Retries with exponential backoff on I/O and
        // protocol errors (broker unreachable or crashing mid-handshake)
        // using the same strategy as reconnection. Only ServerRejected
        // (authentication failure) fails immediately.
        //
        // `last_err` remembers the most recent failure so that, if a
        // `connect_timeout` bound elapses, the caller gets that error rather
        // than a bare "timed out". It is written by the retry arms and read
        // only after the attempt future below has been dropped.
        let mut backoff_secs: u64 = 1;
        let mut last_err: Option<ConnError> = None;
        let attempt = async {
            loop {
                let stream = match TcpStream::connect(&addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            addr = %addr,
                            error = %e,
                            backoff_secs,
                            "initial connect failed, retrying in {}s",
                            backoff_secs,
                        );
                        last_err = Some(ConnError::Io(e));
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(30);
                        continue;
                    }
                };
                let mut framed =
                    Framed::new(stream, StompCodec::with_max_frame_size(max_frame_size));

                let connect = Self::build_connect_frame(
                    &accept_version,
                    &host,
                    &login,
                    &passcode,
                    &client_hb,
                    &client_id,
                    &custom_headers,
                );

                if let Err(e) = framed.send(StompItem::Frame(connect)).await {
                    tracing::warn!(
                        addr = %addr,
                        error = %e,
                        backoff_secs,
                        "failed to send CONNECT frame, retrying in {}s",
                        backoff_secs,
                    );
                    last_err = Some(ConnError::Io(e));
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(30);
                    continue;
                }

                match Self::await_connected_response(&mut framed).await {
                    Ok(server_hb) => {
                        tracing::info!(addr = %addr, "connected to broker");
                        let (cx, cy) = parse_heartbeat_header(&client_hb);
                        let (sx, sy) = parse_heartbeat_header(&server_hb);
                        let (si, ri) = negotiate_heartbeats(cx, cy, sx, sy);
                        return Ok::<_, ConnError>((framed, si, ri));
                    }
                    // Auth errors fail immediately — bad config should not be retried
                    Err(e @ ConnError::ServerRejected(_)) => {
                        return Err(e);
                    }
                    // I/O and protocol errors during handshake (e.g., broker
                    // crashed or closed mid-handshake) — retry with backoff
                    Err(e) => {
                        tracing::warn!(
                            addr = %addr,
                            error = %e,
                            backoff_secs,
                            "handshake failed, retrying in {}s",
                            backoff_secs,
                        );
                        last_err = Some(e);
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(30);
                        continue;
                    }
                }
            }
        };

        let (framed, send_interval, recv_interval) = match connect_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, attempt).await {
                Ok(result) => result?,
                Err(_elapsed) => {
                    // The bound elapsed. Hand back the last error seen, or
                    // synthesize a timeout if the very first attempt was still
                    // in flight and had not recorded one yet.
                    return Err(last_err.unwrap_or_else(|| {
                        ConnError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("connect to {} timed out after {:?}", addr, timeout),
                        ))
                    }));
                }
            },
            None => attempt.await?,
        };

        // Now spawn background task for ongoing I/O and reconnection.
        //
        // Subscribe before spawning, and keep the one receiver for the task's
        // whole life. A broadcast drops a message when nobody is subscribed and
        // delivers only to receivers that existed when it was sent, so
        // subscribing inside the task would lose a shutdown signalled before the
        // task is first polled, and re-subscribing per iteration would discard
        // one that arrived during the reconnect backoff.
        let mut shutdown_sub = shutdown_tx.subscribe();
        let subscriptions_clone = subscriptions.clone();

        tokio::spawn(async move {
            let mut backoff_secs: u64 = 1;

            // Use the already-established connection for the first iteration
            let mut current_framed = Some(framed);
            let mut current_send_interval = send_interval;
            let mut current_recv_interval = recv_interval;
            let mut first_iteration = true;
            // Track subscription errors across reconnections. If a subscription
            // receives too many consecutive errors, we remove it to prevent
            // error loops (e.g., Artemis sending repeated permission errors).
            let mut subscription_errors: HashMap<String, u32> = HashMap::new();
            // Track subscription IDs that have been abandoned so we can ignore
            // subsequent errors for them.
            let mut abandoned_sub_ids: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            const SUBSCRIPTION_ERROR_THRESHOLD: u32 = 3;

            loop {
                // Check for shutdown before attempting connection
                tokio::select! {
                    biased;
                    _ = shutdown_sub.recv() => break,
                    _ = future::ready(()) => {},
                }

                // Either use existing connection or establish new one (reconnect)
                let framed = if let Some(f) = current_framed.take() {
                    f
                } else {
                    // Reconnection attempt
                    match TcpStream::connect(&addr).await {
                        Ok(stream) => {
                            let mut framed = Framed::new(
                                stream,
                                StompCodec::with_max_frame_size(max_frame_size),
                            );

                            let connect = Self::build_connect_frame(
                                &accept_version,
                                &host,
                                &login,
                                &passcode,
                                &client_hb,
                                &client_id,
                                &custom_headers,
                            );

                            if let Err(e) = framed.send(StompItem::Frame(connect)).await {
                                tracing::warn!(
                                    addr = %addr,
                                    error = %e,
                                    backoff_secs,
                                    "reconnect: failed to send CONNECT frame, retrying in {}s",
                                    backoff_secs,
                                );
                                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                                backoff_secs = (backoff_secs * 2).min(30);
                                continue;
                            }

                            match Self::await_connected_response(&mut framed).await {
                                Ok(server_hb) => {
                                    tracing::info!(addr = %addr, "reconnected to broker");
                                    let (cx, cy) = parse_heartbeat_header(&client_hb);
                                    let (sx, sy) = parse_heartbeat_header(&server_hb);
                                    let (si, ri) = negotiate_heartbeats(cx, cy, sx, sy);
                                    current_send_interval = si;
                                    current_recv_interval = ri;
                                    framed
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        addr = %addr,
                                        error = %e,
                                        backoff_secs,
                                        "reconnect: handshake failed, retrying in {}s",
                                        backoff_secs,
                                    );
                                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                                    backoff_secs = (backoff_secs * 2).min(30);
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                addr = %addr,
                                error = %e,
                                backoff_secs,
                                "reconnect: broker unreachable, retrying in {}s",
                                backoff_secs,
                            );
                            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                            backoff_secs = (backoff_secs * 2).min(30);
                            continue;
                        }
                    }
                };

                let (send_interval, recv_interval) = (current_send_interval, current_recv_interval);

                let last_received = Arc::new(AtomicU64::new(current_millis()));
                let writer_last_sent = Arc::new(AtomicU64::new(current_millis()));

                let (mut sink, mut stream) = framed.split();
                let in_tx = in_tx.clone();
                let subscriptions = subscriptions_clone.clone();

                // Clear pending message map on reconnect — messages that were
                // outstanding before the disconnect are considered lost and
                // will be redelivered by the server as appropriate.
                {
                    let mut p = pending_clone.lock().await;
                    p.clear();
                }

                // Resubscribe any existing subscriptions after reconnect.
                // We snapshot the subscription entries while holding the lock
                // and then issue SUBSCRIBE frames using the sink.
                if first_iteration {
                    first_iteration = false;
                } else {
                    let subs_snapshot: Vec<ResubEntry> = {
                        let map = subscriptions.lock().await;
                        let mut v: Vec<ResubEntry> = Vec::new();
                        for (dest, vec) in map.iter() {
                            for entry in vec.iter() {
                                v.push((
                                    dest.clone(),
                                    entry.id.clone(),
                                    entry.ack.clone(),
                                    entry.headers.clone(),
                                ));
                            }
                        }
                        v
                    };

                    for (dest, id, ack, headers) in subs_snapshot {
                        let mut sf = Frame::new("SUBSCRIBE");
                        sf = sf
                            .header("id", &id)
                            .header("destination", &dest)
                            .header("ack", &ack);
                        for (k, v) in headers {
                            sf = sf.header(&k, &v);
                        }
                        if let Err(e) = sink.send(StompItem::Frame(sf)).await {
                            tracing::warn!(
                                destination = %dest,
                                subscription_id = %id,
                                error = %e,
                                "failed to resubscribe after reconnect",
                            );
                        }
                    }
                }
                let mut hb_tick = match send_interval {
                    Some(d) => tokio::time::interval(d),
                    None => tokio::time::interval(Duration::from_secs(86400)),
                };
                let watchdog_half = recv_interval.map(|d| d / 2);

                let conn_start = tokio::time::Instant::now();

                // Set when the branch below takes the shutdown signal. The
                // reconnect check after this loop cannot re-read it from
                // `shutdown_sub`, which is drained by then.
                let mut shutting_down = false;

                'conn: loop {
                    tokio::select! {
                        _ = shutdown_sub.recv() => { let _ = sink.close().await; shutting_down = true; break 'conn; }
                        maybe = out_rx.recv() => {
                            match maybe {
                                Some(item) => if sink.send(item).await.is_err() { break 'conn } else { writer_last_sent.store(current_millis(), Ordering::SeqCst); }
                                None => break 'conn,
                            }
                        }
                        item = stream.next() => {
                            match item {
                                Some(Ok(StompItem::Heartbeat)) => {
                                    last_received.store(current_millis(), Ordering::SeqCst);
                                    if let Some(ref tx) = heartbeat_notify_tx {
                                        let _ = tx.try_send(());
                                    }
                                }
                                Some(Ok(StompItem::Frame(f))) => {
                                    last_received.store(current_millis(), Ordering::SeqCst);
                                    // Dispatch MESSAGE frames to any matching subscribers.
                                    if f.command == "MESSAGE" {
                                        // try to find destination, subscription and message-id headers
                                        let mut dest_opt: Option<String> = None;
                                        let mut sub_opt: Option<String> = None;
                                        let mut msg_id_opt: Option<String> = None;
                                        for (k, v) in &f.headers {
                                            let kl = k.to_lowercase();
                                            if kl == "destination" {
                                                dest_opt = Some(v.clone());
                                            } else if kl == "subscription" {
                                                sub_opt = Some(v.clone());
                                            } else if kl == "message-id" {
                                                msg_id_opt = Some(v.clone());
                                            }
                                        }

                                        // Determine whether we need to track this message as pending
                                        let mut need_pending = false;
                                        if let Some(sub_id) = &sub_opt {
                                            let map = subscriptions.lock().await;
                                            for vec in map.values() {
                                                for entry in vec.iter() {
                                                    if &entry.id == sub_id && entry.ack != "auto" {
                                                        need_pending = true;
                                                    }
                                                }
                                            }
                                        } else if let Some(dest) = &dest_opt {
                                            let map = subscriptions.lock().await;
                                            if let Some(vec) = map.get(dest) {
                                                for entry in vec.iter() {
                                                    if entry.ack != "auto" {
                                                        need_pending = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }

                                        // If required, add to pending map (per-subscription) before
                                        // delivery so ACK/NACK requests from the application can
                                        // reference the message. We require a `message-id` header
                                        // to track messages; if missing, we cannot support ACK/NACK.
                                        if let Some(msg_id) = msg_id_opt.clone().filter(|_| need_pending) {
                                            // If the server provided a subscription id in the
                                            // MESSAGE, store pending under that subscription.
                                            if let Some(sub_id) = &sub_opt {
                                                let mut p = pending_clone.lock().await;
                                                let q = p
                                                    .entry(sub_id.clone())
                                                    .or_insert_with(VecDeque::new);
                                                q.push_back((msg_id.clone(), f.clone()));
                                            } else if let Some(dest) = &dest_opt {
                                                // Destination-based delivery: add the message to
                                                // the pending queue for each matching
                                                // subscription on that destination.
                                                let map = subscriptions.lock().await;
                                                if let Some(vec) = map.get(dest) {
                                                    let mut p = pending_clone.lock().await;
                                                    for entry in vec.iter() {
                                                        let q = p
                                                            .entry(entry.id.clone())
                                                            .or_insert_with(VecDeque::new);
                                                        q.push_back((msg_id.clone(), f.clone()));
                                                    }
                                                }
                                            }
                                        }

                                        // Deliver to subscribers.
                                        if let Some(sub_id) = sub_opt {
                                            let mut map = subscriptions.lock().await;
                                            for vec in map.values_mut() {
                                                vec.retain(|entry| {
                                                    if entry.id == sub_id {
                                                        let _ = entry.sender.try_send(f.clone());
                                                        true
                                                    } else {
                                                        true
                                                    }
                                                });
                                            }
                                        } else if let Some(dest) = dest_opt {
                                            let mut map = subscriptions.lock().await;
                                            if let Some(vec) = map.get_mut(&dest) {
                                                vec.retain(|entry| entry.sender.try_send(f.clone()).is_ok());
                                            }
                                        }
                                    } else if f.command == "RECEIPT" {
                                        // Handle RECEIPT frame: notify any waiting callers
                                        if let Some(receipt_id) = f.get_header("receipt-id") {
                                            let mut receipts = pending_receipts_clone.lock().await;
                                            if let Some(sender) = receipts.remove(receipt_id) {
                                                let _ = sender.send(Ok(()));
                                            }
                                        }
                                        // Don't forward RECEIPT frames to inbound channel
                                        continue;
                                    } else if f.command == "ERROR" {
                                        // An ERROR with receipt-id is the failed response to a
                                        // frame that requested a receipt. Wake that waiter with
                                        // the broker error, then continue forwarding the frame.
                                        if let Some(receipt_id) = f.get_header("receipt-id") {
                                            let mut receipts =
                                                pending_receipts_clone.lock().await;
                                            if let Some(sender) = receipts.remove(receipt_id) {
                                                let _ = sender
                                                    .send(Err(ServerError::from_frame(f.clone())));
                                            }
                                        }

                                        // Track subscription-related errors. If we see repeated
                                        // errors for the same destination, remove the subscription
                                        // to prevent error loops.
                                        //
                                        // First, check if this error is for an already-abandoned
                                        // subscription (Artemis keeps sending errors after we abandon).
                                        let sub_id = extract_subscription_id_from_error(&f);
                                        if let Some(ref id) = sub_id
                                            && abandoned_sub_ids.contains(id)
                                        {
                                            // Skip this error - subscription already abandoned
                                            continue;
                                        }

                                        // Try to identify the destination:
                                        // 1. Extract directly from ERROR frame
                                        // 2. Look up by subscription ID (Artemis uses "subscription N")
                                        let dest = if let Some(d) = extract_destination_from_error(&f)
                                        {
                                            Some(d)
                                        } else if let Some(ref id) = sub_id {
                                            lookup_destination_by_sub_id(id, &subscriptions).await
                                        } else {
                                            None
                                        };

                                        if let Some(dest) = dest {
                                            let count = {
                                                let c = subscription_errors
                                                    .entry(dest.clone())
                                                    .or_insert(0);
                                                *c += 1;
                                                *c
                                            };

                                            if count >= SUBSCRIPTION_ERROR_THRESHOLD {
                                                // Remove the subscription from auto-resubscribe
                                                let mut map = subscriptions.lock().await;
                                                if map.remove(&dest).is_some() {
                                                    // Track the subscription ID as abandoned
                                                    if let Some(id) = sub_id {
                                                        abandoned_sub_ids.insert(id);
                                                    }
                                                    // Send abandonment notification
                                                    let msg = format!(
                                                        "Subscription abandoned: {} errors for {}",
                                                        count, dest
                                                    );
                                                    let abandon_frame = Frame::new("ERROR")
                                                        .header("message", &msg)
                                                        .header("destination", &dest)
                                                        .header("x-abandoned", "true");
                                                    let _ = in_tx.send(abandon_frame).await;
                                                }
                                            }
                                        }
                                    }

                                    let _ = in_tx.send(f).await;
                                }
                                Some(Err(_)) | None => break 'conn,
                            }
                        }
                        _ = hb_tick.tick() => {
                            if let Some(dur) = send_interval {
                                let last = writer_last_sent.load(Ordering::SeqCst);
                                if current_millis().saturating_sub(last) >= dur.as_millis() as u64 {
                                    if sink.send(StompItem::Heartbeat).await.is_err() { break 'conn; }
                                    writer_last_sent.store(current_millis(), Ordering::SeqCst);
                                }
                            }
                        }
                        _ = async { if let Some(interval) = watchdog_half { tokio::time::sleep(interval).await } else { future::pending::<()>().await } } => {
                            if let Some(recv_dur) = recv_interval {
                                let last = last_received.load(Ordering::SeqCst);
                                if current_millis().saturating_sub(last) > (recv_dur.as_millis() as u64 * 2) {
                                    let _ = sink.close().await; break 'conn;
                                }
                            }
                        }
                    }
                }

                if shutting_down || shutdown_sub.try_recv().is_ok() {
                    break;
                }
                let stable_duration = conn_start.elapsed();
                if stable_duration >= Duration::from_secs(backoff_secs.max(5)) {
                    // Connection was stable — reset backoff
                    backoff_secs = 1;
                    tracing::info!(
                        addr = %addr,
                        stable_secs = stable_duration.as_secs(),
                        "connection dropped after stable session, reconnecting in 1s",
                    );
                } else {
                    // Connection died quickly — increase backoff
                    backoff_secs = (backoff_secs * 2).min(30);
                    tracing::warn!(
                        addr = %addr,
                        stable_secs = stable_duration.as_secs(),
                        backoff_secs,
                        "connection dropped quickly, reconnecting in {}s",
                        backoff_secs,
                    );
                }
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            }
        });

        Ok(Connection {
            outbound_tx: out_tx,
            inbound_rx: Arc::new(Mutex::new(in_rx)),
            shutdown_tx,
            subscriptions,
            sub_id_counter,
            pending,
            pending_receipts,
            disconnect_timeout: options
                .disconnect_timeout
                .unwrap_or(Self::DEFAULT_DISCONNECT_TIMEOUT),
        })
    }

    /// Build a CONNECT frame with all specified headers.
    fn build_connect_frame(
        accept_version: &str,
        host: &str,
        login: &str,
        passcode: &str,
        heartbeat: &str,
        client_id: &Option<String>,
        custom_headers: &[(String, String)],
    ) -> Frame {
        let mut connect = Frame::new("CONNECT")
            .header("accept-version", accept_version)
            .header("host", host)
            .header("login", login)
            .header("passcode", passcode)
            .header("heart-beat", heartbeat);

        if let Some(id) = client_id {
            connect = connect.header("client-id", id);
        }

        // Reserved headers that custom_headers cannot override
        let reserved = [
            "accept-version",
            "host",
            "login",
            "passcode",
            "heart-beat",
            "client-id",
        ];

        for (k, v) in custom_headers {
            if !reserved.contains(&k.to_lowercase().as_str()) {
                connect = connect.header(k, v);
            }
        }

        connect
    }

    /// Wait for CONNECTED or ERROR response from the server.
    ///
    /// Returns the server's heartbeat header value on success, or an error
    /// if the server sends an ERROR frame or closes the connection.
    async fn await_connected_response(
        framed: &mut Framed<TcpStream, StompCodec>,
    ) -> Result<String, ConnError> {
        loop {
            match framed.next().await {
                Some(Ok(StompItem::Frame(f))) => {
                    if f.command == "CONNECTED" {
                        // Extract heartbeat from server
                        let server_hb = f.get_header("heart-beat").unwrap_or("0,0").to_string();
                        return Ok(server_hb);
                    } else if f.command == "ERROR" {
                        // Server rejected connection (e.g., invalid credentials)
                        return Err(ConnError::ServerRejected(ServerError::from_frame(f)));
                    }
                    // Ignore other frames during CONNECT phase
                }
                Some(Ok(StompItem::Heartbeat)) => {
                    // Ignore heartbeats during handshake
                    continue;
                }
                Some(Err(e)) => {
                    return Err(ConnError::Io(e));
                }
                None => {
                    return Err(ConnError::Protocol(
                        "connection closed before CONNECTED received".to_string(),
                    ));
                }
            }
        }
    }

    /// Send a text message to a destination.
    ///
    /// This is a convenience wrapper around [`send_frame`](Self::send_frame)
    /// for the common case of sending a string payload with no extra headers.
    ///
    /// # Example
    /// ```ignore
    /// conn.send("/queue/test", "hello").await?;
    /// ```
    pub async fn send(&self, destination: &str, body: impl AsRef<str>) -> Result<(), ConnError> {
        let frame = Frame::new("SEND")
            .header("destination", destination)
            .set_body(body.as_ref().as_bytes().to_vec());
        self.send_frame(frame).await
    }

    /// Send an arbitrary STOMP frame to the broker.
    ///
    /// Use this when you need full control over the frame (custom headers,
    /// binary body, receipt requests, etc.). For simple text messages, prefer
    /// [`send`](Self::send).
    pub async fn send_frame(&self, frame: Frame) -> Result<(), ConnError> {
        // Send a frame to the background writer task.
        //
        // Parameters
        // - `frame`: ownership of the `Frame` to send. The frame is converted
        //   into a `StompItem::Frame` and sent over the internal mpsc channel.
        self.outbound_tx
            .send(StompItem::Frame(frame))
            .await
            .map_err(|_| ConnError::Protocol("send channel closed".into()))
    }

    /// Generate a unique receipt ID.
    fn generate_receipt_id() -> String {
        static RECEIPT_COUNTER: AtomicU64 = AtomicU64::new(1);
        format!("rcpt-{}", RECEIPT_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    /// Send a frame with a receipt request and return a handle to its
    /// confirmation.
    ///
    /// This method adds a unique `receipt` header to the frame, registers the
    /// receipt id for tracking, and returns a [`ReceiptHandle`]. Call
    /// [`ReceiptHandle::wait`] to await the broker's RECEIPT response.
    ///
    /// Any `receipt` header already present on `frame` is ignored in favour of
    /// the generated id; use [`ReceiptHandle::receipt_id`] to read it back.
    ///
    /// The returned handle owns the confirmation channel from the moment the
    /// frame is queued, so the broker's response cannot arrive before there is
    /// somewhere to put it. Awaiting may be deferred freely - see
    /// [`ReceiptHandle`] for sending several frames before awaiting any.
    ///
    /// # Parameters
    /// - `frame`: the frame to send. A `receipt` header will be added.
    ///
    /// # Returns
    /// A [`ReceiptHandle`] for the sent frame.
    ///
    /// # Example
    /// ```ignore
    /// let handle = conn.send_frame_with_receipt(frame).await?;
    /// handle.wait(Duration::from_secs(5)).await?;
    /// ```
    pub async fn send_frame_with_receipt(&self, frame: Frame) -> Result<ReceiptHandle, ConnError> {
        let receipt_id = Self::generate_receipt_id();

        // Create the oneshot channel for notification. The receiver goes into
        // the returned handle, so it stays alive for as long as the caller
        // cares about the response.
        let (tx, rx) = oneshot::channel();

        // Register the pending receipt before sending, so the background task
        // can never see the response with nothing registered for it.
        {
            let mut receipts = self.pending_receipts.lock().await;
            receipts.insert(receipt_id.clone(), tx);
        }

        // Drop any caller-supplied receipt header before adding ours. `Frame::header`
        // appends rather than overwrites, so leaving one in place would put two
        // receipt headers on the wire; brokers honour the first, which would never
        // match the id registered above. Matched case-insensitively, as header
        // lookup is.
        let mut frame = frame;
        frame
            .headers
            .retain(|(key, _)| !key.eq_ignore_ascii_case("receipt"));

        // Add receipt header and send the frame
        let frame_with_receipt = frame.receipt(&receipt_id);
        if let Err(err) = self.send_frame(frame_with_receipt).await {
            // The frame never went out; deregister rather than leave an entry
            // that nothing will ever answer.
            let mut receipts = self.pending_receipts.lock().await;
            receipts.remove(&receipt_id);
            return Err(err);
        }

        Ok(ReceiptHandle {
            receipt_id,
            rx,
            pending_receipts: self.pending_receipts.clone(),
        })
    }

    /// Send a frame and wait for server confirmation via RECEIPT.
    ///
    /// This is a convenience method that combines
    /// [`send_frame_with_receipt`](Connection::send_frame_with_receipt) and
    /// [`ReceiptHandle::wait`]. Use this when you want to ensure a frame was
    /// processed by the server before continuing.
    ///
    /// Any `receipt` header already present on `frame` is ignored in favour of
    /// a generated id. Reach for `send_frame_with_receipt` directly when you
    /// need to send several frames before awaiting any of them.
    ///
    /// # Parameters
    /// - `frame`: the frame to send.
    /// - `timeout`: maximum time to wait for the receipt.
    ///
    /// # Returns
    /// `Ok(())` if the frame was sent and receipt confirmed, or an error if
    /// sending failed or the receipt timed out.
    ///
    /// # Example
    /// ```ignore
    /// let frame = Frame::new("SEND")
    ///     .header("destination", "/queue/orders")
    ///     .set_body(b"order data".to_vec());
    ///
    /// conn.send_frame_confirmed(frame, Duration::from_secs(5)).await?;
    /// println!("Order sent and confirmed!");
    /// ```
    pub async fn send_frame_confirmed(
        &self,
        frame: Frame,
        timeout: Duration,
    ) -> Result<(), ConnError> {
        self.send_frame_with_receipt(frame)
            .await?
            .wait(timeout)
            .await
    }

    /// Subscribe to a destination.
    ///
    /// Parameters
    /// - `destination`: the STOMP destination to subscribe to (e.g. "/queue/foo").
    /// - `ack`: acknowledgement mode to request from the server.
    ///
    /// Returns a tuple `(subscription_id, receiver)` where `subscription_id` is
    /// the opaque id assigned locally for this subscription and `receiver` is a
    /// `mpsc::Receiver<Frame>` which will yield incoming MESSAGE frames for the
    /// destination. The caller should read from the receiver to handle messages.
    /// Subscribe to a destination using optional extra headers.
    ///
    /// This variant accepts additional headers which are stored locally and
    /// re-sent on reconnect. Use `subscribe` as a convenience wrapper when no
    /// extra headers are needed.
    pub async fn subscribe_with_headers(
        &self,
        destination: &str,
        ack: AckMode,
        extra_headers: Vec<(String, String)>,
    ) -> Result<crate::subscription::Subscription, ConnError> {
        let id = self
            .sub_id_counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string();
        let (tx, rx) = mpsc::channel::<Frame>(16);
        {
            let mut map = self.subscriptions.lock().await;
            map.entry(destination.to_string())
                .or_insert_with(Vec::new)
                .push(SubscriptionEntry {
                    id: id.clone(),
                    sender: tx.clone(),
                    ack: ack.as_str().to_string(),
                    headers: extra_headers.clone(),
                });
        }

        let mut f = Frame::new("SUBSCRIBE");
        f = f
            .header("id", &id)
            .header("destination", destination)
            .header("ack", ack.as_str());
        for (k, v) in &extra_headers {
            f = f.header(k, v);
        }
        self.outbound_tx
            .send(StompItem::Frame(f))
            .await
            .map_err(|_| ConnError::Protocol("send channel closed".into()))?;

        Ok(crate::subscription::Subscription::new(
            id,
            destination.to_string(),
            rx,
            self.clone(),
        ))
    }

    /// Convenience wrapper without extra headers.
    pub async fn subscribe(
        &self,
        destination: &str,
        ack: AckMode,
    ) -> Result<crate::subscription::Subscription, ConnError> {
        self.subscribe_with_headers(destination, ack, Vec::new())
            .await
    }

    /// Subscribe with a typed `SubscriptionOptions` structure.
    ///
    /// `SubscriptionOptions.headers` are forwarded to the broker and persisted
    /// for automatic resubscribe after reconnect. Durable subscriptions are
    /// requested through those headers - see the module docs for
    /// [`SubscriptionOptions`](crate::subscription::SubscriptionOptions).
    pub async fn subscribe_with_options(
        &self,
        destination: &str,
        ack: AckMode,
        options: crate::subscription::SubscriptionOptions,
    ) -> Result<crate::subscription::Subscription, ConnError> {
        self.subscribe_with_headers(destination, ack, options.headers)
            .await
    }

    /// Unsubscribe a previously created subscription by its local subscription id.
    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), ConnError> {
        let mut found = false;
        {
            let mut map = self.subscriptions.lock().await;
            let mut remove_keys: Vec<String> = Vec::new();
            for (dest, vec) in map.iter_mut() {
                if let Some(pos) = vec.iter().position(|entry| entry.id == subscription_id) {
                    vec.remove(pos);
                    found = true;
                }
                if vec.is_empty() {
                    remove_keys.push(dest.clone());
                }
            }
            for k in remove_keys {
                map.remove(&k);
            }
        }

        if !found {
            return Err(ConnError::Protocol("subscription id not found".into()));
        }

        let mut f = Frame::new("UNSUBSCRIBE");
        f = f.header("id", subscription_id);
        self.outbound_tx
            .send(StompItem::Frame(f))
            .await
            .map_err(|_| ConnError::Protocol("send channel closed".into()))?;

        Ok(())
    }

    /// Acknowledge a message previously received in `client` or
    /// `client-individual` ack modes.
    ///
    /// STOMP ack semantics:
    /// - `auto`: server considers message delivered immediately; the client
    ///   should not ack.
    /// - `client`: cumulative acknowledgements. ACKing message `M` for
    ///   subscription `S` acknowledges all messages delivered to `S` up to
    ///   and including `M`.
    /// - `client-individual`: only the named message is acknowledged.
    ///
    /// Parameters
    /// - `subscription_id`: the local subscription id returned by
    ///   `Connection::subscribe`. This disambiguates which subscription's
    ///   pending queue to advance for cumulative ACKs.
    /// - `message_id`: the `message-id` header value from the received
    ///   MESSAGE frame to acknowledge.
    ///
    /// Behavior
    /// - The pending queue for `subscription_id` is searched for `message_id`.
    ///   If the subscription used `client` ack mode, all pending messages up to
    ///   and including the matched message are removed. If the subscription
    ///   used `client-individual`, only the matched message is removed.
    /// - An `ACK` frame is sent to the server with `id=<message_id>` and
    ///   `subscription=<subscription_id>` headers.
    #[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
    pub async fn ack(&self, subscription_id: &str, message_id: &str) -> Result<(), ConnError> {
        // Remove from the local pending queue according to subscription ack mode.
        let mut removed_any = false;
        {
            let mut p = self.pending.lock().await;
            if let Some(queue) = p.get_mut(subscription_id) {
                if let Some(pos) = queue.iter().position(|(mid, _)| mid == message_id) {
                    // Determine ack mode for this subscription (default to client).
                    let mut ack_mode = "client".to_string();
                    {
                        let map = self.subscriptions.lock().await;
                        'outer: for vec in map.values() {
                            for entry in vec.iter() {
                                if entry.id == subscription_id {
                                    ack_mode = entry.ack.clone();
                                    break 'outer;
                                }
                            }
                        }
                    }

                    if ack_mode == "client" {
                        // cumulative: remove up to and including pos
                        for _ in 0..=pos {
                            queue.pop_front();
                            removed_any = true;
                        }
                    } else if queue.remove(pos).is_some() {
                        // client-individual: remove only the specific message
                        removed_any = true;
                    }

                    if queue.is_empty() {
                        p.remove(subscription_id);
                    }
                }
            }
        }

        // Send ACK to server (include subscription header for clarity)
        let mut f = Frame::new("ACK");
        f = f
            .header("id", message_id)
            .header("subscription", subscription_id);
        self.outbound_tx
            .send(StompItem::Frame(f))
            .await
            .map_err(|_| ConnError::Protocol("send channel closed".into()))?;

        // If message wasn't found locally, still send ACK to server; server
        // may ignore or treat it as no-op.
        let _ = removed_any;
        Ok(())
    }

    /// Negative-acknowledge a message (NACK).
    ///
    /// Parameters
    /// - `subscription_id`: the local subscription id the message was delivered under.
    /// - `message_id`: the `message-id` header value from the received MESSAGE.
    ///
    /// Behavior
    /// - Removes the message from the local pending queue (cumulatively if the
    ///   subscription used `client` ack mode, otherwise only the single
    ///   message). Sends a `NACK` frame to the server with `id` and
    ///   `subscription` headers.
    #[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
    pub async fn nack(&self, subscription_id: &str, message_id: &str) -> Result<(), ConnError> {
        // Mirror ack removal semantics for pending map.
        let mut removed_any = false;
        {
            let mut p = self.pending.lock().await;
            if let Some(queue) = p.get_mut(subscription_id) {
                if let Some(pos) = queue.iter().position(|(mid, _)| mid == message_id) {
                    let mut ack_mode = "client".to_string();
                    {
                        let map = self.subscriptions.lock().await;
                        'outer2: for vec in map.values() {
                            for entry in vec.iter() {
                                if entry.id == subscription_id {
                                    ack_mode = entry.ack.clone();
                                    break 'outer2;
                                }
                            }
                        }
                    }

                    if ack_mode == "client" {
                        for _ in 0..=pos {
                            queue.pop_front();
                            removed_any = true;
                        }
                    } else if queue.remove(pos).is_some() {
                        removed_any = true;
                    }

                    if queue.is_empty() {
                        p.remove(subscription_id);
                    }
                }
            }
        }

        let mut f = Frame::new("NACK");
        f = f
            .header("id", message_id)
            .header("subscription", subscription_id);
        self.outbound_tx
            .send(StompItem::Frame(f))
            .await
            .map_err(|_| ConnError::Protocol("send channel closed".into()))?;

        let _ = removed_any;
        Ok(())
    }

    /// Helper to send a transaction frame (BEGIN, COMMIT, or ABORT).
    async fn send_transaction_frame(
        &self,
        command: &str,
        transaction_id: &str,
    ) -> Result<(), ConnError> {
        let f = Frame::new(command).header("transaction", transaction_id);
        self.outbound_tx
            .send(StompItem::Frame(f))
            .await
            .map_err(|_| ConnError::Protocol("send channel closed".into()))
    }

    /// Begin a transaction.
    ///
    /// Parameters
    /// - `transaction_id`: unique identifier for the transaction. The caller is
    ///   responsible for ensuring uniqueness within the connection.
    ///
    /// Behavior
    /// - Sends a `BEGIN` frame to the server with `transaction:<transaction_id>`
    ///   header. Subsequent `SEND`, `ACK`, and `NACK` frames may include this
    ///   transaction id to group them into the transaction. The transaction must
    ///   be finalized with either `commit` or `abort`.
    pub async fn begin(&self, transaction_id: &str) -> Result<(), ConnError> {
        self.send_transaction_frame("BEGIN", transaction_id).await
    }

    /// Commit a transaction.
    ///
    /// Parameters
    /// - `transaction_id`: the transaction identifier previously passed to `begin`.
    ///
    /// Behavior
    /// - Sends a `COMMIT` frame to the server with `transaction:<transaction_id>`
    ///   header. All operations within the transaction are applied atomically.
    pub async fn commit(&self, transaction_id: &str) -> Result<(), ConnError> {
        self.send_transaction_frame("COMMIT", transaction_id).await
    }

    /// Abort a transaction.
    ///
    /// Parameters
    /// - `transaction_id`: the transaction identifier previously passed to `begin`.
    ///
    /// Behavior
    /// - Sends an `ABORT` frame to the server with `transaction:<transaction_id>`
    ///   header. All operations within the transaction are discarded.
    pub async fn abort(&self, transaction_id: &str) -> Result<(), ConnError> {
        self.send_transaction_frame("ABORT", transaction_id).await
    }

    /// Receive the next frame from the server.
    ///
    /// Returns `Some(ReceivedFrame::Frame(..))` for normal frames (MESSAGE, etc.),
    /// `Some(ReceivedFrame::Error(..))` for ERROR frames, or `None` if the
    /// connection has been closed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iridium_stomp::ReceivedFrame;
    ///
    /// while let Some(received) = conn.next_frame().await {
    ///     match received {
    ///         ReceivedFrame::Frame(frame) => {
    ///             println!("Got {}: {:?}", frame.command, frame.body);
    ///         }
    ///         ReceivedFrame::Error(err) => {
    ///             eprintln!("Server error: {}", err);
    ///             break;
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn next_frame(&self) -> Option<ReceivedFrame> {
        let mut rx = self.inbound_rx.lock().await;
        let frame = rx.recv().await?;

        // Convert ERROR frames to ServerError for better ergonomics
        if frame.command == "ERROR" {
            Some(ReceivedFrame::Error(ServerError::from_frame(frame)))
        } else {
            Some(ReceivedFrame::Frame(frame))
        }
    }

    /// Gracefully shut down the connection.
    ///
    /// Performs the STOMP 1.2 shutdown sequence: sends a DISCONNECT frame
    /// carrying a `receipt` header, waits for the broker's RECEIPT, then stops
    /// the background task and closes the socket.
    ///
    /// Because frames are written in the order they are submitted, and the
    /// broker only answers a DISCONNECT once it has processed what came before,
    /// a confirmed close also proves that everything previously sent on this
    /// connection reached the broker.
    ///
    /// # Returns
    /// `Ok(())` once the broker has confirmed the DISCONNECT.
    /// `Err(ConnError::ReceiptTimeout)` if it did not answer within the
    /// disconnect timeout, or another `ConnError` if the DISCONNECT could not be
    /// submitted or was rejected.
    ///
    /// **The connection is torn down either way.** An error reports that the
    /// shutdown was not clean - that the broker may not have run whatever it
    /// does on a protocol-level disconnect, such as transactional rollback or
    /// durable subscription cleanup - not that the connection is still open.
    /// Callers with nothing to do about that may discard the result.
    ///
    /// The wait is bounded by [`ConnectOptions::disconnect_timeout`], defaulting
    /// to [`DEFAULT_DISCONNECT_TIMEOUT`](Connection::DEFAULT_DISCONNECT_TIMEOUT),
    /// so an unresponsive broker cannot make `close` hang.
    ///
    /// # Example
    /// ```ignore
    /// // Report an unclean shutdown.
    /// conn.close().await?;
    ///
    /// // Or shut down best-effort.
    /// let _ = conn.close().await;
    /// ```
    pub async fn close(self) -> Result<(), ConnError> {
        // Queued frames are written before this one, and the broker answers the
        // receipt only after processing them, so awaiting it drains the outbound
        // queue rather than abandoning it at the shutdown signal below.
        let result = match self.send_frame_with_receipt(Frame::new("DISCONNECT")).await {
            Ok(handle) => handle.wait(self.disconnect_timeout).await,
            Err(err) => Err(err),
        };

        // Tear down regardless of how the broker answered: the caller asked for
        // the connection to close, and `result` reports only whether it got to
        // do so cleanly.
        let _ = self.shutdown_tx.send(());

        result
    }
}

fn current_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;
    use tokio::sync::mpsc;

    // Helper to build a MESSAGE frame with given message-id and subscription/destination headers
    fn make_message(
        message_id: &str,
        subscription: Option<&str>,
        destination: Option<&str>,
    ) -> Frame {
        let mut f = Frame::new("MESSAGE");
        f = f.header("message-id", message_id);
        if let Some(s) = subscription {
            f = f.header("subscription", s);
        }
        if let Some(d) = destination {
            f = f.header("destination", d);
        }
        f
    }

    fn read_stomp_frame(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
            if byte[0] == 0 {
                break;
            }
        }
        String::from_utf8(bytes).unwrap()
    }

    /// Connect with a short disconnect timeout. These stubs do not answer a
    /// DISCONNECT, so the default would make every teardown wait out the full
    /// receipt timeout.
    async fn connect_for_test(addr: &str) -> Connection {
        Connection::connect_with_options(
            addr,
            "guest",
            "guest",
            "0,0",
            ConnectOptions::default().disconnect_timeout(Duration::from_millis(50)),
        )
        .await
        .unwrap()
    }

    /// Read a header from a raw frame the way a broker does: the first
    /// occurrence wins, and the name is matched case-insensitively.
    fn header_value<'a>(frame: &'a str, name: &str) -> &'a str {
        frame
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name).then_some(value)
            })
            .unwrap()
    }

    fn start_receipt_rejection_server(
        message: &'static str,
        body: &'static str,
        response_delay: Duration,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _connect = read_stomp_frame(&mut stream);
                stream
                    .write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0")
                    .unwrap();
                stream.flush().unwrap();

                let send_frame = read_stomp_frame(&mut stream);
                let receipt_id = header_value(&send_frame, "receipt").to_string();
                thread::sleep(response_delay);
                let error_frame = format!(
                    "ERROR\nreceipt-id:{}\nmessage:{}\n\n{}\0",
                    receipt_id, message, body
                );
                stream.write_all(error_frame.as_bytes()).unwrap();
                stream.flush().unwrap();

                thread::sleep(Duration::from_millis(100));
            }
        });

        (addr, handle)
    }

    fn start_receipt_success_server(
        response_delay: Duration,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _connect = read_stomp_frame(&mut stream);
                stream
                    .write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0")
                    .unwrap();
                stream.flush().unwrap();

                let send_frame = read_stomp_frame(&mut stream);
                let receipt_id = header_value(&send_frame, "receipt").to_string();
                thread::sleep(response_delay);
                let receipt_frame = format!("RECEIPT\nreceipt-id:{}\n\n\0", receipt_id);
                stream.write_all(receipt_frame.as_bytes()).unwrap();
                stream.flush().unwrap();

                thread::sleep(Duration::from_millis(100));
            }
        });

        (addr, handle)
    }

    #[tokio::test]
    async fn connect_timeout_bounds_an_unreachable_broker() {
        // Bind, then drop the listener so the port is closed. A connect there
        // is refused immediately, and without a bound the retry loop would back
        // off and try again forever (#68). The bound must cut it short and hand
        // back the last I/O error it saw, not hang.
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        };

        let start = std::time::Instant::now();
        let result = Connection::connect_with_options(
            &addr.to_string(),
            "guest",
            "guest",
            "0,0",
            ConnectOptions::default().connect_timeout(Duration::from_millis(200)),
        )
        .await;
        let elapsed = start.elapsed();

        // `Connection` is not `Debug`, so inspect the error side only.
        let err = result.err();
        assert!(
            matches!(err, Some(ConnError::Io(_))),
            "expected an Io error, got {:?}",
            err
        );
        // The first refusal is instant; the bound fires during the 1s backoff
        // sleep that follows, well under a second.
        assert!(
            elapsed < Duration::from_secs(1),
            "connect should have given up near the 200ms bound, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn connect_timeout_surfaces_the_last_handshake_error() {
        // A broker that accepts the TCP connection then closes without ever
        // sending CONNECTED — a TLS-only port, say. The handshake fails with
        // ConnError::Protocol ("closed before CONNECTED received"), and the
        // bound must surface that real reason rather than a synthesized
        // timeout.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _connect = read_stomp_frame(&mut stream);
                // Drop the stream without answering: closed mid-handshake.
            }
        });

        let result = Connection::connect_with_options(
            &addr.to_string(),
            "guest",
            "guest",
            "0,0",
            ConnectOptions::default().connect_timeout(Duration::from_millis(200)),
        )
        .await;

        let err = result.err();
        assert!(
            matches!(err, Some(ConnError::Protocol(_))),
            "expected the handshake Protocol error, got {:?}",
            err
        );
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn connect_timeout_does_not_disturb_a_reachable_broker() {
        // A generous bound must not interfere with a broker that answers
        // promptly: the connection still succeeds.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _connect = read_stomp_frame(&mut stream);
                stream
                    .write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0")
                    .unwrap();
                stream.flush().unwrap();
                thread::sleep(Duration::from_millis(100));
            }
        });

        let result = Connection::connect_with_options(
            &addr.to_string(),
            "guest",
            "guest",
            "0,0",
            ConnectOptions::default()
                .connect_timeout(Duration::from_secs(5))
                .disconnect_timeout(Duration::from_millis(50)),
        )
        .await;

        assert!(
            result.is_ok(),
            "expected a connection, got error {:?}",
            result.as_ref().err()
        );
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn receipt_arriving_before_the_wait_is_not_lost() {
        // The broker answers with no delay, so the RECEIPT lands while the
        // caller is still holding the handle. Before #82 the background task
        // fired an orphaned sender and removed the registration, so the wait
        // below timed out on a frame the broker had already confirmed.
        let (addr, server) = start_receipt_success_server(Duration::ZERO);
        let addr = addr.to_string();

        let conn = connect_for_test(&addr).await;

        let handle = conn
            .send_frame_with_receipt(
                Frame::new("SEND")
                    .header("destination", "/queue/fast")
                    .set_body(b"payload".to_vec()),
            )
            .await
            .unwrap();

        // Let the response arrive and be dispatched before anyone awaits it.
        tokio::time::sleep(Duration::from_millis(200)).await;

        handle
            .wait(Duration::from_secs(2))
            .await
            .expect("RECEIPT delivered before the wait began must still resolve it");

        let _ = conn.close().await;
        server.join().unwrap();
    }

    #[tokio::test]
    async fn caller_supplied_receipt_header_is_replaced_not_appended() {
        // The stub echoes back the first receipt header it reads. If a
        // caller-set header survived, the broker would answer with that id
        // while the client tracked the generated one, and the wait would time
        // out. Mixed case, since header lookup is case-insensitive.
        let (addr, server) = start_receipt_success_server(Duration::ZERO);
        let addr = addr.to_string();

        let conn = connect_for_test(&addr).await;

        let handle = conn
            .send_frame_with_receipt(
                Frame::new("SEND")
                    .header("destination", "/queue/test")
                    .header("Receipt", "caller-set")
                    .set_body(b"payload".to_vec()),
            )
            .await
            .unwrap();

        assert_ne!(handle.receipt_id(), "caller-set");

        handle
            .wait(Duration::from_secs(2))
            .await
            .expect("generated receipt id must be the only one on the wire");

        let _ = conn.close().await;
        server.join().unwrap();
    }

    #[tokio::test]
    async fn error_with_receipt_id_rejects_confirmed_send_and_stays_inbound() {
        let (addr, server) =
            start_receipt_rejection_server("publish denied", "not allowed", Duration::ZERO);
        let addr = addr.to_string();

        let conn = connect_for_test(&addr).await;

        let result = conn
            .send_frame_confirmed(
                Frame::new("SEND")
                    .header("destination", "/queue/forbidden")
                    .set_body(b"payload".to_vec()),
                Duration::from_secs(2),
            )
            .await;

        let rejected_receipt_id = match result {
            Err(ConnError::FrameRejected(err)) => {
                assert_eq!(err.message, "publish denied");
                assert_eq!(err.body, Some("not allowed".to_string()));
                err.receipt_id
                    .expect("frame rejection should include receipt-id")
            }
            other => panic!("expected FrameRejected, got {:?}", other),
        };

        let received = tokio::time::timeout(Duration::from_secs(1), conn.next_frame())
            .await
            .unwrap();
        match received {
            Some(ReceivedFrame::Error(err)) => {
                assert_eq!(err.message, "publish denied");
                assert_eq!(err.body, Some("not allowed".to_string()));
                assert_eq!(err.receipt_id, Some(rejected_receipt_id));
            }
            other => panic!("expected forwarded ERROR frame, got {:?}", other),
        }

        let _ = conn.close().await;
        server.join().unwrap();
    }

    #[tokio::test]
    async fn wait_for_receipt_reports_frame_rejection_and_keeps_error_inbound() {
        let (addr, server) = start_receipt_rejection_server(
            "receipt rejected",
            "permission denied",
            Duration::from_millis(100),
        );
        let addr = addr.to_string();

        let conn = connect_for_test(&addr).await;

        let handle = conn
            .send_frame_with_receipt(
                Frame::new("SEND")
                    .header("destination", "/queue/forbidden")
                    .set_body(b"payload".to_vec()),
            )
            .await
            .unwrap();

        let receipt_id = handle.receipt_id().to_string();
        let result = handle.wait(Duration::from_secs(2)).await;

        match result {
            Err(ConnError::FrameRejected(err)) => {
                assert_eq!(err.message, "receipt rejected");
                assert_eq!(err.body, Some("permission denied".to_string()));
                assert_eq!(err.receipt_id, Some(receipt_id.clone()));
            }
            other => panic!("expected FrameRejected, got {:?}", other),
        }

        let received = tokio::time::timeout(Duration::from_secs(1), conn.next_frame())
            .await
            .unwrap();
        match received {
            Some(ReceivedFrame::Error(err)) => {
                assert_eq!(err.message, "receipt rejected");
                assert_eq!(err.body, Some("permission denied".to_string()));
                assert_eq!(err.receipt_id, Some(receipt_id));
            }
            other => panic!("expected forwarded ERROR frame, got {:?}", other),
        }

        let _ = conn.close().await;
        server.join().unwrap();
    }

    #[tokio::test]
    async fn test_cumulative_ack_removes_prefix() {
        // setup channels
        let (out_tx, mut out_rx) = mpsc::channel::<StompItem>(8);
        let (_in_tx, in_rx) = mpsc::channel::<Frame>(8);
        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let subscriptions: Arc<Mutex<Subscriptions>> = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));

        let sub_id_counter = Arc::new(AtomicU64::new(1));

        // create a subscription entry s1 with client (cumulative) ack
        let (sub_sender, _sub_rx) = mpsc::channel::<Frame>(4);
        {
            let mut map = subscriptions.lock().await;
            map.insert(
                "/queue/x".to_string(),
                vec![SubscriptionEntry {
                    id: "s1".to_string(),
                    sender: sub_sender,
                    ack: "client".to_string(),
                    headers: Vec::new(),
                }],
            );
        }

        // fill pending queue for s1: m1,m2,m3
        {
            let mut p = pending.lock().await;
            let mut q = VecDeque::new();
            q.push_back((
                "m1".to_string(),
                make_message("m1", Some("s1"), Some("/queue/x")),
            ));
            q.push_back((
                "m2".to_string(),
                make_message("m2", Some("s1"), Some("/queue/x")),
            ));
            q.push_back((
                "m3".to_string(),
                make_message("m3", Some("s1"), Some("/queue/x")),
            ));
            p.insert("s1".to_string(), q);
        }

        let conn = Connection {
            outbound_tx: out_tx,
            inbound_rx: Arc::new(Mutex::new(in_rx)),
            shutdown_tx,
            subscriptions: subscriptions.clone(),
            sub_id_counter,
            pending: pending.clone(),
            pending_receipts: Arc::new(Mutex::new(HashMap::new())),
            disconnect_timeout: Connection::DEFAULT_DISCONNECT_TIMEOUT,
        };

        // ack m2 cumulatively: should remove m1 and m2, leaving m3
        conn.ack("s1", "m2").await.expect("ack failed");

        // verify pending for s1 contains only m3
        {
            let p = pending.lock().await;
            let q = p.get("s1").expect("missing s1");
            assert_eq!(q.len(), 1);
            assert_eq!(q.front().unwrap().0, "m3");
        }

        // verify an ACK frame was emitted
        if let Some(item) = out_rx.recv().await {
            match item {
                StompItem::Frame(f) => assert_eq!(f.command, "ACK"),
                _ => panic!("expected frame"),
            }
        } else {
            panic!("no outbound frame sent")
        }
    }

    #[tokio::test]
    async fn test_client_individual_ack_removes_only_one() {
        // setup channels
        let (out_tx, mut out_rx) = mpsc::channel::<StompItem>(8);
        let (_in_tx, in_rx) = mpsc::channel::<Frame>(8);
        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let subscriptions: Arc<Mutex<Subscriptions>> = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));

        let sub_id_counter = Arc::new(AtomicU64::new(1));

        // create a subscription entry s2 with client-individual ack
        let (sub_sender, _sub_rx) = mpsc::channel::<Frame>(4);
        {
            let mut map = subscriptions.lock().await;
            map.insert(
                "/queue/y".to_string(),
                vec![SubscriptionEntry {
                    id: "s2".to_string(),
                    sender: sub_sender,
                    ack: "client-individual".to_string(),
                    headers: Vec::new(),
                }],
            );
        }

        // fill pending queue for s2: a,b,c
        {
            let mut p = pending.lock().await;
            let mut q = VecDeque::new();
            q.push_back((
                "a".to_string(),
                make_message("a", Some("s2"), Some("/queue/y")),
            ));
            q.push_back((
                "b".to_string(),
                make_message("b", Some("s2"), Some("/queue/y")),
            ));
            q.push_back((
                "c".to_string(),
                make_message("c", Some("s2"), Some("/queue/y")),
            ));
            p.insert("s2".to_string(), q);
        }

        let conn = Connection {
            outbound_tx: out_tx,
            inbound_rx: Arc::new(Mutex::new(in_rx)),
            shutdown_tx,
            subscriptions: subscriptions.clone(),
            sub_id_counter,
            pending: pending.clone(),
            pending_receipts: Arc::new(Mutex::new(HashMap::new())),
            disconnect_timeout: Connection::DEFAULT_DISCONNECT_TIMEOUT,
        };

        // ack only 'b' individually
        conn.ack("s2", "b").await.expect("ack failed");

        // verify pending for s2 contains a and c
        {
            let p = pending.lock().await;
            let q = p.get("s2").expect("missing s2");
            assert_eq!(q.len(), 2);
            assert_eq!(q[0].0, "a");
            assert_eq!(q[1].0, "c");
        }

        // verify an ACK frame was emitted
        if let Some(item) = out_rx.recv().await {
            match item {
                StompItem::Frame(f) => assert_eq!(f.command, "ACK"),
                _ => panic!("expected frame"),
            }
        } else {
            panic!("no outbound frame sent")
        }
    }

    #[tokio::test]
    async fn test_subscription_receive_delivers_message() {
        // setup channels
        let (out_tx, _out_rx) = mpsc::channel::<StompItem>(8);
        let (_in_tx, in_rx) = mpsc::channel::<Frame>(8);
        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let subscriptions: Arc<Mutex<Subscriptions>> = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));

        let sub_id_counter = Arc::new(AtomicU64::new(1));

        let conn = Connection {
            outbound_tx: out_tx,
            inbound_rx: Arc::new(Mutex::new(in_rx)),
            shutdown_tx,
            subscriptions: subscriptions.clone(),
            sub_id_counter,
            pending: pending.clone(),
            pending_receipts: Arc::new(Mutex::new(HashMap::new())),
            disconnect_timeout: Connection::DEFAULT_DISCONNECT_TIMEOUT,
        };

        // subscribe
        let subscription = conn
            .subscribe("/queue/test", AckMode::Auto)
            .await
            .expect("subscribe failed");

        // find the sender stored in the subscriptions map and push a message
        {
            let map = conn.subscriptions.lock().await;
            let vec = map.get("/queue/test").expect("missing subscription vec");
            let sender = &vec[0].sender;
            let f = make_message("m1", Some(&vec[0].id), Some("/queue/test"));
            sender.try_send(f).expect("send to subscription failed");
        }

        // consume from the subscription receiver
        let mut rx = subscription.into_receiver();
        if let Some(received) = rx.recv().await {
            assert_eq!(received.command, "MESSAGE");
            // message-id header should be present
            let mut found = false;
            for (k, _v) in &received.headers {
                if k.to_lowercase() == "message-id" {
                    found = true;
                    break;
                }
            }
            assert!(found, "message-id header missing");
        } else {
            panic!("no message received on subscription")
        }
    }

    #[tokio::test]
    async fn test_subscription_ack_removes_pending_and_sends_ack() {
        // setup channels
        let (out_tx, mut out_rx) = mpsc::channel::<StompItem>(8);
        let (_in_tx, in_rx) = mpsc::channel::<Frame>(8);
        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let subscriptions: Arc<Mutex<Subscriptions>> = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));

        let sub_id_counter = Arc::new(AtomicU64::new(1));

        let conn = Connection {
            outbound_tx: out_tx,
            inbound_rx: Arc::new(Mutex::new(in_rx)),
            shutdown_tx,
            subscriptions: subscriptions.clone(),
            sub_id_counter,
            pending: pending.clone(),
            pending_receipts: Arc::new(Mutex::new(HashMap::new())),
            disconnect_timeout: Connection::DEFAULT_DISCONNECT_TIMEOUT,
        };

        // subscribe with client ack
        let subscription = conn
            .subscribe("/queue/ack", AckMode::Client)
            .await
            .expect("subscribe failed");

        let sub_id = subscription.id().to_string();

        // drain any initial outbound frames (SUBSCRIBE) emitted by subscribe()
        while out_rx.try_recv().is_ok() {}

        // populate pending queue for this subscription
        {
            let mut p = conn.pending.lock().await;
            let mut q = VecDeque::new();
            q.push_back((
                "mid-1".to_string(),
                make_message("mid-1", Some(&sub_id), Some("/queue/ack")),
            ));
            p.insert(sub_id.clone(), q);
        }

        // ack the message via the subscription helper
        subscription.ack("mid-1").await.expect("ack failed");

        // ensure pending queue no longer contains the message
        {
            let p = conn.pending.lock().await;
            assert!(p.get(&sub_id).is_none() || p.get(&sub_id).unwrap().is_empty());
        }

        // verify an ACK frame was emitted
        if let Some(item) = out_rx.recv().await {
            match item {
                StompItem::Frame(f) => assert_eq!(f.command, "ACK"),
                _ => panic!("expected frame"),
            }
        } else {
            panic!("no outbound frame sent")
        }
    }

    // Helper function to create a test connection and output receiver
    fn setup_test_connection() -> (Connection, mpsc::Receiver<StompItem>) {
        let (out_tx, out_rx) = mpsc::channel::<StompItem>(8);
        let (_in_tx, in_rx) = mpsc::channel::<Frame>(8);
        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let subscriptions: Arc<Mutex<Subscriptions>> = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));
        let sub_id_counter = Arc::new(AtomicU64::new(1));

        let conn = Connection {
            outbound_tx: out_tx,
            inbound_rx: Arc::new(Mutex::new(in_rx)),
            shutdown_tx,
            subscriptions,
            sub_id_counter,
            pending,
            pending_receipts: Arc::new(Mutex::new(HashMap::new())),
            disconnect_timeout: Connection::DEFAULT_DISCONNECT_TIMEOUT,
        };

        (conn, out_rx)
    }

    // Helper function to verify a frame with a transaction header
    fn verify_transaction_frame(frame: Frame, expected_command: &str, expected_tx_id: &str) {
        assert_eq!(frame.command, expected_command);
        assert!(
            frame
                .headers
                .iter()
                .any(|(k, v)| k == "transaction" && v == expected_tx_id),
            "transaction header with id '{}' not found",
            expected_tx_id
        );
    }

    #[tokio::test]
    async fn test_begin_transaction_sends_frame() {
        let (conn, mut out_rx) = setup_test_connection();

        conn.begin("tx1").await.expect("begin failed");

        // verify BEGIN frame was emitted
        if let Some(StompItem::Frame(f)) = out_rx.recv().await {
            verify_transaction_frame(f, "BEGIN", "tx1");
        } else {
            panic!("no outbound frame sent")
        }
    }

    #[tokio::test]
    async fn test_commit_transaction_sends_frame() {
        let (conn, mut out_rx) = setup_test_connection();

        conn.commit("tx1").await.expect("commit failed");

        // verify COMMIT frame was emitted
        if let Some(StompItem::Frame(f)) = out_rx.recv().await {
            verify_transaction_frame(f, "COMMIT", "tx1");
        } else {
            panic!("no outbound frame sent")
        }
    }

    #[tokio::test]
    async fn test_abort_transaction_sends_frame() {
        let (conn, mut out_rx) = setup_test_connection();

        conn.abort("tx1").await.expect("abort failed");

        // verify ABORT frame was emitted
        if let Some(StompItem::Frame(f)) = out_rx.recv().await {
            verify_transaction_frame(f, "ABORT", "tx1");
        } else {
            panic!("no outbound frame sent")
        }
    }

    #[tokio::test]
    async fn test_send_convenience_produces_correct_frame() {
        let (conn, mut out_rx) = setup_test_connection();

        conn.send("/queue/events", "hello world")
            .await
            .expect("send failed");

        if let Some(StompItem::Frame(f)) = out_rx.recv().await {
            assert_eq!(f.command, "SEND");
            assert_eq!(f.get_header("destination"), Some("/queue/events"));
            assert_eq!(f.body, b"hello world");
        } else {
            panic!("no outbound frame sent")
        }
    }

    #[test]
    fn test_extract_destination_from_error_header() {
        // When ERROR frame has destination header, extract it directly
        let frame = Frame::new("ERROR")
            .header("message", "AMQ339016: Error creating STOMP subscription")
            .header("destination", "/topic/test.restricted");

        let dest = extract_destination_from_error(&frame);
        assert_eq!(dest, Some("/topic/test.restricted".to_string()));
    }

    #[test]
    fn test_extract_destination_from_error_message() {
        // When destination is in message header text
        let frame = Frame::new("ERROR").header(
            "message",
            "AMQ339016: Error creating subscription for /topic/test.restricted",
        );

        let dest = extract_destination_from_error(&frame);
        assert_eq!(dest, Some("/topic/test.restricted".to_string()));
    }

    #[test]
    fn test_extract_destination_from_error_body() {
        // When destination is in body text
        let frame = Frame::new("ERROR")
            .header("message", "AMQ339016: Error creating subscription")
            .set_body(b"User guest is not authorized for /queue/orders".to_vec());

        let dest = extract_destination_from_error(&frame);
        assert_eq!(dest, Some("/queue/orders".to_string()));
    }

    #[test]
    fn test_extract_destination_from_error_none() {
        // When no destination can be identified
        let frame = Frame::new("ERROR").header("message", "Generic error without destination info");

        let dest = extract_destination_from_error(&frame);
        assert_eq!(dest, None);
    }

    #[test]
    fn test_extract_destination_from_error_with_trailing_punct() {
        // When destination has trailing punctuation
        let frame = Frame::new("ERROR").header(
            "message",
            "Error for /topic/events, please check permissions",
        );

        let dest = extract_destination_from_error(&frame);
        assert_eq!(dest, Some("/topic/events".to_string()));
    }

    #[test]
    fn test_extract_subscription_id_from_error_artemis_format() {
        // Artemis format: "AMQ339016 Error creating subscription 1"
        let frame =
            Frame::new("ERROR").header("message", "AMQ339016 Error creating subscription 1");

        let sub_id = extract_subscription_id_from_error(&frame);
        assert_eq!(sub_id, Some("1".to_string()));
    }

    #[test]
    fn test_extract_subscription_id_from_error_numeric() {
        // Multiple digit subscription ID
        let frame = Frame::new("ERROR").header("message", "Error for subscription 123 on server");

        let sub_id = extract_subscription_id_from_error(&frame);
        assert_eq!(sub_id, Some("123".to_string()));
    }

    #[test]
    fn test_extract_subscription_id_from_error_none() {
        // No subscription ID in error
        let frame = Frame::new("ERROR").header("message", "Generic connection error");

        let sub_id = extract_subscription_id_from_error(&frame);
        assert_eq!(sub_id, None);
    }

    #[tokio::test]
    async fn test_lookup_destination_by_sub_id() {
        let subscriptions: Arc<Mutex<Subscriptions>> = Arc::new(Mutex::new(HashMap::new()));
        let (sender, _rx) = mpsc::channel::<Frame>(4);

        // Add a subscription
        {
            let mut map = subscriptions.lock().await;
            map.insert(
                "/topic/test.restricted".to_string(),
                vec![SubscriptionEntry {
                    id: "1".to_string(),
                    sender,
                    ack: "auto".to_string(),
                    headers: Vec::new(),
                }],
            );
        }

        // Should find the destination
        let dest = lookup_destination_by_sub_id("1", &subscriptions).await;
        assert_eq!(dest, Some("/topic/test.restricted".to_string()));

        // Should not find non-existent subscription
        let dest = lookup_destination_by_sub_id("999", &subscriptions).await;
        assert_eq!(dest, None);
    }
}
