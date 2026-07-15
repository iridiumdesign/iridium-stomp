#![doc = include_str!("../README.md")]

//! Additional user-facing guides from the `docs/` directory are exposed as
//! rustdoc modules so they appear on docs.rs. See the `subscriptions_docs`
//! module for information about durable subscriptions and `SubscriptionOptions`.
/// STOMP wire-protocol codec for use with `tokio_util::codec::Framed`.
pub mod codec;
/// Connection lifecycle, retry, reconnection, and the main `Connection` API.
pub mod connection;
/// STOMP frame representation (command, headers, body).
pub mod frame;
/// Low-level STOMP frame parser (byte-level decoding).
pub mod parser;
/// Subscription handles and configuration types.
pub mod subscription;

/// Re-export the codec types (`StompCodec`, `StompItem`) for easy use with
/// `tokio_util::codec::Framed` and tests.
pub use codec::{StompCodec, StompItem};

/// Re-export the high-level `Connection`, `AckMode`, `ConnectOptions`, `ConnError`,
/// `Heartbeat`, `ReceiptHandle`, `ReceivedFrame`, `ServerError`, and the heartbeat
/// helper functions.
pub use connection::{
    AckMode, ConnError, ConnectOptions, Connection, Heartbeat, ReceiptHandle, ReceivedFrame,
    ServerError, negotiate_heartbeats, parse_heartbeat_header,
};

/// Re-export the `Frame` type used to construct/send and receive frames.
pub use frame::Frame;
pub use subscription::Subscription;
pub use subscription::SubscriptionOptions;

// Expose the repository `docs/subscriptions.md` as a public rustdoc page so it
// appears alongside the API docs on docs.rs / rustdoc. The module is empty and
// only serves to carry the included markdown.
#[doc = include_str!("../docs/subscriptions.md")]
pub mod subscriptions_docs {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_frame_display() {
        let f = Frame::new("CONNECT")
            .header("accept-version", "1.2")
            .set_body(b"hello".to_vec());
        let s = format!("{}", f);
        assert!(s.contains("CONNECT"));
        assert!(s.contains("Body (5 bytes)"));
    }
}
