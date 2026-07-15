use crate::connection::ConnError;
use crate::connection::Connection;
use crate::frame::Frame;
use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// Options to configure a subscription. `headers` are forwarded to the
/// broker as-is when sending the SUBSCRIBE frame and persisted locally so
/// they can be re-sent on reconnect. This allows broker-specific durable
/// subscription extensions to be used (for example ActiveMQ's durable
/// subscription headers) while keeping the library generic.
///
/// # Durability
///
/// Durability is requested through `headers`, using whatever header the broker
/// defines for it - STOMP itself has no durable-subscription concept. For
/// example, ActiveMQ uses `activemq.subscriptionName`:
///
/// ```ignore
/// let options = SubscriptionOptions {
///     headers: vec![(
///         "activemq.subscriptionName".to_string(),
///         "my-durable-sub".to_string(),
///     )],
/// };
/// ```
///
/// On brokers where a durable queue is declared administratively, such as
/// RabbitMQ, pass that queue as the `destination` argument; nothing extra is
/// needed here.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionOptions {
    /// Extra headers to include on the SUBSCRIBE frame.
    pub headers: Vec<(String, String)>,
}

/// A lightweight handle returned from `Connection::subscribe` that packages the
/// subscription id, destination, and the receiving side of the subscription.
///
/// The `Subscription` provides convenience helpers for acknowledging or
/// negative-acknowledging messages; these delegate to the underlying
/// `Connection` handle.
pub struct Subscription {
    id: String,
    destination: String,
    receiver: mpsc::Receiver<Frame>,
    conn: Connection,
}

impl Subscription {
    pub(crate) fn new(
        id: String,
        destination: String,
        receiver: mpsc::Receiver<Frame>,
        conn: Connection,
    ) -> Self {
        Self {
            id,
            destination,
            receiver,
            conn,
        }
    }

    /// Returns the local subscription id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the destination this subscription listens to.
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Consume the `Subscription` and return the underlying receiver so the
    /// caller can drive message handling directly.
    pub fn into_receiver(self) -> mpsc::Receiver<Frame> {
        self.receiver
    }

    /// Acknowledge a message by its `message-id` header. Delegates to
    /// `Connection::ack` using the local subscription id.
    pub async fn ack(&self, message_id: &str) -> Result<(), ConnError> {
        self.conn.ack(&self.id, message_id).await
    }

    /// Negative-acknowledge a message by its `message-id` header.
    pub async fn nack(&self, message_id: &str) -> Result<(), ConnError> {
        self.conn.nack(&self.id, message_id).await
    }

    /// Consume the subscription and unsubscribe from the server.
    ///
    /// This is a convenience that calls `Connection::unsubscribe` with the
    /// local subscription id and drops the receiver.
    pub async fn unsubscribe(self) -> Result<(), ConnError> {
        self.conn.unsubscribe(&self.id).await
    }
}

impl Stream for Subscription {
    type Item = Frame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safe to get a mutable reference because all fields of `Subscription`
        // are `Unpin` (String, Receiver, Connection). We then delegate to the
        // tokio mpsc receiver's `poll_recv` which returns `Poll<Option<T>>`.
        let this = self.get_mut();
        Pin::new(&mut this.receiver).poll_recv(cx)
    }
}
