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
    /// Set once the subscription has been unsubscribed explicitly, or once the
    /// caller has taken ownership of the receiver via [`into_receiver`]. Guards
    /// `Drop` against sending a second UNSUBSCRIBE, or any UNSUBSCRIBE when the
    /// caller has chosen to keep driving the stream itself.
    ///
    /// [`into_receiver`]: Subscription::into_receiver
    unsubscribed: bool,
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
            unsubscribed: false,
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
    ///
    /// The subscription stays active: the caller now owns the stream, so `Drop`
    /// does not send an UNSUBSCRIBE. Call [`Connection::unsubscribe`] with the
    /// id if you later want to stop it.
    ///
    /// [`Connection::unsubscribe`]: crate::Connection::unsubscribe
    pub fn into_receiver(mut self) -> mpsc::Receiver<Frame> {
        // The caller keeps the stream, so suppress the drop-time UNSUBSCRIBE.
        self.unsubscribed = true;
        // `Subscription` implements `Drop`, so the receiver cannot be moved out
        // directly (E0509). Swap in a throwaway and return the real one; the
        // dummy is dropped harmlessly when `self` drops.
        let (_dummy_tx, dummy_rx) = mpsc::channel(1);
        std::mem::replace(&mut self.receiver, dummy_rx)
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
    /// local subscription id and drops the receiver. It confirms nothing beyond
    /// queuing the UNSUBSCRIBE; the returned error means either the id was no
    /// longer registered locally (for example the subscription had already been
    /// abandoned after repeated broker errors) or the frame could not be queued.
    /// Dropping the handle instead does the same thing on a best-effort basis
    /// (see the `Drop` impl).
    pub async fn unsubscribe(mut self) -> Result<(), ConnError> {
        // Mark first so the upcoming drop does not send a second UNSUBSCRIBE.
        self.unsubscribed = true;
        self.conn.unsubscribe(&self.id).await
    }
}

impl Drop for Subscription {
    /// Best-effort UNSUBSCRIBE when the handle is dropped without an explicit
    /// [`unsubscribe`](Subscription::unsubscribe).
    ///
    /// A dropped handle means the caller is done receiving, so the broker-side
    /// subscription should stop rather than linger and keep delivering (and be
    /// replayed on reconnect). `Drop` cannot `.await`, so this is best-effort
    /// via [`Connection::unsubscribe_best_effort`]. It is skipped when the
    /// subscription was already unsubscribed or when the receiver was handed off
    /// through [`into_receiver`](Subscription::into_receiver).
    fn drop(&mut self) {
        if self.unsubscribed {
            return;
        }
        self.conn.unsubscribe_best_effort(&self.id);
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
