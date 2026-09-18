use crate::connection::ConnError;
use crate::connection::Connection;
use crate::frame::Frame;
use futures::stream::Stream;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
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
/// ```
/// use iridium_stomp::SubscriptionOptions;
///
/// let options = SubscriptionOptions::new()
///     .header("activemq.subscriptionName", "my-durable-sub");
/// ```
///
/// On brokers where a durable queue is declared administratively, such as
/// RabbitMQ, pass that queue as the `destination` argument; nothing extra is
/// needed here.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SubscriptionOptions {
    /// Extra headers to include on the SUBSCRIBE frame.
    pub headers: Vec<(String, String)>,
    /// Capacity of the channel between the connection's background task and
    /// this subscription's receiver, in frames. `None` (the default) means
    /// [`DEFAULT_CHANNEL_CAPACITY`](Self::DEFAULT_CHANNEL_CAPACITY); zero is
    /// treated as one.
    ///
    /// When the channel is full, further messages are parked inside the
    /// connection and moved into the channel, in order, as the consumer frees
    /// room, so a consumer that is merely slow loses nothing. A larger
    /// capacity only means fewer frames take the detour. How much may be
    /// parked is set by [`overflow_limit`](Self::overflow_limit).
    pub channel_capacity: Option<usize>,
    /// Most messages that may be parked for this subscription behind a full
    /// channel. `None` (the default) means
    /// [`DEFAULT_OVERFLOW_LIMIT`](Self::DEFAULT_OVERFLOW_LIMIT). It counts
    /// parked frames only, not those in the channel, and applies in every ack
    /// mode. Zero means no parking: the first message to find the channel full
    /// trips the limit.
    ///
    /// A message that would take the parked queue past the limit fails the
    /// subscription, because a consumer that far behind is treated as stalled
    /// and the alternative is to grow until the process runs out of memory.
    /// The library then:
    ///
    /// - logs the failure with `tracing::error!`;
    /// - sends UNSUBSCRIBE and forgets the subscription, so it is not
    ///   resubscribed after a reconnect;
    /// - discards the parked messages and the one that tripped the limit. In
    ///   the `client` and `client-individual` ack modes none of them was
    ///   acknowledged, so the broker redelivers them to the next subscriber.
    ///   In `auto` mode the broker already counts them delivered and they are
    ///   lost;
    /// - ends the [`Subscription`] stream: it yields what was already in its
    ///   channel and then `None`;
    /// - puts a synthetic ERROR frame on [`Connection::next_frame`] carrying
    ///   `x-overflow: true` and the `destination` and `subscription` headers.
    ///
    /// In the client ack modes the broker's flow control normally keeps the
    /// queue far below the default.
    pub overflow_limit: Option<usize>,
}

impl SubscriptionOptions {
    /// Channel capacity used when `channel_capacity` is `None`, and by
    /// `Connection::subscribe` and `Connection::subscribe_with_headers`.
    pub const DEFAULT_CHANNEL_CAPACITY: usize = 16;

    /// Overflow limit used when `overflow_limit` is `None`, and by
    /// `Connection::subscribe` and `Connection::subscribe_with_headers`.
    pub const DEFAULT_OVERFLOW_LIMIT: usize = 1024;

    /// Create a new `SubscriptionOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one extra header to the SUBSCRIBE frame (builder style).
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Set extra headers to include on the SUBSCRIBE frame.
    pub fn headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }

    /// Set the capacity of the subscription's channel. See
    /// [`channel_capacity`](Self::channel_capacity).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = Some(capacity);
        self
    }

    /// Set how many messages may be parked behind a full channel before the
    /// subscription is failed. See [`overflow_limit`](Self::overflow_limit).
    pub fn overflow_limit(mut self, limit: usize) -> Self {
        self.overflow_limit = Some(limit);
        self
    }
}

/// Why a [`Subscription`] stopped yielding frames.
///
/// A subscription's stream ends (`next()` yields `None`) for one of these
/// reasons, and [`Subscription::ended`] says which. The enum is
/// `#[non_exhaustive]`: match with a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionEnd {
    /// The library gave the subscription up after repeated broker ERROR
    /// frames for its destination, so that it is not resubscribed on
    /// reconnect. `message` is the broker's last `message` header. The same
    /// event puts an ERROR with `x-abandoned: true` on
    /// [`Connection::next_frame`].
    Abandoned {
        /// The broker's `message` header from the ERROR that tipped it over.
        message: String,
    },
    /// The library failed the subscription because more than `limit`
    /// messages were parked behind its full channel; see
    /// [`SubscriptionOptions::overflow_limit`]. The same event puts an ERROR
    /// with `x-overflow: true` on [`Connection::next_frame`].
    Overflowed {
        /// The `overflow_limit` that was exceeded.
        limit: usize,
    },
    /// The application unsubscribed, through [`Connection::unsubscribe`] or
    /// by dropping the handle.
    Unsubscribed,
    /// The connection was closed with [`Connection::close`], or its
    /// background task ended.
    ConnectionClosed,
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
    /// Written once by whoever ends the subscription, before the sending
    /// side of `receiver` is dropped, so that `None` from `next()` always
    /// finds this set. Shared with the connection's registry entry.
    ended: Arc<OnceLock<SubscriptionEnd>>,
}

impl Subscription {
    pub(crate) fn new(
        id: String,
        destination: String,
        receiver: mpsc::Receiver<Frame>,
        conn: Connection,
        ended: Arc<OnceLock<SubscriptionEnd>>,
    ) -> Self {
        Self {
            id,
            destination,
            receiver,
            conn,
            unsubscribed: false,
            ended,
        }
    }

    /// Why this subscription ended, or `None` while it is live.
    ///
    /// Set before the stream ends, so once `next()` has yielded `None` this
    /// is always `Some`. Frames already in the channel are still yielded
    /// first. A reconnect does not end a subscription: it is re-established
    /// and this stays `None`.
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// use iridium_stomp::SubscriptionEnd;
    ///
    /// # async fn example(mut sub: iridium_stomp::Subscription) {
    /// while let Some(frame) = sub.next().await {
    ///     // handle the frame
    /// }
    /// match sub.ended() {
    ///     Some(SubscriptionEnd::Abandoned { message }) => {
    ///         eprintln!("broker kept rejecting the subscription: {message}");
    ///     }
    ///     Some(SubscriptionEnd::Overflowed { limit }) => {
    ///         eprintln!("consumer fell more than {limit} messages behind");
    ///     }
    ///     Some(SubscriptionEnd::Unsubscribed) => {}
    ///     Some(SubscriptionEnd::ConnectionClosed) => {}
    ///     _ => {} // `SubscriptionEnd` is non-exhaustive
    /// }
    /// # }
    /// ```
    pub fn ended(&self) -> Option<SubscriptionEnd> {
        self.ended.get().cloned()
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
    /// id if you later want to stop it. The raw receiver carries no
    /// [`ended`](Self::ended): once it yields `None` there is no saying why.
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

    /// Acknowledge a message by its `ack` header value or its `message-id`
    /// header value; either identifies it. Delegates to `Connection::ack`
    /// using the local subscription id. Prefer [`ack_frame`](Self::ack_frame).
    pub async fn ack(&self, message_id: &str) -> Result<(), ConnError> {
        self.conn.ack(&self.id, message_id).await
    }

    /// Negative-acknowledge a message by its `ack` header value or its
    /// `message-id` header value. Prefer [`nack_frame`](Self::nack_frame).
    pub async fn nack(&self, message_id: &str) -> Result<(), ConnError> {
        self.conn.nack(&self.id, message_id).await
    }

    /// Acknowledge a MESSAGE frame received from this subscription.
    ///
    /// This is the form to use: it reads the id from the frame, so the caller
    /// cannot pick the wrong header. STOMP 1.2 acknowledges by the MESSAGE's
    /// `ack` header, 1.0/1.1 by its `message-id`, and some brokers (ActiveMQ
    /// Classic) silently ignore an ACK that carries the wrong one. Returns
    /// [`ConnError::MissingAckId`] if the frame has neither header.
    pub async fn ack_frame(&self, frame: &Frame) -> Result<(), ConnError> {
        self.conn.ack_frame(&self.id, frame).await
    }

    /// Negative-acknowledge a MESSAGE frame received from this subscription.
    /// See [`ack_frame`](Self::ack_frame).
    pub async fn nack_frame(&self, frame: &Frame) -> Result<(), ConnError> {
        self.conn.nack_frame(&self.id, frame).await
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
