# Subscriptions

This document covers the subscription API in iridium-stomp: how to subscribe,
the available methods, `SubscriptionOptions`, and how resubscription works
across reconnects.

For broker-specific durable subscription recipes, see
[durable_subscriptions.md](durable_subscriptions.md).

---

## Subscribe methods

iridium-stomp provides three ways to subscribe to a destination:

### `subscribe(destination, ack)`

The simplest form. No extra headers or options.

```rust,no_run
# use iridium_stomp::Connection;
# async fn wrapper(conn: Connection) -> Result<(), Box<dyn std::error::Error>> {
use iridium_stomp::AckMode;

let sub = conn.subscribe("/queue/orders", AckMode::Auto).await?;
# let _ = sub;
# Ok(())
# }
```

### `subscribe_with_options(destination, ack, options)`

Accepts a `SubscriptionOptions` struct for typed configuration. Use this
when you need broker-specific headers, such as a durable subscription name.

```rust,no_run
# use iridium_stomp::Connection;
# async fn wrapper(conn: Connection) -> Result<(), Box<dyn std::error::Error>> {
use iridium_stomp::{AckMode, SubscriptionOptions};

let opts = SubscriptionOptions {
    headers: vec![(
        "activemq.subscriptionName".to_string(),
        "my-durable-sub".to_string(),
    )],
    ..Default::default()
};

let sub = conn
    .subscribe_with_options("/topic/my-topic", AckMode::Client, opts)
    .await?;
# let _ = sub;
# Ok(())
# }
```

### `subscribe_with_headers(destination, ack, extra_headers)`

Low-level convenience that forwards arbitrary header pairs on the
SUBSCRIBE frame. Equivalent to `subscribe_with_options` with only the
`headers` field set.

```rust,no_run
# use iridium_stomp::Connection;
# async fn wrapper(conn: Connection) -> Result<(), Box<dyn std::error::Error>> {
use iridium_stomp::AckMode;

let headers = vec![
    ("activemq.subscriptionName".to_string(), "my-sub".to_string()),
];
let sub = conn
    .subscribe_with_headers("/topic/events", AckMode::Client, headers)
    .await?;
# let _ = sub;
# Ok(())
# }
```

---

## `SubscriptionOptions`

| Field | Type | Purpose |
|-------|------|---------|
| `headers` | `Vec<(String, String)>` | Extra headers included on the SUBSCRIBE frame (e.g., broker-specific durable subscription names). |
| `channel_capacity` | `Option<usize>` | Frames the subscription's channel holds. `None` means `SubscriptionOptions::DEFAULT_CHANNEL_CAPACITY` (16), which is also what `subscribe` and `subscribe_with_headers` use. |
| `overflow_limit` | `Option<usize>` | Most messages that may be parked behind a full channel before the subscription is failed. `None` means `SubscriptionOptions::DEFAULT_OVERFLOW_LIMIT` (1024), which is also what `subscribe` and `subscribe_with_headers` use. `Some(0)` allows no parking. |

Headers are preserved internally and replayed on reconnect.

A full channel does not lose messages. While a consumer is behind, the
connection parks further messages for that subscription and moves them into
the channel, in order, as room appears; other subscriptions, heartbeats and
receipts carry on meanwhile.

Parking is bounded by `overflow_limit`. A message that would take the parked
queue past it means the consumer is treated as stalled, and the subscription
is **failed** rather than left to grow until the process runs out of memory:

- The library sends UNSUBSCRIBE and forgets the subscription; it is not
  resubscribed after a reconnect.
- The parked messages, and the one that tripped the limit, are discarded.
  With `AckMode::Client` and `AckMode::ClientIndividual` none of them was
  acknowledged, so the broker redelivers them to the next subscriber. With
  `AckMode::Auto` the broker already counts them delivered, and they are
  lost.
- The `Subscription` stream yields what was already in its channel and then
  ends (`None`), so a `while let Some(..)` loop stops instead of hanging.
- `conn.next_frame()` yields a synthetic ERROR with `x-overflow: true` and
  the `destination` and `subscription` headers, and the failure is logged
  with `tracing::error!`.

Before that point the library logs a `tracing::warn!` when a subscription
starts parking and each time the parked depth doubles (the channel capacity,
twice that, and so on). In the client ack modes the broker's flow control
normally keeps the queue far below the default limit.

STOMP has no durable-subscription concept of its own, so durability is
whatever the broker defines it to be. On ActiveMQ that is a header such as
`activemq.subscriptionName`; on RabbitMQ the queue is declared
administratively and you simply subscribe to it by name as the
`destination`.

---

## Ack modes

The ack mode is set per subscription and determines how the broker tracks
message delivery. See also the
[ack modes section in the subscriber guide](subscriber-guide.md#ack-modes).

| Mode | Behavior |
|------|----------|
| `AckMode::Auto` | Broker considers the message delivered as soon as it sends it. No ACK frame needed. |
| `AckMode::Client` | Client must ACK. Acknowledging a message implicitly acknowledges all prior messages on that subscription (cumulative). |
| `AckMode::ClientIndividual` | Client must ACK each message independently. |

---

## Resubscribe on reconnect

When the connection drops and is re-established, iridium-stomp
automatically re-issues SUBSCRIBE frames for every active subscription.
The original destination, ack mode, subscription ID, and any extra headers
or options you provided are all preserved and replayed.

This means:

- Broker-specific headers (e.g., `activemq.subscriptionName`) are resent,
  so durable topic subscriptions are restored.
- Your application code does not need to handle resubscription — the
  `Subscription` stream simply pauses during the outage and resumes when
  the connection comes back.

---

## Unsubscribe

To stop receiving messages, call `Subscription::unsubscribe` or simply drop
the handle. Either way the library sends an UNSUBSCRIBE frame and removes the
subscription from its internal tracking so it will not be resubscribed on
reconnect.

`unsubscribe` is the explicit form and reports whether the frame was queued.
Dropping the handle does the same on a best-effort basis — it cannot report an
error, and because it runs from `Drop` it uses non-blocking `try_lock`/`try_send`
rather than awaiting. If the outbound channel is momentarily unavailable the
frame may not be sent; if the subscription registry is momentarily locked, the
local entry is not removed right then. In that case the entry is reaped when the
next message for it is delivered (its receiver is closed), so pruning is
eventual rather than guaranteed at the instant of drop — until it happens, a
reconnect in that window could briefly resubscribe it. Use the explicit
`unsubscribe` when you need the removal to be immediate and acknowledged.

One exception: if you took the raw receiver with `into_receiver`, you now own
the stream and dropping it sends nothing. Unsubscribe explicitly with
`Connection::unsubscribe` and the subscription id if you need to stop it.
