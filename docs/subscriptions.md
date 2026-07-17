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

Headers are preserved internally and replayed on reconnect.

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

To stop receiving messages, drop the `Subscription` handle or call
`unsubscribe`. The library sends an UNSUBSCRIBE frame and removes the
subscription from its internal tracking so it will not be resubscribed
on reconnect.
