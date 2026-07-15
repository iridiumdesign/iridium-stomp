# Durable Subscriptions and Broker-Specific Headers

This document provides broker-specific recipes for achieving durable
subscription semantics — where messages are retained while your client is
offline and delivered when it reconnects.

For the general subscription API and `SubscriptionOptions` reference, see
[subscriptions.md](subscriptions.md).

---

## Background

STOMP is a portable protocol and intentionally leaves durability
underspecified. Different brokers provide it in different ways:

- **RabbitMQ** — uses durable, named queues bound to exchanges.
- **ActiveMQ** — supports broker-side durable topic subscriptions via
  `client-id` and a subscription name header.

---

## RabbitMQ: Durable Queues

RabbitMQ does not support ActiveMQ-style durable topic subscriptions. The
equivalent pattern is a durable named queue bound to an exchange.

### Setup

1. Create a durable queue and bind it to the exchange that receives
   messages. This is an administrative step (management UI, `rabbitmqadmin`,
   or the AMQP API):

   ```sh
   rabbitmqadmin declare queue name=my-app-queue durable=true
   rabbitmqadmin declare binding source=amq.topic \
       destination=my-app-queue routing_key="topic.#"
   ```

2. Ensure messages published to the exchange are persistent. How to mark
   messages persistent depends on the publisher — see the RabbitMQ
   documentation.

### Client code

Durability here is a property of the queue you declared above, not of the
SUBSCRIBE frame. Subscribe to that queue by naming it as the destination —
no options are needed at all:

```rust
use iridium_stomp::AckMode;
use iridium_stomp::Connection;

let conn = Connection::connect(
    "127.0.0.1:61613",
    "guest",
    "guest",
    Connection::DEFAULT_HEARTBEAT,
).await?;

let sub = conn
    .subscribe("/queue/my-app-queue", AckMode::Client)
    .await?;
```

Messages that arrived while the consumer was offline are delivered when
the client reconnects and resubscribes to the same queue name. The library
handles resubscription automatically.

---

## ActiveMQ: Durable Topic Subscriptions

ActiveMQ provides broker-side durable subscriptions for topics. This
requires two things:

1. A `client-id` on the CONNECT frame (ties durable subscriptions to a
   client identity).
2. A durable subscription name header on the SUBSCRIBE frame. The exact
   header name is broker-specific — consult the ActiveMQ STOMP
   documentation for your version.

### Client code

```rust
use iridium_stomp::{Connection, ConnectOptions, SubscriptionOptions};
use iridium_stomp::AckMode;

let options = ConnectOptions::new()
    .client_id("my-durable-client");

let conn = Connection::connect_with_options(
    "activemq:61613",
    "user",
    "pass",
    Connection::DEFAULT_HEARTBEAT,
    options,
).await?;

let opts = SubscriptionOptions {
    headers: vec![
        ("activemq.subscriptionName".to_string(), "my-durable-sub".to_string()),
    ],
};

let sub = conn
    .subscribe_with_options("/topic/my-topic", AckMode::Client, opts)
    .await?;
```

On reconnect, the library re-issues the same SUBSCRIBE with the
`activemq.subscriptionName` header, so the broker resumes the durable
subscription.

---

## Notes

- **Durability requires both sides.** A durable queue alone is not enough
  — the publisher must also mark messages as persistent for them to survive
  broker restarts.
- **ACK mode matters.** Use `Client` or `ClientIndividual` if you need
  message-level acknowledgement. With `Auto`, unacknowledged messages may
  be lost on disconnect.
- **Portability.** Broker-specific headers reduce portability. The
  `subscribe_with_options` and `subscribe_with_headers` APIs are
  intentionally flexible so you can pass whatever your broker expects.

---

## References

- RabbitMQ documentation: <https://www.rabbitmq.com/> (exchanges, queues,
  and bindings)
- ActiveMQ STOMP documentation: consult the docs for your ActiveMQ version
  for exact header names and durable subscription behavior.
