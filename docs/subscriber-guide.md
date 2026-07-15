# Building a Subscriber CLI with iridium-stomp

A practical guide for the most common use case: connect to a broker, subscribe
to one or more destinations, and process incoming messages — staying connected
indefinitely with automatic reconnection.

---

## What this guide covers

- Setting up a Rust CLI app with iridium-stomp
- Connecting to ActiveMQ (or any STOMP 1.2 broker)
- Subscribing to a list of destinations and printing messages
- How reconnection works and what to expect
- Ack modes and when to use each
- Handling broker ERROR frames

---

## Project setup

```bash
cargo new stomp-subscriber
cd stomp-subscriber
```

`Cargo.toml`:

```toml
[dependencies]
iridium-stomp = "0.x"
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

---

## The app

A working version of this app is available as a runnable example:

```bash
cargo run --example multi_subscribe
```

The sections below walk through each part of that example.

Multiple subscriptions are merged into a single stream using
`futures::stream::select_all`, so one loop handles all destinations.

```rust
use futures::{StreamExt, stream};
use iridium_stomp::Connection;
use iridium_stomp::AckMode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr  = "localhost:61613";
    let login = "admin";
    let pass  = "admin";
    let destinations = vec![
        "/queue/orders",
        "/queue/notifications",
        "/topic/events",
    ];

    println!("Connecting to {}...", addr);

    let conn = Connection::connect(addr, login, pass, Connection::DEFAULT_HEARTBEAT).await?;

    println!("Connected. Subscribing to {} destinations...", destinations.len());

    let mut subs = Vec::new();
    for dest in &destinations {
        let sub = conn.subscribe(dest, AckMode::Auto).await?;
        println!("  Subscribed (id={}) -> {}", sub.id(), dest);
        subs.push(sub);
    }

    let mut merged = stream::select_all(subs);

    while let Some(frame) = merged.next().await {
        let dest = frame.headers.iter()
            .find(|(k, _)| k.to_lowercase() == "destination")
            .map(|(_, v)| v.as_str())
            .unwrap_or("unknown");
        let body = std::str::from_utf8(&frame.body).unwrap_or("<binary>");
        println!("[{}] {}", dest, body);
    }

    // `merged.next()` returning None means the connection was shut down.
    println!("Connection closed.");
    Ok(())
}
```

The library handles reconnection automatically in the background — all
subscriptions are resubscribed on reconnect, and your merged stream just pauses
during the outage and resumes when the connection comes back.

---

## How connection retry and reconnection work

### Initial connection

`Connection::connect` retries automatically if the broker is unreachable,
using exponential backoff (1s → 2s → 4s → … → 30s cap). This means your
application can start before the broker is available — it will connect once
the broker comes up. Authentication errors (`ConnError::ServerRejected`)
fail immediately so that bad configuration is surfaced fast.

### Reconnection after a drop

When the connection drops:

1. The background task detects the drop (TCP error, stream EOF, or heartbeat
   timeout).
2. It waits with exponential backoff before attempting to reconnect: 1s → 2s →
   4s → 8s → 16s → 30s (cap).
3. If the connection had been alive for at least 5 seconds before dropping, it
   is considered stable and backoff resets to 1s — so a transient blip
   reconnects quickly.
4. Once reconnected, the library re-sends a `SUBSCRIBE` frame for every active
   subscription automatically. Your `sub.next()` loop does not need to do
   anything.

**What the application sees:** `sub.next()` simply stops returning frames during
the outage and starts again when the connection is restored. There is no
disconnect/reconnect event delivered to the subscriber — it looks like a quiet
period with no messages.

**What is lost on reconnect:**

- Messages in transit at the moment of disconnect may be dropped. The broker
  will redeliver them on the new connection if your queue is durable and ack
  mode is not `Auto`.
- Pending ACKs are cleared. If you had received a message but not yet ACK'd it,
  the broker will redeliver it.

---

## Ack modes

| Mode | Behavior | Use when |
|------|----------|----------|
| `AckMode::Auto` | Broker considers message delivered as soon as it sends it | Fire-and-forget; message loss on disconnect is acceptable |
| `AckMode::Client` | You ACK; all messages up to that ID are acknowledged (cumulative) | Ordered processing where you batch ACKs |
| `AckMode::ClientIndividual` | You ACK each message independently | Concurrent processing or when you need per-message control |

For most "stay connected and process" use cases, `AckMode::ClientIndividual`
with explicit ACKs is the safest choice. With multiple subscriptions, ACKing
works the same way — each frame carries the subscription ID the broker needs,
and you ACK through the connection directly:

```rust
use futures::stream;

let destinations = vec!["/queue/orders", "/queue/notifications"];
let mut subs = Vec::new();
for dest in &destinations {
    subs.push(conn.subscribe(dest, AckMode::ClientIndividual).await?);
}
let mut merged = stream::select_all(subs);

while let Some(frame) = merged.next().await {
    let body = std::str::from_utf8(&frame.body).unwrap_or("<binary>");
    println!("Message: {}", body);

    // conn.ack() requires both subscription-id and message-id.
    // Both are present as headers on every MESSAGE frame.
    let sub_id = frame.headers.iter()
        .find(|(k, _)| k.to_lowercase() == "subscription")
        .map(|(_, v)| v.clone());
    let msg_id = frame.headers.iter()
        .find(|(k, _)| k.to_lowercase() == "message-id")
        .map(|(_, v)| v.clone());

    if let (Some(sub_id), Some(msg_id)) = (sub_id, msg_id) {
        conn.ack(&sub_id, &msg_id).await?;
    }
}
```

---

## Heartbeat configuration

See [heartbeats.md](heartbeats.md) for a detailed guide on how negotiation
works, the `Heartbeat` type, and monitoring.

The default (`Connection::DEFAULT_HEARTBEAT`) negotiates 10-second heartbeats
with the broker. The library disconnects and reconnects if no data is received
for 2x the negotiated interval (20 seconds by default).

```rust
// Default: 10s send, 10s receive
Connection::connect(addr, login, pass, Connection::DEFAULT_HEARTBEAT).await?;

// Disable heartbeats entirely (not recommended for production)
Connection::connect(addr, login, pass, Connection::NO_HEARTBEAT).await?;

// Custom: send every 5s, expect every 30s (more tolerant of slow brokers)
use iridium_stomp::Heartbeat;
let hb = Heartbeat::new(5000, 30000);
Connection::connect(addr, login, pass, &hb.to_string()).await?;
```

**If you see frequent reconnects during CPU-heavy workloads:** the Tokio
runtime may be starved and not polling the socket in time, causing the heartbeat
watchdog to fire even though the broker is fine. Either raise the receive
interval or isolate the connection task from CPU-heavy work.

---

## Durable subscriptions (ActiveMQ)

See [durable_subscriptions.md](durable_subscriptions.md) for broker-specific
recipes covering both ActiveMQ and RabbitMQ.

For ActiveMQ durable topic subscribers — where the broker holds messages while
your app is offline — pass the subscription name via `ConnectOptions` and
`SubscriptionOptions`:

```rust
use iridium_stomp::{Connection, ConnectOptions, SubscriptionOptions};
use iridium_stomp::AckMode;

let options = ConnectOptions::new()
    .client_id("my-app-instance-1");  // required for durable subs on ActiveMQ

let conn = Connection::connect_with_options(
    addr, login, pass,
    Connection::DEFAULT_HEARTBEAT,
    options,
).await?;

let sub_opts = SubscriptionOptions {
    headers: vec![
        ("activemq.subscriptionName".into(), "my-durable-sub".into()),
    ],
};

// Subscribe to multiple durable topics
let topics = vec![
    ("/topic/orders",        "my-app-orders-sub"),
    ("/topic/notifications", "my-app-notif-sub"),
];

let mut subs = Vec::new();
for (dest, sub_name) in &topics {
    let sub_opts = SubscriptionOptions {
        headers: vec![
            ("activemq.subscriptionName".into(), (*sub_name).into()),
        ],
        };
    subs.push(conn.subscribe_with_options(dest, AckMode::ClientIndividual, sub_opts).await?);
}

let mut merged = stream::select_all(subs);
```

The subscription headers are preserved across reconnects — the library stores
all headers from each `subscribe_with_options` call and replays them on
resubscription.

---

## Handling shutdown gracefully

```rust
use futures::stream;
use tokio::signal;

let mut merged = stream::select_all(subs);

tokio::select! {
    _ = async {
        while let Some(frame) = merged.next().await {
            println!("Message: {}", std::str::from_utf8(&frame.body).unwrap_or("<binary>"));
        }
    } => {}
    _ = signal::ctrl_c() => {
        println!("Shutting down...");
    }
}

conn.close().await?;
```

---

## Handling broker ERROR frames

**ERROR frames are not delivered through the `Subscription` stream.** `sub.next()`
only yields `MESSAGE` frames routed to that subscription. Broker ERRORs go to a
separate channel and are only visible via `conn.next_frame()`, which returns a
`ReceivedFrame` enum.

To catch them, run a separate task alongside your subscriber loop:

```rust
use iridium_stomp::ReceivedFrame;
use futures::stream;
use tokio::signal;

let conn_errors = conn.clone();

tokio::spawn(async move {
    while let Some(received) = conn_errors.next_frame().await {
        if let ReceivedFrame::Error(err) = received {
            // Check if the library abandoned a subscription after repeated errors
            let abandoned = err.frame.get_header("x-abandoned").is_some();
            if abandoned {
                eprintln!("Subscription abandoned by library: {}", err.message);
                // The subscription will no longer be resubscribed on reconnect.
                // Decide whether to reconnect entirely or exit.
            } else {
                eprintln!("Broker error: {}", err.message);
            }
        }
    }
});

// Your normal subscriber loop
let mut merged = stream::select_all(subs);
while let Some(frame) = merged.next().await {
    // ...
}
```

**Abandonment** is a specific case to watch for. If the broker sends 3
consecutive ERROR frames for the same destination (e.g. a permissions error),
the library stops resubscribing that destination and sends a synthetic ERROR
frame with an `x-abandoned: true` header to `conn.next_frame()`. The
subscription's stream goes silent — `sub.next()` returns `None` — with no
other indication of why. The error task above is the only way to detect this.

---

## Checklist before production use

- [ ] Use `AckMode::ClientIndividual` and ACK every message explicitly, passing both `subscription-id` and `message-id` to `conn.ack()`
- [ ] Use durable queues/subscriptions so messages survive restarts
- [ ] Set heartbeats to an interval your broker and network can sustain
- [ ] Run a separate task polling `conn.next_frame()` to catch broker ERROR frames and abandonment notifications
- [ ] Handle `None` from `merged.next()` (means the `Connection` was closed, not a transient drop)
- [ ] Test with the broker restarted mid-run to verify resubscription works

---

## What to read next

- [Subscriptions](subscriptions.md) — full API reference for subscribe methods, `SubscriptionOptions`, and resubscribe behavior
- [Durable Subscriptions](durable_subscriptions.md) — broker-specific recipes for RabbitMQ durable queues and ActiveMQ durable topics
- [Heartbeats](heartbeats.md) — detailed heartbeat negotiation, the `Heartbeat` type, and monitoring via `heartbeat_notify`
- [`multi_subscribe` example](../examples/multi_subscribe.rs) — runnable version of the patterns in this guide
