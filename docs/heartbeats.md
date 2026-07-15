# Heartbeats

STOMP heartbeats are periodic signals that keep a connection alive and allow
both sides to detect a dead peer. iridium-stomp negotiates heartbeats
automatically during the CONNECT/CONNECTED handshake.

---

## How negotiation works

### 1. Client proposes intervals in CONNECT

The client sends a `heart-beat` header in the CONNECT frame with two
comma-separated values in milliseconds:

```
heart-beat: cx,cy
```

| Value | Meaning |
|-------|---------|
| **cx** | Smallest interval between heartbeats the client *can send*. `0` means the client cannot send heartbeats. |
| **cy** | Smallest interval between heartbeats the client *wants to receive*. `0` means the client does not want to receive heartbeats. |

### 2. Server responds in CONNECTED

The server's CONNECTED frame contains its own `heart-beat` header:

```
heart-beat: sx,sy
```

| Value | Meaning |
|-------|---------|
| **sx** | Smallest interval the server *can send*. |
| **sy** | Smallest interval the server *wants to receive*. |

If the server omits the header, both values default to `0` (heartbeats
disabled).

### 3. Final intervals are negotiated

Per the STOMP spec, each direction takes the **maximum** of the two
corresponding values:

| Direction | Formula | Result |
|-----------|---------|--------|
| Client to server | `max(cx, sy)` | How often the client will send heartbeats |
| Server to client | `max(cy, sx)` | How often the client expects to receive heartbeats |

A direction is disabled only when the negotiated value is `0`. Since the
formula uses `max()`, both sides must advertise `0` for that direction to
be disabled. A `0` from one side alone does not disable the direction if
the other side advertises a non-zero value (because `max(0, N) = N`).

---

## Configuring heartbeats

### Using the `Heartbeat` type (recommended)

```rust
use iridium_stomp::{Connection, Heartbeat};
use std::time::Duration;

// Default: 10 seconds in both directions
let hb = Heartbeat::default(); // "10000,10000"

// Custom: send every 5s, want to receive every 15s
let hb = Heartbeat::new(5000, 15000);

// Symmetric from a Duration
let hb = Heartbeat::from_duration(Duration::from_secs(30)); // "30000,30000"

// Disable heartbeats entirely
let hb = Heartbeat::disabled(); // "0,0"

let conn = Connection::connect(
    "127.0.0.1:61613", "guest", "guest", &hb.to_string()
).await?;
```

### Using a raw string

```rust
let conn = Connection::connect(
    "127.0.0.1:61613", "guest", "guest", "10000,10000"
).await?;
```

### Convenience constants

`Connection::NO_HEARTBEAT` (`"0,0"`) and `Connection::DEFAULT_HEARTBEAT`
(`"10000,10000"`) are available for common cases:

```rust
let conn = Connection::connect(
    "127.0.0.1:61613", "guest", "guest", Connection::DEFAULT_HEARTBEAT
).await?;
```

---

## Monitoring heartbeats

You can receive a notification each time the server sends a heartbeat by
passing a channel via `ConnectOptions`:

```rust
use tokio::sync::mpsc;
use iridium_stomp::{Connection, ConnectOptions};

let (tx, mut rx) = mpsc::channel(16);
let options = ConnectOptions::default()
    .heartbeat_notify(tx);

let conn = Connection::connect_with_options(
    "127.0.0.1:61613", "guest", "guest", Connection::DEFAULT_HEARTBEAT, options
).await?;

// In another task:
tokio::spawn(async move {
    while rx.recv().await.is_some() {
        println!("heartbeat received");
    }
});
```

Notifications use `try_send()` internally, so a full channel buffer will not
block the connection's background task.

---

## Reconnection

When the connection is re-established after a disconnect, heartbeat
negotiation runs again with the new CONNECTED response. The negotiated
intervals may differ if the server's advertised values change between
connections.

---

## Broker notes

- Some brokers require heartbeats for long-lived connections and will
  disconnect idle clients. If you disable heartbeats, verify that your broker
  does not enforce them.
- The negotiated interval is a *minimum*. Either side may send heartbeats
  more frequently.
