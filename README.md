<p align="center">
  <img
    src="https://raw.githubusercontent.com/iridiumdesign/iridium-stomp/main/branding/iridium-stomp-github-banner.png"
    alt="iridium-stomp — frame relay :: async rust"
    width="880">
</p>

[![CI](https://github.com/iridiumdesign/iridium-stomp/actions/workflows/ci.yml/badge.svg)](https://github.com/iridiumdesign/iridium-stomp/actions/workflows/ci.yml)

An asynchronous STOMP 1.2 client library for Rust.

> **Early Production Testing**: This library is heavily tested (300+ unit and
> fuzz tests) and is currently in early production use as a monitoring and
> message processing tool. In that role it has shown improved reliability over
> previous libraries, surviving broker restarts, message queue cycling, and
> heartbeat delays with automatic reconnection. Not all features have been
> exercised in production yet. APIs may change.

## Design Goals

- **Async-first architecture** — Built on Tokio from the ground up.

- **Correct frame parsing** — Handles arbitrary TCP chunk boundaries, binary
  bodies with embedded NULs, and the full STOMP 1.2 frame format.

- **Automatic heartbeat management** — Negotiates heartbeat intervals per the
  spec, sends heartbeats when idle, and detects missed heartbeats from the
  server.

- **Transparent reconnection** — Stability-aware exponential backoff, automatic
  resubscription, and pending message cleanup on disconnect.

- **Small, explicit API** — One way to do things, clearly documented, easy to
  understand.

- **Production-ready testing** — 150+ tests including fuzz testing, stress
  testing, and regression capture for previously-failing edge cases.

## Quick Start

**Send a message:**

```rust,no_run
use iridium_stomp::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::connect(
        "127.0.0.1:61613",
        "guest",
        "guest",
        Connection::DEFAULT_HEARTBEAT,
    ).await?;

    conn.send("/queue/test", "hello from iridium-stomp").await?;

    conn.close().await?;
    Ok(())
}
```

**Listen for messages** (add `futures = "0.3"` to your `Cargo.toml`)**:**

```rust,no_run
use futures::StreamExt;
use iridium_stomp::{AckMode, Connection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::connect(
        "127.0.0.1:61613",
        "guest",
        "guest",
        Connection::DEFAULT_HEARTBEAT,
    ).await?;

    let mut sub = conn.subscribe("/queue/test", AckMode::Auto).await?;

    while let Some(frame) = sub.next().await {
        println!("Received: {}", frame);
    }

    conn.close().await?;
    Ok(())
}
```

## Documentation

For a deeper understanding, read the docs in this order:

1. **[STOMP 1.2 overview](docs/stomp_spec.md)** — protocol concepts (frames, commands, ack modes)
2. **[Subscriber guide](docs/subscriber-guide.md)** — full tutorial covering connect, subscribe, ack, reconnect, and error handling
3. **Reference docs** — [subscriptions](docs/subscriptions.md), [durable subscriptions](docs/durable_subscriptions.md), [heartbeats](docs/heartbeats.md)

### Examples

Run any example with `cargo run --example <name>` (requires a local STOMP broker):

| Example | What it demonstrates |
|---------|---------------------|
| `send` | Send a text message with the convenience method |
| `send_advanced` | Build a SEND frame with custom headers |
| `listen` | Subscribe and print incoming messages |
| `subscribe` | Single subscription with `SubscriptionOptions` and per-message ack |
| `multi_subscribe` | Multiple subscriptions merged into one stream, error monitoring, graceful shutdown |
| `subscribe_with_headers` | Passing broker-specific headers via `subscribe_with_headers` |
| `transactions` | Begin, commit, and abort transactions |

## Features

### Heartbeat Negotiation

Heartbeats are negotiated automatically during connection. Use the provided
constants or the `Heartbeat` struct for type-safe configuration:

```rust,ignore
use iridium_stomp::{Connection, Heartbeat};

// Use predefined constants
let conn = Connection::connect(addr, login, pass, Connection::DEFAULT_HEARTBEAT).await?;
let conn = Connection::connect(addr, login, pass, Connection::NO_HEARTBEAT).await?;

// Or use the Heartbeat struct for custom intervals
let hb = Heartbeat::new(5000, 10000);  // send every 5s, expect every 10s
let conn = Connection::connect(addr, login, pass, &hb.to_string()).await?;

// Create from Duration for symmetric intervals
use std::time::Duration;
let hb = Heartbeat::from_duration(Duration::from_secs(15));
```

The library handles the negotiation (taking the maximum of client and server
preferences), sends heartbeats when the connection is idle, and closes the
connection if the server stops responding.

### Subscription Management

Subscribe to destinations with automatic resubscription on reconnect:

```rust,ignore
use iridium_stomp::AckMode;

// Auto-acknowledge (server considers delivered immediately)
let sub = conn.subscribe("/queue/events", AckMode::Auto).await?;

// Client-acknowledge (cumulative)
let sub = conn.subscribe("/queue/jobs", AckMode::Client).await?;

// Client-individual (per-message acknowledgement)
let sub = conn.subscribe("/queue/tasks", AckMode::ClientIndividual).await?;
```

For broker-specific headers (durable subscriptions, selectors, etc.):

```rust,ignore
use iridium_stomp::SubscriptionOptions;
use iridium_stomp::AckMode;

let options = SubscriptionOptions {
    headers: vec![
        ("activemq.subscriptionName".into(), "my-durable-sub".into()),
        ("selector".into(), "priority > 5".into()),
    ],
};

let sub = conn.subscribe_with_options("/topic/events", AckMode::Client, options).await?;
```

### Cloneable Connection

The `Connection` is cloneable and thread-safe. Multiple tasks can share the
same connection:

```rust,ignore
let conn = Connection::connect(...).await?;
let conn2 = conn.clone();

tokio::spawn(async move {
    conn2.send_frame(some_frame).await.unwrap();
});
```

### Custom CONNECT Headers

Use `ConnectOptions` to customize the STOMP CONNECT frame for broker-specific
requirements like durable subscriptions or virtual hosts:

```rust,ignore
use iridium_stomp::{Connection, ConnectOptions};

let options = ConnectOptions::new()
    .client_id("my-durable-client")     // Required for ActiveMQ durable subscriptions
    .host("/production")                 // Virtual host (RabbitMQ)
    .accept_version("1.1,1.2")          // Version negotiation
    .header("custom-key", "value");     // Broker-specific headers

let conn = Connection::connect_with_options(
    "localhost:61613",
    "guest",
    "guest",
    Connection::DEFAULT_HEARTBEAT,
    options,
).await?;
```

### Receipt Confirmation

Request delivery confirmation from the broker using RECEIPT frames:

```rust,ignore
use iridium_stomp::{Connection, Frame};
use std::time::Duration;

let msg = Frame::new("SEND")
    .header("destination", "/queue/important")
    .set_body(b"critical data".to_vec());

// Send and wait for confirmation (with timeout). The receipt header is added
// for you - do not set one yourself.
conn.send_frame_confirmed(msg, Duration::from_secs(5)).await?;

// Or hold the confirmation and await it later. The receipt id is generated
// for you and carried by the returned handle.
let msg = Frame::new("SEND")
    .header("destination", "/queue/test")
    .set_body(b"data".to_vec());
let handle = conn.send_frame_with_receipt(msg).await?;
handle.wait(Duration::from_secs(5)).await?;
```

### Connection Error Handling

Connection failures (invalid credentials, server unreachable) are reported immediately:

```rust,ignore
use iridium_stomp::Connection;
use iridium_stomp::connection::ConnError;

match Connection::connect("localhost:61613", "user", "pass", Connection::DEFAULT_HEARTBEAT).await {
    Ok(conn) => {
        // Connected successfully
    }
    Err(ConnError::ServerRejected(err)) => {
        // Authentication failed or server rejected connection
        eprintln!("Server rejected: {}", err.message);
    }
    Err(ConnError::Io(err)) => {
        // Network error (connection refused, timeout, etc.)
        eprintln!("Network error: {}", err);
    }
    Err(err) => {
        eprintln!("Connection failed: {}", err);
    }
}
```

### Server Error Handling

Errors received after connection are surfaced as `ReceivedFrame::Error`:

```rust,ignore
use iridium_stomp::{Connection, ReceivedFrame};

while let Some(received) = conn.next_frame().await {
    match received {
        ReceivedFrame::Frame(frame) => {
            println!("Got {}: {:?}", frame.command, frame.get_header("destination"));
        }
        ReceivedFrame::Error(err) => {
            eprintln!("Server error: {}", err.message);
            if let Some(body) = &err.body {
                eprintln!("Details: {}", body);
            }
            break;
        }
    }
}
```

### Connection Retry and Reconnection Backoff

The library uses exponential backoff (1s → 2s → 4s → 8s → 16s → 30s cap)
for both the **initial connection** and **reconnection after a drop**. This
means your application can start before the broker is available —
`Connection::connect` will retry until the broker comes up.

Authentication failures (`ConnError::ServerRejected`) fail immediately on
the initial connection so that bad configuration is surfaced fast. Other
handshake and protocol failures are retried with exponential backoff.

**Initial connection:**

| Scenario | Behavior |
|----------|----------|
| Broker unreachable at startup | Retries with exponential backoff up to 30s cap |
| Broker crashes mid-handshake | Retries with exponential backoff |
| Bad credentials | Fails immediately (`ConnError::ServerRejected`) |

**Reconnection after a drop (stability-aware):**

- If the connection was alive for at least `max(current_backoff, 5)` seconds,
  it is considered stable. On disconnect, backoff resets to 1 second for a fast
  reconnect.
- If the connection dies quickly after establishing (e.g., the broker closes the
  connection during resubscription), backoff doubles on each attempt up to a 30
  second cap: 1s → 2s → 4s → 8s → 16s → 30s.
- Authentication failures during reconnection continue exponential backoff
  without checking connection stability (they do not trigger a backoff reset).

| Scenario | Behavior |
|----------|----------|
| Stable connection drops after minutes | Reconnect in 1s (backoff resets) |
| Broker rejects subscriptions and closes connection | 1s, 2s, 4s, 8s, 16s, 30s cap |
| Authentication failure on reconnect | Exponential backoff (no stability-based reset) |
| Broker unreachable | Exponential backoff up to 30s |

#### Broker-Specific Notes

**Artemis**: When Artemis rejects a SUBSCRIBE due to permissions, it sends a
STOMP ERROR frame but does **not** close the TCP connection. This violates the
[STOMP 1.2 specification](https://stomp.github.io/stomp-specification-1.2.html),
which states: "The server MAY send ERROR frames if something goes wrong. In this
case, it **MUST** then close the connection just after sending the ERROR frame."
Because Artemis keeps the connection open, the reconnect backoff path is never
triggered — errors are delivered inline on the existing connection, potentially
causing a rapid error loop if your application automatically retries
subscriptions. The library surfaces these errors via `ReceivedFrame::Error` for
application-level handling; you may need to implement your own rate limiting or
circuit breaker for Artemis deployments.

**RabbitMQ**: Follows the STOMP spec correctly — ERROR frames are followed by
connection close, which triggers the reconnect backoff as expected.

## CLI

See [docs/cli.md](docs/cli.md) for the full reference (all commands, TUI
keyboard shortcuts, session reports, and exit codes).

An interactive CLI is included for testing and ad-hoc messaging. Install with
the `cli` feature:

```bash
cargo install iridium-stomp --features cli
```

Or run from source:

```bash
cargo run --features cli --bin stomp -- --help
```

### CLI Usage

```bash
# Connect and subscribe to a queue
stomp -a 127.0.0.1:61613 -s /queue/test

# Connect with custom credentials
stomp -a broker.example.com:61613 -l myuser -p mypass -s /queue/events

# Subscribe to multiple queues
stomp -s /queue/orders -s /queue/notifications

# Enable TUI mode for live monitoring
stomp --tui -a 127.0.0.1:61613 -s /topic/events

# Send one message and exit; the exit code says whether the broker took it
stomp -a 127.0.0.1:61613 --send /queue/test 'hello'
```

### TUI Mode

The `--tui` flag enables a full terminal interface with:

- **Activity panel** - Live subscription counts with color coding
- **Message panel** - Scrollable message history with timestamps
- **Heartbeat indicator** - Animated pulse showing connection health
- **Command history** - Up/down arrows to navigate previous commands
- **Header toggle** - Press `Ctrl+H` to show/hide message headers

<!--
  SCREENSHOT TODO: capture the TUI (header + heartbeat, the messages pane, and
  a broker-errors pane if you can provoke one) and save it as
  branding/iridium-stomp-tui.png, then push to main so this absolute URL
  resolves on GitHub, crates.io, and docs.rs (relative paths do not render on
  crates.io — this matches the banner at the top).
-->
<p align="center">
  <img
    src="https://raw.githubusercontent.com/iridiumdesign/iridium-stomp/main/branding/iridium-stomp-tui.png"
    alt="iridium-stomp TUI: activity, messages, and heartbeat panels"
    width="880">
</p>

### Plain Mode

Without `--tui`, the CLI runs in plain mode with simple scrolling output:

```text
> send /queue/test Hello, World!
Sent to /queue/test

> sub /queue/other
Subscribed to: /queue/other

> help
Commands:
  send <destination> <message>  - Send a message
  sub <destination>             - Subscribe to a destination
  quit                          - Exit

> quit
Disconnecting...
```

<!--
  SCREENSHOT TODO: capture a plain-mode session (a subscribe, a received
  message with headers, a send) and save it as
  branding/iridium-stomp-cli.png, then push to main so this absolute URL
  resolves everywhere the README is rendered.
-->
<p align="center">
  <img
    src="https://raw.githubusercontent.com/iridiumdesign/iridium-stomp/main/branding/iridium-stomp-cli.png"
    alt="iridium-stomp plain CLI: interactive send and subscribe session"
    width="880">
</p>

## Running a Local Broker

The examples, the CLI, and the integration tests need a STOMP broker. The
included Docker Compose file starts RabbitMQ with the STOMP plugin enabled:

```bash
docker compose up -d
```

STOMP then listens on `127.0.0.1:61613` with `guest`/`guest`, and the
management UI is at <http://localhost:15672>. Point the CLI at it:

```bash
stomp -a 127.0.0.1:61613 -s /queue/test
```

Stop and remove the broker:

```bash
docker compose down
```

## Testing

The library includes comprehensive tests:

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test heartbeat_unit    # Heartbeat parsing/negotiation
cargo test --test codec_heartbeat   # Wire format encoding/decoding
cargo test --test parser_unit       # Frame parsing edge cases
cargo test --test codec_fuzz        # Randomized chunk splitting
cargo test --test codec_stress      # Concurrent stress testing
```

### Integration Tests in CI

The CI workflow includes a smoke integration test that verifies the library
works against a real RabbitMQ broker with STOMP enabled. This test ensures
end-to-end functionality beyond unit tests.

**How it works:**

1. **Broker Setup**: CI builds a Docker image with RabbitMQ 3.11 and the STOMP plugin pre-enabled (see `.github/docker/rabbitmq-stomp/Dockerfile`)

2. **Readiness Checks**: Before running tests, CI performs multi-stage readiness verification:
   - Waits for RabbitMQ management API to respond (indicates broker is starting)
   - Verifies STOMP plugin is fully enabled via the management API
   - Confirms STOMP port 61613 accepts TCP connections
   
   This ensures the broker is truly ready, preventing flaky test failures from timing issues.

3. **Smoke Test**: Runs `tests/stomp_smoke.rs` which:
   - Attempts a STOMP CONNECT with retry logic (5 attempts with backoff)
   - Verifies the broker responds with CONNECTED frame
   - Reports detailed connection diagnostics on failure

4. **Debugging**: If tests fail, CI automatically dumps RabbitMQ logs for troubleshooting

**Running integration tests locally:**

Use the provided helper script which mimics the CI workflow:

```bash
./scripts/test-with-rabbit.sh
```

Or manually with docker compose:

```bash
# Start RabbitMQ with STOMP
docker compose up -d

# Wait for it to be ready (management UI at http://localhost:15672)
# Then run the smoke test
RUN_STOMP_SMOKE=1 cargo test --test stomp_smoke

# Cleanup
docker compose down
```

The smoke test is skipped by default unless `RUN_STOMP_SMOKE=1` is set, since it requires an external broker.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.

