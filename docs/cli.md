# CLI and TUI

iridium-stomp includes an interactive STOMP client with two modes: a
line-based **plain mode** (default) and a full-screen **TUI mode** with
panels, scrolling, and live activity counts.

---

## Installation

Install from crates.io:

```bash
cargo install iridium-stomp --features cli
```

Or run from source:

```bash
cargo run --features cli --bin stomp -- --help
```

---

## Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `-a, --address` | `127.0.0.1:61613` | Broker address (host:port) |
| `-l, --login` | `guest` | STOMP login username |
| `-p, --passcode` | `guest` | STOMP passcode |
| `--heartbeat` | `10000,10000` | Heartbeat intervals in milliseconds (send,receive) |
| `-s, --subscribe` | *(none)* | Destination to subscribe to on connect (repeatable) |
| `--tui` | off | Enable TUI mode |
| `--summary` | off | Print session summary on exit |
| `--send <DESTINATION> <BODY>` | *(none)* | Send one message and exit, without starting the interactive client |
| `--timeout <SECONDS>` | `5` | How long a `--send` waits on the broker, covering both the connection and the receipt |

```bash
# Connect with defaults
stomp

# Custom broker, credentials, and initial subscriptions
stomp -a broker.example.com:61613 -l myuser -p mypass \
      -s /queue/orders -s /topic/events

# TUI mode with faster heartbeats
stomp --tui --heartbeat 5000,5000 -s /queue/tasks
```

---

## One-shot send

`stomp` is an interactive client; `--send` is its non-interactive side path.
It connects, publishes one message, disconnects, and exits — no REPL, no pipe
tricks:

```bash
stomp -a broker.example.com:61613 --send /queue/orders 'hello'
```

The message is sent with a receipt request and the tool waits for the broker's
answer, so the exit code is worth testing:

```bash
if stomp --send /queue/orders "$payload"; then
    echo "the broker has it"
fi
```

| Exit | Meaning |
|------|---------|
| 0 | The broker confirmed the message |
| 1 | Could not reach the broker within `--timeout` |
| 3 | The broker did not answer within `--timeout`, or the destination was malformed |
| 4 | The broker rejected the message (permissions, unknown destination) |

`--timeout` bounds the whole operation. It matters more than it looks: a normal
`Connection` retries an unreachable broker indefinitely, which is right for a
long-lived service and wrong for a script, so `--send` gives up instead.

`--send` cannot be combined with `--tui`, `-s/--subscribe`, or `--summary`;
those belong to the interactive client.

---

## Interactive commands

Both plain and TUI modes accept the same commands at the `>` prompt:

| Command | Syntax | Description |
|---------|--------|-------------|
| **send** | `send <destination> <message>` | Publish a message to a destination |
| **sub** | `sub <destination>` | Subscribe to a destination |
| **summary** | `summary [file]` | Print session summary (or save to file) |
| **report** | `report [file]` | Full report with message history (or save to file) |
| **clear** | `clear` | Clear message history buffer |
| **about** | `about` | Show copyright and license information |
| **help** | `help` or `?` | List available commands |
| **quit** | `quit`, `exit`, or `q` | Disconnect and exit |

`subscribe` is accepted as an alias for `sub`.

Destinations must start with `/`. The CLI warns if a destination does not
match common patterns like `/topic/`, `/queue/`, `/amq/`, or `/exchange/`.

---

## Plain mode

Plain mode is the default when `--tui` is not set. It reads commands from
stdin and prints messages to stdout as they arrive.

Incoming messages are displayed with full headers:

```
[/topic/events] MESSAGE received:
  content-type: application/json
  message-id: ID:broker-12345
  Body: {"event":"order.created"}
```

Broker errors interrupt output with a `[BROKER ERROR]` prefix:

```
[BROKER ERROR] Invalid destination
  message: Destination not found
```

---

## TUI mode

Enable with `--tui`. The terminal is divided into panels:

```
┌─────────────────────────────────────────────────────────┐
│ Connection info                        Heartbeat status │
├─────────────────────────────────────────────────────────┤
│ Activity counts (subscriptions and message tallies)     │
├─────────────────────────────────┬───────────────────────┤
│ Messages (70%)                  │ Broker errors (30%)   │
│                                 │                       │
├─────────────────────────────────┴───────────────────────┤
│ > command input                                         │
└─────────────────────────────────────────────────────────┘
```

### Header bar

Shows the broker address, login user, and a heartbeat indicator:

| Symbol | Color | Meaning |
|--------|-------|---------|
| `✦` | Green | Heartbeat just received (within 1 second) |
| `◇` | Gray | Healthy (within expected interval) |
| `!` | Red | Late heartbeat warning |
| `○` | Gray | No heartbeat received yet |

### Activity counts

A table listing each subscribed destination with its message count, plus
rows for sent, info, warning, and error totals. Destinations are sorted
alphabetically. Counts are color-coded by type.

### Messages panel

Timestamped messages with destination labels. Color-coded by type:

- Subscriptions: cyan destination, gray body
- Sent: blue
- Errors: red (bold)
- Warnings: yellow
- Info: cyan

Messages auto-scroll to the bottom. Scrolling up pauses auto-scroll until
you scroll back down.

### Broker errors panel

A dedicated right-side panel that appears when broker errors have been
received. Shows error count in the title bar. Errors wrap across lines
with indented continuation.

### Input bar

Full line editing with cursor positioning. Command history is navigated
with the up/down arrow keys. Incomplete input is preserved when browsing
history.

### Keyboard shortcuts

| Key | Action |
|-----|--------|
| `Ctrl+Q` / `Ctrl+C` | Quit |
| `Ctrl+H` | Toggle message header display |
| `Ctrl+Up` | Scroll messages up |
| `Ctrl+Down` | Scroll messages down |
| `Page Up` | Scroll messages up 10 lines |
| `Page Down` | Scroll messages down 10 lines |
| `Ctrl+E` | Scroll errors up |
| `Ctrl+D` | Scroll errors down |
| `Up` / `Down` | Navigate command history |
| `Escape` | Clear input |
| `Home` / `End` | Jump to start/end of input |

---

## Session summary and reports

The `summary` command prints a snapshot of the current session: connection
details, uptime, heartbeat count, and per-destination message counts.

The `report` command includes everything in `summary` plus the full
message history buffer (up to 1000 messages).

Both commands accept an optional filename to write output to a file
instead of the screen:

```
> summary session.txt
> report full-report.txt
```

The `--summary` flag prints the session summary automatically on exit.

---

## Exit codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | SUCCESS | Normal exit |
| 1 | NETWORK_ERROR | Connection refused, timeout, or network failure |
| 2 | AUTH_ERROR | Authentication failed (bad credentials) |
| 3 | PROTOCOL_ERROR | Unexpected server response, protocol violation, or a `--send` the broker never answered |
