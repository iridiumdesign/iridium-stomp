# STOMP 1.2 Specification Summary

A high-level overview of the STOMP 1.2 protocol and how its features map to
iridium-stomp.

The full specification is available at
<https://stomp.github.io/stomp-specification-1.2.html>.

---

## What is STOMP?

STOMP (Simple Text Oriented Messaging Protocol) is a lightweight,
text-based messaging protocol designed for interoperability between clients
and message brokers. It operates over TCP and uses a frame-based format
similar to HTTP.

---

## Frame format

Every STOMP frame follows the same structure:

```
COMMAND
header1:value1
header2:value2

body\0
```

- **Command** — an uppercase verb on the first line.
- **Headers** — zero or more `name:value` pairs, one per line. Headers end at
  the first blank line. STOMP 1.2 requires escaping of `\n`, `\r`, `\`,
  and `:` within header values.
- **Body** — optional content after the blank line, terminated by a NUL
  (`\0`) byte. When a `content-length` header is present the body may
  contain embedded NUL bytes.

---

## Client commands

| Command | Purpose |
|---------|---------|
| **CONNECT** / **STOMP** | Open a session. Includes `accept-version`, `host`, `login`, `passcode`, and `heart-beat` headers. |
| **SEND** | Publish a message to a destination. |
| **SUBSCRIBE** | Register interest in a destination. The broker will push matching messages to the client. |
| **UNSUBSCRIBE** | Cancel an existing subscription by its `id`. |
| **ACK** | Acknowledge consumption of a message (when using `client` or `client-individual` ack mode). |
| **NACK** | Negatively acknowledge a message, telling the broker delivery failed. |
| **BEGIN** | Start a transaction. |
| **COMMIT** | Commit a transaction, applying all its pending operations. |
| **ABORT** | Roll back a transaction, discarding all its pending operations. |
| **DISCONNECT** | Gracefully close the session. A `receipt` header can be used to confirm the server processed all prior frames. |

---

## Server commands

| Command | Purpose |
|---------|---------|
| **CONNECTED** | Confirms a successful session. Returns the negotiated `version`, `heart-beat`, and optional `server` header. |
| **MESSAGE** | Delivers a message to a subscribed client. Includes `subscription`, `message-id`, and `destination` headers. |
| **RECEIPT** | Acknowledges a client frame that carried a `receipt` header. |
| **ERROR** | Reports a protocol or broker error. The server typically closes the connection after sending this frame. |

---

## Subscriptions and ack modes

See [subscriptions.md](subscriptions.md) for the iridium-stomp subscription
API and [durable_subscriptions.md](durable_subscriptions.md) for
broker-specific durability recipes.

When subscribing, the client specifies an acknowledgement mode:

| Mode | Behavior |
|------|----------|
| **auto** | Messages are considered acknowledged as soon as they are sent to the client. No ACK frame is needed. |
| **client** | The client must send an ACK for each message. ACKing a message implicitly acknowledges all prior messages on that subscription (cumulative). |
| **client-individual** | Like `client`, but each ACK applies only to the single message identified by its `id`. |

---

## Heartbeats

Heartbeats keep the connection alive and detect dead peers. The client and
server each advertise a send and receive interval during the handshake. The
negotiated interval in each direction is the maximum of the two sides'
values. A value of `0` disables that direction.

See [heartbeats.md](heartbeats.md) for a detailed guide on configuration
and negotiation in iridium-stomp.

---

## Transactions

STOMP supports lightweight transactions. A client issues `BEGIN` with a
unique transaction id, then includes that id on `SEND`, `ACK`, or `NACK`
frames. `COMMIT` applies all operations atomically; `ABORT` discards them.

---

## Receipts

Any client frame (except `CONNECT`) can include a `receipt` header. The
server responds with a `RECEIPT` frame carrying the same `receipt-id`,
confirming the frame was processed. This is the primary way to get
delivery confirmation from the broker.

---

## Content and encoding

- **content-type** — MIME type of the body (e.g., `text/plain`,
  `application/json`). Defaults to `text/plain` if omitted.
- **content-length** — byte length of the body. Required when the body
  contains NUL bytes; otherwise optional but recommended.
- **Header encoding** — STOMP 1.2 requires escaping of `\n` as `\\n`,
  `\r` as `\\r`, `:` as `\\c`, and `\\` as `\\\\` in header values.
  CONNECT and CONNECTED frames are exempt from this escaping, and brokers
  read that differently: Artemis takes CONNECT header values verbatim,
  RabbitMQ unescapes them, ActiveMQ Classic accepts either. iridium-stomp
  writes a colon in a CONNECT value as it is, which all three accept, and
  still escapes a backslash, CR or LF. A login or passcode containing a
  backslash therefore works on RabbitMQ and ActiveMQ Classic and is
  rejected by Artemis, which reads the escape literally. When reading
  CONNECTED, a value that is not validly escaped is taken verbatim.

---

## Version negotiation

The client sends `accept-version` listing the versions it supports (e.g.,
`1.2`). The server's CONNECTED frame includes a `version` header
confirming the chosen version. iridium-stomp defaults to requesting
version 1.2.

---

## iridium-stomp coverage

iridium-stomp implements the core STOMP 1.2 client features:

| Feature | Supported |
|---------|-----------|
| CONNECT / CONNECTED handshake | Yes |
| SEND | Yes |
| SUBSCRIBE / UNSUBSCRIBE | Yes |
| ACK / NACK (all modes) | Yes |
| Heartbeat negotiation and monitoring | Yes |
| Receipts | Yes |
| Header escaping / unescaping | Yes |
| Binary bodies with content-length | Yes |
| Automatic reconnection with resubscription | Yes |
| Transactions (BEGIN / COMMIT / ABORT) | Yes |
| TLS (`stomp+ssl`) | No — plain TCP only ([#94](https://github.com/iridiumdesign/iridium-stomp/issues/94)) |
| STOMP over WebSocket | No ([#113](https://github.com/iridiumdesign/iridium-stomp/issues/113)) |
