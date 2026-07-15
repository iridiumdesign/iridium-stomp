# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `Connection::close` did not terminate the background task; it reconnected indefinitely ([#96])
  - Every closed connection left a task re-establishing a broker session every 30s for the life of the process, so a long-running service accumulated them. From the broker's side this looked like sessions and reconnects with no client that owned them.
  - Two independent causes. The task subscribed to the shutdown broadcast from inside its own loop, so a `close` issued before the task was first polled reached a channel with no subscribers and was discarded outright, and a shutdown arriving during the reconnect backoff was dropped by the next iteration's re-subscribe. Separately, the inner select consumed the shutdown message while the reconnect check re-read the same drained receiver and concluded it should reconnect — reachable whenever a `Connection` clone kept the outbound channel open, as the CLI does.
  - The task now subscribes once, before it is spawned, and keeps that receiver for its lifetime; the reconnect check consults a flag set where the signal is consumed rather than re-reading it

- Fast RECEIPT could be lost between `send_frame_with_receipt` and `wait_for_receipt` ([#82])
  - `send_frame_with_receipt` registered a sender whose receiver it dropped immediately. A RECEIPT arriving before `wait_for_receipt` ran was delivered to that orphaned receiver and its registration removed, so the subsequent wait blocked for its full timeout and returned `ConnError::ReceiptTimeout` for a frame the broker had accepted. Most likely against a localhost or LAN broker.
  - The confirmation receiver is now owned by the caller from the moment the frame is queued, so the response cannot arrive before there is somewhere to put it
- `send_frame_with_receipt` no longer leaves a stale registration behind when the send itself fails
- A caller-supplied `receipt` header silently broke `send_frame_with_receipt` and `send_frame_confirmed`
  - `Frame::header` appends rather than overwrites, so a hand-set id put two `receipt` headers on the wire. Brokers honour the first, while the client tracked the generated id, so the confirmation never matched and the wait always timed out.
  - Caller-supplied `receipt` headers are now replaced by the generated one, matched case-insensitively to agree with header lookup

### Changed

- **Breaking**: `SubscriptionOptions::durable_queue` is removed ([#91])
  - Despite the name it did not request durability. Its only effect was to override the `destination` argument, so a user who set it expecting an ActiveMQ durable subscription silently got a plain one against a different destination — and because STOMP cannot reject a "wrong" subscription header, the broker accepted it without complaint and messages quietly failed to persist across a disconnect.
  - Durability is requested through `SubscriptionOptions::headers`, which is what the struct-level docs already described. Pass the broker's own header (ActiveMQ's `activemq.subscriptionName`, for example). Where the durable queue is declared administratively, as on RabbitMQ, name it as the `destination` and no options are needed.
  - Migration: move the value into the `destination` argument. `subscribe_with_options("/exchange/x", ack, opts)` with `durable_queue: Some("/queue/y")` was only ever subscribing to `/queue/y`, so it becomes `subscribe_with_options("/queue/y", ack, opts)`.
- **Breaking**: `Connection::send_frame_with_receipt` returns `ReceiptHandle` instead of `String`. Await the confirmation with `handle.wait(timeout)` and read the generated id with `handle.receipt_id()`.
- **Breaking**: `Connection::wait_for_receipt` is removed, superseded by `ReceiptHandle::wait`
  - Before: `let id = conn.send_frame_with_receipt(f).await?; conn.wait_for_receipt(&id, t).await?;`
  - After: `let h = conn.send_frame_with_receipt(f).await?; h.wait(t).await?;`
  - Sending several frames before awaiting any still works — each handle is independent
- `send_frame_confirmed` is now a thin wrapper over `send_frame_with_receipt` + `ReceiptHandle::wait`, rather than duplicating the registration and timeout logic

### Documentation

- `docs/durable_subscriptions.md` no longer presents `durable_queue` as the RabbitMQ durability mechanism ([#91]). Durability there is a property of the administratively declared queue, so the guide now simply subscribes to it by name. `docs/subscriptions.md` gains a note that STOMP has no durable-subscription concept of its own and that durability is whatever the broker defines it to be.
- `examples/subscribe.rs` passed `/exchange/topic` as the destination while setting `durable_queue` to `/queue/example-durable`, so it silently subscribed to the latter and demonstrated the confusion the field caused ([#91]). It now names the queue it means.
- README's receipt examples no longer set a `receipt` header by hand. Both `send_frame_with_receipt` and `send_frame_confirmed` add a generated one, and because `Frame::header` appends rather than overwrites, a hand-set id produced a frame with two `receipt` headers; brokers honour the first, so the confirmation never matched and the wait always timed out.

## [0.4.2] - 2026-06-24

### Added

- Project banner in the README

### Changed

- Repository moved to the `iridiumdesign` GitHub organization; `repository` and
  `homepage` metadata and the URL printed by the CLI now point to
  `github.com/iridiumdesign/iridium-stomp`

## [0.4.1] - 2026-05-13

### Fixed

- Resubscribe loop ran on the initial connection, causing duplicate subscriptions ([#72])
  - User-issued `SUBSCRIBE` raced the background loop's resubscribe pass on the first iteration, producing duplicate deliveries on ActiveMQ Classic and `There already is a subscription for: <id>` errors on Artemis
  - Resubscribe is now correctly limited to reconnect iterations
- Silent failures during resubscription after reconnect ([#48])
  - `sink.send()` errors during the resubscribe pass were swallowed; they are now surfaced via `tracing::warn!` with destination, subscription id, and error context

### Changed

- Restored 100% rustdoc coverage on the public API ([#73])
- Applied `clippy::unnecessary_sort_by` fix in CLI state module (lint added in clippy 1.95) ([#73])

### Dependencies

- `rand` 0.8.5 → 0.8.6 ([#70])

## [0.4.0] - 2026-04-03

### Added

- Initial connection retry with exponential backoff ([#67])
  - `Connection::connect` now retries automatically when the broker is unreachable or crashes mid-handshake (1s → 2s → 4s → 8s → 16s, capped at 30s)
  - Services can start before the broker is available
  - Authentication failures (`ConnError::ServerRejected`) still fail immediately so bad configuration surfaces fast
- `Connection::send()` convenience method for sending a text message in one call
- Tracing observability — connect and reconnect lifecycle events instrumented with the `tracing` crate (warn on failures, info on success)

### Changed

- **Breaking**: `Connection::connect` no longer returns `ConnError::Io` on initial connection failure — it retries instead. Only `ConnError::ServerRejected` returns immediately.
- Examples split into focused use cases: `send`, `send_advanced`, `listen` (replaces `quickstart`)
- Docker Swarm config replaced with Docker Compose for broader accessibility
- Documentation overhaul across subscriber guide, heartbeats, durable subscriptions, README, and all examples ([#64], [#65])

## [0.3.2] - 2026-03-17

### Added

- New `multi_subscribe` example demonstrating multiple destinations, per-message ACK, and error monitoring ([#63])
- Subscriber guide at `docs/subscriber-guide.md`
- `tokio::signal` support in the CLI

### Changed

- Reconnect backoff is now stability-aware: stable connections reset to 1s instead of continuing to back off ([#60])
- CLI error handling improvements ([#63])

### Dependencies

- `bytes` 1.10.1 → 1.11.1 ([#61])
- `time` 0.3.46 → 0.3.47 ([#62])

## [0.3.1] - 2026-01-24

### Fixed

- Update `ratatui` to 0.30 to fix transitive `lru` vulnerability (RUSTSEC-2026-0002)

## [0.3.0] - 2026-01-24

### Added

- Full TUI mode for CLI with `--tui` flag ([#54])
  - Activity panel with live subscription counts and color coding
  - Message panel with scrollable history and timestamps
  - Animated heartbeat indicator (✦ pulse when active, ◇ idle, ! late)
  - Command history navigation with up/down arrows
  - Header toggle with Ctrl+H
  - Session reports with `report <file>` and `summary <file>` commands
  - Destination validation with warnings for non-standard patterns
  - Color-coded messages: errors (red), warnings (yellow), info (cyan), sent (blue)
- `ConnectOptions::with_heartbeat_notify()` for subscribing to heartbeat events
- `--summary` CLI flag to print session summary on exit

### Changed

- CLI refactored into modular structure (args, commands, state, tui, plain modules)

## [0.2.1] - 2026-01-22

### Fixed

- Connection errors (invalid credentials, server rejections) are now reported immediately
  - Previously, ERROR frames during CONNECT were silently ignored
  - `connect()` now returns `ConnError::ServerRejected` on authentication failure
  - Initial STOMP handshake completes before `connect()` returns

### Added

- `ConnError::ServerRejected(ServerError)` variant for connection-time errors

### Changed

- CLI now reports connection errors with clear messages and distinct exit codes
  - Exit 1: Network errors (connection refused, timeout)
  - Exit 2: Authentication errors (invalid credentials)
  - Exit 3: Protocol errors

## [0.2.0] - 2026-01-16

### Fixed

- Implement header escaping per STOMP 1.2 spec ([#32], [#37])
  - Headers containing `\`, `\n`, `\r`, or `:` are now properly escaped/unescaped
  - Invalid escape sequences now return parse errors

### Added

- RECEIPT frame support for delivery confirmation ([#33])
  - `Frame::receipt()` builder method for requesting receipts
  - `Connection::send_frame_with_receipt()` to send with tracking
  - `Connection::wait_for_receipt()` to await confirmation with timeout
  - `Connection::send_frame_confirmed()` convenience method
  - `ConnError::ReceiptTimeout` error variant for timeout handling
- Custom CONNECT headers and version negotiation support ([#34])
  - `ConnectOptions` struct with builder methods for customizing connection
  - `Connection::connect_with_options()` for advanced connection setup
  - Support for `client-id` header (required for ActiveMQ durable subscriptions)
  - Configurable `host` header for virtual hosts
  - Configurable `accept-version` for STOMP version negotiation
  - Custom headers support for broker-specific requirements
- ERROR frames surfaced as first-class type ([#35])
  - `ReceivedFrame` enum distinguishes normal frames from errors
  - `ServerError` struct with `message`, `body`, `receipt_id`, and original frame
  - `Connection::next_frame()` now returns `Option<ReceivedFrame>` (**breaking change**)
  - Pattern matching enables type-safe error handling
- Heartbeat configuration constants and builder ([#36])
  - `Connection::NO_HEARTBEAT` constant for disabling heartbeats
  - `Connection::DEFAULT_HEARTBEAT` constant for 10-second intervals
  - `Heartbeat` struct for type-safe heartbeat configuration
  - `Heartbeat::new()`, `Heartbeat::disabled()`, `Heartbeat::from_duration()` constructors
  - `Display` implementation for STOMP protocol format
- `Frame::get_header()` helper method for retrieving header values

### Changed

- **Breaking**: `Connection::next_frame()` now returns `Option<ReceivedFrame>` instead of `Option<Frame>`. Use pattern matching to handle both normal frames and server errors.

## [0.1.0] - 2025-01-14

### Added

- Initial release
- Async STOMP 1.2 client with Tokio runtime
- Automatic heartbeat negotiation and management
- Transparent reconnection with exponential backoff
- Subscription management with automatic resubscription on reconnect
- ACK modes: Auto, Client (cumulative), ClientIndividual
- Transaction support (BEGIN/COMMIT/ABORT)
- Binary body handling with content-length
- Feature-gated CLI (`--features cli`)
- Comprehensive test suite (150+ tests)

[Unreleased]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.3.2...v0.4.0
[0.3.2]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/iridiumdesign/iridium-stomp/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/iridiumdesign/iridium-stomp/releases/tag/v0.1.0
[#32]: https://github.com/iridiumdesign/iridium-stomp/issues/32
[#33]: https://github.com/iridiumdesign/iridium-stomp/issues/33
[#34]: https://github.com/iridiumdesign/iridium-stomp/issues/34
[#35]: https://github.com/iridiumdesign/iridium-stomp/issues/35
[#36]: https://github.com/iridiumdesign/iridium-stomp/issues/36
[#37]: https://github.com/iridiumdesign/iridium-stomp/pull/37
[#48]: https://github.com/iridiumdesign/iridium-stomp/issues/48
[#54]: https://github.com/iridiumdesign/iridium-stomp/pull/54
[#60]: https://github.com/iridiumdesign/iridium-stomp/pull/60
[#61]: https://github.com/iridiumdesign/iridium-stomp/pull/61
[#62]: https://github.com/iridiumdesign/iridium-stomp/pull/62
[#63]: https://github.com/iridiumdesign/iridium-stomp/pull/63
[#64]: https://github.com/iridiumdesign/iridium-stomp/pull/64
[#65]: https://github.com/iridiumdesign/iridium-stomp/pull/65
[#67]: https://github.com/iridiumdesign/iridium-stomp/pull/67
[#70]: https://github.com/iridiumdesign/iridium-stomp/pull/70
[#72]: https://github.com/iridiumdesign/iridium-stomp/pull/72
[#73]: https://github.com/iridiumdesign/iridium-stomp/pull/73
[#82]: https://github.com/iridiumdesign/iridium-stomp/issues/82
[#91]: https://github.com/iridiumdesign/iridium-stomp/issues/91
[#96]: https://github.com/iridiumdesign/iridium-stomp/issues/96
