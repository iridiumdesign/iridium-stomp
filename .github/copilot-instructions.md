# GitHub Copilot Instructions for iridium-stomp

## Repository Overview

This is an asynchronous STOMP 1.2 client library for Rust, built on Tokio. The library focuses on correct frame parsing, automatic heartbeat management, transparent reconnection, and a small, explicit API.

**Status**: Early production use - heavily tested (400+ tests). Not every feature has been exercised in production, and APIs may change.

## Tech Stack

- **Language**: Rust (Edition 2024)
- **Async Runtime**: Tokio
- **Key Dependencies**: bytes, tokio-util (codec), futures, tracing, thiserror
- **Testing**: Standard Rust tests with fuzz, stress, and integration tests
- **CI**: GitHub Actions with RabbitMQ smoke tests

## Building and Testing

### Pre-submission Checklist

Before submitting any PR, always run:

```bash
# 1. Format code (auto-fix)
cargo fmt --all

# 2. Verify formatting
cargo fmt --all -- --check

# 3. Run lints (CI uses -D warnings)
cargo clippy --all-targets --all-features -- -D warnings

# 4. Run unit tests
cargo test --lib

# 5. Build examples
cargo build --examples
```

### Test Commands

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test heartbeat_unit    # Heartbeat parsing/negotiation
cargo test --test codec_heartbeat   # Wire format encoding/decoding
cargo test --test parser_unit       # Frame parsing edge cases
cargo test --test codec_fuzz        # Randomized chunk splitting
cargo test --test codec_stress      # Concurrent stress testing

# Run smoke integration test locally (requires RabbitMQ and environment variable)
# Note: The test is skipped by default unless RUN_STOMP_SMOKE=1 is set
RUN_STOMP_SMOKE=1 cargo test --test stomp_smoke
```

### Local RabbitMQ Setup

```bash
# Start RabbitMQ with STOMP plugin
docker compose up -d

# Run integration tests
./scripts/test-with-rabbit.sh

# Stop and cleanup
docker compose down -v
```

## Code Style and Conventions

### General Guidelines

- **No unnecessary comments**: Don't add comments unless they match existing style or explain complex logic
- **Use existing libraries**: Prefer existing dependencies over adding new ones
- **Minimal changes**: Make the smallest possible changes to accomplish the task
- **Format with rustfmt**: Always format code with `cargo fmt`

### Rust Conventions

- Follow standard Rust naming conventions (snake_case for functions/variables, CamelCase for types)
- Use `thiserror` for error types
- Use `tracing` for logging/debugging (not `println!`)
- Prefer explicit error handling over panics in library code

### Parser Architecture

The STOMP frame parser uses a slice-based approach:

- **Parser function**: `parse_frame_slice(input: &[u8])` in `src/parser.rs`
- **Returns**: 
  - `Ok(None)` when more bytes needed
  - `Ok(Some((command, headers, body_opt, consumed_bytes)))` on success
  - `Err(String)` on protocol error
- **Key principle**: Stateless parsing that handles arbitrary TCP chunk boundaries
- **Body modes**: Supports both Content-Length delimited and NUL-terminated bodies
- **Heartbeats**: Single LF (0x0A) treated as heartbeat frame

### Testing Strategy

- **Unit tests**: Test individual components in isolation
- **Fuzz tests**: Random chunk splitting to test parser robustness
- **Stress tests**: Concurrent operations to find race conditions
- **Integration tests**: Real RabbitMQ broker in CI with readiness checks
- **Regression tests**: Captured failing sequences added to test suite

When adding tests:
- Add unit tests for new behavior
- Consider fuzz/stress tests for parsing or concurrent code
- Add regression tests for bugs that were discovered

## Project Structure

```
src/
├── lib.rs           # Public API exports
├── codec.rs         # Tokio codec for STOMP frames
├── parser.rs        # Frame parsing logic
├── frame.rs         # Frame structure and builders
├── connection.rs    # Connection management, heartbeats, reconnection
├── subscription.rs  # Subscription handling and message streams
└── bin/
    └── stomp.rs     # CLI tool (requires 'cli' feature)

tests/               # Integration and regression tests
examples/            # Usage examples
```

## Important Caveats

### What This Library IS

- A STOMP 1.2 client implementation
- Async-first with Tokio
- Handles heartbeats, reconnection, and frame parsing correctly
- Production-ready testing approach

### What This Library IS NOT

- Not a STOMP server
- Not a message queue abstraction
- Not compatible with STOMP < 1.2
- Not broker-specific (no ActiveMQ/RabbitMQ extensions in core)

## CI Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs:

1. **Smoke integration test**: Builds RabbitMQ+STOMP container, verifies connectivity with robust readiness checks
2. **Format check**: `cargo fmt --all -- --check`
3. **Clippy**: `cargo clippy --all-targets --all-features -- -D warnings`
4. **Unit tests**: Matrix across stable/beta/nightly Rust
5. **Build examples**: Verifies examples compile

### Smoke Test Robustness

The integration test uses multi-stage readiness checks to prevent flaky failures:
1. Wait for RabbitMQ management API (broker starting)
2. Verify STOMP plugin is enabled (plugin operational)
3. Test STOMP port connection (port accepting connections)
4. Retry logic with exponential backoff in test itself

## Common Tasks

### Adding a New Feature

1. Add unit tests first
2. Implement the feature
3. Update examples if relevant
4. Run the full pre-submission checklist
5. Update documentation if API changed

### Fixing a Bug

1. Add a regression test that reproduces the bug
2. Fix the bug
3. Verify the test now passes
4. Run the pre-submission checklist

### Updating Dependencies

- Avoid updating dependencies unless necessary
- Test thoroughly after updates
- Check for breaking changes in changelogs

## Documentation

- Public API should have doc comments
- Complex algorithms should have explanatory comments
- Keep README.md in sync with API changes
- See CONTRIBUTING.md for developer-specific details

## Security Considerations

- No credentials in source code
- Use environment variables for sensitive data in examples
- STOMP protocol itself doesn't mandate encryption (consider TLS wrapper)
- Validate frame content-length to prevent memory issues
