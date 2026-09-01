# iridium-stomp — the build.
#
# Everything in the `gates` group mirrors .github/workflows/ci.yml. If
# `just check` passes locally, CI passes too; if the two ever disagree,
# the workflow is the authority and this file is the bug.
#
# The one thing this cannot mirror is CI's smoke job, which stands up a
# RabbitMQ with STOMP baked in. `just smoke` does that here, and it
# needs a container runtime — see the `brokers` group.

# The floor in Cargo.toml. Named once here so `just msrv` and the
# rust-version field cannot drift apart silently.
MSRV := "1.88"

# The CLI, the examples, and most integration tests live behind `cli`.
# The default feature set is deliberately empty, so almost nothing
# interesting builds without this.
FEAT := "--features cli"

# STOMP ports, offset so all three brokers can run at once. The offsets
# are set in the stack files, not here; this is for `just brokers`.
RABBIT := "61613"
ACTIVEMQ := "61614"
ARTEMIS := "61615"

# List the recipes. Hidden: `just` runs it, nobody types it.
[private]
default:
    @just --list --unsorted

# ── build ──────────────────────────────────────────────────────────

[doc('Build the library.')]
[group('build')]
build:
    cargo build --all-features

[doc('Build the stomp CLI binary.')]
[group('build')]
cli:
    cargo build {{ FEAT }} --bin stomp

[doc('Build every example. CI builds these; nothing else compiles them.')]
[group('build')]
examples:
    cargo build --examples

[doc('Build the rustdoc and open it.')]
[group('build')]
doc:
    cargo doc --all-features --no-deps --open

# ── run ────────────────────────────────────────────────────────────

# Pass-through to the CLI so you do not have to remember the feature
# flag: just stomp -- --help

[doc('Run the CLI: just stomp -- --help')]
[group('run')]
stomp *args:
    cargo run {{ FEAT }} --bin stomp -- {{ args }}

[doc('Run one example: just example subscribe')]
[group('run')]
example name *args:
    cargo run --example {{ name }} -- {{ args }}

# ── gates ──────────────────────────────────────────────────────────

[doc('Formatting.')]
[group('gates')]
fmt:
    cargo fmt --all -- --check

[doc('Reformat in place.')]
[group('gates')]
fix:
    cargo fmt --all

# Worth knowing that this one can go red without anyone touching the
# code: a clippy release tightens a lint and yesterday's green tree
# fails today. That is what happened in #112 — result_large_err started
# firing on a 144-byte ConnError variant that had been there for months.

[doc('Lints. Warnings are errors, same as CI.')]
[group('gates')]
clippy:
    cargo clippy --all-targets --all-features -- -D warnings

[doc('Unit and integration tests, CLI feature on — same as CI.')]
[group('gates')]
test:
    cargo test --all-targets {{ FEAT }}

# Doc examples were never compiled by CI until #105, so API drift rotted
# them silently. Broker-touching examples are `no_run`, so this compiles
# everything and runs what does not need a socket.

[doc('Compile and run the doc examples.')]
[group('gates')]
doc-test:
    cargo test --doc {{ FEAT }}

[doc('Build and test on the MSRV floor.')]
[group('gates')]
msrv:
    rustup toolchain install {{ MSRV }} --profile minimal
    cargo +{{ MSRV }} test --all-targets {{ FEAT }}

[doc('The gate: everything CI runs except the broker smoke test.')]
[group('gates')]
check: fmt clippy test doc-test examples

[doc('The full gate, smoke test included. Needs a container runtime.')]
[group('gates')]
check-all: check smoke

# ── brokers ────────────────────────────────────────────────────────

# Three brokers, three STOMP ports, because they disagree and the
# disagreements are the interesting part — ActiveMQ's resubscribe
# behavior is what #72 was about. Ports are offset in the stack files
# so all three can run at once.

[doc('Start RabbitMQ on 61613.')]
[group('brokers')]
rabbit:
    docker compose -f docker-compose.yaml up -d

[doc('Start ActiveMQ Classic on 61614.')]
[group('brokers')]
activemq:
    docker compose -f activemq-stack.yaml up -d

[doc('Start Artemis on 61615.')]
[group('brokers')]
artemis:
    docker compose -f artemis-stack.yaml up -d

[doc('Stop every broker stack.')]
[group('brokers')]
brokers-down:
    -docker compose -f docker-compose.yaml down
    -docker compose -f activemq-stack.yaml down
    -docker compose -f artemis-stack.yaml down

[doc('Which STOMP ports are listening.')]
[group('brokers')]
brokers:
    #!/usr/bin/env sh
    for row in "rabbitmq {{ RABBIT }}" "activemq {{ ACTIVEMQ }}" \
               "artemis {{ ARTEMIS }}"; do
        set -- $row
        if nc -z 127.0.0.1 "$2" 2>/dev/null; then
            printf '  %-10s %-6s up\n' "$1" "$2"
        else
            printf '  %-10s %-6s --\n' "$1" "$2"
        fi
    done

# CI's smoke job in one command: build a RabbitMQ image with STOMP
# baked in, wait for the port, run the smoke test, tear it all down.

[doc('The CI smoke test against a real RabbitMQ. Builds and tears down.')]
[group('brokers')]
smoke:
    ./scripts/test-with-rabbit.sh

# ── ship ───────────────────────────────────────────────────────────

# Nothing here publishes. `cargo publish` is a one-way door and stays a
# thing you type yourself, on purpose.

[doc('What ships in the published crate.')]
[group('ship')]
manifest:
    cargo package --list --allow-dirty

[doc('Dry-run the release packaging.')]
[group('ship')]
package:
    cargo package --all-features

[doc('Preflight a release: the gate, then the packaging.')]
[group('ship')]
release-check: check package

# ── tidy ───────────────────────────────────────────────────────────

[doc('Remove build artifacts.')]
[group('tidy')]
clean:
    cargo clean
