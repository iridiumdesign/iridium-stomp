# Contributing to iridium-stomp

Contributions are welcome — bug reports, fixes, docs, and features.

## Licensing

**By submitting a pull request, you agree that your contribution is
licensed under the MIT License**, the same terms as the rest of this
project. See [LICENSE](LICENSE).

This is the standard inbound-equals-outbound arrangement used across
the Rust ecosystem, and it's already how GitHub's Terms of Service treat
contributions to a licensed public repository. Stating it here is a
courtesy so you know what you're agreeing to rather than having to infer
it.

**There is no CLA, and you keep your copyright.** You are not assigning
anything or signing anything away — you retain ownership of what you
write and simply license it under the project's terms, exactly as every
other user of this crate does.

## Unsolicited agent-generated pull requests

**I don't accept them.** If a coding agent wrote the patch and you are
opening the PR without having discussed the work with me first, I will
close it unreviewed.

This is not a position on the tools — there is a
`.github/copilot-instructions.md` in this repository and I use agents on
this crate myself. It's a position on where the cost lands. A patch
takes minutes to generate and an hour to review properly, and on a
protocol client that hour is not optional: frame parsing is the part
that faces untrusted bytes, and the subtle bugs live in reconnect and
subscription state, where reading a diff tells you very little. What
tells you something is running it against a real broker. I can't do
that on your behalf for a patch you haven't run yourself.

Concretely, a PR gets closed on sight if it:

- arrives against an issue you never commented on
- was written by an agent working from the issue text alone
- says the tests couldn't be run in your environment

That last one is the tell. **If you didn't compile it, don't send it.**

Use whatever tools you like on work we've agreed on. Bring the same
judgment to the output that you'd bring to your own.

For what it's worth, the most useful patches this crate has received
came from people who hit the bug themselves against a live broker and
could say which one, which frames, and what they saw. That context is
the part an agent can't manufacture, and it's most of why the fix
landed.

## Before opening a pull request

The CI bar is the same one used locally, and the `justfile` is the
shortest way to clear it. `just check` runs formatting, clippy with
warnings as errors, the tests, the doc examples, and the example
binaries — every CI job except the broker smoke test.

```sh
cargo install --locked just   # once
just                          # list every recipe
just check                    # the gate
just check-all                # the gate plus the RabbitMQ smoke test
```

`just check-all` needs a container runtime; it builds a RabbitMQ with
STOMP baked in, runs the smoke test against it, and tears it down.

The `brokers` group is there because the interesting bugs in a STOMP
client are the places brokers disagree. `just rabbit`, `just activemq`,
and `just artemis` each bring one up on its own STOMP port so all three
can run at once, `just brokers` reports which are listening, and
`just brokers-down` stops them.

If you'd rather not install anything, this is the minimum by hand.
`just check` also compiles the doc examples and the example binaries,
which CI checks and these four commands do not:

```sh
# Formatting
cargo fmt --all -- --check

# Lints — warnings are errors
cargo clippy --all-targets --all-features -- -D warnings

# Library tests
cargo test --lib

# CLI tests — gated behind the `cli` feature, so `--lib` never builds them
cargo test --test cli_oneshot --features cli
```

## Project specifics

**Minimum supported Rust version: 1.88.** The crate uses edition 2024,
which needs 1.85, and a let-chain in `connection.rs` that needs 1.88.
Please don't raise the floor without a reason worth stating in the PR.

**Features.** The default feature set is empty — the library has no
optional dependencies enabled by default. The `cli` feature pulls in
`clap`, `ratatui`, `crossterm`, and `chrono` and builds the `stomp`
binary. Keep CLI-only code behind that gate so library consumers don't
pay for it.

**Packaging.** `Cargo.toml` uses an explicit `include` list with
leading slashes to anchor paths at the package root. If you add a
directory that should ship in the published crate, add it there.

## Scope

This is an async STOMP 1.2 client. Protocol correctness and a small,
predictable API matter more than surface area. If you're planning
something large, open an issue first so we can talk about shape before
you spend the time.

## Bug reports

A minimal reproduction is worth more than a description. Include the
broker you're talking to, the crate version, and the frames involved if
the issue is protocol-level.
