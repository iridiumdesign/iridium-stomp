# Security Policy

## Reporting a vulnerability

Please **don't** open a public issue for a security problem.

Use GitHub's [private vulnerability
reporting](https://github.com/iridiumdesign/iridium-stomp/security/advisories/new)
on this repository. If that isn't available to you, email
**brad@iridiumdesign.com**.

I maintain this crate on my own time, so I can't offer a same-day
response. I aim to acknowledge a report within a week and to have a fix
or a clear explanation within thirty days. If something is being
actively exploited, say so in the subject line and I'll prioritise it.

Please give me a reasonable window to ship a fix before publishing
details. I'll credit you in the advisory unless you'd rather I didn't.

## Supported versions

This crate is pre-1.0. Only the most recent release receives fixes —
there are no maintained back-branches. If you're pinned to an older
version, plan on upgrading to get a security fix.

## Scope

This is a STOMP client. It parses frames arriving over a network
connection from a broker, so it treats broker traffic as untrusted
input. A compromised or hostile broker, or an attacker positioned on the
network, is a realistic threat model.

**In scope:**

- Memory exhaustion or unbounded allocation triggered by malformed or
  hostile frames.
- Panics reachable from network input. This crate is a library, so a
  panic is a denial of service in whatever application embeds it.
- Header parsing or escaping flaws that let an attacker inject or forge
  headers.
- Integer overflow or truncation in `content-length` handling.

**Existing hardening.** The codec bounds decoding by `max_frame_size`
and rejects oversized frames on total wire size, not just on the
declared `content-length` — so a lying length header alone shouldn't
cause unbounded buffering. Configure the limit with
`StompCodec::with_max_frame_size`. If you find a way around that bound,
that's exactly the kind of report I want.

**No `unsafe`.** The crate contains no `unsafe` blocks, so memory-safety
issues would have to come from a dependency rather than from this code.

## Out of scope

**Transport encryption.** This crate speaks plaintext TCP and has no TLS
support. That's a known limitation, not a vulnerability — reports that
amount to "traffic is unencrypted" will be closed as such. If you need
encryption today, terminate TLS in front of the client with a proxy or
tunnel.

Also out of scope: weaknesses in the broker itself, credentials you've
placed in your own configuration, and anything requiring an attacker to
already control the machine running your application.
