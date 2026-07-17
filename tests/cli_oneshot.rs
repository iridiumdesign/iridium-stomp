//! Coverage for #93's one-shot send: `stomp --send <dest> <body>`.
//!
//! These drive the real binary, because the thing under test is the contract a
//! script sees - the exit code - not an internal function. `CARGO_BIN_EXE_stomp`
//! is set by cargo for integration tests.
//!
//! Requires the `cli` feature, which is what builds the binary at all.
#![cfg(feature = "cli")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

/// How the stub broker should answer a SEND that requested a receipt.
#[derive(Clone, Copy)]
enum OnSend {
    /// Confirm it, as a broker that accepted the message.
    Receipt,
    /// Reject it with an ERROR carrying the receipt id, as a broker refusing
    /// the publish (bad permissions, unknown destination).
    Error,
    /// Say nothing at all, as a broker that has stalled.
    Silence,
}

/// Frames the broker saw, as raw text.
type Seen = Arc<Mutex<Vec<String>>>;

fn start_broker(on_send: OnSend) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();

    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut buf = [0u8; 8192];

        if stream.read(&mut buf).is_ok() {
            let _ = stream.write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0");
            let _ = stream.flush();
        }

        // TCP is a byte stream, so a single read may carry a partial frame or
        // several at once. Accumulate bytes and act only on complete,
        // NUL-terminated frames; a partial trailing frame waits in `acc` for
        // the rest to arrive.
        let mut acc: Vec<u8> = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    acc.extend_from_slice(&buf[..n]);
                    while let Some(pos) = acc.iter().position(|&b| b == 0) {
                        let frame: Vec<u8> = acc.drain(..=pos).collect();
                        let text = String::from_utf8_lossy(&frame);
                        let raw = text.trim_matches('\0').trim_start();
                        if raw.trim().is_empty() {
                            continue;
                        }
                        let raw = raw.to_string();
                        let command = raw.lines().next().unwrap_or("").to_string();
                        seen_clone.lock().unwrap().push(raw.clone());

                        let Some(id) = receipt_id_of(&raw) else {
                            continue;
                        };
                        let reply = match (command.as_str(), on_send) {
                            ("SEND", OnSend::Receipt) => {
                                format!("RECEIPT\nreceipt-id:{}\n\n\0", id)
                            }
                            ("SEND", OnSend::Error) => format!(
                                "ERROR\nreceipt-id:{}\nmessage:publish denied\n\n{}\0",
                                id, "not allowed"
                            ),
                            ("SEND", OnSend::Silence) => continue,
                            // Always answer the DISCONNECT so teardown is not
                            // what the timing tests end up measuring.
                            ("DISCONNECT", _) => format!("RECEIPT\nreceipt-id:{}\n\n\0", id),
                            _ => continue,
                        };
                        let _ = stream.write_all(reply.as_bytes());
                        let _ = stream.flush();
                    }
                }
            }
        }
    });

    (addr, seen)
}

fn receipt_id_of(frame: &str) -> Option<&str> {
    frame.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case("receipt").then_some(value)
    })
}

fn commands(seen: &Seen) -> Vec<String> {
    seen.lock()
        .unwrap()
        .iter()
        .map(|f| f.lines().next().unwrap_or("").to_string())
        .collect()
}

fn stomp(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_stomp"))
        .args(args)
        .output()
        .expect("failed to run the stomp binary")
}

/// Exit codes from `src/bin/cli/mod.rs::exit_codes`.
const SUCCESS: i32 = 0;
const NETWORK_ERROR: i32 = 1;
const PROTOCOL_ERROR: i32 = 3;
const FRAME_REJECTED: i32 = 4;

#[test]
fn send_confirmed_by_the_broker_exits_zero() {
    let (addr, seen) = start_broker(OnSend::Receipt);

    let out = stomp(&["-a", &addr, "--send", "/queue/orders", "hello"]);

    assert_eq!(out.status.code(), Some(SUCCESS), "stderr: {}", stderr(&out));

    let cmds = commands(&seen);
    assert_eq!(
        cmds,
        vec!["SEND", "DISCONNECT"],
        "a one-shot should publish and then disconnect cleanly"
    );

    let send = seen.lock().unwrap()[0].clone();
    assert!(
        send.contains("destination:/queue/orders"),
        "frame: {}",
        send
    );
    assert!(send.contains("hello"), "body missing from frame: {}", send);
    assert!(
        receipt_id_of(&send).is_some(),
        "the SEND must request a receipt, or the exit code means nothing: {}",
        send
    );
}

#[test]
fn send_rejected_by_the_broker_exits_frame_rejected() {
    let (addr, _seen) = start_broker(OnSend::Error);

    let out = stomp(&["-a", &addr, "--send", "/queue/forbidden", "payload"]);

    assert_eq!(
        out.status.code(),
        Some(FRAME_REJECTED),
        "a broker refusing the publish must not look like success. stderr: {}",
        stderr(&out)
    );
}

#[test]
fn send_to_a_silent_broker_times_out_rather_than_hanging() {
    let (addr, _seen) = start_broker(OnSend::Silence);

    let started = Instant::now();
    let out = stomp(&[
        "-a",
        &addr,
        "--send",
        "/queue/x",
        "payload",
        "--timeout",
        "1",
    ]);
    let elapsed = started.elapsed();

    assert_eq!(
        out.status.code(),
        Some(PROTOCOL_ERROR),
        "stderr: {}",
        stderr(&out)
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "--timeout must bound the wait (took {:?})",
        elapsed
    );
}

#[test]
fn send_to_an_unreachable_broker_times_out_rather_than_retrying_forever() {
    // Connection::connect retries indefinitely (#68), so without an explicit
    // bound in the one-shot path this would never return.
    let started = Instant::now();
    let out = stomp(&[
        "-a",
        "127.0.0.1:59999",
        "--send",
        "/queue/x",
        "payload",
        "--timeout",
        "2",
    ]);
    let elapsed = started.elapsed();

    assert_eq!(
        out.status.code(),
        Some(NETWORK_ERROR),
        "stderr: {}",
        stderr(&out)
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "a dead broker must fail fast, not retry forever (took {:?})",
        elapsed
    );
}

#[test]
fn send_rejects_a_destination_that_is_not_a_path() {
    let (addr, _seen) = start_broker(OnSend::Receipt);

    let out = stomp(&["-a", &addr, "--send", "queue/no-leading-slash", "payload"]);

    assert_eq!(
        out.status.code(),
        Some(PROTOCOL_ERROR),
        "stderr: {}",
        stderr(&out)
    );
    assert!(
        stderr(&out).contains("Must start with /"),
        "stderr: {}",
        stderr(&out)
    );
}

#[test]
fn send_conflicts_with_the_interactive_modes() {
    // --send is the non-interactive side path; combining it with the TUI or a
    // subscription is a mistake worth naming rather than silently resolving.
    for extra in [vec!["--tui"], vec!["-s", "/queue/x"], vec!["--summary"]] {
        let mut args = vec!["-a", "127.0.0.1:59999", "--send", "/queue/x", "payload"];
        args.extend(extra.iter());
        let out = stomp(&args);
        assert!(
            !out.status.success(),
            "expected {:?} to be rejected alongside --send",
            extra
        );
        assert!(
            stderr(&out).contains("cannot be used with"),
            "expected a conflict error for {:?}, got: {}",
            extra,
            stderr(&out)
        );
    }
}

fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}
