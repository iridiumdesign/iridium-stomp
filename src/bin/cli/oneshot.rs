//! One-shot send: connect, publish a single message, disconnect, exit.
//!
//! This is the non-interactive side path of an interactive-first client. It
//! exists so a script can publish a message and learn from the exit code
//! whether the broker took it, without driving the REPL through a pipe.

use iridium_stomp::{ConnectOptions, Connection, Frame};
use std::time::Duration;

use super::args::Cli;
use super::exit_codes;
use super::plain::format_connection_error_pub;

/// Send a single message and return once the broker has confirmed it.
///
/// # Parameters
/// - `cli`: parsed arguments, for the broker address, credentials and timeout.
/// - `destination`: the STOMP destination to publish to.
/// - `body`: the message body, sent as `text/plain`.
pub async fn run(cli: &Cli, destination: &str, body: &str) -> Result<(), (String, u8)> {
    // Same guard the interactive `send` command applies, so a typo fails here
    // rather than being silently accepted by the broker.
    if !destination.starts_with('/') {
        return Err((
            format!(
                "Invalid destination '{}'. Must start with / (e.g., /topic/test, /queue/test)",
                destination
            ),
            exit_codes::PROTOCOL_ERROR,
        ));
    }

    let timeout = Duration::from_secs(cli.timeout);
    let options = ConnectOptions::default().disconnect_timeout(timeout);

    // `Connection::connect` retries an unreachable broker indefinitely, which is
    // right for a long-lived service and wrong for a script: without this bound
    // a --send at a dead broker would never return. See issue #68.
    let conn = tokio::time::timeout(
        timeout,
        Connection::connect_with_options(
            &cli.address,
            &cli.login,
            &cli.passcode,
            &cli.heartbeat,
            options,
        ),
    )
    .await
    .map_err(|_| {
        (
            format!(
                "Timed out after {}s connecting to {}",
                cli.timeout, cli.address
            ),
            exit_codes::NETWORK_ERROR,
        )
    })?
    .map_err(|e| format_connection_error_pub(&e, &cli.address))?;

    let frame = Frame::new("SEND")
        .header("destination", destination)
        .header("content-type", "text/plain")
        .set_body(body.as_bytes().to_vec());

    // Confirmed rather than fire-and-forget: an exit code that does not depend
    // on the broker having accepted the message would not be worth reporting.
    let sent = conn
        .send_frame_confirmed(frame, timeout)
        .await
        .map_err(|e| format_connection_error_pub(&e, &cli.address));

    // Disconnect cleanly whatever the send did, but let the send's outcome win:
    // it is what the caller asked about.
    let closed = conn.close().await;
    sent?;
    if let Err(e) = closed {
        eprintln!("Warning: broker did not confirm the disconnect: {}", e);
    }

    println!("Sent to {}", destination);
    Ok(())
}
