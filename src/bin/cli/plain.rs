use iridium_stomp::connection::{AckMode, ConnError};
use iridium_stomp::{ConnectOptions, Connection, Frame};
use std::io::{self, BufRead, Write};
use tokio::sync::mpsc;

use super::args::Cli;
use super::commands::{CommandResult, execute_command, print_help};
use super::state::{SharedState, new_shared_state};

/// Run the CLI in plain (non-TUI) mode
pub async fn run(cli: &Cli) -> Result<(), (String, u8)> {
    println!("Connecting to {}...", cli.address);

    // Parse heartbeat to get interval for state
    let hb_parts: Vec<&str> = cli.heartbeat.split(',').collect();
    let hb_interval = hb_parts
        .get(1)
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(10000);

    // Create heartbeat notification channel
    let (hb_tx, mut hb_rx) = mpsc::channel::<()>(16);

    // Build connection options
    let options = ConnectOptions::default().with_heartbeat_notify(hb_tx);

    let conn = Connection::connect_with_options(
        &cli.address,
        &cli.login,
        &cli.passcode,
        &cli.heartbeat,
        options,
    )
    .await
    .map_err(|e| format_connection_error(&e, &cli.address))?;

    println!("Connected.");

    // Create shared state
    let state = new_shared_state(cli.address.clone(), cli.login.clone(), hb_interval);

    // Channel for new subscription requests
    let (sub_tx, mut sub_rx) = mpsc::channel::<String>(16);

    // Subscribe to requested destinations
    for dest in &cli.subscribe {
        subscribe_destination(&conn, dest, state.clone()).await?;
    }

    // Spawn heartbeat monitor task
    let state_hb = state.clone();
    tokio::spawn(async move {
        while hb_rx.recv().await.is_some() {
            let mut s = state_hb.lock().await;
            s.record_heartbeat();
        }
    });

    // Spawn task to handle new subscription requests
    let conn_sub = conn.clone();
    let state_sub = state.clone();
    tokio::spawn(async move {
        while let Some(dest) = sub_rx.recv().await {
            if let Err((msg, _)) = subscribe_destination(&conn_sub, &dest, state_sub.clone()).await
            {
                eprintln!("{}", msg);
            }
        }
    });

    // Spawn task to monitor for ERROR frames from the broker
    let conn_err = conn.clone();
    let state_err = state.clone();
    tokio::spawn(async move {
        loop {
            match conn_err.next_frame().await {
                Some(iridium_stomp::ReceivedFrame::Error(err)) => {
                    let mut s = state_err.lock().await;
                    let msg = if let Some(ref body) = err.body {
                        format!("{}: {}", err.message, body)
                    } else {
                        err.message.clone()
                    };
                    eprintln!("\n[BROKER ERROR] {}", msg);
                    // Print headers for additional context
                    for (k, v) in &err.frame.headers {
                        eprintln!("  {}: {}", k, v);
                    }
                    s.record_message("BROKER ERROR", msg, err.frame.headers.clone());
                    print!("> ");
                    let _ = io::stdout().flush();
                }
                Some(iridium_stomp::ReceivedFrame::Frame(_)) => {
                    // Other frames are handled by subscription receivers
                }
                None => break, // Connection closed
            }
        }
    });

    // Channel to receive user commands from stdin reader
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<String>(16);

    // Spawn blocking stdin reader
    std::thread::spawn(move || {
        let stdin = io::stdin();
        let reader = stdin.lock();
        for line in reader.lines() {
            match line {
                Ok(l) => {
                    if cmd_tx.blocking_send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    println!();
    print_help();
    println!();

    // Main command loop
    loop {
        print!("> ");
        let _ = io::stdout().flush();

        let line = match cmd_rx.recv().await {
            Some(l) => l,
            None => break,
        };

        match execute_command(&line, &conn, state.clone(), &sub_tx, false).await {
            CommandResult::Ok => {}
            CommandResult::Quit => {
                println!("Disconnecting...");
                if cli.summary {
                    let s = state.lock().await;
                    println!("{}", s.generate_summary());
                }
                if let Err(e) = conn.close().await {
                    eprintln!("Warning: broker did not confirm the disconnect: {}", e);
                }
                break;
            }
            CommandResult::Info(msg) => {
                println!("{}", msg);
            }
            CommandResult::Error(msg) => {
                eprintln!("{}", msg);
            }
        }
    }

    Ok(())
}

/// Subscribe to a destination and spawn a message handler task
async fn subscribe_destination(
    conn: &Connection,
    dest: &str,
    state: SharedState,
) -> Result<(), (String, u8)> {
    let sub = conn.subscribe(dest, AckMode::Auto).await.map_err(|e| {
        (
            format!("Failed to subscribe to '{}': {}", dest, e),
            super::exit_codes::PROTOCOL_ERROR,
        )
    })?;

    println!("Subscribed to: {}", dest);

    // Register in state
    {
        let mut s = state.lock().await;
        s.register_subscription(dest);
    }

    // Spawn a task to print incoming messages for this subscription
    let dest_clone = dest.to_string();
    let state_clone = state.clone();
    let mut rx = sub.into_receiver();
    tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            handle_message(&dest_clone, &frame, state_clone.clone()).await;
        }
    });

    Ok(())
}

/// Handle an incoming message
async fn handle_message(dest: &str, frame: &Frame, state: SharedState) {
    // Extract body
    let body = if frame.body.is_empty() {
        String::new()
    } else {
        match std::str::from_utf8(&frame.body) {
            Ok(s) => s.to_string(),
            Err(_) => format!("({} bytes, binary)", frame.body.len()),
        }
    };

    // Record in state
    {
        let mut s = state.lock().await;
        s.record_message(dest, body.clone(), frame.headers.clone());
    }

    // Print to console
    println!("\n[{}] MESSAGE received:", dest);
    for (k, v) in &frame.headers {
        println!("  {}: {}", k, v);
    }
    if !frame.body.is_empty() {
        match std::str::from_utf8(&frame.body) {
            Ok(s) => println!("  Body: {}", s),
            Err(_) => println!("  Body: ({} bytes, binary)", frame.body.len()),
        }
    }
    print!("> ");
    let _ = io::stdout().flush();
}

/// Format a connection error with user-friendly messaging (internal)
fn format_connection_error(err: &ConnError, address: &str) -> (String, u8) {
    format_connection_error_pub(err, address)
}

/// Format a connection error with user-friendly messaging (public)
pub fn format_connection_error_pub(err: &ConnError, address: &str) -> (String, u8) {
    match err {
        ConnError::Io(io_err) => {
            let message = match io_err.kind() {
                std::io::ErrorKind::ConnectionRefused => {
                    format!("Connection refused: {}", address)
                }
                std::io::ErrorKind::TimedOut => {
                    format!("Connection timed out: {}", address)
                }
                _ => {
                    format!("Connection failed: {}", io_err)
                }
            };
            (message, super::exit_codes::NETWORK_ERROR)
        }
        ConnError::ServerRejected(server_err) => {
            let mut message = format!("Authentication failed: {}", server_err.message);
            if let Some(body) = &server_err.body {
                message.push_str(&format!(" ({})", body));
            }
            (message, super::exit_codes::AUTH_ERROR)
        }
        ConnError::FrameRejected(server_err) => {
            let mut message = format!("Frame rejected: {}", server_err.message);
            if let Some(body) = &server_err.body {
                message.push_str(&format!(" ({})", body));
            }
            (message, super::exit_codes::FRAME_REJECTED)
        }
        ConnError::Protocol(msg) => (
            format!("Protocol error: {}", msg),
            super::exit_codes::PROTOCOL_ERROR,
        ),
        ConnError::ReceiptTimeout(id) => (
            format!("Receipt timeout: {}", id),
            super::exit_codes::PROTOCOL_ERROR,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iridium_stomp::connection::ServerError;

    #[test]
    fn frame_rejected_uses_distinct_exit_code() {
        let err = ConnError::FrameRejected(ServerError {
            message: "publish denied".to_string(),
            body: Some("not allowed".to_string()),
            receipt_id: Some("receipt-1".to_string()),
            frame: Frame::new("ERROR")
                .header("message", "publish denied")
                .header("receipt-id", "receipt-1")
                .set_body(b"not allowed".to_vec()),
        });

        let (message, code) = format_connection_error_pub(&err, "127.0.0.1:61613");

        assert_eq!(message, "Frame rejected: publish denied (not allowed)");
        assert_eq!(code, super::super::exit_codes::FRAME_REJECTED);
    }
}
