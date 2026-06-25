use iridium_stomp::{Connection, Frame};
use std::io::Write;
use tokio::sync::mpsc;

use super::state::SharedState;

/// Result of executing a command
pub enum CommandResult {
    /// Command executed successfully
    Ok,
    /// Command requests exit
    Quit,
    /// Informational message (not an error)
    Info(String),
    /// Error executing command
    Error(String),
}

/// Parse and execute a command
pub async fn execute_command(
    line: &str,
    conn: &Connection,
    state: SharedState,
    sub_tx: &mpsc::Sender<String>,
    tui_mode: bool,
) -> CommandResult {
    let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
    if parts.is_empty() || parts[0].is_empty() {
        return CommandResult::Ok;
    }

    match parts[0] {
        "quit" | "exit" | "q" => CommandResult::Quit,

        "send" => {
            if parts.len() < 3 {
                return CommandResult::Error("Usage: send <destination> <message>".to_string());
            }
            let dest = parts[1];
            let msg = parts[2];

            // Validate destination format
            if !dest.starts_with('/') {
                return CommandResult::Error(format!(
                    "Invalid destination '{}'. Must start with / (e.g., /topic/test, /queue/test)",
                    dest
                ));
            }

            // Check if destination is known (subscribed) or matches common patterns
            let is_known = {
                let state = state.lock().await;
                state.subscriptions.contains_key(dest)
            };
            let is_common_pattern = dest.starts_with("/topic/")
                || dest.starts_with("/queue/")
                || dest.starts_with("/amq/")
                || dest.starts_with("/exchange/");

            let warning = if !is_known && !is_common_pattern {
                Some(format!(
                    "Warning: '{}' doesn't match common patterns (/topic/, /queue/, /amq/, /exchange/)",
                    dest
                ))
            } else {
                None
            };

            let frame = Frame::new("SEND")
                .header("destination", dest)
                .header("content-type", "text/plain")
                .set_body(msg.as_bytes().to_vec());
            match conn.send_frame(frame).await {
                Ok(_) => {
                    if tui_mode {
                        let mut state = state.lock().await;
                        if let Some(warn) = warning {
                            state.record_message("WARN", warn, vec![]);
                        }
                        state.record_message("SENT", format!("[{}] {}", dest, msg), vec![]);
                    } else {
                        if let Some(warn) = warning {
                            eprintln!("{}", warn);
                        }
                        println!("Sent to {}", dest);
                    }
                    CommandResult::Ok
                }
                Err(e) => CommandResult::Error(format!("Send error: {}", e)),
            }
        }

        "sub" | "subscribe" => {
            if parts.len() < 2 {
                return CommandResult::Error("Usage: sub <destination>".to_string());
            }
            let dest = parts[1];

            // Validate destination format
            if !dest.starts_with('/') {
                return CommandResult::Error(format!(
                    "Invalid destination '{}'. Must start with / (e.g., /topic/test, /queue/test)",
                    dest
                ));
            }

            // Warn for unusual patterns
            let is_common_pattern = dest.starts_with("/topic/")
                || dest.starts_with("/queue/")
                || dest.starts_with("/amq/")
                || dest.starts_with("/exchange/");

            if !is_common_pattern {
                let warn = format!(
                    "Warning: '{}' doesn't match common patterns (/topic/, /queue/, /amq/, /exchange/)",
                    dest
                );
                if tui_mode {
                    let mut state = state.lock().await;
                    state.record_message("WARN", warn, vec![]);
                } else {
                    eprintln!("{}", warn);
                }
            }

            // Send subscription request to the subscription manager
            if sub_tx.send(dest.to_string()).await.is_err() {
                return CommandResult::Error("Failed to request subscription".to_string());
            }
            // In TUI mode, the subscription task will report success/failure
            // In plain mode, subscribe_destination prints status
            CommandResult::Ok
        }

        "about" => {
            if tui_mode {
                return CommandResult::Info(format!(
                    "iridium-stomp v{} - © 2025 Brad Siegfreid - MIT License",
                    env!("CARGO_PKG_VERSION")
                ));
            }
            print_about();
            CommandResult::Ok
        }

        "summary" => {
            if parts.len() >= 2 {
                // Write to file
                let filename = parts[1];
                let state = state.lock().await;
                match std::fs::File::create(filename) {
                    Ok(mut file) => {
                        if let Err(e) = writeln!(file, "{}", state.generate_summary()) {
                            return CommandResult::Error(format!("Failed to write summary: {}", e));
                        }
                        if tui_mode {
                            return CommandResult::Info(format!("Summary written to {}", filename));
                        }
                        println!("Summary written to {}", filename);
                    }
                    Err(e) => {
                        return CommandResult::Error(format!("Failed to create file: {}", e));
                    }
                }
            } else if tui_mode {
                return CommandResult::Error("Usage: summary <filename>".to_string());
            } else {
                // Print to stdout
                let state = state.lock().await;
                println!("{}", state.generate_summary());
            }
            CommandResult::Ok
        }

        "report" => {
            if parts.len() >= 2 {
                // Write to file
                let filename = parts[1];
                let state = state.lock().await;
                match std::fs::File::create(filename) {
                    Ok(mut file) => {
                        if let Err(e) =
                            writeln!(file, "{}", state.generate_summary_with_options(true, 80))
                        {
                            return CommandResult::Error(format!("Failed to write report: {}", e));
                        }
                        if tui_mode {
                            return CommandResult::Info(format!("Report written to {}", filename));
                        }
                        println!("Report written to {}", filename);
                    }
                    Err(e) => {
                        return CommandResult::Error(format!("Failed to create file: {}", e));
                    }
                }
            } else if tui_mode {
                return CommandResult::Error("Usage: report <filename>".to_string());
            } else {
                // Print to stdout
                let state = state.lock().await;
                println!("{}", state.generate_summary_with_options(true, 80));
            }
            CommandResult::Ok
        }

        "clear" => {
            let mut state = state.lock().await;
            state.clear_messages();
            CommandResult::Ok
        }

        "help" | "?" => {
            if tui_mode {
                return CommandResult::Info(
                    "Commands: send, sub, summary <file>, report <file>, clear, quit".to_string(),
                );
            }
            print_help();
            CommandResult::Ok
        }

        _ => CommandResult::Error(format!(
            "Unknown command: {}. Type 'help' for commands.",
            parts[0]
        )),
    }
}

/// Print help text
pub fn print_help() {
    println!("Commands:");
    println!("  send <destination> <message>  - Send a message");
    println!("  sub <destination>             - Subscribe to a destination");
    println!("  about                         - Show copyright and license");
    println!("  summary [file]                - Print session summary (or save to file)");
    println!(
        "  report [file]                 - Full report with message history (or save to file)"
    );
    println!("  clear                         - Clear message history");
    println!("  quit                          - Exit");
}

/// Print about/copyright information
pub fn print_about() {
    println!();
    println!("iridium-stomp v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("© 2025 Brad Siegfreid");
    println!();
    println!("Licensed under the MIT License.");
    println!("See https://github.com/iridiumdesign/iridium-stomp for more information.");
    println!();
}
