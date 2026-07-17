use clap::Parser;

#[derive(Parser)]
#[command(name = "stomp")]
#[command(version)]
#[command(about = "Interactive STOMP client CLI")]
pub struct Cli {
    /// STOMP broker address (host:port)
    #[arg(short, long, default_value = "127.0.0.1:61613")]
    pub address: String,

    /// Login username
    #[arg(short, long, default_value = "guest")]
    pub login: String,

    /// Passcode
    #[arg(short, long, default_value = "guest")]
    pub passcode: String,

    /// Heartbeat settings (client-send,client-receive in ms)
    #[arg(long, default_value = "10000,10000")]
    pub heartbeat: String,

    /// Destinations to subscribe to (can be specified multiple times)
    #[arg(short, long)]
    pub subscribe: Vec<String>,

    /// Enable TUI mode with panels and live updates
    #[arg(long)]
    pub tui: bool,

    /// Show session summary on exit
    #[arg(long)]
    pub summary: bool,

    /// Send one message, then exit without starting the interactive client.
    ///
    /// The message is sent with a receipt request, so the exit code reflects
    /// whether the broker accepted it: 0 confirmed, 4 rejected, 3 no answer
    /// within --timeout.
    #[arg(
        long,
        num_args = 2,
        value_names = ["DESTINATION", "BODY"],
        conflicts_with_all = ["tui", "subscribe", "summary"],
    )]
    pub send: Option<Vec<String>>,

    /// Seconds to wait for the broker connection before giving up. With --send
    /// it also bounds the receipt wait. Without this bound an unreachable broker
    /// would hang the client indefinitely; raise it if you expect to wait for a
    /// broker that is still coming up.
    #[arg(long, default_value_t = 5, value_name = "SECONDS")]
    pub timeout: u64,
}
