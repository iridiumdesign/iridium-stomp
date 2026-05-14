use chrono::{DateTime, Local};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

/// Maximum number of messages to keep in the ring buffer for display
pub const MAX_MESSAGES: usize = 1000;

/// Maximum number of errors to keep in the ring buffer for display
pub const MAX_ERRORS: usize = 100;

/// Statistics for a single subscription destination
#[derive(Debug, Clone, Default)]
pub struct SubStats {
    /// Number of messages received on this destination
    pub message_count: u64,
}

/// A message to display in the TUI
#[derive(Debug, Clone)]
pub struct DisplayMessage {
    /// Timestamp when the message was received
    pub timestamp: DateTime<Local>,
    /// Destination the message was received from
    pub destination: String,
    /// Message body as string (or description for binary)
    pub body: String,
    /// Headers from the message
    pub headers: Vec<(String, String)>,
}

/// Application state shared across all tasks
pub struct AppState {
    /// Session start time
    pub start_time: DateTime<Local>,

    /// Connection info
    pub host: String,
    pub user: String,
    pub heartbeat_interval_ms: u32,

    /// Subscriptions: destination -> stats
    pub subscriptions: HashMap<String, SubStats>,

    /// Heartbeat tracking
    pub heartbeat_count: u64,
    pub last_heartbeat: Option<Instant>,

    /// Other counters
    pub sent_count: u64,
    pub error_count: u64,
    pub warning_count: u64,
    pub info_count: u64,

    /// Messages (ring buffer for display)
    pub messages: VecDeque<DisplayMessage>,

    /// Broker errors (separate ring buffer for error pane)
    pub errors: VecDeque<DisplayMessage>,

    /// UI state
    pub show_headers: bool,
    pub scroll_offset: usize,
    pub error_scroll_offset: usize,

    /// Current input buffer
    pub input: String,
    /// Cursor position in input
    pub cursor_pos: usize,

    /// Command history
    pub command_history: Vec<String>,
    /// Current position in history (None = not browsing)
    pub history_index: Option<usize>,
    /// Saved input when browsing history
    pub saved_input: String,
}

impl AppState {
    /// Create a new AppState with the given connection info
    pub fn new(host: String, user: String, heartbeat_interval_ms: u32) -> Self {
        Self {
            start_time: Local::now(),
            host,
            user,
            heartbeat_interval_ms,
            subscriptions: HashMap::new(),
            heartbeat_count: 0,
            last_heartbeat: None,
            sent_count: 0,
            error_count: 0,
            warning_count: 0,
            info_count: 0,
            messages: VecDeque::with_capacity(MAX_MESSAGES),
            errors: VecDeque::with_capacity(MAX_ERRORS),
            show_headers: false,
            scroll_offset: 0,
            error_scroll_offset: 0,
            input: String::new(),
            cursor_pos: 0,
            command_history: Vec::new(),
            history_index: None,
            saved_input: String::new(),
        }
    }

    /// Record a heartbeat
    pub fn record_heartbeat(&mut self) {
        self.heartbeat_count += 1;
        self.last_heartbeat = Some(Instant::now());
    }

    /// Get the heartbeat indicator character and whether it's "pulsing"
    /// Returns (indicator, is_pulsing)
    pub fn heartbeat_indicator(&self) -> (&'static str, bool) {
        match self.last_heartbeat {
            Some(last) => {
                let elapsed = last.elapsed().as_millis() as u32;
                // Pulse for 1 second after heartbeat
                if elapsed < 1000 {
                    ("✦", true) // Four pointed star - just received
                } else if elapsed < self.heartbeat_interval_ms * 2 {
                    ("◇", false) // Diamond outline - healthy
                } else {
                    ("!", false) // Warning - heartbeat late
                }
            }
            None => ("○", false), // Empty circle - no heartbeat yet
        }
    }

    /// Record a received message
    pub fn record_message(
        &mut self,
        destination: &str,
        body: String,
        headers: Vec<(String, String)>,
    ) {
        // Update counters based on message type
        match destination {
            "SENT" => self.sent_count += 1,
            "ERROR" => self.error_count += 1,
            "BROKER ERROR" => {
                // Broker errors go to the dedicated error pane
                self.record_error(body, headers);
                return;
            }
            "WARN" => self.warning_count += 1,
            "INFO" => self.info_count += 1,
            _ => {
                // Update subscription stats for actual destinations
                let stats = self
                    .subscriptions
                    .entry(destination.to_string())
                    .or_default();
                stats.message_count += 1;
            }
        }

        // Add to message buffer
        let msg = DisplayMessage {
            timestamp: Local::now(),
            destination: destination.to_string(),
            body,
            headers,
        };

        self.messages.push_back(msg);

        // Trim to max size
        while self.messages.len() > MAX_MESSAGES {
            self.messages.pop_front();
        }
    }

    /// Record a broker error (displayed in separate error pane)
    pub fn record_error(&mut self, body: String, headers: Vec<(String, String)>) {
        self.error_count += 1;

        let msg = DisplayMessage {
            timestamp: Local::now(),
            destination: "BROKER ERROR".to_string(),
            body,
            headers,
        };

        self.errors.push_back(msg);

        // Trim to max size
        while self.errors.len() > MAX_ERRORS {
            self.errors.pop_front();
        }
    }

    /// Register a subscription destination
    pub fn register_subscription(&mut self, destination: &str) {
        self.subscriptions
            .entry(destination.to_string())
            .or_default();
    }

    /// Get total message count across all subscriptions
    pub fn total_message_count(&self) -> u64 {
        self.subscriptions.values().map(|s| s.message_count).sum()
    }

    /// Get session duration as a formatted string
    pub fn session_duration(&self) -> String {
        let duration = Local::now().signed_duration_since(self.start_time);
        let total_secs = duration.num_seconds();
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        format!("{:02}:{:02}:{:02}", hours, mins, secs)
    }

    /// Toggle header display
    pub fn toggle_headers(&mut self) {
        self.show_headers = !self.show_headers;
    }

    /// Clear message history
    pub fn clear_messages(&mut self) {
        self.messages.clear();
        self.scroll_offset = 0;
    }

    /// Add a command to history
    pub fn add_to_history(&mut self, cmd: &str) {
        let cmd = cmd.trim();
        if !cmd.is_empty() {
            // Don't add duplicates of the last command
            if self.command_history.last().map(|s| s.as_str()) != Some(cmd) {
                self.command_history.push(cmd.to_string());
            }
        }
        self.history_index = None;
        self.saved_input.clear();
    }

    /// Navigate to previous command in history
    pub fn history_prev(&mut self) {
        if self.command_history.is_empty() {
            return;
        }

        match self.history_index {
            None => {
                // Starting to browse - save current input
                self.saved_input = self.input.clone();
                self.history_index = Some(self.command_history.len() - 1);
            }
            Some(idx) if idx > 0 => {
                self.history_index = Some(idx - 1);
            }
            _ => return, // Already at oldest
        }

        if let Some(idx) = self.history_index {
            self.input = self.command_history[idx].clone();
            self.cursor_pos = self.input.len();
        }
    }

    /// Navigate to next command in history
    pub fn history_next(&mut self) {
        match self.history_index {
            None => {} // Not browsing history
            Some(idx) => {
                if idx + 1 < self.command_history.len() {
                    self.history_index = Some(idx + 1);
                    self.input = self.command_history[idx + 1].clone();
                    self.cursor_pos = self.input.len();
                } else {
                    // Return to saved input
                    self.history_index = None;
                    self.input = self.saved_input.clone();
                    self.cursor_pos = self.input.len();
                }
            }
        }
    }

    /// Generate session summary text
    pub fn generate_summary(&self) -> String {
        self.generate_summary_with_options(false, 80)
    }

    /// Generate session report with optional message history
    pub fn generate_summary_with_options(
        &self,
        include_messages: bool,
        max_width: usize,
    ) -> String {
        let end_time = Local::now();
        let duration = end_time.signed_duration_since(self.start_time);
        let total_secs = duration.num_seconds();
        let mins = total_secs / 60;
        let secs = total_secs % 60;

        let mut lines = Vec::new();
        lines.push(
            "═══════════════════════════════════════════════════════════════════════════════"
                .to_string(),
        );
        lines.push("  iridium-stomp Session Report".to_string());
        lines.push(
            "═══════════════════════════════════════════════════════════════════════════════"
                .to_string(),
        );
        lines.push(format!("  Host:       {}", self.host));
        lines.push(format!("  User:       {}", self.user));
        lines.push(format!(
            "  Started:    {}",
            self.start_time.format("%Y-%m-%d %H:%M:%S")
        ));
        lines.push(format!(
            "  Ended:      {}",
            end_time.format("%Y-%m-%d %H:%M:%S")
        ));
        lines.push(format!("  Duration:   {}m {}s", mins, secs));
        lines.push(String::new());
        lines.push("  Subscriptions:".to_string());

        // Sort destinations by message count (descending)
        let mut subs: Vec<_> = self.subscriptions.iter().collect();
        subs.sort_by_key(|b| std::cmp::Reverse(b.1.message_count));

        let max_dest_len = subs
            .iter()
            .map(|(d, _)| d.len())
            .max()
            .unwrap_or(20)
            .min(40);
        for (dest, stats) in &subs {
            let dest_display = truncate_str(dest, max_dest_len);
            lines.push(format!(
                "    {:width$} {:>6}",
                dest_display,
                stats.message_count,
                width = max_dest_len
            ));
        }
        lines.push(format!("    {:─>width$}", "", width = max_dest_len + 7));
        lines.push(format!(
            "    {:width$} {:>6}",
            "Total",
            self.total_message_count(),
            width = max_dest_len
        ));
        lines.push(String::new());
        lines.push(format!("  Heartbeats received: {}", self.heartbeat_count));

        if include_messages && !self.messages.is_empty() {
            lines.push(String::new());
            lines.push(
                "───────────────────────────────────────────────────────────────────────────────"
                    .to_string(),
            );
            lines.push("  Message History".to_string());
            lines.push(
                "───────────────────────────────────────────────────────────────────────────────"
                    .to_string(),
            );

            for msg in &self.messages {
                let time = msg.timestamp.format("%H:%M:%S").to_string();
                let prefix = format!("  {} [{}] ", time, msg.destination);
                let body_width = max_width.saturating_sub(prefix.len());
                let body = truncate_str(&msg.body, body_width);
                lines.push(format!("{}{}", prefix, body));
            }
        }

        lines.push(
            "═══════════════════════════════════════════════════════════════════════════════"
                .to_string(),
        );

        lines.join("\n")
    }
}

/// Truncate a string to max_len characters, adding "..." if truncated
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else if max_len <= 3 {
        ".".repeat(max_len)
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

/// Thread-safe shared state
pub type SharedState = Arc<Mutex<AppState>>;

/// Create a new shared state
pub fn new_shared_state(host: String, user: String, heartbeat_interval_ms: u32) -> SharedState {
    Arc::new(Mutex::new(AppState::new(host, user, heartbeat_interval_ms)))
}
