pub mod args;
pub mod commands;
pub mod oneshot;
pub mod plain;
pub mod state;
pub mod tui;

/// Exit codes for different error conditions.
pub mod exit_codes {
    /// Successful execution
    pub const SUCCESS: u8 = 0;
    /// Network/connection error (e.g., host unreachable, connection refused)
    pub const NETWORK_ERROR: u8 = 1;
    /// Authentication error (e.g., invalid credentials)
    pub const AUTH_ERROR: u8 = 2;
    /// Protocol error (e.g., unexpected server response)
    pub const PROTOCOL_ERROR: u8 = 3;
    /// A frame requesting a receipt was rejected by the broker via an ERROR frame.
    pub const FRAME_REJECTED: u8 = 4;
}
