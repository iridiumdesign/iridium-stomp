//! Tests for ConnectOptions and connect_with_options (Issue #34)
//!
//! These tests verify:
//! - ConnectOptions builder methods
//! - Default values
//! - Custom headers

use iridium_stomp::ConnectOptions;

// ============================================================================
// ConnectOptions builder tests
// ============================================================================

#[test]
fn connect_options_default() {
    let opts = ConnectOptions::default();
    assert!(opts.accept_version.is_none());
    assert!(opts.client_id.is_none());
    assert!(opts.host.is_none());
    assert!(opts.headers.is_empty());
}

#[test]
fn connect_options_new() {
    let opts = ConnectOptions::new();
    assert!(opts.accept_version.is_none());
    assert!(opts.client_id.is_none());
    assert!(opts.host.is_none());
    assert!(opts.headers.is_empty());
}

#[test]
fn connect_options_accept_version() {
    let opts = ConnectOptions::default().accept_version("1.0,1.1,1.2");
    assert_eq!(opts.accept_version, Some("1.0,1.1,1.2".to_string()));
}

#[test]
fn connect_options_accept_version_single() {
    let opts = ConnectOptions::default().accept_version("1.1");
    assert_eq!(opts.accept_version, Some("1.1".to_string()));
}

#[test]
fn connect_options_client_id() {
    let opts = ConnectOptions::default().client_id("my-durable-client");
    assert_eq!(opts.client_id, Some("my-durable-client".to_string()));
}

#[test]
fn connect_options_client_id_with_string() {
    let id = String::from("client-123");
    let opts = ConnectOptions::default().client_id(id);
    assert_eq!(opts.client_id, Some("client-123".to_string()));
}

#[test]
fn connect_options_host() {
    let opts = ConnectOptions::default().host("my-vhost");
    assert_eq!(opts.host, Some("my-vhost".to_string()));
}

#[test]
fn connect_options_host_with_slash() {
    let opts = ConnectOptions::default().host("/production");
    assert_eq!(opts.host, Some("/production".to_string()));
}

#[test]
fn connect_options_header_single() {
    let opts = ConnectOptions::default().header("custom-key", "custom-value");
    assert_eq!(opts.headers.len(), 1);
    assert_eq!(
        opts.headers[0],
        ("custom-key".to_string(), "custom-value".to_string())
    );
}

#[test]
fn connect_options_header_multiple() {
    let opts = ConnectOptions::default()
        .header("key1", "value1")
        .header("key2", "value2")
        .header("key3", "value3");
    assert_eq!(opts.headers.len(), 3);
    assert_eq!(opts.headers[0], ("key1".to_string(), "value1".to_string()));
    assert_eq!(opts.headers[1], ("key2".to_string(), "value2".to_string()));
    assert_eq!(opts.headers[2], ("key3".to_string(), "value3".to_string()));
}

#[test]
fn connect_options_builder_chain() {
    let opts = ConnectOptions::default()
        .accept_version("1.2")
        .client_id("test-client")
        .host("test-vhost")
        .header("x-custom", "value");

    assert_eq!(opts.accept_version, Some("1.2".to_string()));
    assert_eq!(opts.client_id, Some("test-client".to_string()));
    assert_eq!(opts.host, Some("test-vhost".to_string()));
    assert_eq!(opts.headers.len(), 1);
    assert_eq!(
        opts.headers[0],
        ("x-custom".to_string(), "value".to_string())
    );
}

#[test]
fn connect_options_clone() {
    let opts1 = ConnectOptions::default()
        .client_id("original")
        .host("vhost1");
    let opts2 = opts1.clone();

    assert_eq!(opts1.client_id, opts2.client_id);
    assert_eq!(opts1.host, opts2.host);
}

#[test]
fn connect_options_debug() {
    let opts = ConnectOptions::default().client_id("test");
    let debug = format!("{:?}", opts);
    assert!(debug.contains("ConnectOptions"));
    assert!(debug.contains("test"));
}

// ============================================================================
// ConnectOptions with special values
// ============================================================================

#[test]
fn connect_options_empty_client_id() {
    // Empty values are accepted by the builder - validation is deferred to the broker.
    // This design allows maximum flexibility; invalid values will be rejected at
    // connection time by the STOMP broker.
    let opts = ConnectOptions::default().client_id("");
    assert_eq!(opts.client_id, Some(String::new()));
}

#[test]
fn connect_options_empty_host() {
    // Empty host accepted by builder; broker will reject if invalid
    let opts = ConnectOptions::default().host("");
    assert_eq!(opts.host, Some(String::new()));
}

#[test]
fn connect_options_header_empty_value() {
    // Empty header values are valid per STOMP spec
    let opts = ConnectOptions::default().header("key", "");
    assert_eq!(opts.headers[0], ("key".to_string(), String::new()));
}

#[test]
fn connect_options_header_empty_key() {
    // Empty keys are accepted by the builder but would be invalid STOMP.
    // Validation is deferred to the broker for simplicity.
    let opts = ConnectOptions::default().header("", "value");
    assert_eq!(opts.headers[0], (String::new(), "value".to_string()));
}

#[test]
fn connect_options_activemq_durable() {
    // Typical ActiveMQ durable subscription setup
    let opts = ConnectOptions::default()
        .client_id("my-app-subscriber-1")
        .header("activemq.prefetchSize", "1");

    assert_eq!(opts.client_id, Some("my-app-subscriber-1".to_string()));
    assert_eq!(opts.headers.len(), 1);
    assert_eq!(
        opts.headers[0],
        ("activemq.prefetchSize".to_string(), "1".to_string())
    );
}

#[test]
fn connect_options_rabbitmq_vhost() {
    // RabbitMQ virtual host example
    let opts = ConnectOptions::default().host("/production");

    assert_eq!(opts.host, Some("/production".to_string()));
}

#[test]
fn connect_options_version_negotiation_fallback() {
    // Client willing to accept multiple versions for compatibility
    let opts = ConnectOptions::default().accept_version("1.0,1.1,1.2");

    assert_eq!(opts.accept_version, Some("1.0,1.1,1.2".to_string()));
}

#[test]
fn connect_options_multiple_custom_headers() {
    // Some brokers accept multiple custom headers
    let opts = ConnectOptions::default()
        .header("x-request-id", "uuid-12345")
        .header("x-correlation-id", "corr-67890")
        .header("x-tenant", "tenant-abc");

    assert_eq!(opts.headers.len(), 3);
}

// ============================================================================
// Heartbeat notification tests
// ============================================================================

#[test]
fn connect_options_heartbeat_notify_default_none() {
    let opts = ConnectOptions::default();
    assert!(opts.heartbeat_tx.is_none());
}

#[test]
fn connect_options_heartbeat_notify_sets_channel() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<()>(16);
    let opts = ConnectOptions::default().heartbeat_notify(tx);
    assert!(opts.heartbeat_tx.is_some());
}

#[test]
fn connect_options_heartbeat_notify_chainable() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<()>(16);
    let opts = ConnectOptions::default()
        .client_id("test-client")
        .heartbeat_notify(tx)
        .host("localhost");

    assert!(opts.heartbeat_tx.is_some());
    assert_eq!(opts.client_id, Some("test-client".to_string()));
    assert_eq!(opts.host, Some("localhost".to_string()));
}

// ============================================================================
// Documentation: Critical header protection
// ============================================================================
//
// Note: Custom headers that would override critical STOMP CONNECT headers
// (accept-version, host, login, passcode, heart-beat, client-id) are silently
// ignored at connection time. This is enforced in Connection::connect_with_options(),
// not in ConnectOptions itself. The builder accepts any header but the connection
// logic filters them.
//
// This design allows ConnectOptions to be a simple data container while the
// Connection enforces protocol safety.
