//! Unit tests for Subscription and SubscriptionOptions.
//!
//! Note: Testing the full Subscription struct requires creating a Connection,
//! which is tested in the connection module's inline tests. This file focuses
//! on testing SubscriptionOptions and the public interface aspects.

use iridium_stomp::SubscriptionOptions;

// =============================================================================
// SubscriptionOptions Tests
// =============================================================================

#[test]
fn subscription_options_default() {
    let opts = SubscriptionOptions::default();
    assert!(opts.headers.is_empty());
}

#[test]
fn subscription_options_with_headers() {
    let opts = SubscriptionOptions::new()
        .header("activemq.subscriptionName", "my-durable-sub")
        .header("selector", "priority > 5");
    assert_eq!(opts.headers.len(), 2);
    assert_eq!(opts.headers[0].0, "activemq.subscriptionName");
    assert_eq!(opts.headers[1].0, "selector");
}

#[test]
fn subscription_options_clone() {
    let original = SubscriptionOptions::new().header("key", "value");
    let cloned = original.clone();

    assert_eq!(original.headers, cloned.headers);
}

#[test]
fn subscription_options_debug() {
    let opts = SubscriptionOptions::new().header("test", "value");
    let debug_str = format!("{:?}", opts);
    assert!(debug_str.contains("SubscriptionOptions"));
    assert!(debug_str.contains("test"));
    assert!(debug_str.contains("value"));
}

#[test]
fn subscription_options_full_config() {
    let opts = SubscriptionOptions::new()
        .header("activemq.subscriptionName", "durable-sub-1")
        .header("activemq.noLocal", "true")
        .header("selector", "type = 'important'");

    assert_eq!(opts.headers.len(), 3);
}

// =============================================================================
// SubscriptionOptions Edge Cases
// =============================================================================

#[test]
fn subscription_options_empty_header_values() {
    let opts = SubscriptionOptions::new()
        .header("empty-value", "")
        .header("", "empty-key");
    assert_eq!(opts.headers[0].1, "");
    assert_eq!(opts.headers[1].0, "");
}

#[test]
fn subscription_options_special_characters() {
    let opts = SubscriptionOptions::new().header("selector", "id > 100 AND type = 'test'");
    assert!(opts.headers[0].1.contains("'test'"));
}
