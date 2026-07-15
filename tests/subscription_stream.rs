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
    let opts = SubscriptionOptions {
        headers: vec![
            (
                "activemq.subscriptionName".to_string(),
                "my-durable-sub".to_string(),
            ),
            ("selector".to_string(), "priority > 5".to_string()),
        ],
    };
    assert_eq!(opts.headers.len(), 2);
    assert_eq!(opts.headers[0].0, "activemq.subscriptionName");
    assert_eq!(opts.headers[1].0, "selector");
}

#[test]
fn subscription_options_clone() {
    let original = SubscriptionOptions {
        headers: vec![("key".to_string(), "value".to_string())],
    };
    let cloned = original.clone();

    assert_eq!(original.headers, cloned.headers);
}

#[test]
fn subscription_options_debug() {
    let opts = SubscriptionOptions {
        headers: vec![("test".to_string(), "value".to_string())],
    };
    let debug_str = format!("{:?}", opts);
    assert!(debug_str.contains("SubscriptionOptions"));
    assert!(debug_str.contains("test"));
    assert!(debug_str.contains("value"));
}

#[test]
fn subscription_options_full_config() {
    let opts = SubscriptionOptions {
        headers: vec![
            (
                "activemq.subscriptionName".to_string(),
                "durable-sub-1".to_string(),
            ),
            ("activemq.noLocal".to_string(), "true".to_string()),
            ("selector".to_string(), "type = 'important'".to_string()),
        ],
    };

    assert_eq!(opts.headers.len(), 3);
}

// =============================================================================
// SubscriptionOptions Edge Cases
// =============================================================================

#[test]
fn subscription_options_empty_header_values() {
    let opts = SubscriptionOptions {
        headers: vec![
            ("empty-value".to_string(), "".to_string()),
            ("".to_string(), "empty-key".to_string()),
        ],
    };
    assert_eq!(opts.headers[0].1, "");
    assert_eq!(opts.headers[1].0, "");
}

#[test]
fn subscription_options_special_characters() {
    let opts = SubscriptionOptions {
        headers: vec![(
            "selector".to_string(),
            "id > 100 AND type = 'test'".to_string(),
        )],
    };
    assert!(opts.headers[0].1.contains("'test'"));
}
