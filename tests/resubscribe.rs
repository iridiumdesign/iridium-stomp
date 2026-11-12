//! SUBSCRIBE and UNSUBSCRIBE across a reconnect (#137). A subscription id
//! belongs to one connection: a broker must be told about each subscription
//! once per connection, and never asked to end one it was not told about.
//! RabbitMQ and Artemis close the connection on a duplicate SUBSCRIBE id, and
//! Artemis on an UNSUBSCRIBE for an id it does not know.

use iridium_stomp::{AckMode, Connection};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// Read frames from `sock` until one whose command is `until` has arrived,
/// and return the commands seen, in order, that one included.
async fn commands_until(sock: &mut TcpStream, until: &str) -> Vec<String> {
    let mut got = Vec::new();
    let mut buf = vec![0u8; 4096];
    loop {
        let mut frames: Vec<&[u8]> = got.split(|&b| b == 0).collect();
        // What follows the last NUL is an incomplete frame, or nothing.
        frames.pop();
        let commands: Vec<String> = frames
            .iter()
            .map(|frame| {
                let text = String::from_utf8_lossy(frame);
                text.trim_start().lines().next().unwrap_or("").to_string()
            })
            .collect();
        if commands.iter().any(|command| command == until) {
            return commands;
        }
        let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("no {until} within 10s; saw {commands:?}"))
            .expect("read");
        assert!(n > 0, "client hung up before {until}; saw {commands:?}");
        got.extend_from_slice(&buf[..n]);
    }
}

async fn connected(sock: &mut TcpStream) {
    commands_until(sock, "CONNECT").await;
    sock.write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0")
        .await
        .expect("write CONNECTED");
}

/// A broker that serves two connections. On the first it waits for
/// `before_drop` frames after CONNECT (none when empty), then drops it and
/// signals `dropped`. On the second it reports every command up to the SEND
/// the test uses as an end marker.
async fn two_connection_broker(
    before_drop: Option<&'static str>,
) -> (
    String,
    oneshot::Receiver<()>,
    tokio::task::JoinHandle<Vec<String>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let broker = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        connected(&mut first).await;
        if let Some(command) = before_drop {
            commands_until(&mut first, command).await;
        }
        drop(first);
        let _ = dropped_tx.send(());
        let (mut second, _) = listener.accept().await.unwrap();
        connected(&mut second).await;
        let mut seen = commands_until(&mut second, "SEND").await;
        seen.pop(); // the marker
        seen
    });
    (addr, dropped_rx, broker)
}

/// Let the client notice the drop, so what follows happens while it is
/// between connections. Its first backoff is two seconds.
async fn while_disconnected(dropped: oneshot::Receiver<()>) {
    dropped.await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn subscribe_while_disconnected_is_sent_once() {
    let (addr, dropped, broker) = two_connection_broker(None).await;
    let conn = Connection::connect(&addr, "u", "p", "0,0").await.unwrap();

    while_disconnected(dropped).await;
    let _sub = conn.subscribe("/queue/a", AckMode::Auto).await.unwrap();
    conn.send("/queue/marker", "end").await.unwrap();

    // Replayed from the registry; the queued frame is not written as well.
    assert_eq!(broker.await.unwrap(), ["SUBSCRIBE"]);
}

#[tokio::test]
async fn unsubscribe_while_disconnected_sends_nothing() {
    let (addr, dropped, broker) = two_connection_broker(Some("SUBSCRIBE")).await;
    let conn = Connection::connect(&addr, "u", "p", "0,0").await.unwrap();
    let sub = conn.subscribe("/queue/a", AckMode::Auto).await.unwrap();

    while_disconnected(dropped).await;
    sub.unsubscribe().await.unwrap();
    conn.send("/queue/marker", "end").await.unwrap();

    // Not replayed, and the new session is not asked to end a subscription
    // it never had.
    assert_eq!(broker.await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn dropping_a_subscription_while_disconnected_sends_nothing() {
    let (addr, dropped, broker) = two_connection_broker(Some("SUBSCRIBE")).await;
    let conn = Connection::connect(&addr, "u", "p", "0,0").await.unwrap();
    let sub = conn.subscribe("/queue/a", AckMode::Auto).await.unwrap();

    while_disconnected(dropped).await;
    drop(sub);
    conn.send("/queue/marker", "end").await.unwrap();

    assert_eq!(broker.await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn subscribe_then_unsubscribe_while_disconnected_sends_nothing() {
    let (addr, dropped, broker) = two_connection_broker(None).await;
    let conn = Connection::connect(&addr, "u", "p", "0,0").await.unwrap();

    while_disconnected(dropped).await;
    let sub = conn.subscribe("/queue/a", AckMode::Auto).await.unwrap();
    sub.unsubscribe().await.unwrap();
    conn.send("/queue/marker", "end").await.unwrap();

    assert_eq!(broker.await.unwrap(), Vec::<String>::new());
}

/// A subscription that outlives a reconnect is replayed once, and the new
/// session, which has it, is told when it ends.
#[tokio::test]
async fn subscription_replayed_after_reconnect_can_be_unsubscribed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (replayed_tx, replayed_rx) = oneshot::channel();
    let broker = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        connected(&mut first).await;
        commands_until(&mut first, "SUBSCRIBE").await;
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        connected(&mut second).await;
        let mut seen = commands_until(&mut second, "SUBSCRIBE").await;
        let _ = replayed_tx.send(());
        seen.extend(commands_until(&mut second, "UNSUBSCRIBE").await);
        seen
    });

    let conn = Connection::connect(&addr, "u", "p", "0,0").await.unwrap();
    let sub = conn.subscribe("/queue/a", AckMode::Auto).await.unwrap();
    replayed_rx.await.unwrap();
    sub.unsubscribe().await.unwrap();

    assert_eq!(broker.await.unwrap(), ["SUBSCRIBE", "UNSUBSCRIBE"]);
}
