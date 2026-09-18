//! Coverage for #119: an ACK or NACK carries the MESSAGE's `ack` header in its
//! `id` (STOMP 1.2), falling back to `message-id` when the broker sent no `ack`
//! header (STOMP 1.0/1.1), whichever of the two the caller names.

use futures::StreamExt;
use iridium_stomp::{AckMode, ConnError, Connection, Frame};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// Raw frames the broker saw, in order.
type Seen = Arc<Mutex<Vec<String>>>;

/// A broker that answers CONNECT, records every frame it receives, and answers
/// each SUBSCRIBE with `on_subscribe`, in which `{sub}` stands for the
/// subscription id.
fn start_broker(on_subscribe: &'static str) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();

    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut buf = [0u8; 4096];

        if stream.read(&mut buf).is_ok() {
            let _ = stream.write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0");
            let _ = stream.flush();
        }

        // TCP is a byte stream: accumulate and act only on complete,
        // NUL-terminated frames.
        let mut acc: Vec<u8> = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => acc.extend_from_slice(&buf[..n]),
            }
            while let Some(pos) = acc.iter().position(|&b| b == 0) {
                let frame: Vec<u8> = acc.drain(..=pos).collect();
                let text = String::from_utf8_lossy(&frame[..pos]);
                let raw = text.trim_start().to_string();
                if raw.is_empty() {
                    continue;
                }
                if raw.starts_with("SUBSCRIBE") {
                    let id = header(&raw, "id").unwrap_or_default();
                    let reply = on_subscribe.replace("{sub}", &id);
                    let _ = stream.write_all(reply.as_bytes());
                    let _ = stream.flush();
                }
                seen_clone.lock().unwrap().push(raw);
            }
        }
    });

    (addr, seen)
}

/// Value of `name` in a raw frame's header block.
fn header(raw: &str, name: &str) -> Option<String> {
    raw.lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            (k == name).then(|| v.to_string())
        })
}

/// Wait until the broker has seen `count` frames of `command`, and return the
/// `id` header of each.
async fn ids_of(seen: &Seen, command: &str, count: usize) -> Vec<String> {
    for _ in 0..100 {
        let ids: Vec<String> = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f.lines().next() == Some(command))
            .map(|f| header(f, "id").unwrap_or_default())
            .collect();
        if ids.len() >= count {
            return ids;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the broker never saw {count} {command} frame(s)");
}

async fn connect(addr: &str) -> Connection {
    Connection::connect(addr, "guest", "guest", "0,0")
        .await
        .unwrap()
}

async fn next(sub: &mut iridium_stomp::Subscription) -> Frame {
    tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("no MESSAGE arrived")
        .expect("subscription ended")
}

/// Three messages the way ActiveMQ Classic sends them: `ack` and `message-id`
/// differ.
const DIFFERING: &str = "\
MESSAGE\nsubscription:{sub}\nmessage-id:M1\nack:A1\ndestination:/queue/t\n\n1\0\
MESSAGE\nsubscription:{sub}\nmessage-id:M2\nack:A2\ndestination:/queue/t\n\n2\0\
MESSAGE\nsubscription:{sub}\nmessage-id:M3\nack:A3\ndestination:/queue/t\n\n3\0";

#[tokio::test]
async fn ack_sends_the_ack_header_whichever_id_the_caller_gives() {
    let (addr, seen) = start_broker(DIFFERING);
    let conn = connect(&addr).await;
    let mut sub = conn
        .subscribe("/queue/t", AckMode::ClientIndividual)
        .await
        .unwrap();

    let first = next(&mut sub).await;
    let second = next(&mut sub).await;
    let third = next(&mut sub).await;

    // By message-id, as every example used to; by the `ack` header, which was
    // the workaround; and by frame.
    sub.ack(first.get_header("message-id").unwrap())
        .await
        .unwrap();
    sub.ack(second.get_header("ack").unwrap()).await.unwrap();
    sub.ack_frame(&third).await.unwrap();

    assert_eq!(ids_of(&seen, "ACK", 3).await, ["A1", "A2", "A3"]);
}

#[tokio::test]
async fn nack_sends_the_ack_header_whichever_id_the_caller_gives() {
    let (addr, seen) = start_broker(DIFFERING);
    let conn = connect(&addr).await;
    let mut sub = conn
        .subscribe("/queue/t", AckMode::ClientIndividual)
        .await
        .unwrap();

    let first = next(&mut sub).await;
    let second = next(&mut sub).await;
    let third = next(&mut sub).await;

    sub.nack(first.get_header("message-id").unwrap())
        .await
        .unwrap();
    sub.nack(second.get_header("ack").unwrap()).await.unwrap();
    sub.nack_frame(&third).await.unwrap();

    assert_eq!(ids_of(&seen, "NACK", 3).await, ["A1", "A2", "A3"]);
}

#[tokio::test]
async fn cumulative_ack_by_message_id_sends_one_ack_with_the_ack_header() {
    let (addr, seen) = start_broker(DIFFERING);
    let conn = connect(&addr).await;
    let mut sub = conn.subscribe("/queue/t", AckMode::Client).await.unwrap();
    for _ in 0..3 {
        next(&mut sub).await;
    }

    sub.ack("M3").await.unwrap();
    // A later frame, so "exactly one ACK" is not just "one so far".
    conn.send_frame(Frame::new("SEND").header("destination", "/queue/out"))
        .await
        .unwrap();
    ids_of(&seen, "SEND", 1).await;

    assert_eq!(ids_of(&seen, "ACK", 1).await, ["A3"]);
}

#[tokio::test]
async fn message_without_an_ack_header_is_acked_by_message_id() {
    // STOMP 1.0/1.1: no `ack` header on the MESSAGE.
    let (addr, seen) =
        start_broker("MESSAGE\nsubscription:{sub}\nmessage-id:M1\ndestination:/queue/t\n\n1\0");
    let conn = connect(&addr).await;
    let mut sub = conn
        .subscribe("/queue/t", AckMode::ClientIndividual)
        .await
        .unwrap();

    let frame = next(&mut sub).await;
    sub.ack_frame(&frame).await.unwrap();
    sub.nack("M1").await.unwrap();

    assert_eq!(ids_of(&seen, "ACK", 1).await, ["M1"]);
    assert_eq!(ids_of(&seen, "NACK", 1).await, ["M1"]);
}

#[tokio::test]
async fn message_routed_by_destination_records_its_ack_header() {
    // No `subscription` header: the library routes by destination.
    let (addr, seen) = start_broker("MESSAGE\nmessage-id:M1\nack:A1\ndestination:/queue/t\n\n1\0");
    let conn = connect(&addr).await;
    let mut sub = conn
        .subscribe("/queue/t", AckMode::ClientIndividual)
        .await
        .unwrap();

    let frame = next(&mut sub).await;
    sub.ack(frame.get_header("message-id").unwrap())
        .await
        .unwrap();

    assert_eq!(ids_of(&seen, "ACK", 1).await, ["A1"]);
}

#[tokio::test]
async fn unknown_id_is_sent_as_given() {
    let (addr, seen) = start_broker(DIFFERING);
    let conn = connect(&addr).await;
    let sub = conn
        .subscribe("/queue/t", AckMode::ClientIndividual)
        .await
        .unwrap();

    // Never delivered, or forgotten at a reconnect: nothing to translate.
    sub.ack("never-seen").await.unwrap();
    sub.nack("nor-this").await.unwrap();

    assert_eq!(ids_of(&seen, "ACK", 1).await, ["never-seen"]);
    assert_eq!(ids_of(&seen, "NACK", 1).await, ["nor-this"]);
}

#[tokio::test]
async fn ack_frame_without_either_header_is_an_error() {
    let (addr, seen) = start_broker("");
    let conn = connect(&addr).await;
    let sub = conn
        .subscribe("/queue/t", AckMode::ClientIndividual)
        .await
        .unwrap();

    let bare = Frame::new("MESSAGE").header("destination", "/queue/t");
    assert!(matches!(
        sub.ack_frame(&bare).await,
        Err(ConnError::MissingAckId)
    ));
    assert!(matches!(
        sub.nack_frame(&bare).await,
        Err(ConnError::MissingAckId)
    ));

    // And nothing was sent for it.
    conn.send_frame(Frame::new("SEND").header("destination", "/queue/out"))
        .await
        .unwrap();
    ids_of(&seen, "SEND", 1).await;
    assert!(
        !seen
            .lock()
            .unwrap()
            .iter()
            .any(|f| f.starts_with("ACK") || f.starts_with("NACK"))
    );
}
