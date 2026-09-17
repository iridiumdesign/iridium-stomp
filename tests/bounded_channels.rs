//! Coverage for #114 and #115: neither the connection-wide inbound channel
//! nor a subscription's channel may stall the background task or lose a
//! message when the application reads it slowly, or not at all.

use futures::StreamExt;
use iridium_stomp::{AckMode, Connection, Frame, ReceivedFrame, SubscriptionOptions};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

/// What the broker observed: complete frames in order, and heartbeat count.
#[derive(Clone, Default)]
struct Seen {
    frames: Arc<Mutex<Vec<String>>>,
    heartbeats: Arc<AtomicUsize>,
}

impl Seen {
    fn count(&self, command: &str) -> usize {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f.lines().next() == Some(command))
            .count()
    }
}

/// Value of `name` in a raw frame's header block.
fn header<'a>(raw: &'a str, name: &str) -> Option<&'a str> {
    raw.lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            (k == name).then_some(v)
        })
}

/// `count` MESSAGE frames for `subscription`, numbered from `first` in both
/// `message-id` and body.
fn messages(subscription: &str, destination: &str, first: usize, count: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for n in first..first + count {
        out.extend_from_slice(
            format!(
                "MESSAGE\nsubscription:{subscription}\nmessage-id:{n}\n\
                 destination:{destination}\n\n{n}\0"
            )
            .as_bytes(),
        );
    }
    out
}

/// A broker that answers CONNECT, asks the client for a heartbeat every
/// 100ms, records what it receives, writes whatever `script` returns for each
/// frame, and then answers any `receipt` header with a RECEIPT.
fn start_broker(mut script: impl FnMut(&str) -> Vec<u8> + Send + 'static) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen = Seen::default();
    let seen_clone = seen.clone();

    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut buf = [0u8; 4096];

        if stream.read(&mut buf).is_ok() {
            let _ = stream.write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,100\n\n\0");
            let _ = stream.flush();
        }

        // TCP is a byte stream: accumulate and act only on complete,
        // NUL-terminated frames. Newlines between frames are heartbeats.
        let mut acc: Vec<u8> = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => acc.extend_from_slice(&buf[..n]),
            }
            loop {
                let eols = acc
                    .iter()
                    .take_while(|&&b| b == b'\n' || b == b'\r')
                    .count();
                let beats = acc[..eols].iter().filter(|&&b| b == b'\n').count();
                seen_clone.heartbeats.fetch_add(beats, Ordering::SeqCst);
                acc.drain(..eols);

                let Some(pos) = acc.iter().position(|&b| b == 0) else {
                    break;
                };
                let frame: Vec<u8> = acc.drain(..=pos).collect();
                let raw = String::from_utf8_lossy(&frame[..pos]).to_string();
                seen_clone.frames.lock().unwrap().push(raw.clone());

                let mut reply = script(&raw);
                if let Some(id) = header(&raw, "receipt") {
                    reply.extend_from_slice(format!("RECEIPT\nreceipt-id:{id}\n\n\0").as_bytes());
                }
                if stream.write_all(&reply).is_err() {
                    return;
                }
                let _ = stream.flush();
            }
        }
    });

    (addr, seen)
}

async fn connect(addr: &str) -> Connection {
    Connection::connect(addr, "guest", "guest", "100,0")
        .await
        .unwrap()
}

fn send_to(destination: &str) -> Frame {
    Frame::new("SEND")
        .header("destination", destination)
        .set_body(b"x".to_vec())
}

/// Next frame from a subscription, or a panic naming how far it got.
async fn next_id(sub: &mut (impl futures::Stream<Item = Frame> + Unpin), expected: usize) -> Frame {
    let frame = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap_or_else(|_| panic!("stalled waiting for message {expected}"))
        .expect("subscription ended early");
    assert_eq!(
        frame.get_header("message-id"),
        Some(expected.to_string().as_str()),
        "messages must arrive exactly once and in order"
    );
    frame
}

// ============================================================================
// #114: the connection-wide inbound channel
// ============================================================================

#[tokio::test]
async fn subscription_only_consumer_never_stalls_the_connection() {
    const BURST: usize = 300;
    let (addr, seen) = start_broker(|raw| match header(raw, "id") {
        Some(id) if raw.starts_with("SUBSCRIBE") => messages(id, "/queue/t", 0, BURST),
        _ => Vec::new(),
    });
    let conn = connect(&addr).await;

    // Never call next_frame(): everything is consumed through the handle.
    let mut sub = conn.subscribe("/queue/t", AckMode::Auto).await.unwrap();
    for n in 0..BURST {
        next_id(&mut sub, n).await;
    }

    // The background task is still reading the socket...
    conn.send_frame_confirmed(send_to("/queue/out"), Duration::from_secs(2))
        .await
        .expect("RECEIPT must still arrive after several hundred messages");

    // ...and still writing heartbeats.
    let before = seen.heartbeats.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(450)).await;
    let after = seen.heartbeats.load(Ordering::SeqCst);
    assert!(
        after >= before + 2,
        "heartbeats must keep flowing (saw {before} then {after})"
    );

    // Subscribed messages were delivered to the subscription only.
    let stray = tokio::time::timeout(Duration::from_millis(200), conn.next_frame()).await;
    assert!(
        stray.is_err(),
        "a subscribed MESSAGE must not also appear on next_frame(), got {stray:?}"
    );
}

#[tokio::test]
async fn unsubscribed_message_still_reaches_next_frame() {
    let (addr, _seen) = start_broker(|raw| {
        if header(raw, "destination") == Some("/control/one") {
            messages("999", "/queue/nobody", 0, 1)
        } else {
            Vec::new()
        }
    });
    let conn = connect(&addr).await;
    // A subscription exists, but not the one the MESSAGE names.
    let _sub = conn.subscribe("/queue/t", AckMode::Auto).await.unwrap();

    conn.send_frame(send_to("/control/one")).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), conn.next_frame())
        .await
        .expect("an unmatched MESSAGE must come out of next_frame()");
    match received {
        Some(ReceivedFrame::Frame(frame)) => {
            assert_eq!(frame.command, "MESSAGE");
            assert_eq!(frame.get_header("destination"), Some("/queue/nobody"));
        }
        other => panic!("expected the unmatched MESSAGE, got {other:?}"),
    }
}

#[tokio::test]
async fn unread_inbound_channel_costs_frames_not_the_connection() {
    // Far more unmatched MESSAGEs than the inbound channel holds (32), none
    // of them ever read. The RECEIPT for the SEND that triggered them is
    // written after the flood.
    let (addr, _seen) = start_broker(|raw| {
        if header(raw, "destination") == Some("/control/flood") {
            messages("999", "/queue/nobody", 0, 200)
        } else {
            Vec::new()
        }
    });
    let conn = connect(&addr).await;

    conn.send_frame_confirmed(send_to("/control/flood"), Duration::from_secs(2))
        .await
        .expect("a full inbound channel must not block RECEIPT handling");
    conn.send_frame_confirmed(send_to("/queue/out"), Duration::from_secs(2))
        .await
        .expect("the connection must keep working afterwards");

    // The channel kept the oldest frames and dropped the overflow.
    let first = tokio::time::timeout(Duration::from_secs(1), conn.next_frame())
        .await
        .unwrap();
    match first {
        Some(ReceivedFrame::Frame(frame)) => {
            assert_eq!(frame.get_header("message-id"), Some("0"));
        }
        other => panic!("expected the first unmatched MESSAGE, got {other:?}"),
    }
}

// ============================================================================
// #115: a subscription's channel
// ============================================================================

#[tokio::test]
async fn slow_consumer_receives_every_message_in_order() {
    // Six times the default capacity of 16, written in one burst.
    const BURST: usize = 100;
    let (addr, seen) = start_broker(|raw| match header(raw, "id") {
        Some(id) if raw.starts_with("SUBSCRIBE") => messages(id, "/queue/t", 0, BURST),
        _ => Vec::new(),
    });
    let conn = connect(&addr).await;
    let mut sub = conn
        .subscribe("/queue/t", AckMode::ClientIndividual)
        .await
        .unwrap();

    for n in 0..BURST {
        let frame = next_id(&mut sub, n).await;
        tokio::time::sleep(Duration::from_millis(5)).await; // "work"
        sub.ack(frame.get_header("message-id").unwrap())
            .await
            .unwrap();

        if n == 0 {
            // The consumer is as far behind as it will get. Receipts and
            // heartbeats must be unaffected.
            let before = seen.heartbeats.load(Ordering::SeqCst);
            conn.send_frame_confirmed(send_to("/queue/out"), Duration::from_secs(2))
                .await
                .expect("RECEIPT must arrive while a subscriber is behind");
            tokio::time::sleep(Duration::from_millis(350)).await;
            let after = seen.heartbeats.load(Ordering::SeqCst);
            assert!(
                after > before,
                "heartbeats must keep flowing while a subscriber is behind"
            );
        }
    }

    let extra = tokio::time::timeout(Duration::from_millis(200), sub.next()).await;
    assert!(extra.is_err(), "no message may arrive twice, got {extra:?}");

    // Every message was handed over, so every message could be acked.
    conn.send_frame_confirmed(send_to("/queue/out"), Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(seen.count("ACK"), BURST);
}

#[tokio::test]
async fn one_slow_subscription_does_not_hold_up_another() {
    let (addr, _seen) = start_broker(|raw| match header(raw, "destination") {
        Some("/queue/slow") if raw.starts_with("SUBSCRIBE") => {
            messages(header(raw, "id").unwrap(), "/queue/slow", 0, 100)
        }
        Some("/queue/fast") if raw.starts_with("SUBSCRIBE") => {
            messages(header(raw, "id").unwrap(), "/queue/fast", 0, 10)
        }
        _ => Vec::new(),
    });
    let conn = connect(&addr).await;

    // Never read while the fast subscription is consumed.
    let mut slow = conn.subscribe("/queue/slow", AckMode::Auto).await.unwrap();
    let mut fast = conn.subscribe("/queue/fast", AckMode::Auto).await.unwrap();
    for n in 0..10 {
        next_id(&mut fast, n).await;
    }
    for n in 0..100 {
        next_id(&mut slow, n).await;
    }
}

#[tokio::test]
async fn channel_capacity_is_configurable() {
    const BURST: usize = 50;
    let (addr, _seen) = start_broker(|raw| match header(raw, "destination") {
        Some("/queue/small") if raw.starts_with("SUBSCRIBE") => {
            messages(header(raw, "id").unwrap(), "/queue/small", 0, BURST)
        }
        _ => Vec::new(),
    });
    let conn = connect(&addr).await;

    let default = conn.subscribe("/queue/a", AckMode::Auto).await.unwrap();
    assert_eq!(
        default.into_receiver().max_capacity(),
        SubscriptionOptions::DEFAULT_CHANNEL_CAPACITY
    );

    let opts = SubscriptionOptions::default().channel_capacity(64);
    let large = conn
        .subscribe_with_options("/queue/b", AckMode::Auto, opts)
        .await
        .unwrap();
    assert_eq!(large.into_receiver().max_capacity(), 64);

    // Zero would panic in tokio; it is treated as one.
    let opts = SubscriptionOptions {
        channel_capacity: Some(0),
        ..Default::default()
    };
    let zero = conn
        .subscribe_with_options("/queue/c", AckMode::Auto, opts)
        .await
        .unwrap();
    assert_eq!(zero.into_receiver().max_capacity(), 1);

    // The smallest channel still delivers a burst completely and in order.
    let opts = SubscriptionOptions::default().channel_capacity(1);
    let mut small = conn
        .subscribe_with_options("/queue/small", AckMode::ClientIndividual, opts)
        .await
        .unwrap();
    for n in 0..BURST {
        next_id(&mut small, n).await;
    }
}

/// Park a burst for a capacity-1 subscription, let go of it, and check the
/// entry is gone: a later MESSAGE for the same id finds no subscription and
/// so comes out of `next_frame()`.
async fn parked_frames_are_discarded(keep_subscribed: bool) {
    let mut sub_id = String::new();
    let (addr, _seen) = start_broker(move |raw| {
        if raw.starts_with("SUBSCRIBE") {
            sub_id = header(raw, "id").unwrap().to_string();
            messages(&sub_id, "/queue/t", 0, 40)
        } else if header(raw, "destination") == Some("/control/late") {
            messages(&sub_id, "/queue/t", 1000, 1)
        } else {
            Vec::new()
        }
    });
    let conn = connect(&addr).await;

    let opts = SubscriptionOptions::default().channel_capacity(1);
    let sub = conn
        .subscribe_with_options("/queue/t", AckMode::ClientIndividual, opts)
        .await
        .unwrap();
    // Let the whole burst arrive: one frame in the channel, the rest parked.
    conn.send_frame_confirmed(send_to("/queue/out"), Duration::from_secs(2))
        .await
        .unwrap();

    if keep_subscribed {
        // No UNSUBSCRIBE and no registry removal: only the closed channel
        // tells the background task the consumer is gone.
        drop(sub.into_receiver());
    } else {
        drop(sub);
    }

    conn.send_frame_confirmed(send_to("/control/late"), Duration::from_secs(2))
        .await
        .expect("the connection must survive a dropped, parked subscription");

    let received = tokio::time::timeout(Duration::from_secs(2), conn.next_frame())
        .await
        .expect("the late MESSAGE must surface on next_frame()");
    match received {
        Some(ReceivedFrame::Frame(frame)) => {
            assert_eq!(frame.get_header("message-id"), Some("1000"));
        }
        other => panic!("expected the late MESSAGE, got {other:?}"),
    }
}

#[tokio::test]
async fn dropped_subscription_with_parked_frames_is_pruned() {
    parked_frames_are_discarded(false).await;
}

#[tokio::test]
async fn closed_receiver_with_parked_frames_is_pruned() {
    parked_frames_are_discarded(true).await;
}
