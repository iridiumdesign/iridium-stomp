//! Coverage for #89: when a `Subscription`'s stream ends, `ended()` says
//! why — abandoned, overflowed, unsubscribed, or connection closed — and a
//! reconnect is none of those.

use futures::StreamExt;
use iridium_stomp::{
    AckMode, Connection, Frame, Subscription, SubscriptionEnd, SubscriptionOptions,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

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

/// `count` MESSAGE frames for `subscription`, numbered from `first`.
fn messages(subscription: &str, first: usize, count: usize) -> String {
    (first..first + count)
        .map(|n| {
            format!(
                "MESSAGE\nsubscription:{subscription}\nmessage-id:{n}\n\
                 destination:/queue/t\n\n{n}\0"
            )
        })
        .collect()
}

/// A broker that answers CONNECT, accepts any number of connections in turn,
/// writes whatever `script` returns for each frame (`{sub}` in it stands for
/// the id of the last SUBSCRIBE seen), answers `receipt` headers with a
/// RECEIPT, and closes the socket on a SEND to `/control/drop`.
fn start_broker(mut script: impl FnMut(&str, usize) -> String + Send + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    thread::spawn(move || {
        let mut connections = 0;
        'accept: loop {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            connections += 1;
            let mut buf = [0u8; 4096];
            if stream.read(&mut buf).is_ok() {
                let _ = stream.write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0");
                let _ = stream.flush();
            }

            let mut sub_id = String::new();
            let mut acc: Vec<u8> = Vec::new();
            loop {
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => continue 'accept,
                    Ok(n) => acc.extend_from_slice(&buf[..n]),
                }
                while let Some(pos) = acc.iter().position(|&b| b == 0) {
                    let frame: Vec<u8> = acc.drain(..=pos).collect();
                    let raw = String::from_utf8_lossy(&frame[..pos])
                        .trim_start()
                        .to_string();
                    if raw.is_empty() {
                        continue;
                    }
                    if header(&raw, "destination").as_deref() == Some("/control/drop") {
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        continue 'accept;
                    }
                    if raw.starts_with("SUBSCRIBE") {
                        sub_id = header(&raw, "id").unwrap_or_default();
                    }
                    let mut reply = script(&raw, connections).replace("{sub}", &sub_id);
                    if let Some(id) = header(&raw, "receipt") {
                        reply.push_str(&format!("RECEIPT\nreceipt-id:{id}\n\n\0"));
                    }
                    if stream.write_all(reply.as_bytes()).is_err() {
                        continue 'accept;
                    }
                    let _ = stream.flush();
                }
            }
        }
    });

    addr
}

async fn connect(addr: &str) -> Connection {
    Connection::connect(addr, "guest", "guest", "0,0")
        .await
        .unwrap()
}

fn send_to(destination: &str) -> Frame {
    Frame::new("SEND")
        .header("destination", destination)
        .set_body(b"x".to_vec())
}

/// Drain the subscription to its end within a timeout and return how many
/// frames came out first.
async fn drain(sub: &mut Subscription) -> usize {
    let mut count = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), sub.next()).await {
            Ok(Some(_)) => count += 1,
            Ok(None) => return count,
            Err(_) => panic!("the subscription did not end"),
        }
    }
}

#[tokio::test]
async fn abandoned_after_repeated_broker_errors() {
    // Three ERRORs naming the destination is the library's threshold.
    let addr = start_broker(|raw, _| {
        if raw.starts_with("SUBSCRIBE") {
            "ERROR\nmessage:not authorized for /queue/t\n\n\0".repeat(3)
        } else {
            String::new()
        }
    });
    let conn = connect(&addr).await;
    let mut sub = conn.subscribe("/queue/t", AckMode::Auto).await.unwrap();
    assert_eq!(sub.ended(), None, "live until the broker gives up on it");

    assert_eq!(drain(&mut sub).await, 0);
    assert_eq!(
        sub.ended(),
        Some(SubscriptionEnd::Abandoned {
            message: "not authorized for /queue/t".to_string()
        })
    );
}

#[tokio::test]
async fn overflowed_past_the_limit() {
    let addr = start_broker(|raw, _| {
        if raw.starts_with("SUBSCRIBE") {
            messages("{sub}", 0, 100)
        } else {
            String::new()
        }
    });
    let conn = connect(&addr).await;
    let opts = SubscriptionOptions::default()
        .channel_capacity(2)
        .overflow_limit(5);
    let mut sub = conn
        .subscribe_with_options("/queue/t", AckMode::Auto, opts)
        .await
        .unwrap();
    // Let the burst arrive and the limit trip before reading anything.
    conn.send_frame_confirmed(send_to("/queue/out"), Duration::from_secs(2))
        .await
        .unwrap();

    // What was already in the channel still comes out, then the end.
    assert_eq!(drain(&mut sub).await, 2);
    assert_eq!(sub.ended(), Some(SubscriptionEnd::Overflowed { limit: 5 }));
}

#[tokio::test]
async fn unsubscribed_while_holding_the_handle() {
    let addr = start_broker(|raw, _| {
        if raw.starts_with("SUBSCRIBE") {
            messages("{sub}", 0, 3)
        } else {
            String::new()
        }
    });
    let conn = connect(&addr).await;
    let mut sub = conn.subscribe("/queue/t", AckMode::Auto).await.unwrap();
    conn.send_frame_confirmed(send_to("/queue/out"), Duration::from_secs(2))
        .await
        .unwrap();

    conn.unsubscribe(sub.id()).await.unwrap();
    assert_eq!(sub.ended(), Some(SubscriptionEnd::Unsubscribed));
    // The three frames already delivered are still yielded first.
    assert_eq!(drain(&mut sub).await, 3);
    assert_eq!(sub.ended(), Some(SubscriptionEnd::Unsubscribed));
}

#[tokio::test]
async fn connection_closed() {
    let addr = start_broker(|_, _| String::new());
    let conn = connect(&addr).await;
    let mut sub = conn.subscribe("/queue/t", AckMode::Auto).await.unwrap();

    let _ = conn.close().await;

    assert_eq!(drain(&mut sub).await, 0);
    assert_eq!(sub.ended(), Some(SubscriptionEnd::ConnectionClosed));
}

#[tokio::test]
async fn live_across_a_reconnect() {
    // Two messages on the first session, two more on the second.
    let addr = start_broker(|raw, connection| {
        if raw.starts_with("SUBSCRIBE") {
            messages("{sub}", connection * 10, 2)
        } else {
            String::new()
        }
    });
    let conn = connect(&addr).await;
    let mut sub = conn.subscribe("/queue/t", AckMode::Auto).await.unwrap();

    for n in [10, 11] {
        let frame = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.get_header("message-id"), Some(n.to_string().as_str()));
        assert_eq!(sub.ended(), None, "live while frames flow");
    }

    conn.send_frame(send_to("/control/drop")).await.unwrap();
    // The library reconnects after its backoff and resubscribes.
    for n in [20, 21] {
        let frame = tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await
            .expect("frames must keep arriving after the reconnect")
            .expect("a reconnect must not end the subscription");
        assert_eq!(frame.get_header("message-id"), Some(n.to_string().as_str()));
    }
    assert_eq!(sub.ended(), None, "a reconnect is not an end");
}
