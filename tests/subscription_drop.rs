//! Coverage for #83: dropping a `Subscription` sends a best-effort UNSUBSCRIBE
//! and stops the broker-side subscription, and does not double up with an
//! explicit `unsubscribe`.

use iridium_stomp::{AckMode, Connection};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// Raw frames the broker saw, in order.
type Seen = Arc<Mutex<Vec<String>>>;

/// A broker that answers CONNECT and records every frame it receives.
fn start_broker() -> (String, Seen) {
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

        loop {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buf[..n]).to_string();
                    for raw in text.split('\0').filter(|f| !f.trim().is_empty()) {
                        seen_clone
                            .lock()
                            .unwrap()
                            .push(raw.trim_start().to_string());
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(200));
    });

    (addr, seen)
}

/// The commands (first line of each recorded frame), in order.
fn commands(seen: &Seen) -> Vec<String> {
    seen.lock()
        .unwrap()
        .iter()
        .map(|f| f.lines().next().unwrap_or("").to_string())
        .collect()
}

/// The `id` header of the first frame whose command matches.
fn id_of_command(seen: &Seen, command: &str) -> Option<String> {
    seen.lock().unwrap().iter().find_map(|frame| {
        let mut lines = frame.lines();
        if lines.next()? != command {
            return None;
        }
        frame.lines().find_map(|line| {
            let (k, v) = line.split_once(':')?;
            (k.eq_ignore_ascii_case("id")).then(|| v.to_string())
        })
    })
}

#[tokio::test]
async fn dropping_a_subscription_sends_unsubscribe() {
    let (addr, seen) = start_broker();
    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();

    let sub = conn
        .subscribe("/queue/orders", AckMode::Auto)
        .await
        .unwrap();
    let sub_id = sub.id().to_string();
    drop(sub);

    // Give the drop's best-effort try_send and the broker thread a moment.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cmds = commands(&seen);
    assert!(
        cmds.contains(&"UNSUBSCRIBE".to_string()),
        "dropping a subscription must send UNSUBSCRIBE; broker saw {:?}",
        cmds
    );
    assert_eq!(
        id_of_command(&seen, "UNSUBSCRIBE").as_deref(),
        Some(sub_id.as_str()),
        "the UNSUBSCRIBE must carry the dropped subscription's id"
    );
}

#[tokio::test]
async fn explicit_unsubscribe_then_drop_sends_exactly_one() {
    let (addr, seen) = start_broker();
    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();

    let sub = conn
        .subscribe("/queue/orders", AckMode::Auto)
        .await
        .unwrap();
    sub.unsubscribe().await.unwrap(); // consumes `sub`, which then drops

    tokio::time::sleep(Duration::from_millis(100)).await;

    let unsub_count = commands(&seen)
        .iter()
        .filter(|c| *c == "UNSUBSCRIBE")
        .count();
    assert_eq!(
        unsub_count, 1,
        "explicit unsubscribe followed by drop must not send a second UNSUBSCRIBE"
    );
}

#[tokio::test]
async fn into_receiver_keeps_the_subscription_active() {
    let (addr, seen) = start_broker();
    let conn = Connection::connect(&addr, "guest", "guest", "0,0")
        .await
        .unwrap();

    let sub = conn
        .subscribe("/queue/orders", AckMode::Auto)
        .await
        .unwrap();
    let rx = sub.into_receiver(); // caller takes the stream; must NOT unsubscribe

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        !commands(&seen).contains(&"UNSUBSCRIBE".to_string()),
        "into_receiver hands off the stream and must not unsubscribe"
    );

    // Dropping the raw receiver also must not send anything (it has no handle).
    drop(rx);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!commands(&seen).contains(&"UNSUBSCRIBE".to_string()));
}
