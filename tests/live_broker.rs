//! Against a real broker: what the fake broker in the other test files
//! cannot vouch for. Skipped unless `STOMP_LIVE_ADDR` is set.
//!
//! ```sh
//! STOMP_LIVE_ADDR=127.0.0.1:61613 STOMP_LIVE_LOGIN=guest \
//! STOMP_LIVE_PASSCODE=guest cargo test --test live_broker -- --test-threads=1
//! ```
//!
//! - `STOMP_LIVE_ANYCAST=1` adds `destination-type: ANYCAST` to every
//!   SEND and `subscription-type: ANYCAST` to every SUBSCRIBE, which is
//!   what Artemis needs for `/queue/...` to be a queue rather than a
//!   multicast address (each header belongs to its own frame; Artemis
//!   ignores either on the other). Without it a
//!   subscription's messages vanish with the subscription, and the
//!   redelivery checks here prove nothing.
//! - `STOMP_LIVE_RESTART_CMD`, if set, is run (`sh -c`) in the middle of
//!   the reconnect test to bounce the broker, e.g.
//!   `docker restart my-artemis`. Without it that test is skipped.
//!
//! Every test uses a destination of its own, named after the process,
//! so runs do not see each other's leftovers; and every test acks or
//! drains what it sends, so nothing is left behind on the broker.

use std::{env, process::Command, time::Duration};

use futures::StreamExt;
use iridium_stomp::{
    AckMode, ConnectOptions, Connection, Frame, ReceivedFrame, Subscription, SubscriptionEnd,
    SubscriptionOptions,
};
use tokio::time::{sleep, timeout};

struct Live {
    addr: String,
    login: String,
    passcode: String,
    anycast: bool,
}

fn live() -> Option<Live> {
    let addr = env::var("STOMP_LIVE_ADDR").ok()?;
    Some(Live {
        addr,
        login: env::var("STOMP_LIVE_LOGIN").unwrap_or_else(|_| "guest".into()),
        passcode: env::var("STOMP_LIVE_PASSCODE").unwrap_or_else(|_| "guest".into()),
        anycast: env::var("STOMP_LIVE_ANYCAST").is_ok_and(|v| v == "1"),
    })
}

impl Live {
    async fn connect(&self) -> Connection {
        Connection::connect_with_options(
            &self.addr,
            &self.login,
            &self.passcode,
            "5000,5000",
            ConnectOptions::default().connect_timeout(Duration::from_secs(10)),
        )
        .await
        .expect("connect")
    }

    fn queue(&self, test: &str) -> String {
        format!("/queue/live-{test}-{}", std::process::id())
    }

    fn options(&self) -> SubscriptionOptions {
        let opts = SubscriptionOptions::new();
        if self.anycast {
            opts.header("subscription-type", "ANYCAST")
        } else {
            opts
        }
    }

    async fn subscribe(
        &self,
        conn: &Connection,
        dest: &str,
        ack: AckMode,
        opts: SubscriptionOptions,
    ) -> Subscription {
        conn.subscribe_with_options(dest, ack, opts)
            .await
            .expect("subscribe")
    }

    async fn send(&self, conn: &Connection, dest: &str, body: &str) {
        let mut frame = Frame::new("SEND")
            .header("destination", dest)
            .header("persistent", "true")
            .set_body(body.as_bytes().to_vec());
        if self.anycast {
            frame = frame.header("destination-type", "ANYCAST");
        }
        conn.send_frame_confirmed(frame, Duration::from_secs(10))
            .await
            .expect("send confirmed");
    }
}

macro_rules! live_or_skip {
    () => {
        match live() {
            Some(live) => live,
            None => {
                eprintln!("STOMP_LIVE_ADDR not set; skipping");
                return;
            }
        }
    };
}

async fn next(sub: &mut Subscription, secs: u64) -> Option<Frame> {
    timeout(Duration::from_secs(secs), sub.next())
        .await
        .unwrap_or_else(|_| panic!("no frame within {secs}s"))
}

fn body(frame: &Frame) -> String {
    String::from_utf8_lossy(&frame.body).into_owned()
}

/// Drain and ack whatever is left on a destination, so the broker is as
/// we found it.
async fn drain(live: &Live, conn: &Connection, dest: &str) -> usize {
    let mut sub = live
        .subscribe(conn, dest, AckMode::ClientIndividual, live.options())
        .await;
    let mut n = 0;
    while let Ok(Some(frame)) = timeout(Duration::from_secs(2), sub.next()).await {
        sub.ack_frame(&frame).await.expect("ack");
        n += 1;
    }
    sub.unsubscribe().await.expect("unsubscribe");
    n
}

/// #114: a consumer that reads only its subscription, never
/// `next_frame()`, used to stall the connection for good on the 33rd
/// message: no heartbeats, no receipts, and the broker's TTL then
/// closed the socket. Two hundred messages, then a confirmed send, then
/// an idle spell longer than any broker's default TTL, then another.
#[tokio::test]
async fn subscription_only_consumer_neither_stalls_nor_times_out() {
    let live = live_or_skip!();
    let conn = live.connect().await;
    let dest = live.queue("stall");
    let mut sub = live
        .subscribe(&conn, &dest, AckMode::ClientIndividual, live.options())
        .await;

    for i in 0..200 {
        live.send(&conn, &dest, &i.to_string()).await;
    }
    for i in 0..200 {
        let frame = next(&mut sub, 10).await.expect("a frame");
        assert_eq!(body(&frame), i.to_string(), "in order");
        sub.ack_frame(&frame).await.expect("ack");
    }

    live.send(&conn, &dest, "after two hundred").await;
    let frame = next(&mut sub, 10).await.expect("a frame");
    sub.ack_frame(&frame).await.expect("ack");

    // Artemis closes a silent connection after 20 s by default.
    sleep(Duration::from_secs(30)).await;
    live.send(&conn, &dest, "after thirty seconds idle").await;
    let frame = next(&mut sub, 10).await.expect("still connected");
    assert_eq!(body(&frame), "after thirty seconds idle");
    sub.ack_frame(&frame).await.expect("ack");

    sub.unsubscribe().await.expect("unsubscribe");
    conn.close().await.expect("close");
}

/// #115: a consumer slower than the producer, with a small channel.
/// Every message arrives, in order, and every ack takes: a fresh
/// subscription afterwards finds the queue empty.
#[tokio::test]
async fn slow_consumer_receives_everything_in_order_and_acks_take() {
    let live = live_or_skip!();
    let conn = live.connect().await;
    let dest = live.queue("slow");
    let mut sub = live
        .subscribe(
            &conn,
            &dest,
            AckMode::ClientIndividual,
            live.options().channel_capacity(4),
        )
        .await;

    for i in 0..300 {
        live.send(&conn, &dest, &i.to_string()).await;
    }
    for i in 0..300 {
        let frame = next(&mut sub, 10).await.expect("a frame");
        assert_eq!(body(&frame), i.to_string(), "in order");
        sleep(Duration::from_millis(3)).await;
        sub.ack_frame(&frame).await.expect("ack");
    }
    assert_eq!(sub.ended(), None);
    sub.unsubscribe().await.expect("unsubscribe");

    assert_eq!(drain(&live, &conn, &dest).await, 0, "every ack took");
    conn.close().await.expect("close");
}

/// #116: at the overflow limit the subscription is failed and the
/// broker is told. In client-individual mode nothing was acked, so a
/// fresh subscription gets all of it back.
#[tokio::test]
async fn overflow_fails_the_subscription_and_the_broker_redelivers() {
    let live = live_or_skip!();
    let conn = live.connect().await;
    let dest = live.queue("overflow");
    let mut sub = live
        .subscribe(
            &conn,
            &dest,
            AckMode::ClientIndividual,
            live.options().channel_capacity(1).overflow_limit(3),
        )
        .await;

    let total = 40;
    for i in 0..total {
        live.send(&conn, &dest, &i.to_string()).await;
    }
    // Never read. The one frame in the channel comes out, then None.
    sleep(Duration::from_secs(2)).await;
    let mut got = 0;
    while let Some(_frame) = next(&mut sub, 5).await {
        got += 1;
    }
    assert_eq!(got, 1, "only what was already in the channel");
    assert_eq!(sub.ended(), Some(SubscriptionEnd::Overflowed { limit: 3 }));

    let notice = timeout(Duration::from_secs(5), conn.next_frame())
        .await
        .expect("an ERROR on next_frame")
        .expect("open");
    match notice {
        ReceivedFrame::Error(e) => {
            assert!(e.to_string().contains("overflow") || format!("{e:?}").contains("overflow"))
        }
        ReceivedFrame::Frame(f) => assert_eq!(f.get_header("x-overflow"), Some("true")),
        other => panic!("unexpected {other:?}"),
    }

    // On a fresh connection. Artemis and ActiveMQ Classic requeue a
    // consumer's unacknowledged messages at UNSUBSCRIBE; RabbitMQ does
    // so only when the connection closes.
    conn.close().await.expect("close");
    let conn = live.connect().await;
    let back = drain(&live, &conn, &dest).await;
    assert_eq!(back, total, "the broker redelivered everything unacked");
    conn.close().await.expect("close");
}

/// #119: acknowledging by message-id, as every example used to, takes
/// on this broker: after a disconnect and a fresh subscription the
/// message does not come back. On ActiveMQ Classic the two headers
/// differ, and before the fix this was where it came back.
#[tokio::test]
async fn ack_by_message_id_takes() {
    let live = live_or_skip!();
    let conn = live.connect().await;
    let dest = live.queue("ack");
    let mut sub = live
        .subscribe(&conn, &dest, AckMode::ClientIndividual, live.options())
        .await;
    live.send(&conn, &dest, "once").await;
    let frame = next(&mut sub, 10).await.expect("a frame");
    let message_id = frame
        .get_header("message-id")
        .expect("message-id")
        .to_owned();
    eprintln!(
        "message-id={message_id} ack={:?} same={}",
        frame.get_header("ack"),
        frame.get_header("ack") == Some(message_id.as_str())
    );
    sub.ack(&message_id).await.expect("ack");
    sleep(Duration::from_millis(500)).await;
    sub.unsubscribe().await.expect("unsubscribe");
    conn.close().await.expect("close");

    let conn = live.connect().await;
    assert_eq!(drain(&live, &conn, &dest).await, 0, "not redelivered");
    conn.close().await.expect("close");
}

/// #89: the handle says why its stream ended.
#[tokio::test]
async fn unsubscribe_and_close_say_why_the_stream_ended() {
    let live = live_or_skip!();
    let conn = live.connect().await;
    let dest = live.queue("ended");

    let mut sub = live
        .subscribe(&conn, &dest, AckMode::Auto, live.options())
        .await;
    assert_eq!(sub.ended(), None);
    conn.unsubscribe(sub.id()).await.expect("unsubscribe");
    assert_eq!(next(&mut sub, 5).await, None);
    assert_eq!(sub.ended(), Some(SubscriptionEnd::Unsubscribed));

    let mut sub = live
        .subscribe(&conn, &dest, AckMode::Auto, live.options())
        .await;
    conn.close().await.expect("close");
    assert_eq!(next(&mut sub, 5).await, None);
    assert_eq!(sub.ended(), Some(SubscriptionEnd::ConnectionClosed));
}

/// The broker goes away and comes back: the subscription is
/// re-established, frames flow again, and `ended()` stays `None`.
/// Needs `STOMP_LIVE_RESTART_CMD`.
#[tokio::test]
async fn subscription_survives_a_broker_restart() {
    let live = live_or_skip!();
    let Ok(restart) = env::var("STOMP_LIVE_RESTART_CMD") else {
        eprintln!("STOMP_LIVE_RESTART_CMD not set; skipping");
        return;
    };
    let conn = live.connect().await;
    let dest = live.queue("restart");
    let mut sub = live
        .subscribe(&conn, &dest, AckMode::Auto, live.options())
        .await;

    live.send(&conn, &dest, "before").await;
    assert_eq!(body(&next(&mut sub, 10).await.expect("before")), "before");

    let status = Command::new("sh")
        .arg("-c")
        .arg(&restart)
        .status()
        .expect("run the restart command");
    assert!(status.success(), "restart command failed");

    // Give the broker time to come back and the client time to notice
    // and reconnect; sends are retried until one is confirmed.
    let mut sent = false;
    for _ in 0..30 {
        sleep(Duration::from_secs(1)).await;
        let mut frame = Frame::new("SEND")
            .header("destination", &dest)
            .set_body(b"after".to_vec());
        if live.anycast {
            frame = frame.header("destination-type", "ANYCAST");
        }
        if conn
            .send_frame_confirmed(frame, Duration::from_secs(3))
            .await
            .is_ok()
        {
            sent = true;
            break;
        }
    }
    assert!(sent, "never reconnected");
    assert_eq!(body(&next(&mut sub, 15).await.expect("after")), "after");
    assert_eq!(sub.ended(), None, "a reconnect is not an end");

    sub.unsubscribe().await.expect("unsubscribe");
    conn.close().await.expect("close");
}
