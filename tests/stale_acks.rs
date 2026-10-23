//! Coverage for #118: a MESSAGE delivered before a reconnect, still held by
//! the application when the connection comes back, must not be acknowledged
//! on the new session, whose broker never issued its id. The broker
//! redelivers it, and the redelivered copy is acknowledged instead.

use futures::StreamExt;
use iridium_stomp::{AckMode, ConnectOptions, Connection, Frame, Subscription};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// How the broker names its deliveries in each session.
#[derive(Clone, Copy)]
enum Ids {
    /// `ack` differs from `message-id` and is scoped to the session, as
    /// ActiveMQ Classic sends them: `ack:m1-s1`, then `ack:m1-s2`.
    PerSession,
    /// No `ack` header, and the same `message-id` in both sessions, as a
    /// broker that reuses its message ids redelivers them.
    Reused,
}

/// The broker's end of one connection: frames are read through their NUL.
struct Wire(BufReader<TcpStream>);

impl Wire {
    fn accept(listener: &TcpListener) -> Self {
        let (socket, _) = listener.accept().unwrap();
        let mut wire = Wire(BufReader::new(socket));
        let connect = wire.recv();
        assert!(
            connect.starts_with("CONNECT\n"),
            "expected CONNECT, got {connect:?}"
        );
        wire.send("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0");
        wire
    }

    fn recv(&mut self) -> String {
        let mut raw = Vec::new();
        self.0.read_until(b'\0', &mut raw).unwrap();
        String::from_utf8(raw).unwrap()
    }

    fn send(&mut self, raw: &str) {
        self.0.get_mut().write_all(raw.as_bytes()).unwrap();
    }

    /// Read until a SEND to `destination`, answering every receipt request
    /// on the way. Returns the ACK and NACK frames seen.
    fn until_send_to(&mut self, destination: &str) -> Vec<String> {
        let mut acks = Vec::new();
        loop {
            let frame = self.recv();
            if let Some(receipt) = header(&frame, "receipt") {
                self.send(&format!("RECEIPT\nreceipt-id:{receipt}\n\n\0"));
            }
            if frame.starts_with("ACK\n") || frame.starts_with("NACK\n") {
                acks.push(frame);
            } else if frame.starts_with("SEND\n")
                && header(&frame, "destination") == Some(destination)
            {
                return acks;
            }
        }
    }
}

fn header<'a>(raw: &'a str, name: &str) -> Option<&'a str> {
    raw.lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
}

/// How the broker runs the second session.
#[derive(Clone, Copy)]
enum Redelivery {
    /// Answer the CONNECT at once and redeliver m1 and m2.
    Prompt,
    /// Hold the CONNECT answer this long, leaving the client reconnecting,
    /// then redeliver m1 and m2.
    AfterHandshakeDelay(Duration),
    /// Answer at once, redeliver m1, and redeliver m2 only when the client
    /// sends to `/control/more`.
    Staged,
}

/// Read the SUBSCRIBE and return its id.
fn subscribed(wire: &mut Wire) -> String {
    let subscribe = wire.recv();
    assert!(
        subscribe.starts_with("SUBSCRIBE\n"),
        "expected SUBSCRIBE, got {subscribe:?}"
    );
    header(&subscribe, "id").unwrap().to_string()
}

/// Message `n` for subscription `sub`, as `session` names it.
fn message(sub: &str, ids: Ids, session: u8, n: u8) -> String {
    let ack = match ids {
        Ids::PerSession => format!("ack:m{n}-s{session}\n"),
        Ids::Reused => String::new(),
    };
    format!("MESSAGE\nsubscription:{sub}\nmessage-id:m{n}\n{ack}destination:/queue/t\n\n{n}\0")
}

/// A broker that delivers m1 and m2, drops the connection on a SEND to
/// `/control/drop`, redelivers both to the replayed SUBSCRIBE as
/// `redelivery` says, and returns the ACK and NACK frames of the second
/// session once the client sends to `/control/done`.
fn broker(ids: Ids, redelivery: Redelivery) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let thread = thread::spawn(move || {
        let mut first = Wire::accept(&listener);
        let sub = subscribed(&mut first);
        first.send(&(message(&sub, ids, 1, 1) + &message(&sub, ids, 1, 2)));
        let before = first.until_send_to("/control/drop");
        assert!(before.is_empty(), "nothing is acked before the reconnect");
        drop(first);

        if let Redelivery::AfterHandshakeDelay(delay) = redelivery {
            thread::sleep(delay);
        }
        let mut second = Wire::accept(&listener);
        let sub = subscribed(&mut second);
        let mut acks = Vec::new();
        if let Redelivery::Staged = redelivery {
            second.send(&message(&sub, ids, 2, 1));
            acks = second.until_send_to("/control/more");
            second.send(&message(&sub, ids, 2, 2));
        } else {
            second.send(&(message(&sub, ids, 2, 1) + &message(&sub, ids, 2, 2)));
        }
        acks.extend(second.until_send_to("/control/done"));
        acks
    });
    (addr, thread)
}

/// Subscribe, let m1 and m2 arrive unread, and lose the connection. Returns
/// once the client has seen the close, with both copies in the
/// subscription's channel.
async fn lose_connection(addr: &str, ack: AckMode) -> (Connection, Subscription) {
    let conn = Connection::connect_with_options(
        addr,
        "guest",
        "guest",
        "0,0",
        ConnectOptions::default().disconnect_timeout(Duration::from_millis(50)),
    )
    .await
    .unwrap();
    let sub = conn.subscribe("/queue/t", ack).await.unwrap();

    // A round trip: both MESSAGEs have been read into the channel.
    conn.send_frame_confirmed(barrier(), Duration::from_secs(2))
        .await
        .unwrap();
    conn.send_frame(Frame::new("SEND").header("destination", "/control/drop"))
        .await
        .unwrap();
    // Let the client see the close: a frame written in the meantime goes
    // onto the dead socket and is lost.
    tokio::time::sleep(Duration::from_millis(500)).await;
    (conn, sub)
}

fn barrier() -> Frame {
    Frame::new("SEND").header("destination", "/queue/barrier")
}

/// Wait for the new session. Sent while the client reconnects, the barrier
/// waits for it, behind the replayed SUBSCRIBE.
async fn await_new_session(conn: &Connection) {
    conn.send_frame_confirmed(barrier(), Duration::from_secs(10))
        .await
        .expect("the connection must come back");
}

/// `lose_connection`, then `await_new_session`: the stale copies are still in
/// the subscription's channel, and the redelivered ones behind them.
async fn reconnect_holding_two(addr: &str, ack: AckMode) -> (Connection, Subscription) {
    let (conn, sub) = lose_connection(addr, ack).await;
    await_new_session(&conn).await;
    (conn, sub)
}

async fn next_copy(sub: &mut Subscription) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("stalled after reconnect")
        .expect("subscription ended early")
}

/// Read the four copies: stale m1, m2, then redelivered m1, m2.
async fn four_copies(sub: &mut Subscription) -> Vec<Frame> {
    let mut frames = Vec::new();
    for _ in 0..4 {
        frames.push(next_copy(sub).await);
    }
    let ids: Vec<_> = frames
        .iter()
        .map(|f| f.get_header("message-id").unwrap())
        .collect();
    assert_eq!(ids, ["m1", "m2", "m1", "m2"]);
    frames
}

/// Tell the broker the test is done and collect what it saw.
async fn finish(conn: Connection, broker: JoinHandle<Vec<String>>) -> Vec<String> {
    conn.send_frame_confirmed(
        Frame::new("SEND").header("destination", "/control/done"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let _ = conn.close().await;
    broker.join().unwrap()
}

fn ack_ids(acks: &[String]) -> Vec<&str> {
    acks.iter().map(|f| header(f, "id").unwrap()).collect()
}

#[tokio::test]
async fn stale_copies_are_not_acked_on_the_new_session() {
    let (addr, broker) = broker(Ids::PerSession, Redelivery::Prompt);
    let (conn, mut sub) = reconnect_holding_two(&addr, AckMode::ClientIndividual).await;

    for frame in four_copies(&mut sub).await {
        sub.ack_frame(&frame).await.unwrap();
    }

    let acks = finish(conn, broker).await;
    assert_eq!(
        ack_ids(&acks),
        ["m1-s2", "m2-s2"],
        "only the redelivered copies' ids, which this session issued"
    );
}

#[tokio::test]
async fn acking_by_message_id_skips_stale_copies_too() {
    // `ack(id)` names the message by `message-id`, the same in both sessions
    // here: the live copy is acked once, and the second ack finds only the
    // retired copy, so nothing more goes out.
    let (addr, broker) = broker(Ids::PerSession, Redelivery::Prompt);
    let (conn, mut sub) = reconnect_holding_two(&addr, AckMode::ClientIndividual).await;

    for frame in four_copies(&mut sub).await {
        sub.ack(frame.get_header("message-id").unwrap())
            .await
            .unwrap();
    }

    let acks = finish(conn, broker).await;
    assert_eq!(ack_ids(&acks), ["m1-s2", "m2-s2"]);
}

#[tokio::test]
async fn reused_ids_ack_each_message_once() {
    // The ids collide across sessions, so a stale copy's ack cannot be told
    // from the redelivered copy's. Each message is still acked exactly once.
    let (addr, broker) = broker(Ids::Reused, Redelivery::Prompt);
    let (conn, mut sub) = reconnect_holding_two(&addr, AckMode::ClientIndividual).await;

    for frame in four_copies(&mut sub).await {
        sub.ack_frame(&frame).await.unwrap();
    }

    let acks = finish(conn, broker).await;
    assert_eq!(ack_ids(&acks), ["m1", "m2"]);
}

#[tokio::test]
async fn a_batched_cumulative_ack_after_reconnect_is_sent() {
    // `client` mode, acking only the last message read. With reused ids the
    // last copy's id is also a retired copy's id; it must still be sent, or
    // the redelivered messages stay unacknowledged.
    let (addr, broker) = broker(Ids::Reused, Redelivery::Prompt);
    let (conn, mut sub) = reconnect_holding_two(&addr, AckMode::Client).await;

    let frames = four_copies(&mut sub).await;
    sub.ack_frame(&frames[3]).await.unwrap();

    let acks = finish(conn, broker).await;
    assert_eq!(ack_ids(&acks), ["m2"]);
}

#[tokio::test]
async fn cumulative_acks_for_every_copy_ack_each_message_once() {
    // `client` mode, acking every copy, with reused ids. Acking a stale copy
    // settles the redelivered one, which must leave the retired entries in
    // place, or the later acks find nothing and go out a second time.
    let (addr, broker) = broker(Ids::Reused, Redelivery::Prompt);
    let (conn, mut sub) = reconnect_holding_two(&addr, AckMode::Client).await;

    for frame in four_copies(&mut sub).await {
        sub.ack_frame(&frame).await.unwrap();
    }

    let acks = finish(conn, broker).await;
    assert_eq!(ack_ids(&acks), ["m1", "m2"]);
}

#[tokio::test]
async fn an_ack_issued_while_reconnecting_is_not_sent_on_the_new_session() {
    // The broker holds the second handshake for a second, and the stale m1
    // is acked inside that window. Its entry is not retired yet, so the ACK
    // is queued; it must be dropped, not written to the new session.
    let (addr, broker) = broker(
        Ids::PerSession,
        Redelivery::AfterHandshakeDelay(Duration::from_secs(1)),
    );
    let (conn, mut sub) = lose_connection(&addr, AckMode::ClientIndividual).await;
    let stale_m1 = next_copy(&mut sub).await;
    sub.ack_frame(&stale_m1).await.unwrap();

    await_new_session(&conn).await;
    // Stale m2, then the redelivered m1 and m2.
    for _ in 0..3 {
        let frame = next_copy(&mut sub).await;
        sub.ack_frame(&frame).await.unwrap();
    }

    let acks = finish(conn, broker).await;
    assert_eq!(ack_ids(&acks), ["m1-s2", "m2-s2"]);
}

#[tokio::test]
async fn an_ack_while_reconnecting_keeps_the_marker_for_the_redelivered_copy() {
    // `client` mode with reused ids. Stale m1 is acked while the client is
    // reconnecting; stale m2's ack then settles both redelivered copies
    // cumulatively. The redelivered copies' own acks must find m1's and m2's
    // markers, not fall through and ack m1 a second time.
    let (addr, broker) = broker(
        Ids::Reused,
        Redelivery::AfterHandshakeDelay(Duration::from_secs(1)),
    );
    let (conn, mut sub) = lose_connection(&addr, AckMode::Client).await;
    let stale_m1 = next_copy(&mut sub).await;
    sub.ack_frame(&stale_m1).await.unwrap();

    await_new_session(&conn).await;
    // Stale m2, then the redelivered m1 and m2.
    for _ in 0..3 {
        let frame = next_copy(&mut sub).await;
        sub.ack_frame(&frame).await.unwrap();
    }

    let acks = finish(conn, broker).await;
    assert_eq!(ack_ids(&acks), ["m2"]);
}

#[tokio::test]
async fn a_partial_redelivery_still_acks_each_message_once() {
    // `client` mode with reused ids, and m2 redelivered only after both stale
    // copies have been acked. Stale m1's ack settles the redelivered m1;
    // stale m2's finds only its marker, which must not take m1's with it.
    let (addr, broker) = broker(Ids::Reused, Redelivery::Staged);
    let (conn, mut sub) = reconnect_holding_two(&addr, AckMode::Client).await;

    for _ in 0..2 {
        let stale = next_copy(&mut sub).await;
        sub.ack_frame(&stale).await.unwrap();
    }
    conn.send_frame(Frame::new("SEND").header("destination", "/control/more"))
        .await
        .unwrap();
    for _ in 0..2 {
        let redelivered = next_copy(&mut sub).await;
        sub.ack_frame(&redelivered).await.unwrap();
    }

    let acks = finish(conn, broker).await;
    assert_eq!(ack_ids(&acks), ["m1", "m2"]);
}
