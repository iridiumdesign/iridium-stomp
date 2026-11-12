//! Liveness against a broker that misbehaves at the transport level: one that
//! goes silent, stops reading, or accepts a connection and never answers.
//! Each test drives the client against a mock broker on a local socket.

use iridium_stomp::{ConnError, ConnectOptions, Connection, Frame};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Read the client's CONNECT and answer CONNECTED with `heart_beat`.
async fn handshake(sock: &mut TcpStream, heart_beat: &str) {
    let mut buf = vec![0u8; 4096];
    let mut got = Vec::new();
    while !got.contains(&0) {
        let n = sock.read(&mut buf).await.expect("read CONNECT");
        assert!(n > 0, "client hung up before sending CONNECT");
        got.extend_from_slice(&buf[..n]);
    }
    let connected = format!("CONNECTED\nversion:1.2\nheart-beat:{heart_beat}\n\n\0");
    sock.write_all(connected.as_bytes())
        .await
        .expect("write CONNECTED");
}

/// Read and discard until the client hangs up; say how long that took.
async fn time_to_hangup(sock: &mut TcpStream) -> Duration {
    let start = Instant::now();
    let mut buf = vec![0u8; 65536];
    loop {
        match sock.read(&mut buf).await {
            Ok(0) | Err(_) => return start.elapsed(),
            Ok(_) => {}
        }
    }
}

/// #132: the receive watchdog has to fire while the client is busy writing.
/// It used to be restarted by every pass through the I/O loop, so a client
/// that sent more often than half the receive interval never noticed a broker
/// that had gone silent.
#[tokio::test]
async fn a_silent_broker_is_noticed_by_a_client_that_keeps_writing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        // Promise a heartbeat every 200ms, then never send one.
        handshake(&mut sock, "200,0").await;
        time_to_hangup(&mut sock).await
    });

    let conn = Connection::connect(&addr, "u", "p", "0,200").await.unwrap();
    let writer = conn.clone();
    tokio::spawn(async move {
        loop {
            let _ = writer.send("/queue/x", "hi").await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    // Two receive intervals is 400ms. Allow a wide margin for a loaded CI
    // runner; before the fix this never happened at all.
    let noticed = tokio::time::timeout(Duration::from_secs(3), broker)
        .await
        .expect("client never dropped the silent broker")
        .unwrap();
    assert!(
        noticed >= Duration::from_millis(350),
        "dropped too early: {noticed:?}"
    );
}

/// A write that is slow but succeeds must not cost a healthy connection.
/// Nothing is read while a write is in progress, so a broker that is late to
/// read a large frame can have its heartbeats sit unread past the receive
/// deadline. The time spent writing is not held against it.
#[tokio::test]
async fn a_slow_write_to_a_live_broker_keeps_the_connection() {
    const FRAME: usize = 8 << 20;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        handshake(&mut sock, "200,0").await;
        let (mut reader, mut writer) = sock.into_split();
        // A live broker: heartbeats well inside the 200ms it promised.
        let beating = tokio::spawn(async move {
            while writer.write_all(b"\n").await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        // But slow to read: leave the frame stuck in the socket for well over
        // the client's 400ms receive deadline before taking it.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let mut buf = vec![0u8; 65536];
        let mut total = 0;
        while total < FRAME {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => total += n,
            }
        }
        // Stay up a while longer: the connection has to survive the moment
        // the write completes, not only reach it.
        let hung_up = tokio::time::timeout(Duration::from_millis(800), async {
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => total += n,
                }
            }
        })
        .await
        .is_ok();
        beating.abort();
        (total, hung_up)
    });

    let conn = Connection::connect(&addr, "u", "p", "0,200").await.unwrap();
    let frame = Frame::new("SEND")
        .header("destination", "/queue/x")
        .set_body(vec![b'x'; FRAME]);
    conn.send_frame(frame).await.unwrap();

    let (total, hung_up) = broker.await.unwrap();
    assert!(
        total >= FRAME,
        "broker read only {total} bytes of the frame"
    );
    assert!(!hung_up, "client dropped a broker that was heartbeating");
}

/// The other side of the test above: slow writes must not hide a broker that
/// has gone silent. It reads, slowly, so every write succeeds, and sends
/// nothing. The receive deadline is not stretched by the time writes take.
#[tokio::test]
async fn slow_writes_do_not_hide_a_silent_broker() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        // Promise a heartbeat every 200ms, then never send one.
        handshake(&mut sock, "200,0").await;
        let start = Instant::now();
        // About 1 MiB every 100ms: each of the client's frames takes that
        // long to write, and none comes near the write timeout.
        let mut buf = vec![0u8; 64 << 10];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => return start.elapsed(),
                Ok(_) => tokio::time::sleep(Duration::from_millis(6)).await,
            }
        }
    });

    let conn = Connection::connect(&addr, "u", "p", "0,200").await.unwrap();
    let flood = conn.clone();
    tokio::spawn(async move {
        loop {
            let frame = Frame::new("SEND")
                .header("destination", "/queue/x")
                .set_body(vec![b'x'; 1 << 20]);
            if flood.send_frame(frame).await.is_err() {
                break;
            }
        }
    });

    // Two receive intervals is 400ms, plus the write in progress when it
    // passes. Ten seconds of writes like these used to go by unnoticed.
    tokio::time::timeout(Duration::from_secs(4), broker)
        .await
        .expect("client never dropped the silent broker")
        .unwrap();
}

/// #135: a heartbeat is due one interval after the last write, whatever that
/// write was. On a fixed tick, a data frame sent mid-interval made the next
/// tick skip, and the gap could approach twice the negotiated interval.
#[tokio::test]
async fn writes_are_never_further_apart_than_the_send_interval() {
    const INTERVAL: Duration = Duration::from_millis(200);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        handshake(&mut sock, "0,200").await;
        let mut buf = vec![0u8; 65536];
        let mut last = Instant::now();
        let mut longest = Duration::ZERO;
        let end = Instant::now() + Duration::from_secs(3);
        while Instant::now() < end {
            match tokio::time::timeout(Duration::from_millis(20), sock.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    longest = longest.max(last.elapsed());
                    last = Instant::now();
                }
                Ok(_) => break,
                Err(_) => longest = longest.max(last.elapsed()),
            }
        }
        longest
    });

    let conn = Connection::connect(&addr, "u", "p", "200,0").await.unwrap();
    // A data frame every 300ms lands in the middle of every other interval,
    // which is what stretched the gap on the old fixed tick.
    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        conn.send("/queue/x", "hi").await.unwrap();
    }

    let longest = broker.await.unwrap();
    // The old code gave 300ms here. Leave room for scheduling jitter and the
    // 20ms the broker's own read loop can add.
    assert!(
        longest < INTERVAL + Duration::from_millis(70),
        "client was quiet for {longest:?} on a {INTERVAL:?} heartbeat"
    );
}

/// #133: a broker that stops reading must not be able to hang `close()`. The
/// I/O loop used to sit in the stalled write, deaf to shutdown, and `close()`
/// waited without limit for room in the outbound queue behind it.
#[tokio::test]
async fn close_returns_when_the_broker_has_stopped_reading() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        handshake(&mut sock, "0,0").await;
        // Hold the socket open and never read from it again.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        drop(sock);
    });

    let options = ConnectOptions::default().disconnect_timeout(Duration::from_millis(500));
    let conn = Connection::connect_with_options(&addr, "u", "p", "0,0", options)
        .await
        .unwrap();

    // Fill the socket buffer, then the outbound queue behind it.
    let flood = conn.clone();
    let flooder = tokio::spawn(async move {
        loop {
            let frame = Frame::new("SEND")
                .header("destination", "/queue/x")
                .set_body(vec![b'x'; 1 << 20]);
            if flood.send_frame(frame).await.is_err() {
                break;
            }
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    let closed = tokio::time::timeout(Duration::from_secs(3), conn.close())
        .await
        .expect("close() hung against a broker that stopped reading");
    assert!(closed.is_err(), "nothing confirmed this DISCONNECT");

    // The background task has gone too: with nothing left to drain the
    // outbound queue, the flooding sender is refused.
    tokio::time::timeout(Duration::from_secs(3), flooder)
        .await
        .expect("background task outlived close()")
        .unwrap();
}

/// #133: a write that makes no progress within the write timeout ends the
/// connection, and the client reconnects, with no `close()` to prompt it.
#[tokio::test]
async fn a_stalled_write_drops_the_connection_and_reconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        handshake(&mut first, "0,0").await;
        // Never read from the first connection again; wait for a second.
        let (mut second, _) = listener.accept().await.unwrap();
        handshake(&mut second, "0,0").await;
        drop(first);
    });

    let options = ConnectOptions::default().write_timeout(Duration::from_millis(300));
    let conn = Connection::connect_with_options(&addr, "u", "p", "0,0", options)
        .await
        .unwrap();
    let flood = conn.clone();
    tokio::spawn(async move {
        loop {
            let frame = Frame::new("SEND")
                .header("destination", "/queue/x")
                .set_body(vec![b'x'; 1 << 20]);
            if flood.send_frame(frame).await.is_err() {
                break;
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(10), broker)
        .await
        .expect("client never gave up on the stalled connection")
        .unwrap();
}

/// #142: when the client gives up on a stalled write it hangs up at once.
/// The socket used to stay open until the reconnect backoff was over, and
/// for that long the broker kept the old session, its unacknowledged
/// messages and its subscriptions.
#[tokio::test]
async fn a_stalled_connection_is_closed_before_the_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        handshake(&mut sock, "0,0").await;
        let start = Instant::now();
        // Read nothing until the client's 300ms write timeout has passed,
        // then take what is in the socket and see how soon the end comes.
        tokio::time::sleep(Duration::from_millis(700)).await;
        let mut buf = vec![0u8; 65536];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => return start.elapsed(),
                Ok(_) => {}
            }
        }
    });

    let options = ConnectOptions::default().write_timeout(Duration::from_millis(300));
    let conn = Connection::connect_with_options(&addr, "u", "p", "0,0", options)
        .await
        .unwrap();
    let flood = conn.clone();
    tokio::spawn(async move {
        loop {
            let frame = Frame::new("SEND")
                .header("destination", "/queue/x")
                .set_body(vec![b'x'; 1 << 20]);
            if flood.send_frame(frame).await.is_err() {
                break;
            }
        }
    });

    // The first backoff after a connection this short is two seconds, so the
    // old behaviour put the hang-up at about 2.3s.
    let hung_up = tokio::time::timeout(Duration::from_secs(10), broker)
        .await
        .expect("client never hung up on the stalled connection")
        .unwrap();
    assert!(
        hung_up < Duration::from_millis(1500),
        "the old socket stayed open for {hung_up:?}"
    );
}

/// A broker that drops the first connection, then accepts the second and
/// never answers its CONNECT. Reports each further connection it accepts.
async fn broker_that_stops_answering() -> (String, tokio::sync::mpsc::Receiver<TcpStream>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        handshake(&mut first, "0,0").await;
        drop(first);
        while let Ok((sock, _)) = listener.accept().await {
            if tx.send(sock).await.is_err() {
                break;
            }
        }
    });
    (addr, rx)
}

/// #134: a reconnect attempt that gets no CONNECTED is given up after the
/// handshake timeout and tried again. It used to wait for good.
#[tokio::test]
async fn a_reconnect_that_gets_no_answer_is_retried() {
    let (addr, mut accepted) = broker_that_stops_answering().await;
    let options = ConnectOptions::default().handshake_timeout(Duration::from_millis(300));
    let _conn = Connection::connect_with_options(&addr, "u", "p", "0,0", options)
        .await
        .unwrap();

    let mut unanswered = accepted.recv().await.expect("first reconnect attempt");
    // The client abandons the attempt...
    let gave_up = tokio::time::timeout(Duration::from_secs(3), time_to_hangup(&mut unanswered))
        .await
        .expect("client never abandoned the unanswered handshake");
    assert!(
        gave_up >= Duration::from_millis(250),
        "gave up after {gave_up:?}"
    );
    // ...and, after its backoff, makes another.
    tokio::time::timeout(Duration::from_secs(10), accepted.recv())
        .await
        .expect("client never tried again")
        .expect("another attempt");
}

/// #134: `close()` during an unanswered reconnect handshake stops the
/// background task and releases the socket. The shutdown signal used to be
/// looked at only between attempts.
#[tokio::test]
async fn close_interrupts_an_unanswered_reconnect() {
    let (addr, mut accepted) = broker_that_stops_answering().await;
    let options = ConnectOptions::default().disconnect_timeout(Duration::from_millis(300));
    let conn = Connection::connect_with_options(&addr, "u", "p", "0,0", options)
        .await
        .unwrap();

    // The default handshake timeout is 30s, so only close() can end this.
    let mut unanswered = accepted.recv().await.expect("reconnect attempt");
    let closed = conn.close().await;
    assert!(
        matches!(closed, Err(ConnError::ReceiptTimeout(_))),
        "{closed:?}"
    );

    tokio::time::timeout(Duration::from_secs(3), time_to_hangup(&mut unanswered))
        .await
        .expect("socket still open after close()");
}

/// A first connection that gets no answer is bounded per attempt too, and
/// reports what happened once `connect_timeout` runs out.
#[tokio::test]
async fn a_first_connection_that_gets_no_answer_times_out() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = listener.accept().await {
            held.push(sock);
        }
    });

    let options = ConnectOptions::default()
        .handshake_timeout(Duration::from_millis(200))
        .connect_timeout(Duration::from_secs(2));
    let err = Connection::connect_with_options(&addr, "u", "p", "0,0", options)
        .await
        .err()
        .expect("nothing answered the CONNECT");
    match err {
        ConnError::Io(e) => {
            assert_eq!(e.kind(), std::io::ErrorKind::TimedOut);
            assert!(e.to_string().contains("no CONNECTED"), "{e}");
        }
        other => panic!("expected the handshake timeout, got {other:?}"),
    }
}
