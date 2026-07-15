use bytes::BytesMut;
use iridium_stomp::codec::{StompCodec, StompItem};
use tokio_util::codec::Decoder;

// Regression test using the minimized failing chunk sequence discovered by the
// reducer. This test currently fails (decoder decodes fewer frames than the
// number of NUL terminators); we'll iterate on `src/codec.rs` until it passes.
#[test]
fn minimized_replay_should_decode_all_frames() {
    let chunks: Vec<&[u8]> = vec![
        &[
            0x53, 0x45, 0x4e, 0x44, 0x0a, 0x0a, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65,
        ],
        &[0x2d, 0x6d],
        &[0x72, 0x2d, 0x32, 0x2d, 0x6d, 0x73, 0x67, 0x2d, 0x30, 0x00],
        &[0x73, 0x67, 0x2d, 0x31, 0x00],
    ];

    let mut combined: Vec<u8> = Vec::new();
    for c in &chunks {
        combined.extend_from_slice(c);
    }
    let expected = combined.iter().filter(|&&b| b == 0).count();

    let mut dec = StompCodec::new();
    let mut buf = BytesMut::new();
    let mut decoded = 0usize;

    let mut bodies: Vec<Vec<u8>> = Vec::new();
    for c in chunks {
        buf.extend_from_slice(c);
        loop {
            eprintln!("calling decode: buf len={} -> {:02x?}", buf.len(), buf);
            match dec.decode(&mut buf) {
                Ok(Some(StompItem::Frame(f))) => {
                    eprintln!("decoded frame, remaining buf len={}", buf.len());
                    decoded += 1;
                    bodies.push(f.body);
                }
                Ok(Some(StompItem::Heartbeat)) => {
                    eprintln!("decoded heartbeat");
                }
                Ok(None) => {
                    eprintln!("decode returned None (need more bytes)");
                    break;
                }
                Err(e) => panic!("decoder returned error on replayed chunks: {}", e),
            }
        }
    }

    // drain remaining
    loop {
        match dec.decode(&mut buf) {
            Ok(Some(StompItem::Frame(_))) => decoded += 1,
            Ok(Some(StompItem::Heartbeat)) => {}
            Ok(None) => break,
            Err(e) => panic!("decoder returned error during drain: {}", e),
        }
    }

    if decoded != expected {
        eprintln!("combined (len={}): {:02x?}", combined.len(), combined);
        let nul_positions: Vec<usize> = combined
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| if b == 0 { Some(i) } else { None })
            .collect();
        eprintln!("nul positions: {:?}", nul_positions);
        eprintln!(
            "decoded bodies ({}): {:?}",
            bodies.len(),
            bodies
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
        );
        eprintln!(
            "remaining buf after drain (len={}): {:02x?}",
            buf.len(),
            buf
        );
    }
    assert_eq!(
        decoded, expected,
        "decoder must decode all frames in minimized sequence"
    );
}
