//! The bind handshake must frame the byte stream, not assume one read is one PDU.
//!
//! Both sides read the handshake with a single `read()` and passed the whole
//! thing to `CommandHeader::decode`:
//!
//! ```ignore
//! let n = stream.read(&mut buffer).await?;
//! let pdu = buffer[0..n].to_vec();   // assumes exactly one PDU arrived
//! CommandHeader::decode(&pdu)        // rejects it if len != command_length
//! ```
//!
//! TCP is a byte stream, so a peer that puts anything on the wire straight after
//! its bind PDU — an `enquire_link`, a queued `deliver_sm`, a pipelined
//! `submit_sm` — can have it coalesce into the same segment. The decode then
//! fails with `PDU length N does not match command_length M`, the session is torn
//! down, and every subsequent request on it fails instantly.
//!
//! This is not exotic: an SMSC with traffic waiting sends it the moment the bind
//! is accepted. It showed up as a ~1-in-20 flake in the correlation load test
//! under two-CPU pinning, where the SMSC's first `deliver_sm` caught up with its
//! own `bind_transceiver_resp`. The load test only made the coalescing likely;
//! this test makes it certain by writing both PDUs in one `write_all`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::{
    bind_transceiver, enquire_link, submit_sm, submit_sm_resp, CommandHeader,
    SmppConnectionInformation, SmppServer, SmppServerListener,
};
use std::net::IpAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::{oneshot, Mutex};

struct BindWatcher {
    bound: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl SmppClientListener for BindWatcher {
    async fn on_smsc_bound(&self, _smsc: SMSC, _session_id: &String) {
        if let Some(tx) = self.bound.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

/// A raw SMSC that accepts the bind and, in the SAME write, appends one more
/// PDU — exactly what a real SMSC with queued traffic does.
async fn coalescing_smsc(listener: TcpListener, resp_tx: oneshot::Sender<()>) {
    let (mut stream, _) = listener.accept().await.expect("accept");

    // Read the client's bind_transceiver, framing it properly ourselves.
    let mut buf = Vec::new();
    let pdu = loop {
        let mut chunk = [0u8; 512];
        let n = stream.read(&mut chunk).await.expect("read bind");
        assert!(n > 0, "client closed before binding");
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() >= 16 {
            let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if buf.len() >= len {
                break buf[..len].to_vec();
            }
        }
    };

    let header = CommandHeader::decode(&pdu).expect("bind header");
    let bind = bind_transceiver::decode(header, &pdu).expect("bind_transceiver");
    let resp = bind.accept("COALESCE".to_string(), Some(0x34));

    // The whole point: bind response and the next PDU in ONE segment.
    let mut wire = resp.encode();
    wire.extend_from_slice(&enquire_link::new(1).encode());
    stream.write_all(&wire).await.expect("write");

    // The client must answer the enquire_link we tucked in behind the bind
    // response. That is what proves the coalesced PDU was framed and handled,
    // rather than merely not having broken the bind.
    let mut seen = Vec::new();
    loop {
        let mut chunk = [0u8; 512];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                seen.extend_from_slice(&chunk[..n]);
                // enquire_link_resp = 0x80000015
                if seen.windows(4).any(|w| w == 0x8000_0015u32.to_be_bytes()) {
                    let _ = resp_tx.send(());
                    break;
                }
            }
        }
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn a_pdu_coalesced_with_the_bind_response_does_not_break_the_bind() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    // resp_tx is handed in below, after the client is constructed.

    let (resp_tx, resp_rx) = oneshot::channel();
    tokio::spawn(coalescing_smsc(listener, resp_tx));
    let (tx, rx) = oneshot::channel();
    let mut client = SmppClient::new(
        "127.0.0.1".to_string(),
        port,
        false,
        BIND_TYPE::TRX,
        "u".to_string(),
        "p".to_string(),
        String::new(),
        1,
        1,
        String::new(),
        Arc::new(BindWatcher {
            bound: Mutex::new(Some(tx)),
        }),
        16,
    );
    client.start().await;

    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect(
            "bind never completed: the bind response was coalesced with the PDU behind it, \
             so the handshake read saw two PDUs and rejected the whole buffer",
        )
        .expect("bind channel dropped");

    tokio::time::timeout(Duration::from_secs(5), resp_rx)
        .await
        .expect(
            "the client never answered the enquire_link that shared a segment with the bind \
             response, so the coalesced PDU was dropped rather than framed",
        )
        .expect("resp channel dropped");

    client.stop().await;
}

// ── The same bug on the SMSC side ───────────────────────────────────────────

/// An ESME that pipelines its first `submit_sm` without waiting for
/// `bind_transceiver_resp` — legal, and what a high-throughput ESME does — put
/// both PDUs in one segment and the SMSC rejected the whole read.
struct CountingSmsc {
    submits: mpsc::Sender<()>,
}

#[async_trait]
impl SmppServerListener for CountingSmsc {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> smpp34::bind_transceiver_resp {
        req.accept("PIPELINE".to_string(), Some(0x34))
    }

    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> submit_sm_resp {
        let _ = self.submits.try_send(());
        req.accept("m-1".to_string())
    }
}

#[tokio::test]
async fn an_esme_pipelining_behind_its_bind_is_not_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);

    let (tx, mut rx) = mpsc::channel(4);
    let mut server = SmppServer::new(
        IpAddr::from([127, 0, 0, 1]),
        port,
        Arc::new(CountingSmsc { submits: tx }),
    );
    server.start().await;

    // Raw ESME: bind_transceiver and submit_sm in a single write.
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    // Hand-built so the test depends on the wire format, not on our encoder.
    // SMPP 3.4 §4.1.5: system_id, password, system_type as C-strings, then
    // interface_version, addr_ton, addr_npi and address_range.
    let mut body = Vec::new();
    body.extend_from_slice(b"u\0");
    body.extend_from_slice(b"p\0");
    body.push(0); // system_type: empty C-string
    body.push(0x34); // interface_version
    body.push(1); // addr_ton
    body.push(1); // addr_npi
    body.push(0); // address_range: empty C-string
    let mut bind = Vec::new();
    bind.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
    bind.extend_from_slice(&0x0000_0009u32.to_be_bytes()); // bind_transceiver
    bind.extend_from_slice(&0u32.to_be_bytes()); // command_status
    bind.extend_from_slice(&1u32.to_be_bytes()); // sequence_number
    bind.extend_from_slice(&body);
    let submit = submit_sm::new(
        2,
        String::new(),
        1,
        1,
        "15550100".to_string(),
        1,
        1,
        "15550199".to_string(),
        0,
        0,
        0,
        String::new(),
        String::new(),
        0,
        0,
        0,
        0,
        b"pipelined".to_vec(),
    );
    let mut wire = bind;
    wire.extend_from_slice(&submit.encode());
    sock.write_all(&wire).await.expect("write");

    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect(
            "the pipelined submit_sm never reached the SMSC: it shared a segment with the \
             bind request, so the handshake read saw two PDUs and rejected the buffer",
        )
        .expect("submit channel closed");

    server.stop().await;
}
