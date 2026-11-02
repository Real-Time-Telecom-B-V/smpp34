//! A refused bind_transmitter / bind_receiver must be answered with its _resp.
//!
//! SMPP 3.4 §4.1.2 / §4.1.4: every bind request gets its response, with
//! command_status carrying the error when the bind is refused. The transceiver
//! path always did this; transmitter and receiver returned the listener's error
//! without writing anything and dropped the connection, so the ESME saw a bare
//! EOF and never learned why.
//!
//! This file is its own test binary because it installs a capturing `log`
//! logger, which is process-global.

use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use smpp34::{
    bind_receiver, bind_receiver_resp, bind_transmitter, bind_transmitter_resp,
    SmppConnectionInformation, SmppError, SmppServer, SmppServerListener,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

// ── Log capture ─────────────────────────────────────────────────────────────

struct Capture {
    errors: Mutex<Vec<String>>,
}

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Error
    }
    fn log(&self, record: &log::Record) {
        if record.level() == log::Level::Error {
            if let Ok(mut errors) = self.errors.lock() {
                errors.push(record.args().to_string());
            }
        }
    }
    fn flush(&self) {}
}

fn capture() -> &'static Capture {
    static CAPTURE: OnceLock<&'static Capture> = OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture: &'static Capture = Box::leak(Box::new(Capture {
            errors: Mutex::new(Vec::new()),
        }));
        log::set_logger(capture).expect("logger already set in this test binary");
        log::set_max_level(log::LevelFilter::Error);
        capture
    })
}

/// Error records that mention this client address. Tests in this binary run in
/// parallel, so the address is what keeps them apart.
fn errors_for(client_address: &str) -> Vec<String> {
    capture()
        .errors
        .lock()
        .expect("capture lock")
        .iter()
        .filter(|line| line.contains(client_address))
        .cloned()
        .collect()
}

// ── Listener ────────────────────────────────────────────────────────────────

struct RejectingSmsc {
    status: SmppError,
    /// When set, the listener holds its answer until this is notified, so the
    /// test can make the peer disappear first.
    hold: Option<Arc<Notify>>,
}

impl RejectingSmsc {
    async fn wait(&self) {
        if let Some(hold) = &self.hold {
            hold.notified().await;
        }
    }
}

#[async_trait]
impl SmppServerListener for RejectingSmsc {
    async fn on_bind_transmitter(
        &self,
        req: bind_transmitter,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transmitter_resp {
        self.wait().await;
        req.reject(self.status)
    }

    async fn on_bind_receiver(
        &self,
        req: bind_receiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_receiver_resp {
        self.wait().await;
        req.reject(self.status)
    }
}

async fn start_server(listener: RejectingSmsc) -> (SmppServer, u16) {
    let probe = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);
    let mut server = SmppServer::new(IpAddr::from([127, 0, 0, 1]), port, Arc::new(listener));
    server.start().await;
    (server, port)
}

/// Hand-built so the test depends on the wire format, not on our encoder.
/// SMPP 3.4 §4.1.1 / §4.1.3: system_id, password, system_type as C-strings,
/// then interface_version, addr_ton, addr_npi and address_range.
fn bind_request(command_id: u32, sequence_number: u32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"u\0");
    body.extend_from_slice(b"p\0");
    body.push(0); // system_type: empty C-string
    body.push(0x34); // interface_version
    body.push(1); // addr_ton
    body.push(1); // addr_npi
    body.push(0); // address_range: empty C-string
    let mut pdu = Vec::new();
    pdu.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
    pdu.extend_from_slice(&command_id.to_be_bytes());
    pdu.extend_from_slice(&0u32.to_be_bytes());
    pdu.extend_from_slice(&sequence_number.to_be_bytes());
    pdu.extend_from_slice(&body);
    pdu
}

fn word(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

/// Send a bind, read everything up to EOF, and check it is exactly one
/// header-only _resp carrying `status` and the request's sequence_number.
async fn assert_refused_with_resp(
    request_id: u32,
    response_id: u32,
    status: SmppError,
    sequence_number: u32,
) {
    let (mut server, port) = start_server(RejectingSmsc { status, hold: None }).await;

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(&bind_request(request_id, sequence_number))
        .await
        .expect("write bind");

    let mut wire = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut wire))
        .await
        .expect("the SMSC neither answered nor closed the connection")
        .expect("read");

    assert_eq!(
        wire.len(),
        16,
        "expected exactly one header-only response then EOF, got {} bytes: {:02x?}",
        wire.len(),
        wire
    );
    assert_eq!(
        word(&wire, 0),
        16,
        "command_length (no body on error, §4.1.2)"
    );
    assert_eq!(word(&wire, 4), response_id, "command_id");
    assert_eq!(word(&wire, 8), status as u32, "command_status");
    assert_eq!(word(&wire, 12), sequence_number, "sequence_number");

    server.stop().await;
}

#[tokio::test]
async fn a_refused_bind_transmitter_gets_its_resp_before_the_close() {
    assert_refused_with_resp(
        0x0000_0002,
        0x8000_0002,
        SmppError::ESME_RBINDFAIL,
        0x0000_1234,
    )
    .await;
}

#[tokio::test]
async fn a_refused_bind_receiver_gets_its_resp_with_the_listeners_status() {
    assert_refused_with_resp(
        0x0000_0001,
        0x8000_0001,
        SmppError::ESME_RINVSYSID,
        0x0000_4321,
    )
    .await;
}

/// The peer sends its bind and resets the connection before the listener has
/// answered. Writing the rejection then fails; that must be one error log, not a
/// panic, and the server must keep serving.
async fn assert_rejection_to_a_gone_peer_logs_once(request_id: u32, resp_name: &str) {
    capture();
    let hold = Arc::new(Notify::new());
    let (mut server, port) = start_server(RejectingSmsc {
        status: SmppError::ESME_RBINDFAIL,
        hold: Some(hold.clone()),
    })
    .await;

    let sock = {
        let mut sock = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        sock.write_all(&bind_request(request_id, 7))
            .await
            .expect("write bind");
        sock
    };
    let client_address = sock.local_addr().expect("local addr").to_string();
    // Zero linger turns the close into an RST, so the server's write fails
    // deterministically instead of landing in a send buffer nobody reads.
    sock.set_zero_linger().expect("linger");
    drop(sock);
    hold.notify_one();

    let marker = format!("could not send the {resp_name} rejection");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while errors_for(&client_address)
        .iter()
        .all(|line| !line.contains(&marker))
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Give a duplicate a chance to show up before counting.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let failures: Vec<String> = errors_for(&client_address)
        .into_iter()
        .filter(|line| line.contains(&marker))
        .collect();
    assert_eq!(
        failures.len(),
        1,
        "expected one '{marker}' error for {client_address}, got {failures:?}"
    );
    assert!(
        failures[0].contains(&format!("127.0.0.1:{port}")),
        "the error must name the server address too: {}",
        failures[0]
    );

    // The server survived: a fresh bind on it is still answered.
    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("server stopped accepting after the failed write");
    sock.write_all(&bind_request(request_id, 8))
        .await
        .expect("write bind");
    hold.notify_one();
    let mut wire = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut wire))
        .await
        .expect("second bind was never answered")
        .expect("read");
    assert_eq!(wire.len(), 16, "second bind got {wire:02x?}");

    server.stop().await;
}

#[tokio::test]
async fn a_bind_transmitter_rejection_to_a_gone_peer_logs_once_and_does_not_panic() {
    assert_rejection_to_a_gone_peer_logs_once(0x0000_0002, "bind_transmitter").await;
}

#[tokio::test]
async fn a_bind_receiver_rejection_to_a_gone_peer_logs_once_and_does_not_panic() {
    assert_rejection_to_a_gone_peer_logs_once(0x0000_0001, "bind_receiver").await;
}
