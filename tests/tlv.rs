//! Optional parameter (TLV) send-path test.
//!
//! Decoding TLVs off the wire has always worked (`request.tlvs`), but nothing
//! could put one on: the message constructors hardcoded an empty TLV list and no
//! `send_*` method or builder took any. This drives both directions end to end —
//! ESME -> SMSC (`submit_sm`, `data_sm`) and SMSC -> ESME (`deliver_sm`) — and
//! asserts the exact tag/value bytes survive the round trip.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::server::ESME;
use smpp34::{
    bind_transceiver, bind_transceiver_resp, data_sm, data_sm_resp, deliver_sm, deliver_sm_resp,
    submit_sm, submit_sm_resp, SmppConnectionInformation, SmppError, SmppServer,
    SmppServerListener, Tlv, TlvList, TlvTag,
};
use tokio::sync::{mpsc, oneshot, Mutex};

/// A vendor-specific tag outside the SMPP 3.4 table — must survive untouched.
const VENDOR_TAG: u16 = 0x1403;

// ── Harness ─────────────────────────────────────────────────────────────────

/// SMSC side: accepts binds, forwards the TLVs of every inbound message PDU to
/// the test, and hands the bound `ESME` back so the test can deliver to it.
struct TlvSmsc {
    esmes: mpsc::Sender<Arc<ESME>>,
    submit_tlvs: mpsc::Sender<Vec<Tlv>>,
    data_tlvs: mpsc::Sender<Vec<Tlv>>,
}

#[async_trait]
impl SmppServerListener for TlvSmsc {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transceiver_resp {
        req.accept("TLV-SMSC".to_string(), Some(0x34))
    }

    async fn on_esme_bound(&self, esme: ESME, _s: &String) {
        let _ = self.esmes.send(Arc::new(esme)).await;
    }

    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> submit_sm_resp {
        let _ = self.submit_tlvs.send(req.tlvs.clone()).await;
        req.accept("msg-1".to_string())
    }

    async fn on_data_sm(
        &self,
        req: data_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> data_sm_resp {
        let _ = self.data_tlvs.send(req.tlvs.clone()).await;
        req.accept("msg-2".to_string())
    }
}

/// ESME side: hands the bound session back and forwards inbound `deliver_sm` /
/// `data_sm` TLVs to the test.
struct TlvEsme {
    bound: Mutex<Option<oneshot::Sender<Arc<SMSC>>>>,
    deliver_tlvs: mpsc::Sender<Vec<Tlv>>,
    data_tlvs: mpsc::Sender<Vec<Tlv>>,
}

#[async_trait]
impl SmppClientListener for TlvEsme {
    async fn on_smsc_bound(&self, smsc: SMSC, _s: &String) {
        if let Some(tx) = self.bound.lock().await.take() {
            let _ = tx.send(Arc::new(smsc));
        }
    }

    async fn on_deliver_sm(
        &self,
        req: deliver_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> deliver_sm_resp {
        let _ = self.deliver_tlvs.send(req.tlvs.clone()).await;
        req.accept()
    }

    async fn on_data_sm(
        &self,
        req: data_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> data_sm_resp {
        let _ = self.data_tlvs.send(req.tlvs.clone()).await;
        req.accept("msg-3".to_string())
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Session {
    smsc: Arc<SMSC>,
    esme: Arc<ESME>,
    submit_tlvs: mpsc::Receiver<Vec<Tlv>>,
    deliver_tlvs: mpsc::Receiver<Vec<Tlv>>,
    smsc_data_tlvs: mpsc::Receiver<Vec<Tlv>>,
    esme_data_tlvs: mpsc::Receiver<Vec<Tlv>>,
    _server: SmppServer,
    _client: SmppClient,
}

/// Bring up an SMSC + a bound TRX ESME on loopback.
async fn bound_session() -> Session {
    let port = free_port();

    let (esme_tx, mut esme_rx) = mpsc::channel(4);
    let (submit_tx, submit_tlvs) = mpsc::channel(16);
    let (smsc_data_tx, smsc_data_tlvs) = mpsc::channel(16);
    let mut server = SmppServer::new(
        IpAddr::from([127, 0, 0, 1]),
        port,
        Arc::new(TlvSmsc {
            esmes: esme_tx,
            submit_tlvs: submit_tx,
            data_tlvs: smsc_data_tx,
        }),
    );
    server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await; // let the accept loop come up

    let (bound_tx, bound_rx) = oneshot::channel();
    let (deliver_tx, deliver_tlvs) = mpsc::channel(16);
    let (esme_data_tx, esme_data_tlvs) = mpsc::channel(16);
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
        Arc::new(TlvEsme {
            bound: Mutex::new(Some(bound_tx)),
            deliver_tlvs: deliver_tx,
            data_tlvs: esme_data_tx,
        }),
        16,
    );
    client.start().await;

    let smsc = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .expect("bind timed out")
        .expect("bind channel dropped");
    let esme = tokio::time::timeout(Duration::from_secs(5), esme_rx.recv())
        .await
        .expect("no ESME bound on the server side")
        .expect("ESME channel dropped");

    Session {
        smsc,
        esme,
        submit_tlvs,
        deliver_tlvs,
        smsc_data_tlvs,
        esme_data_tlvs,
        _server: server,
        _client: client,
    }
}

async fn next_tlvs(rx: &mut mpsc::Receiver<Vec<Tlv>>) -> Vec<Tlv> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for the PDU")
        .expect("TLV channel dropped")
}

// ── ESME -> SMSC ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_sm_builder_sends_tlvs() {
    let mut session = bound_session().await;

    let resp = session
        .smsc
        .submit_sm()
        .source_addr("12345")
        .destination_addr("31600000000")
        .short_message(b"hi")
        .tlv(TlvTag::UserMessageReference, 0x1234u16.to_be_bytes())
        .tlv(TlvTag::MessagePayload, b"a longer payload".to_vec())
        .tlv_raw(VENDOR_TAG, vec![0xAA, 0xBB, 0xCC])
        .send()
        .await
        .expect("submit_sm failed");
    assert!(resp.is_success());

    let tlvs = next_tlvs(&mut session.submit_tlvs).await;
    assert_eq!(tlvs.len(), 3, "all three TLVs must arrive: {tlvs:?}");
    assert_eq!(tlvs.user_message_reference(), Some(0x1234));
    assert_eq!(tlvs.message_payload(), Some(b"a longer payload".as_slice()));
    assert_eq!(
        tlvs.get_tlv_raw(VENDOR_TAG).map(|t| t.value.as_slice()),
        Some([0xAA, 0xBB, 0xCC].as_slice()),
        "an unknown vendor tag must pass through byte-for-byte"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_sm_pdu_path_sends_tlvs_and_assigns_the_sequence_number() {
    let mut session = bound_session().await;

    // The pre-built-PDU path: what a relay does with a decoded PDU it wants to
    // forward. Sequence number 0 here is deliberate — the session owns the
    // sequence space and must overwrite it.
    let pdu = submit_sm::new(
        0,
        String::new(),
        1,
        1,
        "12345".to_string(),
        1,
        1,
        "31600000000".to_string(),
        0,
        0,
        0,
        String::new(),
        String::new(),
        0,
        0,
        0,
        0,
        b"hi".to_vec(),
    )
    .with_tlvs([Tlv::from_tag(TlvTag::SarMsgRefNum, vec![0x00, 0x07])]);

    let resp = session
        .smsc
        .send_submit_sm_pdu(pdu)
        .await
        .expect("submit_sm failed");
    assert!(resp.is_success());
    assert_ne!(
        resp.header.sequence_number, 0,
        "the session must assign the sequence number, not the caller's 0"
    );

    let tlvs = next_tlvs(&mut session.submit_tlvs).await;
    assert_eq!(tlvs.sar_msg_ref_num(), Some(7));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_sm_carries_its_message_payload_tlv_to_the_smsc() {
    let mut session = bound_session().await;

    // data_sm has no short_message at all: the message lives in the
    // message_payload TLV (SMPP 3.4 §4.2.2), so without TLVs it cannot carry one.
    let pdu = data_sm::new(
        0,
        String::new(),
        1,
        1,
        "12345".to_string(),
        1,
        1,
        "31600000000".to_string(),
        0,
        0,
        8,
    )
    .with_tlvs([
        Tlv::from_tag(TlvTag::MessagePayload, b"data_sm body".to_vec()),
        Tlv::from_tag(TlvTag::UserMessageReference, vec![0x00, 0x09]),
    ]);

    let resp = session
        .smsc
        .send_data_sm_pdu(pdu)
        .await
        .expect("data_sm failed");
    assert!(resp.is_success());

    let tlvs = next_tlvs(&mut session.smsc_data_tlvs).await;
    assert_eq!(tlvs.message_payload(), Some(b"data_sm body".as_slice()));
    assert_eq!(tlvs.user_message_reference(), Some(9));
}

// ── SMSC -> ESME ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliver_sm_builder_sends_tlvs() {
    let mut session = bound_session().await;

    // The delivery-receipt shape: receipted_message_id + message_state is how an
    // SMSC reports a DLR, and neither could be sent before.
    let resp = session
        .esme
        .deliver_sm()
        .source_addr("31600000000")
        .destination_addr("12345")
        .esm_class(0x04)
        .tlv(TlvTag::ReceiptedMessageId, b"msg-1\0".to_vec())
        .tlv(TlvTag::MessageStateTlv, vec![2])
        .tlv(TlvTag::NetworkErrorCode, vec![0x03, 0x00, 0x1F])
        .send()
        .await
        .expect("deliver_sm failed");
    assert!(resp.is_success());

    let tlvs = next_tlvs(&mut session.deliver_tlvs).await;
    assert_eq!(tlvs.len(), 3);
    assert_eq!(tlvs.receipted_message_id(), Some("msg-1".to_string()));
    assert_eq!(tlvs.message_state(), Some(2));
    assert_eq!(tlvs.network_error_code(), Some((3, 31)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_sm_carries_its_message_payload_tlv_to_the_esme() {
    let mut session = bound_session().await;

    let pdu = data_sm::new(
        0,
        String::new(),
        1,
        1,
        "31600000000".to_string(),
        1,
        1,
        "12345".to_string(),
        0,
        0,
        0,
    )
    .with_tlvs([Tlv::from_tag(TlvTag::MessagePayload, b"mo body".to_vec())]);

    let resp = session
        .esme
        .send_data_sm_pdu(pdu)
        .await
        .expect("data_sm failed");
    assert!(resp.is_success());

    let tlvs = next_tlvs(&mut session.esme_data_tlvs).await;
    assert_eq!(tlvs.message_payload(), Some(b"mo body".as_slice()));
}

// ── No TLVs set ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_without_tlvs_is_unchanged() {
    let mut session = bound_session().await;

    let resp = session
        .smsc
        .submit_sm()
        .source_addr("12345")
        .destination_addr("31600000000")
        .short_message(b"plain")
        .send()
        .await
        .expect("submit_sm failed");
    assert!(resp.is_success());
    assert_eq!(resp.get_error(), SmppError::ESME_ROK);

    assert!(next_tlvs(&mut session.submit_tlvs).await.is_empty());
}
