//! End-to-end guard for the `query_sm` / `replace_sm` wiring (server hooks,
//! `SMSC::send_*`, and response correlation) and the `SMSC::can_send` predicate.
//!
//! A client binds TRX to an in-process SMSC, sends a `query_sm` and a
//! `replace_sm`, and asserts the responses round-trip with the right fields —
//! exercising `on_query_sm`/`on_replace_sm`, the `state.rs` dispatch branches,
//! `SMSC::send_query_sm`/`send_replace_sm`, and the read-loop correlation.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::{
    bind_transceiver, bind_transceiver_resp, query_sm, query_sm_resp, replace_sm, replace_sm_resp,
    submit_sm_multi, submit_sm_multi_resp, DestAddress, SmppConnectionInformation, SmppError,
    SmppServer, SmppServerListener, UnsuccessSme,
};
use tokio::sync::{oneshot, Mutex};

struct Handler;

#[async_trait]
impl SmppServerListener for Handler {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transceiver_resp {
        req.accept("TEST-SMSC".to_string(), Some(0x34))
    }

    async fn on_query_sm(
        &self,
        req: query_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> query_sm_resp {
        // Echo the queried message_id back with a known state, so the client can
        // confirm the SMSC actually decoded the inbound query.
        let message_id = req.message_id.clone();
        req.accept(
            message_id,
            "2401011200000".to_string(),
            2, /* DELIVERED */
            0,
        )
    }

    async fn on_replace_sm(
        &self,
        req: replace_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> replace_sm_resp {
        assert_eq!(req.short_message, b"replacement");
        req.accept()
    }

    async fn on_submit_sm_multi(
        &self,
        req: submit_sm_multi,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> submit_sm_multi_resp {
        assert_eq!(req.number_of_dests, 2);
        assert_eq!(req.short_message, b"broadcast");
        // Accept, but report the distribution-list destination as unsuccessful so
        // the unsuccess_sme list is exercised end-to-end.
        req.accept(
            "multi-1".to_string(),
            vec![UnsuccessSme {
                dest_addr_ton: 1,
                dest_addr_npi: 1,
                destination_addr: "31699999999".to_string(),
                error_status_code: SmppError::ESME_RINVDSTADR as u32,
            }],
        )
    }
}

struct Binder {
    tx: Mutex<Option<oneshot::Sender<Arc<SMSC>>>>,
}

#[async_trait]
impl SmppClientListener for Binder {
    async fn on_smsc_bound(&self, smsc: SMSC, _s: &String) {
        if let Some(tx) = self.tx.lock().await.take() {
            let _ = tx.send(Arc::new(smsc));
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_and_replace_round_trip() {
    let port = free_port();
    let mut server = SmppServer::new(IpAddr::from([127, 0, 0, 1]), port, Arc::new(Handler));
    server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx, rx) = oneshot::channel();
    let binder = Arc::new(Binder {
        tx: Mutex::new(Some(tx)),
    });
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
        binder,
        16,
    );
    client.start().await;
    let smsc = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("bind timed out")
        .expect("bind channel dropped");

    // can_send predicate (Item 5): a TRX bind can send.
    assert!(smsc.can_send());

    // query_sm round-trip.
    let qresp = smsc
        .send_query_sm("MSGID-42".to_string(), 1, 1, "12345".to_string())
        .await
        .expect("query_sm failed");
    assert!(qresp.is_success());
    assert_eq!(qresp.message_id, "MSGID-42");
    assert_eq!(qresp.message_state, 2);
    assert_eq!(qresp.final_date, "2401011200000");

    // replace_sm round-trip.
    let rresp = smsc
        .send_replace_sm(
            "MSGID-42".to_string(),
            1,
            1,
            "12345".to_string(),
            String::new(),
            String::new(),
            0,
            0,
            b"replacement".to_vec(),
        )
        .await
        .expect("replace_sm failed");
    assert!(rresp.is_success());

    // submit_sm_multi round-trip (Item 4): two destinations, one reported failed.
    let mresp = smsc
        .send_submit_sm_multi(
            String::new(),
            1,
            1,
            "12345".to_string(),
            vec![
                DestAddress::sme("31600000000"),
                DestAddress::distribution_list("vip-list"),
            ],
            0,
            0,
            0,
            String::new(),
            String::new(),
            0,
            0,
            0,
            0,
            b"broadcast".to_vec(),
        )
        .await
        .expect("submit_sm_multi failed");
    assert!(mresp.is_success());
    assert_eq!(mresp.message_id.as_deref(), Some("multi-1"));
    assert_eq!(mresp.no_unsuccess, 1);
    assert_eq!(mresp.unsuccess_sme.len(), 1);
    assert_eq!(mresp.unsuccess_sme[0].destination_addr, "31699999999");

    client.stop().await;
}
