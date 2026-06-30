//! Minimal SMPP client (ESME) showing **both directions**: it binds, submits a
//! message (MO), and receives inbound `deliver_sm` (MT — an MO from the SMSC or a
//! delivery receipt) via `on_deliver_sm`. The counterpart of
//! `python/examples/client.py`.
//!
//! Start a server first (`cargo run --example server`), then in another shell:
//! `cargo run --example client`

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::{deliver_sm, deliver_sm_resp, SmppConnectionInformation};
use tokio::sync::{oneshot, Mutex};

struct Binder {
    tx: Mutex<Option<oneshot::Sender<Arc<SMSC>>>>,
}

#[async_trait]
impl SmppClientListener for Binder {
    /// The bound `SMSC` is handed over here; forward it out so `main` can submit.
    async fn on_smsc_bound(&self, smsc: SMSC, _s: &String) {
        if let Some(tx) = self.tx.lock().await.take() {
            let _ = tx.send(Arc::new(smsc));
        }
    }

    /// Inbound MT: an MO delivered by the SMSC, or the delivery receipt for a
    /// message we submitted. Returning `accept()` acks it.
    async fn on_deliver_sm(
        &self,
        req: deliver_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> deliver_sm_resp {
        println!(
            "MT deliver_sm {} -> {}: {:?}",
            req.source_addr,
            req.destination_addr,
            String::from_utf8_lossy(&req.short_message),
        );
        req.accept()
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = oneshot::channel();
    let binder = Arc::new(Binder {
        tx: Mutex::new(Some(tx)),
    });

    let mut client = SmppClient::new(
        "127.0.0.1".to_string(),
        2775,
        false, // tls
        BIND_TYPE::TRX,
        "esme1".to_string(),
        "secret".to_string(),
        String::new(), // system_type
        0,             // addr_ton
        0,             // addr_npi
        String::new(), // address_range
        binder,
        10, // window_size
    );
    client.start().await;

    let smsc = rx.await.expect("bind");
    println!("bound to {} (session {})", smsc.system_id, smsc.session_id);

    let resp = smsc
        .submit_sm()
        .source_addr("12345")
        .destination_addr("31600000000")
        .short_message(b"hello from rust")
        .send()
        .await;
    println!("submit_sm_resp: {:?}", resp.map(|r| r.message_id));

    // Stay bound briefly so the SMSC's delivery receipt reaches on_deliver_sm.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let _ = smsc.send_unbind().await; // best-effort graceful unbind
    client.stop().await;
}
