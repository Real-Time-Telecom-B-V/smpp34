//! Minimal SMPP server (SMSC) showing **both directions**: it acks every inbound
//! `submit_sm` (MO) and pushes a `deliver_sm` delivery receipt (MT) back to the
//! ESME that sent it. The Rust counterpart of `python/examples/server.py`.
//!
//! Run: `cargo run --example server`  (listens on 127.0.0.1:2775)

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use async_trait::async_trait;
use smpp34::server::ESME;
use smpp34::{
    bind_transceiver, bind_transceiver_resp, submit_sm, submit_sm_resp, SmppConnectionInformation,
    SmppServer, SmppServerListener,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct Handler {
    // Bound ESME sessions keyed by session_id, so we can push MT back to them.
    // `on_esme_bound` hands us the ESME by move; we stash it behind an Arc.
    esmes: Mutex<HashMap<String, Arc<ESME>>>,
}

#[async_trait]
impl SmppServerListener for Handler {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transceiver_resp {
        println!("bind from system_id={:?}", req.system_id);
        req.accept("ExampleSMSC".to_string(), Some(0x34)) // 0x34 = SMPP v3.4
    }

    async fn on_esme_bound(&self, esme: ESME, session_id: &String) {
        self.esmes
            .lock()
            .await
            .insert(session_id.clone(), Arc::new(esme));
    }

    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        session_id: &String,
    ) -> submit_sm_resp {
        println!(
            "MO submit_sm {} -> {}: {:?}",
            req.source_addr,
            req.destination_addr,
            String::from_utf8_lossy(&req.short_message),
        );

        // Push a delivery receipt (MT deliver_sm) back to the ESME that submitted
        // it. on_submit_sm must return its response promptly, so send the MT on a
        // spawned task rather than blocking here.
        if let Some(esme) = self.esmes.lock().await.get(session_id).cloned() {
            let src = req.destination_addr.clone();
            let dst = req.source_addr.clone();
            tokio::spawn(async move {
                let _ = esme
                    .deliver_sm()
                    .source_addr(src)
                    .destination_addr(dst)
                    .esm_class(0x04) // delivery receipt
                    .short_message(b"id:msg-1 stat:DELIVRD")
                    .send()
                    .await;
            });
        }

        req.accept("msg-1".to_string())
    }

    async fn on_esme_unbound(&self, session_id: &String) {
        self.esmes.lock().await.remove(session_id);
    }
}

#[tokio::main]
async fn main() {
    let mut server = SmppServer::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        2775,
        Arc::new(Handler::default()),
    );
    server.start().await;
    println!("SMSC listening on 127.0.0.1:2775 — Ctrl-C to stop");
    tokio::signal::ctrl_c().await.unwrap();
    server.stop().await;
}
