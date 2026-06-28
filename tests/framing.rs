//! Framing regression test.
//!
//! SMPP runs over TCP, which is a byte stream: a single `read()` can return
//! several PDUs, or stop in the middle of one. Earlier read loops assumed each
//! read delivered a whole number of complete PDUs (they sliced past what was
//! read and `clear()`ed the buffer every read), so under pipelined load they
//! panicked or dropped data. This test fires many concurrent `submit_sm` so PDUs
//! coalesce and split across read boundaries, and asserts every one is received
//! and answered — i.e. the accumulating framer reassembles them losslessly.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::{
    bind_transceiver, bind_transceiver_resp, submit_sm, submit_sm_resp, SmppConnectionInformation,
    SmppServer, SmppServerListener,
};
use tokio::sync::{oneshot, Mutex};

struct Counter {
    received: AtomicU64,
}

#[async_trait]
impl SmppServerListener for Counter {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transceiver_resp {
        req.accept("TEST-SMSC".to_string(), Some(0x34))
    }
    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> submit_sm_resp {
        self.received.fetch_add(1, Ordering::SeqCst);
        req.accept("1".to_string())
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

/// Grab an almost-certainly-free loopback port.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_submit_sm_all_received() {
    const N: u64 = 1000;
    const CONCURRENCY: usize = 32;

    let port = free_port();
    let counter = Arc::new(Counter {
        received: AtomicU64::new(0),
    });
    let mut server = SmppServer::new(IpAddr::from([127, 0, 0, 1]), port, counter.clone());
    server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await; // let the accept loop come up

    // Bind one ESME session.
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
        64,
    );
    client.start().await;
    let smsc = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("bind timed out")
        .expect("bind channel dropped");

    // Fire N submit_sm with CONCURRENCY in flight so they pipeline on the wire.
    let remaining = Arc::new(AtomicU64::new(N));
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let smsc = smsc.clone();
        let remaining = remaining.clone();
        handles.push(tokio::spawn(async move {
            while remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| x.checked_sub(1))
                .is_ok()
            {
                let _ = smsc
                    .submit_sm()
                    .source_addr("12345")
                    .destination_addr("31600000000")
                    .short_message(b"framing regression test message")
                    .send()
                    .await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    let got = counter.received.load(Ordering::SeqCst);
    client.stop().await;
    assert_eq!(
        got, N,
        "SMSC received {got} of {N} pipelined submit_sm — frame loss/corruption"
    );
}
