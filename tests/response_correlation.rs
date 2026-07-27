//! Response-correlation guard for both writer tasks, under pipelining.
//!
//! Both writer tasks used to insert into `pending_requests` only *after* the
//! socket write returned. The read loop drops any response it finds no pending
//! entry for, so a response that arrives while the writer task sits between its
//! write and its insert is lost, and the caller blocks until its response timer
//! expires — a lost PDU, not a slow one. Registration now happens before the
//! write, which closes that window by construction.
//!
//! Scope, so this is not mistaken for more than it is: these tests pass both
//! before and after that reordering. What they guard is that correlation holds
//! under sustained pipelining in both directions — `client/mod.rs` (ESME -> SMSC,
//! `submit_sm`) and `server/state.rs` (SMSC -> ESME, `deliver_sm`, the
//! delivery-receipt path) — so a future change that breaks matching outright, or
//! that reintroduces a window wide enough to matter, fails here.
//!
//! To exercise the race itself, drive load pinned to **exactly two CPUs**
//! (`taskset -c 0-1`). That is the one configuration where it shows up: a single
//! CPU makes tokio run one worker and the preemption point disappears, and two
//! CPUs under heavy competing load masks it again. Pinned to two CPUs at 5000
//! pipelined requests per run, 10 of 30 runs lost at least one response before
//! the reordering and 0 of 52 did after. The signature is
//! `No pending request for sequence_number N`, then one response timer later
//! `... with sequence_number N timed out` for that same N.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::server::ESME;
use smpp34::{
    bind_transceiver, bind_transceiver_resp, submit_sm, submit_sm_resp, SmppConnectionInformation,
    SmppServer, SmppServerListener,
};
use tokio::sync::{oneshot, Mutex, Semaphore};

/// Enough sustained pipelining to be a meaningful correlation check while still
/// finishing well inside a second.
const REQUESTS: usize = 20_000;

/// Mirrors the in-flight window the load harness drives with.
const WINDOW: usize = 32;

/// Short enough that a dropped response fails the test quickly instead of
/// stalling for the 30s default, generous enough that a merely slow response
/// (p99 is well under a millisecond on loopback) is never mistaken for a lost
/// one.
const RESPONSE_TIMER_MS: u64 = 2_000;

const SESSION_INIT_TIMER_MS: u64 = 5_000;
const ENQUIRE_LINK_TIMER_MS: u64 = 30_000;
const INACTIVITY_TIMER_MS: u64 = 300_000;
const BUFFER_SIZE: usize = 1_500;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// ── ESME -> SMSC (`client/mod.rs` writer) ───────────────────────────────────

/// Mock SMSC that accepts any transceiver bind and acks every `submit_sm`
/// immediately — the tighter the turnaround, the harder correlation is pushed.
struct AckEverySubmit;

#[async_trait]
impl SmppServerListener for AckEverySubmit {
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
        req.accept("id".to_string())
    }
}

struct CaptureSmsc {
    tx: Mutex<Option<oneshot::Sender<Arc<SMSC>>>>,
}

#[async_trait]
impl SmppClientListener for CaptureSmsc {
    async fn on_smsc_bound(&self, smsc: SMSC, _s: &String) {
        if let Some(tx) = self.tx.lock().await.take() {
            let _ = tx.send(Arc::new(smsc));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_sm_resp_is_never_dropped_under_load() {
    let port = free_port();
    let mut server = SmppServer::new_with_default_timers(
        IpAddr::from([127, 0, 0, 1]),
        port,
        Arc::new(AckEverySubmit),
        SESSION_INIT_TIMER_MS,
        ENQUIRE_LINK_TIMER_MS,
        INACTIVITY_TIMER_MS,
        RESPONSE_TIMER_MS,
        BUFFER_SIZE,
    );
    server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx, rx) = oneshot::channel();
    let mut client = SmppClient::new_with_default_timers(
        "127.0.0.1".to_string(),
        port,
        false,
        BIND_TYPE::TRX,
        "load".to_string(),
        "load".to_string(),
        String::new(),
        1,
        1,
        String::new(),
        Arc::new(CaptureSmsc {
            tx: Mutex::new(Some(tx)),
        }),
        SESSION_INIT_TIMER_MS,
        ENQUIRE_LINK_TIMER_MS,
        INACTIVITY_TIMER_MS,
        RESPONSE_TIMER_MS,
        BUFFER_SIZE,
        WINDOW,
    );
    client.start().await;
    let smsc = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("bind timed out")
        .expect("bind channel dropped");

    let sem = Arc::new(Semaphore::new(WINDOW));
    let lost = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(REQUESTS);

    for _ in 0..REQUESTS {
        let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
        let smsc = smsc.clone();
        let lost = lost.clone();
        handles.push(tokio::spawn(async move {
            let sent = smsc
                .submit_sm()
                .source_addr("15550100".to_string())
                .destination_addr("15550199".to_string())
                .short_message(b"correlation".to_vec())
                .send()
                .await;
            if sent.is_err() {
                lost.fetch_add(1, Ordering::Relaxed);
            }
            drop(permit);
        }));
    }
    for h in handles {
        h.await.expect("submit task panicked");
    }

    client.stop().await;

    assert_eq!(
        lost.load(Ordering::Relaxed),
        0,
        "{REQUESTS} submit_sm should all have been correlated with their \
         submit_sm_resp; a non-zero count means the read loop dropped a \
         response because its pending entry was not registered yet"
    );
}

// ── SMSC -> ESME (`server/state.rs` writer) ─────────────────────────────────

/// Mock SMSC that accepts the bind and hands the session's `ESME` handle out to
/// the test so it can drive `deliver_sm` in the SMSC -> ESME direction.
struct CaptureEsme {
    tx: Mutex<Option<oneshot::Sender<Arc<ESME>>>>,
}

#[async_trait]
impl SmppServerListener for CaptureEsme {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transceiver_resp {
        req.accept("TEST-SMSC".to_string(), Some(0x34))
    }

    async fn on_esme_bound(&self, esme: ESME, _s: &String) {
        if let Some(tx) = self.tx.lock().await.take() {
            let _ = tx.send(Arc::new(esme));
        }
    }
}

/// Client listener that leaves `on_deliver_sm` on its default (accept), so every
/// `deliver_sm` is acked as fast as the runtime allows.
struct AckEveryDeliver;

#[async_trait]
impl SmppClientListener for AckEveryDeliver {
    async fn on_smsc_bound(&self, _smsc: SMSC, _s: &String) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliver_sm_resp_is_never_dropped_under_load() {
    let port = free_port();
    let (tx, rx) = oneshot::channel();
    let mut server = SmppServer::new_with_default_timers(
        IpAddr::from([127, 0, 0, 1]),
        port,
        Arc::new(CaptureEsme {
            tx: Mutex::new(Some(tx)),
        }),
        SESSION_INIT_TIMER_MS,
        ENQUIRE_LINK_TIMER_MS,
        INACTIVITY_TIMER_MS,
        RESPONSE_TIMER_MS,
        BUFFER_SIZE,
    );
    server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = SmppClient::new_with_default_timers(
        "127.0.0.1".to_string(),
        port,
        false,
        BIND_TYPE::TRX,
        "load".to_string(),
        "load".to_string(),
        String::new(),
        1,
        1,
        String::new(),
        Arc::new(AckEveryDeliver),
        SESSION_INIT_TIMER_MS,
        ENQUIRE_LINK_TIMER_MS,
        INACTIVITY_TIMER_MS,
        RESPONSE_TIMER_MS,
        BUFFER_SIZE,
        WINDOW,
    );
    client.start().await;
    let esme = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("esme bind timed out")
        .expect("esme channel dropped");
    assert!(esme.can_receive, "a TRX bind must be able to receive");

    let sem = Arc::new(Semaphore::new(WINDOW));
    let lost = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(REQUESTS);

    for _ in 0..REQUESTS {
        let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
        let esme = esme.clone();
        let lost = lost.clone();
        handles.push(tokio::spawn(async move {
            let sent = esme
                .deliver_sm()
                .source_addr("15550199".to_string())
                .destination_addr("15550100".to_string())
                .short_message(b"correlation".to_vec())
                .send()
                .await;
            if sent.is_err() {
                lost.fetch_add(1, Ordering::Relaxed);
            }
            drop(permit);
        }));
    }
    for h in handles {
        h.await.expect("deliver task panicked");
    }

    client.stop().await;

    assert_eq!(
        lost.load(Ordering::Relaxed),
        0,
        "{REQUESTS} deliver_sm should all have been correlated with their \
         deliver_sm_resp; a non-zero count means the read loop dropped a \
         response because its pending entry was not registered yet"
    );
}
