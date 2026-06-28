//! In-process loopback: an SMSC and an ESME in one process firing `submit_sm`,
//! then exits. Handy as a quick throughput check and as the **profiling target**
//! for `scripts/flamegraph.sh` (a single short-lived process the profiler can
//! sample start-to-finish, exercising both the server and client hot paths).
//!
//! Run: `COUNT=1000000 SESSIONS=4 WINDOW=64 cargo run --release --example perf_loopback`

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
        req.accept("LOOPBACK".to_string(), Some(0x34))
    }
    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> submit_sm_resp {
        self.received.fetch_add(1, Ordering::Relaxed);
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

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    let count = env_u64("COUNT", 1_000_000);
    let sessions = env_u64("SESSIONS", 4) as usize;
    let window = env_u64("WINDOW", 64) as usize;

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let counter = Arc::new(Counter {
        received: AtomicU64::new(0),
    });
    let mut server = SmppServer::new(IpAddr::from([127, 0, 0, 1]), port, counter.clone());
    server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut smscs = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..sessions {
        let (tx, rx) = oneshot::channel();
        let binder = Arc::new(Binder {
            tx: Mutex::new(Some(tx)),
        });
        let mut client = SmppClient::new(
            "127.0.0.1".to_string(),
            port,
            false,
            BIND_TYPE::TRX,
            "perf".to_string(),
            "perf".to_string(),
            String::new(),
            1,
            1,
            String::new(),
            binder,
            window,
        );
        client.start().await;
        smscs.push(rx.await.expect("bind"));
        clients.push(client);
    }

    let remaining = Arc::new(AtomicU64::new(count));
    let start = Instant::now();
    let mut handles = Vec::new();
    for smsc in &smscs {
        for _ in 0..window {
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
                        .short_message(b"perf test message")
                        .send()
                        .await;
                }
            }));
        }
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "perf-loopback: {count} submit_sm in {elapsed:.2}s = {:.0} submit_sm/s ({sessions}x{window}), server received {}",
        count as f64 / elapsed,
        counter.received.load(Ordering::Relaxed)
    );
    for mut c in clients {
        c.stop().await;
    }
}
