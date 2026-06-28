//! Perf ESME — binds `SESSIONS` client sessions to an SMSC and fires `submit_sm`
//! at maximum rate across all of them, then prints the achieved aggregate
//! **submit_sm/sec** (full round-trips: each send awaits its `submit_sm_resp`)
//! and peak RSS. The load half of the docker-compose perf harness; the SMSC
//! side is `perf_smsc`.
//!
//! A single ESME (one TCP connection) can't saturate a multi-core SMSC, so bump
//! `SESSIONS` to fan the load across connections.
//!
//! Run: `SMSC_ADDR=127.0.0.1:2775 SESSIONS=8 COUNT=2000000 WINDOW=64 \
//!       cargo run --release --example perf_esme`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use tokio::sync::{oneshot, Mutex};

/// Hands the bound `SMSC` for one session out to `main`.
struct Binder {
    tx: Mutex<Option<oneshot::Sender<Arc<SMSC>>>>,
}

#[async_trait]
impl SmppClientListener for Binder {
    async fn on_smsc_bound(&self, smsc: SMSC, _session_id: &String) {
        if let Some(tx) = self.tx.lock().await.take() {
            let _ = tx.send(Arc::new(smsc));
        }
    }
}

#[tokio::main]
async fn main() {
    let addr = std::env::var("SMSC_ADDR").unwrap_or_else(|_| "127.0.0.1:2775".to_string());
    let (host, port) = addr.rsplit_once(':').expect("SMSC_ADDR must be host:port");
    let port: u16 = port.parse().expect("port");
    let count: u64 = env_u64("COUNT", 1_000_000);
    let window: usize = env_u64("WINDOW", 64) as usize;
    let sessions: usize = env_u64("SESSIONS", 1) as usize;

    // Bind every session; keep the SmppClient handles alive for the whole run.
    let mut clients = Vec::with_capacity(sessions);
    let mut smscs = Vec::with_capacity(sessions);
    for _ in 0..sessions {
        let (tx, rx) = oneshot::channel();
        let binder = Arc::new(Binder {
            tx: Mutex::new(Some(tx)),
        });
        let mut client = SmppClient::new(
            host.to_string(),
            port,
            false, // tls
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
        let smsc = rx.await.expect("bind never completed");
        smscs.push(smsc);
        clients.push(client);
    }
    eprintln!(
        "perf-esme: {sessions} session(s) bound to {addr}; firing {count} submit_sm at window {window} ..."
    );

    // Shared work counter; `sessions × window` tasks each keep one PDU in flight.
    let remaining = Arc::new(AtomicU64::new(count));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(sessions * window);
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
    let rate = count as f64 / elapsed;
    println!(
        "perf-esme: {count} submit_sm in {elapsed:.2}s = {rate:.0} submit_sm/s  ({sessions} sessions x window {window}, peak RSS {} MB)",
        rss_mb()
    );
    for mut c in clients {
        c.stop().await;
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .nth(1)
                .and_then(|p| p.parse::<u64>().ok())
        })
        .map(|pages| pages * 4096 / 1024 / 1024)
        .unwrap_or(0)
}
