//! Perf SMSC — binds, accepts a transceiver bind, counts `submit_sm`, logs the
//! per-second rate and peak RSS. The receiving half of the docker-compose perf
//! harness (`perf/docker-compose.yml`); the ESME side is `perf_esme`.
//!
//! Run: `SMSC_BIND=0.0.0.0:2775 cargo run --release --example perf_smsc`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::{
    bind_transceiver, bind_transceiver_resp, submit_sm, submit_sm_resp, SmppConnectionInformation,
    SmppServer, SmppServerListener,
};

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
        req.accept("PERF-SMSC".to_string(), Some(0x34))
    }

    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> submit_sm_resp {
        self.received.fetch_add(1, Ordering::Relaxed);
        req.accept("0".to_string())
    }
    // Everything else (binds we reject, unbind, data_sm, lifecycle hooks) uses
    // the SmppServerListener trait defaults.
}

#[tokio::main]
async fn main() {
    let bind = std::env::var("SMSC_BIND").unwrap_or_else(|_| "0.0.0.0:2775".to_string());
    let addr: SocketAddr = bind.parse().expect("SMSC_BIND must be host:port");

    let counter = Arc::new(Counter {
        received: AtomicU64::new(0),
    });
    let mut server = SmppServer::new(addr.ip(), addr.port(), counter.clone());
    server.start().await;
    eprintln!("perf-smsc listening on {addr}");

    let mut last = 0u64;
    let mut peak_rss = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = counter.received.load(Ordering::Relaxed);
        let rate = now - last;
        last = now;
        let rss = rss_mb();
        if rss > peak_rss {
            peak_rss = rss;
        }
        if rate > 0 {
            eprintln!("perf-smsc: {rate} submit_sm/s  (total {now}, peak RSS {peak_rss} MB)");
        }
    }
}

/// Resident set size in MB from `/proc/self/statm` (field 2 = resident pages).
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
