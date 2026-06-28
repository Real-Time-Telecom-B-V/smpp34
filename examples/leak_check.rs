//! Memory-leak check.
//!
//! A counting global allocator tracks **live bytes** (allocated − freed) — RSS
//! is too noisy (the OS/allocator retains freed pages), but live bytes are
//! exact, so a real leak shows up as monotonic growth. Two phases:
//!
//!   1. **codec** — decode + re-encode a PDU (+ TLVs) for many cycles.
//!   2. **bind/unbind** — bind an ESME session to a loopback SMSC, unbind, and
//!      tear it down, over and over (the session-lifecycle allocation path:
//!      read/writer/enquire-link tasks, pending-request maps, per-session state).
//!
//! Each phase asserts live bytes return to a flat baseline. Exits non-zero on a
//! leak. Driven by `scripts/mem_leak_test.sh`.
//!
//! Run: `cargo run --release --example leak_check`

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use smpp34::{
    bind_transceiver, bind_transceiver_resp, decode_tlvs, encode_tlvs, submit_sm, CommandHeader,
    SmppConnectionInformation, SmppServer, SmppServerListener, Tlv, TlvTag,
};
use tokio::sync::{oneshot, Mutex};

// ── Counting allocator ──────────────────────────────────────────────────────
static LIVE: AtomicI64 = AtomicI64::new(0);

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            LIVE.fetch_add(l.size() as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l);
        LIVE.fetch_sub(l.size() as i64, Ordering::Relaxed);
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(l);
        if !p.is_null() {
            LIVE.fetch_add(l.size() as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn realloc(&self, ptr: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, l, new_size);
        if !p.is_null() {
            LIVE.fetch_add(new_size as i64 - l.size() as i64, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> i64 {
    LIVE.load(Ordering::Relaxed)
}

// ── Phase 1: codec workload ─────────────────────────────────────────────────
fn submit_sm_bytes() -> Vec<u8> {
    let sm = b"leak check message of typical length";
    let mut body = Vec::new();
    body.push(0);
    body.extend_from_slice(&[1, 1]);
    body.extend_from_slice(b"12345\0");
    body.extend_from_slice(&[1, 1]);
    body.extend_from_slice(b"31600000000\0");
    body.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0]);
    body.push(sm.len() as u8);
    body.extend_from_slice(sm);
    let cmd_len = (16 + body.len()) as u32;
    let mut pdu = Vec::with_capacity(cmd_len as usize);
    pdu.extend_from_slice(&cmd_len.to_be_bytes());
    pdu.extend_from_slice(&0x0000_0004u32.to_be_bytes());
    pdu.extend_from_slice(&0u32.to_be_bytes());
    pdu.extend_from_slice(&1u32.to_be_bytes());
    pdu.extend_from_slice(&body);
    pdu
}

fn codec_cycle(iters: usize) {
    let bytes = submit_sm_bytes();
    let tlvs = vec![
        Tlv::from_tag(TlvTag::SarMsgRefNum, vec![0x12, 0x34]),
        Tlv::from_tag(TlvTag::MessagePayload, b"concatenated segment".to_vec()),
    ];
    for _ in 0..iters {
        let h = CommandHeader::decode(&bytes).unwrap();
        let pdu = submit_sm::decode(h, &bytes).unwrap();
        std::hint::black_box(pdu.encode());
        std::hint::black_box(decode_tlvs(&encode_tlvs(&tlvs)));
    }
}

// ── Phase 2: bind/unbind churn ──────────────────────────────────────────────
struct Sink {
    received: AtomicU64,
}
#[async_trait]
impl SmppServerListener for Sink {
    async fn on_bind_transceiver(
        &self,
        req: bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> bind_transceiver_resp {
        self.received.fetch_add(1, Ordering::Relaxed);
        req.accept("LEAK-SMSC".to_string(), Some(0x34))
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

async fn bind_then_unbind(port: u16) {
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
        8,
    );
    client.start().await;
    if let Ok(Ok(smsc)) = tokio::time::timeout(Duration::from_secs(2), rx).await {
        let _ = smsc.send_unbind().await; // graceful unbind
    }
    client.stop().await;
}

fn report(phase: &str, base: i64) -> i64 {
    let growth = live() - base;
    println!("  {phase}: live = {} bytes (Δ {:+})", live(), growth);
    growth
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    const CODEC_ITERS: usize = 200_000;
    const CODEC_CYCLES: usize = 10;
    const CODEC_BUDGET: i64 = 256 * 1024;
    const CHURN_PER_CYCLE: usize = 25;
    const CHURN_CYCLES: usize = 8;
    const CHURN_BUDGET: i64 = 256 * 1024; // tight: the enquire-link leak was ~787KB over 200 binds

    // Phase 1: codec.
    println!("[codec] {CODEC_CYCLES} x {CODEC_ITERS} decode+encode round-trips");
    codec_cycle(CODEC_ITERS); // warm up
    let codec_base = live();
    for c in 1..=CODEC_CYCLES {
        codec_cycle(CODEC_ITERS);
        report(&format!("cycle {c:>2}/{CODEC_CYCLES}"), codec_base);
    }
    let codec_growth = live() - codec_base;

    // Phase 2: bind/unbind churn against a loopback SMSC.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let sink = Arc::new(Sink {
        received: AtomicU64::new(0),
    });
    let mut server = SmppServer::new(IpAddr::from([127, 0, 0, 1]), port, sink.clone());
    server.start().await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    println!("\n[bind/unbind] {CHURN_CYCLES} x {CHURN_PER_CYCLE} bind+unbind sessions");
    for _ in 0..CHURN_PER_CYCLE {
        bind_then_unbind(port).await; // warm up (runtime pools settle)
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let churn_base = live();
    for c in 1..=CHURN_CYCLES {
        for _ in 0..CHURN_PER_CYCLE {
            bind_then_unbind(port).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await; // let teardown settle
        report(&format!("cycle {c:>2}/{CHURN_CYCLES}"), churn_base);
    }
    let churn_growth = live() - churn_base;

    // Verdict.
    println!();
    let mut ok = true;
    if codec_growth > CODEC_BUDGET {
        eprintln!("FAIL: codec live bytes grew {codec_growth} (> {CODEC_BUDGET})");
        ok = false;
    }
    if churn_growth > CHURN_BUDGET {
        eprintln!("FAIL: bind/unbind live bytes grew {churn_growth} (> {CHURN_BUDGET})");
        ok = false;
    }
    if !ok {
        std::process::exit(1);
    }
    println!(
        "PASS: codec Δ {codec_growth} ≤ {CODEC_BUDGET}; bind/unbind Δ {churn_growth} ≤ {CHURN_BUDGET} \
         ({} binds accepted)",
        sink.received.load(Ordering::Relaxed)
    );
}
