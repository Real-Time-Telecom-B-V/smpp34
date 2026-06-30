# smpp34

[![crates.io](https://img.shields.io/crates/v/smpp34.svg)](https://crates.io/crates/smpp34)
[![docs.rs](https://docs.rs/smpp34/badge.svg)](https://docs.rs/smpp34)
[![CI](https://github.com/Real-Time-Telecom-B-V/smpp34/actions/workflows/ci.yml/badge.svg)](https://github.com/Real-Time-Telecom-B-V/smpp34/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A pure-Rust implementation of the **SMPP 3.4** protocol (Short Message
Peer-to-Peer): a PDU codec plus an asynchronous ([tokio](https://tokio.rs))
**client** and **server** built on a listener-trait model.

The same engine ships two ways from **one source tree, one version**: a Rust
crate (`cargo add smpp34`, zero Python dependencies) and a Rust-backed **Python**
wheel (`pip install smpp34`) that exposes the identical async client/server to
`asyncio`. The whole SMPP machinery — framing, windowing, timers, codec — runs in
the Rust/tokio core; Python crosses the GIL only once per message, and the
extension is **free-threaded ("no-GIL") ready**. See [Python](#python) below.

- **Client** — bind to an SMSC as an ESME (transmitter, receiver or
  transceiver), send `submit_sm` / `data_sm`, and receive `deliver_sm` and
  session events through your own `SmppClientListener`.
- **Server** — accept ESME binds and dispatch `submit_sm`, `data_sm`,
  `cancel_sm`, … to your `SmppServerListener`; push MT traffic back out with
  `deliver_sm`.
- **Codec** — encode/decode every SMPP 3.4 PDU, including optional TLV
  parameters.

Sessions handle the SMPP timers (`session_init`, `enquire_link`, `inactivity`,
`response`), sequence-number windowing, and optional TLS.

## Supported PDUs

`bind_transmitter` · `bind_receiver` · `bind_transceiver` (+ resps) · `outbind`
· `unbind` · `enquire_link` · `submit_sm` · `submit_sm_multi` · `deliver_sm` ·
`data_sm` · `query_sm` · `cancel_sm` · `replace_sm` · `alert_notification` ·
`generic_nack`, plus arbitrary **TLV** (optional parameter) encode/decode.

## Install

```sh
cargo add smpp34
```

## Quick start — server

Implement `SmppServerListener` and hand it to an `SmppServer`. Every trait
method has a default, so you override only the PDUs you care about — typically
`on_bind_transceiver` and `on_submit_sm`.

```rust,no_run
use std::sync::Arc;
use std::net::{IpAddr, Ipv4Addr};
use async_trait::async_trait;
use smpp34::{
    SmppServer, SmppServerListener, SmppConnectionInformation, SmppError,
    bind_transceiver, bind_transceiver_resp, submit_sm, submit_sm_resp,
};

struct MyEsmeHandler;

#[async_trait]
impl SmppServerListener for MyEsmeHandler {
    async fn on_bind_transceiver(
        &self, req: bind_transceiver, _c: &SmppConnectionInformation, _s: &String,
    ) -> bind_transceiver_resp {
        if req.system_id == "esme1" && req.password == "secret" {
            req.accept("MySMSC".to_string(), Some(0x34))   // 0x34 = SMPP v3.4
        } else {
            req.reject(SmppError::ESME_RINVPASWD)
        }
    }

    async fn on_submit_sm(
        &self, req: submit_sm, _c: &SmppConnectionInformation, _s: &String,
    ) -> submit_sm_resp {
        // ... route the message ...
        req.accept("message-id-1".to_string())
    }

    // Every other method has a sensible default (binds reject, on_data_sm /
    // on_cancel_sm reject, on_unbind acks, notifications no-op), so you override
    // only what you need.
}

#[tokio::main]
async fn main() {
    let listener = Arc::new(MyEsmeHandler);
    let mut server = SmppServer::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2775, listener);
    server.start().await;          // spawns the accept loop
    tokio::signal::ctrl_c().await.unwrap();
    server.stop().await;
}
```

## Quick start — client

Bind to an SMSC, then submit once the bind completes (the `on_smsc_bound`
callback hands you an `SMSC` you can submit through).

```rust,no_run
use std::sync::Arc;
use smpp34::client::{SmppClient, BIND_TYPE};

// `listener` implements SmppClientListener; its on_smsc_bound callback hands
// you the bound `SMSC` handle to submit through.
let listener = Arc::new(MyClientHandler::new());
let mut client = SmppClient::new(
    "smsc.example.net".to_string(), 2775, /* tls = */ false,
    BIND_TYPE::TRX,
    "esme1".to_string(), "secret".to_string(), "GATEWAY".to_string(),
    /* addr_ton */ 0, /* addr_npi */ 0, /* address_range */ String::new(),
    listener.clone(),
    /* window_size */ 20,
);
client.start().await;
// inside on_smsc_bound(smsc, _): use the fluent builder (only set what you need)
//   smsc.submit_sm()
//       .source_addr("12345")
//       .destination_addr("31600000000")
//       .short_message(b"hello")
//       .send().await?;
```

## Python

`pip install smpp34` gives a Rust-backed wheel exposing the **same** async
client/server to `asyncio`. The hot path stays in the Rust/tokio core; the Python
layer is an event-pull bridge that crosses the GIL once per message. abi3 wheels
cover CPython 3.9+, plus free-threaded ("no-GIL") builds.

```sh
pip install smpp34
```

**Client** — `connect()` binds and returns an `Smsc`; `await` sends and inbound
events:

```python
import asyncio, smpp34

async def main():
    client = smpp34.Client(host="smsc.example.net", port=2775,
                           system_id="esme1", password="secret", bind_type="TRX")
    smsc = await client.connect()
    resp = await smsc.submit_sm(destination_addr="31600000000",
                                source_addr="12345", short_message=b"hello")
    print(resp.message_id, resp.is_success)

    # pull inbound deliver_sm (MO / DLR) — one GIL crossing per message
    ev = await smsc.next()                       # DeliverSmEvent | Disconnected
    if isinstance(ev, smpp34.DeliverSmEvent):
        print(ev.source_addr, ev.short_message)
    await smsc.unbind()

asyncio.run(main())
```

**Server** — `await next()` yields an `Esme` when a client binds, a
`SubmitSmEvent` per inbound message (answer with `accept`/`reject`), or `Unbound`:

```python
import asyncio, smpp34

async def main():
    server = smpp34.Server(host="0.0.0.0", port=2775, system_id="MySMSC")
    await server.start()
    sessions = {}
    while True:
        ev = await server.next()
        if isinstance(ev, smpp34.Esme):
            sessions[ev.session_id] = ev          # stash the MT send handle
        elif isinstance(ev, smpp34.SubmitSmEvent):
            ev.accept(message_id="abc")           # or ev.reject(smpp34.ESME_RMSGQFUL)
            # push MT back to that ESME (here, a delivery receipt):
            await sessions[ev.session_id].deliver_sm(
                source_addr=ev.destination_addr, destination_addr=ev.source_addr,
                esm_class=0x04, short_message=b"id:abc stat:DELIVRD")

asyncio.run(main())
```

**Codec** — encode/decode PDUs without any I/O:

```python
pdu = smpp34.SubmitSm(source_addr="12345", destination_addr="31600000000",
                      short_message=b"hello")
wire = pdu.encode()
msg = smpp34.decode(wire)        # -> typed SubmitSm / DeliverSm / RawPdu
```

Runnable samples: [`python/examples/`](python/examples) (and the Rust
equivalents in [`examples/`](examples) — `client.rs` / `server.rs`). A throughput
and leak harness lives in [`python/perf/`](python/perf), mirroring the Rust
`examples/{perf_loopback,leak_check}.rs`.

## TLS

Pass `tls = true` to `SmppClient::new` to bind over TLS. The TLS transport is
backed by [`tokio-native-tls`](https://crates.io/crates/tokio-native-tls)
(system OpenSSL).

## Timers & windowing

`SmppServer::new` / `SmppClient::new` use sensible default timers. Use the
`*_with_default_timers` constructors to set `session_init`, `enquire_link`,
`inactivity` and `response` timers (milliseconds) and the buffer / window size
explicitly.

## Performance

Measured on a 24-core AMD Ryzen AI 9 HX 370 (single box — the SMSC and ESME are
co-located, so the load generator competes for cores; separate hosts scale
further). Reproduce with `cargo bench` and `perf/docker-compose.yml`.

**Codec** — single core, criterion (`cargo bench`):

| Operation | Throughput |
|---|---:|
| `submit_sm` decode | ~6.6 M PDU/s |
| `submit_sm` encode | ~12.3 M PDU/s |
| `deliver_sm` decode / encode | ~6.8 M / ~12.6 M PDU/s |
| TLV decode / encode | ~20 M / ~14 M /s |

**End-to-end** — real TCP, full `submit_sm` round-trip (send → `submit_sm_resp`):

| Load | Throughput | Peak RSS |
|---|---:|---:|
| 1 ESME, window 64 | ~180k submit_sm/s | 6 MB |
| 4 ESMEs, window 64 | **~635k submit_sm/s** | 7 MB |

The SMSC server sustains **625k+ submit_sm/s** even pinned to half the cores;
past a few sessions the single-box benchmark is bound by the co-located load
generator, not by smpp34.

### Rust vs Python (and free-threading)

Both languages drive the **same** Rust/tokio engine, so the protocol work —
framing, windowing, timers, codec — runs at full Rust speed either way. The
difference is the Python boundary: the event-pull bridge crosses the GIL **once
per message**.

| Path | Throughput | Notes |
|---|---:|---|
| Rust — 4 ESMEs, window 64 | **~635k submit_sm/s** | full multi-threaded tokio core |
| Python — single `asyncio` loop | ~25k submit_sm/s | one GIL crossing per message (release wheel) |
| Codec decode — GIL CPython | ~4.5M PDU/s (flat vs threads) | the GIL serialises the pyo3 call |
| Codec decode — free-threaded, 4 threads | **~12M PDU/s** | no GIL → real parallelism |

A few things worth knowing:

- **Build matters.** A `maturin develop` *debug* build is ~3× slower; published
  wheels are `--release`. The ~25k/s figure is a release build.
- **Tune the tokio pool** with `SMPP34_WORKER_THREADS` (default `2`). On a
  many-core box, a large pool thrashes the GIL on every future completion and
  throughput *drops* (24 threads ≈ 17k/s vs 1 thread ≈ 28k/s); on a free-threaded
  build, raise it.
- **Free-threaded ("no-GIL") is correctness-clean** — the full test suite passes
  with the GIL off (`sys._is_gil_enabled() == False`) — and lets CPU-bound Python
  work (the codec, your per-message handlers) run in parallel, as the codec row
  shows. Single-process async I/O is bounded by the shared tokio runtime, so scale
  that across processes.
- ~25k submit_sm/s on one event loop is already well above the per-bind rate
  limits carriers typically impose.

Reproduce: [`python/perf/perf_loopback.py`](python/perf/perf_loopback.py)
(`COUNT=200000 SESSIONS=4 WINDOW=64`), the thread-scaling demo
[`python/perf/perf_threads.py`](python/perf/perf_threads.py), and the leak check
[`python/perf/leak_check.py`](python/perf/leak_check.py) — all after a release
build (`maturin develop --release`).

### Profiling

A CPU flamegraph of the loopback under load (`scripts/flamegraph.sh`) puts
**~99.98% of samples in the kernel** (the socket/syscall path) — the user-space
codec barely appears. smpp34 is **syscall-bound, not CPU-bound**: at these rates
the cost is the ~600k+ `read`/`write` syscalls per second plus the runtime's
wakeups, not parsing (the codec runs at millions of PDUs/sec). The remaining
headroom is therefore **I/O batching** — coalescing writes and cutting wakeups —
not faster code.

## Memory

`scripts/mem_leak_test.sh` installs a counting global allocator and asserts
**live bytes stay flat** (exact — unlike noisy RSS) across both codec round-trips
and bind/unbind churn. RSS holds at single-digit MB while pushing millions of
PDUs. (The bind/unbind check found and now guards a real session-teardown leak —
see the [changelog](CHANGELOG.md).)

## SMPP 3.4 compliance

The PDU / TLV / session support matrix is in
[`docs/COMPLIANCE.md`](https://github.com/Real-Time-Telecom-B-V/smpp34/blob/main/docs/COMPLIANCE.md);
[`docs/COMPARISON.md`](https://github.com/Real-Time-Telecom-B-V/smpp34/blob/main/docs/COMPARISON.md)
maps smpp34 against other SMPP stacks.

## MSRV

Rust **1.80**.

## Stability

The SMPP 3.4 codec and the client/server session handling are used in
production, and the crate follows [Semantic Versioning](VERSIONING.md): the
public Rust API is the contract. The 17-argument `send_submit_sm` /
`send_deliver_sm` remain available, but the `submit_sm()` / `deliver_sm()`
builders are the recommended way to send.

## License

[MIT](LICENSE) © Real Time Telecom B.V.

Maintained by [Real Time Telecom B.V.](https://realtime-telecom.nl) — carrier-grade
telecom infrastructure in Rust.
