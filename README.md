# smpp34

[![crates.io](https://img.shields.io/crates/v/smpp34.svg)](https://crates.io/crates/smpp34)
[![docs.rs](https://docs.rs/smpp34/badge.svg)](https://docs.rs/smpp34)
[![CI](https://github.com/Real-Time-Telecom-B-V/smpp34/actions/workflows/ci.yml/badge.svg)](https://github.com/Real-Time-Telecom-B-V/smpp34/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A pure-Rust implementation of the **SMPP 3.4** protocol (Short Message
Peer-to-Peer): a PDU codec plus an asynchronous ([tokio](https://tokio.rs))
**client** and **server** built on a listener-trait model.

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
