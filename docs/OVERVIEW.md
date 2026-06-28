# smpp34 — architecture overview

A map of the crate's internals, for contributors. (Excluded from the published
package via `Cargo.toml` `exclude`.) For usage see the [README](../README.md);
for protocol coverage see [COMPLIANCE.md](COMPLIANCE.md).

## Module map

| Path | Responsibility |
|---|---|
| `src/lib.rs` | Crate root; re-exports the public surface. |
| `src/common/mod.rs` | `CommandHeader`, `SmppError` (the SMPP status enum), `DeliveryReceipt`, the (crate-internal) `CommandId`, connection types, and shared decode helpers. |
| `src/common/commands/*.rs` | One module per PDU: struct + `encode`/`decode` + `accept`/`reject` builders. Covers the full SMPP 3.4 command set. |
| `src/common/tlv.rs` | TLV (optional parameter) codec: `Tlv`, `TlvTag`, `TlvList`, `encode_tlvs`/`decode_tlvs`. |
| `src/client/mod.rs` | `SmppClient` (ESME), the bound `SMSC` handle + `SubmitSmBuilder`, `SmppClientListener`, `BIND_TYPE`, and the `SmppConnection` (TCP/TLS) transport. |
| `src/server/mod.rs` | `SmppServer` (SMSC), the bound `ESME` handle + `DeliverSmBuilder`, `SmppServerListener`. |
| `src/server/state.rs` | Per-session server state machine + read loop. |

Tests: codec round-trip unit tests + the 1.1.0 builder/trait-default tests run in
CI (`cargo test`); `tests/framing.rs` is the pipelined-load framing regression
guard; two live-network harnesses (`tests/{client,server}.rs`) are `#[ignore]`d.
Perf/leak harness lives in `benches/`, `examples/`, `perf/`, `scripts/`.

## Read-loop framing (the core)

Both `server/state.rs` and the client's main loop read into a `BytesMut` that
**accumulates across reads**, then extract every *complete* length-delimited PDU
from the front (`split_to(command_length)`), leaving any partial tail for the
next read. TCP is a byte stream — a read can deliver several PDUs or stop
mid-PDU — so this reassembly is mandatory (the pre-1.1.1 loops assumed whole-PDU
reads and panicked/dropped data under pipelining; `tests/framing.rs` guards it).
A `command_length` < 16 or > 1 MB is treated as fatal (a length-delimited stream
can't resync from a bogus length).

## Public API surface (the SemVer contract)

- **Client:** `SmppClient::new` / `new_with_default_timers`, `start`, `stop`,
  `is_alive`; `SMSC::submit_sm()` builder (+ `send_submit_sm` / `send_data_sm` /
  `send_cancel_sm` / `send_unbind`); `SmppClientListener` (all methods defaulted);
  `BIND_TYPE`.
- **Server:** `SmppServer::new` / `new_with_default_timers`, `start`, `stop`,
  `is_alive`; `ESME::deliver_sm()` builder (+ `send_deliver_sm` / `send_data_sm` /
  `send_alert_notification` / `send_unbind`); `SmppServerListener` (all methods
  defaulted — override only what you need).
- **Codec:** every PDU struct + `SmppError`, `SmppConnectionInformation`, the TLV
  API. PDU `decode` takes `&[u8]`.

## Feature matrix

| Area | Status |
|---|---|
| PDU codec (full 3.4 set + TLV) | ✅ |
| Async client / server (TX/RX/TRX) | ✅ |
| Session timers, sequence windowing | ✅ |
| Pipelined framing (PDUs spanning reads) | ✅ (since 1.1.1) |
| TLS | ✅ via `tokio-native-tls` (system OpenSSL); system trust store only — no custom CA / mTLS wiring yet |
| Memory: flat under load + bind/unbind churn | ✅ (counting-allocator leak check) |

## Performance & memory

See the README "Performance" / "Memory" sections for the baseline numbers and
how to reproduce (`cargo bench`, `perf/docker-compose.yml`,
`scripts/mem_leak_test.sh`). Headroom not yet taken: the server spawns a `tokio`
task **per PDU** and copies each PDU with `to_vec` — `scripts/flamegraph.sh` is
the starting point for that optimization pass.

## Remaining design notes

- **TLS → rustls.** The TLS path is OpenSSL-backed (C). Moving to `rustls` would
  enable a zero-C build and custom-CA / mTLS support — a backward-compatible
  addition (new minor) when it lands.
- **Per-PDU task spawn.** The spawn-per-PDU + per-PDU `to_vec` is the obvious
  throughput lever; a fixed worker pool / borrowed-slice path is a future minor.
