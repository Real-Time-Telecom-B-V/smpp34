# smpp34 — release-readiness overview

A pre-release map of the crate and the decisions to make before tagging a
version. (Excluded from the published package via `Cargo.toml` `exclude`.)

## Module map

| Path | Responsibility |
|---|---|
| `src/lib.rs` | Crate root; re-exports the public surface. |
| `src/common/mod.rs` | `CommandHeader`, `SmppError` (the SMPP status enum), `DeliveryReceipt`, the (crate-internal) `CommandId`, connection types, and shared decode helpers. |
| `src/common/commands/*.rs` | One module per PDU: struct + `encode`/`decode` + `accept`/`reject` builders. Covers the full SMPP 3.4 command set. |
| `src/common/tlv.rs` | TLV (optional parameter) codec: `Tlv`, `TlvTag`, `TlvList`, `encode_tlvs`/`decode_tlvs`. |
| `src/client/mod.rs` | `SmppClient` (ESME side), the bound `SMSC` handle, `SmppClientListener`, `BIND_TYPE`, and the `SmppConnection` (TCP/TLS) transport. |
| `src/server/mod.rs` | `SmppServer` (SMSC side), the bound `ESME` handle, `SmppServerListener`. |
| `src/server/state.rs` | Per-session server state machine. |

~5.3k lines of Rust. 51 unit tests (codec round-trips) run in CI; 2 live-network
integration tests are `#[ignore]`d (need a peer on `127.0.0.1:2775`).

## Public API surface (the SemVer contract)

- **Client:** `SmppClient::new` / `new_with_default_timers`, `start`, `stop`,
  `is_alive`; `SMSC::{send_submit_sm, send_data_sm, send_cancel_sm, send_unbind}`;
  `SmppClientListener` (`on_deliver_sm`, `on_data_sm`, `on_alert_notification`,
  `on_unbind`, `on_timeout`, `on_smsc_bound`, `on_smsc_unbound`); `BIND_TYPE`.
- **Server:** `SmppServer::new` / `new_with_default_timers`, `start`, `stop`,
  `is_alive`; `ESME::{send_deliver_sm, send_data_sm, send_alert_notification,
  send_unbind}`; `SmppServerListener` (one method per inbound PDU).
- **Codec:** every PDU struct + `SmppError`, `SmppConnectionInformation`, and the
  TLV API.

## Feature matrix

| Area | Status |
|---|---|
| PDU codec (full 3.4 set + TLV) | ✅ production |
| Async client (TX/RX/TRX bind) | ✅ production |
| Async server | ✅ production |
| Session timers, sequence windowing | ✅ |
| TLS | ✅ via `tokio-native-tls` (system OpenSSL); system trust store only — no custom CA / mTLS wiring yet |
| Delivery receipts (`DeliveryReceipt`) | ✅ encode helper |

## Known rough edges (decide before locking a 1.0 API)

1. **`ptr_arg` on ~36 public signatures** — several `pub` functions take
   `&Vec<u8>` / `&String` where `&[u8]` / `&str` would do. clippy flags these
   (left unfixed on purpose: changing them edits the public API). Cheap to fix
   *now*, pre-publish; expensive after a 1.0 stability promise.
2. **Very wide constructors** — `send_submit_sm` / `send_deliver_sm` take 17
   positional args. A builder (or an args struct) would be a friendlier 1.0 API.
3. **Listener traits have no default methods** — implementors must write all
   ~10 methods even to reject everything. Adding defaults is an ergonomic win.
4. **TLS** is OpenSSL-backed (C). A future move to `rustls` enables a zero-C
   build and custom-CA/mTLS support.

None of these block a release; they're the agenda for an API-ergonomics pass.

## Release decision: version number

- **Option A — `1.0.0` now.** Signals production-ready (it is). But it freezes
  the rough edges above into the `1.x` stable contract; fixing them later is a
  `2.0`.
- **Option B — `0.x` first (e.g. `0.2.0`), then `1.0.0` after the ergonomics
  pass.** Publishes + claims the name immediately while leaving room to fix
  `ptr_arg`, the wide constructors, and listener defaults as `0.(y+1)` breaking
  changes before committing to `1.0`.

**Recommendation: Option B.** The rough edges are known and worth fixing before
a stability promise; `0.x` lets us ship now and polish toward a clean `1.0`.
(Your call — you mentioned `1.0.0`; happy to go straight there if you'd rather
lock the current API.)

## crates.io prerequisites

- **Name `smpp34` is available** (verified against the crates.io API/index).
- **First publish:** either run one manual `cargo publish` with a scoped API
  token to create + claim the crate under the RTT org, **or** pre-configure
  Trusted Publishing for the not-yet-existing crate. Subsequent releases use the
  OIDC Trusted Publishing already wired in `.github/workflows/release.yaml` (no
  stored token) — configure it on the crate pointing at
  `Real-Time-Telecom-B-V/smpp34` + `release.yaml`.
- **Tagging:** `release.yaml`'s `verify-version` refuses to publish unless
  `Cargo.toml` `version` equals the `vX.Y.Z` tag.

## History / privacy

Git history was rewritten (`git filter-repo`) before any non-personal push:
all 63 commits now carry a single `…@users.noreply.github.com` identity (the
`mvdvlies@gmail.com` address is gone), and the one stray public IP was scrubbed
from every revision. No credentials were ever committed.
