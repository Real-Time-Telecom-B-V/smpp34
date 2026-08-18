# Changelog

All notable changes to `smpp34` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) — see
[`VERSIONING.md`](VERSIONING.md).

## [Unreleased]

## [1.4.1] - 2026-08-18

### Fixed

- **The bind handshake now frames the byte stream instead of assuming one read is
  one PDU.** Both sides read the handshake with a single `read()` and passed the
  whole buffer to `CommandHeader::decode`, which rejects it when
  `pdu.len() != command_length`. TCP has no message boundaries, so a peer that
  puts anything on the wire straight after its bind PDU can have it coalesce into
  the same segment — and the session was then torn down before it started, with
  every request on it failing instantly.
  - SMSC -> ESME: an SMSC with traffic queued sends it the moment it accepts the
    bind, so its first `deliver_sm` catches up with its own
    `bind_transceiver_resp`. The client logged
    `PDU length 451 does not match command_length 31` then
    `Unable to decode bind response`.
  - ESME -> SMSC: symmetric. An ESME that pipelines its first `submit_sm` without
    waiting for the bind response is doing something legal, and the SMSC rejected
    the whole read.
  - Bytes that arrived behind the bind PDU are no longer discarded: they are
    replayed ahead of the socket so the session read loop frames them like any
    other bytes. (Parking them in the read buffer does not work — that loop only
    drains after a read returns data, so a peer that then went quiet would strand
    them.)

  This surfaced as a roughly 1-in-20 failure of
  `deliver_sm_resp_is_never_dropped_under_load` under two-CPU pinning, where all
  20 000 sends failed at once rather than the handful a correlation race would
  lose. It predates the 1.4.0 changes — reproduced at the same rate on the commit
  before them.

## [1.4.0] - 2026-08-17

Nothing in this release changes an existing signature or documented guarantee a
caller could depend on; it removes ways the library could take your process down.
`src/` went from **50** `.unwrap()`/`.expect()` calls outside `#[cfg(test)]` to
**zero**, and several of those were reachable straight from the wire.

### Added

- `SmppClientListener::on_connection_failed` and
  `SmppServerListener::on_listen_failed` — the session could not be established,
  or the listening socket could not be bound. Both are defaulted to a no-op, so
  existing implementors are unaffected.
- `SmppError::from_command_status`, the non-panicking wire-status mapping.

### Fixed

- **A failed connect is reported instead of panicking a tokio worker.**
  `SmppClient::start` opened its socket inside the spawned session task and
  `.unwrap()`ed the result, so an ordinary refused connect panicked a runtime
  worker. The panic did not even settle the pending bind — the listener owns that
  channel — so the caller waited out its entire connect timeout and then reported
  `bind timed out`, naming the wrong end of the session. The socket is now opened
  before the task is spawned and the real cause is delivered to
  `on_connection_failed`, which the Python `connect()` surfaces verbatim
  (`TCP connect to 127.0.0.1:9 failed: Connection refused`).
- **`SmppServer::start` is accepting by the time it returns.** It bound its
  `TcpListener` inside the spawned accept loop, so "started" only meant
  "scheduled": connecting immediately afterwards was a race. Every test in this
  crate hid it with a `sleep(100 ms)`; the Python suite did not sleep and failed
  under free-threaded CPython in CI. `start` now binds first, a bind error goes to
  `on_listen_failed` (Python's `Server.start()` raises it) instead of being
  `.unwrap()`ed, and an `accept` error stops the loop with a logged reason rather
  than panicking. The sleeps are gone from the tests.
- **A `command_status` the peer is entitled to send no longer panics the session.**
  `get_error()` was `FromPrimitive::from_u32(status).expect(..)` on all 14 response
  types, so any status outside our enum aborted the task that read it. SMPP 3.4
  §5.1.3 leaves `0x00000400`-`0x000004FF` explicitly vendor-specific and reserves
  several other ranges, and real SMSCs use them — and `get_error()` is called
  directly on what the peer sent (`generic_nack.get_error()`,
  `bind_*_resp.get_error()`). Unknown statuses now map to `ESME_RUNKNOWNERR` and
  are logged with their raw value, which stays readable via `command_status()`.
- **Response timers use a monotonic clock.** The pending-request maps keyed on
  `SystemTime` and called `.expect("Unable to elapse")` on every check, so a
  wall-clock step backwards (NTP, suspend/resume) either skewed a timeout or
  panicked outright. They now use `Instant`, which is monotonic and whose
  `elapsed()` cannot fail — removing 20 panic sites and the underlying bug.
- **`stop()` on something that never started is a no-op.** It was
  `self.handle.take().expect("Called stop on non-running thread")`, reachable on
  exactly the instance a caller is most likely to clean up now that a failed
  connect or bind returns instead of panicking. `Drop` goes through the same path.
- **A peer that leaves mid-write no longer panics us.** Sending a bind rejection
  or a `generic_nack` to a disconnected peer used to be
  `.expect("Can not write to stream")`. The failure is now logged with the
  connection's addresses; the session was being torn down regardless.
- **Framing reads cannot panic.** The four `.expect("Can not read PDU length")` /
  `.expect("Can not read sequence_number")` sites read header fields before the
  header was validated. They go through `be_u32_at`, which yields 0 out of range —
  the value SMPP 3.4 advises for an undeterminable sequence_number, and a length
  that fails the existing `< 16` framing check.
- `Vec::with_capacity` on a `command_length` no longer unwraps a `try_into`;
  capacity is a hint, so an unrepresentable length falls back to 0.
- The Python `submit_sm` responder tolerates a poisoned mutex instead of
  unwrapping it, which would have left the peer's `submit_sm` unanswered.

## [1.3.0] - 2026-08-03

### Added

- **TLVs can be sent.** Decoding optional parameters always worked (`pdu.tlvs`),
  but nothing could put one on the wire: `submit_sm::new` / `data_sm::new` were
  `pub(crate)` and hardcoded an empty TLV list, and no `send_*` method or builder
  took any. So no `message_payload` past the 254-byte `short_message` limit, no
  `sar_*` concatenation, no delivery receipt with `receipted_message_id` /
  `message_state`, no vendor tags.
  - Builders: `.tlv(TlvTag, value)`, `.tlv_raw(u16, value)`, `.tlvs(iter)` on
    `SubmitSmBuilder` and `DeliverSmBuilder`.
  - Pre-built PDUs (relaying a decoded PDU, or anything the fixed-argument
    `send_*` cannot express): `SMSC::send_submit_sm_pdu` /
    `send_data_sm_pdu` / `send_submit_sm_multi_pdu`, `ESME::send_deliver_sm_pdu`
    / `send_data_sm_pdu`. The session assigns the sequence number.
  - Codec: `submit_sm::new` and `data_sm::new` are now `pub`, and every message
    PDU has `with_tlvs(iter)` / `push_tlv(tlv)`. `command_length` is recomputed
    at encode time, so TLVs can be attached in any order.
  - `Tlv::from_u8` / `from_u16` / `from_u32` / `from_c_string` for the typed
    values, mirroring the existing `as_*` accessors. `TlvTag::ALL` enumerates the
    44 spec tags.
  - Python: `smpp34.Tlv`, a `tlvs=` keyword on `SubmitSm` / `DeliverSm`,
    `Smsc.submit_sm()` and `Esme.deliver_sm()`, a `.tlvs` property on those and
    on `SubmitSmEvent` / `DeliverSmEvent`, and the tag constants as
    `smpp34.TLV_*`.
- **`data_sm` supports TLVs at all.** It had no `tlvs` field, so it could neither
  decode nor encode them — and a `data_sm` carries its message *only* in the
  `message_payload` TLV (§4.2.2), so every `data_sm` this library sent was an
  empty message and every one it received lost its body. `data_sm_resp` likewise
  gained TLVs; it is the one response PDU in 3.4 with optional parameters
  (§4.2.3: `delivery_failure_reason`, `network_error_code`,
  `additional_status_info_text`, `dpf_result`).

### Fixed

- **`alert_notification` decoded every field 16 bytes off.** Its `decode` parsed
  from byte 0 while every other PDU's `decode` skips the header itself, and the
  client read loop hands it the whole PDU — so an inbound `alert_notification`
  reached `on_alert_notification` with `source_addr_ton` taken from
  `command_length` and the addresses shredded. It parsed without erroring, which
  is why it went unnoticed. `decode` now takes a complete PDU like the rest.
- **`alert_notification` encoded `ms_availability_status` as a bare octet.** It
  is optional parameter 0x0422 (§4.12.1) and belongs in a TLV, so what went on
  the wire was malformed and a spec-compliant peer's version was misparsed. It
  is now TLV-encoded; a lone trailing octet is still accepted on decode, since
  peers running smpp34 ≤ 1.2.1 emit that form and the two cannot be confused (a
  TLV is at least 4 bytes).
- The wrong-bind-direction panic in `SMSC::send_submit_sm` said "Can not send
  deliver_sm on non RX/TRX bind".

## [1.2.1] - 2026-07-27

### Fixed

- **Register a pending request before its PDU goes on the wire.** Both writer
  tasks (`client/mod.rs` for ESME to SMSC, `server/state.rs` for SMSC to ESME)
  inserted into `pending_requests` only after the socket write returned. The read
  loop drops any response it has no pending entry for, so a response that came
  back while the writer task sat between its write and its insert was lost, and
  the caller blocked until its response timer expired (30s by default) instead of
  getting the `submit_sm_resp` or `deliver_sm_resp` that had already arrived.
  Registration now happens first, which closes the window by construction. A
  failed write removes the registration again, so the caller fails immediately
  rather than waiting out the timer for a PDU that never left.
- New `tests/response_correlation.rs` drives 20k pipelined requests in each
  direction and asserts every one correlates. Note this guards correlation under
  load, it does not reproduce the race above on demand.

  The race does reproduce, but only with the load pinned to **exactly two CPUs**.
  One CPU makes tokio run a single worker, which removes the preemption point
  altogether, and two CPUs under heavy competing load masks it again. Pinned to
  two CPUs at 5000 pipelined requests per run, 10 of 30 runs lost at least one
  response on 1.2.0 and 0 of 52 did on 1.2.1. A lost response shows up as
  `No pending request for sequence_number N` followed, one response timer later,
  by `... with sequence_number N timed out` for that same N.

## [1.2.0] - 2026-06-30

### Added

- **Python bindings (`pip install smpp34`).** A Rust-backed wheel, built from the
  same source tree and version, exposing the async client/server to `asyncio` plus
  a pure codec — additive and behind an optional `python` Cargo feature, so
  `cargo add smpp34` and crates.io consumers still pull **zero** pyo3.
  - Async API: `smpp34.Client` / `Smsc` (connect, `submit_sm`, pull inbound
    `DeliverSmEvent` via `next()`, `unbind`), `smpp34.Server` / `Esme` (`start`,
    pull `Esme` / `SubmitSmEvent` / `Unbound` via `next()`, `deliver_sm`,
    `accept`/`reject`). The hot path stays 100% in the Rust/tokio core — Python
    crosses the GIL once per message via an event-pull bridge (PyO3 +
    pyo3-async-runtimes).
  - **Free-threaded ("no-GIL") ready** — the module declares `gil_used = false`
    and all shared state is `Arc`/channel-based.
  - Codec API: `SubmitSm` / `DeliverSm` / `RawPdu` classes + `decode()`; abi3
    wheels (CPython 3.9+) and version-specific free-threaded wheels.
  - `python/examples/` (client + server) and a Python throughput/leak harness in
    `python/perf/`, mirroring the Rust `examples/`.
- Simple Rust samples `examples/client.rs` and `examples/server.rs` showing the
  full bidirectional flow (submit_sm + deliver_sm).
- **`query_sm` / `replace_sm` fully wired** (requested by siphon-smpp): server
  hooks `SmppServerListener::on_query_sm` / `on_replace_sm` (default-reject,
  dispatched in TX/TRX), and `SMSC::send_query_sm` / `send_replace_sm` with
  response correlation. Guarded by `tests/query_replace.rs`.
- `cancel_sm` and `alert_notification` PDU fields are now `pub` (matching
  `query_sm` / `replace_sm` / `submit_sm`), so inbound handlers can read the
  message_id / addressing / `ms_availability_status`.
- `SMSC::can_send()` predicate (mirrors `ESME::can_receive`) to gate outbound
  sends before a wrong-direction bind would panic.
- **`submit_sm_multi` fully implemented** (was a stub): the `dest_address` list
  (`DestAddress::Sme` / `DistributionList`), `submit_sm_multi_resp` with its
  `unsuccess_sme` failure list (`UnsuccessSme`), `SmppServerListener::on_submit_sm_multi`
  + dispatch, and `SMSC::send_submit_sm_multi`. Guarded by codec unit tests and
  the `tests/query_replace.rs` round-trip.

### Changed

- `Cargo.toml`: `crate-type = ["cdylib", "rlib"]` and an optional pyo3 dependency
  behind `python` / `extension-module` features. CI now lints/tests the default
  (pyo3-free) build **and** the `python` feature separately — never `--all-features`
  (which would enable `extension-module` and break the Rust test link).

## [1.1.1] - 2026-06-28

### Fixed

- **Framing under pipelined load.** The server and client read loops assumed each
  TCP read delivered a whole number of complete PDUs (they sliced past the bytes
  actually read and `clear()`ed the buffer every read), so under pipelining they
  panicked (`range end index … out of range`) or dropped PDUs. Replaced with an
  accumulating, length-delimited framer that reassembles PDUs across reads.
  Found by the new perf harness; guarded by `tests/framing.rs`.
- **Session-teardown leak.** A client session orphaned its `enquire_link`
  keep-alive task on close (the abort was commented out), leaking ~4 KB per
  bind/unbind. Found by the bind/unbind memory-leak check; guarded by
  `examples/leak_check.rs`.

### Added

- Criterion codec benchmarks (`benches/codec.rs`); a real-TCP perf harness
  (`examples/perf_smsc` + `perf_esme`, `perf/docker-compose.yml`); a
  counting-allocator memory-leak check (`examples/leak_check.rs` +
  `scripts/mem_leak_test.sh`); flamegraph tooling (`examples/perf_loopback` +
  `scripts/flamegraph.sh`).
- Performance + memory baselines in the README; SMPP 3.4 compliance matrix
  (`docs/COMPLIANCE.md`) and a comparison to other SMPP stacks
  (`docs/COMPARISON.md`).

## [1.1.0] - 2026-06-26

### Added

- Fluent builders `SMSC::submit_sm()` and `ESME::deliver_sm()` — an ergonomic
  alternative to the 17-argument `send_submit_sm` / `send_deliver_sm` (which
  remain available). Setters take `impl Into<String>` / `impl Into<Vec<u8>>`
  and every field defaults to `0` / empty.
- Default implementations for every `SmppClientListener` / `SmppServerListener`
  method, so an implementor overrides only the callbacks it needs (binds reject,
  `on_submit_sm` / `on_data_sm` reject, `on_unbind` acks, notifications no-op).

### Changed

- PDU `decode` methods now take `&[u8]` instead of `&Vec<u8>` (callers passing
  `&vec` are unaffected — `&Vec<u8>` coerces).
- Dependency updates: `nom` 7 → 8, `env_logger` 0.10 → 0.11, minor/patch bumps
  (tokio, log, bytes, chrono, uuid, test-log), and CI action versions.

## [1.0.0] - 2026-06-26

First public release. The crate has existed and been used in production
privately; this is the initial open-source cut under the MIT license.

### Added

- SMPP 3.4 PDU codec for the full command set (`bind_*`, `outbind`, `unbind`,
  `enquire_link`, `submit_sm`, `submit_sm_multi`, `deliver_sm`, `data_sm`,
  `query_sm`, `cancel_sm`, `replace_sm`, `alert_notification`, `generic_nack`)
  plus TLV (optional parameter) encode/decode.
- Async ([tokio](https://tokio.rs)) `SmppClient` (ESME) and `SmppServer` (SMSC)
  with a listener-trait dispatch model, SMPP session timers, sequence-number
  windowing, and optional TLS.

### Changed

- Packaged for crates.io: MIT license, crate metadata, README, `VERSIONING.md`,
  CI / release / audit workflows, `cargo-deny` policy.
- Removed the unused `tokio-rustls` dependency (the TLS path uses
  `tokio-native-tls`); moved `env_logger` / `test-log` to dev-dependencies.

[Unreleased]: https://github.com/Real-Time-Telecom-B-V/smpp34/compare/v1.4.1...main
[1.4.1]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.4.1
[1.4.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.4.0
[1.3.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.3.0
[1.2.1]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.2.1
[1.2.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.2.0
[1.1.1]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.1.1
[1.1.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.1.0
[1.0.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.0.0
