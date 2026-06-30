# Changelog

All notable changes to `smpp34` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) — see
[`VERSIONING.md`](VERSIONING.md).

## [Unreleased]

## [1.2.0] - 2026-06-28

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

[Unreleased]: https://github.com/Real-Time-Telecom-B-V/smpp34/compare/v1.2.0...main
[1.2.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.2.0
[1.1.1]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.1.1
[1.1.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.1.0
[1.0.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.0.0
