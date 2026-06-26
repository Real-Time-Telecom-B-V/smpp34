# Changelog

All notable changes to `smpp34` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) — see
[`VERSIONING.md`](VERSIONING.md).

## [Unreleased]

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

[Unreleased]: https://github.com/Real-Time-Telecom-B-V/smpp34/compare/v1.0.0...main
[1.0.0]: https://github.com/Real-Time-Telecom-B-V/smpp34/releases/tag/v1.0.0
