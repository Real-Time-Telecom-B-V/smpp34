//! # smpp34
//!
//! A pure-Rust implementation of the **SMPP 3.4** protocol (Short Message Peer-to-Peer):
//! PDU codec plus async ([tokio]) **client** and **server** built on a listener-trait model.
//!
//! - [`SmppClient`](client::SmppClient) binds to an SMSC as an ESME (TX / RX / TRX) and
//!   sends `submit_sm` / `data_sm`; inbound `deliver_sm` and lifecycle events are delivered
//!   to your [`SmppClientListener`](client::SmppClientListener).
//! - [`SmppServer`] accepts ESME binds and dispatches `submit_sm`, `data_sm`, `cancel_sm`,
//!   etc. to your [`SmppServerListener`]; the [`ESME`](server::ESME) handle sends `deliver_sm`.
//! - The wire codec for every PDU lives in [`common`], including optional [`Tlv`] parameters.
//!
//! Sessions manage the SMPP timers (`session_init`, `enquire_link`, `inactivity`, `response`),
//! sequence-number windowing, and (optionally) TLS.
//!
//! See the crate README for a runnable bind → `submit_sm` example.

#![allow(non_camel_case_types)]

pub mod client;
pub mod common;
pub mod server;

/// Optional PyO3 bindings (`--features python`). Compiled out of the default,
/// pyo3-free build that crates.io consumers use.
#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
pub use python::register;

pub use server::SmppServer;
pub use server::SmppServerListener;

pub use common::tlv::{decode_tlvs, encode_tlvs, tlvs_encoded_len, Tlv, TlvList, TlvTag};
pub use common::*;

#[macro_use]
extern crate num_derive;
