//! PyO3 bindings — `pip install smpp34` gives a Rust-backed wheel exposing the
//! **same** codec and (Phase 2) the async tokio client/server to Python asyncio.
//!
//! Compiled only with `--features python`; the default crate build is pyo3-free, so
//! `cargo add smpp34` / crates.io consumers pull zero pyo3. Two entry points share one
//! `add_contents()`:
//! * `#[pymodule] fn _smpp34` — the standalone wheel (maturin `module-name`).
//! * `pub fn register(py, parent)` — mount `smpp34` as a submodule of another extension
//!   (mirrors the `sms-tpdu-py::register` embedding pattern in rtt-infra).
//!
//! Performance contract: the hot path (framing, windowing, timers, encode/decode) stays
//! 100% in Rust/tokio. Nothing here touches the byte stream per-syscall — only per
//! application message. See `~/.claude/plans/dapper-tumbling-mountain.md`.
#![allow(clippy::too_many_arguments)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use num_traits::FromPrimitive;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};

use crate::client::{SmppClient, SmppClientListener, BIND_TYPE, SMSC};
use crate::common::CommandId;
use crate::server::{SmppServer, SmppServerListener, ESME};
use crate::{
    deliver_sm, deliver_sm_resp, submit_sm, submit_sm_resp, CommandHeader,
    SmppConnectionInformation, SmppError as CoreSmppError,
};

// ── Error mapping ───────────────────────────────────────────────────────────
// Codec failures surface as `smpp34.SmppError`, whose message carries the SMPP
// command_status variant + numeric value (e.g. "ESME_RINVPARLEN (0x00000005)").
create_exception!(
    smpp34,
    SmppError,
    PyException,
    "SMPP protocol / codec error (message carries the command_status)."
);

fn smpp_err(e: CoreSmppError) -> PyErr {
    SmppError::new_err(format!("{e:?} (0x{:08X})", e as u32))
}

// ── Message PDUs (submit_sm / deliver_sm — structurally identical) ───────────
// `header` is module-private on the core struct, so we carry `sequence_number`
// alongside `inner` (sourced from the constructor arg or the parsed header).
//
// rustfmt is non-idempotent on the multi-line `#[pyo3(signature = ...)]` attribute
// inside a declarative macro body, so freeze the macro's formatting.
#[rustfmt::skip]
macro_rules! message_pdu {
    ($cls:ident, $inner:ty, $new:path, $cmd:path, $doc:literal) => {
        #[doc = $doc]
        #[pyclass(module = "smpp34._smpp34")]
        pub struct $cls {
            inner: $inner,
            sequence_number: u32,
        }

        #[pymethods]
        impl $cls {
            #[new]
            #[pyo3(signature = (
                source_addr,
                destination_addr,
                short_message = Vec::new(),
                *,
                service_type = String::new(),
                source_addr_ton = 0,
                source_addr_npi = 0,
                dest_addr_ton = 0,
                dest_addr_npi = 0,
                esm_class = 0,
                protocol_id = 0,
                priority_flag = 0,
                schedule_delivery_time = String::new(),
                validity_period = String::new(),
                registered_delivery = 0,
                replace_if_present_flag = 0,
                data_coding = 0,
                sm_default_msg_id = 0,
                sequence_number = 1,
            ))]
            fn new(
                source_addr: String,
                destination_addr: String,
                short_message: Vec<u8>,
                service_type: String,
                source_addr_ton: u8,
                source_addr_npi: u8,
                dest_addr_ton: u8,
                dest_addr_npi: u8,
                esm_class: u8,
                protocol_id: u8,
                priority_flag: u8,
                schedule_delivery_time: String,
                validity_period: String,
                registered_delivery: u8,
                replace_if_present_flag: u8,
                data_coding: u8,
                sm_default_msg_id: u8,
                sequence_number: u32,
            ) -> PyResult<Self> {
                if short_message.len() > 254 {
                    return Err(PyValueError::new_err(
                        "short_message exceeds the 254-byte SMPP limit; use message_payload TLV",
                    ));
                }
                let inner = $new(
                    sequence_number,
                    service_type,
                    source_addr_ton,
                    source_addr_npi,
                    source_addr,
                    dest_addr_ton,
                    dest_addr_npi,
                    destination_addr,
                    esm_class,
                    protocol_id,
                    priority_flag,
                    schedule_delivery_time,
                    validity_period,
                    registered_delivery,
                    replace_if_present_flag,
                    data_coding,
                    sm_default_msg_id,
                    short_message,
                );
                Ok(Self {
                    inner,
                    sequence_number,
                })
            }

            #[getter]
            fn command_id(&self) -> u32 {
                $cmd as u32
            }
            #[getter]
            fn sequence_number(&self) -> u32 {
                self.sequence_number
            }
            #[getter]
            fn service_type(&self) -> String {
                self.inner.service_type.clone()
            }
            #[getter]
            fn source_addr_ton(&self) -> u8 {
                self.inner.source_addr_ton
            }
            #[getter]
            fn source_addr_npi(&self) -> u8 {
                self.inner.source_addr_npi
            }
            #[getter]
            fn source_addr(&self) -> String {
                self.inner.source_addr.clone()
            }
            #[getter]
            fn dest_addr_ton(&self) -> u8 {
                self.inner.dest_addr_ton
            }
            #[getter]
            fn dest_addr_npi(&self) -> u8 {
                self.inner.dest_addr_npi
            }
            #[getter]
            fn destination_addr(&self) -> String {
                self.inner.destination_addr.clone()
            }
            #[getter]
            fn esm_class(&self) -> u8 {
                self.inner.esm_class
            }
            #[getter]
            fn protocol_id(&self) -> u8 {
                self.inner.protocol_id
            }
            #[getter]
            fn priority_flag(&self) -> u8 {
                self.inner.priority_flag
            }
            #[getter]
            fn schedule_delivery_time(&self) -> String {
                self.inner.schedule_delivery_time.clone()
            }
            #[getter]
            fn validity_period(&self) -> String {
                self.inner.validity_period.clone()
            }
            #[getter]
            fn registered_delivery(&self) -> u8 {
                self.inner.registered_delivery
            }
            #[getter]
            fn replace_if_present_flag(&self) -> u8 {
                self.inner.replace_if_present_flag
            }
            #[getter]
            fn data_coding(&self) -> u8 {
                self.inner.data_coding
            }
            #[getter]
            fn sm_default_msg_id(&self) -> u8 {
                self.inner.sm_default_msg_id
            }
            #[getter]
            fn sm_length(&self) -> u8 {
                self.inner.sm_length
            }

            #[getter]
            fn short_message<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
                PyBytes::new(py, &self.inner.short_message)
            }

            /// Encode to wire bytes (a complete SMPP PDU, header + body).
            fn encode<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
                PyBytes::new(py, &self.inner.clone().encode())
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!(
                        stringify!($cls),
                        "(seq={}, src={:?}, dst={:?}, dcs=0x{:02x}, len={})"
                    ),
                    self.sequence_number,
                    self.inner.source_addr,
                    self.inner.destination_addr,
                    self.inner.data_coding,
                    self.inner.sm_length,
                )
            }
        }

        impl $cls {
            fn from_decoded(inner: $inner, sequence_number: u32) -> Self {
                Self {
                    inner,
                    sequence_number,
                }
            }
        }
    };
}

message_pdu!(
    SubmitSm,
    crate::submit_sm,
    crate::submit_sm::new,
    CommandId::submit_sm,
    "An SMPP `submit_sm` PDU (ESME → SMSC). `short_message` is `bytes`."
);
message_pdu!(
    DeliverSm,
    crate::deliver_sm,
    crate::deliver_sm::new,
    CommandId::deliver_sm,
    "An SMPP `deliver_sm` PDU (SMSC → ESME). `short_message` is `bytes`."
);

// ── Generic fallback for PDUs without a dedicated wrapper ────────────────────
/// A decoded SMPP PDU header plus the raw body, returned by [`decode`] for any
/// `command_id` without a dedicated Python class. Lets `decode()` never fail on
/// an otherwise-valid PDU.
#[pyclass(module = "smpp34._smpp34")]
pub struct RawPdu {
    #[pyo3(get)]
    command_length: u32,
    #[pyo3(get)]
    command_id: u32,
    #[pyo3(get)]
    command_status: u32,
    #[pyo3(get)]
    sequence_number: u32,
    body: Vec<u8>,
}

#[pymethods]
impl RawPdu {
    /// The PDU body (everything after the 16-byte header).
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.body)
    }

    fn __repr__(&self) -> String {
        format!(
            "RawPdu(command_id=0x{:08x}, command_status=0x{:08x}, seq={}, body_len={})",
            self.command_id,
            self.command_status,
            self.sequence_number,
            self.body.len(),
        )
    }
}

/// Decode a single complete SMPP PDU from `data` into a typed object.
///
/// Returns a [`SubmitSm`] / [`DeliverSm`] for those command IDs, otherwise a
/// [`RawPdu`]. Raises `smpp34.SmppError` if the header is malformed or
/// `len(data)` does not equal the PDU's `command_length`.
#[pyfunction]
fn decode<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    let header = CommandHeader::decode(data).map_err(smpp_err)?;
    let cmd = header.command_id;
    let seq = header.sequence_number;

    if cmd == CommandId::submit_sm as u32 {
        let inner = crate::submit_sm::decode(header, data).map_err(smpp_err)?;
        Ok(Bound::new(py, SubmitSm::from_decoded(inner, seq))?.into_any())
    } else if cmd == CommandId::deliver_sm as u32 {
        let inner = crate::deliver_sm::decode(header, data).map_err(smpp_err)?;
        Ok(Bound::new(py, DeliverSm::from_decoded(inner, seq))?.into_any())
    } else {
        let raw = RawPdu {
            command_length: header.command_length,
            command_id: header.command_id,
            command_status: header.command_status,
            sequence_number: seq,
            body: data.get(16..).unwrap_or(&[]).to_vec(),
        };
        Ok(Bound::new(py, raw)?.into_any())
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Async client/server bridge
//
// The performance contract: the whole SMPP machinery (framing, windowing, timers,
// encode/decode) runs in Rust/tokio. Python only crosses the GIL once per
// application message, via an EVENT-PULL model:
//   * inbound PDUs are forwarded into a tokio mpsc channel; Python `await`s them.
//   * sends are `future_into_py` over the existing async `SMSC`/`ESME` methods.
//   * the one callback that must return a value to the session loop (server
//     `on_submit_sm`) carries a `oneshot` responder — Python's `accept`/`reject`
//     answers it. No Rust->Python coroutine calls anywhere on the hot path.
// ════════════════════════════════════════════════════════════════════════════

const DEFAULT_INBOUND_CAP: usize = 1024;

fn map_error(code: u32) -> CoreSmppError {
    CoreSmppError::from_u32(code).unwrap_or(CoreSmppError::ESME_RSYSERR)
}

// ── Response wrappers ───────────────────────────────────────────────────────
/// Result of `Smsc.submit_sm(...)`.
#[pyclass(module = "smpp34._smpp34")]
pub struct SubmitSmResp {
    #[pyo3(get)]
    message_id: Option<String>,
    #[pyo3(get)]
    command_status: u32,
}

#[pymethods]
impl SubmitSmResp {
    #[getter]
    fn is_success(&self) -> bool {
        self.command_status == CoreSmppError::ESME_ROK as u32
    }
    fn __repr__(&self) -> String {
        format!(
            "SubmitSmResp(message_id={:?}, command_status=0x{:08x})",
            self.message_id, self.command_status
        )
    }
}

/// Result of `Esme.deliver_sm(...)`.
#[pyclass(module = "smpp34._smpp34")]
pub struct DeliverSmResp {
    #[pyo3(get)]
    command_status: u32,
}

#[pymethods]
impl DeliverSmResp {
    #[getter]
    fn is_success(&self) -> bool {
        self.command_status == CoreSmppError::ESME_ROK as u32
    }
    fn __repr__(&self) -> String {
        format!(
            "DeliverSmResp(command_status=0x{:08x})",
            self.command_status
        )
    }
}

// ── Inbound events ──────────────────────────────────────────────────────────
/// An inbound `deliver_sm` (SMSC -> ESME: MO message or delivery receipt),
/// yielded by `Smsc.next()`. The SMPP-level ACK is sent automatically by the
/// Rust core (a NAK is returned to the peer only under inbound backpressure).
#[pyclass(module = "smpp34._smpp34")]
pub struct DeliverSmEvent {
    inner: deliver_sm,
}

#[pymethods]
impl DeliverSmEvent {
    #[getter]
    fn service_type(&self) -> String {
        self.inner.service_type.clone()
    }
    #[getter]
    fn source_addr(&self) -> String {
        self.inner.source_addr.clone()
    }
    #[getter]
    fn source_addr_ton(&self) -> u8 {
        self.inner.source_addr_ton
    }
    #[getter]
    fn source_addr_npi(&self) -> u8 {
        self.inner.source_addr_npi
    }
    #[getter]
    fn destination_addr(&self) -> String {
        self.inner.destination_addr.clone()
    }
    #[getter]
    fn dest_addr_ton(&self) -> u8 {
        self.inner.dest_addr_ton
    }
    #[getter]
    fn dest_addr_npi(&self) -> u8 {
        self.inner.dest_addr_npi
    }
    #[getter]
    fn esm_class(&self) -> u8 {
        self.inner.esm_class
    }
    #[getter]
    fn protocol_id(&self) -> u8 {
        self.inner.protocol_id
    }
    #[getter]
    fn data_coding(&self) -> u8 {
        self.inner.data_coding
    }
    #[getter]
    fn registered_delivery(&self) -> u8 {
        self.inner.registered_delivery
    }

    #[getter]
    fn short_message<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.short_message)
    }

    fn __repr__(&self) -> String {
        format!(
            "DeliverSmEvent(src={:?}, dst={:?}, dcs=0x{:02x}, len={})",
            self.inner.source_addr,
            self.inner.destination_addr,
            self.inner.data_coding,
            self.inner.sm_length,
        )
    }
}

/// Sentinel yielded by `Smsc.next()` when the session has dropped (unbind or
/// transport error). The `Smsc` must not be used after this.
#[pyclass(module = "smpp34._smpp34")]
pub struct Disconnected {}

#[pymethods]
impl Disconnected {
    fn __repr__(&self) -> &'static str {
        "Disconnected()"
    }
}

/// An inbound `submit_sm` (ESME -> SMSC), yielded by `Server.next()`. The server
/// MUST answer with `accept(message_id)` or `reject(command_status)`; if the
/// event is dropped without a decision the core NAKs with `ESME_RSYSERR`.
#[pyclass(module = "smpp34._smpp34")]
pub struct SubmitSmEvent {
    req: submit_sm,
    #[pyo3(get)]
    session_id: String,
    responder: std::sync::Mutex<Option<oneshot::Sender<submit_sm_resp>>>,
}

#[pymethods]
impl SubmitSmEvent {
    #[getter]
    fn service_type(&self) -> String {
        self.req.service_type.clone()
    }
    #[getter]
    fn source_addr(&self) -> String {
        self.req.source_addr.clone()
    }
    #[getter]
    fn source_addr_ton(&self) -> u8 {
        self.req.source_addr_ton
    }
    #[getter]
    fn source_addr_npi(&self) -> u8 {
        self.req.source_addr_npi
    }
    #[getter]
    fn destination_addr(&self) -> String {
        self.req.destination_addr.clone()
    }
    #[getter]
    fn dest_addr_ton(&self) -> u8 {
        self.req.dest_addr_ton
    }
    #[getter]
    fn dest_addr_npi(&self) -> u8 {
        self.req.dest_addr_npi
    }
    #[getter]
    fn esm_class(&self) -> u8 {
        self.req.esm_class
    }
    #[getter]
    fn protocol_id(&self) -> u8 {
        self.req.protocol_id
    }
    #[getter]
    fn data_coding(&self) -> u8 {
        self.req.data_coding
    }
    #[getter]
    fn registered_delivery(&self) -> u8 {
        self.req.registered_delivery
    }

    #[getter]
    fn short_message<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.req.short_message)
    }

    /// Accept the message, returning the given SMSC-assigned message_id.
    fn accept(&self, message_id: String) -> PyResult<()> {
        match self.responder.lock().unwrap().take() {
            Some(tx) => {
                let _ = tx.send(self.req.clone().accept(message_id));
                Ok(())
            }
            None => Err(PyRuntimeError::new_err("submit_sm already answered")),
        }
    }

    /// Reject the message with an SMPP command_status (e.g. `smpp34.ESME_RMSGQFUL`).
    #[pyo3(signature = (command_status = CoreSmppError::ESME_RSYSERR as u32))]
    fn reject(&self, command_status: u32) -> PyResult<()> {
        match self.responder.lock().unwrap().take() {
            Some(tx) => {
                let _ = tx.send(self.req.clone().reject(map_error(command_status)));
                Ok(())
            }
            None => Err(PyRuntimeError::new_err("submit_sm already answered")),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "SubmitSmEvent(session={:?}, src={:?}, dst={:?}, len={})",
            self.session_id, self.req.source_addr, self.req.destination_addr, self.req.sm_length,
        )
    }
}

/// Yielded by `Server.next()` when a bound ESME session ends.
#[pyclass(module = "smpp34._smpp34")]
pub struct Unbound {
    #[pyo3(get)]
    session_id: String,
}

#[pymethods]
impl Unbound {
    fn __repr__(&self) -> String {
        format!("Unbound(session_id={:?})", self.session_id)
    }
}

// ── Event enums forwarded over the channels (plain Send data, no GIL) ────────
enum ClientEvent {
    DeliverSm(deliver_sm),
    Disconnected,
}

impl<'py> IntoPyObject<'py> for ClientEvent {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ClientEvent::DeliverSm(inner) => {
                Ok(Bound::new(py, DeliverSmEvent { inner })?.into_any())
            }
            ClientEvent::Disconnected => Ok(Bound::new(py, Disconnected {})?.into_any()),
        }
    }
}

enum ServerEvent {
    EsmeBound(Arc<ESME>),
    // `submit_sm` is ~232 bytes; box it so the channel element stays small.
    SubmitSm {
        req: Box<submit_sm>,
        session_id: String,
        responder: oneshot::Sender<submit_sm_resp>,
    },
    Unbound(String),
}

impl<'py> IntoPyObject<'py> for ServerEvent {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ServerEvent::EsmeBound(inner) => Ok(Bound::new(py, Esme { inner })?.into_any()),
            ServerEvent::SubmitSm {
                req,
                session_id,
                responder,
            } => Ok(Bound::new(
                py,
                SubmitSmEvent {
                    req: *req,
                    session_id,
                    responder: std::sync::Mutex::new(Some(responder)),
                },
            )?
            .into_any()),
            ServerEvent::Unbound(session_id) => {
                Ok(Bound::new(py, Unbound { session_id })?.into_any())
            }
        }
    }
}

// ── Forwarding listeners (the entire Rust<->Python bridge) ───────────────────
struct ClientForwarder {
    bind_tx: std::sync::Mutex<Option<oneshot::Sender<Arc<SMSC>>>>,
    events_tx: mpsc::Sender<ClientEvent>,
}

#[async_trait]
impl SmppClientListener for ClientForwarder {
    async fn on_smsc_bound(&self, smsc: SMSC, _session_id: &String) {
        if let Some(tx) = self.bind_tx.lock().unwrap().take() {
            let _ = tx.send(Arc::new(smsc));
        }
    }

    async fn on_deliver_sm(
        &self,
        deliver_sm: deliver_sm,
        _c: &SmppConnectionInformation,
        _session_id: &String,
    ) -> deliver_sm_resp {
        // Forward a clone; keep the original to ACK/NAK. try_send gives real
        // backpressure: a full inbound queue NAKs the peer (ESME_RMSGQFUL)
        // rather than buffering unboundedly.
        match self
            .events_tx
            .try_send(ClientEvent::DeliverSm(deliver_sm.clone()))
        {
            Ok(()) => deliver_sm.accept(),
            Err(_) => deliver_sm.reject(CoreSmppError::ESME_RMSGQFUL),
        }
    }

    async fn on_smsc_unbound(&self, _session_id: &String) {
        let _ = self.events_tx.try_send(ClientEvent::Disconnected);
    }
}

struct ServerForwarder {
    system_id: String,
    events_tx: mpsc::Sender<ServerEvent>,
}

#[async_trait]
impl SmppServerListener for ServerForwarder {
    async fn on_bind_transmitter(
        &self,
        req: crate::bind_transmitter,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> crate::bind_transmitter_resp {
        req.accept(self.system_id.clone(), Some(0x34))
    }
    async fn on_bind_receiver(
        &self,
        req: crate::bind_receiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> crate::bind_receiver_resp {
        req.accept(self.system_id.clone(), Some(0x34))
    }
    async fn on_bind_transceiver(
        &self,
        req: crate::bind_transceiver,
        _c: &SmppConnectionInformation,
        _s: &String,
    ) -> crate::bind_transceiver_resp {
        req.accept(self.system_id.clone(), Some(0x34))
    }

    async fn on_esme_bound(&self, esme: ESME, _session_id: &String) {
        let _ = self
            .events_tx
            .try_send(ServerEvent::EsmeBound(Arc::new(esme)));
    }

    async fn on_submit_sm(
        &self,
        req: submit_sm,
        _c: &SmppConnectionInformation,
        session_id: &String,
    ) -> submit_sm_resp {
        let (rtx, rrx) = oneshot::channel();
        let event = ServerEvent::SubmitSm {
            req: Box::new(req.clone()),
            session_id: session_id.clone(),
            responder: rtx,
        };
        match self.events_tx.try_send(event) {
            // Wait for Python's accept/reject; if the event is dropped undecided
            // (sender gone), NAK with a system error.
            Ok(()) => rrx
                .await
                .unwrap_or_else(|_| req.reject(CoreSmppError::ESME_RSYSERR)),
            // Inbound backpressure -> protocol-level flow control.
            Err(_) => req.reject(CoreSmppError::ESME_RMSGQFUL),
        }
    }

    async fn on_esme_unbound(&self, session_id: &String) {
        let _ = self
            .events_tx
            .try_send(ServerEvent::Unbound(session_id.clone()));
    }
}

// ── Python-facing handles ───────────────────────────────────────────────────
/// A bound ESME session (client side). Send with `submit_sm(...)`, receive
/// inbound `deliver_sm` with `await next()`, and `await unbind()` to close.
#[pyclass(module = "smpp34._smpp34")]
pub struct Smsc {
    inner: Arc<SMSC>,
    events_rx: Arc<TokioMutex<mpsc::Receiver<ClientEvent>>>,
    // Keep the owning SmppClient alive for as long as a handle to the session
    // exists, so dropping the Python `Client` first does not kill the session.
    _client: Arc<TokioMutex<SmppClient>>,
}

#[pymethods]
impl Smsc {
    #[getter]
    fn system_id(&self) -> String {
        self.inner.system_id.clone()
    }
    #[getter]
    fn session_id(&self) -> String {
        self.inner.session_id.clone()
    }

    /// Send a `submit_sm` and await the `submit_sm_resp`.
    #[pyo3(signature = (
        destination_addr,
        short_message = Vec::new(),
        *,
        source_addr = String::new(),
        service_type = String::new(),
        source_addr_ton = 0,
        source_addr_npi = 0,
        dest_addr_ton = 1,
        dest_addr_npi = 1,
        esm_class = 0,
        protocol_id = 0,
        priority_flag = 0,
        schedule_delivery_time = String::new(),
        validity_period = String::new(),
        registered_delivery = 0,
        replace_if_present_flag = 0,
        data_coding = 0,
        sm_default_msg_id = 0,
    ))]
    fn submit_sm<'py>(
        &self,
        py: Python<'py>,
        destination_addr: String,
        short_message: Vec<u8>,
        source_addr: String,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        dest_addr_ton: u8,
        dest_addr_npi: u8,
        esm_class: u8,
        protocol_id: u8,
        priority_flag: u8,
        schedule_delivery_time: String,
        validity_period: String,
        registered_delivery: u8,
        replace_if_present_flag: u8,
        data_coding: u8,
        sm_default_msg_id: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        let smsc = self.inner.clone();
        future_into_py(py, async move {
            match smsc
                .send_submit_sm(
                    service_type,
                    source_addr_ton,
                    source_addr_npi,
                    source_addr,
                    dest_addr_ton,
                    dest_addr_npi,
                    destination_addr,
                    esm_class,
                    protocol_id,
                    priority_flag,
                    schedule_delivery_time,
                    validity_period,
                    registered_delivery,
                    replace_if_present_flag,
                    data_coding,
                    sm_default_msg_id,
                    short_message,
                )
                .await
            {
                Ok(resp) => Ok(SubmitSmResp {
                    message_id: resp.message_id.clone(),
                    command_status: resp.command_status(),
                }),
                Err(e) => Err(smpp_err(e)),
            }
        })
    }

    /// Await the next inbound event: a `DeliverSmEvent`, or `Disconnected` once
    /// the session has ended.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events_rx = self.events_rx.clone();
        future_into_py(py, async move {
            let mut rx = events_rx.lock().await;
            Ok(rx.recv().await.unwrap_or(ClientEvent::Disconnected))
        })
    }

    /// Gracefully close the session: best-effort `unbind`, then stop the session
    /// task. The `unbind_resp` is intentionally not surfaced — the SMSC routinely
    /// closes the socket before it can be correlated, which is normal at teardown.
    fn unbind<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let smsc = self.inner.clone();
        let client = self._client.clone();
        future_into_py(py, async move {
            let _ = smsc.send_unbind().await;
            client.lock().await.stop().await;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Smsc(system_id={:?}, session_id={:?})",
            self.inner.system_id, self.inner.session_id
        )
    }
}

/// An SMPP client (ESME). Construct, then `await connect()` to bind and obtain a
/// [`Smsc`] session handle.
#[pyclass(module = "smpp34._smpp34")]
pub struct Client {
    client: Arc<TokioMutex<SmppClient>>,
    bind_rx: Arc<TokioMutex<Option<oneshot::Receiver<Arc<SMSC>>>>>,
    events_rx: Arc<TokioMutex<mpsc::Receiver<ClientEvent>>>,
    connect_timeout_ms: u64,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (
        host,
        port,
        system_id,
        password,
        *,
        bind_type = "TRX".to_string(),
        system_type = String::new(),
        tls = false,
        addr_ton = 0,
        addr_npi = 0,
        address_range = String::new(),
        window_size = 10,
        inbound_capacity = DEFAULT_INBOUND_CAP,
        connect_timeout_ms = 10_000,
    ))]
    fn new(
        host: String,
        port: u16,
        system_id: String,
        password: String,
        bind_type: String,
        system_type: String,
        tls: bool,
        addr_ton: u8,
        addr_npi: u8,
        address_range: String,
        window_size: usize,
        inbound_capacity: usize,
        connect_timeout_ms: u64,
    ) -> PyResult<Self> {
        let bind = match bind_type.to_ascii_uppercase().as_str() {
            "RX" => BIND_TYPE::RX,
            "TX" => BIND_TYPE::TX,
            "TRX" => BIND_TYPE::TRX,
            other => {
                return Err(PyValueError::new_err(format!(
                    "bind_type must be one of RX/TX/TRX, got {other:?}"
                )))
            }
        };
        let (bind_tx, bind_rx) = oneshot::channel();
        let (events_tx, events_rx) = mpsc::channel(inbound_capacity.max(1));
        let forwarder = Arc::new(ClientForwarder {
            bind_tx: std::sync::Mutex::new(Some(bind_tx)),
            events_tx,
        });
        let client = SmppClient::new(
            host,
            port,
            tls,
            bind,
            system_id,
            password,
            system_type,
            addr_ton,
            addr_npi,
            address_range,
            forwarder,
            window_size,
        );
        Ok(Self {
            client: Arc::new(TokioMutex::new(client)),
            bind_rx: Arc::new(TokioMutex::new(Some(bind_rx))),
            events_rx: Arc::new(TokioMutex::new(events_rx)),
            connect_timeout_ms,
        })
    }

    /// Start the session and await the bind handshake; returns a [`Smsc`].
    fn connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let events_rx = self.events_rx.clone();
        let bind_slot = self.bind_rx.clone();
        let timeout_ms = self.connect_timeout_ms;
        future_into_py(py, async move {
            let bind_rx = bind_slot
                .lock()
                .await
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("connect() already called"))?;
            {
                let mut guard = client.lock().await;
                guard.start().await;
            }
            let smsc = match tokio::time::timeout(Duration::from_millis(timeout_ms), bind_rx).await
            {
                Ok(Ok(smsc)) => smsc,
                Ok(Err(_)) => {
                    return Err(SmppError::new_err("session closed before bind completed"))
                }
                Err(_) => return Err(SmppError::new_err("bind timed out")),
            };
            Ok(Smsc {
                inner: smsc,
                events_rx,
                _client: client,
            })
        })
    }

    /// Whether the underlying session task is still running.
    fn is_alive<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            let guard = client.lock().await;
            Ok(guard.is_alive())
        })
    }

    /// Stop the session task (without sending an unbind).
    fn stop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            client.lock().await.stop().await;
            Ok(())
        })
    }
}

/// An SMPP server (SMSC). Construct, `await start()`, then `await next()` for
/// inbound events: an [`Esme`] handle when a client binds, a [`SubmitSmEvent`]
/// per inbound message, or [`Unbound`] when a session ends.
#[pyclass(module = "smpp34._smpp34")]
pub struct Server {
    server: Arc<TokioMutex<SmppServer>>,
    events_rx: Arc<TokioMutex<mpsc::Receiver<ServerEvent>>>,
}

#[pymethods]
impl Server {
    #[new]
    #[pyo3(signature = (
        host,
        port,
        *,
        system_id = "SMSC".to_string(),
        inbound_capacity = DEFAULT_INBOUND_CAP,
    ))]
    fn new(host: String, port: u16, system_id: String, inbound_capacity: usize) -> PyResult<Self> {
        let ip = host
            .parse::<std::net::IpAddr>()
            .map_err(|e| PyValueError::new_err(format!("invalid host {host:?}: {e}")))?;
        let (events_tx, events_rx) = mpsc::channel(inbound_capacity.max(1));
        let forwarder = Arc::new(ServerForwarder {
            system_id,
            events_tx,
        });
        let server = SmppServer::new(ip, port, forwarder);
        Ok(Self {
            server: Arc::new(TokioMutex::new(server)),
            events_rx: Arc::new(TokioMutex::new(events_rx)),
        })
    }

    /// Bind the listening socket and start accepting connections.
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let server = self.server.clone();
        future_into_py(py, async move {
            let mut guard = server.lock().await;
            guard.start().await;
            Ok(())
        })
    }

    /// Await the next server event: an [`Esme`] (a client bound), a
    /// [`SubmitSmEvent`], or an [`Unbound`].
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events_rx = self.events_rx.clone();
        future_into_py(py, async move {
            let mut rx = events_rx.lock().await;
            match rx.recv().await {
                Some(ev) => Ok(ev),
                None => Err(SmppError::new_err("server stopped")),
            }
        })
    }

    /// Stop accepting connections and shut the listener down.
    fn stop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let server = self.server.clone();
        future_into_py(py, async move {
            server.lock().await.stop().await;
            Ok(())
        })
    }
}

/// A bound ESME session as seen by the server. Push MT traffic with
/// `deliver_sm(...)`; `await unbind()` to close.
#[pyclass(module = "smpp34._smpp34")]
pub struct Esme {
    inner: Arc<ESME>,
}

#[pymethods]
impl Esme {
    #[getter]
    fn system_id(&self) -> String {
        self.inner.system_id.clone()
    }
    #[getter]
    fn session_id(&self) -> String {
        self.inner.session_id.clone()
    }
    #[getter]
    fn can_receive(&self) -> bool {
        self.inner.can_receive
    }

    /// Send a `deliver_sm` (MO / delivery receipt) and await its response.
    #[pyo3(signature = (
        destination_addr,
        short_message = Vec::new(),
        *,
        source_addr = String::new(),
        service_type = String::new(),
        source_addr_ton = 0,
        source_addr_npi = 0,
        dest_addr_ton = 1,
        dest_addr_npi = 1,
        esm_class = 0,
        protocol_id = 0,
        priority_flag = 0,
        schedule_delivery_time = String::new(),
        validity_period = String::new(),
        registered_delivery = 0,
        replace_if_present_flag = 0,
        data_coding = 0,
        sm_default_msg_id = 0,
    ))]
    fn deliver_sm<'py>(
        &self,
        py: Python<'py>,
        destination_addr: String,
        short_message: Vec<u8>,
        source_addr: String,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        dest_addr_ton: u8,
        dest_addr_npi: u8,
        esm_class: u8,
        protocol_id: u8,
        priority_flag: u8,
        schedule_delivery_time: String,
        validity_period: String,
        registered_delivery: u8,
        replace_if_present_flag: u8,
        data_coding: u8,
        sm_default_msg_id: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        let esme = self.inner.clone();
        future_into_py(py, async move {
            match esme
                .send_deliver_sm(
                    service_type,
                    source_addr_ton,
                    source_addr_npi,
                    source_addr,
                    dest_addr_ton,
                    dest_addr_npi,
                    destination_addr,
                    esm_class,
                    protocol_id,
                    priority_flag,
                    schedule_delivery_time,
                    validity_period,
                    registered_delivery,
                    replace_if_present_flag,
                    data_coding,
                    sm_default_msg_id,
                    short_message,
                )
                .await
            {
                Ok(resp) => Ok(DeliverSmResp {
                    command_status: resp.command_status(),
                }),
                Err(e) => Err(smpp_err(e)),
            }
        })
    }

    /// Best-effort `unbind` of this ESME session. The response is not surfaced —
    /// the peer routinely closes the socket before it can be correlated.
    fn unbind<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let esme = self.inner.clone();
        future_into_py(py, async move {
            let _ = esme.send_unbind().await;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Esme(system_id={:?}, session_id={:?})",
            self.inner.system_id, self.inner.session_id
        )
    }
}

// ── Module assembly ─────────────────────────────────────────────────────────
fn init_runtime() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        // The asyncio bridge wakes the Python event loop on every future
        // completion, and that wake needs the GIL. On a many-core box a default
        // worker pool (= ncpu) means dozens of tokio threads all contend for that
        // one GIL and throughput collapses. Cap the pool (the SMPP work itself is
        // I/O-bound); override with SMPP34_WORKER_THREADS. On a free-threaded
        // interpreter, where there is no GIL to contend for, set it higher.
        let threads = std::env::var("SMPP34_WORKER_THREADS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(2);
        builder.worker_threads(threads);
        pyo3_async_runtimes::tokio::init(builder);
    });
}

const ERROR_CODES: &[(&str, u32)] = &[
    ("ESME_ROK", 0x00000000),
    ("ESME_RINVMSGLEN", 0x00000001),
    ("ESME_RINVCMDLEN", 0x00000002),
    ("ESME_RINVCMDID", 0x00000003),
    ("ESME_RSYSERR", 0x00000008),
    ("ESME_RINVSRCADR", 0x0000000A),
    ("ESME_RINVDSTADR", 0x0000000B),
    ("ESME_RINVMSGID", 0x0000000C),
    ("ESME_RMSGQFUL", 0x00000014),
    ("ESME_RTHROTTLED", 0x00000058),
    ("ESME_RUNKNOWNERR", 0x000000FF),
];

fn add_contents(m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_runtime();
    m.add("SmppError", m.py().get_type::<SmppError>())?;
    // Codec surface
    m.add_class::<SubmitSm>()?;
    m.add_class::<DeliverSm>()?;
    m.add_class::<RawPdu>()?;
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    // Async client/server
    m.add_class::<Client>()?;
    m.add_class::<Smsc>()?;
    m.add_class::<Server>()?;
    m.add_class::<Esme>()?;
    m.add_class::<DeliverSmEvent>()?;
    m.add_class::<SubmitSmEvent>()?;
    m.add_class::<Unbound>()?;
    m.add_class::<Disconnected>()?;
    m.add_class::<SubmitSmResp>()?;
    m.add_class::<DeliverSmResp>()?;
    // Common SMPP command_status constants (for reject()).
    for (name, code) in ERROR_CODES {
        m.add(*name, *code)?;
    }
    Ok(())
}

/// Standalone wheel entry point (maturin `module-name = "smpp34._smpp34"`).
#[pymodule]
fn _smpp34(m: &Bound<'_, PyModule>) -> PyResult<()> {
    add_contents(m)
}

/// Embedding entry point: build a `smpp34` submodule and attach it to `parent`,
/// so a host extension can expose smpp34 without a second shared object. Mirrors
/// `sms_tpdu_py::register`.
pub fn register(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "smpp34")?;
    add_contents(&m)?;
    parent.setattr("smpp34", &m)?;
    Ok(())
}
