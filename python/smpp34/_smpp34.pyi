"""Type stubs for the compiled ``smpp34._smpp34`` extension.

Hand-written because the compiled (abi3) module exposes no introspectable
signatures. Keep in sync with ``src/python.rs``.
"""

from __future__ import annotations

class SmppError(Exception):
    """SMPP protocol / codec error.

    The message carries the SMPP ``command_status`` variant and numeric value,
    e.g. ``"ESME_RINVPARLEN (0x00000005)"``.
    """

class Tlv:
    """An SMPP optional parameter: a 2-byte tag and its raw value bytes.

    The tag constants live on the module (``smpp34.TLV_MESSAGE_PAYLOAD``, ...);
    vendor-specific tags (0x1400-0x3FFF) work just as well, the value is passed
    through untouched.
    """

    def __init__(self, tag: int, value: bytes) -> None: ...
    @property
    def tag(self) -> int: ...
    @property
    def value(self) -> bytes: ...
    def as_int(self) -> int | None: ...
    def as_string(self) -> str | None: ...

class SubmitSm:
    """An SMPP ``submit_sm`` PDU (ESME -> SMSC)."""

    def __init__(
        self,
        source_addr: str,
        destination_addr: str,
        short_message: bytes = ...,
        *,
        service_type: str = ...,
        source_addr_ton: int = ...,
        source_addr_npi: int = ...,
        dest_addr_ton: int = ...,
        dest_addr_npi: int = ...,
        esm_class: int = ...,
        protocol_id: int = ...,
        priority_flag: int = ...,
        schedule_delivery_time: str = ...,
        validity_period: str = ...,
        registered_delivery: int = ...,
        replace_if_present_flag: int = ...,
        data_coding: int = ...,
        sm_default_msg_id: int = ...,
        tlvs: list[Tlv] = ...,
        sequence_number: int = ...,
    ) -> None: ...
    @property
    def command_id(self) -> int: ...
    @property
    def sequence_number(self) -> int: ...
    @property
    def service_type(self) -> str: ...
    @property
    def source_addr_ton(self) -> int: ...
    @property
    def source_addr_npi(self) -> int: ...
    @property
    def source_addr(self) -> str: ...
    @property
    def dest_addr_ton(self) -> int: ...
    @property
    def dest_addr_npi(self) -> int: ...
    @property
    def destination_addr(self) -> str: ...
    @property
    def esm_class(self) -> int: ...
    @property
    def protocol_id(self) -> int: ...
    @property
    def priority_flag(self) -> int: ...
    @property
    def schedule_delivery_time(self) -> str: ...
    @property
    def validity_period(self) -> str: ...
    @property
    def registered_delivery(self) -> int: ...
    @property
    def replace_if_present_flag(self) -> int: ...
    @property
    def data_coding(self) -> int: ...
    @property
    def sm_default_msg_id(self) -> int: ...
    @property
    def sm_length(self) -> int: ...
    @property
    def short_message(self) -> bytes: ...
    @property
    def tlvs(self) -> list[Tlv]: ...
    def encode(self) -> bytes: ...

class DeliverSm:
    """An SMPP ``deliver_sm`` PDU (SMSC -> ESME). Same field set as ``SubmitSm``."""

    def __init__(
        self,
        source_addr: str,
        destination_addr: str,
        short_message: bytes = ...,
        *,
        service_type: str = ...,
        source_addr_ton: int = ...,
        source_addr_npi: int = ...,
        dest_addr_ton: int = ...,
        dest_addr_npi: int = ...,
        esm_class: int = ...,
        protocol_id: int = ...,
        priority_flag: int = ...,
        schedule_delivery_time: str = ...,
        validity_period: str = ...,
        registered_delivery: int = ...,
        replace_if_present_flag: int = ...,
        data_coding: int = ...,
        sm_default_msg_id: int = ...,
        tlvs: list[Tlv] = ...,
        sequence_number: int = ...,
    ) -> None: ...
    @property
    def command_id(self) -> int: ...
    @property
    def sequence_number(self) -> int: ...
    @property
    def service_type(self) -> str: ...
    @property
    def source_addr_ton(self) -> int: ...
    @property
    def source_addr_npi(self) -> int: ...
    @property
    def source_addr(self) -> str: ...
    @property
    def dest_addr_ton(self) -> int: ...
    @property
    def dest_addr_npi(self) -> int: ...
    @property
    def destination_addr(self) -> str: ...
    @property
    def esm_class(self) -> int: ...
    @property
    def protocol_id(self) -> int: ...
    @property
    def priority_flag(self) -> int: ...
    @property
    def schedule_delivery_time(self) -> str: ...
    @property
    def validity_period(self) -> str: ...
    @property
    def registered_delivery(self) -> int: ...
    @property
    def replace_if_present_flag(self) -> int: ...
    @property
    def data_coding(self) -> int: ...
    @property
    def sm_default_msg_id(self) -> int: ...
    @property
    def sm_length(self) -> int: ...
    @property
    def short_message(self) -> bytes: ...
    @property
    def tlvs(self) -> list[Tlv]: ...
    def encode(self) -> bytes: ...

class RawPdu:
    """A decoded PDU header + raw body, for command IDs without a dedicated class."""

    @property
    def command_length(self) -> int: ...
    @property
    def command_id(self) -> int: ...
    @property
    def command_status(self) -> int: ...
    @property
    def sequence_number(self) -> int: ...
    @property
    def body(self) -> bytes: ...

def decode(data: bytes) -> SubmitSm | DeliverSm | RawPdu:
    """Decode one complete SMPP PDU into a typed object.

    Raises ``SmppError`` if the header is malformed or ``len(data)`` does not
    equal the PDU's ``command_length``.
    """
    ...

# SMPP 3.4 optional-parameter tags, for Tlv(tag, value).
TLV_DEST_ADDR_SUBUNIT: int
TLV_DEST_NETWORK_TYPE: int
TLV_DEST_BEARER_TYPE: int
TLV_DEST_TELEMATICS_ID: int
TLV_SOURCE_ADDR_SUBUNIT: int
TLV_SOURCE_NETWORK_TYPE: int
TLV_SOURCE_BEARER_TYPE: int
TLV_SOURCE_TELEMATICS_ID: int
TLV_QOS_TIME_TO_LIVE: int
TLV_PAYLOAD_TYPE: int
TLV_ADDITIONAL_STATUS_INFO_TEXT: int
TLV_RECEIPTED_MESSAGE_ID: int
TLV_MS_MSG_WAIT_FACILITIES: int
TLV_PRIVACY_INDICATOR: int
TLV_SOURCE_SUBADDRESS: int
TLV_DEST_SUBADDRESS: int
TLV_USER_MESSAGE_REFERENCE: int
TLV_USER_RESPONSE_CODE: int
TLV_SOURCE_PORT: int
TLV_DESTINATION_PORT: int
TLV_SAR_MSG_REF_NUM: int
TLV_LANGUAGE_INDICATOR: int
TLV_SAR_TOTAL_SEGMENTS: int
TLV_SAR_SEGMENT_SEQNUM: int
TLV_SC_INTERFACE_VERSION: int
TLV_CALLBACK_NUM_PRES_IND: int
TLV_CALLBACK_NUM_ATAG: int
TLV_NUMBER_OF_MESSAGES: int
TLV_CALLBACK_NUM: int
TLV_DPF_RESULT: int
TLV_SET_DPF: int
TLV_MS_AVAILABILITY_STATUS: int
TLV_NETWORK_ERROR_CODE: int
TLV_MESSAGE_PAYLOAD: int
TLV_DELIVERY_FAILURE_REASON: int
TLV_MORE_MESSAGES_TO_SEND: int
TLV_MESSAGE_STATE: int
TLV_USSD_SERVICE_OP: int
TLV_DISPLAY_TIME: int
TLV_SMS_SIGNAL: int
TLV_MS_VALIDITY: int
TLV_ALERT_ON_MESSAGE_DELIVERY: int
TLV_ITS_REPLY_TYPE: int
TLV_ITS_SESSION_INFO: int

# ── Async client / server ───────────────────────────────────────────────────
# SMPP command_status constants, for SubmitSmEvent.reject(...).
ESME_ROK: int
ESME_RINVMSGLEN: int
ESME_RINVCMDLEN: int
ESME_RINVCMDID: int
ESME_RSYSERR: int
ESME_RINVSRCADR: int
ESME_RINVDSTADR: int
ESME_RINVMSGID: int
ESME_RMSGQFUL: int
ESME_RTHROTTLED: int
ESME_RUNKNOWNERR: int

class SubmitSmResp:
    """Result of ``Smsc.submit_sm(...)``."""

    @property
    def message_id(self) -> str | None: ...
    @property
    def command_status(self) -> int: ...
    @property
    def is_success(self) -> bool: ...

class DeliverSmResp:
    """Result of ``Esme.deliver_sm(...)``."""

    @property
    def command_status(self) -> int: ...
    @property
    def is_success(self) -> bool: ...

class DeliverSmEvent:
    """Inbound ``deliver_sm`` (MO / delivery receipt), yielded by ``Smsc.next()``.

    The SMPP-level ACK is sent automatically by the core (a NAK is returned to
    the peer only under inbound backpressure).
    """

    @property
    def service_type(self) -> str: ...
    @property
    def source_addr(self) -> str: ...
    @property
    def source_addr_ton(self) -> int: ...
    @property
    def source_addr_npi(self) -> int: ...
    @property
    def destination_addr(self) -> str: ...
    @property
    def dest_addr_ton(self) -> int: ...
    @property
    def dest_addr_npi(self) -> int: ...
    @property
    def esm_class(self) -> int: ...
    @property
    def protocol_id(self) -> int: ...
    @property
    def data_coding(self) -> int: ...
    @property
    def registered_delivery(self) -> int: ...
    @property
    def short_message(self) -> bytes: ...
    @property
    def tlvs(self) -> list[Tlv]: ...

class SubmitSmEvent:
    """Inbound ``submit_sm`` (ESME -> SMSC), yielded by ``Server.next()``.

    Answer with ``accept(message_id)`` or ``reject(command_status)``; dropping
    the event without a decision NAKs the peer with ``ESME_RSYSERR``.
    """

    @property
    def session_id(self) -> str: ...
    @property
    def service_type(self) -> str: ...
    @property
    def source_addr(self) -> str: ...
    @property
    def source_addr_ton(self) -> int: ...
    @property
    def source_addr_npi(self) -> int: ...
    @property
    def destination_addr(self) -> str: ...
    @property
    def dest_addr_ton(self) -> int: ...
    @property
    def dest_addr_npi(self) -> int: ...
    @property
    def esm_class(self) -> int: ...
    @property
    def protocol_id(self) -> int: ...
    @property
    def data_coding(self) -> int: ...
    @property
    def registered_delivery(self) -> int: ...
    @property
    def short_message(self) -> bytes: ...
    @property
    def tlvs(self) -> list[Tlv]: ...
    def accept(self, message_id: str) -> None: ...
    def reject(self, command_status: int = ...) -> None: ...

class Unbound:
    """Yielded by ``Server.next()`` when a bound ESME session ends."""

    @property
    def session_id(self) -> str: ...

class Disconnected:
    """Yielded by ``Smsc.next()`` once the session has ended."""

class Smsc:
    """A bound ESME session (client side)."""

    @property
    def system_id(self) -> str: ...
    @property
    def session_id(self) -> str: ...
    async def submit_sm(
        self,
        destination_addr: str,
        short_message: bytes = ...,
        *,
        source_addr: str = ...,
        service_type: str = ...,
        source_addr_ton: int = ...,
        source_addr_npi: int = ...,
        dest_addr_ton: int = ...,
        dest_addr_npi: int = ...,
        esm_class: int = ...,
        protocol_id: int = ...,
        priority_flag: int = ...,
        schedule_delivery_time: str = ...,
        validity_period: str = ...,
        registered_delivery: int = ...,
        replace_if_present_flag: int = ...,
        data_coding: int = ...,
        sm_default_msg_id: int = ...,
        tlvs: list[Tlv] = ...,
    ) -> SubmitSmResp: ...
    async def next(self) -> DeliverSmEvent | Disconnected: ...
    async def unbind(self) -> None: ...

class Client:
    """An SMPP client (ESME). Construct, then ``await connect()`` to bind."""

    def __init__(
        self,
        host: str,
        port: int,
        system_id: str,
        password: str,
        *,
        bind_type: str = ...,
        system_type: str = ...,
        tls: bool = ...,
        addr_ton: int = ...,
        addr_npi: int = ...,
        address_range: str = ...,
        window_size: int = ...,
        inbound_capacity: int = ...,
        connect_timeout_ms: int = ...,
    ) -> None: ...
    async def connect(self) -> Smsc: ...
    async def is_alive(self) -> bool: ...
    async def stop(self) -> None: ...

class Esme:
    """A bound ESME session as seen by the server (push MT traffic)."""

    @property
    def system_id(self) -> str: ...
    @property
    def session_id(self) -> str: ...
    @property
    def can_receive(self) -> bool: ...
    async def deliver_sm(
        self,
        destination_addr: str,
        short_message: bytes = ...,
        *,
        source_addr: str = ...,
        service_type: str = ...,
        source_addr_ton: int = ...,
        source_addr_npi: int = ...,
        dest_addr_ton: int = ...,
        dest_addr_npi: int = ...,
        esm_class: int = ...,
        protocol_id: int = ...,
        priority_flag: int = ...,
        schedule_delivery_time: str = ...,
        validity_period: str = ...,
        registered_delivery: int = ...,
        replace_if_present_flag: int = ...,
        data_coding: int = ...,
        sm_default_msg_id: int = ...,
        tlvs: list[Tlv] = ...,
    ) -> DeliverSmResp: ...
    async def unbind(self) -> None: ...

class Server:
    """An SMPP server (SMSC). Construct, ``await start()``, then ``await next()``."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        system_id: str = ...,
        inbound_capacity: int = ...,
    ) -> None: ...
    async def start(self) -> None: ...
    async def next(self) -> Esme | SubmitSmEvent | Unbound: ...
    async def stop(self) -> None: ...
