"""smpp34 — Rust-backed SMPP 3.4 codec + async client/server for Python.

The heavy lifting (TCP framing, sequence windowing, SMPP timers, PDU
encode/decode) runs entirely in the Rust/tokio core; Python only crosses the GIL
once per application-level message. The same library powers the Rust crate
(``cargo add smpp34``) and this wheel (``pip install smpp34``) from one source tree.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from ._smpp34 import (
    Client,
    DeliverSm,
    DeliverSmEvent,
    DeliverSmResp,
    Disconnected,
    Esme,
    RawPdu,
    Server,
    Smsc,
    SmppError,
    SubmitSm,
    SubmitSmEvent,
    SubmitSmResp,
    Tlv,
    Unbound,
    decode,
)

try:
    __version__ = version("smpp34")
except PackageNotFoundError:  # running from a source checkout without an installed dist
    __version__ = "0.0.0+unknown"

__all__ = [
    # codec
    "SubmitSm",
    "DeliverSm",
    "RawPdu",
    "Tlv",
    "SmppError",
    "decode",
    # async client / server
    "Client",
    "Smsc",
    "Server",
    "Esme",
    "DeliverSmEvent",
    "SubmitSmEvent",
    "SubmitSmResp",
    "DeliverSmResp",
    "Unbound",
    "Disconnected",
    "__version__",
]

# Re-export the constants defined on the extension module: SMPP command_status
# (ESME_*) for SubmitSmEvent.reject(...), and the optional-parameter tags
# (TLV_*) for Tlv(tag, value).
from . import _smpp34 as _ext  # noqa: E402

for _name in dir(_ext):
    if _name.startswith(("ESME_", "TLV_")):
        globals()[_name] = getattr(_ext, _name)
        __all__.append(_name)
del _ext, _name
