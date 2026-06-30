"""Codec parity / round-trip tests for the smpp34 wheel.

These exercise the same Rust codec the crate ships, through the Python surface:
``encode`` -> ``decode`` -> ``encode`` must be byte-stable, fields must survive the
round-trip, and the wire format is locked against a golden vector.
"""

from __future__ import annotations

import struct

import pytest

import smpp34

# A submit_sm encoded by the Rust core (golden — locks the wire format).
# src="12345", dst="31600000000", short_message=b"hello world", seq=7, all else default.
GOLDEN_SUBMIT_SM = bytes.fromhex(
    "0000003c00000004000000000000000700000031323334350000003331"
    "363030303030303030000000000000000000000b68656c6c6f20776f726c64"
)


def test_submit_sm_matches_golden_vector() -> None:
    sm = smpp34.SubmitSm(
        source_addr="12345",
        destination_addr="31600000000",
        short_message=b"hello world",
        data_coding=0,
        sequence_number=7,
    )
    assert sm.encode() == GOLDEN_SUBMIT_SM


def test_decode_golden_vector_fields() -> None:
    msg = smpp34.decode(GOLDEN_SUBMIT_SM)
    assert isinstance(msg, smpp34.SubmitSm)
    assert msg.command_id == 0x00000004
    assert msg.sequence_number == 7
    assert msg.source_addr == "12345"
    assert msg.destination_addr == "31600000000"
    assert msg.short_message == b"hello world"
    assert msg.sm_length == len(b"hello world")
    assert msg.data_coding == 0


@pytest.mark.parametrize("cls,expected_command_id", [
    (smpp34.SubmitSm, 0x00000004),
    (smpp34.DeliverSm, 0x00000005),
])
def test_round_trip_byte_stable(cls, expected_command_id) -> None:
    pdu = cls(
        source_addr="441234567890",
        destination_addr="31600000000",
        short_message=bytes(range(0, 200)),  # binary payload, dcs=4
        data_coding=4,
        esm_class=0x40,
        registered_delivery=1,
        sequence_number=12345,
    )
    wire = pdu.encode()
    decoded = smpp34.decode(wire)
    assert type(decoded) is cls
    assert decoded.command_id == expected_command_id
    assert decoded.sequence_number == 12345
    assert decoded.source_addr == "441234567890"
    assert decoded.destination_addr == "31600000000"
    assert decoded.short_message == bytes(range(0, 200))
    assert decoded.esm_class == 0x40
    assert decoded.registered_delivery == 1
    # re-encoding the decoded PDU reproduces the exact bytes
    assert decoded.encode() == wire


def test_empty_short_message() -> None:
    sm = smpp34.SubmitSm(source_addr="1", destination_addr="2")
    assert sm.short_message == b""
    assert sm.sm_length == 0
    assert smpp34.decode(sm.encode()).short_message == b""


def test_short_message_over_254_rejected() -> None:
    with pytest.raises(ValueError):
        smpp34.SubmitSm(source_addr="1", destination_addr="2", short_message=b"x" * 255)


def test_unknown_command_id_falls_back_to_rawpdu() -> None:
    # enquire_link is a bare 16-byte header (command_id 0x15).
    enquire_link = struct.pack(">IIII", 16, 0x00000015, 0, 42)
    raw = smpp34.decode(enquire_link)
    assert isinstance(raw, smpp34.RawPdu)
    assert raw.command_id == 0x15
    assert raw.command_status == 0
    assert raw.sequence_number == 42
    assert raw.command_length == 16
    assert raw.body == b""


def test_malformed_too_short_raises() -> None:
    with pytest.raises(smpp34.SmppError):
        smpp34.decode(b"\x00\x00")


def test_length_mismatch_raises() -> None:
    # command_length says 60 but only 16 bytes provided.
    bad = struct.pack(">IIII", 60, 0x00000004, 0, 1)
    with pytest.raises(smpp34.SmppError):
        smpp34.decode(bad)


def test_smpp_error_message_carries_status() -> None:
    try:
        smpp34.decode(b"\x00\x00")
    except smpp34.SmppError as exc:
        # message embeds the variant name + numeric command_status
        assert "0x" in str(exc)
    else:
        pytest.fail("expected SmppError")
