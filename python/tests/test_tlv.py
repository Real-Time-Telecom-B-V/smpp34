"""Optional parameter (TLV) tests for the smpp34 wheel.

The Python surface had no TLV support at all: you could neither read the
optional parameters off an inbound PDU nor put any on an outbound one. These
lock the wire format against a hand-built vector and check the tags survive both
the codec and a live session.
"""

from __future__ import annotations

import asyncio
import socket

import smpp34

# A submit_sm with two optional parameters, hand-built from SMPP 3.4 §4.4.1 +
# §3.2.1: user_message_reference (0x0204) and a vendor-specific tag (0x1403).
# src="12345", dst="999", short_message=b"hi", registered_delivery=1, seq=0x12345678.
GOLDEN_SUBMIT_SM_TLV = bytes.fromhex(
    "000000380000000400000000123456780001013132333435000101"
    "3939390000000000000100000002686902040002123414030003aabbcc"
)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── Codec ───────────────────────────────────────────────────────────────────


def test_submit_sm_with_tlvs_matches_golden_vector() -> None:
    sm = smpp34.SubmitSm(
        source_addr="12345",
        destination_addr="999",
        short_message=b"hi",
        source_addr_ton=1,
        source_addr_npi=1,
        dest_addr_ton=1,
        dest_addr_npi=1,
        registered_delivery=1,
        sequence_number=0x12345678,
        tlvs=[
            smpp34.Tlv(smpp34.TLV_USER_MESSAGE_REFERENCE, (0x1234).to_bytes(2, "big")),
            smpp34.Tlv(0x1403, b"\xaa\xbb\xcc"),
        ],
    )
    assert sm.encode() == GOLDEN_SUBMIT_SM_TLV


def test_decode_golden_vector_tlvs() -> None:
    msg = smpp34.decode(GOLDEN_SUBMIT_SM_TLV)
    assert isinstance(msg, smpp34.SubmitSm)
    assert msg.short_message == b"hi"

    tags = [t.tag for t in msg.tlvs]
    assert tags == [smpp34.TLV_USER_MESSAGE_REFERENCE, 0x1403]
    assert msg.tlvs[0].as_int() == 0x1234
    assert msg.tlvs[0].value == b"\x12\x34"
    # An unknown vendor tag passes through byte for byte.
    assert msg.tlvs[1].value == b"\xaa\xbb\xcc"


def test_tlv_round_trip_is_byte_stable() -> None:
    pdu = smpp34.DeliverSm(
        source_addr="31600000000",
        destination_addr="12345",
        esm_class=0x04,
        tlvs=[
            smpp34.Tlv(smpp34.TLV_RECEIPTED_MESSAGE_ID, b"msg-1\x00"),
            smpp34.Tlv(smpp34.TLV_MESSAGE_STATE, b"\x02"),
        ],
    )
    wire = pdu.encode()
    decoded = smpp34.decode(wire)
    assert [t.tag for t in decoded.tlvs] == [
        smpp34.TLV_RECEIPTED_MESSAGE_ID,
        smpp34.TLV_MESSAGE_STATE,
    ]
    assert decoded.tlvs[0].as_string() == "msg-1"
    assert decoded.tlvs[1].as_int() == 2
    assert decoded.encode() == wire


def test_no_tlvs_is_an_empty_list() -> None:
    sm = smpp34.SubmitSm(source_addr="1", destination_addr="2")
    assert sm.tlvs == []
    assert smpp34.decode(sm.encode()).tlvs == []


def test_message_payload_carries_more_than_254_bytes() -> None:
    # The documented way past the short_message limit.
    body = bytes(range(256)) * 4
    sm = smpp34.SubmitSm(
        source_addr="1",
        destination_addr="2",
        tlvs=[smpp34.Tlv(smpp34.TLV_MESSAGE_PAYLOAD, body)],
    )
    decoded = smpp34.decode(sm.encode())
    assert decoded.short_message == b""
    assert decoded.tlvs[0].value == body


def test_tlv_accessors() -> None:
    assert smpp34.Tlv(0x0427, b"\x02").as_int() == 2
    assert smpp34.Tlv(0x0204, b"\x12\x34").as_int() == 0x1234
    assert smpp34.Tlv(0x0017, b"\x00\x00\x00\x0f").as_int() == 15
    # 3-byte values (network_error_code) have no single integer reading.
    assert smpp34.Tlv(0x0423, b"\x03\x00\x1f").as_int() is None
    assert smpp34.Tlv(0x001E, b"msg-1\x00").as_string() == "msg-1"
    assert smpp34.Tlv(0x0424, b"\xff\xfe").as_string() is None


def test_tlv_equality_and_repr() -> None:
    assert smpp34.Tlv(0x0204, b"\x00\x01") == smpp34.Tlv(0x0204, b"\x00\x01")
    assert smpp34.Tlv(0x0204, b"\x00\x01") != smpp34.Tlv(0x0204, b"\x00\x02")
    assert "0x0204" in repr(smpp34.Tlv(0x0204, b"\x00\x01"))


def test_tag_constants_match_the_spec() -> None:
    assert smpp34.TLV_MESSAGE_PAYLOAD == 0x0424
    assert smpp34.TLV_RECEIPTED_MESSAGE_ID == 0x001E
    assert smpp34.TLV_MESSAGE_STATE == 0x0427
    assert smpp34.TLV_USER_MESSAGE_REFERENCE == 0x0204
    assert smpp34.TLV_SAR_MSG_REF_NUM == 0x020C
    assert smpp34.TLV_MS_AVAILABILITY_STATUS == 0x0422
    assert len([n for n in dir(smpp34) if n.startswith("TLV_")]) == 44


# ── Live session ────────────────────────────────────────────────────────────


def test_tlvs_survive_a_live_session_in_both_directions() -> None:
    asyncio.run(_tlvs_both_directions())


async def _tlvs_both_directions() -> None:
    port = free_port()
    server = smpp34.Server("127.0.0.1", port, system_id="TLV-SMSC")
    await server.start()

    client = smpp34.Client("127.0.0.1", port, "user", "pass", bind_type="TRX")
    smsc = await client.connect()

    esme = await asyncio.wait_for(server.next(), timeout=5)
    assert isinstance(esme, smpp34.Esme)

    # ESME -> SMSC. The Rust future is already running on the tokio runtime, so
    # the response can be awaited after the server has answered it.
    submit = smsc.submit_sm(
        "31600000000",
        b"hi",
        source_addr="12345",
        registered_delivery=1,
        tlvs=[
            smpp34.Tlv(smpp34.TLV_USER_MESSAGE_REFERENCE, (7).to_bytes(2, "big")),
            smpp34.Tlv(smpp34.TLV_MESSAGE_PAYLOAD, b"the real body"),
        ],
    )
    event = await asyncio.wait_for(server.next(), timeout=5)
    assert isinstance(event, smpp34.SubmitSmEvent)
    assert [t.tag for t in event.tlvs] == [
        smpp34.TLV_USER_MESSAGE_REFERENCE,
        smpp34.TLV_MESSAGE_PAYLOAD,
    ]
    assert event.tlvs[0].as_int() == 7
    assert event.tlvs[1].value == b"the real body"
    event.accept("msg-1")
    assert (await asyncio.wait_for(submit, timeout=5)).is_success

    # SMSC -> ESME: a delivery receipt, which exists only as TLVs.
    resp = await esme.deliver_sm(
        "12345",
        source_addr="31600000000",
        esm_class=0x04,
        tlvs=[
            smpp34.Tlv(smpp34.TLV_RECEIPTED_MESSAGE_ID, b"msg-1\x00"),
            smpp34.Tlv(smpp34.TLV_MESSAGE_STATE, b"\x02"),
        ],
    )
    assert resp.is_success

    dlr = await asyncio.wait_for(smsc.next(), timeout=5)
    assert isinstance(dlr, smpp34.DeliverSmEvent)
    assert dlr.esm_class == 0x04
    assert dlr.tlvs[0].as_string() == "msg-1"
    assert dlr.tlvs[1].as_int() == 2

    await smsc.unbind()
    await server.stop()
