//! A peer must not be able to panic us with values it is entitled to send.
//!
//! `get_error()` used to be `FromPrimitive::from_u32(command_status).expect(..)`,
//! so any `command_status` outside our enum panicked the task that read it. That
//! is not a hostile input: SMPP 3.4 §5.1.3 leaves `0x00000400`-`0x000004FF`
//! explicitly vendor-specific and reserves several other ranges, and real SMSCs
//! use them. The panic landed inside the session task, on a code path reached
//! straight from the wire — `generic_nack.get_error()` and
//! `bind_*_resp.get_error()` are called on whatever the peer sent.
//!
//! These drive the decode path end to end: build the PDU bytes a peer would put
//! on the wire, decode them with the public API, and read the error back.

use smpp34::{generic_nack, unbind, unbind_resp, CommandHeader, SmppError};

/// The 16-octet PDU a peer sends for a response carrying `command_status`.
fn response_pdu(command_id: u32, command_status: u32, sequence_number: u32) -> Vec<u8> {
    let mut pdu = Vec::with_capacity(16);
    pdu.extend_from_slice(&16u32.to_be_bytes()); // command_length
    pdu.extend_from_slice(&command_id.to_be_bytes());
    pdu.extend_from_slice(&command_status.to_be_bytes());
    pdu.extend_from_slice(&sequence_number.to_be_bytes());
    pdu
}

/// Every status SMPP 3.4 §5.1.3 permits a peer to send but that our enum does
/// not name. Each of these panicked the session task before.
const UNNAMED_STATUSES: &[(u32, &str)] = &[
    (0x0000_0400, "first vendor-specific status"),
    (0x0000_04FF, "last vendor-specific status"),
    (0x0000_0450, "mid vendor-specific range"),
    (0x0000_0100, "first reserved-range status"),
    (0x0000_03FF, "last reserved-range status"),
    (0x0000_00C5, "start of the reserved C5-FD gap"),
    (0x0000_00FD, "end of the reserved C5-FD gap"),
    (0xFFFF_FFFF, "all bits set, the worst case a peer can send"),
];

#[test]
fn a_vendor_specific_command_status_does_not_panic_and_reads_as_unknown() {
    for (status, why) in UNNAMED_STATUSES {
        let pdu = response_pdu(0x8000_0000, *status, 0x0000_0001);
        let header = CommandHeader::decode(&pdu).expect("a 16-byte header is valid");
        let nack = generic_nack::decode(header, &pdu).expect("generic_nack decodes");

        // The point of the test: this call used to panic.
        assert_eq!(
            nack.get_error(),
            SmppError::ESME_RUNKNOWNERR,
            "command_status 0x{status:08X} ({why}) should report as ESME_RUNKNOWNERR"
        );
    }
}

#[test]
fn the_raw_command_status_survives_the_mapping() {
    // ESME_RUNKNOWNERR is a lossy reading, so the exact number a peer sent has to
    // stay reachable. Otherwise diagnosing a vendor status becomes impossible.
    let pdu = response_pdu(0x8000_0006, 0x0000_0451, 0x0000_0007);
    let header = CommandHeader::decode(&pdu).expect("header");
    let resp = unbind_resp::decode(header, &pdu).expect("unbind_resp decodes");

    assert_eq!(resp.get_error(), SmppError::ESME_RUNKNOWNERR);
    assert_eq!(
        resp.command_status(),
        0x0000_0451,
        "the vendor-specific value itself must still be readable"
    );
}

#[test]
fn every_named_status_still_maps_to_itself() {
    // The fallback must not swallow the statuses we do know: a regression here
    // would turn every real error into ESME_RUNKNOWNERR and lose all of them.
    for expected in [
        SmppError::ESME_ROK,
        SmppError::ESME_RINVMSGLEN,
        SmppError::ESME_RINVCMDLEN,
        SmppError::ESME_RINVCMDID,
        SmppError::ESME_RINVBNDSTS,
        SmppError::ESME_RSYSERR,
        SmppError::ESME_RMSGQFUL,
        SmppError::ESME_RINVPASWD,
        SmppError::ESME_RDELIVERYFAILURE,
        SmppError::ESME_RUNKNOWNERR,
    ] {
        let status = expected as u32;
        let pdu = response_pdu(0x8000_0000, status, 0x0000_0002);
        let header = CommandHeader::decode(&pdu).expect("header");
        let nack = generic_nack::decode(header, &pdu).expect("generic_nack decodes");
        assert_eq!(
            nack.get_error(),
            expected,
            "named status 0x{status:08X} must map to itself, not to the fallback"
        );
    }
}

#[test]
fn an_unnamed_status_on_a_bind_response_is_also_survivable() {
    // The bind path is where this actually bit: state.rs calls get_error() on the
    // response it just built from the peer's status to decide the transition.
    let resp = unbind::generic_reject(0x0000_0003, SmppError::ESME_RINVBNDSTS);
    assert_eq!(resp.get_error(), SmppError::ESME_RINVBNDSTS);

    // And through a hand-built wire PDU with a vendor status.
    let pdu = response_pdu(0x8000_0006, 0x0000_0470, 0x0000_0003);
    let header = CommandHeader::decode(&pdu).expect("header");
    let decoded = unbind_resp::decode(header, &pdu).expect("unbind_resp decodes");
    assert_eq!(decoded.get_error(), SmppError::ESME_RUNKNOWNERR);
    assert_eq!(decoded.command_status(), 0x0000_0470);
}
