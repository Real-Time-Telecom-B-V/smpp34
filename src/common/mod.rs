mod commands;
pub mod tlv;

use std::net::SocketAddr;

use chrono::DateTime;
use chrono::Utc;
use downcast_rs::impl_downcast;
use downcast_rs::DowncastSync;
use log::error;
// Re-exports
pub use commands::bind_transmitter::*;
pub use commands::bind_receiver::*;
pub use commands::bind_transceiver::*;
pub use commands::outbind::*;
pub use commands::unbind::*;
pub use commands::submit_sm::*;
pub use commands::submit_sm_multi::*;
pub use commands::data_sm::*;
pub use commands::deliver_sm::*;
pub use commands::query_sm::*;
pub use commands::cancel_sm::*;
pub use commands::replace_sm::*;
pub use commands::enquire_link::*;
pub use commands::alert_notification::*;
pub use commands::generic_nack::*;

use nom::{
    self,
    bytes::complete::{take, take_until},
    IResult,
};

/// The general format of an SMPP PDU consists of a PDU header followed by a PDU body
/// 
/// The SMPP Header is a mandatory part of every SMPP PDU and must always be present. The
/// SMPP PDU Body is optional and may not be included with every SMPP PDU.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandHeader {

    /// The command_length parameter indicates the length in octets of the SMPP message. The SMPP
    /// message header (including the command_length field itself), the mandatory parameters and the
    /// optional parameters are all considered.
    pub command_length: u32,

    /// The command_id field identifies the type of message the SMPP PDU represents, for example,
    /// submit_sm, query_sm etc.
    /// A command identifier is allocated to each SMPP request primitive. For reserved range value
    /// settings refer to Table 5-1:.
    /// A response command identifier is allocated to each response primitive. For reserved range
    /// value settings refer to Table 5-1: (In general a response command identifier is identical to the
    /// corresponding request command identifier, but with bit 31 set). 
    pub command_id: u32,

    /// The command_length parameter indicates the length in octets of the SMPP message. The SMPP
    /// message header (including the command_length field itself), the mandatory parameters and the
    /// optional parameters are all considered.
    pub command_status: u32, 

    /// A sequence number allows a response PDU to be correlated with a request PDU.
    /// The associated SMPP response PDU must preserve this field.
    /// The allowed sequence_number range is from 0x00000001 to 0x7FFFFFFF
    pub sequence_number: u32, 
}

impl CommandHeader {
    pub fn decode(pdu: &Vec<u8>) -> Result<CommandHeader, SmppError> {
        if pdu.len() < 16 { // PDU Command Header is mandatory, we need at least 16 bytes
            error!("PDU can not contain a valid SMPP header as it's shorter than 16 bytes");
            Err(SmppError::ESME_RINVCMDLEN)
        } else {
            let command_header = CommandHeader {
                command_length: u32::from_be_bytes(pdu[0..4].try_into().map_err(|_| SmppError::ESME_RINVCMDLEN)?),
                command_id: u32::from_be_bytes(pdu[4..8].try_into().map_err(|_| SmppError::ESME_RINVCMDLEN)?),
                command_status: u32::from_be_bytes(pdu[8..12].try_into().map_err(|_| SmppError::ESME_RINVCMDLEN)?),
                sequence_number: u32::from_be_bytes(pdu[12..16].try_into().map_err(|_| SmppError::ESME_RINVCMDLEN)?),
            };

            if pdu.len() != command_header.command_length as usize {
                error!("PDU length {} does not match command_length {}", pdu.len(), command_header.command_length);
                Err(SmppError::ESME_RINVMSGLEN)
            } else {
                Ok(command_header)
            }
        }
    }

    pub fn encode(self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(16);
        buffer.append(&mut self.command_length.to_be_bytes().to_vec());
        buffer.append(&mut self.command_id.to_be_bytes().to_vec());
        buffer.append(&mut self.command_status.to_be_bytes().to_vec());
        buffer.append(&mut self.sequence_number.to_be_bytes().to_vec());
        buffer
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeliveryReceipt {
    pub id: String,
    pub sub: u8,
    pub dlvrd: u8,
    pub submit_date: String,
    pub done_date: String,
    pub stat: String,
    pub err: String,
    pub text: String,
}

impl DeliveryReceipt {

    pub fn new(id: String, sub: u8, dlvrd: u8, submit_date: DateTime<Utc> , done_date: DateTime<Utc>, stat: String, err: String, text: String) -> DeliveryReceipt {
        
        DeliveryReceipt {
            id,
            sub,
            dlvrd,
            submit_date: submit_date.format("%y%m%d%H%M").to_string(),
            done_date: done_date.format("%y%m%d%H%M").to_string(),
            stat,
            err,
            text,
        }
    }

    pub fn encode(&self) -> String {
        format!("id:{} sub:{} dlvrd:{} submit date:{} done date:{} stat:{} err:{} text:{}",
            self.id, self.sub, self.dlvrd, self.submit_date, self.done_date, self.stat, self.err, self.text)
    }

}

// SMPP command_id is a 4-octet unsigned integer (SMPP v3.4 §3.2); response
// IDs set the high bit (0x80000000 | request_id), so an explicit u32 repr is
// required for the discriminants to be portable to 32-bit targets.
#[repr(u32)]
#[derive(Debug, PartialEq, FromPrimitive)]
pub (crate) enum CommandId {
    generic_nack = 0x80000000,
    bind_receiver =  0x00000001,
    bind_receiver_resp =  0x80000001,
    bind_transmitter =  0x00000002,
    bind_transmitter_resp =  0x80000002,
    query_sm =  0x00000003,
    query_sm_resp =  0x80000003,
    submit_sm =  0x00000004,
    submit_sm_resp =  0x80000004,
    deliver_sm =  0x00000005,
    deliver_sm_resp =  0x80000005,
    unbind =  0x00000006,
    unbind_resp =  0x80000006,
    replace_sm =  0x00000007,
    replace_sm_resp =  0x80000007,
    cancel_sm =  0x00000008,
    cancel_sm_resp =  0x80000008,
    bind_transceiver =  0x00000009,
    bind_transceiver_resp =  0x80000009,

    outbind =  0x0000000B,
    
    enquire_link = 0x00000015,
    enquire_link_resp = 0x80000015,

    submit_multi = 0x00000021,
    submit_multi_resp = 0x80000021,
    
    alert_notification = 0x00000102,
    
    data_sm = 0x00000103,
    data_sm_resp = 0x80000103,
}

#[derive(Debug, Clone, Copy, PartialEq, FromPrimitive, ToPrimitive)]
pub enum SmppError {
    ESME_ROK = 0x00000000, // No Error
    ESME_RINVMSGLEN = 0x00000001, // Message Length is invalid
    ESME_RINVCMDLEN = 0x00000002, // Command Length is invalid
    ESME_RINVCMDID = 0x00000003, // Invalid Command ID
    ESME_RINVBNDSTS = 0x00000004, // Incorrect BIND Status for given command
    ESME_RALYBND = 0x00000005, // ESME Already in Bound State
    ESME_RINVPRTFLG = 0x00000006, // Invalid Priority Flag
    ESME_RINVREGDLVFLG = 0x00000007, // Invalid Registered Delivery Flag
    ESME_RSYSERR = 0x00000008, // System Error
    
    ESME_RINVSRCADR = 0x0000000A, // Invalid Source Address
    ESME_RINVDSTADR = 0x0000000B, // Invalid Dest Addr
    ESME_RINVMSGID = 0x0000000C, // Message ID is invalid
    ESME_RBINDFAIL = 0x0000000D, // Bind Failed
    ESME_RINVPASWD = 0x0000000E, // Invalid Password
    ESME_RINVSYSID = 0x0000000F, // Invalid System ID
    
    ESME_RCANCELFAIL = 0x00000011, // Cancel SM Failed
    
    ESME_RREPLACEFAIL = 0x00000013, // Replace SM Failed

    ESME_RMSGQFUL = 0x00000014, // Message Queue Full
    ESME_RINVSERTYP = 0x00000015, // Invalid Service Type

    ESME_RINVNUMDESTS = 0x00000033, // Invalid number of destinations
    ESME_RINVDLNAME = 0x00000034, // Invalid Distribution List name

    ESME_RINVDESTFLAG = 0x00000040, // Destination flag is invalid (submit_multi)

    ESME_RINVSUBREP = 0x00000042, // Invalid ‘submit with replace’ request (i.e. submit_sm with replace_if_present_flag set)
    ESME_RINVESMCLASS = 0x00000043, // Invalid esm_class field data
    ESME_RCNTSUBDL = 0x00000044, // Cannot Submit to Distribution List
    ESME_RSUBMITFAIL = 0x00000045, // submit_sm or submit_multi failed

    ESME_RINVSRCTON = 0x00000048, // Invalid Source address TON
    ESME_RINVSRCNPI = 0x00000049, // Invalid Source address NPI
    ESME_RINVDSTTON = 0x00000050, // Invalid Destination address TON
    ESME_RINVDSTNPI = 0x00000051, // Invalid Destination address NPI

    ESME_RINVSYSTYP = 0x00000053, // Invalid system_type field
    ESME_RINVREPFLAG = 0x00000054, // Invalid replace_if_present flag
    ESME_RINVNUMMSGS = 0x00000055, // Invalid number of messages

    ESME_RTHROTTLED = 0x00000058, // Throttling error (ESME has exceeded allowed message limits)

    ESME_RINVSCHED = 0x00000061, // Invalid Scheduled Delivery Time
    ESME_RINVEXPIRY = 0x00000062, // Invalid message validity period (Expiry time)
    ESME_RINVDFTMSGID = 0x00000063, // Predefined Message Invalid or Not Found
    ESME_RX_T_APPN = 0x00000064, // ESME Receiver Temporary App Error Code
    ESME_RX_P_APPN = 0x00000065, // ESME Receiver Permanent App Error Code
    ESME_RX_R_APPN = 0x00000066, // ESME Receiver Reject Message Error Code
    ESME_RQUERYFAIL = 0x00000067, // query_sm request failed

    ESME_RINVOPTPARSTREAM = 0x000000C0, // Error in the optional part of the PDU Body.
    ESME_ROPTPARNOTALLWD = 0x000000C1, // Optional Parameter not allowed
    ESME_RINVPARLEN = 0x000000C2, // Invalid Parameter Length.
    ESME_RMISSINGOPTPARAM = 0x000000C3, // Expected Optional Parameter missing
    ESME_RINVOPTPARAMVAL = 0x000000C4, // Invalid Optional Parameter Value

    ESME_RDELIVERYFAILURE = 0x000000FE, // Delivery Failure (used for data_sm_resp)
    ESME_RUNKNOWNERR = 0x000000FF, // Unknown Error
}

fn encode_bind_request(header: CommandHeader, system_id: String, password: String, system_type: String, interface_version: u8, addr_ton: u8, addr_npi: u8, address_range: String) -> Vec<u8> {
    let mut buffer: Vec<u8> = Vec::with_capacity(header.command_status as usize);
    buffer.append(&mut header.encode());
    buffer.append(&mut system_id.into_bytes());
    buffer.push(0x00); // system_id is a C-Octet-String so terminate with 0x00
    buffer.append(&mut password.into_bytes());
    buffer.push(0x00); // password is a C-Octet-String so terminate with 0x00
    buffer.append(&mut system_type.into_bytes());
    buffer.push(0x00); // system_type is a C-Octet-String so terminate with 0x00
    buffer.push(interface_version);
    buffer.push(addr_ton);
    buffer.push(addr_npi);
    buffer.append(&mut address_range.into_bytes());
    buffer.push(0x00); // address_range is a C-Octet-String so terminate with 0x00
    buffer
}

fn encode_bind_response(header: CommandHeader, system_id: Option<String>, sc_interface_version: Option<u8>) -> Vec<u8> {
    let command_status = header.command_status;

    let mut buffer: Vec<u8> = Vec::with_capacity(header.command_length as usize);
    buffer.append(&mut header.encode());

    if command_status == SmppError::ESME_ROK as u32 {
        if let Some(id) = system_id {
            buffer.append(&mut id.into_bytes());
        }
        buffer.push(0x00); // system_id is a C-Octet-String so terminate with 0x00
    }

    if let Some(version) = sc_interface_version {
        let version_tlv = tlv::Tlv::from_tag(tlv::TlvTag::ScInterfaceVersion, vec![version]);
        buffer.extend_from_slice(&version_tlv.encode());
    }

    buffer
}

struct CommonBindRequestParameters {
    header: CommandHeader,
    system_id: String,
    password: String,
    system_type: String,
    interface_version: u8,
    addr_ton: u8,
    addr_npi: u8,
    address_range: String
}

fn parse_next_int(pdu: &Vec<u8>, position: usize) -> Result<u8, SmppError> {
    if position < pdu.len() {
        Ok(pdu[position])
    } else {
        error!("Can not parse next int, insufficient bytes left");
        Err(SmppError::ESME_RINVPARLEN)
    }
}

fn parse_c_octet_string(bytes: Vec<u8>, maximum_length: usize) -> Result<String, SmppError> {
    // First we find the position of the 0x00 byte
    if let Some(index) = bytes.iter().position(|&r| r == 0x00) {
        if index <= maximum_length {
            String::from_utf8(bytes[0..index].to_vec()).map_err(|_x| SmppError::ESME_RINVPARLEN)
        } else {
            error!("C-Octet-String is too long");
            Err(SmppError::ESME_RINVPARLEN)
        }
    } else {
        error!("Can not find null byte at all, C-Octet-String not terminated?!");
        Err(SmppError::ESME_RINVPARLEN)
    }
}

// Helper to parse C-Octet strings (null-terminated)
pub(crate) fn parse_c_octet_string_nom(input: &[u8]) -> IResult<&[u8], String> {
    let (input, result) = take_until("\0")(input)?;
    let (input, _) = take(1usize)(input)?; // consume the null byte
    Ok((input, String::from_utf8_lossy(result).to_string()))
}

fn parse_octet_string_as_vec(bytes: Vec<u8>, supposed_length: usize, maximum_length: usize) -> Result<Vec<u8>, SmppError> {
    if supposed_length > maximum_length {
        error!("Octet-String supposed length {} is over maximum allowed length {}", supposed_length, maximum_length);
        Err(SmppError::ESME_RINVPARLEN)
    }
    else if bytes.len() < supposed_length {
        error!("Octet-String supposed length {} is too long for amount of remaining bytes {}", supposed_length, bytes.len());
        Err(SmppError::ESME_RINVPARLEN)
    } else {
        Ok(bytes[0..supposed_length].to_vec())
    }
}


fn decode_bind_request(header: CommandHeader, pdu: &Vec<u8>) -> Result<CommonBindRequestParameters, SmppError> {
    if pdu.len() < 16 {
        return Err(SmppError::ESME_RINVCMDLEN);
    }
    let input = &pdu[16..];
    let (input, system_id) = parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
    let (input, password) = parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
    let (input, system_type) = parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
    let (input, interface_version_bytes) = take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
    let (input, addr_ton_bytes) = take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
    let (input, addr_npi_bytes) = take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
    let (_input, address_range) = parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;

    Ok(CommonBindRequestParameters {
        header, system_id, password, system_type,
        interface_version: interface_version_bytes[0],
        addr_ton: addr_ton_bytes[0],
        addr_npi: addr_npi_bytes[0],
        address_range,
    })
}


#[derive(Debug, Clone)]
pub struct SmppConnectionInformation {
    pub server_address: SocketAddr,
    pub client_address: SocketAddr,
}

pub trait SmppReply : DowncastSync {} 

impl_downcast!(sync SmppReply); 

pub (crate) struct WriteFrame {
    /// If a sequence number is set it's a request, so we expect a response, if not it's a response from our end
    pub(crate) our_sequence_number: Option<u32>,

    /// The actual PDU to send on the TCP connection
    pub(crate) pdu: Vec<u8>,

    pub(crate) oneshot: Option<tokio::sync::oneshot::Sender<Box<dyn SmppReply + Send + Sync + 'static>>>,
}
