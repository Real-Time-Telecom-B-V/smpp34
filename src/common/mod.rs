mod commands;

use log::error;

// Re-exports
pub use commands::bind_transmitter::*;
pub use commands::bind_receiver::*;
pub use commands::bind_transceiver::*;
pub use commands::outbind::*;
pub use commands::unbind::*;
pub use commands::submit_sm::*;

pub use commands::generic_nack::*;


/// The general format of an SMPP PDU consists of a PDU header followed by a PDU body
/// 
/// The SMPP Header is a mandatory part of every SMPP PDU and must always be present. The
/// SMPP PDU Body is optional and may not be included with every SMPP PDU.
#[derive(Debug, Clone)]
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
                command_length: u32::from_be_bytes(pdu[0..4].try_into().expect("Can not read command_length")),
                command_id: u32::from_be_bytes(pdu[4..8].try_into().expect("Can not read command_id")), 
                command_status: u32::from_be_bytes(pdu[8..12].try_into().expect("Can not read command_status")),
                sequence_number: u32::from_be_bytes(pdu[12..16].try_into().expect("Can not read sequence_number")),
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

#[derive(Debug, PartialEq, FromPrimitive, ToPrimitive)]
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

fn encode_bind_response(header: CommandHeader, system_id: Option<String>, sc_interface_version: Option<u8>) -> Vec<u8> {
    let command_status = header.command_status;

    let mut buffer: Vec<u8> = Vec::with_capacity(header.command_length as usize);
    buffer.append(&mut header.encode());

    if command_status == SmppError::ESME_ROK as u32 {
        buffer.append(&mut system_id.expect("How can we have no system_id when command_status is ESME_ROK").into_bytes());
        buffer.push(0x00); // system_id is a C-Octet-String so terminate with 0x00
    }

    if sc_interface_version.is_some() {
        let mut tlv_tag = vec![0x02, 0x10, 0x00, 0x01]; // TLV 0x0210 with Length 0x0001 as sc_interfae_version is only 1 byte
        buffer.append(&mut tlv_tag); 
        buffer.push(sc_interface_version.unwrap());
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

fn parse_octet_string(bytes: Vec<u8>, supposed_length: usize, maximum_length: usize) -> Result<String, SmppError> {
    if supposed_length > maximum_length {
        error!("Octet-String supposed length {} is over maximum allowed length {}", supposed_length, maximum_length);
        Err(SmppError::ESME_RINVPARLEN)
    }
    else if bytes.len() < supposed_length {
        error!("Octet-String supposed length {} is too long for amount of remaining bytes {}", supposed_length, bytes.len());
        Err(SmppError::ESME_RINVPARLEN)
    } else {
        String::from_utf8(bytes[0..supposed_length].to_vec()).map_err(|_x| SmppError::ESME_RINVPARLEN)
    }
}

/*fn parse_optional_u8(bytes: Vec<u8>, tag) -> Result<Option<u16>, SmppError> {
    Err(SmppError::ESME_RINVOPTPARAMVAL)
}

fn parse_optional_u16(bytes: Vec<u8>) -> Result<u16, SmppError> {
    Err(SmppError::ESME_RINVOPTPARAMVAL)
}*/

fn decode_bind_request(header: CommandHeader, pdu: &Vec<u8>) -> Result<CommonBindRequestParameters, SmppError> {
    // CommondHeader decode method makes sure that PDU length matches the command_length so no need to check this again
    
    // First we expect the system_id which is a C-Octet-String terminated by 0 and maximum 16 in length
    let system_id = parse_c_octet_string(pdu[16..].to_vec(), 16)?;

    // Then we expect the password which is a C-Octet-String terminated by 0 and maximum 9 in length
    let password = parse_c_octet_string(pdu[16 + system_id.len() + 1..].to_vec(), 9)?;

    // Then we expect the system_type which is a C-Octet-String terminated by 0 and maximum 13 in length
    let system_type = parse_c_octet_string(pdu[16 + system_id.len() + password.len() + 2..].to_vec(), 13)?;

    let interface_version = parse_next_int(pdu, 16 + system_id.len() + password.len() + system_type.len() + 3)?;
    let addr_ton = parse_next_int(pdu, 16 + system_id.len() + password.len() + system_type.len() + 4)?;
    let addr_npi = parse_next_int(pdu, 16 + system_id.len() + password.len() + system_type.len() + 5)?;

    // Then we expect the address_range which is a C-Octet-String terminated by 0 and maximum 41 in length
    let address_range = parse_c_octet_string(pdu[16 + system_id.len() + password.len() + system_type.len() + 6..].to_vec(), 41)?;

    Ok(CommonBindRequestParameters {
        header,
        system_id,
        password,
        system_type,
        interface_version,
        addr_ton,
        addr_npi,
        address_range,
    })
}




pub struct submit_sm_multi {

}

pub struct submit_sm_multi_resp {
    
}


pub struct data_sm {

}

pub struct data_sm_resp {
    
}

pub struct deliver_sm {

}

pub struct deliver_sm_resp {
    
}

pub struct query_sm {

}

pub struct query_sm_resp {
    
}

pub struct cancel_sm {

}

pub struct cancel_sm_resp {
    
}

pub struct replace_sm {

}

pub struct replace_sm_resp {
    
}

pub struct enquire_link {

}

pub struct enquire_link_resp {

}

pub struct alert_notification {

}

