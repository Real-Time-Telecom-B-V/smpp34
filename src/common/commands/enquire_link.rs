use log::error;
use num_traits::FromPrimitive;

use crate::{CommandHeader, SmppError, CommandId};

/// 
/// This message can be sent by either the ESME or SMSC and is used to provide a confidence check of the communication path between an ESME and an SMSC. On receipt of this request
/// the receiving party should respond with an enquire_link_resp, thus verifying that the application level connection between the SMSC and the ESME is functioning. The ESME may 
/// also respond by sending any valid SMPP primitive.
/// 
/// See '4.11 “ENQUIRE_LINK” Operation' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct enquire_link {
    header: CommandHeader,
}

impl enquire_link {

    /// Construct a enquire_link primitive
    /// 
    /// # Arguments
    /// 
    /// * `seq_no` - sequence number 
    pub fn new(seq_no: u32) -> enquire_link {
        enquire_link { header: CommandHeader { command_length: 16 as u32, command_id: CommandId::enquire_link as u32, command_status: SmppError::ESME_ROK as u32, sequence_number: seq_no } }
    }

    /// Decode a enquire_link PDU
    /// 
    /// # Arguments
    /// 
    /// * `header` - already decoded Command Header which is only used for validation as enquire_link should not have a body
    /// * `pdu` - the complete PDU used for extra validation
    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<enquire_link, SmppError> {
        if header.command_id == CommandId::enquire_link as u32 {
            if pdu.len() == 16 {
                Ok(enquire_link {
                    header,
                })
            } else {
                Err(SmppError::ESME_ROPTPARNOTALLWD) // Body should be empty
            }
        }
        else {
            error!("Passed a non enquire_link PDU to enquire_link::decode(), command_id: 0x{:08X}", header.command_id);
            return Err(SmppError::ESME_RINVCMDID)
        }
    }

    /// Encode a enquire_link based on inner command header
    pub fn encode(self) -> Vec<u8> {
        self.header.encode()
    }

    /// Create a enquire_link_resp with ESME_ROK as command_status
    pub fn accept(self) -> enquire_link_resp {
        enquire_link_resp { header: CommandHeader {
            command_length: 16, // No body
            command_id: CommandId::enquire_link_resp as u32,
            command_status: SmppError::ESME_ROK as u32,
            sequence_number: self.header.sequence_number,
        }}
    }

    /// Create a enquire_link_resp with a specific error as command_status
    /// 
    /// # Arguments
    /// 
    /// * `error` - the error to define as the command_status in the command header
    pub fn reject(self, error: SmppError) -> enquire_link_resp {
        enquire_link_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::enquire_link_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        } }
    }

    /// Create a enquire_link_resp with given sequence number and a specific error as command_status
    /// 
    /// Can be used if decoding of the enquire_link PDU fails or some internal error occurs
    /// 
    /// # Arguments
    /// 
    /// * `sequence_number` - the sequence_number to set in the command header
    /// * `error` - the error to define as the command_status in the command header
    pub fn generic_reject(sequence_number: u32, error: SmppError) -> enquire_link_resp {
        enquire_link_resp  { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::enquire_link_resp as u32,
            command_status: error as u32,
            sequence_number,
        }}
    }
}


/// The SMPP enquire_link_resp PDU is used to reply to an enquire_link request. It comprises the SMPP
/// message header only.
/// 
/// See '4.11.2 “ENQUIRE_LINK_RESP” Syntax' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct enquire_link_resp {
    header: CommandHeader,

}

impl enquire_link_resp {

    /// Helper to detect whether the enquire_link_resp is successful (i.e. has status ESME_ROK)
    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    
    /// Get inner command_status
    pub fn command_status(&self) -> u32 { self.header.command_status }

    /// Convert inner command_status into an SmppError enum
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    /// Encode a enquire_link_resp based on inner command header
    pub fn encode(self) -> Vec<u8> { self.header.encode() }
}

#[cfg(test)]
mod all_enquire_link_tests {
    #[cfg(test)]
    mod enquire_link_tests {
        use crate::{CommandHeader, SmppError, enquire_link, CommandId};

        #[test]
        fn new_enquire_link() {
            let enquire_link = enquire_link::new(0x1234);
            assert_eq!(enquire_link.header.command_length, 16);
            assert_eq!(enquire_link.header.command_id, CommandId::enquire_link as u32);
            assert_eq!(enquire_link.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(enquire_link.header.sequence_number, 0x1234);
        }
    
        #[test]
        fn encode_enquire_link() {
            let enquire_link = enquire_link::new(0x12344567);
            let pdu = enquire_link.encode();
            assert_eq!(pdu, vec![0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x45, 0x67]);
        }
    
        #[test]
        fn decode_enquire_link() {
            let pdu = vec![0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x45, 0x67];
            let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
            let decoded = enquire_link::decode(decoded_command_header, &pdu).expect("Unable to decode enquire_link");
    
            assert_eq!(decoded.header.command_length, 16);
            assert_eq!(decoded.header.command_id, CommandId::enquire_link as u32);
            assert_eq!(decoded.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(decoded.header.sequence_number, 0x12344567);
        }
    
        #[test]
        fn decode_enquire_link_with_body() {
            let pdu = vec![0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x45, 0x67, 0xde, 0xad, 0xbe, 0xef];
            let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
            let decoded = enquire_link::decode(decoded_command_header, &pdu).unwrap_err();
            assert_eq!(decoded, SmppError::ESME_ROPTPARNOTALLWD)
        }
    
        #[test]
        fn decode_generic_nack_with_invalid_command_id() {
            let pdu = vec![0x00, 0x00, 0x00, 0x10, 0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x34, 0x45, 0x67];
            let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
            let result = enquire_link::decode(decoded_command_header, &pdu);
            assert!(result.is_err());
        }
    
        #[test]
        fn enquire_link_accept() {
            let enquire_link = enquire_link { header: CommandHeader { command_length: 16, command_id: CommandId::enquire_link as u32, command_status: SmppError::ESME_ROK as u32, sequence_number: 0x12344567 } };
            let enquire_link_resp = enquire_link.accept();
    
            assert_eq!(enquire_link_resp.header.command_length, 16);
            assert_eq!(enquire_link_resp.header.command_id, CommandId::enquire_link_resp as u32);
            assert_eq!(enquire_link_resp.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(enquire_link_resp.header.sequence_number, 0x12344567);
        }
    
        #[test]
        fn enquire_link_reject() {
            let enquire_link = enquire_link { header: CommandHeader { command_length: 16, command_id: CommandId::enquire_link as u32, command_status: SmppError::ESME_ROK as u32, sequence_number: 0x12344567 } };
            let enquire_link_resp = enquire_link.reject(SmppError::ESME_RINVBNDSTS);
    
            assert_eq!(enquire_link_resp.header.command_length, 16);
            assert_eq!(enquire_link_resp.header.command_id, CommandId::enquire_link_resp as u32);
            assert_eq!(enquire_link_resp.header.command_status, SmppError::ESME_RINVBNDSTS as u32);
            assert_eq!(enquire_link_resp.header.sequence_number, 0x12344567);
        }
    
        #[test]
        fn enquire_link_generic_reject() {
            let enquire_link_resp = enquire_link::generic_reject(0x12344567, SmppError::ESME_RINVBNDSTS);
    
            assert_eq!(enquire_link_resp.header.command_length, 16);
            assert_eq!(enquire_link_resp.header.command_id, CommandId::enquire_link_resp as u32);
            assert_eq!(enquire_link_resp.header.command_status, SmppError::ESME_RINVBNDSTS as u32);
            assert_eq!(enquire_link_resp.header.sequence_number, 0x12344567);
        }
    
    }
    
    #[cfg(test)]
    mod enquire_link_resp_tests {
        use crate::{enquire_link, SmppError};
    
        #[test]
        fn enquire_link_resp_is_success() {
            let enquire_link_resp = enquire_link::generic_reject(0x1234567, SmppError::ESME_ROK);
            assert_eq!(enquire_link_resp.is_success(), true);
    
            let enquire_link_resp = enquire_link::generic_reject(0x1234567, SmppError::ESME_RINVBNDSTS);
            assert_eq!(enquire_link_resp.is_success(), false);
        }
    
        #[test]
        fn enquire_link_resp_command_status() {
            let enquire_link_resp = enquire_link::generic_reject(0x1234567, SmppError::ESME_RINVBNDSTS);
            assert_eq!(enquire_link_resp.header.command_status, 0x00000004);
        }
    
        #[test]
        fn enquire_link_resp_get_error() {
            let enquire_link_resp = enquire_link::generic_reject(0x1234567, SmppError::ESME_RINVBNDSTS);
            assert_eq!(enquire_link_resp.get_error(), SmppError::ESME_RINVBNDSTS);
        }
    
        #[test]
        fn encode_enquire_link_resp() {
            let enquire_link_resp = enquire_link::generic_reject(0x12344567, SmppError::ESME_RINVBNDSTS);
            let pdu = enquire_link_resp.encode();
            assert_eq!(pdu, vec![0x00, 0x00, 0x00, 0x10, 0x80, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x04, 0x12, 0x34, 0x45, 0x67]);
        }
    
    }
}

