use num_traits::FromPrimitive;

use crate::{CommandHeader, SmppError, CommandId};

/// 
/// The purpose of the SMPP unbind operation is to deregister an instance of an ESME from the
/// SMSC and inform the SMSC that the ESME no longer wishes to use this network connection
/// for the submission or delivery of messages.
/// Thus, the unbind operation may be viewed as a form of SMSC logoff request to close the
/// current SMPP session.
/// 
/// See '4.2 “UNBIND” Operation' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct unbind {
    header: CommandHeader,
}

impl unbind {

    pub(crate) fn with_sequence_number(sequence_number: u32) -> unbind {
        unbind {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::unbind as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number
            },
        }
    }

    /// Decode a unbind PDU
    /// 
    /// # Arguments
    /// 
    /// * `header` - already decoded Command Header which is only used for validation as unbind should not have a body
    /// * `pdu` - the complete PDU used for extra validation
    pub(crate) fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<unbind, SmppError> {
        if header.command_id == CommandId::unbind as u32 {
            if pdu.len() == 16 {
                Ok(unbind {
                    header,
                })
            } else {
                Err(SmppError::ESME_ROPTPARNOTALLWD) // Body should be empty
            }
        }
        else {
            panic!("Passed a non unbind to decode()")
        }
    }

    /// Encode a unbind based on inner command header
    pub fn encode(self) -> Vec<u8> {
        self.header.encode()
    }

    /// Create a unbind_resp with ESME_ROK as command_status
    pub fn accept(self) -> unbind_resp {
        unbind_resp { header: CommandHeader {
            command_length: 16, // No body
            command_id: CommandId::unbind_resp as u32,
            command_status: SmppError::ESME_ROK as u32,
            sequence_number: self.header.sequence_number,
        }}
    }

    /// Create a unbind_resp with a specific error as command_status
    /// 
    /// # Arguments
    /// 
    /// * `error` - the error to define as the command_status in the command header
    pub fn reject(self, error: SmppError) -> unbind_resp {
        unbind_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::unbind_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        } }
    }

    /// Create a unbind_resp with given sequence number and a specific error as command_status
    /// 
    /// Can be used if decoding of the unbind PDU fails or some internal error occurs
    /// 
    /// # Arguments
    /// 
    /// * `sequence_number` - the sequence_number to set in the command header
    /// * `error` - the error to define as the command_status in the command header
    pub fn generic_reject(sequence_number: u32, error: SmppError) -> unbind_resp {
        unbind_resp  { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::unbind_resp as u32,
            command_status: error as u32,
            sequence_number,
        }}
    }
}


/// The SMPP unbind_resp PDU is used to reply to an unbind request. It comprises the SMPP
/// message header only.
/// 
/// See '4.2.2 “UNBIND_RESP”' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct unbind_resp {
    header: CommandHeader,

}

impl unbind_resp {

    /// Helper to detect whether the unbind_resp is successful (i.e. has status ESME_ROK)
    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    
    /// Get inner command_status
    pub fn command_status(&self) -> u32 { self.header.command_status }

    /// Convert inner command_status into an SmppError enum
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    /// Encode a unbind_resp based on inner command header
    pub fn encode(self) -> Vec<u8> { self.header.encode() }
}

#[cfg(test)]
mod all_unbind_tests {
    #[cfg(test)]
    mod unbind_tests {
        use crate::{CommandHeader, SmppError, unbind, CommandId};
    
    
        #[test]
        fn encode_unbind1() {
            let unbind = unbind::with_sequence_number(0x12344567);
            let pdu = unbind.encode();
            assert_eq!(pdu, vec![0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x45, 0x67]);
        }

        #[test]
        fn encode_unbind2() {
            let unbind = unbind { header: CommandHeader { command_length: 16, command_id: CommandId::unbind as u32, command_status: SmppError::ESME_ROK as u32, sequence_number: 0x12344567 } };
            let pdu = unbind.encode();
            assert_eq!(pdu, vec![0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x45, 0x67]);
        }
    
        #[test]
        fn decode_unbind() {
            let pdu = vec![0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x45, 0x67];
            let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
            let decoded = unbind::decode(decoded_command_header, &pdu).expect("Unable to decode unbind");
    
            assert_eq!(decoded.header.command_length, 16);
            assert_eq!(decoded.header.command_id, CommandId::unbind as u32);
            assert_eq!(decoded.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(decoded.header.sequence_number, 0x12344567);
        }
    
        #[test]
        fn decode_unbind_with_body() {
            let pdu = vec![0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x45, 0x67, 0xde, 0xad, 0xbe, 0xef];
            let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
            let decoded = unbind::decode(decoded_command_header, &pdu).unwrap_err();
            assert_eq!(decoded, SmppError::ESME_ROPTPARNOTALLWD)
        }
    
        #[test]
        #[should_panic(expected = "Passed a non unbind to decode()")]
        fn decode_generic_nack_with_invalid_command_id() {
            let pdu = vec![0x00, 0x00, 0x00, 0x10, 0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x34, 0x45, 0x67];
            let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
            unbind::decode(decoded_command_header, &pdu).expect("Unable to decode unbind");
        }
    
        #[test]
        fn unbind_accept() {
            let unbind = unbind { header: CommandHeader { command_length: 16, command_id: CommandId::unbind as u32, command_status: SmppError::ESME_ROK as u32, sequence_number: 0x12344567 } };
            let unbind_resp = unbind.accept();
    
            assert_eq!(unbind_resp.header.command_length, 16);
            assert_eq!(unbind_resp.header.command_id, CommandId::unbind_resp as u32);
            assert_eq!(unbind_resp.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(unbind_resp.header.sequence_number, 0x12344567);
        }
    
        #[test]
        fn unbind_reject() {
            let unbind = unbind { header: CommandHeader { command_length: 16, command_id: CommandId::unbind as u32, command_status: SmppError::ESME_ROK as u32, sequence_number: 0x12344567 } };
            let unbind_resp = unbind.reject(SmppError::ESME_RINVBNDSTS);
    
            assert_eq!(unbind_resp.header.command_length, 16);
            assert_eq!(unbind_resp.header.command_id, CommandId::unbind_resp as u32);
            assert_eq!(unbind_resp.header.command_status, SmppError::ESME_RINVBNDSTS as u32);
            assert_eq!(unbind_resp.header.sequence_number, 0x12344567);
        }
    
        #[test]
        fn unbind_generic_reject() {
            let unbind_resp = unbind::generic_reject(0x12344567, SmppError::ESME_RINVBNDSTS);
    
            assert_eq!(unbind_resp.header.command_length, 16);
            assert_eq!(unbind_resp.header.command_id, CommandId::unbind_resp as u32);
            assert_eq!(unbind_resp.header.command_status, SmppError::ESME_RINVBNDSTS as u32);
            assert_eq!(unbind_resp.header.sequence_number, 0x12344567);
        }
    
    }
    
    #[cfg(test)]
    mod unbind_resp_tests {
        use crate::{unbind, SmppError};
    
        #[test]
        fn unbind_resp_is_success() {
            let unbind_resp = unbind::generic_reject(0x1234567, SmppError::ESME_ROK);
            assert_eq!(unbind_resp.is_success(), true);
    
            let unbind_resp = unbind::generic_reject(0x1234567, SmppError::ESME_RINVBNDSTS);
            assert_eq!(unbind_resp.is_success(), false);
        }
    
        #[test]
        fn unbind_resp_command_status() {
            let unbind_resp = unbind::generic_reject(0x1234567, SmppError::ESME_RINVBNDSTS);
            assert_eq!(unbind_resp.header.command_status, 0x00000004);
        }
    
        #[test]
        fn unbind_resp_get_error() {
            let unbind_resp = unbind::generic_reject(0x1234567, SmppError::ESME_RINVBNDSTS);
            assert_eq!(unbind_resp.get_error(), SmppError::ESME_RINVBNDSTS);
        }
    
        #[test]
        fn encode_unbind_resp() {
            let unbind_resp = unbind::generic_reject(0x12344567, SmppError::ESME_RINVBNDSTS);
            let pdu = unbind_resp.encode();
            assert_eq!(pdu, vec![0x00, 0x00, 0x00, 0x10, 0x80, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x04, 0x12, 0x34, 0x45, 0x67]);
        }
    
    }
}

