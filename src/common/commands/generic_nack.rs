use num_traits::FromPrimitive;

use crate::{CommandHeader, CommandId, SmppError, SmppReply};

/// 
/// This is a generic negative acknowledgement to an SMPP PDU submitted with an invalid
/// message header. A generic_nack response is returned in the following cases:
/// 
/// * Invalid command_length If the receiving SMPP entity, on decoding an SMPP PDU, detects an invalid command_length (either too short or too long), it should assume that the data is corrupt. In such cases a generic_nack PDU must be returned to the message originator.
/// * Unknown command_id If an unknown or invalid command_id is received, a generic_nack PDU must also be returned to the originator.
/// 
/// See '4.3 “GENERIC_NACK” PDU' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct generic_nack {
    pub(crate) header: CommandHeader
}

impl SmppReply for generic_nack {}

impl generic_nack {

    /// Construct a generic_nack primitive
    /// 
    /// # Arguments
    /// 
    /// * `error` - command_status to be sent back (expecting an error code here)
    /// * `seq_no` - sequence number (if it could be read from the incoming PDU otherwise 0)
    pub fn new(error: SmppError, seq_no: u32) -> generic_nack {
        generic_nack { header: CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: seq_no } }
    }

    /// Decode a generic_nack PDU
    /// 
    /// # Arguments
    /// 
    /// * `header` - already decoded Command Header which is only used for validation as generic_nack should not have a body
    /// * `pdu` - the complete PDU used for extra validation
    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<generic_nack, SmppError> {
        if header.command_id == CommandId::generic_nack as u32 {
            if pdu.len() == 16 {
                Ok(generic_nack {
                    header,
                })
            } else {
                Err(SmppError::ESME_ROPTPARNOTALLWD) // Body should be empty
            }
        }
        else {
            return Err(SmppError::ESME_RINVCMDID)
        }
    }

    /// Encode a generic_nack based on inner command header
    pub fn encode(self) -> Vec<u8> {
        self.header.encode()
    }

    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }
}

#[cfg(test)]
mod generic_nack_tests {
    use crate::{generic_nack, CommandId, SmppError, CommandHeader};


    #[test]
    fn new_generic_nack() {
        let generic_nack = generic_nack::new(SmppError::ESME_RALYBND, 0x1234);
        assert_eq!(generic_nack.header.command_length, 16);
        assert_eq!(generic_nack.header.command_id, CommandId::generic_nack as u32);
        assert_eq!(generic_nack.header.command_status, SmppError::ESME_RALYBND as u32);
        assert_eq!(generic_nack.header.sequence_number, 0x1234);
    }

    #[test]
    fn encode_generic_nack() {
        let generic_nack = generic_nack::new(SmppError::ESME_RALYBND, 0x12344567);
        let pdu = generic_nack.encode();
        assert_eq!(pdu, vec![0x00, 0x00, 0x00, 0x10, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x34, 0x45, 0x67]);
    }

    #[test]
    fn decode_generic_nack() {
        let pdu = vec![0x00, 0x00, 0x00, 0x10, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x34, 0x45, 0x67];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        let decoded = generic_nack::decode(decoded_command_header, &pdu).expect("Unable to decode generic_nack");

        assert_eq!(decoded.header.command_length, 16);
        assert_eq!(decoded.header.command_id, CommandId::generic_nack as u32);
        assert_eq!(decoded.header.command_status, SmppError::ESME_RALYBND as u32);
        assert_eq!(decoded.header.sequence_number, 0x12344567);
    }

    #[test]
    fn decode_generic_nack_with_body() {
        let pdu = vec![0x00, 0x00, 0x00, 0x14, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x34, 0x45, 0x67, 0xde, 0xad, 0xbe, 0xef];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        let decoded = generic_nack::decode(decoded_command_header, &pdu).unwrap_err();
        assert_eq!(decoded, SmppError::ESME_ROPTPARNOTALLWD)
    }

    #[test]
    fn decode_generic_nack_with_invalid_command_id() {
        let pdu = vec![0x00, 0x00, 0x00, 0x10, 0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x34, 0x45, 0x67];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        let result = generic_nack::decode(decoded_command_header, &pdu);
        assert!(result.is_err());
    }
}