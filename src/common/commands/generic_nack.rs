use crate::{CommandHeader, SmppError, CommandId};

#[derive(Debug, Clone)]
pub struct generic_nack {
    pub(crate) header: CommandHeader
}

impl generic_nack {

    pub fn new(error: SmppError, seq_no: u32) -> generic_nack {
        generic_nack { header: CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: seq_no } }
    }

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
            panic!("Passed a non generic_nack to decode()")
        }
    }

    pub fn encode(self) -> Vec<u8> {
        self.header.encode()
    }
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
    #[should_panic(expected = "Passed a non generic_nack to decode()")]
    fn decode_generic_nack_with_invalid_command_id() {
        let pdu = vec![0x00, 0x00, 0x00, 0x10, 0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x34, 0x45, 0x67];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        generic_nack::decode(decoded_command_header, &pdu).expect("Unable to decode generic_nack");
    }
}