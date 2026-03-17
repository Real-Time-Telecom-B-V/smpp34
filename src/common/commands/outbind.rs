use crate::{CommandHeader, SmppError, CommandId, common::parse_c_octet_string};

/// The purpose of the outbind operation is to allow the SMSC signal an ESME to originate a
/// bind_receiver request to the SMSC. An example of where such a facility might be applicable
/// would be where the SMSC had outstanding messages for delivery to the ESME.
/// An outbind SMPP session between an SMSC and an ESME can be initiated by the SMSC first
/// establishing a network connection with the ESME.
/// Once a network connection has been established, the SMSC should bind to the ESME by
/// issuing an “outbind” request. The ESME should respond with a “bind_receiver” request to
/// which the SMSC will reply with a “bind_receiver_resp”.
/// If the ESME does not accept the outbind session (e.g. because of an illegal system_id or
/// password etc.) the ESME should disconnect the network connection.
/// Once the SMPP session is established the characteristics of the session are that of a normal
/// SMPP receiver session.
/// See '4.1.7 “OUTBIND” Operation' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct outbind {
    header: CommandHeader,
    system_id: String,
    password: String
}

impl outbind {

    /// Construct a outbind primitive
    /// 
    /// # Arguments
    /// 
    /// * `seq_no` - sequence number 
    /// * `system_id` - system_id to set in the outbind (may be empty)
    /// * `password` - password to set in the outbind (may be empty)
    pub fn new(seq_no: u32, system_id: String, password: String) -> outbind {
        outbind { header: CommandHeader { command_length: (16 + system_id.len() + password.len() + 2) as u32, command_id: CommandId::outbind as u32, command_status: SmppError::ESME_ROK as u32, sequence_number: seq_no }, system_id, password }
    }

    /// Decode a outbind PDU
    /// 
    /// # Arguments
    /// 
    /// * `header` - already decoded Command Header which is only used for validation as outbind should not have a body
    /// * `pdu` - the complete PDU used for extra validation
    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<outbind, SmppError> {
        if header.command_id == CommandId::outbind as u32 {
            if pdu.len() >= 18 { // We expect a body of 2 C-Octet-Strings which may be empty
                // CommondHeader decode method makes sure that PDU length matches the command_length so no need to check this again
    
                // First we expect the system_id which is a C-Octet-String terminated by 0 and maximum 16 in length
                let system_id = parse_c_octet_string(pdu[16..].to_vec(), 16)?;

                // Then we expect the password which is a C-Octet-String terminated by 0 and maximum 9 in length
                let password = parse_c_octet_string(pdu[16 + system_id.len() + 1..].to_vec(), 9)?;

                Ok(outbind {
                    header, system_id, password
                })
            } else {
                Err(SmppError::ESME_RINVPARLEN) // outbind should have command header + a minimum of 2 NULL bytes as system_id and password are C-Octet-String which may be empty to provide passwordless authentication
            }
        }
        else {
            return Err(SmppError::ESME_RINVCMDID)
        }
    }

    /// Encode a outbind based on inner command header
    pub fn encode(self) -> Vec<u8> {
        if self.system_id.len() > 16 {
            panic!("system_id can only be 16 digits long")
        } else if self.password.len() > 9 {
            panic!("password can only be 9 digits long")
        } else {
            let mut buffer: Vec<u8> = Vec::with_capacity(16 + self.system_id.len() + self.password.len() + 2); // Length of two C-Octet-Strings including NULL terminators
            buffer.append(&mut self.header.encode());
            buffer.append(&mut self.system_id.as_bytes().to_vec());
            buffer.push(0x00);
            buffer.append(&mut self.password.as_bytes().to_vec());
            buffer.push(0x00);
            buffer
        }
    }
}

#[cfg(test)]
mod outbind_tests {
    use crate::{outbind, CommandId, SmppError, CommandHeader};

    #[test]
    fn new_outbind() {
        let outbind = outbind::new(0x1234, "abc".to_owned(), "123".to_owned());
        assert_eq!(outbind.header.command_length, 24);
        assert_eq!(outbind.header.command_id, CommandId::outbind as u32);
        assert_eq!(outbind.header.command_status, SmppError::ESME_ROK as u32);
        assert_eq!(outbind.header.sequence_number, 0x1234);
        assert_eq!(outbind.system_id, "abc");
        assert_eq!(outbind.password, "123");
    }

    #[test]
    fn encode_outbind() {
        let outbind = outbind::new(0x1234, "abc".to_owned(), "123".to_owned());
        let pdu = outbind.encode();
        assert_eq!(pdu, vec![0x00, 0x0, 0x00, 0x18, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x61, 0x62, 0x63, 0x00, 0x31, 0x32, 0x33, 0x00]);
    }

    #[test]
    fn decode_outbind() {
        let pdu = vec![0x00, 0x0, 0x00, 0x18, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x61, 0x62, 0x63, 0x00, 0x31, 0x32, 0x33, 0x00];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        let decoded = outbind::decode(decoded_command_header, &pdu).expect("Unable to decode outbind");

        assert_eq!(decoded.header.command_length, 24);
        assert_eq!(decoded.header.command_id, CommandId::outbind as u32);
        assert_eq!(decoded.header.command_status, SmppError::ESME_ROK as u32);
        assert_eq!(decoded.header.sequence_number, 0x1234);
        assert_eq!(decoded.system_id, "abc");
        assert_eq!(decoded.password, "123");
    }

    #[test]
    fn decode_outbind_empty_system_id_and_password() {
        let pdu = vec![0x00, 0x0, 0x00, 0x12, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x00, 0x00];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        let decoded = outbind::decode(decoded_command_header, &pdu).expect("Unable to decode outbind");

        assert_eq!(decoded.header.command_length, 18);
        assert_eq!(decoded.header.command_id, CommandId::outbind as u32);
        assert_eq!(decoded.header.command_status, SmppError::ESME_ROK as u32);
        assert_eq!(decoded.header.sequence_number, 0x1234);
        assert_eq!(decoded.system_id, "");
        assert_eq!(decoded.password, "");
    }

    #[test]
    fn decode_outbind_without_body() {
        let pdu = vec![0x00, 0x0, 0x00, 0x10, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        let decoded = outbind::decode(decoded_command_header, &pdu).unwrap_err();
        assert_eq!(decoded, SmppError::ESME_RINVPARLEN)
    }

    #[test]
    fn decode_outbind_with_invalid_command_id() {
        let pdu = vec![0x00, 0x0, 0x00, 0x18, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x61, 0x62, 0x63, 0x00, 0x31, 0x32, 0x33, 0x00];
        let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
        let result = outbind::decode(decoded_command_header, &pdu);
        assert!(result.is_err());
    }
}