use nom::{
    number::complete::be_u8,
    IResult,
};
use num_traits::FromPrimitive;

use crate::{common::parse_c_octet_string_nom, CommandHeader, CommandId, SmppError, SmppReply};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct cancel_sm {
    header: CommandHeader,
    service_type: String,
    message_id: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    source_addr: String,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    destination_addr: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct cancel_sm_resp {
    header: CommandHeader
}

impl SmppReply for cancel_sm_resp {}

// Function to parse cancel_sm PDU
fn parse_cancel_sm(header: CommandHeader, pdu: &[u8]) -> IResult<&[u8], cancel_sm> {
    
    let (pdu, service_type) = parse_c_octet_string_nom(pdu)?;
    let (pdu, message_id) = parse_c_octet_string_nom(pdu)?;
    
    let (pdu, source_addr_ton) = be_u8(pdu)?;
    let (pdu, source_addr_npi) = be_u8(pdu)?;
    let (pdu, source_addr) = parse_c_octet_string_nom(pdu)?;
    
    let (pdu, dest_addr_ton) = be_u8(pdu)?;
    let (pdu, dest_addr_npi) = be_u8(pdu)?;
    let (pdu, destination_addr) = parse_c_octet_string_nom(pdu)?;

    Ok((pdu, cancel_sm {
        header,
        service_type,
        message_id,
        source_addr_ton,
        source_addr_npi,
        source_addr,
        dest_addr_ton,
        dest_addr_npi,
        destination_addr,
    }))
}

fn parse_cancel_sm_resp(header: CommandHeader, pdu: &[u8]) -> IResult<&[u8], cancel_sm_resp> {
    Ok((pdu, cancel_sm_resp { header }))
}

impl cancel_sm {

    pub (crate) fn new(sequence_number: u32, service_type: String, message_id: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String) -> cancel_sm {
        assert!(service_type.len() < 6, "service_type can be a maximum of 6 octets including NULL terminator for C-Octet-String");
        assert!(source_addr.len() < 21, "source_addr can be a maximum of 21 octets including NULL terminator for C-Octet-String");
        assert!(destination_addr.len() < 21, "destination_addr can be a maximum of 21 octets including NULL terminator for C-Octet-String");

        cancel_sm {
            header: CommandHeader {
                command_length: (16 + service_type.len() + 1 + 2 + source_addr.len() + 1 + 2 + destination_addr.len() + 1) as u32,
                command_id: CommandId::cancel_sm as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number
            },
            service_type,
            message_id,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
        }
    }
    
    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<cancel_sm, SmppError> {
        match parse_cancel_sm(header, pdu) {
            Ok((_, cancel_sm)) => Ok(cancel_sm),
            Err(_) => Err(SmppError::ESME_RINVPARLEN),
        }
    }

    /*
     * Function to encode cancel_sm PDU
     * 
     * Returns a Vec<u8> buffer with the encoded PDU
     * 
     * # Example
     * ```
     * let cancel_sm = cancel_sm::new(0x12345678, "test".to_string(), "MessageID".to_string(), 0x01, 0x01, "source".to_string(), 0x01, 0x01, "dest_addr".to_string());
     * let encoded = cancel_sm.encode();
     * ```
     */
    pub fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.header.command_status as usize);
        buffer.append(&mut self.header.encode());
        buffer.append(&mut self.service_type.into_bytes());
        buffer.push(0x00); // service_type is a C-Octet-String so terminate with 0x00
        buffer.append(&mut self.message_id.into_bytes());
        buffer.push(0x00); // message_id is a C-Octet-String so terminate with 0x00
        buffer.push(self.source_addr_ton);
        buffer.push(self.source_addr_npi);
        buffer.append(&mut self.source_addr.into_bytes());
        buffer.push(0x00); // source_addr is a C-Octet-String so terminate with 0x00
        buffer.push(self.dest_addr_ton);
        buffer.push(self.dest_addr_npi);
        buffer.append(&mut self.destination_addr.into_bytes());
        buffer.push(0x00); // destination_addr is a C-Octet-String so terminate with 0x00
        buffer

    }   

    pub fn accept(self) -> cancel_sm_resp {
        cancel_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::cancel_sm_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            }
        }
    }

    pub fn reject(self, error: SmppError) -> cancel_sm_resp {
        cancel_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::cancel_sm_resp as u32,
                command_status: error as u32,
                sequence_number: self.header.sequence_number,
            }
        }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> cancel_sm_resp {
        cancel_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::cancel_sm_resp as u32,
                command_status: error as u32,
                sequence_number,
            }
        }
    }
    
}

impl cancel_sm_resp {

    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    pub fn command_status(&self) -> u32 { self.header.command_status }
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<cancel_sm_resp, SmppError> {
        match parse_cancel_sm_resp(header, pdu) {
            Ok((_, cancel_sm_resp)) => Ok(cancel_sm_resp),
            Err(_) => Err(SmppError::ESME_RINVPARLEN),
        }
    }

    pub fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.header.command_status as usize);
        buffer.append(&mut self.header.encode());
        buffer
    }
}

#[cfg(test)]
mod all_cancel_sm_tests {
    #[cfg(test)]
    mod cancel_sm_tests {
        use crate::{cancel_sm, CommandHeader, CommandId, SmppError};
    
        #[test]
        fn decode_cancel_sm() {
            let pdu = vec![0x00, 0x00, 0x00, 0x34, // command_length
            0x00, 0x00, 0x00, 0x08, // command_id (cancel_sm)
            0x00, 0x00, 0x00, 0x00, // command_status
            0x12, 0x34, 0x45, 0x67, // sequence_number
            b't', b'e', b's', b't', 0x00, // service_type
            b'M', b'e', b's', b's', b'a', b'g', b'e', b'I', b'D', 0x00, // message_id
            0x01, // source_addr_ton
            0x01, // source_addr_npi
            b's', b'o', b'u', b'r', b'c', b'e', 0x00, // source_addr
            0x01, // dest_addr_ton
            0x01, // dest_addr_npi
            b'd', b'e', b's', b't', b'_', b'a', b'd', b'd', b'r', 0x00 // destination_addr
            ];


            let decoded_command_header = CommandHeader::decode(&pdu).expect("Can not decode command header");
            let decoded = cancel_sm::decode(decoded_command_header, &pdu[16..]).expect("Unable to decode unbind");
    
            assert_eq!(decoded.header.command_length, 52);
            assert_eq!(decoded.header.command_id, CommandId::cancel_sm as u32);
            assert_eq!(decoded.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(decoded.header.sequence_number, 0x12344567);

            assert_eq!(decoded.service_type, "test");
            assert_eq!(decoded.message_id, "MessageID");
            assert_eq!(decoded.source_addr_ton, 0x01);
            assert_eq!(decoded.source_addr_npi, 0x01);
            assert_eq!(decoded.source_addr, "source");
            assert_eq!(decoded.dest_addr_ton, 0x01);
            assert_eq!(decoded.dest_addr_npi, 0x01);
            assert_eq!(decoded.destination_addr, "dest_addr");

            
        }
    
        #[test]
        fn encode_cancel_sm() {
            let cancel_sm = cancel_sm::new(
                0x12345678,
                "test".to_string(),
                "MessageID".to_string(),
                0x01,
                0x01,
                "source".to_string(),
                0x01,
                0x01,
                "dest_addr".to_string(),
            );
    
            let encoded = cancel_sm.encode();
    
            let expected_pdu = vec![
                0x00, 0x00, 0x00, 0x2A, // command_length
                0x00, 0x00, 0x00, 0x08, // command_id (cancel_sm)
                0x00, 0x00, 0x00, 0x00, // command_status
                0x12, 0x34, 0x56, 0x78, // sequence_number
                b't', b'e', b's', b't', 0x00, // service_type
                b'M', b'e', b's', b's', b'a', b'g', b'e', b'I', b'D', 0x00, // message_id
                0x01, // source_addr_ton
                0x01, // source_addr_npi
                b's', b'o', b'u', b'r', b'c', b'e', 0x00, // source_addr
                0x01, // dest_addr_ton
                0x01, // dest_addr_npi
                b'd', b'e', b's', b't', b'_', b'a', b'd', b'd', b'r', 0x00 // destination_addr
            ];
    
            assert_eq!(encoded, expected_pdu);
        }
        
    
    }
    
    #[cfg(test)]
    mod cancel_sm_resp_tests {
    
        #[cfg(test)]
        mod cancel_sm_resp_tests {
            use crate::{cancel_sm, SmppError};


            #[test]
            fn encode_cancel_sm_resp() {
                let cancel_sm = cancel_sm::new(
                    0x12345678,
                    "test".to_string(),
                    "MessageID".to_string(),
                    0x01,
                    0x01,
                    "source".to_string(),
                    0x01,
                    0x01,
                    "dest_addr".to_string(),
                );

                let cancel_sm_resp = cancel_sm.accept();
                let encoded = cancel_sm_resp.encode();

                let expected_pdu = vec![
                    0x00, 0x00, 0x00, 0x10, // command_length
                    0x80, 0x00, 0x00, 0x08, // command_id (cancel_sm_resp)
                    0x00, 0x00, 0x00, 0x00, // command_status
                    0x12, 0x34, 0x56, 0x78, // sequence_number
                ];

                assert_eq!(encoded, expected_pdu);
            }

            #[test]
            fn encode_cancel_sm_resp_reject() {
                let cancel_sm = cancel_sm::new(
                    0x12345678,
                    "test".to_string(),
                    "MessageID".to_string(),
                    0x01,
                    0x01,
                    "source".to_string(),
                    0x01,
                    0x01,
                    "dest_addr".to_string(),
                );

                let cancel_sm_resp_rejected = cancel_sm.reject(SmppError::ESME_RINVPARLEN);
                let encoded_rejected = cancel_sm_resp_rejected.encode();

                let expected_pdu_rejected = vec![
                    0x00, 0x00, 0x00, 0x10, // command_length
                    0x80, 0x00, 0x00, 0x08, // command_id (cancel_sm_resp)
                    0x00, 0x00, 0x00, 0xC2, // command_status (ESME_RINVPARLEN)
                    0x12, 0x34, 0x56, 0x78, // sequence_number
                ];

                assert_eq!(encoded_rejected, expected_pdu_rejected);
            }
        }
    }
}
