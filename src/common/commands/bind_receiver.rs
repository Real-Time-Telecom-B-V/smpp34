use num_traits::FromPrimitive;

use crate::common::tlv::{decode_tlvs, TlvList};
use crate::{
    common::{
        decode_bind_request, encode_bind_request, encode_bind_response, parse_c_octet_string_nom,
    },
    CommandHeader, CommandId, SmppError,
};

#[derive(Debug, Clone)]
pub struct bind_receiver {
    header: CommandHeader,
    pub system_id: String,
    pub password: String,
    pub system_type: String,
    pub interface_version: u8,
    pub addr_ton: u8,
    pub addr_npi: u8,
    pub address_range: String,
}

impl bind_receiver {
    pub(crate) fn new(
        sequence_number: u32,
        system_id: String,
        password: String,
        system_type: String,
        addr_ton: u8,
        addr_npi: u8,
        address_range: String,
    ) -> bind_receiver {
        assert!(
            system_id.len() < 16,
            "system_id can be a maximum of 16 octets including NULL terminator for C-Octet-String"
        );
        assert!(
            password.len() < 9,
            "password can be a maximum of 9 octets including NULL terminator for C-Octet-String"
        );
        assert!(system_type.len() < 13, "system_type can be a maximum of 13 octets including NULL terminator for C-Octet-String");
        assert!(address_range.len() < 41, "address_range can be a maximum of 41 octets including NULL terminator for C-Octet-String");

        bind_receiver {
            header: CommandHeader {
                command_length: (16
                    + system_id.len()
                    + 1
                    + password.len()
                    + 1
                    + system_type.len()
                    + 1
                    + 3
                    + address_range.len()
                    + 1) as u32,
                command_id: CommandId::bind_receiver as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number,
            },
            system_id,
            password,
            system_type,
            interface_version: 0x34,
            addr_ton,
            addr_npi,
            address_range,
        }
    }

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<bind_receiver, SmppError> {
        let result = decode_bind_request(header, pdu)?;
        Ok(bind_receiver {
            header: result.header,
            system_id: result.system_id,
            password: result.password,
            system_type: result.system_type,
            interface_version: result.interface_version,
            addr_ton: result.addr_ton,
            addr_npi: result.addr_npi,
            address_range: result.address_range,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        encode_bind_request(
            self.header,
            self.system_id,
            self.password,
            self.system_type,
            self.interface_version,
            self.addr_ton,
            self.addr_npi,
            self.address_range,
        )
    }

    pub fn accept(self, system_id: String, sc_interface_version: Option<u8>) -> bind_receiver_resp {
        bind_receiver_resp {
            header: CommandHeader {
                command_length: 16
                    + system_id.len() as u32
                    + 1
                    + if sc_interface_version.is_some() { 5 } else { 0 }, // sc_interface_version is a TLV of 5 bytes
                command_id: CommandId::bind_receiver_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            },
            system_id: Some(system_id),
            sc_interface_version,
        }
    }

    pub fn reject(self, error: SmppError) -> bind_receiver_resp {
        bind_receiver_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::bind_receiver_resp as u32,
                command_status: error as u32,
                sequence_number: self.header.sequence_number,
            },
            system_id: None,
            sc_interface_version: None,
        }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> bind_receiver_resp {
        bind_receiver_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::bind_receiver_resp as u32,
                command_status: error as u32,
                sequence_number,
            },
            system_id: None,
            sc_interface_version: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct bind_receiver_resp {
    header: CommandHeader,
    pub system_id: Option<String>,
    pub sc_interface_version: Option<u8>,
}

impl bind_receiver_resp {
    pub fn is_success(&self) -> bool {
        self.header.command_status == SmppError::ESME_ROK as u32
    }
    pub fn command_status(&self) -> u32 {
        self.header.command_status
    }
    pub fn get_error(&self) -> SmppError {
        FromPrimitive::from_u32(self.header.command_status)
            .expect("Can not convert command_status to SmppError")
    }

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<bind_receiver_resp, SmppError> {
        if header.command_status != SmppError::ESME_ROK as u32 {
            return Ok(bind_receiver_resp {
                header,
                system_id: None,
                sc_interface_version: None,
            });
        }
        if pdu.len() <= 16 {
            return Err(SmppError::ESME_RINVPARLEN);
        }
        let input = &pdu[16..];
        let (input, system_id) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let sc_interface_version = if !input.is_empty() {
            let tlvs = decode_tlvs(input);
            tlvs.sc_interface_version()
        } else {
            None
        };
        Ok(bind_receiver_resp {
            header,
            system_id: Some(system_id),
            sc_interface_version,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        encode_bind_response(self.header, self.system_id, self.sc_interface_version)
    }
}
