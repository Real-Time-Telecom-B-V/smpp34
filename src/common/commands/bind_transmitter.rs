use num_traits::FromPrimitive;

use crate::{CommandHeader, SmppError, common::{decode_bind_request, encode_bind_response}, CommandId};

#[derive(Debug, Clone)]
pub struct bind_transmitter {
    header: CommandHeader,
    pub system_id: String,
    pub password: String,
    pub system_type: String,
    pub interface_version: u8,
    pub addr_ton: u8,
    pub addr_npi: u8,
    pub address_range: String
}

impl bind_transmitter {

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<bind_transmitter, SmppError> {
        let result = decode_bind_request(header, pdu)?;
        Ok(bind_transmitter { header: result.header, system_id: result.system_id, password: result.password, system_type: result.system_type, interface_version: result.interface_version, addr_ton: result.addr_ton, addr_npi: result.addr_npi, address_range: result.address_range })
    }

    pub fn encode(self) -> Vec<u8> {
        todo!()
    }

    pub fn accept(self, system_id: String, sc_interface_version: Option<u8>) -> bind_transmitter_resp {
        bind_transmitter_resp { 
            header: CommandHeader {
                command_length: 16 + system_id.len() as u32 + 1 + if sc_interface_version.is_some() { 5 } else { 0 }, // sc_interface_version is a TLV of 5 bytes
                command_id: CommandId::bind_transmitter_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
        }, system_id: Some(system_id), sc_interface_version }
    }

    pub fn reject(self, error: SmppError) -> bind_transmitter_resp {
        bind_transmitter_resp { header: CommandHeader {
            command_length: 16, 
            command_id: CommandId::bind_transmitter_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        }, system_id: None, sc_interface_version: None }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> bind_transmitter_resp {
        bind_transmitter_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::bind_transmitter_resp as u32,
            command_status: error as u32,
            sequence_number,
        }, system_id: None, sc_interface_version: None }
    }
    
}

#[derive(Debug, Clone)]
pub struct bind_transmitter_resp {
    header: CommandHeader,
    pub system_id: Option<String>,
    pub sc_interface_version: Option<u8>
}

impl bind_transmitter_resp {

    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    pub fn command_status(&self) -> u32 { self.header.command_status }
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    pub fn decode(_header: CommandHeader, _pdu: &Vec<u8>) -> Result<bind_transmitter_resp, SmppError> {
        todo!()
    }

    pub fn encode(self) -> Vec<u8> { encode_bind_response(self.header, self.system_id, self.sc_interface_version) }

}
