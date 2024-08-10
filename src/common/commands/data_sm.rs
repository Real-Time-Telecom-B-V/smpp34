use log::warn;
use num_traits::FromPrimitive;

use crate::{CommandHeader, common::{parse_c_octet_string, parse_next_int}, SmppError, CommandId};

#[derive(Debug, Clone)]
pub struct data_sm  {
    header: CommandHeader,
    /// The service_type parameter can be used to indicate the SMS Application service associated with the message.
    /// Specifying the service_type allows the ESME to
    /// • avail of enhanced messaging services such as “replace by service” type
    /// • to control the teleservice used on the air interface.
    /// Set to NULL for default SMSC settings.
    pub service_type: String,
    pub source_addr_ton: u8,
    pub source_addr_npi: u8,
    pub source_addr: String,
    pub dest_addr_ton: u8,
    pub dest_addr_npi: u8,
    pub destination_addr: String,
    pub esm_class: u8,
    pub registered_delivery: u8,
    pub data_coding: u8,
}

impl data_sm {

    // TODO optional parameters
    pub (crate) fn new(sequence_number: u32, service_type: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, registered_delivery: u8, data_coding: u8) -> data_sm {
        data_sm { 
            header: CommandHeader { 
                command_length: (16 + service_type.len() + 1 + 2 + source_addr.len() + 1 + 2 + destination_addr.len() + 3) as u32,  
                command_id: CommandId::data_sm as u32, 
                command_status: SmppError::ESME_ROK as u32, 
                sequence_number }, 
            service_type, 
            source_addr_ton, 
            source_addr_npi, 
            source_addr, 
            dest_addr_ton, 
            dest_addr_npi, 
            destination_addr, 
            esm_class, 
            registered_delivery, 
            data_coding
        }
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<data_sm, SmppError> {
        warn!("Decode not fully implemented yet, optional parameters not available");
    
        let service_type = parse_c_octet_string(pdu[16..].to_vec(), 6)?;

        let start = 16 + service_type.len();
        let source_addr_ton =  parse_next_int(pdu, start + 1)?;
        let source_addr_npi =  parse_next_int(pdu, start + 2)?;
        let source_addr = parse_c_octet_string(pdu[start + 3..].to_vec(), 21)?;

        let start = start + 2 + source_addr.len() + 1;
        let dest_addr_ton =  parse_next_int(pdu, start + 1)?;
        let dest_addr_npi =  parse_next_int(pdu, start + 2)?;
        let destination_addr = parse_c_octet_string(pdu[start + 3..].to_vec(), 21)?;

        let start = start + 2 + destination_addr.len() + 1;
        let esm_class = parse_next_int(pdu, start + 1)?;
        let registered_delivery = parse_next_int(pdu, start + 2)?;
        let data_coding = parse_next_int(pdu, start + 3)?;

        
        Ok(data_sm {
            header,
            service_type,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
            esm_class,
            registered_delivery,
            data_coding,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        todo!()
    }

    pub fn accept(self, message_id: String) -> data_sm_resp {
        if message_id.len() > 65 {
            panic!("message_id has a maximum length of 65 characters")
        }

        data_sm_resp { header: CommandHeader {
            command_length: 16 + message_id.len() as u32 + 1, // message_id is a C-Octet-String
            command_id: CommandId::data_sm_resp as u32,
            command_status: SmppError::ESME_ROK as u32,
            sequence_number: self.header.sequence_number,
        }, message_id: Some(message_id) }
    }

    pub fn reject(self, error: SmppError) -> data_sm_resp {
        data_sm_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::data_sm_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        }, message_id: None }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> data_sm_resp {
        data_sm_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::data_sm_resp as u32,
            command_status: error as u32,
            sequence_number,
        }, message_id: None }
    }
}

#[derive(Debug, Clone)]
pub struct data_sm_resp {
    header: CommandHeader,
    /// This field is unused and is set to NULL
    message_id: Option<String>
}

impl data_sm_resp {

    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    pub fn command_status(&self) -> u32 { self.header.command_status }
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    pub fn encode(self) -> Vec<u8> { 
        let mut buffer:Vec<u8> = Vec::with_capacity(self.header.command_length.try_into().unwrap());
        buffer.append(&mut self.header.encode());

        if let Some(message_id) = self.message_id {
            buffer.append(&mut message_id.as_bytes().to_vec());
            buffer.push(0x00); // Terminate C-Octet-String
        }

        buffer
     }

     pub fn decode(_header: CommandHeader, _pdu: &Vec<u8>) -> Result<data_sm_resp, SmppError> {
        todo!()
     }
}