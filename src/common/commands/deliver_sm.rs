
use log::warn;
use num_traits::FromPrimitive;

use crate::{common::{parse_c_octet_string, parse_next_int, parse_octet_string_as_vec}, CommandHeader, CommandId, SmppError, SmppReply};

#[derive(Debug, Clone)]
pub struct deliver_sm  {
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
    pub protocol_id: u8,
    pub priority_flag: u8,
    pub schedule_delivery_time: String,
    pub validity_period: String,
    pub registered_delivery: u8,
    pub replace_if_present_flag: u8,
    pub data_coding: u8,
    pub sm_default_msg_id: u8,
    pub sm_length: u8,
    pub short_message: Vec<u8>,
    pub user_message_reference: Option<u16>,
}

impl deliver_sm {

    // TODO optional parameters
    pub fn new(sequence_number: u32, service_type: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, protocol_id: u8, priority_flag: u8, schedule_delivery_time: String, validity_period: String, registered_delivery: u8, replace_if_present_flag: u8, data_coding: u8, sm_default_msg_id: u8, short_message: Vec<u8>) -> deliver_sm {
        
        assert!(short_message.len() <= 254, "Message can only be a maximum of 254 characters");

        deliver_sm { 
            header: CommandHeader { 
                command_length: (16 + service_type.len() + 1 + 2 + source_addr.len() + 1 + 2 + destination_addr.len() + 1 + 10 + short_message.len()) as u32,  
                command_id: CommandId::deliver_sm as u32, 
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
            protocol_id, 
            priority_flag, 
            schedule_delivery_time, 
            validity_period, 
            registered_delivery, 
            replace_if_present_flag, 
            data_coding, 
            sm_default_msg_id, 
            sm_length: short_message.len() as u8, 
            short_message, 
            user_message_reference: None 
        }
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<deliver_sm, SmppError> {
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
        let protocol_id = parse_next_int(pdu, start + 2)?;
        let priority_flag = parse_next_int(pdu, start + 3)?;
        let schedule_delivery_time =  parse_c_octet_string(pdu[start + 4..].to_vec(), 17)?;

        let start = start + 3 + schedule_delivery_time.len() + 1;
        let validity_period = parse_c_octet_string(pdu[start..].to_vec(), 17)?;

        let start = start + validity_period.len() + 1;
        let registered_delivery = parse_next_int(pdu, start + 1)?;
        let replace_if_present_flag = parse_next_int(pdu, start + 2)?;
        let data_coding = parse_next_int(pdu, start + 3)?;
        let sm_default_msg_id = parse_next_int(pdu, start + 4)?;
        let sm_length = parse_next_int(pdu, start + 5)?;
        let short_message = parse_octet_string_as_vec(pdu[start + 6..].to_vec(), sm_length as usize, 254)?;

        

        Ok(deliver_sm {
            header,
            service_type,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
            esm_class,
            protocol_id,
            priority_flag,
            schedule_delivery_time,
            validity_period,
            registered_delivery,
            replace_if_present_flag,
            data_coding,
            sm_default_msg_id,
            sm_length,
            short_message,
            user_message_reference: None
        })
    }

    pub fn encode(mut self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.header.command_status as usize);
        buffer.append(&mut self.header.encode());
        buffer.append(&mut self.service_type.into_bytes());
        buffer.push(0x00); // service_type is a C-Octet-String so terminate with 0x00
        buffer.push(self.source_addr_ton);
        buffer.push(self.source_addr_npi);
        buffer.append(&mut self.source_addr.into_bytes());
        buffer.push(0x00); // source_addr is a C-Octet-String so terminate with 0x00
        buffer.push(self.dest_addr_ton);
        buffer.push(self.dest_addr_npi);
        buffer.append(&mut self.destination_addr.into_bytes());
        buffer.push(0x00); // destination_addr is a C-Octet-String so terminate with 0x00
        buffer.push(self.esm_class);
        buffer.push(self.protocol_id);
        buffer.push(self.priority_flag);
        buffer.append(&mut self.schedule_delivery_time.into_bytes());
        buffer.push(0x00); // schedule_delivery_time is a C-Octet-String so terminate with 0x00
        buffer.append(&mut self.validity_period.into_bytes());
        buffer.push(0x00); // validity_period is a C-Octet-String so terminate with 0x00
        buffer.push(self.registered_delivery);
        buffer.push(self.replace_if_present_flag);
        buffer.push(self.data_coding);
        buffer.push(self.sm_default_msg_id);
        buffer.push(self.sm_length);
        buffer.append(&mut self.short_message);

        // TODO optional parameters
        buffer
    }

    pub fn accept(self) -> deliver_sm_resp {
        deliver_sm_resp { header: CommandHeader {
            command_length: 16 + 1, // message_id is a C-Octet-String (and is always NULL in deliver_sm_resp)
            command_id: CommandId::deliver_sm_resp as u32,
            command_status: SmppError::ESME_ROK as u32,
            sequence_number: self.header.sequence_number,
        }, message_id: "".into() }
    }

    pub fn reject(self, error: SmppError) -> deliver_sm_resp {
        deliver_sm_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::deliver_sm_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        }, message_id: "".into() }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> deliver_sm_resp {
        deliver_sm_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::deliver_sm_resp as u32,
            command_status: error as u32,
            sequence_number,
        }, message_id: "".into() }
    }
}

#[derive(Debug, Clone)]
pub struct deliver_sm_resp {
    header: CommandHeader,
    /// This field is unused and is set to NULL
    message_id: String
}

impl deliver_sm_resp {

    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    pub fn command_status(&self) -> u32 { self.header.command_status }
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    pub fn encode(self) -> Vec<u8> { 
        let mut buffer:Vec<u8> = Vec::with_capacity(self.header.command_length.try_into().unwrap());
        buffer.append(&mut self.header.encode());
        buffer.append(&mut self.message_id.as_bytes().to_vec());
        buffer.push(0x00); // Terminate C-Octet-String

        buffer
     }

     pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<deliver_sm_resp, SmppError> {
        let message_id = parse_c_octet_string(pdu[..].to_vec(), 0)?;
        Ok(deliver_sm_resp { header, message_id })
     }
}

impl SmppReply for deliver_sm_resp {}