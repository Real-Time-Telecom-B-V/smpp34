use log::warn;
use num_traits::FromPrimitive;
use crate::{common::{parse_c_octet_string, parse_next_int, parse_octet_string}, CommandHeader, CommandId, SmppError, SmppReply};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct submit_sm  {
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
    pub short_message: String,
    pub user_message_reference: Option<u16>,
}

impl submit_sm {

    // TODO optional parameters
    pub (crate) fn new(sequence_number: u32, service_type: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, protocol_id: u8, priority_flag: u8, schedule_delivery_time: String, validity_period: String, registered_delivery: u8, replace_if_present_flag: u8, data_coding: u8, sm_default_msg_id: u8, short_message: String) -> submit_sm {
        
        assert!(short_message.len() <= 254, "Message can only be a maximum of 254 characters");

        submit_sm { 
            header: CommandHeader { 
                command_length: (16 + service_type.len() + 1 + 2 + source_addr.len() + 1 + 2 + destination_addr.len() + 1 + 10 + short_message.len()) as u32,  
                command_id: CommandId::submit_sm as u32, 
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

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<submit_sm, SmppError> {
        warn!("Decode of submit_sm not fully implemented yet, optional parameters not available");
    
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
        let short_message = parse_octet_string(pdu[start + 6..].to_vec(), sm_length as usize, 254)?;

        

        Ok(submit_sm {
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

    pub fn encode(self) -> Vec<u8> {


        assert!(self.source_addr.len() <= 21, "source_addr can only be a maximum of 21 characters");
        assert!(self.destination_addr.len() <= 21, "destination_addr can only be a maximum of 21 characters");
        assert!(self.schedule_delivery_time.len() <= 17, "schedule_delivery_time can only be a maximum of 17 characters");
        assert!(self.validity_period.len() <= 17, "validity_period can only be a maximum of 17 characters");
        assert!(self.short_message.len() <= 254, "short_message can only be a maximum of 254 characters");
        assert!(self.service_type.len() <= 6, "service_type can only be a maximum of 6 characters");
        assert!(self.sm_length as usize == self.short_message.len(), "sm_length must be equal to the length of short_message");

        let mut buffer:Vec<u8> = Vec::with_capacity(self.header.command_length.try_into().unwrap());
        buffer.append(&mut self.header.encode());
        buffer.append(&mut self.service_type.as_bytes().to_vec());
        buffer.push(0x00); // Terminate C-Octet-String
        buffer.push(self.source_addr_ton);
        buffer.push(self.source_addr_npi);
        buffer.append(&mut self.source_addr.as_bytes().to_vec());
        buffer.push(0x00); // Terminate C-Octet-String
        buffer.push(self.dest_addr_ton);
        buffer.push(self.dest_addr_npi);
        buffer.append(&mut self.destination_addr.as_bytes().to_vec());
        buffer.push(0x00); // Terminate C-Octet-String
        buffer.push(self.esm_class);
        buffer.push(self.protocol_id);
        buffer.push(self.priority_flag);
        buffer.append(&mut self.schedule_delivery_time.as_bytes().to_vec());
        buffer.push(0x00); // Terminate C-Octet-String
        buffer.append(&mut self.validity_period.as_bytes().to_vec());
        buffer.push(0x00); // Terminate C-Octet-String
        buffer.push(self.registered_delivery);
        buffer.push(self.replace_if_present_flag);
        buffer.push(self.data_coding);
        buffer.push(self.sm_default_msg_id);
        buffer.push(self.sm_length);
        if self.sm_length > 0 {
            buffer.append(&mut self.short_message.as_bytes().to_vec());
        }
        
        if let Some(user_message_reference) = self.user_message_reference {
            let user_message_reference = user_message_reference.to_be_bytes();
            for byte in user_message_reference {
                buffer.push(byte);
            }
        }
        buffer.push(0x00); // Terminate C-Octet-String
        buffer
    }

    pub fn accept(self, message_id: String) -> submit_sm_resp {
        if message_id.len() > 65 {
            panic!("message_id has a maximum length of 65 characters")
        }

        submit_sm_resp { header: CommandHeader {
            command_length: 16 + message_id.len() as u32 + 1, // message_id is a C-Octet-String
            command_id: CommandId::submit_sm_resp as u32,
            command_status: SmppError::ESME_ROK as u32,
            sequence_number: self.header.sequence_number,
        }, message_id: Some(message_id) }
    }

    pub fn reject(self, error: SmppError) -> submit_sm_resp {
        submit_sm_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::submit_sm_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        }, message_id: None }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> submit_sm_resp {
        submit_sm_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::submit_sm_resp as u32,
            command_status: error as u32,
            sequence_number,
        }, message_id: None }
    }
}

#[derive(Debug, Clone)]
pub struct submit_sm_resp {
    pub header: CommandHeader,
    pub message_id: Option<String>
}

impl submit_sm_resp {

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

     pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<submit_sm_resp, SmppError> {
        let message_id = parse_c_octet_string(pdu[16..].to_vec(), 65)?;
        Ok(submit_sm_resp { header, message_id: Some(message_id) })
     }
}

impl SmppReply for submit_sm_resp {}
