use crate::{CommandHeader, CommandId, SmppError};

pub struct alert_notification {
    header: CommandHeader,
    source_addr_ton: u8, 
    source_addr_npi: u8, 
    source_addr: String, 
    esme_addr_ton: u8, 
    esme_addr_npi: u8, 
    esme_addr: String,
    ms_availability_status: Option<u8>,
}

impl alert_notification {
    pub(crate) fn new(sequence_number: u32, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, esme_addr_ton: u8, esme_addr_npi: u8, esme_addr: String, ms_availability_status: Option<u8>) -> alert_notification {

        assert!(source_addr.len() <= 65, "source_addr can be a maximum of 65 characters");
        assert!(esme_addr.len() <= 65, "esme_addr can be a maximum of 65 characters");

        alert_notification { 
            header: CommandHeader { 
                command_length: (16 + 2 + source_addr.len() + 1 + 2 + esme_addr.len() + 1 + if ms_availability_status.is_some() {5 } else { 0 }) as u32 ,
                command_id: CommandId::alert_notification as u32, 
                command_status: SmppError::ESME_ROK as u32, 
                sequence_number
            },
            source_addr_ton, 
            source_addr_npi, 
            source_addr, 
            esme_addr_ton, 
            esme_addr_npi, 
            esme_addr, 
            ms_availability_status 
        }
    }

    pub fn encode(self) -> Vec<u8> {
        todo!()
    }
}
