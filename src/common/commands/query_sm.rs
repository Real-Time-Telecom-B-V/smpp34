use crate::common::parse_c_octet_string_nom;
use crate::{CommandHeader, CommandId, SmppError, SmppReply};
use nom::bytes::complete::take;
use num_traits::FromPrimitive;

#[derive(Debug, Clone)]
pub struct query_sm {
    pub header: CommandHeader,
    pub message_id: String,
    pub source_addr_ton: u8,
    pub source_addr_npi: u8,
    pub source_addr: String,
}

impl query_sm {
    pub fn new(
        sequence_number: u32,
        message_id: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
    ) -> query_sm {
        let cmd_len = (16 + message_id.len() + 1 + 1 + 1 + source_addr.len() + 1) as u32;
        query_sm {
            header: CommandHeader {
                command_length: cmd_len,
                command_id: CommandId::query_sm as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number,
            },
            message_id,
            source_addr_ton,
            source_addr_npi,
            source_addr,
        }
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<query_sm, SmppError> {
        if pdu.len() < 16 {
            return Err(SmppError::ESME_RINVCMDLEN);
        }
        let input = &pdu[16..];
        let (input, message_id) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, source_addr_ton_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, source_addr_npi_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (_input, source_addr) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;

        Ok(query_sm {
            header,
            message_id,
            source_addr_ton: source_addr_ton_bytes[0],
            source_addr_npi: source_addr_npi_bytes[0],
            source_addr,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.header.command_length as usize);
        buffer.extend_from_slice(&self.header.encode());
        buffer.extend_from_slice(self.message_id.as_bytes());
        buffer.push(0x00);
        buffer.push(self.source_addr_ton);
        buffer.push(self.source_addr_npi);
        buffer.extend_from_slice(self.source_addr.as_bytes());
        buffer.push(0x00);
        buffer
    }

    pub fn accept(
        self,
        message_id: String,
        final_date: String,
        message_state: u8,
        error_code: u8,
    ) -> query_sm_resp {
        let cmd_len = (16 + message_id.len() + 1 + final_date.len() + 1 + 1 + 1) as u32;
        query_sm_resp {
            header: CommandHeader {
                command_length: cmd_len,
                command_id: CommandId::query_sm_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id,
            final_date,
            message_state,
            error_code,
        }
    }

    pub fn reject(self, error: SmppError) -> query_sm_resp {
        query_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::query_sm_resp as u32,
                command_status: error as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: String::new(),
            final_date: String::new(),
            message_state: 0,
            error_code: 0,
        }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> query_sm_resp {
        query_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::query_sm_resp as u32,
                command_status: error as u32,
                sequence_number,
            },
            message_id: String::new(),
            final_date: String::new(),
            message_state: 0,
            error_code: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct query_sm_resp {
    pub header: CommandHeader,
    pub message_id: String,
    pub final_date: String,
    pub message_state: u8,
    pub error_code: u8,
}

impl query_sm_resp {
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

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<query_sm_resp, SmppError> {
        if header.command_status != SmppError::ESME_ROK as u32 {
            return Ok(query_sm_resp {
                header,
                message_id: String::new(),
                final_date: String::new(),
                message_state: 0,
                error_code: 0,
            });
        }
        if pdu.len() <= 16 {
            return Err(SmppError::ESME_RINVPARLEN);
        }
        let input = &pdu[16..];
        let (input, message_id) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, final_date) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, message_state_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (_input, error_code_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;

        Ok(query_sm_resp {
            header,
            message_id,
            final_date,
            message_state: message_state_bytes[0],
            error_code: error_code_bytes[0],
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let is_ok = self.header.command_status == SmppError::ESME_ROK as u32;
        let mut buffer: Vec<u8> = Vec::with_capacity(self.header.command_length as usize);
        buffer.extend_from_slice(&self.header.encode());
        if is_ok {
            buffer.extend_from_slice(self.message_id.as_bytes());
            buffer.push(0x00);
            buffer.extend_from_slice(self.final_date.as_bytes());
            buffer.push(0x00);
            buffer.push(self.message_state);
            buffer.push(self.error_code);
        }
        buffer
    }
}

impl SmppReply for query_sm_resp {}
