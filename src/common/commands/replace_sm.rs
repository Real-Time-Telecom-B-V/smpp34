use crate::common::parse_c_octet_string_nom;
use crate::{CommandHeader, CommandId, SmppError, SmppReply};
use nom::bytes::complete::take;
use num_traits::FromPrimitive;

#[derive(Debug, Clone)]
pub struct replace_sm {
    pub header: CommandHeader,
    pub message_id: String,
    pub source_addr_ton: u8,
    pub source_addr_npi: u8,
    pub source_addr: String,
    pub schedule_delivery_time: String,
    pub validity_period: String,
    pub registered_delivery: u8,
    pub sm_default_msg_id: u8,
    pub sm_length: u8,
    pub short_message: Vec<u8>,
}

impl replace_sm {
    pub fn new(
        sequence_number: u32,
        message_id: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        schedule_delivery_time: String,
        validity_period: String,
        registered_delivery: u8,
        sm_default_msg_id: u8,
        short_message: Vec<u8>,
    ) -> replace_sm {
        assert!(
            short_message.len() <= 254,
            "Message can only be a maximum of 254 characters"
        );
        let cmd_len = (16
            + message_id.len()
            + 1
            + 1
            + 1
            + source_addr.len()
            + 1
            + schedule_delivery_time.len()
            + 1
            + validity_period.len()
            + 1
            + 1
            + 1
            + 1
            + short_message.len()) as u32;
        replace_sm {
            header: CommandHeader {
                command_length: cmd_len,
                command_id: CommandId::replace_sm as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number,
            },
            message_id,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            schedule_delivery_time,
            validity_period,
            registered_delivery,
            sm_default_msg_id,
            sm_length: short_message.len() as u8,
            short_message,
        }
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<replace_sm, SmppError> {
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
        let (input, source_addr) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, schedule_delivery_time) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, validity_period) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, registered_delivery_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, sm_default_msg_id_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, sm_length_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let sm_length = sm_length_bytes[0];
        let (_input, short_message) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(sm_length as usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;

        Ok(replace_sm {
            header,
            message_id,
            source_addr_ton: source_addr_ton_bytes[0],
            source_addr_npi: source_addr_npi_bytes[0],
            source_addr,
            schedule_delivery_time,
            validity_period,
            registered_delivery: registered_delivery_bytes[0],
            sm_default_msg_id: sm_default_msg_id_bytes[0],
            sm_length,
            short_message: short_message.to_vec(),
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
        buffer.extend_from_slice(self.schedule_delivery_time.as_bytes());
        buffer.push(0x00);
        buffer.extend_from_slice(self.validity_period.as_bytes());
        buffer.push(0x00);
        buffer.push(self.registered_delivery);
        buffer.push(self.sm_default_msg_id);
        buffer.push(self.sm_length);
        buffer.extend_from_slice(&self.short_message);
        buffer
    }

    pub fn accept(self) -> replace_sm_resp {
        replace_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::replace_sm_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            },
        }
    }

    pub fn reject(self, error: SmppError) -> replace_sm_resp {
        replace_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::replace_sm_resp as u32,
                command_status: error as u32,
                sequence_number: self.header.sequence_number,
            },
        }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> replace_sm_resp {
        replace_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::replace_sm_resp as u32,
                command_status: error as u32,
                sequence_number,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct replace_sm_resp {
    pub header: CommandHeader,
}

impl replace_sm_resp {
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

    pub fn decode(header: CommandHeader, _pdu: &Vec<u8>) -> Result<replace_sm_resp, SmppError> {
        Ok(replace_sm_resp { header })
    }

    pub fn encode(self) -> Vec<u8> {
        self.header.encode()
    }
}

impl SmppReply for replace_sm_resp {}
