use crate::common::parse_c_octet_string_nom;
use crate::{CommandHeader, CommandId, SmppError, SmppReply};
use nom::bytes::complete::take;
use num_traits::FromPrimitive;

#[derive(Debug, Clone)]
pub struct data_sm {
    header: CommandHeader,
    /// The service_type parameter can be used to indicate the SMS Application service associated with the message.
    /// Specifying the service_type allows the ESME to
    /// - avail of enhanced messaging services such as "replace by service" type
    /// - to control the teleservice used on the air interface.
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
    pub(crate) fn new(
        sequence_number: u32,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        dest_addr_ton: u8,
        dest_addr_npi: u8,
        destination_addr: String,
        esm_class: u8,
        registered_delivery: u8,
        data_coding: u8,
    ) -> data_sm {
        data_sm {
            header: CommandHeader {
                command_length: (16
                    + service_type.len()
                    + 1
                    + 2
                    + source_addr.len()
                    + 1
                    + 2
                    + destination_addr.len()
                    + 1
                    + 3) as u32,
                command_id: CommandId::data_sm as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number,
            },
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
        }
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<data_sm, SmppError> {
        if pdu.len() < 16 {
            return Err(SmppError::ESME_RINVCMDLEN);
        }
        let input = &pdu[16..];
        let (input, service_type) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, source_addr_ton_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, source_addr_npi_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, source_addr) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, dest_addr_ton_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, dest_addr_npi_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, destination_addr) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, esm_class_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, registered_delivery_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (_input, data_coding_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;

        Ok(data_sm {
            header,
            service_type,
            source_addr_ton: source_addr_ton_bytes[0],
            source_addr_npi: source_addr_npi_bytes[0],
            source_addr,
            dest_addr_ton: dest_addr_ton_bytes[0],
            dest_addr_npi: dest_addr_npi_bytes[0],
            destination_addr,
            esm_class: esm_class_bytes[0],
            registered_delivery: registered_delivery_bytes[0],
            data_coding: data_coding_bytes[0],
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.header.command_length as usize);
        buffer.extend_from_slice(&self.header.encode());
        buffer.extend_from_slice(self.service_type.as_bytes());
        buffer.push(0x00);
        buffer.push(self.source_addr_ton);
        buffer.push(self.source_addr_npi);
        buffer.extend_from_slice(self.source_addr.as_bytes());
        buffer.push(0x00);
        buffer.push(self.dest_addr_ton);
        buffer.push(self.dest_addr_npi);
        buffer.extend_from_slice(self.destination_addr.as_bytes());
        buffer.push(0x00);
        buffer.push(self.esm_class);
        buffer.push(self.registered_delivery);
        buffer.push(self.data_coding);
        buffer
    }

    pub fn accept(self, message_id: String) -> data_sm_resp {
        if message_id.len() > 65 {
            panic!("message_id has a maximum length of 65 characters")
        }

        data_sm_resp {
            header: CommandHeader {
                command_length: 16 + message_id.len() as u32 + 1, // message_id is a C-Octet-String
                command_id: CommandId::data_sm_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: Some(message_id),
        }
    }

    pub fn reject(self, error: SmppError) -> data_sm_resp {
        data_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::data_sm_resp as u32,
                command_status: error as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: None,
        }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> data_sm_resp {
        data_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::data_sm_resp as u32,
                command_status: error as u32,
                sequence_number,
            },
            message_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct data_sm_resp {
    header: CommandHeader,
    /// This field is unused and is set to NULL
    message_id: Option<String>,
}

impl data_sm_resp {
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

    pub fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> =
            Vec::with_capacity(self.header.command_length.try_into().unwrap());
        buffer.append(&mut self.header.encode());

        if let Some(message_id) = self.message_id {
            buffer.append(&mut message_id.as_bytes().to_vec());
            buffer.push(0x00); // Terminate C-Octet-String
        }

        buffer
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<data_sm_resp, SmppError> {
        if header.command_status != SmppError::ESME_ROK as u32 {
            return Ok(data_sm_resp {
                header,
                message_id: None,
            });
        }
        if pdu.len() > 16 {
            let input = &pdu[16..];
            let (_input, message_id) =
                parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
            Ok(data_sm_resp {
                header,
                message_id: Some(message_id),
            })
        } else {
            Ok(data_sm_resp {
                header,
                message_id: None,
            })
        }
    }
}

impl SmppReply for data_sm_resp {}
