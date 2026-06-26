use crate::common::parse_c_octet_string_nom;
use crate::common::tlv::{decode_tlvs, encode_tlvs, tlvs_encoded_len, Tlv};
use crate::{CommandHeader, CommandId, SmppError, SmppReply};
use nom::bytes::complete::take;
use num_traits::FromPrimitive;

#[derive(Debug, Clone)]
pub struct deliver_sm {
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
    pub tlvs: Vec<Tlv>,
}

fn message_command_length(
    service_type: &str,
    source_addr: &str,
    destination_addr: &str,
    schedule_delivery_time: &str,
    validity_period: &str,
    short_message: &[u8],
) -> u32 {
    (16 + service_type.len()
        + 1
        + 1
        + 1
        + source_addr.len()
        + 1
        + 1
        + 1
        + destination_addr.len()
        + 1
        + 1
        + 1
        + 1
        + schedule_delivery_time.len()
        + 1
        + validity_period.len()
        + 1
        + 1
        + 1
        + 1
        + 1
        + 1
        + short_message.len()) as u32
}

impl deliver_sm {
    pub fn new(
        sequence_number: u32,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        dest_addr_ton: u8,
        dest_addr_npi: u8,
        destination_addr: String,
        esm_class: u8,
        protocol_id: u8,
        priority_flag: u8,
        schedule_delivery_time: String,
        validity_period: String,
        registered_delivery: u8,
        replace_if_present_flag: u8,
        data_coding: u8,
        sm_default_msg_id: u8,
        short_message: Vec<u8>,
    ) -> deliver_sm {
        assert!(
            short_message.len() <= 254,
            "Message can only be a maximum of 254 characters"
        );

        let cmd_len = message_command_length(
            &service_type,
            &source_addr,
            &destination_addr,
            &schedule_delivery_time,
            &validity_period,
            &short_message,
        );

        deliver_sm {
            header: CommandHeader {
                command_length: cmd_len,
                command_id: CommandId::deliver_sm as u32,
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
            tlvs: Vec::new(),
        }
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<deliver_sm, SmppError> {
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
        let (input, protocol_id_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, priority_flag_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, schedule_delivery_time) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, validity_period) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, registered_delivery_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, replace_if_present_flag_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, data_coding_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, sm_default_msg_id_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (input, sm_length_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let sm_length = sm_length_bytes[0];
        let (input, short_message) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(sm_length as usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;

        let tlvs = decode_tlvs(input);

        Ok(deliver_sm {
            header,
            service_type,
            source_addr_ton: source_addr_ton_bytes[0],
            source_addr_npi: source_addr_npi_bytes[0],
            source_addr,
            dest_addr_ton: dest_addr_ton_bytes[0],
            dest_addr_npi: dest_addr_npi_bytes[0],
            destination_addr,
            esm_class: esm_class_bytes[0],
            protocol_id: protocol_id_bytes[0],
            priority_flag: priority_flag_bytes[0],
            schedule_delivery_time,
            validity_period,
            registered_delivery: registered_delivery_bytes[0],
            replace_if_present_flag: replace_if_present_flag_bytes[0],
            data_coding: data_coding_bytes[0],
            sm_default_msg_id: sm_default_msg_id_bytes[0],
            sm_length,
            short_message: short_message.to_vec(),
            tlvs,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let base_len = message_command_length(
            &self.service_type,
            &self.source_addr,
            &self.destination_addr,
            &self.schedule_delivery_time,
            &self.validity_period,
            &self.short_message,
        );
        let total_len = base_len + tlvs_encoded_len(&self.tlvs) as u32;

        let mut buffer: Vec<u8> = Vec::with_capacity(total_len as usize);
        let header = CommandHeader {
            command_length: total_len,
            ..self.header
        };
        buffer.extend_from_slice(&header.encode());
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
        buffer.push(self.protocol_id);
        buffer.push(self.priority_flag);
        buffer.extend_from_slice(self.schedule_delivery_time.as_bytes());
        buffer.push(0x00);
        buffer.extend_from_slice(self.validity_period.as_bytes());
        buffer.push(0x00);
        buffer.push(self.registered_delivery);
        buffer.push(self.replace_if_present_flag);
        buffer.push(self.data_coding);
        buffer.push(self.sm_default_msg_id);
        buffer.push(self.sm_length);
        buffer.extend_from_slice(&self.short_message);
        buffer.extend_from_slice(&encode_tlvs(&self.tlvs));
        buffer
    }

    pub fn accept(self) -> deliver_sm_resp {
        deliver_sm_resp {
            header: CommandHeader {
                command_length: 16 + 1, // message_id is a C-Octet-String (and is always NULL in deliver_sm_resp)
                command_id: CommandId::deliver_sm_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: "".into(),
        }
    }

    pub fn reject(self, error: SmppError) -> deliver_sm_resp {
        deliver_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::deliver_sm_resp as u32,
                command_status: error as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: "".into(),
        }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> deliver_sm_resp {
        deliver_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::deliver_sm_resp as u32,
                command_status: error as u32,
                sequence_number,
            },
            message_id: "".into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct deliver_sm_resp {
    header: CommandHeader,
    /// This field is unused and is set to NULL
    message_id: String,
}

impl deliver_sm_resp {
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
        buffer.append(&mut self.message_id.as_bytes().to_vec());
        buffer.push(0x00); // Terminate C-Octet-String

        buffer
    }

    pub fn decode(header: CommandHeader, pdu: &Vec<u8>) -> Result<deliver_sm_resp, SmppError> {
        if header.command_status != SmppError::ESME_ROK as u32 {
            return Ok(deliver_sm_resp {
                header,
                message_id: String::new(),
            });
        }
        if pdu.len() > 16 {
            let input = &pdu[16..];
            let (_input, message_id) =
                parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
            Ok(deliver_sm_resp { header, message_id })
        } else {
            Ok(deliver_sm_resp {
                header,
                message_id: String::new(),
            })
        }
    }
}

impl SmppReply for deliver_sm_resp {}
