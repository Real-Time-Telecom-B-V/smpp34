use crate::common::parse_c_octet_string_nom;
use crate::common::tlv::{decode_tlvs, encode_tlvs, tlvs_encoded_len, Tlv};
use crate::{CommandHeader, CommandId, SmppError, SmppReply};
use nom::bytes::complete::take;
use num_traits::FromPrimitive;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct submit_sm {
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

impl submit_sm {
    /// Build a `submit_sm`. The optional parameters start out empty — attach
    /// them with [`with_tlvs`](submit_sm::with_tlvs) / [`push_tlv`](submit_sm::push_tlv),
    /// or use the [`SubmitSmBuilder`](crate::client::SubmitSmBuilder) returned by
    /// `SMSC::submit_sm()`.
    ///
    /// `sequence_number` is ignored when the PDU is handed to
    /// [`SMSC::send_submit_sm_pdu`](crate::client::SMSC::send_submit_sm_pdu):
    /// the session owns the sequence space and overwrites it.
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
    ) -> submit_sm {
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

        submit_sm {
            header: CommandHeader {
                command_length: cmd_len,
                command_id: CommandId::submit_sm as u32,
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

    /// Append optional parameters (TLVs), consuming and returning the PDU so it
    /// chains off [`new`](submit_sm::new). `command_length` is recomputed at
    /// encode time, so TLVs can be attached in any order.
    pub fn with_tlvs(mut self, tlvs: impl IntoIterator<Item = Tlv>) -> Self {
        self.tlvs.extend(tlvs);
        self
    }

    /// Append a single optional parameter (TLV).
    pub fn push_tlv(&mut self, tlv: Tlv) {
        self.tlvs.push(tlv);
    }

    pub(crate) fn set_sequence_number(&mut self, sequence_number: u32) {
        self.header.sequence_number = sequence_number;
    }

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<submit_sm, SmppError> {
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

        Ok(submit_sm {
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

    pub fn accept(self, message_id: String) -> submit_sm_resp {
        if message_id.len() > 65 {
            panic!("message_id has a maximum length of 65 characters")
        }

        submit_sm_resp {
            header: CommandHeader {
                command_length: 16 + message_id.len() as u32 + 1, // message_id is a C-Octet-String
                command_id: CommandId::submit_sm_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: Some(message_id),
        }
    }

    pub fn reject(self, error: SmppError) -> submit_sm_resp {
        submit_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::submit_sm_resp as u32,
                command_status: error as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: None,
        }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> submit_sm_resp {
        submit_sm_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::submit_sm_resp as u32,
                command_status: error as u32,
                sequence_number,
            },
            message_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct submit_sm_resp {
    pub header: CommandHeader,
    pub message_id: Option<String>,
}

impl submit_sm_resp {
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

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<submit_sm_resp, SmppError> {
        if header.command_status != SmppError::ESME_ROK as u32 {
            // As per specification: "The submit_sm_resp PDU Body is not returned if the command_status field contains a non-zero value."
            Ok(submit_sm_resp {
                header,
                message_id: None,
            })
        } else {
            let input = &pdu[16..];
            let (_input, message_id) =
                parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
            Ok(submit_sm_resp {
                header,
                message_id: Some(message_id),
            })
        }
    }
}

impl SmppReply for submit_sm_resp {}

#[cfg(test)]
mod submit_sm_tlv_tests {
    use super::*;
    use crate::common::tlv::{TlvList, TlvTag};

    /// Hand-built wire image (SMPP 3.4 §4.4.1): mandatory body followed by two
    /// optional parameters — `user_message_reference` (0x0204) and a vendor tag
    /// outside the spec table.
    fn known_answer() -> Vec<u8> {
        vec![
            0x00, 0x00, 0x00, 0x38, // command_length = 56
            0x00, 0x00, 0x00, 0x04, // command_id = submit_sm
            0x00, 0x00, 0x00, 0x00, // command_status
            0x12, 0x34, 0x56, 0x78, // sequence_number
            0x00, // service_type (NULL)
            0x01, // source_addr_ton
            0x01, // source_addr_npi
            b'1', b'2', b'3', b'4', b'5', 0x00, // source_addr
            0x01, // dest_addr_ton
            0x01, // dest_addr_npi
            b'9', b'9', b'9', 0x00, // destination_addr
            0x00, // esm_class
            0x00, // protocol_id
            0x00, // priority_flag
            0x00, // schedule_delivery_time (NULL)
            0x00, // validity_period (NULL)
            0x01, // registered_delivery
            0x00, // replace_if_present_flag
            0x00, // data_coding
            0x00, // sm_default_msg_id
            0x02, // sm_length
            b'h', b'i', // short_message
            0x02, 0x04, 0x00, 0x02, 0x12, 0x34, // TLV user_message_reference
            0x14, 0x03, 0x00, 0x03, 0xAA, 0xBB, 0xCC, // TLV vendor-specific
        ]
    }

    fn sample() -> submit_sm {
        submit_sm::new(
            0x12345678,
            String::new(),
            1,
            1,
            "12345".to_string(),
            1,
            1,
            "999".to_string(),
            0,
            0,
            0,
            String::new(),
            String::new(),
            1,
            0,
            0,
            0,
            b"hi".to_vec(),
        )
        .with_tlvs([
            Tlv::from_tag(TlvTag::UserMessageReference, vec![0x12, 0x34]),
            Tlv::new(0x1403, vec![0xAA, 0xBB, 0xCC]),
        ])
    }

    #[test]
    fn encodes_tlvs_to_the_spec_wire_image() {
        assert_eq!(sample().encode(), known_answer());
    }

    #[test]
    fn command_length_covers_the_tlvs() {
        let encoded = sample().encode();
        assert_eq!(
            u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize,
            encoded.len(),
            "command_length must include the optional parameters"
        );
    }

    #[test]
    fn decodes_tlvs_from_the_spec_wire_image() {
        let pdu = known_answer();
        let header = CommandHeader::decode(&pdu).expect("header");
        let decoded = submit_sm::decode(header, &pdu).expect("decode");

        assert_eq!(decoded.short_message, b"hi");
        assert_eq!(decoded.tlvs.len(), 2);
        assert_eq!(decoded.tlvs.user_message_reference(), Some(0x1234));
        assert_eq!(
            decoded.tlvs.get_tlv_raw(0x1403).map(|t| t.value.as_slice()),
            Some([0xAA, 0xBB, 0xCC].as_slice())
        );
    }

    #[test]
    fn without_tlvs_the_body_ends_at_the_short_message() {
        let encoded = submit_sm::new(
            0x12345678,
            String::new(),
            1,
            1,
            "12345".to_string(),
            1,
            1,
            "999".to_string(),
            0,
            0,
            0,
            String::new(),
            String::new(),
            1,
            0,
            0,
            0,
            b"hi".to_vec(),
        )
        .encode();
        // 56 minus the 13 TLV bytes.
        assert_eq!(encoded.len(), 43);
        assert_eq!(encoded[3], 43);
    }

    #[test]
    fn push_tlv_appends_in_order() {
        let mut pdu = submit_sm::new(
            1,
            String::new(),
            0,
            0,
            String::new(),
            0,
            0,
            String::new(),
            0,
            0,
            0,
            String::new(),
            String::new(),
            0,
            0,
            0,
            0,
            Vec::new(),
        );
        pdu.push_tlv(Tlv::from_tag(TlvTag::SarMsgRefNum, vec![0x00, 0x01]));
        pdu.push_tlv(Tlv::from_tag(TlvTag::SarTotalSegments, vec![0x02]));
        assert_eq!(pdu.tlvs.len(), 2);
        assert_eq!(pdu.tlvs[0].tag, TlvTag::SarMsgRefNum as u16);
        assert_eq!(pdu.tlvs[1].tag, TlvTag::SarTotalSegments as u16);
    }
}
