use crate::common::parse_c_octet_string_nom;
use crate::common::tlv::{decode_tlvs, encode_tlvs, tlvs_encoded_len, Tlv};
use crate::{CommandHeader, CommandId, SmppError, SmppReply};
use nom::bytes::complete::take;

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
    /// Optional parameters. `data_sm` has no `short_message` field at all: the
    /// message body travels here, in the `message_payload` TLV (§4.2.2).
    pub tlvs: Vec<Tlv>,
}

/// Header + mandatory body, excluding the optional parameters (§4.2.2).
fn base_command_length(service_type: &str, source_addr: &str, destination_addr: &str) -> u32 {
    (16 + service_type.len()
        + 1
        + 2 // source_addr_ton + source_addr_npi
        + source_addr.len()
        + 1
        + 2 // dest_addr_ton + dest_addr_npi
        + destination_addr.len()
        + 1
        + 3) as u32 // esm_class + registered_delivery + data_coding
}

impl data_sm {
    /// Build a `data_sm`. The optional parameters start out empty — attach them
    /// with [`with_tlvs`](data_sm::with_tlvs) / [`push_tlv`](data_sm::push_tlv).
    /// Since `data_sm` carries its message in the `message_payload` TLV, a
    /// `data_sm` without TLVs carries no message.
    ///
    /// `sequence_number` is ignored when the PDU is handed to
    /// [`SMSC::send_data_sm_pdu`](crate::client::SMSC::send_data_sm_pdu) or
    /// [`ESME::send_data_sm_pdu`](crate::server::ESME::send_data_sm_pdu): the
    /// session owns the sequence space and overwrites it.
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
        registered_delivery: u8,
        data_coding: u8,
    ) -> data_sm {
        data_sm {
            header: CommandHeader {
                command_length: base_command_length(&service_type, &source_addr, &destination_addr),
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
            tlvs: Vec::new(),
        }
    }

    /// Append optional parameters (TLVs), consuming and returning the PDU so it
    /// chains off [`new`](data_sm::new). `command_length` is recomputed at
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

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<data_sm, SmppError> {
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
        let (input, data_coding_bytes) =
            take::<usize, &[u8], nom::error::Error<&[u8]>>(1usize)(input)
                .map_err(|_| SmppError::ESME_RINVPARLEN)?;

        let tlvs = decode_tlvs(input);

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
            tlvs,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let total_len = base_command_length(
            &self.service_type,
            &self.source_addr,
            &self.destination_addr,
        ) + tlvs_encoded_len(&self.tlvs) as u32;

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
        buffer.push(self.registered_delivery);
        buffer.push(self.data_coding);
        buffer.extend_from_slice(&encode_tlvs(&self.tlvs));
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
            tlvs: Vec::new(),
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
            tlvs: Vec::new(),
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
            tlvs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct data_sm_resp {
    header: CommandHeader,
    /// This field is unused and is set to NULL
    message_id: Option<String>,
    /// Optional parameters. `data_sm_resp` is the only response PDU in SMPP 3.4
    /// that defines any (§4.2.3): `delivery_failure_reason`,
    /// `network_error_code`, `additional_status_info_text`, `dpf_result`.
    pub tlvs: Vec<Tlv>,
}

impl data_sm_resp {
    pub fn is_success(&self) -> bool {
        self.header.command_status == SmppError::ESME_ROK as u32
    }
    pub fn command_status(&self) -> u32 {
        self.header.command_status
    }
    pub fn get_error(&self) -> SmppError {
        SmppError::from_command_status(self.header.command_status)
    }

    /// Append optional parameters (TLVs), consuming and returning the response
    /// so it chains off [`data_sm::accept`] / [`data_sm::reject`].
    pub fn with_tlvs(mut self, tlvs: impl IntoIterator<Item = Tlv>) -> Self {
        self.tlvs.extend(tlvs);
        self
    }

    /// Append a single optional parameter (TLV).
    pub fn push_tlv(&mut self, tlv: Tlv) {
        self.tlvs.push(tlv);
    }

    pub fn encode(self) -> Vec<u8> {
        let base_len = 16
            + match &self.message_id {
                Some(message_id) => message_id.len() as u32 + 1, // C-Octet-String
                None => 0,
            };
        let total_len = base_len + tlvs_encoded_len(&self.tlvs) as u32;

        let mut buffer: Vec<u8> = Vec::with_capacity(total_len as usize);
        let header = CommandHeader {
            command_length: total_len,
            ..self.header
        };
        buffer.append(&mut header.encode());

        if let Some(message_id) = self.message_id {
            buffer.append(&mut message_id.as_bytes().to_vec());
            buffer.push(0x00); // Terminate C-Octet-String
        }
        buffer.extend_from_slice(&encode_tlvs(&self.tlvs));

        buffer
    }

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<data_sm_resp, SmppError> {
        if header.command_status != SmppError::ESME_ROK as u32 {
            // §4.2.3: a failed data_sm_resp carries no message_id, but it may
            // still carry the optional parameters that explain the failure
            // (delivery_failure_reason, network_error_code, …).
            return Ok(data_sm_resp {
                header,
                message_id: None,
                tlvs: decode_tlvs(pdu.get(16..).unwrap_or(&[])),
            });
        }
        if pdu.len() > 16 {
            let input = &pdu[16..];
            let (input, message_id) =
                parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
            Ok(data_sm_resp {
                header,
                message_id: Some(message_id),
                tlvs: decode_tlvs(input),
            })
        } else {
            Ok(data_sm_resp {
                header,
                message_id: None,
                tlvs: Vec::new(),
            })
        }
    }
}

impl SmppReply for data_sm_resp {}

#[cfg(test)]
mod data_sm_tlv_tests {
    use super::*;
    use crate::common::tlv::{Tlv, TlvList, TlvTag};

    /// `data_sm` (SMPP 3.4 §4.2.2) has no `short_message` field at all — the
    /// message body travels in the `message_payload` TLV, so a `data_sm` without
    /// optional parameters cannot carry a message.
    fn known_answer() -> Vec<u8> {
        vec![
            0x00, 0x00, 0x00, 0x2A, // command_length = 42
            0x00, 0x00, 0x01, 0x03, // command_id = data_sm
            0x00, 0x00, 0x00, 0x00, // command_status
            0x00, 0x00, 0x00, 0x07, // sequence_number
            0x00, // service_type (NULL)
            0x01, // source_addr_ton
            0x01, // source_addr_npi
            b'1', b'2', b'3', b'4', b'5', 0x00, // source_addr
            0x01, // dest_addr_ton
            0x01, // dest_addr_npi
            b'9', b'9', b'9', 0x00, // destination_addr
            0x00, // esm_class
            0x00, // registered_delivery
            0x08, // data_coding (UCS2)
            0x04, 0x24, 0x00, 0x04, b'b', b'o', b'd', b'y', // TLV message_payload
        ]
    }

    fn sample() -> data_sm {
        data_sm::new(
            7,
            String::new(),
            1,
            1,
            "12345".to_string(),
            1,
            1,
            "999".to_string(),
            0,
            0,
            8,
        )
        .with_tlvs([Tlv::from_tag(TlvTag::MessagePayload, b"body".to_vec())])
    }

    #[test]
    fn encodes_the_message_payload_tlv_to_the_spec_wire_image() {
        assert_eq!(sample().encode(), known_answer());
    }

    #[test]
    fn decodes_the_message_payload_tlv_from_the_spec_wire_image() {
        let pdu = known_answer();
        let header = CommandHeader::decode(&pdu).expect("header");
        let decoded = data_sm::decode(header, &pdu).expect("decode");

        assert_eq!(decoded.data_coding, 8);
        assert_eq!(decoded.tlvs.len(), 1);
        assert_eq!(decoded.tlvs.message_payload(), Some(b"body".as_slice()));
    }

    #[test]
    fn command_length_covers_the_tlvs() {
        let encoded = sample().encode();
        assert_eq!(
            u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize,
            encoded.len()
        );
    }

    /// §4.2.3: `data_sm_resp` is the only response PDU in 3.4 with optional
    /// parameters. A rejection explains itself in `network_error_code` /
    /// `delivery_failure_reason`, which is the whole point of them.
    #[test]
    fn data_sm_resp_carries_its_failure_tlvs() {
        let req = sample();
        let resp = req
            .reject(SmppError::ESME_RSUBMITFAIL)
            .with_tlvs([Tlv::from_tag(
                TlvTag::NetworkErrorCode,
                vec![0x03, 0x00, 0x1F],
            )]);
        let encoded = resp.encode();

        assert_eq!(
            encoded,
            vec![
                0x00, 0x00, 0x00, 0x17, // command_length = 23
                0x80, 0x00, 0x01, 0x03, // command_id = data_sm_resp
                0x00, 0x00, 0x00, 0x45, // command_status = ESME_RSUBMITFAIL
                0x00, 0x00, 0x00, 0x07, // sequence_number (echoed from the request)
                0x04, 0x23, 0x00, 0x03, 0x03, 0x00, 0x1F, // TLV network_error_code
            ]
        );

        let header = CommandHeader::decode(&encoded).expect("header");
        let decoded = data_sm_resp::decode(header, &encoded).expect("decode");
        assert!(!decoded.is_success());
        assert_eq!(decoded.tlvs.network_error_code(), Some((3, 31)));
    }

    #[test]
    fn data_sm_resp_tlvs_follow_the_message_id() {
        let resp = sample()
            .accept("id-1".to_string())
            .with_tlvs([Tlv::from_tag(
                TlvTag::AdditionalStatusInfoText,
                b"ok\0".to_vec(),
            )]);
        let encoded = resp.encode();

        // 16 header + 5 ("id-1" + NUL) + 7 TLV
        assert_eq!(encoded.len(), 28);
        assert_eq!(encoded[3], 28, "command_length must cover the TLVs");

        let header = CommandHeader::decode(&encoded).expect("header");
        let decoded = data_sm_resp::decode(header, &encoded).expect("decode");
        assert!(decoded.is_success());
        assert_eq!(
            decoded
                .tlvs
                .get_tlv(TlvTag::AdditionalStatusInfoText)
                .and_then(Tlv::as_string),
            Some("ok".to_string())
        );
    }

    #[test]
    fn without_tlvs_the_body_ends_at_data_coding() {
        let encoded = data_sm::new(
            7,
            String::new(),
            1,
            1,
            "12345".to_string(),
            1,
            1,
            "999".to_string(),
            0,
            0,
            8,
        )
        .encode();
        // 42 minus the 8 TLV bytes.
        assert_eq!(encoded.len(), 34);
        assert_eq!(encoded[3], 34);
    }
}
