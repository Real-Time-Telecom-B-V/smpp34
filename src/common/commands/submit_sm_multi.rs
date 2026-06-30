use nom::{
    number::complete::{be_u32, be_u8},
    IResult,
};
use num_traits::FromPrimitive;

use crate::common::parse_c_octet_string_nom;
use crate::common::tlv::{decode_tlvs, encode_tlvs, tlvs_encoded_len, Tlv};
use crate::{CommandHeader, CommandId, SmppError, SmppReply};

/// One entry of a `submit_sm_multi` `dest_address` list — either a single SME
/// (mobile) address, or the name of an SMSC-side distribution list (SMPP 3.4
/// §5.2.25, `dest_flag` 0x01 vs 0x02).
#[derive(Debug, Clone, PartialEq)]
pub enum DestAddress {
    /// `dest_flag = 0x01`
    Sme {
        dest_addr_ton: u8,
        dest_addr_npi: u8,
        destination_addr: String,
    },
    /// `dest_flag = 0x02`
    DistributionList { dl_name: String },
}

impl DestAddress {
    /// Convenience constructor for an SME address (TON/NPI default to 1/1).
    pub fn sme(destination_addr: impl Into<String>) -> DestAddress {
        DestAddress::Sme {
            dest_addr_ton: 1,
            dest_addr_npi: 1,
            destination_addr: destination_addr.into(),
        }
    }

    /// Convenience constructor for a distribution-list name.
    pub fn distribution_list(dl_name: impl Into<String>) -> DestAddress {
        DestAddress::DistributionList {
            dl_name: dl_name.into(),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            DestAddress::Sme {
                destination_addr, ..
            } => 1 + 1 + 1 + destination_addr.len() + 1,
            DestAddress::DistributionList { dl_name } => 1 + dl_name.len() + 1,
        }
    }

    fn encode_into(&self, buffer: &mut Vec<u8>) {
        match self {
            DestAddress::Sme {
                dest_addr_ton,
                dest_addr_npi,
                destination_addr,
            } => {
                buffer.push(0x01);
                buffer.push(*dest_addr_ton);
                buffer.push(*dest_addr_npi);
                buffer.extend_from_slice(destination_addr.as_bytes());
                buffer.push(0x00);
            }
            DestAddress::DistributionList { dl_name } => {
                buffer.push(0x02);
                buffer.extend_from_slice(dl_name.as_bytes());
                buffer.push(0x00);
            }
        }
    }
}

fn parse_dest_address(input: &[u8]) -> IResult<&[u8], DestAddress> {
    let (input, dest_flag) = be_u8(input)?;
    if dest_flag == 0x02 {
        let (input, dl_name) = parse_c_octet_string_nom(input)?;
        Ok((input, DestAddress::DistributionList { dl_name }))
    } else {
        // 0x01 (SME address); treat any other flag as SME for robustness.
        let (input, dest_addr_ton) = be_u8(input)?;
        let (input, dest_addr_npi) = be_u8(input)?;
        let (input, destination_addr) = parse_c_octet_string_nom(input)?;
        Ok((
            input,
            DestAddress::Sme {
                dest_addr_ton,
                dest_addr_npi,
                destination_addr,
            },
        ))
    }
}

/// One entry of a `submit_sm_multi_resp` `unsuccess_sme` list — a destination the
/// SMSC could not accept, with its per-destination `error_status_code`.
#[derive(Debug, Clone, PartialEq)]
pub struct UnsuccessSme {
    pub dest_addr_ton: u8,
    pub dest_addr_npi: u8,
    pub destination_addr: String,
    pub error_status_code: u32,
}

impl UnsuccessSme {
    fn encoded_len(&self) -> usize {
        1 + 1 + self.destination_addr.len() + 1 + 4
    }

    fn encode_into(&self, buffer: &mut Vec<u8>) {
        buffer.push(self.dest_addr_ton);
        buffer.push(self.dest_addr_npi);
        buffer.extend_from_slice(self.destination_addr.as_bytes());
        buffer.push(0x00);
        buffer.extend_from_slice(&self.error_status_code.to_be_bytes());
    }
}

fn parse_unsuccess_sme(input: &[u8]) -> IResult<&[u8], UnsuccessSme> {
    let (input, dest_addr_ton) = be_u8(input)?;
    let (input, dest_addr_npi) = be_u8(input)?;
    let (input, destination_addr) = parse_c_octet_string_nom(input)?;
    let (input, error_status_code) = be_u32(input)?;
    Ok((
        input,
        UnsuccessSme {
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
            error_status_code,
        },
    ))
}

#[derive(Debug, Clone)]
pub struct submit_sm_multi {
    pub header: CommandHeader,
    pub service_type: String,
    pub source_addr_ton: u8,
    pub source_addr_npi: u8,
    pub source_addr: String,
    pub number_of_dests: u8,
    pub dest_addresses: Vec<DestAddress>,
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

impl submit_sm_multi {
    pub fn new(
        sequence_number: u32,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        dest_addresses: Vec<DestAddress>,
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
    ) -> submit_sm_multi {
        assert!(
            short_message.len() <= 254,
            "Message can only be a maximum of 254 characters"
        );
        assert!(
            dest_addresses.len() <= 254,
            "submit_sm_multi can address a maximum of 254 destinations"
        );

        let mut pdu = submit_sm_multi {
            header: CommandHeader {
                command_length: 0,
                command_id: CommandId::submit_multi as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number,
            },
            service_type,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            number_of_dests: dest_addresses.len() as u8,
            dest_addresses,
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
        };
        pdu.header.command_length = pdu.base_len();
        pdu
    }

    /// Length of the fixed body + dest list (header included), excluding TLVs.
    fn base_len(&self) -> u32 {
        let dests: usize = self
            .dest_addresses
            .iter()
            .map(DestAddress::encoded_len)
            .sum();
        (16 + self.service_type.len()
            + 1
            + 1 // source_addr_ton
            + 1 // source_addr_npi
            + self.source_addr.len()
            + 1
            + 1 // number_of_dests
            + dests
            + 1 // esm_class
            + 1 // protocol_id
            + 1 // priority_flag
            + self.schedule_delivery_time.len()
            + 1
            + self.validity_period.len()
            + 1
            + 1 // registered_delivery
            + 1 // replace_if_present_flag
            + 1 // data_coding
            + 1 // sm_default_msg_id
            + 1 // sm_length
            + self.short_message.len()) as u32
    }

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<submit_sm_multi, SmppError> {
        match parse_submit_sm_multi(header, pdu) {
            Ok((_, pdu)) => Ok(pdu),
            Err(_) => Err(SmppError::ESME_RINVPARLEN),
        }
    }

    pub fn encode(self) -> Vec<u8> {
        let total_len = self.base_len() + tlvs_encoded_len(&self.tlvs) as u32;
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
        buffer.push(self.number_of_dests);
        for dest in &self.dest_addresses {
            dest.encode_into(&mut buffer);
        }
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

    /// Accept the request, returning the SMSC-assigned `message_id` and the list
    /// of destinations that could not be accepted (empty = all succeeded).
    pub fn accept(
        self,
        message_id: String,
        unsuccess_sme: Vec<UnsuccessSme>,
    ) -> submit_sm_multi_resp {
        let unsuccess_len: usize = unsuccess_sme.iter().map(UnsuccessSme::encoded_len).sum();
        let command_length = (16 + message_id.len() + 1 + 1 + unsuccess_len) as u32;
        submit_sm_multi_resp {
            header: CommandHeader {
                command_length,
                command_id: CommandId::submit_multi_resp as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number: self.header.sequence_number,
            },
            message_id: Some(message_id),
            no_unsuccess: unsuccess_sme.len() as u8,
            unsuccess_sme,
        }
    }

    pub fn reject(self, error: SmppError) -> submit_sm_multi_resp {
        submit_sm_multi::generic_reject(self.header.sequence_number, error)
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> submit_sm_multi_resp {
        submit_sm_multi_resp {
            header: CommandHeader {
                command_length: 16,
                command_id: CommandId::submit_multi_resp as u32,
                command_status: error as u32,
                sequence_number,
            },
            message_id: None,
            no_unsuccess: 0,
            unsuccess_sme: Vec::new(),
        }
    }
}

fn parse_submit_sm_multi(header: CommandHeader, pdu: &[u8]) -> IResult<&[u8], submit_sm_multi> {
    let input = &pdu[16..];
    let (input, service_type) = parse_c_octet_string_nom(input)?;
    let (input, source_addr_ton) = be_u8(input)?;
    let (input, source_addr_npi) = be_u8(input)?;
    let (input, source_addr) = parse_c_octet_string_nom(input)?;

    let (mut input, number_of_dests) = be_u8(input)?;
    let mut dest_addresses = Vec::with_capacity(number_of_dests as usize);
    for _ in 0..number_of_dests {
        let (rest, dest) = parse_dest_address(input)?;
        dest_addresses.push(dest);
        input = rest;
    }

    let (input, esm_class) = be_u8(input)?;
    let (input, protocol_id) = be_u8(input)?;
    let (input, priority_flag) = be_u8(input)?;
    let (input, schedule_delivery_time) = parse_c_octet_string_nom(input)?;
    let (input, validity_period) = parse_c_octet_string_nom(input)?;
    let (input, registered_delivery) = be_u8(input)?;
    let (input, replace_if_present_flag) = be_u8(input)?;
    let (input, data_coding) = be_u8(input)?;
    let (input, sm_default_msg_id) = be_u8(input)?;
    let (input, sm_length) = be_u8(input)?;
    let (input, short_message) = nom::bytes::complete::take(sm_length as usize)(input)?;

    let tlvs = decode_tlvs(input);

    Ok((
        input,
        submit_sm_multi {
            header,
            service_type,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            number_of_dests,
            dest_addresses,
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
            short_message: short_message.to_vec(),
            tlvs,
        },
    ))
}

#[derive(Debug, Clone)]
pub struct submit_sm_multi_resp {
    pub header: CommandHeader,
    pub message_id: Option<String>,
    pub no_unsuccess: u8,
    pub unsuccess_sme: Vec<UnsuccessSme>,
}

impl submit_sm_multi_resp {
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

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<submit_sm_multi_resp, SmppError> {
        // No body is returned on a non-zero command_status (SMPP 3.4 §4.5.2).
        if header.command_status != SmppError::ESME_ROK as u32 {
            return Ok(submit_sm_multi_resp {
                header,
                message_id: None,
                no_unsuccess: 0,
                unsuccess_sme: Vec::new(),
            });
        }

        let input = &pdu[16..];
        let (input, message_id) =
            parse_c_octet_string_nom(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let (mut input, no_unsuccess) = be_u8::<&[u8], nom::error::Error<&[u8]>>(input)
            .map_err(|_| SmppError::ESME_RINVPARLEN)?;
        let mut unsuccess_sme = Vec::with_capacity(no_unsuccess as usize);
        for _ in 0..no_unsuccess {
            let (rest, entry) =
                parse_unsuccess_sme(input).map_err(|_| SmppError::ESME_RINVPARLEN)?;
            unsuccess_sme.push(entry);
            input = rest;
        }

        Ok(submit_sm_multi_resp {
            header,
            message_id: Some(message_id),
            no_unsuccess,
            unsuccess_sme,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let is_ok = self.header.command_status == SmppError::ESME_ROK as u32;
        let mut buffer: Vec<u8> = Vec::with_capacity(self.header.command_length as usize);
        buffer.extend_from_slice(&self.header.encode());
        if is_ok {
            if let Some(message_id) = &self.message_id {
                buffer.extend_from_slice(message_id.as_bytes());
            }
            buffer.push(0x00); // message_id C-Octet-String terminator
            buffer.push(self.no_unsuccess);
            for entry in &self.unsuccess_sme {
                entry.encode_into(&mut buffer);
            }
        }
        buffer
    }
}

impl SmppReply for submit_sm_multi_resp {}

#[cfg(test)]
mod submit_sm_multi_tests {
    use super::*;

    fn sample() -> submit_sm_multi {
        submit_sm_multi::new(
            0x12345678,
            "WAP".to_string(),
            1,
            1,
            "12345".to_string(),
            vec![
                DestAddress::sme("31600000000"),
                DestAddress::Sme {
                    dest_addr_ton: 5,
                    dest_addr_npi: 0,
                    destination_addr: "SHORTCODE".to_string(),
                },
                DestAddress::distribution_list("vip-list"),
            ],
            0,
            0,
            0,
            String::new(),
            String::new(),
            1,
            0,
            0,
            0,
            b"hello multi".to_vec(),
        )
    }

    #[test]
    fn round_trip_request() {
        let pdu = sample();
        let encoded = pdu.clone().encode();
        // command_length is self-consistent with the encoded length.
        assert_eq!(encoded.len(), pdu.header.command_length as usize);

        let header = CommandHeader::decode(&encoded).expect("header");
        assert_eq!(header.command_id, CommandId::submit_multi as u32);
        let decoded = submit_sm_multi::decode(header, &encoded).expect("decode");

        assert_eq!(decoded.service_type, "WAP");
        assert_eq!(decoded.source_addr, "12345");
        assert_eq!(decoded.number_of_dests, 3);
        assert_eq!(decoded.dest_addresses.len(), 3);
        assert_eq!(decoded.dest_addresses[0], DestAddress::sme("31600000000"));
        assert_eq!(
            decoded.dest_addresses[1],
            DestAddress::Sme {
                dest_addr_ton: 5,
                dest_addr_npi: 0,
                destination_addr: "SHORTCODE".to_string()
            }
        );
        assert_eq!(
            decoded.dest_addresses[2],
            DestAddress::distribution_list("vip-list")
        );
        assert_eq!(decoded.short_message, b"hello multi");
        // Re-encode must be byte-identical.
        assert_eq!(decoded.encode(), encoded);
    }

    #[test]
    fn round_trip_resp_with_failures() {
        let resp = sample().accept(
            "msg-1".to_string(),
            vec![UnsuccessSme {
                dest_addr_ton: 1,
                dest_addr_npi: 1,
                destination_addr: "31699999999".to_string(),
                error_status_code: SmppError::ESME_RINVDSTADR as u32,
            }],
        );
        let encoded = resp.clone().encode();
        let header = CommandHeader::decode(&encoded).expect("header");
        assert_eq!(header.command_id, CommandId::submit_multi_resp as u32);
        let decoded = submit_sm_multi_resp::decode(header, &encoded).expect("decode");

        assert!(decoded.is_success());
        assert_eq!(decoded.message_id.as_deref(), Some("msg-1"));
        assert_eq!(decoded.no_unsuccess, 1);
        assert_eq!(decoded.unsuccess_sme.len(), 1);
        assert_eq!(decoded.unsuccess_sme[0].destination_addr, "31699999999");
        assert_eq!(
            decoded.unsuccess_sme[0].error_status_code,
            SmppError::ESME_RINVDSTADR as u32
        );
    }

    #[test]
    fn reject_has_no_body() {
        let resp = sample().reject(SmppError::ESME_RSUBMITFAIL);
        let encoded = resp.encode();
        assert_eq!(encoded.len(), 16); // header only
        let header = CommandHeader::decode(&encoded).expect("header");
        let decoded = submit_sm_multi_resp::decode(header, &encoded).expect("decode");
        assert!(!decoded.is_success());
        assert_eq!(decoded.message_id, None);
    }
}
