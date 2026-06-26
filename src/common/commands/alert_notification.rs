use nom::{number::complete::be_u8, IResult};

use crate::{common::parse_c_octet_string_nom, CommandHeader, CommandId, SmppError};

#[derive(Debug, Clone)]
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

// Function to parse cancel_sm PDU
fn parse_alert_notification(
    header: CommandHeader,
    pdu: &[u8],
) -> IResult<&[u8], alert_notification> {
    let (pdu, source_addr_ton) = be_u8(pdu)?;
    let (pdu, source_addr_npi) = be_u8(pdu)?;
    let (pdu, source_addr) = parse_c_octet_string_nom(pdu)?;

    let (pdu, esme_addr_ton) = be_u8(pdu)?;
    let (pdu, esme_addr_npi) = be_u8(pdu)?;
    let (pdu, esme_addr) = parse_c_octet_string_nom(pdu)?;

    let (pdu, ms_availability_status) = if !pdu.is_empty() {
        let (pdu, ms_availability_status) = be_u8(pdu)?;
        (pdu, Some(ms_availability_status))
    } else {
        (pdu, None)
    };

    Ok((
        pdu,
        alert_notification {
            header,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            esme_addr_ton,
            esme_addr_npi,
            esme_addr,
            ms_availability_status,
        },
    ))
}

impl alert_notification {
    pub(crate) fn new(
        sequence_number: u32,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        esme_addr_ton: u8,
        esme_addr_npi: u8,
        esme_addr: String,
        ms_availability_status: Option<u8>,
    ) -> alert_notification {
        assert!(
            source_addr.len() <= 65,
            "source_addr can be a maximum of 65 characters"
        );
        assert!(
            esme_addr.len() <= 65,
            "esme_addr can be a maximum of 65 characters"
        );

        alert_notification {
            header: CommandHeader {
                command_length: (16
                    + 2
                    + source_addr.len()
                    + 1
                    + 2
                    + esme_addr.len()
                    + 1
                    + if ms_availability_status.is_some() {
                        1
                    } else {
                        0
                    }) as u32,
                command_id: CommandId::alert_notification as u32,
                command_status: SmppError::ESME_ROK as u32,
                sequence_number,
            },
            source_addr_ton,
            source_addr_npi,
            source_addr,
            esme_addr_ton,
            esme_addr_npi,
            esme_addr,
            ms_availability_status,
        }
    }

    pub fn encode(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend(self.header.encode().iter());
        buffer.push(self.source_addr_ton);
        buffer.push(self.source_addr_npi);
        buffer.extend(self.source_addr.as_bytes());
        buffer.push(0);
        buffer.push(self.esme_addr_ton);
        buffer.push(self.esme_addr_npi);
        buffer.extend(self.esme_addr.as_bytes());
        buffer.push(0);
        if let Some(ms_availability_status) = self.ms_availability_status {
            buffer.push(ms_availability_status);
        }
        buffer
    }

    pub fn decode(header: CommandHeader, pdu: &[u8]) -> Result<alert_notification, SmppError> {
        match parse_alert_notification(header, pdu) {
            Ok((_, cancel_sm)) => Ok(cancel_sm),
            Err(_) => Err(SmppError::ESME_RINVPARLEN),
        }
    }
}

#[cfg(test)]
mod all_alert_notification_tests {

    #[cfg(test)]
    mod alert_notification_tests {
        use crate::{alert_notification, CommandHeader, CommandId, SmppError};

        #[test]
        fn decode_alert_notification() {
            let pdu = vec![
                0x00, 0x00, 0x00, 0x26, // command_length
                0x00, 0x00, 0x01, 0x02, // command_id (alert_notification)
                0x00, 0x00, 0x00, 0x00, // command_status
                0x12, 0x34, 0x56, 0x78, // sequence_number
                0x01, // source_addr_ton
                0x01, // source_addr_npi
                b's', b'o', b'u', b'r', b'c', b'e', 0x00, // source_addr
                0x01, // esme_addr_ton
                0x01, // esme_addr_npi
                b'd', b'e', b's', b't', b'_', b'a', b'd', b'd', b'r', 0x00, // esme_addr
                0x01, // ms_availability_status
            ];

            let decoded_command_header =
                CommandHeader::decode(&pdu).expect("Can not decode command header");
            let decoded = alert_notification::decode(decoded_command_header, &pdu[16..])
                .expect("Unable to decode alert_notification");

            assert_eq!(decoded.header.command_length, 38);
            assert_eq!(
                decoded.header.command_id,
                CommandId::alert_notification as u32
            );
            assert_eq!(decoded.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(decoded.header.sequence_number, 0x12345678);

            assert_eq!(decoded.source_addr_ton, 0x01);
            assert_eq!(decoded.source_addr_npi, 0x01);
            assert_eq!(decoded.source_addr, "source");
            assert_eq!(decoded.esme_addr_ton, 0x01);
            assert_eq!(decoded.esme_addr_npi, 0x01);
            assert_eq!(decoded.esme_addr, "dest_addr");
            assert_eq!(decoded.ms_availability_status, Some(0x01));
        }

        #[test]
        fn decode_alert_notification_no_ms_availability_status() {
            let pdu = vec![
                0x00, 0x00, 0x00, 0x25, // command_length
                0x00, 0x00, 0x01, 0x02, // command_id (alert_notification)
                0x00, 0x00, 0x00, 0x00, // command_status
                0x12, 0x34, 0x56, 0x78, // sequence_number
                0x01, // source_addr_ton
                0x01, // source_addr_npi
                b's', b'o', b'u', b'r', b'c', b'e', 0x00, // source_addr
                0x01, // esme_addr_ton
                0x01, // esme_addr_npi
                b'd', b'e', b's', b't', b'_', b'a', b'd', b'd', b'r', 0x00, // esme_addr
            ];

            let decoded_command_header =
                CommandHeader::decode(&pdu).expect("Can not decode command header");
            let decoded = alert_notification::decode(decoded_command_header, &pdu[16..])
                .expect("Unable to decode alert_notification");

            assert_eq!(decoded.header.command_length, 37);
            assert_eq!(
                decoded.header.command_id,
                CommandId::alert_notification as u32
            );
            assert_eq!(decoded.header.command_status, SmppError::ESME_ROK as u32);
            assert_eq!(decoded.header.sequence_number, 0x12345678);

            assert_eq!(decoded.source_addr_ton, 0x01);
            assert_eq!(decoded.source_addr_npi, 0x01);
            assert_eq!(decoded.source_addr, "source");
            assert_eq!(decoded.esme_addr_ton, 0x01);
            assert_eq!(decoded.esme_addr_npi, 0x01);
            assert_eq!(decoded.esme_addr, "dest_addr");
            assert_eq!(decoded.ms_availability_status, None);
        }

        #[test]
        fn encode_alert_notification() {
            let alert_notification = alert_notification::new(
                0x12345678,
                0x01,
                0x01,
                "source".to_string(),
                0x01,
                0x01,
                "dest_addr".to_string(),
                Some(0x01),
            );

            let encoded = alert_notification.encode();

            let expected_pdu = vec![
                0x00, 0x00, 0x00, 0x26, // command_length
                0x00, 0x00, 0x01, 0x02, // command_id (alert_notification)
                0x00, 0x00, 0x00, 0x00, // command_status
                0x12, 0x34, 0x56, 0x78, // sequence_number
                0x01, // source_addr_ton
                0x01, // source_addr_npi
                b's', b'o', b'u', b'r', b'c', b'e', 0x00, // source_addr
                0x01, // esme_addr_ton
                0x01, // esme_addr_npi
                b'd', b'e', b's', b't', b'_', b'a', b'd', b'd', b'r', 0x00, // esme_addr
                0x01, // ms_availability_status
            ];

            assert_eq!(encoded, expected_pdu);
        }

        #[test]
        fn encode_alert_notification_no_ms_availability_status() {
            let alert_notification = alert_notification::new(
                0x12345678,
                0x01,
                0x01,
                "source".to_string(),
                0x01,
                0x01,
                "dest_addr".to_string(),
                None,
            );

            let encoded = alert_notification.encode();

            let expected_pdu = vec![
                0x00, 0x00, 0x00, 0x25, // command_length
                0x00, 0x00, 0x01, 0x02, // command_id (alert_notification)
                0x00, 0x00, 0x00, 0x00, // command_status
                0x12, 0x34, 0x56, 0x78, // sequence_number
                0x01, // source_addr_ton
                0x01, // source_addr_npi
                b's', b'o', b'u', b'r', b'c', b'e', 0x00, // source_addr
                0x01, // esme_addr_ton
                0x01, // esme_addr_npi
                b'd', b'e', b's', b't', b'_', b'a', b'd', b'd', b'r', 0x00, // esme_addr
            ];

            assert_eq!(encoded, expected_pdu);
        }
    }
}
