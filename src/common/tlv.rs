/// SMPP 3.4 Optional Parameter (TLV) support — §3.2.1, §4.8
use log::warn;
use nom::bytes::complete::take;
use nom::IResult;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Tlv {
    pub tag: u16,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum TlvTag {
    DestAddrSubunit = 0x0005,
    DestNetworkType = 0x0006,
    DestBearerType = 0x0007,
    DestTelematicsId = 0x0008,
    SourceAddrSubunit = 0x000D,
    SourceNetworkType = 0x000E,
    SourceBearerType = 0x000F,
    SourceTelematicsId = 0x0010,
    QosTimeToLive = 0x0017,
    PayloadType = 0x0019,
    AdditionalStatusInfoText = 0x001D,
    ReceiptedMessageId = 0x001E,
    MsMsgWaitFacilities = 0x0030,
    PrivacyIndicator = 0x0201,
    SourceSubaddress = 0x0202,
    DestSubaddress = 0x0203,
    UserMessageReference = 0x0204,
    UserResponseCode = 0x0205,
    SourcePort = 0x020A,
    DestinationPort = 0x020B,
    SarMsgRefNum = 0x020C,
    LanguageIndicator = 0x020D,
    SarTotalSegments = 0x020E,
    SarSegmentSeqnum = 0x020F,
    ScInterfaceVersion = 0x0210,
    CallbackNumPresInd = 0x0302,
    CallbackNumAtag = 0x0303,
    NumberOfMessages = 0x0304,
    CallbackNum = 0x0381,
    DpfResult = 0x0420,
    SetDpf = 0x0421,
    MsAvailabilityStatus = 0x0422,
    NetworkErrorCode = 0x0423,
    MessagePayload = 0x0424,
    DeliveryFailureReason = 0x0425,
    MoreMessagesToSend = 0x0426,
    MessageStateTlv = 0x0427,
    UssdServiceOp = 0x0501,
    DisplayTime = 0x1201,
    SmsSignal = 0x1203,
    MsValidity = 0x1204,
    AlertOnMessageDelivery = 0x130C,
    ItsReplyType = 0x1380,
    ItsSessionInfo = 0x1383,
}

impl Tlv {
    pub fn new(tag: u16, value: Vec<u8>) -> Self { Tlv { tag, value } }
    pub fn from_tag(tag: TlvTag, value: Vec<u8>) -> Self { Tlv { tag: tag as u16, value } }
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.value.len());
        buf.extend_from_slice(&self.tag.to_be_bytes());
        buf.extend_from_slice(&(self.value.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.value);
        buf
    }
    pub fn encoded_len(&self) -> usize { 4 + self.value.len() }
    pub fn as_u8(&self) -> Option<u8> { if self.value.len() == 1 { Some(self.value[0]) } else { None } }
    pub fn as_u16(&self) -> Option<u16> { if self.value.len() == 2 { Some(u16::from_be_bytes([self.value[0], self.value[1]])) } else { None } }
    pub fn as_u32(&self) -> Option<u32> { if self.value.len() == 4 { Some(u32::from_be_bytes([self.value[0], self.value[1], self.value[2], self.value[3]])) } else { None } }
    pub fn as_string(&self) -> Option<String> {
        let bytes = if self.value.last() == Some(&0x00) { &self.value[..self.value.len() - 1] } else { &self.value };
        String::from_utf8(bytes.to_vec()).ok()
    }
}

pub trait TlvList {
    fn get_tlv(&self, tag: TlvTag) -> Option<&Tlv>;
    fn get_tlv_raw(&self, tag: u16) -> Option<&Tlv>;
    fn receipted_message_id(&self) -> Option<String>;
    fn message_state(&self) -> Option<u8>;
    fn network_error_code(&self) -> Option<(u8, u16)>;
    fn user_message_reference(&self) -> Option<u16>;
    fn sar_msg_ref_num(&self) -> Option<u16>;
    fn sar_total_segments(&self) -> Option<u8>;
    fn sar_segment_seqnum(&self) -> Option<u8>;
    fn message_payload(&self) -> Option<&[u8]>;
    fn source_port(&self) -> Option<u16>;
    fn destination_port(&self) -> Option<u16>;
    fn sc_interface_version(&self) -> Option<u8>;
    fn more_messages_to_send(&self) -> Option<u8>;
    fn delivery_failure_reason(&self) -> Option<u8>;
}

impl TlvList for Vec<Tlv> {
    fn get_tlv(&self, tag: TlvTag) -> Option<&Tlv> { self.iter().find(|t| t.tag == tag as u16) }
    fn get_tlv_raw(&self, tag: u16) -> Option<&Tlv> { self.iter().find(|t| t.tag == tag) }
    fn receipted_message_id(&self) -> Option<String> { self.get_tlv(TlvTag::ReceiptedMessageId)?.as_string() }
    fn message_state(&self) -> Option<u8> { self.get_tlv(TlvTag::MessageStateTlv)?.as_u8() }
    fn network_error_code(&self) -> Option<(u8, u16)> {
        let tlv = self.get_tlv(TlvTag::NetworkErrorCode)?;
        if tlv.value.len() == 3 { Some((tlv.value[0], u16::from_be_bytes([tlv.value[1], tlv.value[2]]))) } else { None }
    }
    fn user_message_reference(&self) -> Option<u16> { self.get_tlv(TlvTag::UserMessageReference)?.as_u16() }
    fn sar_msg_ref_num(&self) -> Option<u16> { self.get_tlv(TlvTag::SarMsgRefNum)?.as_u16() }
    fn sar_total_segments(&self) -> Option<u8> { self.get_tlv(TlvTag::SarTotalSegments)?.as_u8() }
    fn sar_segment_seqnum(&self) -> Option<u8> { self.get_tlv(TlvTag::SarSegmentSeqnum)?.as_u8() }
    fn message_payload(&self) -> Option<&[u8]> { self.get_tlv(TlvTag::MessagePayload).map(|t| t.value.as_slice()) }
    fn source_port(&self) -> Option<u16> { self.get_tlv(TlvTag::SourcePort)?.as_u16() }
    fn destination_port(&self) -> Option<u16> { self.get_tlv(TlvTag::DestinationPort)?.as_u16() }
    fn sc_interface_version(&self) -> Option<u8> { self.get_tlv(TlvTag::ScInterfaceVersion)?.as_u8() }
    fn more_messages_to_send(&self) -> Option<u8> { self.get_tlv(TlvTag::MoreMessagesToSend)?.as_u8() }
    fn delivery_failure_reason(&self) -> Option<u8> { self.get_tlv(TlvTag::DeliveryFailureReason)?.as_u8() }
}

fn parse_single_tlv(input: &[u8]) -> IResult<&[u8], Tlv> {
    let (input, tag_bytes) = take(2usize)(input)?;
    let (input, length_bytes) = take(2usize)(input)?;
    let tag = u16::from_be_bytes([tag_bytes[0], tag_bytes[1]]);
    let length = u16::from_be_bytes([length_bytes[0], length_bytes[1]]);
    let (input, value) = take(length as usize)(input)?;
    Ok((input, Tlv { tag, value: value.to_vec() }))
}

pub fn decode_tlvs(mut input: &[u8]) -> Vec<Tlv> {
    let mut tlvs = Vec::new();
    while input.len() >= 4 {
        match parse_single_tlv(input) {
            Ok((remaining, tlv)) => { tlvs.push(tlv); input = remaining; }
            Err(_) => { warn!("Failed to parse TLV, {} bytes remaining", input.len()); break; }
        }
    }
    tlvs
}

pub fn encode_tlvs(tlvs: &[Tlv]) -> Vec<u8> {
    let mut buf = Vec::new();
    for tlv in tlvs { buf.extend_from_slice(&tlv.encode()); }
    buf
}

pub fn tlvs_encoded_len(tlvs: &[Tlv]) -> usize {
    tlvs.iter().map(|t| t.encoded_len()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tlv_encode_decode_roundtrip() {
        let tlv = Tlv::from_tag(TlvTag::UserMessageReference, vec![0x00, 0x42]);
        let encoded = tlv.encode();
        assert_eq!(encoded, vec![0x02, 0x04, 0x00, 0x02, 0x00, 0x42]);
        let decoded = decode_tlvs(&encoded);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].tag, TlvTag::UserMessageReference as u16);
        assert_eq!(decoded[0].as_u16(), Some(0x0042));
    }

    #[test]
    fn test_multiple_tlvs() {
        let tlvs = vec![
            Tlv::from_tag(TlvTag::ScInterfaceVersion, vec![0x34]),
            Tlv::from_tag(TlvTag::ReceiptedMessageId, b"msg123\0".to_vec()),
            Tlv::from_tag(TlvTag::MessageStateTlv, vec![0x02]),
        ];
        let encoded = encode_tlvs(&tlvs);
        let decoded = decode_tlvs(&encoded);
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].as_u8(), Some(0x34));
        assert_eq!(decoded[1].as_string(), Some("msg123".to_string()));
        assert_eq!(decoded[2].as_u8(), Some(0x02));
        assert_eq!(decoded.sc_interface_version(), Some(0x34));
        assert_eq!(decoded.receipted_message_id(), Some("msg123".to_string()));
        assert_eq!(decoded.message_state(), Some(0x02));
    }

    #[test]
    fn test_empty_input() { assert!(decode_tlvs(&[]).is_empty()); }

    #[test]
    fn test_truncated_tlv_ignored() { assert!(decode_tlvs(&[0x02, 0x04, 0x00]).is_empty()); }

    #[test]
    fn test_unknown_tag_preserved() {
        let tlv = Tlv::new(0xFFFF, vec![0x01, 0x02, 0x03]);
        let decoded = decode_tlvs(&tlv.encode());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].tag, 0xFFFF);
        assert_eq!(decoded[0].value, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_sar_tlvs() {
        let tlvs = vec![
            Tlv::from_tag(TlvTag::SarMsgRefNum, vec![0x00, 0x01]),
            Tlv::from_tag(TlvTag::SarTotalSegments, vec![0x03]),
            Tlv::from_tag(TlvTag::SarSegmentSeqnum, vec![0x02]),
        ];
        assert_eq!(tlvs.sar_msg_ref_num(), Some(1));
        assert_eq!(tlvs.sar_total_segments(), Some(3));
        assert_eq!(tlvs.sar_segment_seqnum(), Some(2));
    }

    #[test]
    fn test_network_error_code() {
        let tlvs = vec![Tlv::from_tag(TlvTag::NetworkErrorCode, vec![0x03, 0x00, 0x1F])];
        assert_eq!(tlvs.network_error_code(), Some((3, 31)));
    }

    #[test]
    fn test_message_payload() {
        let tlvs = vec![Tlv::from_tag(TlvTag::MessagePayload, b"Hello World".to_vec())];
        assert_eq!(tlvs.message_payload(), Some(b"Hello World".as_slice()));
    }
}
