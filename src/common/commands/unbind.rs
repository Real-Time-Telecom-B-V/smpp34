use num_traits::FromPrimitive;

use crate::{CommandHeader, SmppError, CommandId};

/// 
/// The purpose of the SMPP unbind operation is to deregister an instance of an ESME from the
/// SMSC and inform the SMSC that the ESME no longer wishes to use this network connection
/// for the submission or delivery of messages.
/// Thus, the unbind operation may be viewed as a form of SMSC logoff request to close the
/// current SMPP session.
/// 
/// See '4.2 “UNBIND” Operation' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct unbind {
    header: CommandHeader,
}

impl unbind {

    /// Decode a unbind PDU
    /// 
    /// # Arguments
    /// 
    /// * `header` - already decoded Command Header which is only used for validation as unbind should not have a body
    /// * `pdu` - the complete PDU used for extra validation
    pub fn decode(header: CommandHeader, _pdu: &Vec<u8>) -> Result<unbind, SmppError> {
        // TODO check if body is empty
        Ok(unbind {
            header,
        })
    }

    /// Encode a unbind based on inner command header
    pub fn encode(self) -> Vec<u8> {
        self.header.encode()
    }

    /// Create a unbind_resp with ESME_ROK as command_status
    pub fn accept(self) -> unbind_resp {
        unbind_resp { header: CommandHeader {
            command_length: 16, // No body
            command_id: CommandId::unbind_resp as u32,
            command_status: SmppError::ESME_ROK as u32,
            sequence_number: self.header.sequence_number,
        }}
    }

    /// Create a unbind_resp with a specific error as command_status
    /// 
    /// # Arguments
    /// 
    /// * `error` - the error to define as the command_status in the command header
    pub fn reject(self, error: SmppError) -> unbind_resp {
        unbind_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::unbind_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        } }
    }

    /// Create a unbind_resp with given sequence number and a specific error as command_status
    /// 
    /// Can be used if decoding of the unbind PDU fails or some internal error occurs
    /// 
    /// # Arguments
    /// 
    /// * `sequence_number` - the sequence_number to set in the command header
    /// * `error` - the error to define as the command_status in the command header
    pub fn generic_reject(sequence_number: u32, error: SmppError) -> unbind_resp {
        unbind_resp  { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::unbind_resp as u32,
            command_status: error as u32,
            sequence_number,
        }}
    }
}


/// The SMPP unbind_resp PDU is used to reply to an unbind request. It comprises the SMPP
/// message header only.
/// 
/// See '4.2.2 “UNBIND_RESP”' at <https://smpp.org/SMPP_v3_4_Issue1_2.pdf>
#[derive(Debug, Clone)]
pub struct unbind_resp {
    header: CommandHeader,

}

impl unbind_resp {

    /// Helper to detect whether the unbind_resp is successful (i.e. has status ESME_ROK)
    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    
    /// Get inner command_status
    pub fn command_status(&self) -> u32 { self.header.command_status }

    /// Convert inner command_status into an SmppError enum
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    /// Encode a unbind_resp based on inner command header
    pub fn encode(self) -> Vec<u8> { self.header.encode() }
}

#[cfg(test)]
mod unbind_tests {

}

#[cfg(test)]
mod unbind_resp_tests {

}