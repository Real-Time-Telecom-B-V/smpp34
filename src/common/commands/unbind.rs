use num_traits::FromPrimitive;

use crate::{CommandHeader, SmppError, common::{decode_bind_request, encode_bind_response}, CommandId};

#[derive(Debug, Clone)]
pub struct unbind {
    header: CommandHeader,
}

impl unbind {
    pub fn decode(header: CommandHeader, _pdu: &Vec<u8>) -> Result<unbind, SmppError> {
        // TODO check if body is empty
        Ok(unbind {
            header,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        todo!()
    }

    pub fn accept(self) -> unbind_resp {
        unbind_resp { header: CommandHeader {
            command_length: 16, // No body
            command_id: CommandId::unbind_resp as u32,
            command_status: SmppError::ESME_ROK as u32,
            sequence_number: self.header.sequence_number,
        }}
    }

    pub fn reject(self, error: SmppError) -> unbind_resp {
        unbind_resp { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::unbind_resp as u32,
            command_status: error as u32,
            sequence_number: self.header.sequence_number,
        } }
    }

    pub fn generic_reject(sequence_number: u32, error: SmppError) -> unbind_resp {
        unbind_resp  { header: CommandHeader {
            command_length: 16,
            command_id: CommandId::unbind_resp as u32,
            command_status: error as u32,
            sequence_number,
        }}
    }
}

#[derive(Debug, Clone)]
pub struct unbind_resp {
    header: CommandHeader,

}

impl unbind_resp {

    pub fn is_success(&self) -> bool { self.header.command_status == SmppError::ESME_ROK as u32}
    pub fn command_status(&self) -> u32 { self.header.command_status }
    pub fn get_error(&self) -> SmppError { FromPrimitive::from_u32(self.header.command_status).expect("Can not convert command_status to SmppError") }

    pub fn encode(self) -> Vec<u8> { self.header.encode() }
}