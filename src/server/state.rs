use std::net::TcpStream;

use crate::common::SmppError;

pub (crate) struct OPEN {
    pub stream: TcpStream
}

impl OPEN {
    pub(crate) fn bind_transmitter(self, pdu: &Vec<u8>) -> Result<BOUND_TX, SmppError> {
        todo!()
    }

    pub(crate) fn bind_receiver(self, pdu: &Vec<u8>) -> Result<BOUND_RX, SmppError> {
        todo!()
    }

    pub(crate) fn bind_transceiver(self, pdu: &Vec<u8>) -> Result<BOUND_TRX, SmppError> {
        todo!()
    }
}

trait Bound {
    fn handle_unbind() -> Result<CLOSED, SmppError>; // We always expect to end at CLOSED state
    fn unbind()  -> Result<CLOSED, SmppError>; // We always expect to end at CLOSED state
    fn handle_pdu(pdu: Vec<u8>) -> Result<Vec<u8>, SmppError>;
}

pub (crate) struct BOUND_TX {
    stream: TcpStream
}

impl Bound for BOUND_TX {
    fn handle_unbind() -> Result<CLOSED, SmppError> {
        todo!()
    }

    fn unbind() -> Result<CLOSED, SmppError> {
        todo!()
    }

    fn handle_pdu(pdu: Vec<u8>) -> Result<Vec<u8>, SmppError> {
        todo!()
    }
}

pub (crate) struct BOUND_RX {
    stream: TcpStream
}

impl Bound for BOUND_RX {
    fn handle_unbind() -> Result<CLOSED, SmppError> {
        todo!()
    }

    fn unbind() -> Result<CLOSED, SmppError> {
        todo!()
    }

    fn handle_pdu(pdu: Vec<u8>) -> Result<Vec<u8>, SmppError> {
        todo!()
    }
}

pub (crate) struct BOUND_TRX {
    stream: TcpStream
}

impl Bound for BOUND_TRX {
    fn handle_unbind() -> Result<CLOSED, SmppError> {
        todo!()
    }

    fn unbind() -> Result<CLOSED, SmppError> {
        todo!()
    }

    fn handle_pdu(pdu: Vec<u8>) -> Result<Vec<u8>, SmppError> {
        todo!()
    }
}

pub (crate) struct CLOSED {
    
}