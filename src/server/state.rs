use std::{net::TcpStream, io::{BufReader, Write, self, BufRead}};

use log::{info, error};

use crate::{common::SmppError, bind_transmitter, bind_receiver_resp, bind_receiver, bind_transceiver_resp, bind_transceiver, bind_transmitter_resp, CommandHeader, CommandId};

use super::SmppConnectionInformation;

///
/// OPEN (Connected and Bind Pending)
/// An ESME has established a network connection to the SMSC but has not yet issued a
/// Bind request.
///
pub (crate) struct OPEN {
}

impl OPEN {
    pub(crate) fn bind_transmitter(self, reader: BufReader<TcpStream>, bind_transmitter: bind_transmitter, bind_transmitter_resp: &bind_transmitter_resp, connection_information: &SmppConnectionInformation) -> Result<BOUND_TX, SmppError> {
        let mut stream = reader.into_inner();
        if bind_transmitter_resp.is_success() {
            let result = stream.write(&bind_transmitter_resp.clone().encode());
            if result.is_ok() {
                let new_state = BOUND_TX {
                    stream,
                    system_id: bind_transmitter.system_id
                };
                info!("Connection from {} on server {} with system_id {} went to state BOUND_TX", connection_information.client_address, connection_information.server_address, new_state.system_id);
                Ok(new_state)
            } else {
                error!("Connection from {} on server {} with system_id {} unable to transistion to state BOUND_TX, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_transmitter.system_id);
                stream.shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            Err(bind_transmitter_resp.get_error())
        }
    }

    pub(crate) fn bind_receiver(self, reader: BufReader<TcpStream>, bind_receiver: bind_receiver, bind_receiver_resp: bind_receiver_resp, connection_information: &SmppConnectionInformation) -> Result<BOUND_RX, SmppError> {
        if bind_receiver_resp.is_success() {
            let mut stream = reader.into_inner();
            let result = stream.write(&bind_receiver_resp.encode());
            if result.is_ok() {
                let new_state = BOUND_RX {
                    stream,
                    system_id: bind_receiver.system_id
                };
                info!("Connection from {} on server {} with system_id {} went to state BOUND_RX", connection_information.client_address, connection_information.server_address, new_state.system_id);
                Ok(new_state)
            } else {
                error!("Connection from {} on server {} with system_id {} unable to transistion to state BOUND_RX, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_receiver.system_id);
                stream.shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            Err(bind_receiver_resp.get_error())
        }
    }

    pub(crate) fn bind_transceiver(self, reader: BufReader<TcpStream>, bind_transceiver: bind_transceiver, bind_transceiver_resp: &bind_transceiver_resp, connection_information: &SmppConnectionInformation) -> Result<BOUND_TRX, SmppError> {
        let mut stream = reader.into_inner();
        if bind_transceiver_resp.is_success() {
            let result = stream.write(&bind_transceiver_resp.clone().encode());
            if result.is_ok() {
                let new_state = BOUND_TRX {
                    stream,
                    system_id: bind_transceiver.system_id
                };
                info!("Connection from {} on server {} with system_id {} went to state BOUND_TRX", connection_information.client_address, connection_information.server_address, new_state.system_id);
                Ok(new_state)
            } else {
                error!("Connection from {} on server {} with system_id {} unable to transistion to state BOUND_TRX, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_transceiver.system_id);
                stream.shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            error!("Connection from {} on server {} with system_id {} was rejected with error {:?}, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_transceiver.system_id, bind_transceiver_resp.get_error()) ;
            stream.write(&bind_transceiver_resp.clone().encode()).expect("Unable to write to TCP socket");
            stream.shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");
            Err(bind_transceiver_resp.get_error())
        }
    }
}

pub (crate) struct BOUND_TX {
    stream: TcpStream,
    system_id: String,
}

impl BOUND_TX {
    pub fn read_loop(self) -> CLOSED {
        info!("BOUND_TX going into read_loop");
        info!("BOUND_TX going to CLOSED state");
        self.stream.shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");

        CLOSED {  }
    }
}

pub (crate) struct BOUND_RX {
    stream: TcpStream,
    system_id: String,
}

impl BOUND_RX {
    pub fn read_loop(self) -> CLOSED {
        info!("BOUND_RX going into read_loop");
        info!("BOUND_RX going to CLOSED state");
        self.stream.shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");

        CLOSED {  }
    }
}


pub (crate) struct BOUND_TRX {
    stream: TcpStream,
    system_id: String,
}

impl BOUND_TRX {
    pub fn read_loop(self) -> CLOSED {
        info!("BOUND_TRX going into read_loop");
        let mut reader = io::BufReader::new(self.stream);

        //loop { // TODO allow thread to be interrupted!
            match reader.fill_buf() {
                Ok(bytes) => {
                    let pdu = bytes.to_vec();
                    let pdu_length = pdu.len();

                    // Try read sequence_number in case we need a generic_nack.
                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                    let potential_seq_no = if pdu_length >= 16 { u32::from_be_bytes(pdu[12..16].try_into().expect("Can not read sequence_number")) } else { 0 };
                    let command_header = CommandHeader::decode(&pdu);

                    match command_header {
                        Ok(header) => {
                            if header.command_id == CommandId::submit_sm as u32 {
                                info!("Rejecting submit_sm for the fun of it");
                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: SmppError::ESME_RINVBNDSTS as u32, sequence_number: potential_seq_no };
                                reader.into_inner().write(&generic_nack.encode()).expect("Can not write to stream");
                            } else {
                                error!("Did not expect command_id {} for this bind, sending ESME_RINVBNDSTS in generick_nack", header.command_id);
                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: SmppError::ESME_RINVBNDSTS as u32, sequence_number: potential_seq_no };
                                reader.into_inner().write(&generic_nack.encode()).expect("Can not write to stream");
                            }
                        },
                        Err(error) => {
                            error!("Unable to decode command_header for PDU, sending {:?} in generic_nack", error); 
                            let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                            reader.into_inner().write(&generic_nack.encode()).expect("Can not write to stream");
                        } 
                    }
                },
                Err(_error) => todo!(),
            }
        //}
    

        info!("BOUND_TRX going to CLOSED state");
        //reader.into_inner().shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");

        CLOSED {  }
    }
}

pub (crate) struct CLOSED {
    
}