use std::{net::TcpStream, io::{BufReader, Write, Read}, sync::Arc};

use log::{info, error};

use crate::{common::SmppError, bind_transmitter, bind_receiver_resp, bind_receiver, bind_transceiver_resp, bind_transceiver, bind_transmitter_resp, CommandHeader, CommandId, SmppServerListener, submit_sm, unbind};

use super::SmppConnectionInformation;

///
/// OPEN (Connected and Bind Pending)
/// An ESME has established a network connection to the SMSC but has not yet issued a
/// Bind request.
///
pub (crate) struct OPEN {
}

impl OPEN {
    pub(crate) fn bind_transmitter(self, reader: BufReader<TcpStream>, bind_transmitter: bind_transmitter, bind_transmitter_resp: &bind_transmitter_resp, connection_information: &SmppConnectionInformation, handler: Arc<SmppServerListener>) -> Result<BOUND_TX, SmppError> {
        let mut stream = reader.into_inner();
        if bind_transmitter_resp.is_success() {
            let result = stream.write(&bind_transmitter_resp.clone().encode());
            if result.is_ok() {
                let new_state = BOUND_TX {
                    stream,
                    system_id: bind_transmitter.system_id,
                    handler: handler.clone(),
                    connection_information: connection_information.clone(),
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

    pub(crate) fn bind_receiver(self, reader: BufReader<TcpStream>, bind_receiver: bind_receiver, bind_receiver_resp: bind_receiver_resp, connection_information: &SmppConnectionInformation, handler: Arc<SmppServerListener>) -> Result<BOUND_RX, SmppError> {
        if bind_receiver_resp.is_success() {
            let mut stream = reader.into_inner();
            let result = stream.write(&bind_receiver_resp.encode());
            if result.is_ok() {
                let new_state = BOUND_RX {
                    stream,
                    system_id: bind_receiver.system_id,
                    handler: handler.clone(),
                    connection_information: connection_information.clone(),
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

    pub(crate) fn bind_transceiver(self, reader: BufReader<TcpStream>, bind_transceiver: bind_transceiver, bind_transceiver_resp: &bind_transceiver_resp, connection_information: &SmppConnectionInformation, handler: Arc<SmppServerListener>) -> Result<BOUND_TRX, SmppError> {
        let mut stream = reader.into_inner();
        if bind_transceiver_resp.is_success() {
            let result = stream.write(&bind_transceiver_resp.clone().encode());
            if result.is_ok() {
                let new_state = BOUND_TRX {
                    stream,
                    system_id: bind_transceiver.system_id,
                    handler: handler.clone(),
                    connection_information: connection_information.clone(),
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
    handler: Arc<SmppServerListener>,
    connection_information: SmppConnectionInformation,
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
    handler: Arc<SmppServerListener>,
    connection_information: SmppConnectionInformation,
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
    handler: Arc<SmppServerListener>,
    connection_information: SmppConnectionInformation,
}

impl BOUND_TRX {
    pub fn read_loop(mut self) -> CLOSED {
        info!("BOUND_TRX going into read_loop");
        let mut line;
        loop {
            line = [0; 1024];
            let result = self.stream.read(&mut line);
            match result {
                Ok(n) => {
                    let pdu = line[0..n].to_vec();
                    let pdu_length = pdu.len();

                    // Try read sequence_number in case we need a generic_nack.
                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                    let potential_seq_no = if pdu_length >= 16 { u32::from_be_bytes(pdu[12..16].try_into().expect("Can not read sequence_number")) } else { 0 };
                    let command_header = CommandHeader::decode(&pdu);

                    match command_header {
                        Ok(header) => {
                            if header.command_id == CommandId::submit_sm as u32 {
                                match submit_sm::decode(header, &pdu) {
                                    Ok(submit_sm) => {
                                        let submit_sm_resp = (self.handler.on_submit_sm)(submit_sm.clone(), &self.connection_information);
                                        self.stream.write(&submit_sm_resp.encode()).expect("Can not write to stream");
                                        
                                    },
                                    Err(error) => {
                                        error!("Connection from {} on server {}, unable to decode submit_sm", self.connection_information.client_address, self.connection_information.server_address);
                                        let error = submit_sm::generic_reject(potential_seq_no, error).encode();
                                        self.stream.write(&error).expect("Can not write to stream");
                                    }
                                }
                            } else if header.command_id == CommandId::unbind as u32 {
                                match unbind::decode(header, &pdu) {
                                    Ok(unbind) => {
                                        let unbind_resp = (self.handler.on_unbind)(unbind.clone(), &self.connection_information);
                                        self.stream.write(&unbind_resp.encode()).expect("Can not write to stream");
                                        
                                    },
                                    Err(error) => {
                                        error!("Connection from {} on server {}, unable to decode submit_sm", self.connection_information.client_address, self.connection_information.server_address);
                                        let error = unbind::generic_reject(potential_seq_no, error).encode();
                                        self.stream.write(&error).expect("Can not write to stream");
                                    }
                                }

                                // Regardless of the outcome let's break the TCP connection
                                self.stream.shutdown(std::net::Shutdown::Both).expect("Unable to close TCP connection");
                                break; 

                            } else {
                                error!("Did not expect command_id {} for this bind, sending ESME_RINVBNDSTS in generick_nack", header.command_id);
                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: SmppError::ESME_RINVBNDSTS as u32, sequence_number: potential_seq_no };
                                self.stream.write(&generic_nack.encode()).expect("Can not write to stream");
                            }
                        },
                        Err(error) => {
                            error!("Unable to decode command_header for PDU, sending {:?} in generic_nack", error); 
                            let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                            self.stream.write(&generic_nack.encode()).expect("Can not write to stream");
                        } 
                    }
                },
                _ => break,
            }
        }

        info!("BOUND_TRX going to CLOSED state");
        CLOSED {  }
    }
}

pub (crate) struct CLOSED {
    
}