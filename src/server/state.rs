use std::{io::{BufReader, Write, Read, self}, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering, AtomicU32}}, thread, time::Duration, cell::RefCell};

use futures::executor::block_on;
use log::{info, error};
use tokio::{net::TcpStream, io::{AsyncWriteExt, AsyncReadExt}, time::{interval, timeout}};

use crate::{common::SmppError, bind_transmitter, bind_receiver_resp, bind_receiver, bind_transceiver_resp, bind_transceiver, bind_transmitter_resp, CommandHeader, CommandId, SmppServerListener, submit_sm, unbind, outbind, enquire_link, enquire_link_resp};

use super::SmppConnectionInformation;

///
/// OPEN (Connected and Bind Pending)
/// An ESME has established a network connection to the SMSC but has not yet issued a
/// Bind request.
///
pub (crate) struct OPEN {
}

impl OPEN {
    pub(crate) async fn bind_transmitter(self, mut stream: TcpStream, bind_transmitter: bind_transmitter, bind_transmitter_resp: &bind_transmitter_resp, connection_information: &SmppConnectionInformation, handler: Arc<SmppServerListener>) -> Result<BOUND_TX, SmppError> {
        if bind_transmitter_resp.is_success() {
            let result = stream.write(&bind_transmitter_resp.clone().encode()).await;
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
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            Err(bind_transmitter_resp.get_error())
        }
    }

    pub(crate) async fn bind_receiver(self, mut stream: TcpStream, bind_receiver: bind_receiver, bind_receiver_resp: bind_receiver_resp, connection_information: &SmppConnectionInformation, handler: Arc<SmppServerListener>) -> Result<BOUND_RX, SmppError> {
        if bind_receiver_resp.is_success() {
            let result = stream.write(&bind_receiver_resp.encode()).await;
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
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            Err(bind_receiver_resp.get_error())
        }
    }

    pub(crate) async fn bind_transceiver(self, mut stream: TcpStream, bind_transceiver: bind_transceiver, bind_transceiver_resp: &bind_transceiver_resp, connection_information: &SmppConnectionInformation, handler: Arc<SmppServerListener>) -> Result<BOUND_TRX, SmppError> {
        if bind_transceiver_resp.is_success() {
            let result = stream.write(&bind_transceiver_resp.clone().encode()).await;
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
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            error!("Connection from {} on server {} with system_id {} was rejected with error {:?}, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_transceiver.system_id, bind_transceiver_resp.get_error()) ;
            stream.write(&bind_transceiver_resp.clone().encode()).await.expect("Unable to write to TCP socket");
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
    pub async fn read_loop(self, enquire_link_timer: u64, inactivity_timer: u64) -> CLOSED {
        info!("BOUND_TRX going into read_loop with enquire_link_timer {}ms and inactivity_timer {}ms", enquire_link_timer, inactivity_timer);
        let sequence_number = Arc::new(AtomicU32::new(1));
        let alive = Arc::new(AtomicBool::new(false));
        alive.store(true, Ordering::SeqCst);

        let mut buffer = [0; 1024];
        let (mut reader, writer) = self.stream.into_split();        
        let writer = Arc::new(Mutex::new(writer));
 
        let send_enquire_link = alive.clone();
        let enquire_link_writer = writer.clone();
        let enquire_link_sequence_number = sequence_number.clone();
        tokio::task::spawn(async move {
            info!("enquire_link timer for {} on server {} started, sending every {}ms", self.connection_information.client_address, self.connection_information.server_address, enquire_link_timer);
            let mut interval = interval(Duration::from_millis(enquire_link_timer));
            interval.tick().await; // tick for the first time to warm the timer
            while send_enquire_link.load(Ordering::SeqCst) {
                let sequence_number = enquire_link_sequence_number.fetch_add(1, Ordering::SeqCst);
                info!("enquire_link to {} on server {} with sequence_number {}", self.connection_information.client_address, self.connection_information.server_address, sequence_number);
                block_on(enquire_link_writer.lock().unwrap().write(&enquire_link::new(sequence_number).encode())).expect("Unable to send enquire_link");
                interval.tick().await;

                // TODO we need to implement response_timer!! Do we need to record outstanding operations?!
            }
            info!("enquire_link timer for {} on server {} stopped", self.connection_information.client_address, self.connection_information.server_address);
        });

        let inactivity_timer = tokio::time::Duration::from_millis(inactivity_timer);

        loop {
            
            let result = timeout(inactivity_timer, reader.read(&mut buffer)).await;
            match result {
                Ok(Ok(n)) => {
                    let pdu = buffer[0..n].to_vec();
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
                                        let writer = writer.clone();
                                        let handler = self.handler.clone();
                                        let connection_information = self.connection_information.clone();
                                        tokio::task::spawn_blocking( move || {
                                            let submit_sm_resp = (handler.on_submit_sm)(submit_sm.clone(), &connection_information);
                                            block_on(writer.lock().unwrap().write(&submit_sm_resp.encode())).expect("Can not write to stream");
                                        });
                                    },
                                    Err(error) => {
                                        error!("Connection from {} on server {}, unable to decode submit_sm", self.connection_information.client_address, self.connection_information.server_address);
                                        let error = submit_sm::generic_reject(potential_seq_no, error).encode();
                                        writer.lock().unwrap().write(&error).await.expect("Unable to write to stream");
                                    }
                                }
                            } else if header.command_id == CommandId::enquire_link as u32 {
                                match enquire_link::decode(header, &pdu) {
                                    Ok(enquire_link) => {
                                        info!("enquire_link from {} on server {} with sequence_number {}", self.connection_information.client_address, self.connection_information.server_address, potential_seq_no);
                                        let enquire_link_resp = enquire_link.accept();
                                        writer.lock().unwrap().write(&enquire_link_resp.encode()).await.expect("Unable to write to stream");
                                    },
                                    Err(error) => {
                                        error!("Connection from {} on server {}, unable to decode enquire_link", self.connection_information.client_address, self.connection_information.server_address);
                                        let error = submit_sm::generic_reject(potential_seq_no, error).encode();
                                        writer.lock().unwrap().write(&error).await.expect("Unable to write to stream");
                                    }
                                }
                            } else if header.command_id == CommandId::enquire_link_resp as u32 {
                                // Just log! We do not need to handle this as there is a timer on the socket read this already triggers inactivity_timeout
                                info!("enquire_link_resp from {} on server {}", self.connection_information.client_address, self.connection_information.server_address);

                                // TODO however we should verify if we got an answer??!?!
                                
                            } else if header.command_id == CommandId::unbind as u32 {
                                match unbind::decode(header, &pdu) {
                                    Ok(unbind) => {
                                        let unbind_resp = (self.handler.on_unbind)(unbind.clone(), &self.connection_information);
                                        writer.lock().unwrap().write(&unbind_resp.encode()).await.expect("Unable to write to stream");
                                    },
                                    Err(error) => {
                                        error!("Connection from {} on server {}, unable to decode submit_sm", self.connection_information.client_address, self.connection_information.server_address);
                                        let error = unbind::generic_reject(potential_seq_no, error).encode();
                                        writer.lock().unwrap().write(&error).await.expect("Unable to write to stream");
                                    }
                                }
                                break; 
                            } else {
                                error!("Did not expect command_id {} for this bind, sending ESME_RINVBNDSTS in generick_nack", header.command_id);
                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: SmppError::ESME_RINVBNDSTS as u32, sequence_number: potential_seq_no };
                                writer.lock().unwrap().write(&generic_nack.encode()).await.expect("Unable to write to stream");
                            }
                        },
                        Err(error) => {
                            error!("Unable to decode command_header for PDU, sending {:?} in generic_nack", error); 
                            let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                            writer.lock().unwrap().write(&generic_nack.encode()).await.expect("Unable to write to stream");
                        } 
                    }
                },
                Err(_e) => {
                    error!("inactivity_timer triggered, closing TCP connection");
                    writer.lock().unwrap().shutdown().await.expect("Unable to close TCP connection");
                    break
                },
                Ok(Err(e)) => {
                    error!("{} ", e);
                    break
                },
            }
        }

        info!("BOUND_TRX going to CLOSED state");
        alive.store(false, Ordering::SeqCst);

        CLOSED {  }
    }
}

pub (crate) struct CLOSED {
    
}