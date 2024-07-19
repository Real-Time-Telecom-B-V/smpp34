use std::{net::{IpAddr, SocketAddr}, sync::{atomic::{AtomicBool, Ordering, AtomicU32}, Arc, mpsc::Sender, Mutex}};
use std::collections::HashSet;
use futures::executor::block_on;
use log::{info, error};
use tokio::{task::{JoinHandle, self}, net::TcpListener, io::{AsyncReadExt, AsyncWriteExt}, time::timeout};
use uuid::Uuid;

use crate::{server::state::OPEN, common::{CommandHeader, CommandId, SmppError}, bind_transmitter, bind_transmitter_resp, bind_transceiver, bind_receiver, bind_receiver_resp, unbind, unbind_resp, bind_transceiver_resp, submit_sm_resp, submit_sm, generic_nack, deliver_sm, alert_notification, data_sm, SmppConnectionInformation, deliver_sm_resp, data_sm_resp, WriteFrame};

mod state;

pub struct SmppServer {
    address: IpAddr,
    port: u16,
    handle: Option<JoinHandle<()>>,
    alive: Arc<AtomicBool>,
    handler: Arc<SmppServerListener>,
    session_init_timer: u64,
    enquire_link_timer: u64,
    inactivity_timer: u64,
    response_timer: u64,
    buffer_size: usize,
    esmes: HashSet<ESME>
}


pub struct ESME {
    can_receive: bool,
    tx_channel: Sender<WriteFrame>,
    sequence_number: Arc<AtomicU32>,
}

impl ESME {

    fn next_sequence_number(&self) -> u32 {
        self.sequence_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn send_deliver_sm(&self, service_type: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, 
        dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, protocol_id: u8, priority_flag: u8, 
        schedule_delivery_time: String, validity_period: String, registered_delivery: u8, replace_if_present_flag: u8, 
        data_coding: u8, sm_default_msg_id: u8, short_message: String) -> u32 {
        if self.can_receive {
            let sequence_number = self.next_sequence_number();
            let deliver_sm = deliver_sm::new(sequence_number.clone(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, protocol_id, priority_flag, schedule_delivery_time, validity_period, registered_delivery, replace_if_present_flag, data_coding, sm_default_msg_id, short_message);
            self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: deliver_sm.encode() }).expect("Unable to send deliver_sm request to writer thread");
            sequence_number
        } else {
            panic!("Can not send deliver_sm on non RX/TRX bind");
        }
    }

    pub fn send_unbind(&self) -> u32 {
        let sequence_number = self.next_sequence_number();
        let unbind = unbind::with_sequence_number(sequence_number.clone());
        self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: unbind.encode() }).expect("Unable to send unbind request to writer thread");
        sequence_number
    }

    pub fn send_data_sm(&self, service_type: String, source_addr_ton: u8,  source_addr_npi: u8, source_addr: String,  dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, registered_delivery: u8, data_coding: u8) -> u32 {
        let sequence_number = self.next_sequence_number();
        let data_sm = data_sm::new(sequence_number.clone(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, registered_delivery, data_coding);
        self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: data_sm.encode() }).expect("Unable to send data_sm request to writer thread");
        sequence_number
    }

    pub fn send_alert_notification(&self, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, esme_addr_ton: u8, esme_addr_npi: u8, esme_addr: String, ms_availability_status: Option<u8>) -> u32 {
        if self.can_receive {
            let sequence_number = self.next_sequence_number();
            let alert_notification = alert_notification::new(sequence_number.clone(), source_addr_ton, source_addr_npi, source_addr, esme_addr_ton, esme_addr_npi, esme_addr, ms_availability_status);
            self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: alert_notification.encode() }).expect("Unable to send alert_notification request to writer thread");
            sequence_number
        } else {
            panic!("Can not send alert_notification on non RX/TRX bind");
        }
    }
}

pub struct SmppServerListener {
    pub on_bind_transmitter: fn(bind_transmitter, &SmppConnectionInformation, session_id: &String) -> bind_transmitter_resp,
    pub on_bind_receiver: fn(bind_receiver, &SmppConnectionInformation, session_id: &String) -> bind_receiver_resp,
    pub on_bind_transceiver: fn(bind_transceiver, &SmppConnectionInformation, session_id: &String) -> bind_transceiver_resp,
    pub on_unbind: fn(unbind, &SmppConnectionInformation, session_id: &String) -> unbind_resp,
    pub on_submit_sm: fn(submit_sm, &SmppConnectionInformation, session_id: &String) ->  submit_sm_resp,

    pub on_deliver_sm_resp: fn(deliver_sm_resp, &SmppConnectionInformation, session_id: &String),
    pub on_data_sm_resp: fn(data_sm_resp, &SmppConnectionInformation, session_id: &String),

    
    /// Notification sent when an SMPP command timed-out (respone_timer triggered)
    pub on_timeout: fn(sequence_number: u32, session_id: &String),

    /// Notification sent when an ESME is in bound state and is ready for receiving commands. 
    /// The ESME wraps the MPSC channel towards the writer thread of the bind
    pub on_esme_bound: fn(esme: ESME, session_id: &String),

    /// Notification sent when the ESME has become unavailable due to a bind being closed or transport error
    /// It is up to the user of this listener to drop the ESME received on the on_esme_bound notificiation, any attempt to write to the ESME after will result in a panic as the MSPC channel is closed
    pub on_esme_unbound: fn(session_id: &String)
}

impl SmppServer {

    pub fn new(address: IpAddr, port: u16, handler: Arc<SmppServerListener>) -> SmppServer {
        SmppServer::new_with_default_timers(address, port, handler, 5000, 30000, 60000, 2000, 1500)
    } 

    pub fn new_with_default_timers(address: IpAddr, port: u16, handler: Arc<SmppServerListener>, session_init_timer: u64, enquire_link_timer: u64, inactivity_timer: u64, response_timer: u64, buffer_size: usize) -> SmppServer {
        SmppServer { address, port, handle: None, alive: Arc::new(AtomicBool::new(false)), handler, session_init_timer, enquire_link_timer, inactivity_timer, response_timer, buffer_size, esmes: HashSet::new() }
    } 

    pub fn start(&mut self) {

        if self.alive.load(Ordering::SeqCst) {
            panic!("Can not start server twice")
        }

        info!("Starting smpp server on {}:{}", self.address, self.port);
        self.alive.store(true, Ordering::SeqCst);

        let server_socket_address = SocketAddr::new(self.address, self.port); // Will be moved out
        let alive = self.alive.clone();
        let handler = self.handler.clone();
        let session_init_timer = self.session_init_timer;
        let enquire_link_timer = self.enquire_link_timer;
        let response_timer = self.response_timer;
        let inactivity_timer = self.inactivity_timer;
        let buffer_size: usize = self.buffer_size;

        self.handle = Some(tokio::spawn(async move {
            let listener = TcpListener::bind(server_socket_address).await.unwrap();

            while alive.load(Ordering::SeqCst) {
                loop {
                    let (mut stream, client_socket_address) = listener.accept().await.unwrap();
                    if alive.load(Ordering::SeqCst) {
                        let handler = handler.clone();
                        let session_init_timer_duration = tokio::time::Duration::from_millis(session_init_timer);
                        task::spawn_blocking (move || {
                            let session_id = Uuid::new_v4().to_string();
                            let session_state = OPEN { session_id };
                            let connection_information = SmppConnectionInformation {
                                server_address: server_socket_address,
                                client_address: client_socket_address,
                            };
                            
                            info!("Got a connection from {} on server {}, waiting {}ms for bind", connection_information.client_address, connection_information.server_address, session_init_timer);
                            let mut buffer = [0; 1024]; // Not using BytesMut here as we always first get a bind before expecting big traffic so choose a low buffer size
                            let first_read = block_on(timeout(session_init_timer_duration, stream.read(&mut buffer)));

                            match first_read {
                                Ok(Ok(n)) => {
                                    let pdu = buffer[0..n].to_vec();
                                    let pdu_length = pdu.len();

                                    // Try read sequence_number in case we need a generic_nack.
                                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                                    let potential_seq_no = if pdu_length >= 16 { u32::from_be_bytes(pdu[12..16].try_into().expect("Can not read sequence_number")) } else { 0 };
                                    let command_header = CommandHeader::decode(&pdu);

                                    match command_header {
                                        Ok(header) => {
                                            if header.command_id == CommandId::bind_receiver as u32 {
                                                match bind_receiver::decode(header, &pdu) {
                                                    Ok(bind_receiver) => {
                                                        let bind_receiver_resp = (handler.on_bind_receiver)(bind_receiver.clone(), &connection_information, &session_state.session_id);
                                                        let session_state = block_on(session_state.bind_receiver(stream, bind_receiver, bind_receiver_resp, &connection_information, handler));
                                                        // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                        if session_state.is_ok() {
                                                            let state = session_state.unwrap();
                                                            block_on(state.read_loop(enquire_link_timer, inactivity_timer, response_timer, buffer_size)); // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                        } 
                                                    },
                                                    Err(error) => {
                                                        error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                        let error = bind_receiver::generic_reject(potential_seq_no, error).encode();
                                                        block_on(stream.write(&error)).expect("Can not write to stream");
                                                    }
                                                }
                                            } else if header.command_id == CommandId::bind_transmitter as u32 {
                                                match bind_transmitter::decode(header, &pdu) {
                                                    Ok(bind_transmitter) => {
                                                        let bind_transmitter_resp = (handler.on_bind_transmitter)(bind_transmitter.clone(), &connection_information, &session_state.session_id);
                                                        let session_state = block_on(session_state.bind_transmitter(stream, bind_transmitter, &bind_transmitter_resp, &connection_information, handler));
                                                        // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                        if session_state.is_ok() {
                                                            let state = session_state.unwrap();
                                                            block_on(state.read_loop(enquire_link_timer, inactivity_timer, response_timer, buffer_size)); // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                    } 
                                                    },
                                                    Err(error) => {
                                                        error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                        let error = bind_transmitter::generic_reject(potential_seq_no, error).encode();
                                                        block_on(stream.write(&error)).expect("Can not write to stream");
                                                    }
                                                }
                                            } else if header.command_id == CommandId::bind_transceiver as u32 {
                                                match bind_transceiver::decode(header, &pdu) {
                                                    Ok(bind_transceiver) => {
                                                        let bind_transceiver_resp = (handler.on_bind_transceiver)(bind_transceiver.clone(), &connection_information, &session_state.session_id);
                                                        let session_state = block_on(session_state.bind_transceiver(stream, bind_transceiver, &bind_transceiver_resp, &connection_information, handler));
                                                        // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                        if session_state.is_ok() {
                                                            let state = session_state.unwrap();
                                                            block_on(state.read_loop(enquire_link_timer, inactivity_timer, response_timer, buffer_size)); // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                        } 
                                                    },
                                                    Err(error) => {
                                                        error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                        let error = bind_transceiver::generic_reject(potential_seq_no, error).encode();
                                                        block_on(stream.write(&error)).expect("Can not write to stream");
                                                    }
                                                }
                                            } else {
                                                // Only allow bind commands, if not a bind command tell ESME about invalid bind status
                                                error!("Did not expect command_id {} as bind not established yet, sending ESME_RINVBNDSTS in generick_nack", header.command_id);

                                                let generic_nack = generic_nack::new(SmppError::ESME_RINVBNDSTS, potential_seq_no);
                                                block_on(stream.write(&generic_nack.encode())).expect("Can not write to stream");
                                            }
                                        },
                                        Err(error) => {
                                            error!("Unable to decode command_header for PDU, sending {:?} in generic_nack", error); 
                                            let generic_nack = generic_nack::new(error, potential_seq_no);
                                            block_on(stream.write(&generic_nack.encode())).expect("Can not write to stream");
                                        } 
                                    }
                                }, _ => {
                                    error!("Unable to read initial SMPP PDU from {} on server {}, after waiting {}ms for bind, TCP connection will be closed", connection_information.client_address, connection_information.server_address, session_init_timer);
                                }
                            }
                        });
                    } else {
                        break;
                    }
                } 
            }
        }));

    }

    pub fn stop(&mut self) {

        // TODO send unbind!!

        
        info!("Stopping smpp server");
        self.alive.store(false, Ordering::SeqCst);
        self.handle
            .take().expect("Called stop on non-running thread")
            .abort();
    }
}



impl Drop for SmppServer {
    fn drop(&mut self) {
        if self.alive.load(Ordering::SeqCst) {
            self.stop();
        }
    }
}