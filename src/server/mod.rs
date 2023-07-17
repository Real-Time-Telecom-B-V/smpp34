use std::{net::{IpAddr, TcpListener, SocketAddr, Shutdown}, thread::{self}, time::Duration, sync::{atomic::{AtomicBool, Ordering}, Arc}, io::{self, BufRead, Write}};

use log::{info, error};
use tokio::task::{JoinHandle, self};

use crate::{server::state::OPEN, common::{CommandHeader, CommandId, SmppError}, bind_transmitter, bind_transmitter_resp, bind_transceiver, bind_receiver, bind_receiver_resp, unbind, unbind_resp, bind_transceiver_resp, submit_sm_resp, submit_sm, generic_nack};


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
}
#[derive(Debug, Clone)]
pub struct SmppConnectionInformation {
    pub server_address: SocketAddr,
    pub client_address: SocketAddr,
}

pub struct SmppServerListener {
    pub on_bind_transmitter: fn(bind_transmitter, &SmppConnectionInformation) -> bind_transmitter_resp,
    pub on_bind_receiver: fn(bind_receiver, &SmppConnectionInformation) -> bind_receiver_resp,
    pub on_bind_transceiver: fn(bind_transceiver, &SmppConnectionInformation) -> bind_transceiver_resp,
    pub on_unbind: fn(unbind, &SmppConnectionInformation) -> unbind_resp,
    pub on_submit_sm: fn(submit_sm, &SmppConnectionInformation) ->  submit_sm_resp,
}

// See https://stackoverflow.com/a/42044143
impl SmppServer {

    pub fn new(address: IpAddr, port: u16, handler: Arc<SmppServerListener>) -> SmppServer {
        SmppServer::new_with_default_timers(address, port, handler, 5000, 30000, 60000, 2000)
    } 

    pub fn new_with_default_timers(address: IpAddr, port: u16, handler: Arc<SmppServerListener>, session_init_timer: u64, enquire_link_timer: u64, inactivity_timer: u64, response_timer: u64,) -> SmppServer {
        SmppServer { address, port, handle: None, alive: Arc::new(AtomicBool::new(false)), handler, session_init_timer, enquire_link_timer, inactivity_timer, response_timer }
    } 

    pub fn start(&mut self) {

        if self.alive.load(Ordering::SeqCst) {
            panic!("Can not start server twice")
        }

        info!("Starting smpp server on {}:{}", self.address, self.port);
        self.alive.store(true, Ordering::SeqCst);

        let socket_address = SocketAddr::new(self.address, self.port); // Will be moved out
        let alive = self.alive.clone();
        let handler = self.handler.clone();
        let session_init_timer = self.session_init_timer;

        self.handle = Some(tokio::spawn(async move {
            let listener = TcpListener::bind(socket_address).unwrap();
            listener.set_nonblocking(true).expect("Cannot set non-blocking");

            while alive.load(Ordering::SeqCst) {
                for stream in listener.incoming() {
                    if alive.load(Ordering::SeqCst) {
                        match stream {
                            Ok(stream) => {
                                let handler = handler.clone();
                                task::block_in_place (move || {
                                    let session_state = OPEN { };
                                    let connection_information = SmppConnectionInformation {
                                        server_address: socket_address,
                                        client_address: stream.peer_addr().unwrap(),
                                    };
                                    
                                    info!("Got a connection from {} on server {}, waiting {}ms for bind", connection_information.client_address, connection_information.server_address, session_init_timer);
                                    stream.set_nonblocking(false).expect("Can not set to non-blocking");
                                    stream.set_read_timeout(Some(Duration::from_millis(session_init_timer))).expect("Can not set read time-out");

                                    // Wrap the stream in a BufReader, so we can use the BufRead methods
                                    let mut reader = io::BufReader::new(stream);
                                    let first_read = reader.fill_buf();

                                    if first_read.is_ok() {
                                        let pdu = first_read.unwrap().to_vec();
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
                                                            let bind_receiver_resp = (handler.on_bind_receiver)(bind_receiver.clone(), &connection_information);
                                                            let session_state = session_state.bind_receiver(reader, bind_receiver, bind_receiver_resp, &connection_information, handler);
                                                            // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                            if session_state.is_ok() {
                                                                session_state.unwrap().read_loop(); // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                            } 
                                                        },
                                                        Err(error) => {
                                                            error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                            let error = bind_receiver::generic_reject(potential_seq_no, error).encode();
                                                            reader.into_inner().write(&error).expect("Can not write to stream");
                                                        }
                                                    }
                                                } else if header.command_id == CommandId::bind_transmitter as u32 {
                                                    match bind_transmitter::decode(header, &pdu) {
                                                        Ok(bind_transmitter) => {
                                                            let bind_transmitter_resp = (handler.on_bind_transmitter)(bind_transmitter.clone(), &connection_information);
                                                            let session_state = session_state.bind_transmitter(reader, bind_transmitter, &bind_transmitter_resp, &connection_information, handler);
                                                            // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                            if session_state.is_ok() {
                                                                session_state.unwrap().read_loop(); // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                        } 
                                                        },
                                                        Err(error) => {
                                                            error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                            let error = bind_transmitter::generic_reject(potential_seq_no, error).encode();
                                                            reader.into_inner().write(&error).expect("Can not write to stream");
                                                        }
                                                    }
                                                } else if header.command_id == CommandId::bind_transceiver as u32 {
                                                    match bind_transceiver::decode(header, &pdu) {
                                                        Ok(bind_transceiver) => {
                                                            let bind_transceiver_resp = (handler.on_bind_transceiver)(bind_transceiver.clone(), &connection_information);
                                                            let session_state = session_state.bind_transceiver(reader, bind_transceiver, &bind_transceiver_resp, &connection_information, handler);
                                                            // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                            if session_state.is_ok() {
                                                                session_state.unwrap().read_loop(); // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                            } 
                                                        },
                                                        Err(error) => {
                                                            error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                            let error = bind_transceiver::generic_reject(potential_seq_no, error).encode();
                                                            reader.into_inner().write(&error).expect("Can not write to stream");
                                                        }
                                                    }
                                                } else {
                                                    // Only allow bind commands, if not a bind command tell ESME about invalid bind status
                                                    error!("Did not expect command_id {} as bind not established yet, sending ESME_RINVBNDSTS in generick_nack", header.command_id);

                                                    let generic_nack = generic_nack::new(SmppError::ESME_RINVBNDSTS, potential_seq_no);
                                                    reader.into_inner().write(&generic_nack.encode()).expect("Can not write to stream");
                                                }
                                            },
                                            Err(error) => {
                                                error!("Unable to decode command_header for PDU, sending {:?} in generic_nack", error); 
                                                let generic_nack = generic_nack::new(error, potential_seq_no);
                                                reader.into_inner().write(&generic_nack.encode()).expect("Can not write to stream");
                                            } 
                                        }

                                    } else {
                                        error!("Unable to read initial SMPP PDU from {} on server {}, after waiting {}ms for bind, TCP connection will be closed", connection_information.client_address, connection_information.server_address, session_init_timer);
                                        reader.into_inner().shutdown(Shutdown::Both).expect("Unable to close TCP connection");
                                    }
                                });
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                // wait until network socket is ready, typically implemented
                                // via platform-specific APIs such as epoll or IOCP
                                thread::sleep(Duration::from_millis(100));
                                break;
                            }
                            Err(e) => panic!("encountered IO error: {e}"),
                        }
                    } else {
                        break;
                    }
                } 
            }
        }));

    }

    pub fn stop(&mut self) {
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