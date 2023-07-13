use std::{net::{IpAddr, TcpListener, SocketAddr}, thread::{self, JoinHandle}, time::Duration, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, io::{self, BufRead, Write}};

use crate::{server::state::OPEN, common::{CommandHeader, CommandId, SmppError, submit_sm_resp, submit_sm, data_sm, data_sm_resp}, bind_transmitter, bind_transmitter_resp, bind_transceiver, bind_receiver, bind_receiver_resp, unbind, unbind_resp, bind_transceiver_resp};


mod state;

pub struct SmppServer {
    address: IpAddr,
    port: u16,
    handle: Option<JoinHandle<()>>,
    alive: Arc<AtomicBool>,
    handler: Arc<SmppServerListener>,
}

pub struct SmppServerListener {
    pub on_bind_transmitter: fn(bind_transmitter) -> bind_transmitter_resp,
    pub on_bind_receiver: fn(bind_receiver) -> bind_receiver_resp,
    pub on_bind_transceiver: fn(bind_transceiver) -> bind_transceiver_resp,
    pub on_unbind: fn(unbind) -> unbind_resp,
}

pub trait SmppServerHandler {
    fn bind_transmitter(bind_transmitter: bind_transmitter) -> bind_transmitter_resp where Self: Sized;
    fn submit_sm(submit_sm: submit_sm ) -> submit_sm_resp where Self: Sized;
    fn data_sm(data_sm: data_sm) -> data_sm_resp where Self: Sized;
}

// See https://stackoverflow.com/a/42044143
impl SmppServer {

    pub fn new(address: IpAddr, port: u16, handler: Arc<SmppServerListener>) -> SmppServer {
        SmppServer { address, port, handle: None, alive: Arc::new(AtomicBool::new(false)), handler }
    } 

    pub fn start(&mut self) {

        if self.alive.load(Ordering::SeqCst) {
            panic!("Can not start server twice")
        }

        println!("Starting smpp server on {}:{}", self.address, self.port);
        self.alive.store(true, Ordering::SeqCst);

        let socket_address = SocketAddr::new(self.address, self.port); // Will be moved out

        let alive = self.alive.clone();
        let handler = self.handler.clone();

        self.handle = Some(thread::spawn(move || {

            let listener = TcpListener::bind(socket_address).unwrap();
            listener.set_nonblocking(true).expect("Cannot set non-blocking");

            while alive.load(Ordering::SeqCst) {
                for stream in listener.incoming() {
                    if alive.load(Ordering::SeqCst) {
                        match stream {
                            Ok(stream) => {
                                let handler = handler.clone();
                                thread::spawn(move || {
                                    let mut session_state = OPEN { stream };
                                    println!("Got a connection from {}, waiting for bind", session_state.stream.peer_addr().unwrap());

                                    let stream = &mut session_state.stream;

                                    stream.set_nonblocking(false).expect("Can not set to non-blocking");
                                    stream.set_read_timeout(Some(Duration::new(2, 0))).expect("Can not set read time-out");

                                    // Wrap the stream in a BufReader, so we can use the BufRead methods
                                    let mut reader = io::BufReader::new(stream);

                                    // Read current current data in the TcpStream
                                    let pdu: Vec<u8> = reader.fill_buf().expect("stuff").to_vec();
                                    let pdu_length = pdu.len();
                                    
                                    // Try read sequence_number in case we need a generic_nack.
                                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                                    let potential_seq_no = if pdu_length >= 16 { u32::from_be_bytes(pdu[12..16].try_into().expect("Can not read sequence_number")) } else { 0 };
                                    let command_header = CommandHeader::decode(&pdu);

                                    match command_header {
                                        Ok(header) => {
                                            if header.command_id == CommandId::bind_receiver as u32 {
                                                let decoded: Result<bind_receiver, SmppError> = bind_receiver::decode(header, &pdu);
                                                let response: Vec<u8> = if decoded.is_ok() {
                                                    (handler.on_bind_receiver)(decoded.unwrap()).encode()
                                                } else {
                                                    bind_receiver::generic_reject(potential_seq_no, decoded.unwrap_err()).encode()
                                                };
                                                reader.into_inner().write(&response).expect("Can not write to stream");
                                            } else if header.command_id == CommandId::bind_transmitter as u32 {
                                                let decoded: Result<bind_transmitter, SmppError> = bind_transmitter::decode(header, &pdu);
                                                let response: Vec<u8> = if decoded.is_ok() {
                                                    (handler.on_bind_transmitter)(decoded.unwrap()).encode()
                                                } else {
                                                    bind_transmitter::generic_reject(potential_seq_no, decoded.unwrap_err()).encode()
                                                };
                                                reader.into_inner().write(&response).expect("Can not write to stream");
                                            } else if header.command_id == CommandId::bind_transceiver as u32 {
                                                let decoded = bind_transceiver::decode(header, &pdu);
                                                let response: Vec<u8> = if decoded.is_ok() {
                                                    (handler.on_bind_transceiver)(decoded.unwrap()).encode()
                                                } else {
                                                    let error = decoded.unwrap_err();
                                                    println!("error {:?}", error);
                                                    bind_transceiver::generic_reject(potential_seq_no, error).encode()
                                                };
                                                reader.into_inner().write(&response).expect("Can not write to stream");
                                            } else {
                                                println!("command_id {} not implemented", header.command_id);
                                                // Only allow bind commands, if not a bind command tell ESME about invalid bind status
                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: SmppError::ESME_RINVBNDSTS as u32, sequence_number: potential_seq_no };
                                                reader.into_inner().write(&generic_nack.encode()).expect("Can not write to stream");
                                            }
                                        },
                                        Err(error) => {
                                            println!("generic_nack {:?}", error); // Send generic_nack
                                            let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                            reader.into_inner().write(&generic_nack.encode()).expect("Can not write to stream");
                                        } 
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
                            //None => todo!(),
                        }
                    } else {
                        break;
                    }
                } 
            }
        }));

    }

    pub fn stop(&mut self) {
        println!("Stopping smpp server");
        self.alive.store(false, Ordering::SeqCst);
        self.handle
            .take().expect("Called stop on non-running thread")
            .join().expect("Could not join spawned thread");
    }
}

impl Drop for SmppServer {
    fn drop(&mut self) {
        if self.alive.load(Ordering::SeqCst) {
            self.stop();
        }
    }
}