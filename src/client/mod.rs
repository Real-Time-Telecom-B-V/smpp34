use core::{fmt};
use std::{sync::{atomic::{AtomicU32, Ordering, AtomicBool}, mpsc::Sender, Arc}, net::{IpAddr, SocketAddr}, time::{Duration, Instant}, thread};

use bytes::BytesMut;
use futures::{executor::block_on, channel::mpsc::unbounded};
use log::{info, error};
use tokio::{task::{JoinHandle, self}, net::TcpStream, io::{AsyncWriteExt, AsyncReadExt}, time::timeout};
use uuid::Uuid;

use crate::{SmppConnectionInformation, unbind_resp, unbind, submit_sm_resp, data_sm, submit_sm, bind_receiver, SmppError, CommandId, bind_transmitter, bind_transceiver, deliver_sm, data_sm_resp, deliver_sm_resp, alert_notification, CommandHeader};

#[derive(Debug, Clone, PartialEq)]
pub enum BIND_TYPE {
    RX,
    TX,
    TRX
}

impl fmt::Display for BIND_TYPE {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct SmppClient {
    server_address: IpAddr,
    server_port: u16,
    bind_type: BIND_TYPE,
    system_id: String,
    password: String,
    system_type: String,
    addr_ton: u8,
    addr_npi: u8,
    address_range: String,
    handle: Option<JoinHandle<()>>,
    alive: Arc<AtomicBool>,
    handler: Arc<SmppClientListener>,
    session_init_timer: u64,
    enquire_link_timer: u64,
    inactivity_timer: u64,
    response_timer: u64,
    buffer_size: usize,
    window_size: usize,
}

pub struct SMSC {
    can_send: bool,
    tx_channel: Sender<Vec<u8>>,
    sequence_number: Arc<AtomicU32>,
}

impl SMSC {
    
    fn next_sequence_number(&self) -> u32 {
        self.sequence_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn send_submit_sm(&self, service_type: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, protocol_id: u8, priority_flag: u8, schedule_delivery_time: String, validity_period: String, registered_delivery: u8, replace_if_present_flag: u8, data_coding: u8, sm_default_msg_id: u8, short_message: String) -> u32 {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            let submit_sm = submit_sm::new(sequence_number.clone(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, protocol_id, priority_flag, schedule_delivery_time, validity_period, registered_delivery, replace_if_present_flag, data_coding, sm_default_msg_id, short_message);
            self.tx_channel.send(submit_sm.encode()).expect("Unable to send deliver_sm request to writer thread");
            sequence_number
        } else {
            panic!("Can not send deliver_sm on non RX/TRX bind");
        }
    }
    
    pub fn send_unbind(&self) -> u32 {
        let sequence_number = self.next_sequence_number();
        let unbind = unbind::with_sequence_number(sequence_number.clone());
        self.tx_channel.send(unbind.encode()).expect("Unable to send unbind request to writer thread");
        sequence_number
    }

    pub fn send_data_sm(&self, service_type: String, source_addr_ton: u8,  source_addr_npi: u8, source_addr: String,  dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, registered_delivery: u8, data_coding: u8) -> u32 {
        let sequence_number = self.next_sequence_number();
        let data_sm = data_sm::new(sequence_number.clone(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, registered_delivery, data_coding);
        self.tx_channel.send(data_sm.encode()).expect("Unable to send data_sm request to writer thread");
        sequence_number
    }
    
}

pub struct SmppClientListener {

    pub on_unbind: fn(unbind, &SmppConnectionInformation, session_id: &String) -> unbind_resp,
    
    pub on_submit_sm_resp: fn(submit_sm_resp, &SmppConnectionInformation, session_id: &String),
    pub on_data_sm_resp: fn(data_sm_resp, &SmppConnectionInformation, session_id: &String),

    pub on_deliver_sm: fn(deliver_sm, &SmppConnectionInformation, session_id: &String) -> deliver_sm_resp,
    pub on_alert_notification: fn(alert_notification, &SmppConnectionInformation, session_id: &String),

    /// Notification sent when an SMPP command timed-out (respone_timer triggered)
    pub on_timeout: fn(sequence_number: u32, session_id: &String),
    
    /// Notification sent when an SMSC is in bound state and is ready for receiving commands. 
    /// The SMSC wraps the MPSC channel towards the writer thread of the bind
    pub on_smsc_bound: fn(smsc: SMSC, session_id: &String),

    /// Notification sent when the SMSC has become unavailable due to a bind being closed or transport error
    /// It is up to the user of this listener to drop the SMSC received on the on_smsc_bound notificiation, any attempt to write to the SMSC after will result in a panic as the MSPC channel is closed
    pub on_smsc_unbound: fn(session_id: &String)
}




impl SmppClient {

    pub fn new(server_address: IpAddr, server_port: u16, bind_type: BIND_TYPE, system_id: String, password: String, system_type: String, addr_ton: u8, addr_npi: u8, address_range: String, handler: Arc<SmppClientListener>, window_size: usize) -> SmppClient {
        SmppClient::new_with_default_timers(server_address, server_port, bind_type, system_id, password, system_type, addr_ton, addr_npi, address_range, handler, 5000, 30000, 60000, 2000, 1500, window_size)
    } 

    pub fn new_with_default_timers(server_address: IpAddr, server_port: u16, bind_type: BIND_TYPE, system_id: String, password: String, system_type: String, addr_ton: u8, addr_npi: u8, address_range: String, handler: Arc<SmppClientListener>, session_init_timer: u64, enquire_link_timer: u64, inactivity_timer: u64, response_timer: u64, buffer_size: usize, window_size: usize) -> SmppClient {
        SmppClient { server_address, server_port, bind_type, system_id, password, system_type, addr_ton, addr_npi, address_range, handle: None, alive: Arc::new(AtomicBool::new(false)), handler, session_init_timer, enquire_link_timer, inactivity_timer, response_timer, buffer_size, window_size }
    } 

    pub fn start(&mut self) {

        if self.alive.load(Ordering::SeqCst) {
            panic!("Can not start client twice")
        }

        info!("Starting smpp client for server {}:{}", self.server_address, self.server_port);
        self.alive.store(true, Ordering::SeqCst);

        let server_socket_address = SocketAddr::new(self.server_address, self.server_port); // Will be moved out
        let alive = self.alive.clone();
        let listener = self.handler.clone();
        let session_init_timer = self.session_init_timer;
        let enquire_link_timer = self.enquire_link_timer;
        let response_timer = self.response_timer;
        let inactivity_timer = self.inactivity_timer;
        let buffer_size: usize = self.buffer_size;
        let bind_type = self.bind_type.clone();
        let system_id = self.system_id.clone();
        let password = self.password.clone();
        let system_type = self.system_type.clone();
        let addr_ton = self.addr_ton.clone();
        let addr_npi = self.addr_npi.clone();
        let address_range = self.address_range.clone();

        self.handle = Some(tokio::spawn(async move {
            let mut stream = TcpStream::connect(server_socket_address).await.expect("Can not connect"); // TODO connection timeout
            // TODO set connection timeout!
            info!("smpp client connected to server {}:{} sending bind PDU", server_socket_address.ip(), server_socket_address.port());

            let bind_pdu: Vec<u8> = match bind_type {
                BIND_TYPE::RX => {
                    bind_receiver::new(1, system_id, password, system_type, addr_ton, addr_npi, address_range).encode()
                },
                BIND_TYPE::TX => {
                    bind_transmitter::new(1, system_id, password, system_type, addr_ton, addr_npi, address_range).encode()
                },
                BIND_TYPE::TRX => {
                    bind_transceiver::new(1, system_id, password, system_type, addr_ton, addr_npi, address_range).encode()
                },
            };

            // Send bind request
            stream.write(&bind_pdu).await.expect("Unable to write to TCP stream");


            info!("Bind PDU sent, waiting for response");
            let session_init_timer_duration = tokio::time::Duration::from_millis(session_init_timer);
            let mut buffer = [0; 1024]; // Not using BytesMut here as we always first get a bind before expecting big traffic so chose a low buffer size
            let first_read = timeout(session_init_timer_duration, stream.read(&mut buffer)).await;

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
                            if potential_seq_no == 1 && header.command_status == SmppError::ESME_ROK as u32
                                && ((bind_type == BIND_TYPE::RX && header.command_id == CommandId::bind_receiver_resp as u32) 
                                || (bind_type == BIND_TYPE::TX && header.command_id == CommandId::bind_transmitter_resp as u32) 
                                || (bind_type == BIND_TYPE::TRX && header.command_id ==CommandId::bind_transceiver_resp as u32) 
                            ) {
                                let session_id = Uuid::new_v4().to_string();
                                info!("Successfuly bound in {} mode", bind_type);

                                let read_timeout = tokio::time::Duration::from_millis(500); // Set a little time-out so we are able to detect if inactivity_timer or enquire_link timers expired
                                let mut buffer = BytesMut::with_capacity(buffer_size);
                                let mut last_read = Instant::now(); 
                                let sequence_number = Arc::new(AtomicU32::new(2));

                                /*(listener.on_smsc_bound)(SMSC {
                                    can_send: bind_type == BIND_TYPE::TX || bind_type == BIND_TYPE::TRX, 
                                    tx_channel: tx.clone(), 
                                    sequence_number }, &session_id 
                                );*/

                                // Main read loop
                                while alive.load(Ordering::SeqCst) {
                                    
                                }
                            } else {
                                error!("No valid bind response received command_id {} command_status {} sequence_number {}", header.command_id, header.command_status, header.sequence_number);
                            }
                        },
                        Err(_) => error!("Unable to decode bind response"),
                    }
                },
                _ => error!("No bind response from server in {}ms", session_init_timer),
            }
        }));
    }

    pub fn stop(&mut self) {

        // TODO send unbind!!
        info!("Stopping smpp client");
        self.alive.store(false, Ordering::SeqCst);
        self.handle
            .take().expect("Called stop on non-running thread")
            .abort();
    }
}



impl Drop for SmppClient {
    fn drop(&mut self) {
        if self.alive.load(Ordering::SeqCst) {
            self.stop();
        }
    }
}