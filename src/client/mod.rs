use core::fmt;
use std::{sync::{atomic::{AtomicU32, Ordering, AtomicBool}, Arc}, net::SocketAddr, time::{Duration, Instant, SystemTime}, collections::HashMap};

use async_trait::async_trait;
use bytes::BytesMut;
use log::{info, error};
use tokio::{io::{self, split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt}, net::TcpStream, sync::{mpsc::{channel, Sender}, Mutex}, task::JoinHandle, time::{interval, timeout}};

use tokio_native_tls::{native_tls, TlsConnector, TlsStream};
use uuid::Uuid;

use crate::{alert_notification, bind_receiver, bind_transceiver, bind_transmitter, cancel_sm, data_sm, data_sm_resp, deliver_sm, deliver_sm_resp, enquire_link, submit_sm, submit_sm_resp, unbind, unbind_resp, CommandHeader, CommandId, SmppConnectionInformation, SmppError, WriteFrame};

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
    server_address: String,
    server_port: u16,
    tls: bool,
    bind_type: BIND_TYPE,
    system_id: String,
    password: String,
    system_type: String,
    addr_ton: u8,
    addr_npi: u8,
    address_range: String,
    handle: Option<JoinHandle<()>>,
    alive: Arc<AtomicBool>,
    handler: Arc<dyn SmppClientListener + Send + Sync + 'static>,
    session_init_timer: u64,
    enquire_link_timer: u64,
    inactivity_timer: u64,
    response_timer: u64,
    buffer_size: usize,
    window_size: usize,
}

pub struct SMSC {
    pub client_address: SocketAddr,
    pub server_address: SocketAddr,
    pub session_id: String,
    pub system_id: String,
    can_send: bool,
    tx_channel: Sender<WriteFrame>,
    sequence_number: Arc<AtomicU32>,
}

impl SMSC {
    
    fn next_sequence_number(&self) -> u32 {
        self.sequence_number.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn send_submit_sm(&self, service_type: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, protocol_id: u8, priority_flag: u8, schedule_delivery_time: String, validity_period: String, registered_delivery: u8, replace_if_present_flag: u8, data_coding: u8, sm_default_msg_id: u8, short_message: String) -> u32 {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            let submit_sm = submit_sm::new(sequence_number.clone(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, protocol_id, priority_flag, schedule_delivery_time, validity_period, registered_delivery, replace_if_present_flag, data_coding, sm_default_msg_id, short_message);
            info!("[{} on server {}] sending submit_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
            self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: submit_sm.encode() } ).await.expect("Unable to send deliver_sm request to writer thread");
            sequence_number
        } else {
            panic!("Can not send deliver_sm on non RX/TRX bind");
        }
    }
    
    pub async fn send_unbind(&self) -> u32 {
        let sequence_number = self.next_sequence_number();
        let unbind = unbind::with_sequence_number(sequence_number.clone());
        info!("[{} on server {}] sending unbind with sequence_number {}", self.client_address, self.server_address, sequence_number);
        self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: unbind.encode() }).await.expect("Unable to send unbind request to writer thread");
        sequence_number
    }

    pub async fn send_data_sm(&self, service_type: String, source_addr_ton: u8,  source_addr_npi: u8, source_addr: String,  dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, registered_delivery: u8, data_coding: u8) -> u32 {
        let sequence_number = self.next_sequence_number();
        let data_sm = data_sm::new(sequence_number.clone(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, registered_delivery, data_coding);
        info!("[{} on server {}] sending data_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
        self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: data_sm.encode() }).await.expect("Unable to send data_sm request to writer thread");
        sequence_number
    }

    pub async fn send_cancel_sm(&self, service_type: String, message_id: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String) -> u32 {
        let sequence_number = self.next_sequence_number();
        let cancel_sm = cancel_sm::new(sequence_number.clone(), service_type, message_id, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr);
        info!("[{} on server {}] sending cancel_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
        self.tx_channel.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: cancel_sm.encode() }).await.expect("Unable to send cancel_sm request to writer thread");
        sequence_number
    }
    
}

#[async_trait]
pub trait SmppClientListener {

    async fn on_unbind(&self, unbind: unbind, connection_information: &SmppConnectionInformation, session_id: &String) -> unbind_resp;
    async fn on_unbind_resp(&self, unbind_resp: unbind_resp, connection_information: &SmppConnectionInformation, session_id: &String);
    
    async fn on_submit_sm_resp(&self, submit_sm_resp: submit_sm_resp, connection_information: &SmppConnectionInformation, session_id: &String);
    async fn on_data_sm_resp(&self, data_sm_resp: data_sm_resp, connection_information: &SmppConnectionInformation, session_id: &String);

    async fn on_deliver_sm(&self, deliver_sm: deliver_sm, connection_information: &SmppConnectionInformation, session_id: &String) -> deliver_sm_resp;
    async fn on_alert_notification(&self, alert_notification: alert_notification, connection_information: &SmppConnectionInformation, session_id: &String);

    /// Notification sent when an SMPP command timed-out (respone_timer triggered)
    async fn on_timeout(&self, sequence_number: u32, session_id: &String);
    
    /// Notification sent when an SMSC is in bound state and is ready for receiving commands. 
    /// The SMSC wraps the MPSC channel towards the writer thread of the bind
    async fn on_smsc_bound(&self, smsc: SMSC, session_id: &String);

    /// Notification sent when the SMSC has become unavailable due to a bind being closed or transport error
    /// It is up to the user of this listener to drop the SMSC received on the on_smsc_bound notificiation, any attempt to write to the SMSC after will result in a panic as the MSPC channel is closed
    async fn on_smsc_unbound(&self, session_id: &String);
}

struct StreamWrapper {
    server_address: SocketAddr,
    client_address: SocketAddr,
    read_half: Box<dyn AsyncRead + Unpin + Send>,
    write_half: Box<dyn AsyncWrite + Unpin + Send>,
}

impl StreamWrapper {
    pub fn new_tcp(stream: TcpStream) -> io::Result<Self> {
        let server_address = stream.peer_addr().unwrap().clone();
        let client_address = stream.local_addr().unwrap().clone();

        let (read_half, write_half) = split(stream);
        Ok(StreamWrapper {
            server_address,
            client_address,
            read_half: Box::new(read_half),
            write_half: Box::new(write_half),
        })
    }

    pub fn new_tls(stream: TlsStream<TcpStream>) -> io::Result<Self> {
        let server_address = stream.get_ref().get_ref().get_ref().peer_addr().unwrap().clone();
        let client_address = stream.get_ref().get_ref().get_ref().local_addr().unwrap().clone();

        let (read_half, write_half) = split(stream);
        Ok(StreamWrapper {
            server_address,
            client_address,
            read_half: Box::new(read_half),
            write_half: Box::new(write_half),
        })
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_half.read(buf).await
    }

    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_half.write(buf).await
    }

    pub async fn split(self) -> (Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>) {
        let read_half = self.read_half;
        let write_half = self.write_half;
        (read_half, write_half)
    }
    
    fn local_addr(&self) -> SocketAddr {
        self.client_address
    }
    
    fn peer_addr(&self) -> SocketAddr {
        self.server_address
    }
}


impl SmppClient {

    pub fn new(server_address: String, server_port: u16, tls: bool, bind_type: BIND_TYPE, system_id: String, password: String, system_type: String, addr_ton: u8, addr_npi: u8, address_range: String, handler: Arc<dyn SmppClientListener + Send + Sync + 'static>, window_size: usize) -> SmppClient {
        SmppClient::new_with_default_timers(server_address, server_port, tls, bind_type, system_id, password, system_type, addr_ton, addr_npi, address_range, handler, 5000, 30000, 60000, 2000, 1500, window_size)
    } 

    pub fn new_with_default_timers(server_address: String, server_port: u16, tls: bool, bind_type: BIND_TYPE, system_id: String, password: String, system_type: String, addr_ton: u8, addr_npi: u8, address_range: String, handler: Arc<dyn SmppClientListener + Send + Sync + 'static>, session_init_timer: u64, enquire_link_timer: u64, inactivity_timer: u64, response_timer: u64, buffer_size: usize, window_size: usize) -> SmppClient {
        SmppClient { server_address, server_port, tls, bind_type, system_id, password, system_type, addr_ton, addr_npi, address_range, handle: None, alive: Arc::new(AtomicBool::new(false)), handler, session_init_timer, enquire_link_timer, inactivity_timer, response_timer, buffer_size, window_size }
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    } 

    pub async fn start(&mut self) {

        if self.alive.load(Ordering::SeqCst) {
            panic!("Can not start client twice")
        }

        info!("Starting smpp client for server {} with window size: {}", self.server_address, self.window_size);
        self.alive.store(true, Ordering::SeqCst);

        let server_socket_address = self.server_address.clone();
        let server_socker_port = self.server_port.clone();
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
        let tls = self.tls.clone();


        self.handle = Some(tokio::spawn(async move {

            let mut stream = if tls {
                let connector = TlsConnector::from(native_tls::TlsConnector::builder()
                .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
                .build()
                .unwrap());

                let domain = server_socket_address.clone();
                let address = format!("{}:{}", &domain, &server_socker_port);
                let stream = TcpStream::connect(address).await.unwrap();
                let stream = connector.connect(&domain, stream).await.unwrap();

                StreamWrapper::new_tls(stream).unwrap()
            } else {
                let address = format!("{}:{}", &server_socket_address, &server_socker_port);
                let stream = TcpStream::connect(address).await.unwrap();

                StreamWrapper::new_tcp(stream).unwrap()
            };

            // TODO set connection timeout!
            info!("smpp client connected to server {}, sending bind PDU", server_socket_address);

            let connection_information  = SmppConnectionInformation {
                server_address: stream.peer_addr(),
                client_address: stream.local_addr(),
            };

            let bind_pdu: Vec<u8> = match bind_type {
                BIND_TYPE::RX => {
                    bind_receiver::new(1, system_id.clone(), password, system_type, addr_ton, addr_npi, address_range).encode()
                },
                BIND_TYPE::TX => {
                    bind_transmitter::new(1, system_id.clone(), password, system_type, addr_ton, addr_npi, address_range).encode()
                },
                BIND_TYPE::TRX => {
                    bind_transceiver::new(1, system_id.clone(), password, system_type, addr_ton, addr_npi, address_range).encode()
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

                                let (mut reader, writer) = stream.split().await;
                                let writer = Arc::new(Mutex::new(writer));

                                let (tx, mut rx) = channel::<WriteFrame>(100);
                                let pending_requests: Arc<Mutex<HashMap<u32, SystemTime>>> = Arc::new(Mutex::new(HashMap::new()));

                                let read_timeout = tokio::time::Duration::from_millis(500); // Set a little time-out so we are able to detect if inactivity_timer or enquire_link timers expired
                                let mut buffer = BytesMut::with_capacity(buffer_size);
                                let mut last_read = Instant::now(); 
                                let sequence_number = Arc::new(AtomicU32::new(2));

                                let writer_alive = alive.clone();
                                let writer_stream = writer.clone();
                                let writer_pending_requests = pending_requests.clone();
                                let writer_thread = tokio::task::spawn(async move {
                                    info!("[{} on server {}] writer thread started", connection_information.client_address, connection_information.server_address);
                                    while writer_alive.load(Ordering::SeqCst) {
                                       if let Some(frame) = rx.recv().await {
                                            match writer_stream.lock().await.write(&frame.pdu).await {
                                                Ok(_) => { 
                                                    if frame.our_sequence_number.is_some() {
                                                        writer_pending_requests.lock().await.insert(frame.our_sequence_number.unwrap(), SystemTime::now()); 
                                                    }
                                                },
                                                Err(e) => { error!("Unable to write to TCP stream {}", e) },
                                            }
                                       } else {
                                            error!("[{} on server {}] writer thread unable to receive frame", connection_information.client_address, connection_information.server_address);
                                            break;
                                       }
                                    }
                                    info!("[{} on server {}] writer thread stopped", connection_information.client_address, connection_information.server_address);
                                });

                                let send_enquire_link = alive.clone();
                                let enquire_link_sequence_number = sequence_number.clone();
                                let enquire_link_writer = writer.clone();
                                let enquire_link_writer_tx = tx.clone();
                                let (enquire_link_tx, mut enquire_link_rx) = channel::<u32>(100);
                                let enquire_link_ticker = tokio::task::spawn(async move {
                                    info!("[{} on server {}] enquire_link timer started, sending every {}ms", connection_information.client_address, connection_information.server_address, enquire_link_timer);
                                    let mut enquire_link_timer = interval(Duration::from_millis(enquire_link_timer));
                                    let response_timer = Duration::from_millis(response_timer);
                                    enquire_link_timer.tick().await; // tick for the first time to warm the timer
                                    enquire_link_timer.tick().await; // tick for the second time to start sending enquire_links only on next interval (as we just opened the connection it makes no sense to tick immediately)

                                    while send_enquire_link.load(Ordering::SeqCst) {
                                        let sequence_number = enquire_link_sequence_number.fetch_add(1, Ordering::SeqCst);
                                        info!("[{} on server {}] sending enquire_link with sequence_number {}", connection_information.client_address, connection_information.server_address, sequence_number);

                                        enquire_link_writer_tx.send(WriteFrame { our_sequence_number: Some(sequence_number), pdu: enquire_link::new(sequence_number).encode() }).await.expect("Can not send to writer thread");

                                        match enquire_link_rx.recv().await {
                                            Some(sequence) => {
                                                // We want the sequence number to match, otherwise we must kill this bind
                                                if sequence != sequence_number { 
                                                    error!("[{} on server {}] enquire_link_resp with sequence_number {} did not match sequence_number {}", connection_information.client_address, connection_information.server_address, sequence, sequence_number);
                                                    enquire_link_writer.lock().await.shutdown().await.expect("Unable to close TCP stream");
                                                    break;
                                                } 
                                        },
                                            None => {
                                                error!("[{} on server {}] enquire_link with sequence_number {} no response within {}ms", connection_information.client_address, connection_information.server_address, sequence_number, response_timer.as_millis());
                                                enquire_link_writer.lock().await.shutdown().await.expect("Unable to close TCP stream");
                                                break;
                                            }
                                        }
                                
                                        // Wait for next interval to send timer again
                                        enquire_link_timer.tick().await;
                                    }
                                    info!("[{} on server {}] enquire_link timer stopped", connection_information.client_address, connection_information.server_address);
                                });

                                listener.on_smsc_bound(SMSC {
                                    can_send: bind_type == BIND_TYPE::TX || bind_type == BIND_TYPE::TRX, 
                                    tx_channel: tx.clone(), 
                                    sequence_number,
                                    server_address: connection_information.server_address.clone(),
                                    client_address: connection_information.client_address.clone(),
                                    session_id: session_id.clone(),
                                    system_id: system_id.clone(), }, &session_id 
                                ).await;

                                // Main read loop
                                while alive.load(Ordering::SeqCst) {
                                    let result = timeout(read_timeout, reader.read_buf(&mut buffer)).await;
                                    match result {
                                        Ok(Ok(frame_length)) => {
                                            let frame = buffer[0..frame_length].to_vec();
                                            if frame_length >= 16 { // anything else we don't want
                                                let mut cursor = 0;
                                                let mut last_pdu_was_unbind = false;
                                                while cursor < frame_length && frame_length - cursor >= 16 { // Only read when we have bytes left in the frame AND at least 16 bytes left in the buffer (we choose to ignore and not decode)
                                                    let pdu_length: u32 = u32::from_be_bytes(frame[cursor..cursor + 4].try_into().expect("Can not read PDU length"));
                                                    let pdu = buffer[cursor as usize..cursor + pdu_length as usize].to_vec();

                                                    // Try read sequence_number in case we need a generic_nack.
                                                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                                                    let potential_seq_no = if pdu_length >= 16 { u32::from_be_bytes(pdu[12..16].try_into().expect("Can not read sequence_number")) } else { 0 };
                                                    let command_header = CommandHeader::decode(&pdu);
                                                    let tx = tx.clone();

                                                    match command_header {
                                                        Ok(header) => {
                                                            if header.command_id == CommandId::deliver_sm as u32 && (bind_type == BIND_TYPE::RX || bind_type == BIND_TYPE::TRX)  {
                                                                match deliver_sm::decode(header, &pdu) {
                                                                    Ok(deliver_sm) => {
                                                                        info!("[{} on server {}] received deliver_sm with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        let handler = listener.clone();
                                                                        let connection_information = connection_information.clone();
                                                                        let submit_sm_session_id = session_id.clone();

                                                                        let submit_sm_resp = handler.on_deliver_sm(deliver_sm.clone(), &connection_information, &submit_sm_session_id).await;
                                                                        tx.send(WriteFrame { our_sequence_number: None, pdu: submit_sm_resp.encode() }).await.expect("Can not send to writer thread");
                                                                    },
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode submit_sm", connection_information.client_address, connection_information.server_address);
                                                                        let error = submit_sm::generic_reject(potential_seq_no, error).encode();
                                                                        tx.send(WriteFrame { our_sequence_number: None, pdu: error }).await.expect("Can not send to writer thread");
                                                                    }
                                                                }
                                                            } else if header.command_id == CommandId::submit_sm_resp as u32 && (bind_type == BIND_TYPE::TX || bind_type == BIND_TYPE::TRX) {
                                                                let mut guard = pending_requests.lock().await;
                                                                if let Some(time) = guard.remove(&header.sequence_number) {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed = time.elapsed().expect("Unable to elapse").as_millis();
                                                                    if  lapsed > response_timer.into() {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match submit_sm_resp::decode(header, &pdu) {
                                                                            Ok(submit_sm_resp) => {
                                                                                info!("[{} on server {}] received submit_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let handler = listener.clone();
                                                                                let connection_information = connection_information.clone();
                                                                                let submit_sm_session_id = session_id.clone();
        
                                                                                handler.on_submit_sm_resp(submit_sm_resp.clone(), &connection_information, &submit_sm_session_id).await;
                                                                            },
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode submit_sm_resp", connection_information.client_address, connection_information.server_address);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode() }).await.expect("Can not send to writer thread");
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id == CommandId::data_sm_resp as u32 && (bind_type == BIND_TYPE::TX || bind_type == BIND_TYPE::TRX) {
                                                                let mut guard = pending_requests.lock().await;
                                                                if let Some(time) = guard.remove(&header.sequence_number) {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed = time.elapsed().expect("Unable to elapse").as_millis();
                                                                    if  lapsed > response_timer.into() {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match data_sm_resp::decode(header, &pdu) {
                                                                            Ok(data_sm_resp) => {
                                                                                info!("[{} on server {}] received data_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let handler = listener.clone();
                                                                                let connection_information = connection_information.clone();
                                                                                let data_sm_session_id = session_id.clone();
        
                                                                                handler.on_data_sm_resp(data_sm_resp.clone(), &connection_information, &data_sm_session_id).await;
                                                                            },
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode data_sm_resp", connection_information.client_address, connection_information.server_address);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode() }).await.expect("Can not send to writer thread");
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id == CommandId::alert_notification as u32 {
                                                                match alert_notification::decode(header, &pdu) {
                                                                    Ok(alert_notification) => {
                                                                        info!("[{} on server {}] received alert_notification with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        let handler = listener.clone();
                                                                        let connection_information = connection_information.clone();
                                                                        let submit_sm_session_id = session_id.clone();

                                                                        handler.on_alert_notification(alert_notification.clone(), &connection_information, &submit_sm_session_id).await;
                                                                    },
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode alert_notification", connection_information.client_address, connection_information.server_address);
                                                                        let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                        tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode() }).await.expect("Can not send to writer thread");
                                                                    }
                                                                }
                                                            } else if header.command_id == CommandId::enquire_link as u32 {
                                                                match enquire_link::decode(header, &pdu) {
                                                                    Ok(enquire_link) => {
                                                                        info!("[{} on server {}] received enquire_link with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        let enquire_link_resp = enquire_link.accept();
                                                                        info!("[{} on server {}] sending enquire_link_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        tx.send(WriteFrame { our_sequence_number: None, pdu: enquire_link_resp.encode() }).await.expect("Can not send to writer thread");
                                                                    },
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode enquire_link", connection_information.client_address, connection_information.server_address);
                                                                        let error = submit_sm::generic_reject(potential_seq_no, error).encode();
                                                                        tx.send(WriteFrame { our_sequence_number: None, pdu: error }).await.expect("Can not send to writer thread");
                                                                    }
                                                                }
                                                            } else if header.command_id == CommandId::enquire_link_resp as u32 {
                                                                info!("[{} on server {}] received enquire_link_resp for sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);

                                                                // Send it to enquire_link thread for verification, it's waiting on it!
                                                                enquire_link_tx.send(header.sequence_number).await.expect("Unable to send sequence to enquire_link thread");

                                                                // Cleanup pending requests
                                                                let mut guard = pending_requests.lock().await;
                                                                if let Some(time) = guard.remove(&header.sequence_number) {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed = time.elapsed().expect("Unable to elapse").as_millis();
                                                                    if  lapsed > response_timer.into() {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await; 
                                                                    } 

                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                                
                                                            } else if header.command_id == CommandId::unbind as u32 {

                                                                // Whether or not the unbind fails, we don't care, if any ESMe sends us an unbind we stop the connection, so first we stop the enquire_link timer
                                                                enquire_link_ticker.abort(); 

                                                                match unbind::decode(header, &pdu) {
                                                                    Ok(unbind) => {
                                                                        let unbind_resp = listener.on_unbind(unbind.clone(), &connection_information, &session_id).await;
                                                                        tx.send(WriteFrame { our_sequence_number: None, pdu: unbind_resp.encode() }).await.expect("Can not send to writer thread");
                                                                    },
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode unbind", connection_information.client_address, connection_information.server_address);
                                                                        let error = unbind::generic_reject(potential_seq_no, error).encode();
                                                                        tx.send(WriteFrame { our_sequence_number: None, pdu: error }).await.expect("Can not send to writer thread");
                                                                    }
                                                                }

                                                                last_pdu_was_unbind = true;

                                                                break; 
                                                            } else if header.command_id == CommandId::unbind_resp as u32 {
                                                                info!("[{} on server {}] received unbind_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                listener.on_unbind_resp(unbind_resp::decode(header, &pdu).unwrap(), &connection_information, &session_id).await;
                                                                
                                                                last_pdu_was_unbind = true;
                                                                break;

                                                            } else {
                                                                error!("[{} on server {}] Did not expect command_id {} for this bind, sending ESME_RINVBNDSTS in generick_nack", connection_information.client_address, connection_information.server_address, header.command_id);
                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: SmppError::ESME_RINVBNDSTS as u32, sequence_number: potential_seq_no };
                                                                tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode() }).await.expect("Can not send to writer thread");
                                                            }
                                                        },
                                                        Err(error) => {
                                                            error!("[{} on server {}] Unable to decode command_header for PDU, sending {:?} in generic_nack", connection_information.client_address, connection_information.server_address, error); 
                                                            let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                            tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode() }).await.expect("Can not send to writer thread");

                                                            enquire_link_ticker.abort(); // When the TCP stream is closed stop enquiring the link
                                                        } 
                                                    }

                                                    cursor = cursor + pdu_length as usize;
                                                }

                                                if last_pdu_was_unbind {
                                                    break // Break the read loop so we can go to CLOSED state
                                                }

                                                last_read = Instant::now();

                                                // Last thing to do is general time-out detection
                                                let mut pending_requests = pending_requests.lock().await;

                                                for (sequence_number, time) in pending_requests.iter_mut() {
                                                    let lapsed = time.elapsed().expect("Unable to elapse").as_millis();
                                                    if lapsed > response_timer.into() {
                                                    // pending_requests.remove(&sequence_number_to_remove);
                                                        error!("[{} on server {}] Response for sequence_number {} did not come in after {}ms lapsed", connection_information.client_address, connection_information.server_address, sequence_number, lapsed);
                                                        
                                                        listener.on_timeout(sequence_number.clone(), &session_id).await; 
                                                        
                                                    }
                                                }
                                            }
                                        },
                                        Err(_e) => { /* Nothing to do here as we introduce the interval to not constantly block this thread */ },
                                        Ok(Err(e)) => {
                                            error!("[{} on server {}] {} ", connection_information.client_address, connection_information.server_address, e);
                                            break
                                        },
                                    }

                                    if enquire_link_ticker.is_finished() {
                                        error!("[{} on server {}] enquire_link thread finished, stopping read loop", connection_information.client_address, connection_information.server_address);
                                        break;
                                    } else if last_read.elapsed().as_millis() > inactivity_timer.into() {
                                        // Please note, it is more likely that the enquire_link timer stopped earlier as it expects a response likely within 2000ms (default) but in some weird scenario that it it's stuck we can always trigger the inactivity timer by keeping
                                        // track of when the last packet was read
                                        error!("[{} on server {}] inactivity_timer triggered after {}ms, closing TCP connection", connection_information.client_address, connection_information.server_address, inactivity_timer);
                                        break;
                                    }

                                    buffer.clear(); // Make sure we start reading with an empty buffer
                                }

                                listener.on_smsc_unbound(&session_id).await;

                                info!("[{} on server {}] {} going to CLOSED state", connection_information.client_address, connection_information.server_address, bind_type);
                                alive.store(false, Ordering::SeqCst);

                            
                                //enquire_link_ticker.abort(); // Stop enquiring the link as we are closing the connection
                                writer_thread.abort(); // Stop allowing the sending of writing of new PDUs 
                            } else {
                                match header.command_status {
                                    status if status == SmppError::ESME_RINVPASWD as u32 => error!("Bind failed, invalid password, command_id {:#x} command_status {} sequuence_number {}", header.command_id, header.command_status, header.sequence_number),
                                    status if status == SmppError::ESME_RINVSYSID as u32 => error!("Bind failed, invalid system_id, command_id {:#x} command_status {} sequuence_number {}", header.command_id, header.command_status, header.sequence_number),
                                    status if status == SmppError::ESME_RSYSERR as u32 => error!("Bind failed, system error, command_id {:#x} command_status {} sequuence_number {}", header.command_id, header.command_status, header.sequence_number),
                                    status if status == SmppError::ESME_RBINDFAIL as u32 => error!("Bind failed, generic error, command_id {:#x} command_status {} sequuence_number {}", header.command_id, header.command_status, header.sequence_number),
                                    _ => error!("Bind failed with unknown error, command_id {:#x} command_status {} sequuence_number {}", header.command_id, header.command_status, header.sequence_number),
                                }
                                
                            }
                        },
                        Err(_) => error!("Unable to decode bind response"),
                    }
                },
                _ => error!("No bind response from server in {}ms", session_init_timer),
            }
        }));
    }

    pub async fn stop(&mut self) {

        // We except the user of this code to send unbind before stopping the client
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
            futures::executor::block_on(self.stop());
        }
    }
}