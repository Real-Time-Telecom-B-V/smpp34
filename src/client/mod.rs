use core::fmt;
use std::{sync::{atomic::{AtomicU32, Ordering, AtomicBool}, mpsc::Sender, Arc}, net::{IpAddr, SocketAddr}};

use futures::executor::block_on;
use log::info;
use tokio::{task::{JoinHandle, self}, net::TcpStream, io::AsyncWriteExt};
use uuid::Uuid;

use crate::{SmppConnectionInformation, unbind_resp, unbind, submit_sm_resp, data_sm, submit_sm, bind_receiver, SmppError, CommandId};

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
    handle: Option<JoinHandle<()>>,
    alive: Arc<AtomicBool>,
    handler: Arc<SmppClientListener>,
    session_init_timer: u64,
    enquire_link_timer: u64,
    inactivity_timer: u64,
    response_timer: u64,
    buffer_size: usize
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

    pub fn send_submit_sm(&self, service_type: String, source_addr_ton: u8, source_addr_npi: u8, source_addr: String, dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, protocol_id: u8, priority_flag: u8, schedule_delivery_time: String, validity_period: String, registered_delivery: u8, replace_if_present_flag: u8, data_coding: u8, sm_default_msg_id: u8, short_message: String) {
        if self.can_send {
            let submit_sm = submit_sm::new(self.next_sequence_number(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, protocol_id, priority_flag, schedule_delivery_time, validity_period, registered_delivery, replace_if_present_flag, data_coding, sm_default_msg_id, short_message);
            self.tx_channel.send(submit_sm.encode()).expect("Unable to send deliver_sm request to writer thread");
        } else {
            panic!("Can not send deliver_sm on non RX/TRX bind");
        }
    }
    
    pub fn send_unbind(&self) {
        let unbind = unbind::with_sequence_number(self.sequence_number.fetch_add(1, Ordering::SeqCst));
        self.tx_channel.send(unbind.encode()).expect("Unable to send unbind request to writer thread");
    }

    pub fn send_data_sm(&self, service_type: String, source_addr_ton: u8,  source_addr_npi: u8, source_addr: String,  dest_addr_ton: u8, dest_addr_npi: u8, destination_addr: String, esm_class: u8, registered_delivery: u8, data_coding: u8) {
        let data_sm = data_sm::new(self.next_sequence_number(), service_type, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, esm_class, registered_delivery, data_coding);
        self.tx_channel.send(data_sm.encode()).expect("Unable to send data_sm request to writer thread");
    }
    
}

pub struct SmppClientListener {

    pub on_unbind: fn(unbind, &SmppConnectionInformation, session_id: &String) -> unbind_resp,
    pub on_submit_sm_resp: fn(submit_sm_resp, &SmppConnectionInformation, session_id: &String),
    
    /// Notification sent when an SMSC is in bound state and is ready for receiving commands. 
    /// The SMSC wraps the MPSC channel towards the writer thread of the bind
    pub on_smsc_bound: fn(smsc: SMSC, session_id: &String),

    /// Notification sent when the SMSC has become unavailable due to a bind being closed or transport error
    /// It is up to the user of this listener to drop the SMSC received on the on_smsc_bound notificiation, any attempt to write to the SMSC after will result in a panic as the MSPC channel is closed
    pub on_smsc_unbound: fn(session_id: &String)
}




impl SmppClient {

    pub fn new(server_address: IpAddr, server_port: u16, bind_type: BIND_TYPE, handler: Arc<SmppClientListener>) -> SmppClient {
        SmppClient::new_with_default_timers(server_address, server_port, bind_type, handler, 5000, 30000, 60000, 2000, 1500)
    } 

    pub fn new_with_default_timers(server_address: IpAddr, server_port: u16, bind_type: BIND_TYPE, handler: Arc<SmppClientListener>, session_init_timer: u64, enquire_link_timer: u64, inactivity_timer: u64, response_timer: u64, buffer_size: usize) -> SmppClient {
        SmppClient { server_address, server_port, bind_type, handle: None, alive: Arc::new(AtomicBool::new(false)), handler, session_init_timer, enquire_link_timer, inactivity_timer, response_timer, buffer_size }
    } 

    pub fn start(&mut self) {

        if self.alive.load(Ordering::SeqCst) {
            panic!("Can not start client twice")
        }

        info!("Starting smpp client for server {}:{}", self.server_address, self.server_port);
        self.alive.store(true, Ordering::SeqCst);

        let server_socket_address = SocketAddr::new(self.server_address, self.server_port); // Will be moved out
        let alive = self.alive.clone();
        let handler = self.handler.clone();
        let session_init_timer = self.session_init_timer;
        let enquire_link_timer = self.enquire_link_timer;
        let response_timer = self.response_timer;
        let inactivity_timer = self.inactivity_timer;
        let buffer_size: usize = self.buffer_size;
        let bind_type = self.bind_type.clone();

        self.handle = Some(task::spawn_blocking(move || {

            let mut stream = block_on(TcpStream::connect(server_socket_address)).expect("Can not connect");

            match bind_type {
                BIND_TYPE::RX => {
                    let bind_receiver = bind_receiver { 
                        header: crate::CommandHeader { 
                            command_length: 16, 
                            command_id: CommandId::bind_receiver as u32, 
                            command_status: SmppError::ESME_ROK as u32, 
                            sequence_number: 0 
                        }, 
                        system_id: todo!(), 
                        password: todo!(), 
                        system_type: todo!(), 
                        interface_version: todo!(), 
                        addr_ton: todo!(), 
                        addr_npi: todo!(), 
                        address_range: todo!() 
                    };
                },
                BIND_TYPE::TX => todo!(),
                BIND_TYPE::TRX => todo!(),
            };


            while alive.load(Ordering::SeqCst) {
                let session_id = Uuid::new_v4().to_string();
                break;
            }
        }));

    }

    pub fn stop(&mut self) {
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