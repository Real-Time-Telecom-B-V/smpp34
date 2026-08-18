use core::fmt;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::BytesMut;
use log::{debug, error, info};
use tokio::{
    io::{self, split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{
        mpsc::{channel, Sender},
        oneshot, Mutex,
    },
    task::JoinHandle,
    time::{interval, timeout},
};

use tokio_native_tls::{native_tls, TlsConnector, TlsStream};
use uuid::Uuid;

use crate::common::be_u32_at;
use crate::common::{frame_first_pdu, Framing};
use crate::{
    alert_notification, bind_receiver, bind_transceiver, bind_transmitter, cancel_sm,
    cancel_sm_resp, data_sm, data_sm_resp, deliver_sm, deliver_sm_resp, enquire_link, generic_nack,
    query_sm, query_sm_resp, replace_sm, replace_sm_resp, submit_sm, submit_sm_multi,
    submit_sm_multi_resp, submit_sm_resp, unbind, unbind_resp, CommandHeader, CommandId,
    DestAddress, SmppConnectionInformation, SmppError, SmppReply, Tlv, TlvTag, WriteFrame,
};
use std::io::Cursor;

#[derive(Debug, Clone, PartialEq)]
pub enum BIND_TYPE {
    RX,
    TX,
    TRX,
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
    response_timer: u64,
}

impl SMSC {
    fn next_sequence_number(&self) -> u32 {
        self.sequence_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Start building a `submit_sm` to send on this session — an ergonomic
    /// alternative to the 17-argument [`send_submit_sm`](SMSC::send_submit_sm).
    pub fn submit_sm(&self) -> SubmitSmBuilder<'_> {
        SubmitSmBuilder::new(self)
    }

    pub async fn send_submit_sm(
        &self,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        dest_addr_ton: u8,
        dest_addr_npi: u8,
        destination_addr: String,
        esm_class: u8,
        protocol_id: u8,
        priority_flag: u8,
        schedule_delivery_time: String,
        validity_period: String,
        registered_delivery: u8,
        replace_if_present_flag: u8,
        data_coding: u8,
        sm_default_msg_id: u8,
        short_message: Vec<u8>,
    ) -> Result<submit_sm_resp, SmppError> {
        self.send_submit_sm_pdu(submit_sm::new(
            0, // overwritten by send_submit_sm_pdu, which owns the sequence space
            service_type,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
            esm_class,
            protocol_id,
            priority_flag,
            schedule_delivery_time,
            validity_period,
            registered_delivery,
            replace_if_present_flag,
            data_coding,
            sm_default_msg_id,
            short_message,
        ))
        .await
    }

    /// Send a pre-built `submit_sm` and await its response.
    ///
    /// This is the path for anything the fixed-argument
    /// [`send_submit_sm`](SMSC::send_submit_sm) cannot express — above all
    /// optional parameters (TLVs), which the caller attaches with
    /// [`submit_sm::with_tlvs`] / [`submit_sm::push_tlv`] — and for relaying a
    /// PDU that was decoded elsewhere. [`SMSC::submit_sm`] wraps it in a fluent
    /// builder.
    ///
    /// The session assigns the sequence number: whatever the PDU carries is
    /// overwritten, so responses correlate against this session's window.
    pub async fn send_submit_sm_pdu(
        &self,
        mut submit_sm: submit_sm,
    ) -> Result<submit_sm_resp, SmppError> {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            submit_sm.set_sequence_number(sequence_number);
            info!(
                "[{} on server {}] sending submit_sm with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            let (tx, rx) = oneshot::channel();

            match self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: submit_sm.encode(),
                    oneshot: Some(tx),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("[{} on server {}] unable to send submit_sm with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                    return Err(SmppError::ESME_RSYSERR);
                }
            }

            let response = timeout(Duration::from_millis(self.response_timer), rx).await;

            match response {
                Ok(Ok(response)) => {
                    // response can be either submit_sm_resp or generic_nack
                    if let Some(submit_sm_resp) = response.as_any().downcast_ref::<submit_sm_resp>()
                    {
                        info!(
                            "[{} on server {}] received submit_sm_resp with sequence_number {}",
                            self.client_address, self.server_address, sequence_number
                        );
                        Ok(submit_sm_resp.clone())
                    } else if let Some(generic_nack) =
                        response.as_any().downcast_ref::<generic_nack>()
                    {
                        error!("[{} on server {}] received generic_nack in response to submit_sm with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack);
                        Err(generic_nack.get_error())
                    } else {
                        error!("[{} on server {}] received unknown response to submit_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
                        Err(SmppError::ESME_RSYSERR)
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "[{} on server {}] unable to receive submit_sm_resp: {}",
                        self.client_address, self.server_address, e
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
                Err(_) => {
                    error!(
                        "[{} on server {}] submit_sm_resp with sequence_number {} timed out",
                        self.client_address, self.server_address, sequence_number
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
            }
        } else {
            panic!("Can not send submit_sm on non TX/TRX bind");
        }
    }

    pub async fn send_unbind(&self) -> Result<unbind_resp, SmppError> {
        let sequence_number = self.next_sequence_number();
        let unbind = unbind::with_sequence_number(sequence_number);
        info!(
            "[{} on server {}] sending unbind with sequence_number {}",
            self.client_address, self.server_address, sequence_number
        );

        let (tx, rx) = oneshot::channel();

        match self
            .tx_channel
            .send(WriteFrame {
                our_sequence_number: Some(sequence_number),
                pdu: unbind.encode(),
                oneshot: Some(tx),
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("[{} on server {}] unable to send unbind with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                return Err(SmppError::ESME_RSYSERR);
            }
        }

        let response = timeout(Duration::from_millis(self.response_timer), rx).await;
        match response {
            Ok(Ok(response)) => {
                // response can be either unbind_resp or generic_nack
                if let Some(unbind_resp) = response.as_any().downcast_ref::<unbind_resp>() {
                    info!(
                        "[{} on server {}] received unbind_resp with sequence_number {}",
                        self.client_address, self.server_address, sequence_number
                    );
                    Ok(unbind_resp.clone())
                } else if let Some(generic_nack) = response.as_any().downcast_ref::<generic_nack>()
                {
                    error!("[{} on server {}] received generic_nack in response to unbind with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack);
                    Err(generic_nack.get_error())
                } else {
                    error!("[{} on server {}] received unknown response to unbind with sequence_number {}", self.client_address, self.server_address, sequence_number);
                    Err(SmppError::ESME_RSYSERR)
                }
            }
            Ok(Err(e)) => {
                error!(
                    "[{} on server {}] unable to receive unbind_resp: {}",
                    self.client_address, self.server_address, e
                );
                Err(SmppError::ESME_RSYSERR)
            }
            Err(_) => {
                error!(
                    "[{} on server {}] unbind_resp with sequence_number {} timed out",
                    self.client_address, self.server_address, sequence_number
                );
                Err(SmppError::ESME_RSYSERR)
            }
        }
    }

    pub async fn send_data_sm(
        &self,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        dest_addr_ton: u8,
        dest_addr_npi: u8,
        destination_addr: String,
        esm_class: u8,
        registered_delivery: u8,
        data_coding: u8,
    ) -> Result<data_sm_resp, SmppError> {
        self.send_data_sm_pdu(data_sm::new(
            0, // overwritten by send_data_sm_pdu, which owns the sequence space
            service_type,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            dest_addr_ton,
            dest_addr_npi,
            destination_addr,
            esm_class,
            registered_delivery,
            data_coding,
        ))
        .await
    }

    /// Send a pre-built `data_sm` and await its response.
    ///
    /// `data_sm` has no `short_message` field: the message body travels in the
    /// `message_payload` TLV (§4.2.2), so this is the only way to send one that
    /// actually carries a message. Attach the TLVs with
    /// [`data_sm::with_tlvs`] / [`data_sm::push_tlv`].
    ///
    /// The session assigns the sequence number: whatever the PDU carries is
    /// overwritten, so responses correlate against this session's window.
    pub async fn send_data_sm_pdu(&self, mut data_sm: data_sm) -> Result<data_sm_resp, SmppError> {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            data_sm.set_sequence_number(sequence_number);
            info!(
                "[{} on server {}] sending data_sm with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            let (tx, rx) = oneshot::channel();

            match self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: data_sm.encode(),
                    oneshot: Some(tx),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("[{} on server {}] unable to send data_sm with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                    return Err(SmppError::ESME_RSYSERR);
                }
            }

            let response = timeout(Duration::from_millis(self.response_timer), rx).await;

            match response {
                Ok(Ok(response)) => {
                    // response can be either data_sm_resp or generic_nack
                    if let Some(data_sm_resp) = response.as_any().downcast_ref::<data_sm_resp>() {
                        info!(
                            "[{} on server {}] received data_sm_resp with sequence_number {}",
                            self.client_address, self.server_address, sequence_number
                        );
                        Ok(data_sm_resp.clone())
                    } else if let Some(generic_nack) =
                        response.as_any().downcast_ref::<generic_nack>()
                    {
                        error!("[{} on server {}] received generic_nack in response to data_sm with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack);
                        Err(generic_nack.get_error())
                    } else {
                        error!("[{} on server {}] received unknown response to data_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
                        Err(SmppError::ESME_RSYSERR)
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "[{} on server {}] unable to receive data_sm_resp: {}",
                        self.client_address, self.server_address, e
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
                Err(_) => {
                    error!(
                        "[{} on server {}] data_sm_resp with sequence_number {} timed out",
                        self.client_address, self.server_address, sequence_number
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
            }
        } else {
            panic!("Can not send data_sm on non TX/TRX bind");
        }
    }

    pub async fn send_cancel_sm(
        &self,
        _service_type: String,
        _message_id: String,
        _source_addr_ton: u8,
        _source_addr_npi: u8,
        _source_addr: String,
        _dest_addr_ton: u8,
        _dest_addr_npi: u8,
        _destination_addr: String,
    ) -> Result<cancel_sm_resp, SmppError> {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            let cancel_sm = cancel_sm::new(
                sequence_number,
                _service_type,
                _message_id,
                _source_addr_ton,
                _source_addr_npi,
                _source_addr,
                _dest_addr_ton,
                _dest_addr_npi,
                _destination_addr,
            );
            info!(
                "[{} on server {}] sending cancel_sm with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            let (tx, rx) = oneshot::channel();

            match self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: cancel_sm.encode(),
                    oneshot: Some(tx),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("[{} on server {}] unable to send cancel_sm with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                    return Err(SmppError::ESME_RSYSERR);
                }
            }

            let response = timeout(Duration::from_millis(self.response_timer), rx).await;

            match response {
                Ok(Ok(response)) => {
                    // response can be either cancel_sm_resp or generic_nack
                    if let Some(cancel_sm_resp) = response.as_any().downcast_ref::<cancel_sm_resp>()
                    {
                        info!(
                            "[{} on server {}] received cancel_sm_resp with sequence_number {}",
                            self.client_address, self.server_address, sequence_number
                        );
                        Ok(cancel_sm_resp.clone())
                    } else if let Some(generic_nack) =
                        response.as_any().downcast_ref::<generic_nack>()
                    {
                        error!("[{} on server {}] received generic_nack in response to cancel_sm with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack);
                        Err(generic_nack.get_error())
                    } else {
                        error!("[{} on server {}] received unknown response to cancel_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
                        Err(SmppError::ESME_RSYSERR)
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "[{} on server {}] unable to receive cancel_sm_resp: {}",
                        self.client_address, self.server_address, e
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
                Err(_) => {
                    error!(
                        "[{} on server {}] cancel_sm_resp with sequence_number {} timed out",
                        self.client_address, self.server_address, sequence_number
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
            }
        } else {
            panic!("Can not send cancel_sm on non TX/TRX bind");
        }
    }

    pub async fn send_query_sm(
        &self,
        message_id: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
    ) -> Result<query_sm_resp, SmppError> {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            let query_sm = query_sm::new(
                sequence_number,
                message_id,
                source_addr_ton,
                source_addr_npi,
                source_addr,
            );
            info!(
                "[{} on server {}] sending query_sm with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            let (tx, rx) = oneshot::channel();

            match self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: query_sm.encode(),
                    oneshot: Some(tx),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("[{} on server {}] unable to send query_sm with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                    return Err(SmppError::ESME_RSYSERR);
                }
            }

            let response = timeout(Duration::from_millis(self.response_timer), rx).await;

            match response {
                Ok(Ok(response)) => {
                    // response can be either query_sm_resp or generic_nack
                    if let Some(query_sm_resp) = response.as_any().downcast_ref::<query_sm_resp>() {
                        info!(
                            "[{} on server {}] received query_sm_resp with sequence_number {}",
                            self.client_address, self.server_address, sequence_number
                        );
                        Ok(query_sm_resp.clone())
                    } else if let Some(generic_nack) =
                        response.as_any().downcast_ref::<generic_nack>()
                    {
                        error!("[{} on server {}] received generic_nack in response to query_sm with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack);
                        Err(generic_nack.get_error())
                    } else {
                        error!("[{} on server {}] received unknown response to query_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
                        Err(SmppError::ESME_RSYSERR)
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "[{} on server {}] unable to receive query_sm_resp: {}",
                        self.client_address, self.server_address, e
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
                Err(_) => {
                    error!(
                        "[{} on server {}] query_sm_resp with sequence_number {} timed out",
                        self.client_address, self.server_address, sequence_number
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
            }
        } else {
            panic!("Can not send query_sm on non TX/TRX bind");
        }
    }

    pub async fn send_replace_sm(
        &self,
        message_id: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        schedule_delivery_time: String,
        validity_period: String,
        registered_delivery: u8,
        sm_default_msg_id: u8,
        short_message: Vec<u8>,
    ) -> Result<replace_sm_resp, SmppError> {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            let replace_sm = replace_sm::new(
                sequence_number,
                message_id,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                schedule_delivery_time,
                validity_period,
                registered_delivery,
                sm_default_msg_id,
                short_message,
            );
            info!(
                "[{} on server {}] sending replace_sm with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            let (tx, rx) = oneshot::channel();

            match self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: replace_sm.encode(),
                    oneshot: Some(tx),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("[{} on server {}] unable to send replace_sm with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                    return Err(SmppError::ESME_RSYSERR);
                }
            }

            let response = timeout(Duration::from_millis(self.response_timer), rx).await;

            match response {
                Ok(Ok(response)) => {
                    // response can be either replace_sm_resp or generic_nack
                    if let Some(replace_sm_resp) =
                        response.as_any().downcast_ref::<replace_sm_resp>()
                    {
                        info!(
                            "[{} on server {}] received replace_sm_resp with sequence_number {}",
                            self.client_address, self.server_address, sequence_number
                        );
                        Ok(replace_sm_resp.clone())
                    } else if let Some(generic_nack) =
                        response.as_any().downcast_ref::<generic_nack>()
                    {
                        error!("[{} on server {}] received generic_nack in response to replace_sm with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack);
                        Err(generic_nack.get_error())
                    } else {
                        error!("[{} on server {}] received unknown response to replace_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
                        Err(SmppError::ESME_RSYSERR)
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "[{} on server {}] unable to receive replace_sm_resp: {}",
                        self.client_address, self.server_address, e
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
                Err(_) => {
                    error!(
                        "[{} on server {}] replace_sm_resp with sequence_number {} timed out",
                        self.client_address, self.server_address, sequence_number
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
            }
        } else {
            panic!("Can not send replace_sm on non TX/TRX bind");
        }
    }

    /// Submit one message to many destinations (SME addresses and/or distribution
    /// lists) in a single `submit_sm_multi` PDU.
    pub async fn send_submit_sm_multi(
        &self,
        service_type: String,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        dest_addresses: Vec<DestAddress>,
        esm_class: u8,
        protocol_id: u8,
        priority_flag: u8,
        schedule_delivery_time: String,
        validity_period: String,
        registered_delivery: u8,
        replace_if_present_flag: u8,
        data_coding: u8,
        sm_default_msg_id: u8,
        short_message: Vec<u8>,
    ) -> Result<submit_sm_multi_resp, SmppError> {
        self.send_submit_sm_multi_pdu(submit_sm_multi::new(
            0, // overwritten by send_submit_sm_multi_pdu, which owns the sequence space
            service_type,
            source_addr_ton,
            source_addr_npi,
            source_addr,
            dest_addresses,
            esm_class,
            protocol_id,
            priority_flag,
            schedule_delivery_time,
            validity_period,
            registered_delivery,
            replace_if_present_flag,
            data_coding,
            sm_default_msg_id,
            short_message,
        ))
        .await
    }

    /// Send a pre-built `submit_sm_multi` and await its response — the path for
    /// attaching optional parameters (TLVs) with
    /// [`submit_sm_multi::with_tlvs`] / [`submit_sm_multi::push_tlv`].
    ///
    /// The session assigns the sequence number: whatever the PDU carries is
    /// overwritten, so responses correlate against this session's window.
    pub async fn send_submit_sm_multi_pdu(
        &self,
        mut submit_sm_multi: submit_sm_multi,
    ) -> Result<submit_sm_multi_resp, SmppError> {
        if self.can_send {
            let sequence_number = self.next_sequence_number();
            submit_sm_multi.set_sequence_number(sequence_number);
            info!(
                "[{} on server {}] sending submit_sm_multi with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            let (tx, rx) = oneshot::channel();

            match self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: submit_sm_multi.encode(),
                    oneshot: Some(tx),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("[{} on server {}] unable to send submit_sm_multi with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                    return Err(SmppError::ESME_RSYSERR);
                }
            }

            let response = timeout(Duration::from_millis(self.response_timer), rx).await;

            match response {
                Ok(Ok(response)) => {
                    // response can be either submit_sm_multi_resp or generic_nack
                    if let Some(submit_sm_multi_resp) =
                        response.as_any().downcast_ref::<submit_sm_multi_resp>()
                    {
                        info!("[{} on server {}] received submit_sm_multi_resp with sequence_number {}", self.client_address, self.server_address, sequence_number);
                        Ok(submit_sm_multi_resp.clone())
                    } else if let Some(generic_nack) =
                        response.as_any().downcast_ref::<generic_nack>()
                    {
                        error!("[{} on server {}] received generic_nack in response to submit_sm_multi with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack);
                        Err(generic_nack.get_error())
                    } else {
                        error!("[{} on server {}] received unknown response to submit_sm_multi with sequence_number {}", self.client_address, self.server_address, sequence_number);
                        Err(SmppError::ESME_RSYSERR)
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "[{} on server {}] unable to receive submit_sm_multi_resp: {}",
                        self.client_address, self.server_address, e
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
                Err(_) => {
                    error!(
                        "[{} on server {}] submit_sm_multi_resp with sequence_number {} timed out",
                        self.client_address, self.server_address, sequence_number
                    );
                    Err(SmppError::ESME_RSYSERR)
                }
            }
        } else {
            panic!("Can not send submit_sm_multi on non TX/TRX bind");
        }
    }

    /// Whether this session can send ESME→SMSC requests (TX or TRX bind). Mirror
    /// of [`ESME::can_receive`]; lets callers gate `send_*` before invoking them
    /// (the `send_*` methods panic on a wrong-direction bind).
    pub fn can_send(&self) -> bool {
        self.can_send
    }
}

/// Fluent builder for a `submit_sm`, returned by [`SMSC::submit_sm`].
///
/// Every field defaults to `0` / empty, so set only what you need and then call
/// [`send`](SubmitSmBuilder::send). String setters take `impl Into<String>` and
/// `short_message` takes `impl Into<Vec<u8>>`.
///
/// ```ignore
/// smsc.submit_sm()
///     .source_addr("12345")
///     .destination_addr("31600000000")
///     .short_message(b"hello")
///     .registered_delivery(1)
///     .tlv(TlvTag::UserMessageReference, 42u16.to_be_bytes())
///     .send()
///     .await?;
/// ```
pub struct SubmitSmBuilder<'a> {
    smsc: &'a SMSC,
    service_type: String,
    source_addr_ton: u8,
    source_addr_npi: u8,
    source_addr: String,
    dest_addr_ton: u8,
    dest_addr_npi: u8,
    destination_addr: String,
    esm_class: u8,
    protocol_id: u8,
    priority_flag: u8,
    schedule_delivery_time: String,
    validity_period: String,
    registered_delivery: u8,
    replace_if_present_flag: u8,
    data_coding: u8,
    sm_default_msg_id: u8,
    short_message: Vec<u8>,
    tlvs: Vec<Tlv>,
}

impl<'a> SubmitSmBuilder<'a> {
    fn new(smsc: &'a SMSC) -> Self {
        SubmitSmBuilder {
            smsc,
            service_type: String::new(),
            source_addr_ton: 0,
            source_addr_npi: 0,
            source_addr: String::new(),
            dest_addr_ton: 0,
            dest_addr_npi: 0,
            destination_addr: String::new(),
            esm_class: 0,
            protocol_id: 0,
            priority_flag: 0,
            schedule_delivery_time: String::new(),
            validity_period: String::new(),
            registered_delivery: 0,
            replace_if_present_flag: 0,
            data_coding: 0,
            sm_default_msg_id: 0,
            short_message: Vec::new(),
            tlvs: Vec::new(),
        }
    }

    pub fn service_type(mut self, v: impl Into<String>) -> Self {
        self.service_type = v.into();
        self
    }
    pub fn source_addr_ton(mut self, v: u8) -> Self {
        self.source_addr_ton = v;
        self
    }
    pub fn source_addr_npi(mut self, v: u8) -> Self {
        self.source_addr_npi = v;
        self
    }
    pub fn source_addr(mut self, v: impl Into<String>) -> Self {
        self.source_addr = v.into();
        self
    }
    pub fn dest_addr_ton(mut self, v: u8) -> Self {
        self.dest_addr_ton = v;
        self
    }
    pub fn dest_addr_npi(mut self, v: u8) -> Self {
        self.dest_addr_npi = v;
        self
    }
    pub fn destination_addr(mut self, v: impl Into<String>) -> Self {
        self.destination_addr = v.into();
        self
    }
    pub fn esm_class(mut self, v: u8) -> Self {
        self.esm_class = v;
        self
    }
    pub fn protocol_id(mut self, v: u8) -> Self {
        self.protocol_id = v;
        self
    }
    pub fn priority_flag(mut self, v: u8) -> Self {
        self.priority_flag = v;
        self
    }
    pub fn schedule_delivery_time(mut self, v: impl Into<String>) -> Self {
        self.schedule_delivery_time = v.into();
        self
    }
    pub fn validity_period(mut self, v: impl Into<String>) -> Self {
        self.validity_period = v.into();
        self
    }
    pub fn registered_delivery(mut self, v: u8) -> Self {
        self.registered_delivery = v;
        self
    }
    pub fn replace_if_present_flag(mut self, v: u8) -> Self {
        self.replace_if_present_flag = v;
        self
    }
    pub fn data_coding(mut self, v: u8) -> Self {
        self.data_coding = v;
        self
    }
    pub fn sm_default_msg_id(mut self, v: u8) -> Self {
        self.sm_default_msg_id = v;
        self
    }
    pub fn short_message(mut self, v: impl Into<Vec<u8>>) -> Self {
        self.short_message = v.into();
        self
    }

    /// Append an optional parameter (TLV) from the SMPP 3.4 table, e.g.
    /// `.tlv(TlvTag::MessagePayload, body)`. Multi-octet values are network byte
    /// order — `42u16.to_be_bytes()`, not `42u16.to_le_bytes()`.
    pub fn tlv(mut self, tag: TlvTag, value: impl Into<Vec<u8>>) -> Self {
        self.tlvs.push(Tlv::from_tag(tag, value.into()));
        self
    }

    /// Append an optional parameter by raw tag — for vendor-specific parameters
    /// (0x1400-0x3FFF) and anything else outside [`TlvTag`].
    pub fn tlv_raw(mut self, tag: u16, value: impl Into<Vec<u8>>) -> Self {
        self.tlvs.push(Tlv::new(tag, value.into()));
        self
    }

    /// Append several optional parameters at once.
    pub fn tlvs(mut self, tlvs: impl IntoIterator<Item = Tlv>) -> Self {
        self.tlvs.extend(tlvs);
        self
    }

    /// Send the assembled `submit_sm` on the session and await its response.
    pub async fn send(self) -> Result<submit_sm_resp, SmppError> {
        self.smsc
            .send_submit_sm_pdu(
                submit_sm::new(
                    0, // assigned by the session
                    self.service_type,
                    self.source_addr_ton,
                    self.source_addr_npi,
                    self.source_addr,
                    self.dest_addr_ton,
                    self.dest_addr_npi,
                    self.destination_addr,
                    self.esm_class,
                    self.protocol_id,
                    self.priority_flag,
                    self.schedule_delivery_time,
                    self.validity_period,
                    self.registered_delivery,
                    self.replace_if_present_flag,
                    self.data_coding,
                    self.sm_default_msg_id,
                    self.short_message,
                )
                .with_tlvs(self.tlvs),
            )
            .await
    }
}

#[async_trait]
/// Callbacks for a client (ESME) session.
///
/// Every method has a default implementation, so an implementor only overrides
/// the ones it needs. `on_deliver_sm` and `on_unbind` default to acking;
/// `on_data_sm` defaults to rejecting with `ESME_RSYSERR`; the notification
/// hooks default to a no-op.
// `session_id: &String` stays `&String` on these trait methods: switching to
// `&str` would break every existing impl's signature, so it is deferred to a
// future major release.
#[allow(clippy::ptr_arg)]
pub trait SmppClientListener {
    async fn on_unbind(
        &self,
        unbind: unbind,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> unbind_resp {
        unbind.accept()
    }
    async fn on_deliver_sm(
        &self,
        deliver_sm: deliver_sm,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> deliver_sm_resp {
        deliver_sm.accept()
    }
    async fn on_data_sm(
        &self,
        data_sm: data_sm,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> data_sm_resp {
        data_sm.reject(SmppError::ESME_RSYSERR)
    }
    async fn on_alert_notification(
        &self,
        _alert_notification: alert_notification,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) {
    }

    /// Notification sent when an SMPP command timed-out (respone_timer triggered)
    async fn on_timeout(&self, _sequence_number: u32, _session_id: &String) {}

    /// Notification sent when an SMSC is in bound state and is ready for receiving commands.
    /// The SMSC wraps the MPSC channel towards the writer thread of the bind
    async fn on_smsc_bound(&self, _smsc: SMSC, _session_id: &String) {}

    /// Notification sent when the SMSC has become unavailable due to a bind being closed or transport error
    /// It is up to the user of this listener to drop the SMSC received on the on_smsc_bound notificiation, any attempt to write to the SMSC after will result in a panic as the MSPC channel is closed
    async fn on_smsc_unbound(&self, _session_id: &String) {}

    /// Notification sent when the session could never be established: the TCP
    /// connect, the TLS handshake or the socket setup failed. No bind follows and
    /// no session task is started, so this is the only notification an attempt
    /// like that produces. `error` names the step and the address that failed.
    ///
    /// Defaulted to a no-op so existing implementors are unaffected.
    async fn on_connection_failed(&self, _error: &str) {}
}

struct StreamWrapper {
    server_address: SocketAddr,
    client_address: SocketAddr,
    read_half: Box<dyn AsyncRead + Unpin + Send>,
    write_half: Box<dyn AsyncWrite + Unpin + Send>,
}

impl StreamWrapper {
    pub fn new_tcp(stream: TcpStream) -> io::Result<Self> {
        // These are the whole reason the constructor is fallible: propagate them
        // instead of unwrapping, or the `io::Result` is decorative.
        let server_address = stream.peer_addr()?;
        let client_address = stream.local_addr()?;

        let (read_half, write_half) = split(stream);
        Ok(StreamWrapper {
            server_address,
            client_address,
            read_half: Box::new(read_half),
            write_half: Box::new(write_half),
        })
    }

    pub fn new_tls(stream: TlsStream<TcpStream>) -> io::Result<Self> {
        let server_address = stream.get_ref().get_ref().get_ref().peer_addr()?;
        let client_address = stream.get_ref().get_ref().get_ref().local_addr()?;

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

    pub async fn split(
        self,
    ) -> (
        Box<dyn AsyncRead + Unpin + Send>,
        Box<dyn AsyncWrite + Unpin + Send>,
    ) {
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

/// Open the transport for a session, plain or TLS.
///
/// Every step names what failed and where, because this is the string the
/// caller sees through `on_connection_failed` — "bind timed out" for a refused
/// connect sent people looking at the wrong end of the session.
async fn establish_stream(
    server_address: &str,
    server_port: u16,
    tls: bool,
) -> Result<StreamWrapper, String> {
    let address = format!("{}:{}", server_address, server_port);

    if tls {
        let connector = native_tls::TlsConnector::builder()
            .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
            .build()
            .map_err(|e| format!("TLS connector setup failed: {e}"))?;
        let connector = TlsConnector::from(connector);

        let stream = TcpStream::connect(&address)
            .await
            .map_err(|e| format!("TCP connect to {address} failed: {e}"))?;
        let stream = connector
            .connect(server_address, stream)
            .await
            .map_err(|e| format!("TLS handshake with {address} failed: {e}"))?;

        StreamWrapper::new_tls(stream)
            .map_err(|e| format!("socket setup for {address} failed: {e}"))
    } else {
        let stream = TcpStream::connect(&address)
            .await
            .map_err(|e| format!("TCP connect to {address} failed: {e}"))?;

        StreamWrapper::new_tcp(stream)
            .map_err(|e| format!("socket setup for {address} failed: {e}"))
    }
}

impl SmppClient {
    pub fn new(
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
        handler: Arc<dyn SmppClientListener + Send + Sync + 'static>,
        window_size: usize,
    ) -> SmppClient {
        SmppClient::new_with_default_timers(
            server_address,
            server_port,
            tls,
            bind_type,
            system_id,
            password,
            system_type,
            addr_ton,
            addr_npi,
            address_range,
            handler,
            5000,
            30000,
            300000,
            30000,
            1500,
            window_size,
        )
    }

    pub fn new_with_default_timers(
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
        handler: Arc<dyn SmppClientListener + Send + Sync + 'static>,
        session_init_timer: u64,
        enquire_link_timer: u64,
        inactivity_timer: u64,
        response_timer: u64,
        buffer_size: usize,
        window_size: usize,
    ) -> SmppClient {
        SmppClient {
            server_address,
            server_port,
            tls,
            bind_type,
            system_id,
            password,
            system_type,
            addr_ton,
            addr_npi,
            address_range,
            handle: None,
            alive: Arc::new(AtomicBool::new(false)),
            handler,
            session_init_timer,
            enquire_link_timer,
            inactivity_timer,
            response_timer,
            buffer_size,
            window_size,
        }
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub async fn start(&mut self) {
        const MAX_PDU_LEN: usize = 1024 * 1024;
        if self.alive.load(Ordering::SeqCst) {
            panic!("Can not start client twice")
        }

        info!(
            "Starting smpp client for server {} with window size: {}",
            self.server_address, self.window_size
        );

        let server_socket_address = self.server_address.clone();
        let server_socker_port = self.server_port;
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
        let addr_ton = self.addr_ton;
        let addr_npi = self.addr_npi;
        let address_range = self.address_range.clone();
        let tls = self.tls;

        // Connect BEFORE spawning the session task. Doing it inside the task left
        // the failure with nowhere to go, so it was `.unwrap()`ed: that panicked a
        // tokio worker, and because the listener still owns the bind channel the
        // panic did not even close it, so the caller waited out its full bind
        // timeout and blamed the bind for a refused connect. Connecting here lets
        // the real cause reach `on_connection_failed`, and makes `start()`
        // returning mean the socket is up.
        let mut stream =
            match establish_stream(&server_socket_address, server_socker_port, tls).await {
                Ok(stream) => stream,
                Err(error) => {
                    error!(
                        "smpp client could not establish a session with server {}:{}: {}",
                        server_socket_address, server_socker_port, error
                    );
                    listener.on_connection_failed(&error).await;
                    return;
                }
            };

        self.handle = Some(tokio::spawn(async move {
            // TODO set connection timeout!
            info!(
                "smpp client connected to server {}, sending bind PDU",
                server_socket_address
            );

            let connection_information = SmppConnectionInformation {
                server_address: stream.peer_addr(),
                client_address: stream.local_addr(),
            };

            let bind_pdu: Vec<u8> = match bind_type {
                BIND_TYPE::RX => bind_receiver::new(
                    1,
                    system_id.clone(),
                    password,
                    system_type,
                    addr_ton,
                    addr_npi,
                    address_range,
                )
                .encode(),
                BIND_TYPE::TX => bind_transmitter::new(
                    1,
                    system_id.clone(),
                    password,
                    system_type,
                    addr_ton,
                    addr_npi,
                    address_range,
                )
                .encode(),
                BIND_TYPE::TRX => bind_transceiver::new(
                    1,
                    system_id.clone(),
                    password,
                    system_type,
                    addr_ton,
                    addr_npi,
                    address_range,
                )
                .encode(),
            };

            // Send bind request. A write failure here means the peer accepted the
            // connection and then went away, which is ordinary; report it the same
            // way a failed connect is reported rather than panicking the task.
            if let Err(error) = stream.write(&bind_pdu).await {
                let error =
                    format!("writing the bind PDU to {server_socket_address} failed: {error}");
                error!("smpp client could not bind: {}", error);
                listener.on_connection_failed(&error).await;
                return;
            }

            info!("Bind PDU sent, waiting for response");
            let session_init_timer_duration =
                tokio::time::Duration::from_millis(session_init_timer);
            // Frame the handshake instead of assuming one read is one PDU. An SMSC
            // with traffic already queued sends it the instant it accepts the bind,
            // so its first deliver_sm/enquire_link routinely lands in the same TCP
            // segment as the bind response. Reading `n` bytes and handing all of
            // them to CommandHeader::decode failed with "PDU length N does not
            // match command_length M" and tore the session down before it started.
            //
            // The handshake fills the buffer the session read loop goes on to use,
            // so whatever arrived behind the bind response is preserved and framed
            // by that loop rather than discarded.
            let mut handshake_buffer = BytesMut::with_capacity(buffer_size.max(1024));
            let first_read = loop {
                match frame_first_pdu(&handshake_buffer, MAX_PDU_LEN) {
                    Framing::Complete(len) => break Ok(Ok(handshake_buffer.split_to(len))),
                    Framing::Invalid(command_length) => {
                        error!(
                            "invalid command_length {} in bind response from server {}, closing connection",
                            command_length, server_socket_address
                        );
                        break Ok(Ok(BytesMut::new()));
                    }
                    Framing::Incomplete => {
                        // StreamWrapper exposes an inherent read(), not AsyncRead,
                        // so accumulate through a chunk.
                        let mut chunk = [0u8; 1024];
                        match timeout(session_init_timer_duration, stream.read(&mut chunk)).await {
                            // EOF before a whole PDU arrived.
                            Ok(Ok(0)) => break Ok(Ok(BytesMut::new())),
                            Ok(Ok(n)) => {
                                handshake_buffer.extend_from_slice(&chunk[..n]);
                                continue;
                            }
                            Ok(Err(e)) => break Ok(Err(e)),
                            Err(e) => break Err(e),
                        }
                    }
                }
            };

            match first_read {
                Ok(Ok(pdu)) => {
                    let pdu_length = pdu.len();

                    // Try read sequence_number in case we need a generic_nack.
                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                    let potential_seq_no = if pdu_length >= 16 {
                        be_u32_at(&pdu, 12)
                    } else {
                        0
                    };
                    let command_header = CommandHeader::decode(&pdu);

                    match command_header {
                        Ok(header) => {
                            if potential_seq_no == 1
                                && header.command_status == SmppError::ESME_ROK as u32
                                && ((bind_type == BIND_TYPE::RX
                                    && header.command_id == CommandId::bind_receiver_resp as u32)
                                    || (bind_type == BIND_TYPE::TX
                                        && header.command_id
                                            == CommandId::bind_transmitter_resp as u32)
                                    || (bind_type == BIND_TYPE::TRX
                                        && header.command_id
                                            == CommandId::bind_transceiver_resp as u32))
                            {
                                let session_id = Uuid::new_v4().to_string();
                                info!("Successfuly bound in {} mode", bind_type);

                                alive.store(true, Ordering::SeqCst);

                                let (read_half, mut writer) = stream.split().await;
                                // Anything that shared a TCP segment with the bind
                                // response is replayed ahead of the socket so the read
                                // loop frames it like any other bytes. Parking it in
                                // `buffer` would not work: the loop only drains after a
                                // read returns data, so a peer that then went quiet
                                // would strand it.
                                let mut reader: Box<dyn AsyncRead + Unpin + Send> =
                                    if handshake_buffer.is_empty() {
                                        read_half
                                    } else {
                                        Box::new(Cursor::new(handshake_buffer).chain(read_half))
                                    };

                                let (tx, mut rx) = channel::<WriteFrame>(100);
                                let pending_requests: Arc<
                                    Mutex<
                                        HashMap<
                                            u32,
                                            (
                                                Instant,
                                                Option<
                                                    tokio::sync::oneshot::Sender<
                                                        Box<dyn SmppReply + Send + Sync + 'static>,
                                                    >,
                                                >,
                                            ),
                                        >,
                                    >,
                                > = Arc::new(Mutex::new(HashMap::new()));

                                let read_timeout = tokio::time::Duration::from_millis(500); // Set a little time-out so we are able to detect if inactivity_timer or enquire_link timers expired
                                let mut buffer = BytesMut::with_capacity(buffer_size);
                                let mut last_read = Instant::now();
                                let sequence_number = Arc::new(AtomicU32::new(2));

                                let writer_alive = alive.clone();
                                let writer_pending_requests = pending_requests.clone();
                                let writer_thread = tokio::task::spawn(async move {
                                    info!(
                                        "[{} on server {}] writer thread started",
                                        connection_information.client_address,
                                        connection_information.server_address
                                    );
                                    while writer_alive.load(Ordering::SeqCst) {
                                        if let Some(frame) = rx.recv().await {
                                            // Register the pending request BEFORE the PDU goes on
                                            // the wire. The peer's response can be read and matched
                                            // while this task is still between the write and the
                                            // insert, and the read loop drops any response it finds
                                            // no pending entry for — leaving the caller to block
                                            // until its response timer expires.
                                            if let Some(our_sequence_number) =
                                                frame.our_sequence_number
                                            {
                                                writer_pending_requests.lock().await.insert(
                                                    our_sequence_number,
                                                    (Instant::now(), frame.oneshot),
                                                );
                                            }
                                            match writer.write(&frame.pdu).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    error!("Unable to write to TCP stream {}", e);
                                                    // The PDU never left, so no response can ever
                                                    // arrive. Drop the registration so the caller
                                                    // fails now instead of waiting out the full
                                                    // response timer.
                                                    if let Some(our_sequence_number) =
                                                        frame.our_sequence_number
                                                    {
                                                        writer_pending_requests
                                                            .lock()
                                                            .await
                                                            .remove(&our_sequence_number);
                                                    }
                                                }
                                            }
                                        } else {
                                            error!("[{} on server {}] writer thread unable to receive frame", connection_information.client_address, connection_information.server_address);
                                            break;
                                        }
                                    }
                                    info!(
                                        "[{} on server {}] writer thread stopped",
                                        connection_information.client_address,
                                        connection_information.server_address
                                    );
                                });

                                let send_enquire_link = alive.clone();
                                let enquire_link_sequence_number = sequence_number.clone();
                                let enquire_link_writer_tx = tx.clone();
                                let (enquire_link_tx, mut enquire_link_rx) = channel::<u32>(100);
                                let enquire_link_ticker = tokio::task::spawn(async move {
                                    info!("[{} on server {}] enquire_link timer started, sending every {}ms", connection_information.client_address, connection_information.server_address, enquire_link_timer);
                                    let mut enquire_link_timer =
                                        interval(Duration::from_millis(enquire_link_timer));
                                    let response_timer = Duration::from_millis(response_timer);
                                    enquire_link_timer.tick().await; // tick for the first time to warm the timer
                                    enquire_link_timer.tick().await; // tick for the second time to start sending enquire_links only on next interval (as we just opened the connection it makes no sense to tick immediately)

                                    while send_enquire_link.load(Ordering::SeqCst) {
                                        let sequence_number = enquire_link_sequence_number
                                            .fetch_add(1, Ordering::SeqCst);
                                        info!("[{} on server {}] sending enquire_link with sequence_number {}", connection_information.client_address, connection_information.server_address, sequence_number);

                                        if let Err(e) = enquire_link_writer_tx
                                            .send(WriteFrame {
                                                our_sequence_number: Some(sequence_number),
                                                pdu: enquire_link::new(sequence_number).encode(),
                                                oneshot: None,
                                            })
                                            .await
                                        {
                                            error!("[{} on server {}] unable to send enquire_link, writer closed: {}", connection_information.client_address, connection_information.server_address, e);
                                            break;
                                        }

                                        let response =
                                            timeout(response_timer, enquire_link_rx.recv()).await;

                                        match response {
                                            Ok(Some(sequence)) => {
                                                // We want the sequence number to match, otherwise we must kill this bind
                                                if sequence != sequence_number {
                                                    error!("[{} on server {}] enquire_link_resp with sequence_number {} did not match sequence_number {}", connection_information.client_address, connection_information.server_address, sequence, sequence_number);
                                                    break;
                                                }
                                            }
                                            Ok(None) => {
                                                error!("[{} on server {}] enquire_link with sequence_number {} channel closed", connection_information.client_address, connection_information.server_address, sequence_number);
                                                break;
                                            }
                                            Err(_) => {
                                                error!("[{} on server {}] enquire_link with sequence_number {} no response within {}ms", connection_information.client_address, connection_information.server_address, sequence_number, response_timer.as_millis());
                                                break;
                                            }
                                        }

                                        // Wait for next interval to send timer again
                                        enquire_link_timer.tick().await;
                                    }
                                    info!(
                                        "[{} on server {}] enquire_link timer stopped",
                                        connection_information.client_address,
                                        connection_information.server_address
                                    );
                                });

                                listener
                                    .on_smsc_bound(
                                        SMSC {
                                            can_send: bind_type == BIND_TYPE::TX
                                                || bind_type == BIND_TYPE::TRX,
                                            tx_channel: tx.clone(),
                                            sequence_number,
                                            server_address: connection_information.server_address,
                                            client_address: connection_information.client_address,
                                            session_id: session_id.clone(),
                                            system_id: system_id.clone(),
                                            response_timer,
                                        },
                                        &session_id,
                                    )
                                    .await;

                                // Bound on one PDU's command_length (see server/state.rs).
                                // Main read loop
                                while alive.load(Ordering::SeqCst) {
                                    let result =
                                        timeout(read_timeout, reader.read_buf(&mut buffer)).await;
                                    match result {
                                        Ok(Ok(frame_length)) => {
                                            if frame_length == 0 {
                                                break; // EOF — the peer closed the connection
                                            }
                                            {
                                                // TCP is a byte stream: `buffer` accumulates across
                                                // reads. Extract every COMPLETE PDU from the front;
                                                // any partial tail is kept for the next read.
                                                let mut cursor = 0;
                                                let mut last_pdu_was_unbind = false;
                                                let mut writer_dead = false;
                                                while buffer.len() - cursor >= 16 {
                                                    let pdu_length: u32 =
                                                        be_u32_at(&buffer, cursor);
                                                    // Reject a bogus length — a length-delimited
                                                    // stream cannot resync from one.
                                                    if (pdu_length as usize) < 16
                                                        || pdu_length as usize > MAX_PDU_LEN
                                                    {
                                                        error!("[{} on server {}] invalid PDU command_length {}, closing connection", connection_information.client_address, connection_information.server_address, pdu_length);
                                                        writer_dead = true;
                                                        break;
                                                    }
                                                    // Incomplete PDU — wait for the rest next read.
                                                    if buffer.len() - cursor < pdu_length as usize {
                                                        break;
                                                    }
                                                    let pdu = buffer
                                                        [cursor..cursor + pdu_length as usize]
                                                        .to_vec();

                                                    // Try read sequence_number in case we need a generic_nack.
                                                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                                                    let potential_seq_no = if pdu_length >= 16 {
                                                        be_u32_at(&pdu, 12)
                                                    } else {
                                                        0
                                                    };
                                                    let command_header =
                                                        CommandHeader::decode(&pdu);
                                                    let tx = tx.clone();

                                                    match command_header {
                                                        Ok(header) => {
                                                            if header.command_id
                                                                == CommandId::deliver_sm as u32
                                                                && (bind_type == BIND_TYPE::RX
                                                                    || bind_type == BIND_TYPE::TRX)
                                                            {
                                                                match deliver_sm::decode(
                                                                    header, &pdu,
                                                                ) {
                                                                    Ok(deliver_sm) => {
                                                                        info!("[{} on server {}] received deliver_sm with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        let handler =
                                                                            listener.clone();
                                                                        let conn =
                                                                            connection_information
                                                                                .clone();
                                                                        let sid =
                                                                            session_id.clone();
                                                                        let tx = tx.clone();
                                                                        tokio::spawn(async move {
                                                                            let resp = handler
                                                                                .on_deliver_sm(
                                                                                    deliver_sm,
                                                                                    &conn, &sid,
                                                                                )
                                                                                .await;
                                                                            if let Err(e) = tx.send(WriteFrame { our_sequence_number: None, pdu: resp.encode(), oneshot: None }).await {
                                                                                error!("[{} on server {}] unable to send deliver_sm_resp, writer closed: {}", conn.client_address, conn.server_address, e);
                                                                            }
                                                                        });
                                                                    }
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode submit_sm: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                        let error = submit_sm::generic_reject(potential_seq_no, error).encode();
                                                                        if tx.send(WriteFrame { our_sequence_number: None, pdu: error, oneshot: None }).await.is_err() {
                                                                            error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                            writer_dead = true;
                                                                            break;
                                                                        }
                                                                    }
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::data_sm as u32
                                                            {
                                                                match data_sm::decode(header, &pdu)
                                                                {
                                                                    Ok(data_sm) => {
                                                                        info!("[{} on server {}] received data_sm with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        let handler =
                                                                            listener.clone();
                                                                        let conn =
                                                                            connection_information
                                                                                .clone();
                                                                        let sid =
                                                                            session_id.clone();
                                                                        let tx = tx.clone();
                                                                        tokio::spawn(async move {
                                                                            let resp = handler
                                                                                .on_data_sm(
                                                                                    data_sm, &conn,
                                                                                    &sid,
                                                                                )
                                                                                .await;
                                                                            if let Err(e) = tx.send(WriteFrame { our_sequence_number: None, pdu: resp.encode(), oneshot: None }).await {
                                                                                error!("[{} on server {}] unable to send data_sm_resp, writer closed: {}", conn.client_address, conn.server_address, e);
                                                                            }
                                                                        });
                                                                    }
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode data_sm: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                        let error = data_sm::generic_reject(potential_seq_no, error).encode();
                                                                        if tx.send(WriteFrame { our_sequence_number: None, pdu: error, oneshot: None }).await.is_err() {
                                                                            error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                            writer_dead = true;
                                                                            break;
                                                                        }
                                                                    }
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::submit_sm_resp as u32
                                                                && (bind_type == BIND_TYPE::TX
                                                                    || bind_type == BIND_TYPE::TRX)
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match submit_sm_resp::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(submit_sm_resp) => {
                                                                                info!("[{} on server {}] received submit_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(submit_sm_resp.clone())) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] submit_sm_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send submit_sm_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for submit_sm", connection_information.client_address, connection_information.server_address);
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode submit_sm_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                                    writer_dead = true;
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::submit_multi_resp
                                                                    as u32
                                                                && (bind_type == BIND_TYPE::TX
                                                                    || bind_type == BIND_TYPE::TRX)
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match submit_sm_multi_resp::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(submit_sm_multi_resp) => {
                                                                                info!("[{} on server {}] received submit_sm_multi_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(submit_sm_multi_resp)) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] submit_sm_multi_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send submit_sm_multi_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for submit_sm_multi", connection_information.client_address, connection_information.server_address);
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode submit_sm_multi_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                                    writer_dead = true;
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::data_sm_resp as u32
                                                                && (bind_type == BIND_TYPE::TX
                                                                    || bind_type == BIND_TYPE::TRX)
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match data_sm_resp::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(data_sm_resp) => {
                                                                                info!("[{} on server {}] received data_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(data_sm_resp)) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] data_sm_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send data_sm_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for data_sm", connection_information.client_address, connection_information.server_address);
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode data_sm_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                                    writer_dead = true;
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::cancel_sm_resp as u32
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match cancel_sm_resp::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(cancel_sm_resp) => {
                                                                                info!("[{} on server {}] received cancel_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(cancel_sm_resp)) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] cancel_sm_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send cancel_sm_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for cancel_sm", connection_information.client_address, connection_information.server_address);
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode cancel_sm_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                                    writer_dead = true;
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::query_sm_resp as u32
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match query_sm_resp::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(query_sm_resp) => {
                                                                                info!("[{} on server {}] received query_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(query_sm_resp)) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] query_sm_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send query_sm_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for query_sm", connection_information.client_address, connection_information.server_address);
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode query_sm_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                                    writer_dead = true;
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::replace_sm_resp as u32
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match replace_sm_resp::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(replace_sm_resp) => {
                                                                                info!("[{} on server {}] received replace_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(replace_sm_resp)) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] replace_sm_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send replace_sm_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for replace_sm", connection_information.client_address, connection_information.server_address);
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode replace_sm_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                                    writer_dead = true;
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::alert_notification
                                                                    as u32
                                                            {
                                                                match alert_notification::decode(
                                                                    header, &pdu,
                                                                ) {
                                                                    Ok(alert_notification) => {
                                                                        info!("[{} on server {}] received alert_notification with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        let handler =
                                                                            listener.clone();
                                                                        let connection_information =
                                                                            connection_information
                                                                                .clone();
                                                                        let submit_sm_session_id =
                                                                            session_id.clone();

                                                                        handler.on_alert_notification(alert_notification.clone(), &connection_information, &submit_sm_session_id).await;
                                                                    }
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode alert_notification: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                        let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                        if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                            error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                            writer_dead = true;
                                                                            break;
                                                                        }
                                                                    }
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::enquire_link as u32
                                                            {
                                                                match enquire_link::decode(
                                                                    header, &pdu,
                                                                ) {
                                                                    Ok(enquire_link) => {
                                                                        info!("[{} on server {}] received enquire_link with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        let enquire_link_resp =
                                                                            enquire_link.accept();
                                                                        info!("[{} on server {}] sending enquire_link_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                        if tx.send(WriteFrame { our_sequence_number: None, pdu: enquire_link_resp.encode(), oneshot: None }).await.is_err() {
                                                                            error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                            writer_dead = true;
                                                                            break;
                                                                        }
                                                                    }
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode enquire_link: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                        let error = submit_sm::generic_reject(potential_seq_no, error).encode();
                                                                        if tx.send(WriteFrame { our_sequence_number: None, pdu: error, oneshot: None }).await.is_err() {
                                                                            error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                            writer_dead = true;
                                                                            break;
                                                                        }
                                                                    }
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::enquire_link_resp
                                                                    as u32
                                                            {
                                                                info!("[{} on server {}] received enquire_link_resp for sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);

                                                                // Forward to enquire_link timer task; if that task has already exited, the read loop will detect that via enquire_link_ticker.is_finished() below and break cleanly.
                                                                if let Err(e) = enquire_link_tx
                                                                    .send(header.sequence_number)
                                                                    .await
                                                                {
                                                                    error!("[{} on server {}] enquire_link timer task gone, dropping enquire_link_resp seq {}: {}", connection_information.client_address, connection_information.server_address, header.sequence_number, e);
                                                                }

                                                                // Cleanup pending requests
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, _)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::unbind as u32
                                                            {
                                                                // Whether or not the unbind fails, we don't care, if any ESMe sends us an unbind we stop the connection, so first we stop the enquire_link timer
                                                                enquire_link_ticker.abort();

                                                                match unbind::decode(header, &pdu) {
                                                                    Ok(unbind) => {
                                                                        let unbind_resp = listener.on_unbind(unbind.clone(), &connection_information, &session_id).await;
                                                                        if let Err(e) = tx.send(WriteFrame { our_sequence_number: None, pdu: unbind_resp.encode(), oneshot: None }).await {
                                                                            error!("[{} on server {}] unable to send unbind_resp, writer closed: {}", connection_information.client_address, connection_information.server_address, e);
                                                                        }
                                                                    }
                                                                    Err(error) => {
                                                                        error!("[{} on server {}] unable to decode unbind: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                        let error =
                                                                            unbind::generic_reject(
                                                                                potential_seq_no,
                                                                                error,
                                                                            )
                                                                            .encode();
                                                                        if let Err(e) = tx.send(WriteFrame { our_sequence_number: None, pdu: error, oneshot: None }).await {
                                                                            error!("[{} on server {}] unable to send unbind generic_reject, writer closed: {}", connection_information.client_address, connection_information.server_address, e);
                                                                        }
                                                                    }
                                                                }

                                                                last_pdu_was_unbind = true;

                                                                break;
                                                            } else if header.command_id
                                                                == CommandId::unbind_resp as u32
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match unbind_resp::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(unbind_resp) => {
                                                                                info!("[{} on server {}] received unbind_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(unbind_resp.clone())) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] unbind_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send unbind_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for unbind", connection_information.client_address, connection_information.server_address);
                                                                                }

                                                                                last_pdu_was_unbind = true;
                                                                                break;
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode unbind_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                let generic_nack = CommandHeader { command_length: 16, command_id: CommandId::generic_nack as u32, command_status: error as u32, sequence_number: potential_seq_no };
                                                                                if tx.send(WriteFrame { our_sequence_number: None, pdu: generic_nack.encode(), oneshot: None }).await.is_err() {
                                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                                    writer_dead = true;
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            } else if header.command_id
                                                                == CommandId::generic_nack as u32
                                                            {
                                                                let mut guard =
                                                                    pending_requests.lock().await;
                                                                if let Some((time, oneshot)) = guard
                                                                    .remove(&header.sequence_number)
                                                                {
                                                                    drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                                                    // Time-out detection
                                                                    let lapsed =
                                                                        time.elapsed().as_millis();
                                                                    if lapsed
                                                                        > response_timer.into()
                                                                    {
                                                                        error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                                                        listener.on_timeout(header.sequence_number, &session_id).await;
                                                                    } else {
                                                                        match generic_nack::decode(
                                                                            header, &pdu,
                                                                        ) {
                                                                            Ok(generic_nack) => {
                                                                                info!("[{} on server {}] received generic_nack with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                                                let connection_information = connection_information.clone();

                                                                                // Send the response to the original sender
                                                                                if let Some(
                                                                                    oneshot,
                                                                                ) = oneshot
                                                                                {
                                                                                    match oneshot.send(Box::new(generic_nack.clone())) {
                                                                                        Ok(_) => {
                                                                                            info!("[{} on server {}] generic_nack sent to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        },
                                                                                        Err(_) => {
                                                                                            error!("[{} on server {}] unable to send generic_nack to original sender", connection_information.client_address, connection_information.server_address);
                                                                                        }
                                                                                    }
                                                                                } else {
                                                                                    error!("[{} on server {}] No oneshot channel registered for generic_nack", connection_information.client_address, connection_information.server_address);
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                error!("[{} on server {}] unable to decode generic_nack: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                                                // Not sending another generic_nack in response to a generic_nack as this would likely create an infinite loop
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                                                }
                                                            } else {
                                                                error!("[{} on server {}] received unsupported PDU with command_id {} and sequence_number {}, sending generic_nack", connection_information.client_address, connection_information.server_address, header.command_id, potential_seq_no);
                                                                let generic_nack = CommandHeader {
                                                                    command_length: 16,
                                                                    command_id:
                                                                        CommandId::generic_nack
                                                                            as u32,
                                                                    command_status:
                                                                        SmppError::ESME_RINVCMDID
                                                                            as u32,
                                                                    sequence_number:
                                                                        potential_seq_no,
                                                                };
                                                                if tx
                                                                    .send(WriteFrame {
                                                                        our_sequence_number: None,
                                                                        pdu: generic_nack.encode(),
                                                                        oneshot: None,
                                                                    })
                                                                    .await
                                                                    .is_err()
                                                                {
                                                                    error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                                    writer_dead = true;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        Err(error) => {
                                                            error!("[{} on server {}] Unable to decode command_header for PDU, sending {:?} in generic_nack", connection_information.client_address, connection_information.server_address, error);
                                                            let generic_nack = CommandHeader {
                                                                command_length: 16,
                                                                command_id: CommandId::generic_nack
                                                                    as u32,
                                                                command_status: error as u32,
                                                                sequence_number: potential_seq_no,
                                                            };
                                                            if tx
                                                                .send(WriteFrame {
                                                                    our_sequence_number: None,
                                                                    pdu: generic_nack.encode(),
                                                                    oneshot: None,
                                                                })
                                                                .await
                                                                .is_err()
                                                            {
                                                                error!("[{} on server {}] writer channel closed, stopping read loop", connection_information.client_address, connection_information.server_address);
                                                            }

                                                            enquire_link_ticker.abort(); // When the TCP stream is closed stop enquiring the link
                                                            writer_dead = true;
                                                            break;
                                                        }
                                                    }

                                                    cursor += pdu_length as usize;
                                                }

                                                // Drop consumed PDUs; keep any partial tail.
                                                let _ = buffer.split_to(cursor);

                                                if last_pdu_was_unbind || writer_dead {
                                                    break; // Break the read loop so we can go to CLOSED state
                                                }

                                                last_read = Instant::now();

                                                // Last thing to do is general time-out detection
                                                let mut guard = pending_requests.lock().await;
                                                let mut timed_out_sequences = Vec::new();

                                                guard.retain(|sequence_number, (time, _)| {
                                                    let lapsed = time.elapsed().as_millis();
                                                    if lapsed > response_timer.into() {
                                                        timed_out_sequences.push(*sequence_number);
                                                        error!("[{} on server {}] Response for sequence_number {} did not come in after {}ms lapsed", connection_information.client_address, connection_information.server_address, sequence_number, lapsed);
                                                        false // Remove this entry
                                                    } else {
                                                        true // Keep this entry
                                                    }
                                                });

                                                // Release the lock before async calls
                                                drop(guard);

                                                // Notify listeners for timed-out requests
                                                for sequence_number in timed_out_sequences {
                                                    listener
                                                        .on_timeout(sequence_number, &session_id)
                                                        .await;
                                                }
                                            }
                                        }
                                        Err(_e) => { /* Nothing to do here as we introduce the interval to not constantly block this thread */
                                        }
                                        Ok(Err(e)) => {
                                            error!(
                                                "[{} on server {}] {} ",
                                                connection_information.client_address,
                                                connection_information.server_address,
                                                e
                                            );
                                            break;
                                        }
                                    }

                                    if enquire_link_ticker.is_finished() {
                                        error!("[{} on server {}] enquire_link thread finished, stopping read loop", connection_information.client_address, connection_information.server_address);
                                        break;
                                    } else if last_read.elapsed().as_millis()
                                        > inactivity_timer.into()
                                    {
                                        // Please note, it is more likely that the enquire_link timer stopped earlier as it expects a response likely within 2000ms (default) but in some weird scenario that it it's stuck we can always trigger the inactivity timer by keeping
                                        // track of when the last packet was read
                                        error!("[{} on server {}] inactivity_timer triggered after {}ms, closing TCP connection", connection_information.client_address, connection_information.server_address, inactivity_timer);
                                        break;
                                    }
                                }

                                listener.on_smsc_unbound(&session_id).await;

                                info!(
                                    "[{} on server {}] {} going to CLOSED state",
                                    connection_information.client_address,
                                    connection_information.server_address,
                                    bind_type
                                );
                                alive.store(false, Ordering::SeqCst);

                                enquire_link_ticker.abort(); // orphaned ticker = per-session leak
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
                        }
                        Err(_) => error!("Unable to decode bind response"),
                    }
                }
                _ => error!("No bind response from server in {}ms", session_init_timer),
            }
        }));
    }

    pub async fn stop(&mut self) {
        // We except the user of this code to send unbind before stopping the client
        info!("Stopping smpp client");
        self.alive.store(false, Ordering::SeqCst);
        match self.handle.take() {
            Some(handle) => handle.abort(),
            // Reachable whenever start() returned without spawning, i.e. the
            // session could not be established. Also hit by Drop after an
            // explicit stop(). Either way there is nothing to abort.
            None => debug!("stop() called on an smpp client that is not running"),
        }
    }
}

impl Drop for SmppClient {
    fn drop(&mut self) {
        if self.alive.load(Ordering::SeqCst) {
            futures::executor::block_on(self.stop());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compiles only because every `SmppClientListener` method has a default —
    // this empty impl is the proof that the defaults exist.
    struct MinimalClient;
    #[async_trait]
    impl SmppClientListener for MinimalClient {}

    #[test]
    fn minimal_client_listener_compiles() {
        let _listener: &dyn SmppClientListener = &MinimalClient;
    }

    // The `submit_sm()` builder must forward its fields into the on-wire PDU in
    // the right positions. Build one against a fake (channel-backed) SMSC,
    // capture the encoded frame, decode it, and assert the fields round-trip.
    #[tokio::test]
    async fn submit_sm_builder_maps_fields() {
        let (tx, mut rx) = channel(1);
        let smsc = SMSC {
            client_address: "127.0.0.1:1234".parse().unwrap(),
            server_address: "127.0.0.1:2775".parse().unwrap(),
            session_id: "s".to_string(),
            system_id: "sys".to_string(),
            can_send: true,
            tx_channel: tx,
            sequence_number: Arc::new(AtomicU32::new(1)),
            response_timer: 50,
        };
        let _sender = tokio::spawn(async move {
            let _ = smsc
                .submit_sm()
                .source_addr("12345")
                .destination_addr("31600000000")
                .short_message(b"hello")
                .registered_delivery(1)
                .data_coding(8)
                .send()
                .await;
        });
        let frame = rx.recv().await.expect("builder writes a frame");
        let header = CommandHeader::decode(&frame.pdu).expect("valid header");
        let pdu = submit_sm::decode(header, &frame.pdu).expect("valid submit_sm");
        assert_eq!(pdu.source_addr, "12345");
        assert_eq!(pdu.destination_addr, "31600000000");
        assert_eq!(pdu.short_message, b"hello".to_vec());
        assert_eq!(pdu.registered_delivery, 1);
        assert_eq!(pdu.data_coding, 8);
    }
}
