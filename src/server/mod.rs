use async_trait::async_trait;
use log::{error, info};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc::Sender, oneshot},
    task::{self, JoinHandle},
    time::timeout,
};
use uuid::Uuid;

use crate::{
    alert_notification, bind_receiver, bind_receiver_resp, bind_transceiver, bind_transceiver_resp,
    bind_transmitter, bind_transmitter_resp, cancel_sm, cancel_sm_resp,
    common::{CommandHeader, CommandId, SmppError},
    data_sm, data_sm_resp, deliver_sm, deliver_sm_resp, generic_nack, query_sm, query_sm_resp,
    replace_sm, replace_sm_resp,
    server::state::OPEN,
    submit_sm, submit_sm_multi, submit_sm_multi_resp, submit_sm_resp, unbind, unbind_resp,
    SmppConnectionInformation, WriteFrame,
};

mod state;

pub struct SmppServer {
    address: IpAddr,
    port: u16,
    handle: Option<JoinHandle<()>>,
    alive: Arc<AtomicBool>,
    handler: Arc<dyn SmppServerListener + Send + Sync + 'static>,
    session_init_timer: u64,
    enquire_link_timer: u64,
    inactivity_timer: u64,
    response_timer: u64,
    buffer_size: usize,
}

pub struct ESME {
    pub server_address: SocketAddr,
    pub client_address: SocketAddr,
    pub session_id: String,
    pub system_id: String,
    pub can_receive: bool,
    tx_channel: Sender<WriteFrame>,
    sequence_number: Arc<AtomicU32>,
    response_timer: u64,
}

impl ESME {
    fn next_sequence_number(&self) -> u32 {
        self.sequence_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Start building a `deliver_sm` to send on this session — an ergonomic
    /// alternative to the 17-argument [`send_deliver_sm`](ESME::send_deliver_sm).
    pub fn deliver_sm(&self) -> DeliverSmBuilder<'_> {
        DeliverSmBuilder::new(self)
    }

    pub async fn send_deliver_sm(
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
    ) -> Result<deliver_sm_resp, SmppError> {
        if self.can_receive {
            let sequence_number = self.next_sequence_number();
            let deliver_sm = deliver_sm::new(
                sequence_number,
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
            );
            info!(
                "[{} on server {}] sending deliver_sm with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            let (tx, rx) = oneshot::channel();

            let result = self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: deliver_sm.encode(),
                    oneshot: Some(tx),
                })
                .await;

            match result {
                Ok(_) => {
                    match timeout(Duration::from_millis(self.response_timer), rx).await {
                        Ok(Ok(response)) => {
                            // response can be either deliver_sm_resp or generic_nack
                            if let Some(deliver_sm_resp) =
                                response.as_any().downcast_ref::<deliver_sm_resp>()
                            {
                                info!("[{} on server {}] received deliver_sm_resp with sequence_number {}", self.client_address, self.server_address, sequence_number);
                                Ok(deliver_sm_resp.clone())
                            } else if let Some(generic_nack) =
                                response.as_any().downcast_ref::<generic_nack>()
                            {
                                error!("[{} on server {}] received generic_nack in response to deliver_sm with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack.get_error());
                                Err(generic_nack.get_error())
                            } else {
                                error!("[{} on server {}] received unknown response type in response to deliver_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
                                Err(SmppError::ESME_RSYSERR)
                            }
                        }
                        Ok(Err(e)) => {
                            error!(
                                "[{} on server {}] unable to receive deliver_sm_resp: {}",
                                self.client_address, self.server_address, e
                            );
                            Err(SmppError::ESME_RSYSERR)
                        }
                        Err(_) => {
                            error!("[{} on server {}] deliver_sm_resp with sequence_number {} timed out", self.client_address, self.server_address, sequence_number);
                            Err(SmppError::ESME_RSYSERR)
                        }
                    }
                }
                Err(e) => {
                    error!("[{} on server {}] unable to send deliver_sm with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                    Err(SmppError::ESME_RSYSERR)
                }
            }
        } else {
            panic!("Can not send deliver_sm on non RX/TRX bind");
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
                    error!("[{} on server {}] received generic_nack in response to unbind with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack.get_error());
                    Err(generic_nack.get_error())
                } else {
                    error!("[{} on server {}] received unknown response type in response to unbind with sequence_number {}", self.client_address, self.server_address, sequence_number);
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
        let sequence_number = self.next_sequence_number();
        let data_sm = data_sm::new(
            sequence_number,
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
        );
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
            Ok(_) => {
                match timeout(Duration::from_millis(self.response_timer), rx).await {
                    Ok(Ok(response)) => {
                        // response can be either data_sm_resp or generic_nack
                        if let Some(data_sm_resp) = response.as_any().downcast_ref::<data_sm_resp>()
                        {
                            info!(
                                "[{} on server {}] received data_sm_resp with sequence_number {}",
                                self.client_address, self.server_address, sequence_number
                            );
                            Ok(data_sm_resp.clone())
                        } else if let Some(generic_nack) =
                            response.as_any().downcast_ref::<generic_nack>()
                        {
                            error!("[{} on server {}] received generic_nack in response to data_sm with sequence_number {}: {:?}", self.client_address, self.server_address, sequence_number, generic_nack.get_error());
                            Err(generic_nack.get_error())
                        } else {
                            error!("[{} on server {}] received unknown response type in response to data_sm with sequence_number {}", self.client_address, self.server_address, sequence_number);
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
            }
            Err(e) => {
                error!("[{} on server {}] unable to send data_sm with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                Err(SmppError::ESME_RSYSERR)
            }
        }
    }

    pub async fn send_alert_notification(
        &self,
        source_addr_ton: u8,
        source_addr_npi: u8,
        source_addr: String,
        esme_addr_ton: u8,
        esme_addr_npi: u8,
        esme_addr: String,
        ms_availability_status: Option<u8>,
    ) -> u32 {
        if self.can_receive {
            let sequence_number = self.next_sequence_number();
            let alert_notification = alert_notification::new(
                sequence_number,
                source_addr_ton,
                source_addr_npi,
                source_addr,
                esme_addr_ton,
                esme_addr_npi,
                esme_addr,
                ms_availability_status,
            );
            info!(
                "[{} on server {}] sending alert_notification with sequence_number {}",
                self.client_address, self.server_address, sequence_number
            );

            // No one-shot as this is a notification and we do not expect a response
            match self
                .tx_channel
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: alert_notification.encode(),
                    oneshot: None,
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("[{} on server {}] unable to send alert_notification with sequence_number {} to writer thread: {}", self.client_address, self.server_address, sequence_number, e);
                }
            }
            sequence_number
        } else {
            panic!("Can not send alert_notification on non RX/TRX bind");
        }
    }
}

/// Fluent builder for a `deliver_sm`, returned by [`ESME::deliver_sm`].
///
/// Every field defaults to `0` / empty, so set only what you need and then call
/// [`send`](DeliverSmBuilder::send). String setters take `impl Into<String>` and
/// `short_message` takes `impl Into<Vec<u8>>`.
///
/// ```ignore
/// esme.deliver_sm()
///     .source_addr("31600000000")
///     .destination_addr("12345")
///     .short_message(b"hello")
///     .send()
///     .await?;
/// ```
pub struct DeliverSmBuilder<'a> {
    esme: &'a ESME,
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
}

impl<'a> DeliverSmBuilder<'a> {
    fn new(esme: &'a ESME) -> Self {
        DeliverSmBuilder {
            esme,
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

    /// Send the assembled `deliver_sm` on the session and await its response.
    pub async fn send(self) -> Result<deliver_sm_resp, SmppError> {
        self.esme
            .send_deliver_sm(
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
            .await
    }
}

#[async_trait]
/// Callbacks for a server (SMSC) session.
///
/// Every method has a default implementation, so an implementor only overrides
/// the ones it needs. Binds default to `reject(ESME_RBINDFAIL)`, `on_submit_sm`
/// / `on_data_sm` to `reject(ESME_RSYSERR)`, `on_cancel_sm` to
/// `reject(ESME_RCANCELFAIL)`, `on_unbind` to acking, and the notification hooks
/// to a no-op. Override `on_bind_transceiver` + `on_submit_sm` for a typical SMSC.
// `session_id: &String` stays `&String` on these trait methods: switching to
// `&str` would break every existing impl's signature, so it is deferred to a
// future major release.
#[allow(clippy::ptr_arg)]
pub trait SmppServerListener {
    async fn on_bind_transmitter(
        &self,
        bind_transmitter: bind_transmitter,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> bind_transmitter_resp {
        bind_transmitter.reject(SmppError::ESME_RBINDFAIL)
    }
    async fn on_bind_receiver(
        &self,
        bind_receiver: bind_receiver,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> bind_receiver_resp {
        bind_receiver.reject(SmppError::ESME_RBINDFAIL)
    }
    async fn on_bind_transceiver(
        &self,
        bind_transceiver: bind_transceiver,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> bind_transceiver_resp {
        bind_transceiver.reject(SmppError::ESME_RBINDFAIL)
    }
    async fn on_unbind(
        &self,
        unbind: unbind,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> unbind_resp {
        unbind.accept()
    }
    async fn on_submit_sm(
        &self,
        submit_sm: submit_sm,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> submit_sm_resp {
        submit_sm.reject(SmppError::ESME_RSYSERR)
    }
    async fn on_submit_sm_multi(
        &self,
        submit_sm_multi: submit_sm_multi,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> submit_sm_multi_resp {
        submit_sm_multi.reject(SmppError::ESME_RSYSERR)
    }
    async fn on_cancel_sm(
        &self,
        cancel_sm: cancel_sm,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> cancel_sm_resp {
        cancel_sm.reject(SmppError::ESME_RCANCELFAIL)
    }
    async fn on_query_sm(
        &self,
        query_sm: query_sm,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> query_sm_resp {
        query_sm.reject(SmppError::ESME_RQUERYFAIL)
    }
    async fn on_replace_sm(
        &self,
        replace_sm: replace_sm,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> replace_sm_resp {
        replace_sm.reject(SmppError::ESME_RREPLACEFAIL)
    }
    async fn on_data_sm(
        &self,
        data_sm: data_sm,
        _connection_information: &SmppConnectionInformation,
        _session_id: &String,
    ) -> data_sm_resp {
        data_sm.reject(SmppError::ESME_RSYSERR)
    }

    /// Notification sent when an SMPP command timed-out (respone_timer triggered)
    async fn on_timeout(&self, _sequence_number: u32, _session_id: &String) {}

    /// Notification sent when an ESME is in bound state and is ready for receiving commands.
    /// The ESME wraps the MPSC channel towards the writer thread of the bind
    async fn on_esme_bound(&self, _esme: ESME, _session_id: &String) {}

    /// Notification sent when the ESME has become unavailable due to a bind being closed or transport error
    /// It is up to the user of this listener to drop the ESME received on the on_esme_bound notificiation, any attempt to write to the ESME after will result in a panic as the MSPC channel is closed
    async fn on_esme_unbound(&self, _session_id: &String) {}
}

impl SmppServer {
    pub fn new(
        address: IpAddr,
        port: u16,
        handler: Arc<dyn SmppServerListener + Send + Sync>,
    ) -> SmppServer {
        SmppServer::new_with_default_timers(
            address, port, handler, 5000, 30000, 300000, 30000, 1500,
        )
    }

    pub fn new_with_default_timers(
        address: IpAddr,
        port: u16,
        handler: Arc<dyn SmppServerListener + Send + Sync>,
        session_init_timer: u64,
        enquire_link_timer: u64,
        inactivity_timer: u64,
        response_timer: u64,
        buffer_size: usize,
    ) -> SmppServer {
        SmppServer {
            address,
            port,
            handle: None,
            alive: Arc::new(AtomicBool::new(false)),
            handler,
            session_init_timer,
            enquire_link_timer,
            inactivity_timer,
            response_timer,
            buffer_size,
        }
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub async fn start(&mut self) {
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
                        let session_init_timer_duration =
                            tokio::time::Duration::from_millis(session_init_timer);
                        task::spawn(async move {
                            let session_id = Uuid::new_v4().to_string();
                            let session_state = OPEN { session_id };
                            let connection_information = SmppConnectionInformation {
                                server_address: server_socket_address,
                                client_address: client_socket_address,
                            };

                            info!(
                                "Got a connection from {} on server {}, waiting {}ms for bind",
                                connection_information.client_address,
                                connection_information.server_address,
                                session_init_timer
                            );
                            let mut buffer = [0; 1024]; // Not using BytesMut here as we always first get a bind before expecting big traffic so choose a low buffer size
                            let first_read =
                                timeout(session_init_timer_duration, stream.read(&mut buffer))
                                    .await;

                            match first_read {
                                Ok(Ok(n)) => {
                                    let pdu = buffer[0..n].to_vec();
                                    let pdu_length = pdu.len();

                                    // Try read sequence_number in case we need a generic_nack.
                                    // If we have at least 16 bytes we are able to read sequence number, if not set it to 0x00000000 as advised in SMPP 3.4 spec
                                    let potential_seq_no = if pdu_length >= 16 {
                                        u32::from_be_bytes(
                                            pdu[12..16]
                                                .try_into()
                                                .expect("Can not read sequence_number"),
                                        )
                                    } else {
                                        0
                                    };
                                    let command_header = CommandHeader::decode(&pdu);

                                    match command_header {
                                        Ok(header) => {
                                            if header.command_id == CommandId::bind_receiver as u32
                                            {
                                                match bind_receiver::decode(header, &pdu) {
                                                    Ok(bind_receiver) => {
                                                        let system_id =
                                                            bind_receiver.system_id.clone();
                                                        let bind_receiver_resp = handler
                                                            .on_bind_receiver(
                                                                bind_receiver.clone(),
                                                                &connection_information,
                                                                &session_state.session_id,
                                                            )
                                                            .await;
                                                        let session_state = session_state
                                                            .bind_receiver(
                                                                stream,
                                                                bind_receiver,
                                                                bind_receiver_resp,
                                                                &connection_information,
                                                                handler,
                                                            )
                                                            .await;
                                                        // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                        if session_state.is_ok() {
                                                            let state = session_state.unwrap();
                                                            state
                                                                .read_loop(
                                                                    system_id,
                                                                    enquire_link_timer,
                                                                    inactivity_timer,
                                                                    response_timer,
                                                                    buffer_size,
                                                                )
                                                                .await; // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                        }
                                                    }
                                                    Err(error) => {
                                                        error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                        let error = bind_receiver::generic_reject(
                                                            potential_seq_no,
                                                            error,
                                                        )
                                                        .encode();
                                                        stream
                                                            .write_all(&error)
                                                            .await
                                                            .expect("Can not write to stream");
                                                    }
                                                }
                                            } else if header.command_id
                                                == CommandId::bind_transmitter as u32
                                            {
                                                match bind_transmitter::decode(header, &pdu) {
                                                    Ok(bind_transmitter) => {
                                                        let system_id =
                                                            bind_transmitter.system_id.clone();
                                                        let bind_transmitter_resp = handler
                                                            .on_bind_transmitter(
                                                                bind_transmitter.clone(),
                                                                &connection_information,
                                                                &session_state.session_id,
                                                            )
                                                            .await;
                                                        let session_state = session_state
                                                            .bind_transmitter(
                                                                stream,
                                                                bind_transmitter,
                                                                &bind_transmitter_resp,
                                                                &connection_information,
                                                                handler,
                                                            )
                                                            .await;
                                                        // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                        if session_state.is_ok() {
                                                            let state = session_state.unwrap();
                                                            state
                                                                .read_loop(
                                                                    system_id,
                                                                    enquire_link_timer,
                                                                    inactivity_timer,
                                                                    response_timer,
                                                                    buffer_size,
                                                                )
                                                                .await; // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                        }
                                                    }
                                                    Err(error) => {
                                                        error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                        let error =
                                                            bind_transmitter::generic_reject(
                                                                potential_seq_no,
                                                                error,
                                                            )
                                                            .encode();
                                                        stream
                                                            .write_all(&error)
                                                            .await
                                                            .expect("Can not write to stream");
                                                    }
                                                }
                                            } else if header.command_id
                                                == CommandId::bind_transceiver as u32
                                            {
                                                match bind_transceiver::decode(header, &pdu) {
                                                    Ok(bind_transceiver) => {
                                                        let system_id =
                                                            bind_transceiver.system_id.clone();
                                                        let bind_transceiver_resp = handler
                                                            .on_bind_transceiver(
                                                                bind_transceiver.clone(),
                                                                &connection_information,
                                                                &session_state.session_id,
                                                            )
                                                            .await;
                                                        let session_state = session_state
                                                            .bind_transceiver(
                                                                stream,
                                                                bind_transceiver,
                                                                &bind_transceiver_resp,
                                                                &connection_information,
                                                                handler,
                                                            )
                                                            .await;
                                                        // Note from now on the state handler is handling writes to the stream, so we only need to check whether it succeeded or not to be able to go into session mode
                                                        if session_state.is_ok() {
                                                            let state = session_state.unwrap();
                                                            state
                                                                .read_loop(
                                                                    system_id,
                                                                    enquire_link_timer,
                                                                    inactivity_timer,
                                                                    response_timer,
                                                                    buffer_size,
                                                                )
                                                                .await; // When this function stops either the TCP connection was interrupted or some unbind event happened. Nothing else todo.
                                                        }
                                                    }
                                                    Err(error) => {
                                                        error!("Connection from {} on server {}, unable to decode bind_receiver", connection_information.client_address, connection_information.server_address);
                                                        let error =
                                                            bind_transceiver::generic_reject(
                                                                potential_seq_no,
                                                                error,
                                                            )
                                                            .encode();
                                                        stream
                                                            .write_all(&error)
                                                            .await
                                                            .expect("Can not write to stream");
                                                    }
                                                }
                                            } else {
                                                // Only allow bind commands, if not a bind command tell ESME about invalid bind status
                                                error!("Did not expect command_id {} as bind not established yet, sending ESME_RINVBNDSTS in generick_nack", header.command_id);

                                                let generic_nack = generic_nack::new(
                                                    SmppError::ESME_RINVBNDSTS,
                                                    potential_seq_no,
                                                );
                                                stream
                                                    .write_all(&generic_nack.encode())
                                                    .await
                                                    .expect("Can not write to stream");
                                            }
                                        }
                                        Err(error) => {
                                            error!("Unable to decode command_header for PDU, sending {:?} in generic_nack", error);
                                            let generic_nack =
                                                generic_nack::new(error, potential_seq_no);
                                            stream
                                                .write_all(&generic_nack.encode())
                                                .await
                                                .expect("Can not write to stream");
                                        }
                                    }
                                }
                                _ => {
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

    pub async fn stop(&mut self) {
        // TODO send unbind!!

        info!("Stopping smpp server");
        self.alive.store(false, Ordering::SeqCst);
        self.handle
            .take()
            .expect("Called stop on non-running thread")
            .abort();
    }
}

impl Drop for SmppServer {
    fn drop(&mut self) {
        if self.alive.load(Ordering::SeqCst) {
            futures::executor::block_on(self.stop());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compiles only because every `SmppServerListener` method has a default —
    // this empty impl is the proof that the defaults exist.
    struct MinimalServer;
    #[async_trait]
    impl SmppServerListener for MinimalServer {}

    #[test]
    fn minimal_server_listener_compiles() {
        let _listener: &dyn SmppServerListener = &MinimalServer;
    }
}
