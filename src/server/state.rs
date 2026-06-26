use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime},
};

use bytes::BytesMut;

use log::{error, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc::channel, Mutex},
    time::{interval, timeout},
};

use crate::{
    bind_receiver, bind_receiver_resp, bind_transceiver, bind_transceiver_resp, bind_transmitter,
    bind_transmitter_resp, cancel_sm, common::SmppError, data_sm, data_sm_resp, deliver_sm_resp,
    enquire_link, generic_nack, server::ESME, submit_sm, unbind, CommandHeader, CommandId,
    SmppReply, SmppServerListener, WriteFrame,
};

use crate::SmppConnectionInformation;

#[derive(Debug, Clone, PartialEq)]
pub enum BOUND_TYPE {
    BOUND_RX,
    BOUND_TX,
    BOUND_TRX,
}

impl fmt::Display for BOUND_TYPE {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

async fn read_loop(
    bound_type: BOUND_TYPE,
    listener: Arc<dyn SmppServerListener + Send + Sync + 'static>,
    stream: TcpStream,
    connection_information: SmppConnectionInformation,
    system_id: String,
    enquire_link_timer: u64,
    inactivity_timer: u64,
    response_timer: u64,
    buffer_size: usize,
    session_id: String,
) -> CLOSED {
    info!("[{} on server {}] {} going into read_loop with enquire_link_timer {}ms and inactivity_timer {}ms and read buffer size {} bytes", connection_information.client_address, connection_information.server_address, bound_type, enquire_link_timer, inactivity_timer, buffer_size);
    let sequence_number = Arc::new(AtomicU32::new(1));
    let alive = Arc::new(AtomicBool::new(false));
    alive.store(true, Ordering::SeqCst);

    let (mut reader, mut writer) = stream.into_split();

    let (tx, mut rx) = channel::<WriteFrame>(100);

    let pending_requests: Arc<
        Mutex<
            HashMap<
                u32,
                (
                    SystemTime,
                    Option<
                        tokio::sync::oneshot::Sender<Box<dyn SmppReply + Send + Sync + 'static>>,
                    >,
                ),
            >,
        >,
    > = Arc::new(Mutex::new(HashMap::new()));

    let writer_alive = alive.clone();
    let writer_pending_requests = pending_requests.clone();
    let writer_thread = tokio::task::spawn(async move {
        info!(
            "[{} on server {}] writer thread started",
            connection_information.client_address, connection_information.server_address
        );
        while writer_alive.load(Ordering::SeqCst) {
            if let Some(frame) = rx.recv().await {
                match writer.write(&frame.pdu).await {
                    Ok(_) => {
                        if frame.our_sequence_number.is_some() {
                            writer_pending_requests.lock().await.insert(
                                frame.our_sequence_number.unwrap(),
                                (SystemTime::now(), frame.oneshot),
                            );
                        }
                    }
                    Err(e) => {
                        error!("Unable to write to TCP stream {}", e)
                    }
                }
            } else {
                error!(
                    "[{} on server {}] writer thread unable to receive frame",
                    connection_information.client_address, connection_information.server_address
                );
                break;
            }
        }
        info!(
            "[{} on server {}] writer thread stopped",
            connection_information.client_address, connection_information.server_address
        );
    });

    let send_enquire_link = alive.clone();
    let enquire_link_sequence_number = sequence_number.clone();
    let enquire_link_writer_tx = tx.clone();
    let (enquire_link_tx, mut enquire_link_rx) = channel::<u32>(100); // Explicitly a sync channel
    let enquire_link_ticker = tokio::task::spawn(async move {
        info!(
            "[{} on server {}] enquire_link timer started, sending every {}ms",
            connection_information.client_address,
            connection_information.server_address,
            enquire_link_timer
        );
        let mut enquire_link_timer = interval(Duration::from_millis(enquire_link_timer));
        let response_timer = Duration::from_millis(response_timer);
        enquire_link_timer.tick().await; // tick for the first time to warm the timer
        enquire_link_timer.tick().await; // tick for the second time to start sending enquire_links only on next interval (as we just opened the connection it makes no sense to tick immediately)

        while send_enquire_link.load(Ordering::SeqCst) {
            let sequence_number = enquire_link_sequence_number.fetch_add(1, Ordering::SeqCst);
            info!(
                "[{} on server {}] sending enquire_link with sequence_number {}",
                connection_information.client_address,
                connection_information.server_address,
                sequence_number
            );

            if let Err(e) = enquire_link_writer_tx
                .send(WriteFrame {
                    our_sequence_number: Some(sequence_number),
                    pdu: enquire_link::new(sequence_number).encode(),
                    oneshot: None,
                })
                .await
            {
                error!(
                    "[{} on server {}] unable to send enquire_link, writer closed: {}",
                    connection_information.client_address, connection_information.server_address, e
                );
                break;
            }

            let response = timeout(response_timer, enquire_link_rx.recv()).await;

            match response {
                Ok(Some(sequence)) => {
                    // We want the sequence number to match, otherwise we must kill this bind
                    if sequence != sequence_number {
                        error!("[{} on server {}] received enquire_link_resp with sequence_number {} did not match sequence_number {}", connection_information.client_address, connection_information.server_address, sequence, sequence_number);
                        break;
                    }
                }
                Ok(None) => {
                    error!("[{} on server {}] sent enquire_link with sequence_number {} channel closed", connection_information.client_address, connection_information.server_address, sequence_number);
                    break;
                }
                Err(_) => {
                    error!("[{} on server {}] sent enquire_link with sequence_number {} no response within {}ms", connection_information.client_address, connection_information.server_address, sequence_number, response_timer.as_millis());
                    break;
                }
            }

            // Wait for next interval to send timer again
            enquire_link_timer.tick().await;
        }
        info!(
            "[{} on server {}] enquire_link timer stopped",
            connection_information.client_address, connection_information.server_address
        );
    });

    let read_timeout = tokio::time::Duration::from_millis(500); // Set a little time-out so we are able to detect if inactivity_timer or enquire_link timers expired
    let mut buffer = BytesMut::with_capacity(buffer_size);
    let mut last_read = Instant::now();

    listener
        .on_esme_bound(
            ESME {
                client_address: connection_information.client_address,
                server_address: connection_information.server_address,
                system_id: system_id.clone(),
                session_id: session_id.clone(),
                can_receive: bound_type == BOUND_TYPE::BOUND_RX
                    || bound_type == BOUND_TYPE::BOUND_TRX,
                tx_channel: tx.clone(),
                sequence_number,
                response_timer,
            },
            &session_id,
        )
        .await;

    loop {
        let result = timeout(read_timeout, reader.read_buf(&mut buffer)).await;
        match result {
            Ok(Ok(frame_length)) => {
                let frame = buffer[0..frame_length].to_vec();
                if frame_length >= 16 {
                    // anything else we don't want
                    let mut cursor = 0;
                    let mut last_pdu_was_unbind = false;
                    let mut writer_dead = false;
                    while cursor < frame_length && frame_length - cursor >= 16 {
                        // Only read when we have bytes left in the frame AND at least 16 bytes left in the buffer (we choose to ignore and not decode)
                        let pdu_length: u32 = u32::from_be_bytes(
                            frame[cursor..cursor + 4]
                                .try_into()
                                .expect("Can not read PDU length"),
                        );
                        let pdu = buffer[cursor..cursor + pdu_length as usize].to_vec();

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
                        let tx = tx.clone();

                        match command_header {
                            Ok(header) => {
                                if header.command_id == CommandId::submit_sm as u32
                                    && (bound_type == BOUND_TYPE::BOUND_TX
                                        || bound_type == BOUND_TYPE::BOUND_TRX)
                                {
                                    match submit_sm::decode(header, &pdu) {
                                        Ok(submit_sm) => {
                                            info!("[{} on server {}] received submit_sm with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                            let handler = listener.clone();
                                            let conn = connection_information.clone();
                                            let sid = session_id.clone();
                                            let tx = tx.clone();
                                            tokio::spawn(async move {
                                                let resp = handler
                                                    .on_submit_sm(submit_sm, &conn, &sid)
                                                    .await;
                                                if let Err(e) = tx
                                                    .send(WriteFrame {
                                                        our_sequence_number: None,
                                                        pdu: resp.encode(),
                                                        oneshot: None,
                                                    })
                                                    .await
                                                {
                                                    error!("[{} on server {}] unable to send submit_sm_resp, writer closed: {}", conn.client_address, conn.server_address, e);
                                                }
                                            });
                                        }
                                        Err(error) => {
                                            error!("[{} on server {}] unable to decode submit_sm: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                            let error =
                                                submit_sm::generic_reject(potential_seq_no, error)
                                                    .encode();
                                            if tx
                                                .send(WriteFrame {
                                                    our_sequence_number: None,
                                                    pdu: error,
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
                                } else if header.command_id == CommandId::data_sm as u32 {
                                    match data_sm::decode(header, &pdu) {
                                        Ok(data_sm) => {
                                            info!("[{} on server {}] received data_sm with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                            let handler = listener.clone();
                                            let conn = connection_information.clone();
                                            let sid = session_id.clone();
                                            let tx = tx.clone();
                                            tokio::spawn(async move {
                                                let resp =
                                                    handler.on_data_sm(data_sm, &conn, &sid).await;
                                                if let Err(e) = tx
                                                    .send(WriteFrame {
                                                        our_sequence_number: None,
                                                        pdu: resp.encode(),
                                                        oneshot: None,
                                                    })
                                                    .await
                                                {
                                                    error!("[{} on server {}] unable to send data_sm_resp, writer closed: {}", conn.client_address, conn.server_address, e);
                                                }
                                            });
                                        }
                                        Err(error) => {
                                            error!("[{} on server {}] unable to decode data_sm: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                            let error =
                                                data_sm::generic_reject(potential_seq_no, error)
                                                    .encode();
                                            if tx
                                                .send(WriteFrame {
                                                    our_sequence_number: None,
                                                    pdu: error,
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
                                } else if header.command_id == CommandId::cancel_sm as u32
                                    && (bound_type == BOUND_TYPE::BOUND_TX
                                        || bound_type == BOUND_TYPE::BOUND_TRX)
                                {
                                    match cancel_sm::decode(header, &pdu) {
                                        Ok(cancel_sm) => {
                                            info!("[{} on server {}] received cancel_sm with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                            let handler = listener.clone();
                                            let conn = connection_information.clone();
                                            let sid = session_id.clone();
                                            let tx = tx.clone();
                                            tokio::spawn(async move {
                                                let resp = handler
                                                    .on_cancel_sm(cancel_sm, &conn, &sid)
                                                    .await;
                                                if let Err(e) = tx
                                                    .send(WriteFrame {
                                                        our_sequence_number: None,
                                                        pdu: resp.encode(),
                                                        oneshot: None,
                                                    })
                                                    .await
                                                {
                                                    error!("[{} on server {}] unable to send cancel_sm_resp, writer closed: {}", conn.client_address, conn.server_address, e);
                                                }
                                            });
                                        }
                                        Err(error) => {
                                            error!("[{} on server {}] unable to decode cancel_sm: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                            let error =
                                                submit_sm::generic_reject(potential_seq_no, error)
                                                    .encode();
                                            if tx
                                                .send(WriteFrame {
                                                    our_sequence_number: None,
                                                    pdu: error,
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
                                } else if header.command_id == CommandId::enquire_link as u32 {
                                    match enquire_link::decode(header, &pdu) {
                                        Ok(enquire_link) => {
                                            info!("[{} on server {}] received enquire_link with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                            let enquire_link_resp = enquire_link.accept();
                                            info!("[{} on server {}] sending enquire_link_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                            if tx
                                                .send(WriteFrame {
                                                    our_sequence_number: None,
                                                    pdu: enquire_link_resp.encode(),
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
                                        Err(error) => {
                                            error!("[{} on server {}] unable to decode enquire_link: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                            let error =
                                                submit_sm::generic_reject(potential_seq_no, error)
                                                    .encode();
                                            if tx
                                                .send(WriteFrame {
                                                    our_sequence_number: None,
                                                    pdu: error,
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
                                } else if header.command_id == CommandId::enquire_link_resp as u32 {
                                    info!("[{} on server {}] received enquire_link_resp for sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);

                                    // Forward to enquire_link timer task; if that task has already exited, the read loop will detect that via enquire_link_ticker.is_finished() below and break cleanly.
                                    if let Err(e) =
                                        enquire_link_tx.send(header.sequence_number).await
                                    {
                                        error!("[{} on server {}] enquire_link timer task gone, dropping enquire_link_resp seq {}: {}", connection_information.client_address, connection_information.server_address, header.sequence_number, e);
                                    }

                                    // Cleanup pending requests
                                    let mut guard = pending_requests.lock().await;
                                    if let Some((time, _)) = guard.remove(&header.sequence_number) {
                                        drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                        // Time-out detection
                                        let lapsed =
                                            time.elapsed().expect("Unable to elapse").as_millis();
                                        if lapsed > response_timer.into() {
                                            error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                            listener
                                                .on_timeout(header.sequence_number, &session_id)
                                                .await;
                                        }
                                    } else {
                                        error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                    }
                                } else if header.command_id == CommandId::unbind as u32 {
                                    // Whether or not the unbind fails, we don't care, if any ESMe sends us an unbind we stop the connection, so first we stop the enquire_link timer
                                    enquire_link_ticker.abort();

                                    match unbind::decode(header, &pdu) {
                                        Ok(unbind) => {
                                            info!("[{} on server {}] received unbind with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                            let unbind_resp = listener
                                                .on_unbind(
                                                    unbind.clone(),
                                                    &connection_information,
                                                    &session_id,
                                                )
                                                .await;
                                            if let Err(e) = tx
                                                .send(WriteFrame {
                                                    our_sequence_number: None,
                                                    pdu: unbind_resp.encode(),
                                                    oneshot: None,
                                                })
                                                .await
                                            {
                                                error!("[{} on server {}] unable to send unbind_resp, writer closed: {}", connection_information.client_address, connection_information.server_address, e);
                                            }
                                        }
                                        Err(error) => {
                                            error!("[{} on server {}] unable to decode unbind: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                            let error =
                                                unbind::generic_reject(potential_seq_no, error)
                                                    .encode();
                                            if let Err(e) = tx
                                                .send(WriteFrame {
                                                    our_sequence_number: None,
                                                    pdu: error,
                                                    oneshot: None,
                                                })
                                                .await
                                            {
                                                error!("[{} on server {}] unable to send unbind generic_reject, writer closed: {}", connection_information.client_address, connection_information.server_address, e);
                                            }
                                        }
                                    }

                                    last_pdu_was_unbind = true;

                                    break;
                                } else if header.command_id == CommandId::deliver_sm_resp as u32
                                    && (bound_type == BOUND_TYPE::BOUND_TX
                                        || bound_type == BOUND_TYPE::BOUND_TRX)
                                {
                                    let mut guard = pending_requests.lock().await;
                                    if let Some((time, oneshot)) =
                                        guard.remove(&header.sequence_number)
                                    {
                                        drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                        // Time-out detection
                                        let lapsed =
                                            time.elapsed().expect("Unable to elapse").as_millis();
                                        if lapsed > response_timer.into() {
                                            error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                            listener
                                                .on_timeout(header.sequence_number, &session_id)
                                                .await;
                                        } else {
                                            match deliver_sm_resp::decode(header, &pdu) {
                                                Ok(deliver_sm_resp) => {
                                                    info!("[{} on server {}] received deliver_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                    let connection_information =
                                                        connection_information.clone();

                                                    // Send the response to the original sender
                                                    if let Some(oneshot) = oneshot {
                                                        match oneshot
                                                            .send(Box::new(deliver_sm_resp.clone()))
                                                        {
                                                            Ok(_) => {
                                                                info!("[{} on server {}] deliver_sm_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                            }
                                                            Err(_) => {
                                                                error!("[{} on server {}] unable to send deliver_sm_resp to original sender", connection_information.client_address, connection_information.server_address);
                                                            }
                                                        }
                                                    } else {
                                                        error!("[{} on server {}] No oneshot channel registered for deliver_sm", connection_information.client_address, connection_information.server_address);
                                                    }
                                                }
                                                Err(error) => {
                                                    error!("[{} on server {}] unable to decode deliver_sm_resp: {:?}, PDU ({} bytes): {:02X?}", connection_information.client_address, connection_information.server_address, error, pdu.len(), pdu);
                                                    let error = submit_sm::generic_reject(
                                                        potential_seq_no,
                                                        error,
                                                    )
                                                    .encode();
                                                    if tx
                                                        .send(WriteFrame {
                                                            our_sequence_number: None,
                                                            pdu: error,
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
                                        }
                                    } else {
                                        error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                    }
                                } else if header.command_id == CommandId::data_sm_resp as u32 {
                                    let mut guard = pending_requests.lock().await;
                                    if let Some((time, oneshot)) =
                                        guard.remove(&header.sequence_number)
                                    {
                                        drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                        // Time-out detection
                                        let lapsed =
                                            time.elapsed().expect("Unable to elapse").as_millis();
                                        if lapsed > response_timer.into() {
                                            error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                            listener
                                                .on_timeout(header.sequence_number, &session_id)
                                                .await;
                                        } else {
                                            match data_sm_resp::decode(header, &pdu) {
                                                Ok(data_sm_resp) => {
                                                    info!("[{} on server {}] received data_sm_resp with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                    let connection_information =
                                                        connection_information.clone();

                                                    // Send the response to the original sender
                                                    if let Some(oneshot) = oneshot {
                                                        match oneshot
                                                            .send(Box::new(data_sm_resp.clone()))
                                                        {
                                                            Ok(_) => {
                                                                info!("[{} on server {}] data_sm_resp sent to original sender", connection_information.client_address, connection_information.server_address);
                                                            }
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
                                                    let error = submit_sm::generic_reject(
                                                        potential_seq_no,
                                                        error,
                                                    )
                                                    .encode();
                                                    if tx
                                                        .send(WriteFrame {
                                                            our_sequence_number: None,
                                                            pdu: error,
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
                                        }
                                    } else {
                                        error!("[{} on server {}] No pending request for sequence_number {}", connection_information.client_address, connection_information.server_address, header.sequence_number);
                                    }
                                } else if header.command_id == CommandId::generic_nack as u32 {
                                    let mut guard = pending_requests.lock().await;
                                    if let Some((time, oneshot)) =
                                        guard.remove(&header.sequence_number)
                                    {
                                        drop(guard); // Explicitly drop the mutex guard so writes are not blocked

                                        // Time-out detection
                                        let lapsed =
                                            time.elapsed().expect("Unable to elapse").as_millis();
                                        if lapsed > response_timer.into() {
                                            error!("[{} on server {}] Response came in for sequence_number {} after time-out {}ms lapsed", connection_information.client_address, connection_information.server_address, header.sequence_number, lapsed);
                                            listener
                                                .on_timeout(header.sequence_number, &session_id)
                                                .await;
                                        } else {
                                            match generic_nack::decode(header, &pdu) {
                                                Ok(generic_nack) => {
                                                    info!("[{} on server {}] received generic_nack with sequence_number {}", connection_information.client_address, connection_information.server_address, potential_seq_no);
                                                    let connection_information =
                                                        connection_information.clone();

                                                    // Send the response to the original sender
                                                    if let Some(oneshot) = oneshot {
                                                        match oneshot
                                                            .send(Box::new(generic_nack.clone()))
                                                        {
                                                            Ok(_) => {
                                                                info!("[{} on server {}] generic_nack sent to original sender", connection_information.client_address, connection_information.server_address);
                                                            }
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
                                        command_id: CommandId::generic_nack as u32,
                                        command_status: SmppError::ESME_RINVCMDID as u32,
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
                                        writer_dead = true;
                                        break;
                                    }
                                }
                            }
                            Err(error) => {
                                error!("[{} on server {}] Unable to decode command_header for PDU, sending {:?} in generic_nack", connection_information.client_address, connection_information.server_address, error);
                                let generic_nack = CommandHeader {
                                    command_length: 16,
                                    command_id: CommandId::generic_nack as u32,
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

                    if last_pdu_was_unbind || writer_dead {
                        break; // Break the read loop so we can go to CLOSED state
                    }

                    last_read = Instant::now();

                    // Last thing to do is general time-out detection
                    let mut pending_requests = pending_requests.lock().await;
                    let mut timed_out_sequences = Vec::new();

                    pending_requests.retain(|sequence_number, (time, _)| {
                        let lapsed = time.elapsed().expect("Unable to elapse").as_millis();
                        if lapsed > response_timer.into() {
                            timed_out_sequences.push(*sequence_number);
                            error!("[{} on server {}] Response for sequence_number {} did not come in after {}ms lapsed", connection_information.client_address, connection_information.server_address, sequence_number, lapsed);
                            false // Remove this entry
                        } else {
                            true // Keep this entry
                        }
                    });

                    // Release the lock before async calls
                    drop(pending_requests);

                    // Notify listeners for timed-out requests
                    for sequence_number in timed_out_sequences {
                        listener.on_timeout(sequence_number, &session_id).await;
                    }
                }
            }
            Err(_e) => { /* Nothing to do here as we introduce the interval to not constantly block this thread */
            }
            Ok(Err(e)) => {
                error!(
                    "[{} on server {}] {} ",
                    connection_information.client_address, connection_information.server_address, e
                );
                break;
            }
        }

        if enquire_link_ticker.is_finished() {
            error!(
                "[{} on server {}] enquire_link thread finished, stopping read loop",
                connection_information.client_address, connection_information.server_address
            );
            break;
        } else if last_read.elapsed().as_millis() > inactivity_timer.into() {
            // Please note, it is more likely that the enquire_link timer stopped earlier as it expects a response likely within 2000ms (default) but in some weird scenario that it it's stuck we can always trigger the inactivity timer by keeping
            // track of when the last packet was read
            error!(
                "[{} on server {}] inactivity_timer triggered after {}ms, closing TCP connection",
                connection_information.client_address,
                connection_information.server_address,
                inactivity_timer
            );
            break;
        }

        buffer.clear(); // Make sure we start reading with an empty buffer
    }

    listener.on_esme_unbound(&session_id).await;

    info!(
        "[{} on server {}] {} going to CLOSED state",
        connection_information.client_address, connection_information.server_address, bound_type
    );
    alive.store(false, Ordering::SeqCst);

    enquire_link_ticker.abort(); // Stop enquiring the link as we are closing the connection
    writer_thread.abort(); // Stop allowing the sending of writing of new PDUs

    CLOSED {}
}

///
/// OPEN (Connected and Bind Pending)
/// An ESME has established a network connection to the SMSC but has not yet issued a
/// Bind request.
///
pub(crate) struct OPEN {
    pub(crate) session_id: String,
}

impl OPEN {
    pub(crate) async fn bind_transmitter(
        self,
        mut stream: TcpStream,
        bind_transmitter: bind_transmitter,
        bind_transmitter_resp: &bind_transmitter_resp,
        connection_information: &SmppConnectionInformation,
        handler: Arc<dyn SmppServerListener + Send + Sync + 'static>,
    ) -> Result<BOUND_TX, SmppError> {
        if bind_transmitter_resp.is_success() {
            let result = stream.write(&bind_transmitter_resp.clone().encode()).await;
            if result.is_ok() {
                let new_state = BOUND_TX {
                    session_id: self.session_id,
                    stream,
                    system_id: bind_transmitter.system_id,
                    handler: handler.clone(),
                    connection_information: connection_information.clone(),
                };
                info!(
                    "Connection from {} on server {} with system_id {} went to state BOUND_TX",
                    connection_information.client_address,
                    connection_information.server_address,
                    new_state.system_id
                );
                Ok(new_state)
            } else {
                error!("Connection from {} on server {} with system_id {} unable to transistion to state BOUND_TX, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_transmitter.system_id);
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            Err(bind_transmitter_resp.get_error())
        }
    }

    pub(crate) async fn bind_receiver(
        self,
        mut stream: TcpStream,
        bind_receiver: bind_receiver,
        bind_receiver_resp: bind_receiver_resp,
        connection_information: &SmppConnectionInformation,
        handler: Arc<dyn SmppServerListener + Send + Sync + 'static>,
    ) -> Result<BOUND_RX, SmppError> {
        if bind_receiver_resp.is_success() {
            let result = stream.write(&bind_receiver_resp.encode()).await;
            if result.is_ok() {
                let new_state = BOUND_RX {
                    session_id: self.session_id,
                    stream,
                    system_id: bind_receiver.system_id,
                    handler: handler.clone(),
                    connection_information: connection_information.clone(),
                };
                info!(
                    "Connection from {} on server {} with system_id {} went to state BOUND_RX",
                    connection_information.client_address,
                    connection_information.server_address,
                    new_state.system_id
                );
                Ok(new_state)
            } else {
                error!("Connection from {} on server {} with system_id {} unable to transistion to state BOUND_RX, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_receiver.system_id);
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            Err(bind_receiver_resp.get_error())
        }
    }

    pub(crate) async fn bind_transceiver(
        self,
        mut stream: TcpStream,
        bind_transceiver: bind_transceiver,
        bind_transceiver_resp: &bind_transceiver_resp,
        connection_information: &SmppConnectionInformation,
        handler: Arc<dyn SmppServerListener + Send + Sync + 'static>,
    ) -> Result<BOUND_TRX, SmppError> {
        if bind_transceiver_resp.is_success() {
            let result = stream.write(&bind_transceiver_resp.clone().encode()).await;
            if result.is_ok() {
                let new_state = BOUND_TRX {
                    session_id: self.session_id,
                    stream,
                    system_id: bind_transceiver.system_id,
                    handler: handler.clone(),
                    connection_information: connection_information.clone(),
                };
                info!(
                    "Connection from {} on server {} with system_id {} went to state BOUND_TRX",
                    connection_information.client_address,
                    connection_information.server_address,
                    new_state.system_id
                );
                Ok(new_state)
            } else {
                error!("Connection from {} on server {} with system_id {} unable to transistion to state BOUND_TRX, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_transceiver.system_id);
                Err(SmppError::ESME_RSYSERR)
            }
        } else {
            error!("Connection from {} on server {} with system_id {} was rejected with error {:?}, closing TCP connection", connection_information.client_address, connection_information.server_address, bind_transceiver.system_id, bind_transceiver_resp.get_error());
            stream
                .write_all(&bind_transceiver_resp.clone().encode())
                .await
                .expect("Unable to write to TCP socket");
            Err(bind_transceiver_resp.get_error())
        }
    }
}

pub(crate) struct BOUND_TX {
    pub(crate) session_id: String,
    stream: TcpStream,
    system_id: String,
    handler: Arc<dyn SmppServerListener + Send + Sync + 'static>,
    connection_information: SmppConnectionInformation,
}

impl BOUND_TX {
    pub(crate) async fn read_loop(
        self,
        system_id: String,
        enquire_link_timer: u64,
        inactivity_timer: u64,
        response_timer: u64,
        buffer_size: usize,
    ) -> CLOSED {
        read_loop(
            BOUND_TYPE::BOUND_TX,
            self.handler,
            self.stream,
            self.connection_information,
            system_id,
            enquire_link_timer,
            inactivity_timer,
            response_timer,
            buffer_size,
            self.session_id,
        )
        .await
    }
}

pub(crate) struct BOUND_RX {
    pub(crate) session_id: String,
    stream: TcpStream,
    system_id: String,
    handler: Arc<dyn SmppServerListener + Send + Sync>,
    connection_information: SmppConnectionInformation,
}

impl BOUND_RX {
    pub(crate) async fn read_loop(
        self,
        system_id: String,
        enquire_link_timer: u64,
        inactivity_timer: u64,
        response_timer: u64,
        buffer_size: usize,
    ) -> CLOSED {
        read_loop(
            BOUND_TYPE::BOUND_RX,
            self.handler,
            self.stream,
            self.connection_information,
            system_id,
            enquire_link_timer,
            inactivity_timer,
            response_timer,
            buffer_size,
            self.session_id,
        )
        .await
    }
}

pub(crate) struct BOUND_TRX {
    pub(crate) session_id: String,
    stream: TcpStream,
    system_id: String,
    handler: Arc<dyn SmppServerListener + Send + Sync>,
    connection_information: SmppConnectionInformation,
}

impl BOUND_TRX {
    pub(crate) async fn read_loop(
        self,
        system_id: String,
        enquire_link_timer: u64,
        inactivity_timer: u64,
        response_timer: u64,
        buffer_size: usize,
    ) -> CLOSED {
        read_loop(
            BOUND_TYPE::BOUND_TRX,
            self.handler,
            self.stream,
            self.connection_information,
            system_id,
            enquire_link_timer,
            inactivity_timer,
            response_timer,
            buffer_size,
            self.session_id,
        )
        .await
    }
}

pub(crate) struct CLOSED {}
