use std::sync::{atomic::AtomicU32, mpsc::Sender, Arc};


pub struct SMSC {
    can_receive: bool,
    tx_channel: Sender<Vec<u8>>,
    sequence_number: Arc<AtomicU32>,
}