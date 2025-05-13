

use async_trait::async_trait;
use log::info;
use smpp34::{alert_notification, client::{SmppClientListener, SMSC}, deliver_sm, deliver_sm_resp, unbind, unbind_resp, SmppConnectionInformation};
use tokio::sync::Mutex;


struct TestSmppClientListener {
    pub smscs: Mutex<Vec<SMSC>>,
}

impl TestSmppClientListener {
    pub fn new() -> Self {
        TestSmppClientListener{
            smscs: Mutex::new(Vec::new()),
        }
    }

    pub async fn unbind(&self) {
        let smscs = self.smscs.lock().await;
        if let Some(smsc) = smscs.get(0) {
            let result = smsc.send_unbind().await;
            match result {
                Ok(_) => {
                    info!("Unbound session {}", smsc.session_id);
                }
                Err(e) => {
                    info!("Error unbinding session {}: {:?}", smsc.session_id, e);
                }
            }
        }
    }
}

#[async_trait]
impl SmppClientListener for TestSmppClientListener {

    async fn on_unbind(&self, request: unbind, _connection_information: &SmppConnectionInformation, _session_id: &String) -> unbind_resp {
        request.accept()
    }
    
    async fn on_deliver_sm(&self, request: deliver_sm, _connection_information: &SmppConnectionInformation, _session_id: &String) -> deliver_sm_resp {
        request.accept()
    }

    async fn on_alert_notification(&self, _request: alert_notification, _connection_information: &SmppConnectionInformation, _session_id: &String) {
        
    }
    
    async fn on_timeout(&self, _sequence_number: u32, _session_id: &String) {
    }
    
    async fn on_smsc_bound(&self, smsc: SMSC, _session_id: &String) {
        info!("SMSC bound for session {} with system_id {} and address {}", _session_id, smsc.system_id, smsc.server_address);
        self.smscs.lock().await.push(smsc);
    }
    
    async fn on_smsc_unbound(&self, _session_id: &String) {
        info!("SMSC for session {} unbound!", _session_id);
        self.smscs.lock().await.retain(|smsc| smsc.session_id != *_session_id);
    }
}



mod tests {
    use std::{sync::Arc, thread, time::Duration};
    use smpp34::client::{SmppClient, BIND_TYPE};

    use crate::*;

    use test_log::test;

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_server_bind() {

        let listener = Arc::new(TestSmppClientListener::new());
    
        let mut client = SmppClient::new("127.0.0.1".to_owned() ,2775, false,
            BIND_TYPE::TRX, 
            "username".to_owned(), 
            "password".to_owned(), 
            "GATEWAY".to_owned(), 
            1, 
            1, 
            "".to_owned(), 
            listener.clone(), 
            20
        );
        client.start().await;

        thread::sleep(Duration::from_millis(10000));

        listener.unbind().await;
        
        client.stop().await;

    }
}