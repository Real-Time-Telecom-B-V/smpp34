
use std::sync::Mutex;

use log::info;
use smpp34::{alert_notification, client::SMSC, data_sm_resp, deliver_sm, deliver_sm_resp, submit_sm, submit_sm_resp, unbind, unbind_resp, SmppConnectionInformation};

static SMSCS: Mutex<Vec<SMSC>> = Mutex::new(Vec::new());

pub fn on_unbind(request: unbind, _connection_information: &SmppConnectionInformation, _session_id: &String) -> unbind_resp {
    request.accept()
}

pub fn on_unbind_resp(_response: unbind_resp, _connection_information: &SmppConnectionInformation, _session_id: &String) {
    
}
pub fn on_submit_sm(request: submit_sm, _connection_information: &SmppConnectionInformation, _session_id: &String) -> submit_sm_resp {
    request.accept("1234".to_string())
}
    
pub fn on_submit_sm_resp(_response: submit_sm_resp, _connection_information: &SmppConnectionInformation, _session_id: &String){
    
}
pub fn on_data_sm_resp(_response: data_sm_resp, _connection_information: &SmppConnectionInformation, _session_id: &String){
    
}

pub fn on_deliver_sm(request: deliver_sm, _connection_information: &SmppConnectionInformation, _session_id: &String) -> deliver_sm_resp {
    request.accept()
}
pub fn on_alert_notification(_request: alert_notification, _connection_information: &SmppConnectionInformation, _session_id: &String) {
    
}

pub fn on_timeout(_sequence_number: u32, _session_id: &String) {
}

pub fn on_smsc_bound(smsc: SMSC, _session_id: &String) {
    info!("SMSC bound for session {} with system_id {} and address {}", _session_id, smsc.system_id, smsc.server_address);
    SMSCS.lock().unwrap().push(smsc);
}

pub fn on_smsc_unbound(_session_id: &String) {
    info!("SMSC for session {} unbound!", _session_id);
    SMSCS.lock().unwrap().retain(|smsc| smsc.session_id != *_session_id);
}

mod tests {
    use std::{sync::Arc, thread, time::Duration};
    use smpp34::client::{SmppClientListener, SmppClient, BIND_TYPE};

    use crate::*;

    use test_log::test;

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_server_bind() {
        let listener = SmppClientListener {
            on_unbind,
            on_unbind_resp,
            on_submit_sm_resp,
            on_data_sm_resp,
            on_deliver_sm,
            on_alert_notification,
            on_timeout,
            on_smsc_bound,
            on_smsc_unbound,
        };
    
        let mut client = SmppClient::new("127.0.0.1".to_owned() ,2775, false,
            BIND_TYPE::TRX, 
            "66wnb9e8".to_owned(), 
            "x445cre7".to_owned(), 
            "GATEWAY".to_owned(), 
            1, 
            1, 
            "".to_owned(), 
            Arc::new(listener), 
            20
        );
        client.start();

        thread::sleep(Duration::from_millis(10000));

        SMSCS.lock().unwrap().get(0).map(|smsc| {
            smsc.send_unbind();
        });
        
        client.stop();

    }
}