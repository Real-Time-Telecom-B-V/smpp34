
use smpp34::{client::SMSC, unbind, SmppConnectionInformation, deliver_sm_resp, deliver_sm, data_sm_resp, submit_sm_resp, alert_notification, unbind_resp};

pub fn on_unbind(request: unbind, connection_information: &SmppConnectionInformation, session_id: &String) -> unbind_resp {
    request.accept()
}
    
pub fn on_submit_sm_resp(response: submit_sm_resp, connection_information: &SmppConnectionInformation, session_id: &String){
    
}
pub fn on_data_sm_resp(response: data_sm_resp, connection_information: &SmppConnectionInformation, session_id: &String){
    
}

pub fn on_deliver_sm(request: deliver_sm, connection_information: &SmppConnectionInformation, session_id: &String) -> deliver_sm_resp {
    request.accept()
}
pub fn on_alert_notification(request: alert_notification, connection_information: &SmppConnectionInformation, session_id: &String) {
    
}

pub fn on_timeout(sequence_number: u32, session_id: &String) {
}

pub fn on_smsc_bound(smsc: SMSC, session_id: &String) {

}

pub fn on_smsc_unbound(session_id: &String) {
    
}

mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use smpp34::client::{SmppClientListener, SmppClient, BIND_TYPE};

    use crate::*;

    #[test]
    fn test_multi_pdu_frame() {
        let listener = SmppClientListener {
            on_unbind,
            on_submit_sm_resp,
            on_data_sm_resp,
            on_deliver_sm,
            on_alert_notification,
            on_timeout,
            on_smsc_bound,
            on_smsc_unbound,
        };
    
        let mut client = SmppClient::new(
            std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 
            2775, 
            BIND_TYPE::TRX, 
            "smpp1".to_owned(), 
            "abcd1234".to_owned(), 
            "GATEWAY".to_owned(), 
            1, 
            1, 
            "".to_owned(), 
            Arc::new(listener), 
            20
        );
        client.start();
    }
}