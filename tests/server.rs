use std::sync::Mutex;

use async_trait::async_trait;
use log::{error, info};
use smpp34::{bind_receiver, bind_receiver_resp, bind_transceiver, bind_transceiver_resp, bind_transmitter, bind_transmitter_resp, cancel_sm, cancel_sm_resp, data_sm_resp, server::ESME, submit_sm, submit_sm_resp, unbind, unbind_resp, SmppConnectionInformation, SmppError, SmppServerListener};





struct TestSmppServerListener {
    pub esmes: Mutex<Vec<ESME>>,
}

impl TestSmppServerListener {
    pub fn new() -> Self {
        TestSmppServerListener {
            esmes: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl SmppServerListener for TestSmppServerListener {

    async fn on_bind_transmitter(&self, request: bind_transmitter, connection_information: &SmppConnectionInformation, _session_id: &String) -> bind_transmitter_resp {
        info!("[bind_transmitter@{}] <{}> system_id={} password={} system_type={} interface_version={:#04x}, addr_ton={:#04x}, addr_npi={:#04x}, address_range={}", connection_information.server_address, connection_information.client_address, request.system_id, request.password, request.system_type, request.interface_version, request.addr_ton, request.addr_npi, request.address_range);
        error!("[bind_transmitter@{}] <{}> Invalid system_id", connection_information.server_address, connection_information.client_address);
        request.reject(SmppError::ESME_RINVSYSID)
    }
    
    async fn on_bind_receiver(&self, request: bind_receiver, connection_information: &SmppConnectionInformation, _session_id: &String) -> bind_receiver_resp {
        info!("[bind_receiver@{}] <{}> system_id={} password={} system_type={} interface_version={:#04x}, addr_ton={:#04x}, addr_npi={:#04x}, address_range={}", connection_information.server_address, connection_information.client_address, request.system_id, request.password, request.system_type, request.interface_version, request.addr_ton, request.addr_npi, request.address_range);
        error!("[bind_receiver@{}] <{}>Invalid system_id", connection_information.server_address, connection_information.client_address);
        request.reject(SmppError::ESME_RINVSYSID)
    }
    
    async fn on_bind_transceiver(&self, request: bind_transceiver, connection_information: &SmppConnectionInformation, _session_id: &String) -> bind_transceiver_resp {
        info!("[bind_transceiver@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);
    
        if request.system_id == "usernampe" {
            if request.password == "password" {
                info!("[bind_transceiver_resp@{}] <{}> Accepted bind for system_id {}", connection_information.server_address, connection_information.client_address, request.system_id);
                request.accept(String::from("MySMSC"), Some(0x34))
            } else {
                error!("[bind_transceiver_resp@{}] <{}> Invalid password for system_id {}", connection_information.server_address, connection_information.client_address, request.system_id);
                request.reject(SmppError::ESME_RINVPASWD)
            }
        } else {
            error!("[bind_transceiver_resp@{}] <{}> Invalid system_id {}", connection_information.server_address, connection_information.client_address, request.system_id);
            request.reject(SmppError::ESME_RINVSYSID)
        }
    }
    
    async fn on_unbind(&self, request: unbind, connection_information: &SmppConnectionInformation, _session_id: &String) -> unbind_resp {
        info!("[unbind@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);
        request.accept()
    }
    
    async fn on_submit_sm(&self, request: submit_sm, connection_information: &SmppConnectionInformation, _session_id: &String) -> submit_sm_resp {
    
        info!("[submit_sm@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);
        request.accept(String::from("1234"))
    }
    
    async fn on_cancel_sm(&self, request: cancel_sm, connection_information: &SmppConnectionInformation, _session_id: &String) -> cancel_sm_resp {
        info!("[cancel_sm@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);
        request.accept()
    }

    async fn on_data_sm(&self, request: smpp34::data_sm, connection_information: &SmppConnectionInformation, _session_id: &String) -> data_sm_resp {
        info!("[data_sm@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);
        request.accept(String::from("5678"))
    }
    
    async fn on_timeout(&self, sequence_number: u32, _session_id: &String) {
        info!("[timeout] sequence_number: {}", sequence_number);
    }
    
    async fn on_esme_bound(&self, esme: ESME, session_id: &String) {
        let client_address = esme.client_address.clone();
        let system_id = esme.system_id.clone();
        self.esmes.lock().unwrap().push(esme);
        info!("ESME bound for session {} with system_id {} and address {}", session_id, system_id, client_address);
    }
    
    async fn on_esme_unbound(&self, session_id: &String) {
        self.esmes.lock().unwrap().retain(|esme| esme.session_id != *session_id);
        info!("ESME for session {} unbound!", session_id);
    }
}

mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use smpp34::SmppServer;

    use crate::*;

    use test_log::test;

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_multi_pdu_frame() {
        let listener = Arc::new(TestSmppServerListener::new());
        let mut server = SmppServer::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2775, listener);
        server.start().await;

        
    }
}

