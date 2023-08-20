use log::{error, info};
use smpp34::{server::ESME, bind_transmitter, bind_transmitter_resp, bind_receiver, bind_receiver_resp, SmppError, bind_transceiver, bind_transceiver_resp, unbind, unbind_resp, submit_sm_resp, submit_sm, SmppConnectionInformation, deliver_sm_resp};



pub fn on_bind_transmitter(request: bind_transmitter, connection_information: &SmppConnectionInformation, session_id: &String) -> bind_transmitter_resp {
    info!("[bind_transmitter@{}] <{}> system_id={} password={} system_type={} interface_version={:#04x}, addr_ton={:#04x}, addr_npi={:#04x}, address_range={}", connection_information.server_address, connection_information.client_address, request.system_id, request.password, request.system_type, request.interface_version, request.addr_ton, request.addr_npi, request.address_range);
    error!("[bind_transmitter@{}] <{}> Invalid system_id", connection_information.server_address, connection_information.client_address);
    request.reject(SmppError::ESME_RINVSYSID)
}

pub fn on_bind_receiver(request: bind_receiver, connection_information: &SmppConnectionInformation, session_id: &String) -> bind_receiver_resp {
    info!("[bind_receiver@{}] <{}> system_id={} password={} system_type={} interface_version={:#04x}, addr_ton={:#04x}, addr_npi={:#04x}, address_range={}", connection_information.server_address, connection_information.client_address, request.system_id, request.password, request.system_type, request.interface_version, request.addr_ton, request.addr_npi, request.address_range);
    error!("[bind_receiver@{}] <{}>Invalid system_id", connection_information.server_address, connection_information.client_address);
    request.reject(SmppError::ESME_RINVSYSID)
}

pub fn on_bind_transceiver(request: bind_transceiver, connection_information: &SmppConnectionInformation, session_id: &String) -> bind_transceiver_resp {
    info!("[bind_transceiver@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);

    if request.system_id == "matthias" {
        if request.password == "abc123" {
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

pub fn on_unbind(request: unbind, connection_information: &SmppConnectionInformation, session_id: &String) -> unbind_resp {
    info!("[unbind@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);
    request.accept()
}

pub fn on_submit_sm(request: submit_sm, connection_information: &SmppConnectionInformation, session_id: &String) -> submit_sm_resp {

    info!("[submit_sm@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, request);
    request.accept(String::from("1234"))
}

pub fn on_deliver_sm_resp(response: deliver_sm_resp, connection_information: &SmppConnectionInformation, session_id: &String)  {
    info!("[deliver_sm_resp@{}] <{}> {:?}", connection_information.server_address, connection_information.client_address, response);
}

pub fn on_timeout(sequence_number: u32, session_id: &String) {
}

pub fn on_esme_bound(esme: ESME, session_id: &String) {

}

pub fn on_esme_unbound(session_id: &String) {
    
}

mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use smpp34::{SmppServer, SmppServerListener};

    use crate::*;

    #[test]
    fn test_multi_pdu_frame() {
        let listener = SmppServerListener { 
            on_bind_transmitter, 
            on_bind_receiver, 
            on_bind_transceiver, 
            on_unbind,
            on_submit_sm,
            on_deliver_sm_resp,
            on_timeout,
            on_esme_bound,
            on_esme_unbound,
        };
    
        let mut server = SmppServer::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2775, Arc::new(listener));
        server.start();
    }
}

