# SMPP 3.4 compliance

What `smpp34` implements against the [SMPP 3.4 specification](https://smpp.org/SMPP_v3_4_Issue1_2.pdf).
Three layers: the **wire codec** (encode/decode), the high-level **client (ESME)**
send API, and the high-level **server (SMSC)** dispatch (`SmppServerListener`).

Legend: ✅ supported · ◑ codec-only (encode/decode works; not wired into the
high-level client/server dispatch — drive it via the codec yourself).

## PDUs (§4, §5.3)

| PDU | Codec | Client (ESME) | Server (SMSC) |
|---|:---:|:---:|:---:|
| `bind_transmitter` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_bind_transmitter`) |
| `bind_receiver` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_bind_receiver`) |
| `bind_transceiver` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_bind_transceiver`) |
| `outbind` | ✅ | ◑ | ◑ |
| `unbind` / `_resp` | ✅ | ✅ | ✅ (`on_unbind`) |
| `enquire_link` / `_resp` | ✅ | ✅ auto keep-alive | ✅ auto keep-alive |
| `generic_nack` | ✅ | ✅ | ✅ (malformed-PDU reject) |
| `submit_sm` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_submit_sm`) |
| `submit_sm_multi` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_submit_sm_multi`) |
| `deliver_sm` / `_resp` | ✅ | ✅ dispatch (`on_deliver_sm`) | ✅ send (`ESME::deliver_sm`) |
| `data_sm` / `_resp` | ✅ | ✅ | ✅ (`on_data_sm`) |
| `query_sm` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_query_sm`) |
| `cancel_sm` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_cancel_sm`) |
| `replace_sm` / `_resp` | ✅ | ✅ send | ✅ dispatch (`on_replace_sm`) |
| `alert_notification` | ✅ | ✅ dispatch (`on_alert_notification`) | ✅ send |

## Optional parameters / TLVs (§3.2.1, §5.3.2.1)

Full TLV codec (`encode_tlvs` / `decode_tlvs`, arbitrary tag/value). The 44
spec-defined tags are enumerated in `TlvTag` (`Tlv::from_tag(TlvTag::…, value)`),
e.g. `message_payload`, the `sar_*` concatenation set, `sc_interface_version`,
`callback_num`, `network_error_code`, `ussd_service_op`, `message_state`, …
Unknown tags round-trip transparently via `Tlv::new(tag, value)`.

## Sessions & data types

| Feature | Status |
|---|:---:|
| Bind modes: TX / RX / TRX | ✅ |
| Session timers: `session_init`, `enquire_link`, `inactivity`, `response` | ✅ |
| Sequence-number windowing (configurable window) | ✅ |
| Async request/response correlation | ✅ |
| C-Octet-String, Octet-String, integer codecs | ✅ |
| TLS transport | ✅ (`tokio-native-tls`) |
| Raw `short_message` octets + `DeliveryReceipt` parsing | ✅ (GSM 7-bit / UCS-2 *text* encoding of the payload is left to the caller) |

## Not in scope

- SMPP **5.0** (`broadcast_sm`, `query_broadcast_sm`, …) — this crate targets 3.4.
- A turnkey routing/store-and-forward gateway — `smpp34` is a protocol library;
  routing/persistence are the application's job.
