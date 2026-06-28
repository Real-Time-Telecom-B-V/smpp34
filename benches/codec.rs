//! Codec micro-benchmarks: PDU encode/decode + TLV throughput.
//!
//! Run with `cargo bench`. Numbers feed the README "Performance" table.
//!
//! `submit_sm::new` is `pub(crate)`, so we build a fixture the public way:
//! assemble a valid wire buffer, decode it once to obtain a value for the
//! encode benches, then measure decode (from bytes) and encode (clone + encode).

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use smpp34::{decode_tlvs, deliver_sm, encode_tlvs, submit_sm, CommandHeader, Tlv, TlvTag};

/// Assemble a valid `submit_sm`/`deliver_sm` wire buffer (they share the body
/// layout); `command_id` selects which (0x0004 submit_sm, 0x0005 deliver_sm).
fn message_pdu(command_id: u32, short_message: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0); // service_type (empty C-octet string)
    body.push(1); // source_addr_ton
    body.push(1); // source_addr_npi
    body.extend_from_slice(b"12345\0"); // source_addr
    body.push(1); // dest_addr_ton
    body.push(1); // dest_addr_npi
    body.extend_from_slice(b"31600000000\0"); // destination_addr
    body.push(0); // esm_class
    body.push(0); // protocol_id
    body.push(0); // priority_flag
    body.push(0); // schedule_delivery_time (empty)
    body.push(0); // validity_period (empty)
    body.push(0); // registered_delivery
    body.push(0); // replace_if_present_flag
    body.push(0); // data_coding
    body.push(0); // sm_default_msg_id
    body.push(short_message.len() as u8); // sm_length
    body.extend_from_slice(short_message);

    let cmd_len = (16 + body.len()) as u32;
    let mut pdu = Vec::with_capacity(cmd_len as usize);
    pdu.extend_from_slice(&cmd_len.to_be_bytes());
    pdu.extend_from_slice(&command_id.to_be_bytes());
    pdu.extend_from_slice(&0u32.to_be_bytes()); // command_status
    pdu.extend_from_slice(&1u32.to_be_bytes()); // sequence_number
    pdu.extend_from_slice(&body);
    pdu
}

/// A handful of common optional parameters (concatenated short-message style).
fn sample_tlvs() -> Vec<Tlv> {
    vec![
        Tlv::from_tag(TlvTag::SarMsgRefNum, 0x1234u16.to_be_bytes().to_vec()),
        Tlv::from_tag(TlvTag::SarTotalSegments, vec![3]),
        Tlv::from_tag(TlvTag::SarSegmentSeqnum, vec![1]),
        Tlv::from_tag(
            TlvTag::MessagePayload,
            b"a longer concatenated segment".to_vec(),
        ),
    ]
}

fn bench_codec(c: &mut Criterion) {
    let sm = b"Hello, this is a test SMS of fairly typical length.";
    let submit_bytes = message_pdu(0x0000_0004, sm);
    let deliver_bytes = message_pdu(0x0000_0005, sm);

    // Decode the fixtures once to obtain owned values for the encode benches.
    let submit_val = {
        let h = CommandHeader::decode(&submit_bytes).expect("valid header");
        submit_sm::decode(h, &submit_bytes).expect("valid submit_sm fixture")
    };
    let deliver_val = {
        let h = CommandHeader::decode(&deliver_bytes).expect("valid header");
        deliver_sm::decode(h, &deliver_bytes).expect("valid deliver_sm fixture")
    };

    let mut g = c.benchmark_group("codec");
    g.throughput(Throughput::Elements(1));

    g.bench_function("submit_sm/decode", |b| {
        b.iter(|| {
            let h = CommandHeader::decode(&submit_bytes).unwrap();
            submit_sm::decode(h, &submit_bytes).unwrap()
        })
    });
    g.bench_function("submit_sm/encode", |b| {
        b.iter_batched(|| submit_val.clone(), |p| p.encode(), BatchSize::SmallInput)
    });
    g.bench_function("deliver_sm/decode", |b| {
        b.iter(|| {
            let h = CommandHeader::decode(&deliver_bytes).unwrap();
            deliver_sm::decode(h, &deliver_bytes).unwrap()
        })
    });
    g.bench_function("deliver_sm/encode", |b| {
        b.iter_batched(
            || deliver_val.clone(),
            |p| p.encode(),
            BatchSize::SmallInput,
        )
    });

    let tlvs = sample_tlvs();
    let tlv_bytes = encode_tlvs(&tlvs);
    g.bench_function("tlv/encode", |b| b.iter(|| encode_tlvs(&tlvs)));
    g.bench_function("tlv/decode", |b| b.iter(|| decode_tlvs(&tlv_bytes)));

    g.finish();
}

criterion_group!(benches, bench_codec);
criterion_main!(benches);
