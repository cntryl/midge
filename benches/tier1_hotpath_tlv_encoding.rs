//! Tier 1 — Hot Path TLV Encoding Benchmarks
//!
//! Covers common TLV primitive encoding/decoding hot paths used by WAL and SST.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::common::tlv::{
    decode_tlv_field, decode_varint32, encode_bytes_with_tag, encode_u64_with_tag,
    encode_u8_with_tag, encode_varint32, encode_varint_with_tag,
};
use cntryl_midge::BytesMut;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

cntryl_stress::stress_allocator!();

const TLV_PRIMITIVE_BATCH_SIZE: usize = 1_048_576;
const TLV_FIELD_DECODE_BATCH_SIZE: usize = 262_144;
const TLV_PRIMITIVES_PER_LOGICAL_OPERATION: usize = 32;

fn tlv_logical_operation_count() -> u64 {
    tlv_logical_operation_count_for(TLV_PRIMITIVE_BATCH_SIZE)
}

fn tlv_logical_operation_count_for(batch_size: usize) -> u64 {
    let logical_operations = batch_size / TLV_PRIMITIVES_PER_LOGICAL_OPERATION;
    u64::try_from(logical_operations).expect("TLV logical operation count fits in u64")
}

fn record_tlv_batch_parameters(ctx: &mut StressContext) {
    record_tlv_batch_parameters_for(ctx, TLV_PRIMITIVE_BATCH_SIZE);
}

fn record_tlv_batch_parameters_for(ctx: &mut StressContext, batch_size: usize) {
    ctx.parameter("batch_size", batch_size);
    ctx.parameter(
        "primitives_per_logical_operation",
        TLV_PRIMITIVES_PER_LOGICAL_OPERATION,
    );
    ctx.parameter("logical_unit", "tlv_primitive_batch");
}

fn varied_varint32_value(base: u32, index: usize) -> u32 {
    let offset = u32::try_from(index % 16_384).expect("batch index fits in u32");
    match base {
        0..=127 => 1 + (base.saturating_sub(1).wrapping_add(offset) % 127),
        128..=16_383 => 128 + (base.saturating_sub(128).wrapping_add(offset) % 16_256),
        16_384..=2_097_151 => {
            16_384 + (base.saturating_sub(16_384).wrapping_add(offset) % 2_080_768)
        }
        2_097_152..=268_435_455 => {
            2_097_152 + (base.saturating_sub(2_097_152).wrapping_add(offset) % 266_338_304)
        }
        _ if base > u32::MAX - 16_384 => base.saturating_sub(offset),
        _ => base.wrapping_add(offset),
    }
}

fn varied_payload(data: &[u8], index: usize) -> Vec<u8> {
    let mut payload = data.to_vec();
    if let Some(first) = payload.first_mut() {
        *first = first.wrapping_add(u8::try_from(index % 251).expect("index remainder fits in u8"));
    }
    if let Some(last) = payload.last_mut() {
        *last ^= u8::try_from(index % 239).expect("index remainder fits in u8");
    }
    payload
}

fn run_varint32_encode(ctx: &mut StressContext, name: &'static str, value: u32) {
    let mut buf = BytesMut::with_capacity(5);
    encode_varint32(&mut buf, value);
    assert_eq!(decode_varint32(buf.as_ref()).unwrap(), value);
    buf.clear();
    let values: Vec<u32> = (0..TLV_PRIMITIVE_BATCH_SIZE)
        .map(|i| varied_varint32_value(value, i))
        .collect();
    ctx.parameter("case", name);
    record_tlv_batch_parameters(ctx);

    let measurement_name = format!("varint32_encode_{name}");
    stress_config::measure_hot_path_batch(
        ctx,
        measurement_name,
        tlv_logical_operation_count(),
        || {
            let mut encoded_len = 0usize;
            for value in &values {
                buf.clear();
                encode_varint32(&mut buf, black_box(*value));
                encoded_len = encoded_len.wrapping_add(buf.len());
                black_box(buf.as_ref());
            }
            black_box(encoded_len);
        },
    );
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_encode_small_1",
        validated_micro = "true"
    )
)]
fn varint32_encode_small_1(ctx: &mut StressContext) {
    run_varint32_encode(ctx, "small_1", 1);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_encode_small_127",
        validated_micro = "true"
    )
)]
fn varint32_encode_small_127(ctx: &mut StressContext) {
    run_varint32_encode(ctx, "small_127", 127);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_encode_medium_256",
        validated_micro = "true"
    )
)]
fn varint32_encode_medium_256(ctx: &mut StressContext) {
    run_varint32_encode(ctx, "medium_256", 256);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_encode_medium_16384",
        validated_micro = "true"
    )
)]
fn varint32_encode_medium_16384(ctx: &mut StressContext) {
    run_varint32_encode(ctx, "medium_16384", 16_384);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_encode_large_1m",
        validated_micro = "true"
    )
)]
fn varint32_encode_large_1m(ctx: &mut StressContext) {
    run_varint32_encode(ctx, "large_1m", 1_000_000);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_encode_max",
        validated_micro = "true"
    )
)]
fn varint32_encode_max(ctx: &mut StressContext) {
    run_varint32_encode(ctx, "max", u32::MAX);
}

fn run_varint32_decode(ctx: &mut StressContext, name: &'static str, value: u32) {
    let mut buf = BytesMut::with_capacity(5);
    encode_varint32(&mut buf, value);
    let data = buf.freeze();
    assert_eq!(decode_varint32(data.as_ref()).unwrap(), value);
    let encoded_values: Vec<_> = (0..TLV_PRIMITIVE_BATCH_SIZE)
        .map(|i| {
            let mut encoded = BytesMut::with_capacity(5);
            encode_varint32(&mut encoded, varied_varint32_value(value, i));
            encoded.freeze()
        })
        .collect();
    ctx.parameter("case", name);
    record_tlv_batch_parameters(ctx);

    let measurement_name = format!("varint32_decode_{name}");
    stress_config::measure_hot_path_batch(
        ctx,
        measurement_name,
        tlv_logical_operation_count(),
        || {
            let mut decoded = 0u32;
            for data in &encoded_values {
                decoded = decoded.wrapping_add(decode_varint32(black_box(data.as_ref())).unwrap());
            }
            black_box(decoded);
        },
    );
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_decode_small_1",
        validated_micro = "true"
    )
)]
fn varint32_decode_small_1(ctx: &mut StressContext) {
    run_varint32_decode(ctx, "small_1", 1);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_decode_small_127",
        validated_micro = "true"
    )
)]
fn varint32_decode_small_127(ctx: &mut StressContext) {
    run_varint32_decode(ctx, "small_127", 127);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_decode_medium_256",
        validated_micro = "true"
    )
)]
fn varint32_decode_medium_256(ctx: &mut StressContext) {
    run_varint32_decode(ctx, "medium_256", 256);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_decode_medium_16384",
        validated_micro = "true"
    )
)]
fn varint32_decode_medium_16384(ctx: &mut StressContext) {
    run_varint32_decode(ctx, "medium_16384", 16_384);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_decode_large_1m",
        validated_micro = "true"
    )
)]
fn varint32_decode_large_1m(ctx: &mut StressContext) {
    run_varint32_decode(ctx, "large_1m", 1_000_000);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "varint32_decode_max",
        validated_micro = "true"
    )
)]
fn varint32_decode_max(ctx: &mut StressContext) {
    run_varint32_decode(ctx, "max", u32::MAX);
}

fn run_encode_u8_tag(ctx: &mut StressContext, name: &'static str, value: u8) {
    let mut buf = BytesMut::with_capacity(3);
    encode_u8_with_tag(&mut buf, 7, value);
    let (tag, decoded, consumed) = decode_tlv_field(buf.as_ref()).unwrap();
    assert_eq!(tag, 7);
    assert_eq!(decoded, &[value]);
    assert_eq!(consumed, buf.len());
    buf.clear();
    ctx.parameter("case", name);
    record_tlv_batch_parameters(ctx);

    let measurement_name = format!("encode_u8_tag_{name}");
    stress_config::measure_hot_path_batch(
        ctx,
        measurement_name,
        tlv_logical_operation_count(),
        || {
            let mut encoded_len = 0usize;
            for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
                buf.clear();
                encode_u8_with_tag(&mut buf, 7, black_box(value));
                encoded_len = encoded_len.wrapping_add(buf.len());
                black_box(buf.as_ref());
            }
            black_box(encoded_len);
        },
    );
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_0",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_0(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "0", 0);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_1",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_1(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "1", 1);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_127",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_127(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "127", 127);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_255",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_255(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "255", 255);
}

fn run_encode_u64_tag(ctx: &mut StressContext, name: &'static str, value: u64) {
    let mut buf = BytesMut::with_capacity(10);
    encode_u64_with_tag(&mut buf, 9, value);
    let (tag, decoded, consumed) = decode_tlv_field(buf.as_ref()).unwrap();
    assert_eq!(tag, 9);
    assert_eq!(decoded, value.to_be_bytes().as_slice());
    assert_eq!(consumed, buf.len());
    buf.clear();
    ctx.parameter("case", name);
    record_tlv_batch_parameters(ctx);

    let measurement_name = format!("encode_u64_tag_{name}");
    stress_config::measure_hot_path_batch(
        ctx,
        measurement_name,
        tlv_logical_operation_count(),
        || {
            let mut encoded_len = 0usize;
            for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
                buf.clear();
                encode_u64_with_tag(&mut buf, 9, black_box(value));
                encoded_len = encoded_len.wrapping_add(buf.len());
                black_box(buf.as_ref());
            }
            black_box(encoded_len);
        },
    );
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_0",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_0(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "0", 0);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_1m",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_1m(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "1m", 1_000_000);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_i64_max",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_i64_max(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "i64_max", i64::MAX as u64);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_max",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_max(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "max", u64::MAX);
}

fn run_encode_bytes_tag(ctx: &mut StressContext, name: &'static str, data: &[u8]) {
    let mut buf = BytesMut::with_capacity(1 + 5 + data.len());
    encode_bytes_with_tag(&mut buf, 11, data).unwrap();
    let (tag, value, consumed) = decode_tlv_field(buf.as_ref()).unwrap();
    assert_eq!(tag, 11);
    assert_eq!(value, data);
    assert_eq!(consumed, buf.len());
    buf.clear();
    ctx.parameter("case", name);
    ctx.parameter("payload_size", data.len());
    record_tlv_batch_parameters(ctx);

    let measurement_name = format!("encode_bytes_tag_{name}");
    stress_config::measure_hot_path_batch(
        ctx,
        measurement_name,
        tlv_logical_operation_count(),
        || {
            let mut encoded_len = 0usize;
            for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
                buf.clear();
                encode_bytes_with_tag(&mut buf, 11, black_box(data)).unwrap();
                encoded_len = encoded_len.wrapping_add(buf.len());
                black_box(buf.as_ref());
            }
            black_box(encoded_len);
        },
    );
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_bytes_tag_8b",
        validated_micro = "true"
    )
)]
fn encode_bytes_tag_8b(ctx: &mut StressContext) {
    run_encode_bytes_tag(ctx, "8b", &[0u8; 8]);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_bytes_tag_64b",
        validated_micro = "true"
    )
)]
fn encode_bytes_tag_64b(ctx: &mut StressContext) {
    run_encode_bytes_tag(ctx, "64b", &[1u8; 64]);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_bytes_tag_256b",
        validated_micro = "true"
    )
)]
fn encode_bytes_tag_256b(ctx: &mut StressContext) {
    run_encode_bytes_tag(ctx, "256b", &[2u8; 256]);
}

fn run_decode_field(ctx: &mut StressContext, name: &'static str, data: &[u8]) {
    let mut buf = BytesMut::with_capacity(1 + 5 + data.len());
    encode_bytes_with_tag(&mut buf, 11, data).unwrap();
    let encoded = buf.freeze();
    let (tag, value, consumed) = decode_tlv_field(encoded.as_ref()).unwrap();
    assert_eq!(tag, 11);
    assert_eq!(value, data);
    assert_eq!(consumed, encoded.len());
    let encoded_fields: Vec<_> = (0..TLV_FIELD_DECODE_BATCH_SIZE)
        .map(|i| {
            let payload = varied_payload(data, i);
            let mut encoded = BytesMut::with_capacity(1 + 5 + payload.len());
            encode_bytes_with_tag(&mut encoded, 11, payload.as_slice()).unwrap();
            encoded.freeze()
        })
        .collect();
    ctx.parameter("case", name);
    ctx.parameter("payload_size", data.len());
    record_tlv_batch_parameters_for(ctx, TLV_FIELD_DECODE_BATCH_SIZE);

    let measurement_name = format!("decode_field_{name}");
    stress_config::measure_hot_path_batch(
        ctx,
        measurement_name,
        tlv_logical_operation_count_for(TLV_FIELD_DECODE_BATCH_SIZE),
        || {
            let mut total = 0usize;
            for encoded in &encoded_fields {
                let (tag, value, consumed) = decode_tlv_field(black_box(encoded.as_ref())).unwrap();
                total = total.wrapping_add(usize::from(tag));
                total = total.wrapping_add(value.len());
                total = total.wrapping_add(consumed);
                total = total.wrapping_add(usize::from(value.first().copied().unwrap_or(0)));
                total = total.wrapping_add(usize::from(value.last().copied().unwrap_or(0)));
            }
            black_box(total);
        },
    );
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "decode_field_8b",
        validated_micro = "true"
    )
)]
fn decode_field_8b(ctx: &mut StressContext) {
    run_decode_field(ctx, "8b", &[0u8; 8]);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "decode_field_64b",
        validated_micro = "true"
    )
)]
fn decode_field_64b(ctx: &mut StressContext) {
    run_decode_field(ctx, "64b", &[1u8; 64]);
}

#[stress(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "decode_field_256b",
        validated_micro = "true"
    )
)]
fn decode_field_256b(ctx: &mut StressContext) {
    run_decode_field(ctx, "256b", &[2u8; 256]);
}

#[stress(tier = 1, metadata(component = "tlv", scenario = "sst_entry_full"))]
fn sst_entry_full(ctx: &mut StressContext) {
    let key_delta = black_box(b"mykey");
    let value = black_box(b"myvalue");
    let seq = black_box(12345u64);
    let entry_type = black_box(0u8);
    let mut buf = BytesMut::with_capacity(256);
    record_tlv_batch_parameters(ctx);

    stress_config::measure_hot_path_batch(
        ctx,
        "sst_entry_full",
        tlv_logical_operation_count(),
        || {
            let mut encoded_len = 0usize;
            for i in 0..TLV_PRIMITIVE_BATCH_SIZE {
                buf.clear();
                encode_varint_with_tag(&mut buf, 1, 0);
                encode_bytes_with_tag(&mut buf, 2, key_delta).unwrap();
                encode_bytes_with_tag(&mut buf, 3, value).unwrap();
                let sequence_offset = u64::try_from(i).expect("batch index fits in u64");
                encode_u64_with_tag(&mut buf, 4, seq.wrapping_add(sequence_offset));
                encode_u8_with_tag(&mut buf, 5, entry_type);
                encoded_len = encoded_len.wrapping_add(buf.len());
                black_box(buf.as_ref());
            }
            black_box(encoded_len);
        },
    );
}

stress_main!();
