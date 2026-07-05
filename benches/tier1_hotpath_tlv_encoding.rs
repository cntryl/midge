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
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

cntryl_stress::stress_allocator!();

const TLV_PRIMITIVE_BATCH_SIZE: usize = 2048;

fn run_varint32_encode(ctx: &mut StressContext, name: &'static str, value: u32) {
    let mut buf = BytesMut::with_capacity(5);
    encode_varint32(&mut buf, value);
    assert_eq!(decode_varint32(buf.as_ref()).unwrap(), value);
    buf.clear();
    ctx.parameter("case", name);
    ctx.parameter("batch_size", TLV_PRIMITIVE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TLV_PRIMITIVE_BATCH_SIZE as u64, || {
        let mut encoded_len = 0usize;
        for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
            buf.clear();
            encode_varint32(&mut buf, black_box(value));
            encoded_len = encoded_len.wrapping_add(buf.len());
            black_box(buf.as_ref());
        }
        black_box(encoded_len);
    });
}

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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
    ctx.parameter("case", name);
    ctx.parameter("batch_size", TLV_PRIMITIVE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TLV_PRIMITIVE_BATCH_SIZE as u64, || {
        let mut decoded = 0u32;
        for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
            decoded ^= decode_varint32(black_box(data.as_ref())).unwrap();
        }
        black_box(decoded);
    });
}

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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

#[stress_test(
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
    ctx.parameter("batch_size", TLV_PRIMITIVE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TLV_PRIMITIVE_BATCH_SIZE as u64, || {
        let mut encoded_len = 0usize;
        for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
            buf.clear();
            encode_u8_with_tag(&mut buf, 7, black_box(value));
            encoded_len = encoded_len.wrapping_add(buf.len());
            black_box(buf.as_ref());
        }
        black_box(encoded_len);
    });
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_0",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_0(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "u8_0", 0);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_1",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_1(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "u8_1", 1);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_127",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_127(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "u8_127", 127);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u8_tag_255",
        validated_micro = "true"
    )
)]
fn encode_u8_tag_255(ctx: &mut StressContext) {
    run_encode_u8_tag(ctx, "u8_255", 255);
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
    ctx.parameter("batch_size", TLV_PRIMITIVE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TLV_PRIMITIVE_BATCH_SIZE as u64, || {
        let mut encoded_len = 0usize;
        for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
            buf.clear();
            encode_u64_with_tag(&mut buf, 9, black_box(value));
            encoded_len = encoded_len.wrapping_add(buf.len());
            black_box(buf.as_ref());
        }
        black_box(encoded_len);
    });
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_0",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_0(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "u64_0", 0);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_1m",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_1m(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "u64_1000000", 1_000_000);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_i64_max",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_i64_max(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "u64_9223372036854775807", i64::MAX as u64);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_u64_tag_max",
        validated_micro = "true"
    )
)]
fn encode_u64_tag_max(ctx: &mut StressContext) {
    run_encode_u64_tag(ctx, "u64_18446744073709551615", u64::MAX);
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
    ctx.parameter("batch_size", TLV_PRIMITIVE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TLV_PRIMITIVE_BATCH_SIZE as u64, || {
        let mut encoded_len = 0usize;
        for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
            buf.clear();
            encode_bytes_with_tag(&mut buf, 11, black_box(data)).unwrap();
            encoded_len = encoded_len.wrapping_add(buf.len());
            black_box(buf.as_ref());
        }
        black_box(encoded_len);
    });
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_bytes_tag_8b",
        validated_micro = "true"
    )
)]
fn encode_bytes_tag_8b(ctx: &mut StressContext) {
    run_encode_bytes_tag(ctx, "small_8b", &[0u8; 8]);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_bytes_tag_64b",
        validated_micro = "true"
    )
)]
fn encode_bytes_tag_64b(ctx: &mut StressContext) {
    run_encode_bytes_tag(ctx, "medium_64b", &[1u8; 64]);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "encode_bytes_tag_256b",
        validated_micro = "true"
    )
)]
fn encode_bytes_tag_256b(ctx: &mut StressContext) {
    run_encode_bytes_tag(ctx, "large_256b", &[2u8; 256]);
}

fn run_decode_field(ctx: &mut StressContext, name: &'static str, data: &[u8]) {
    let mut buf = BytesMut::with_capacity(1 + 5 + data.len());
    encode_bytes_with_tag(&mut buf, 11, data).unwrap();
    let encoded = buf.freeze();
    let (tag, value, consumed) = decode_tlv_field(encoded.as_ref()).unwrap();
    assert_eq!(tag, 11);
    assert_eq!(value, data);
    assert_eq!(consumed, encoded.len());
    ctx.parameter("case", name);
    ctx.parameter("payload_size", data.len());
    ctx.parameter("batch_size", TLV_PRIMITIVE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TLV_PRIMITIVE_BATCH_SIZE as u64, || {
        let mut total = 0usize;
        for _ in 0..TLV_PRIMITIVE_BATCH_SIZE {
            let (tag, value, consumed) = decode_tlv_field(black_box(encoded.as_ref())).unwrap();
            total = total.wrapping_add(usize::from(tag));
            total = total.wrapping_add(value.len());
            total = total.wrapping_add(consumed);
        }
        black_box(total);
    });
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "decode_field_8b",
        validated_micro = "true"
    )
)]
fn decode_field_8b(ctx: &mut StressContext) {
    run_decode_field(ctx, "small_8b", &[0u8; 8]);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "decode_field_64b",
        validated_micro = "true"
    )
)]
fn decode_field_64b(ctx: &mut StressContext) {
    run_decode_field(ctx, "medium_64b", &[1u8; 64]);
}

#[stress_test(
    tier = 1,
    metadata(
        component = "tlv",
        scenario = "decode_field_256b",
        validated_micro = "true"
    )
)]
fn decode_field_256b(ctx: &mut StressContext) {
    run_decode_field(ctx, "large_256b", &[2u8; 256]);
}

#[stress_test(tier = 1, metadata(component = "tlv", scenario = "sst_entry_full"))]
fn sst_entry_full(ctx: &mut StressContext) {
    let key_delta = black_box(b"mykey");
    let value = black_box(b"myvalue");
    let seq = black_box(12345u64);
    let entry_type = black_box(0u8);
    let mut buf = BytesMut::with_capacity(256);

    ctx.measure_micro(|| {
        buf.clear();
        encode_varint_with_tag(&mut buf, 1, 0);
        encode_bytes_with_tag(&mut buf, 2, key_delta).unwrap();
        encode_bytes_with_tag(&mut buf, 3, value).unwrap();
        encode_u64_with_tag(&mut buf, 4, seq);
        encode_u8_with_tag(&mut buf, 5, entry_type);
        black_box(&buf);
    });
}

stress_main!();
