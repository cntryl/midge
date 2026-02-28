//! Tier 1 — Hot Path TLV Encoding Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers common TLV primitive encoding/decoding hot paths:
//! - Varint32 encoding/decoding (critical for SST and WAL)
//! - Tagged field encoding (bytes, u64, u8)
//! - Field decoding from serialized TLV
//!
//! These are the foundational building blocks used by both WAL and SST,
//! so performance here directly impacts overall I/O throughput.

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::BytesMut;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier1;

use cntryl_midge::common::tlv::{
    decode_tlv_field, decode_varint32, encode_bytes_with_tag, encode_u64_with_tag,
    encode_u8_with_tag, encode_varint32, encode_varint_with_tag,
};
use std::hint::black_box;

// ============================================================================
// Varint Encoding Benchmarks (Most Critical Path)
// ============================================================================

/// Benchmark varint32 encoding for various value ranges.
/// Varints are used extensively in both WAL and SST encoding.
fn bench_varint32_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint32_encode");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let test_cases = [
        ("small_1", 1u32),
        ("small_127", 127u32),
        ("medium_256", 256u32),
        ("medium_16384", 16_384u32),
        ("large_1m", 1_000_000u32),
        ("max", u32::MAX),
    ];

    for (name, value) in test_cases {
        group.bench_function(name, |b| {
            let mut buf = BytesMut::with_capacity(5);
            b.iter(|| {
                buf.clear();
                encode_varint32(&mut buf, black_box(value));
                black_box(buf.as_ref());
            });
        });
    }

    group.finish();
}

/// Benchmark varint32 decoding for various value ranges.
fn bench_varint32_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint32_decode");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Pre-encode test values
    let test_cases = vec![
        ("small_1", 1u32),
        ("small_127", 127u32),
        ("medium_256", 256u32),
        ("medium_16384", 16_384u32),
        ("large_1m", 1_000_000u32),
        ("max", u32::MAX),
    ];

    for (name, value) in &test_cases {
        let mut buf = BytesMut::with_capacity(5);
        encode_varint32(&mut buf, *value);
        let data = buf.freeze();

        group.bench_function(*name, |b| {
            b.iter(|| black_box(decode_varint32(black_box(data.as_ref())).unwrap()));
        });
    }

    group.finish();
}

// ============================================================================
// Tagged Field Encoding Benchmarks
// ============================================================================

/// Benchmark tagged field encoding for fixed sizes and values.
fn bench_tagged_field_encoding(c: &mut Criterion) {
    let mut group_u8 = c.benchmark_group("hotpath_tlv_encode_u8_tag");
    group_u8.sampling_mode(SamplingMode::Flat);
    group_u8.throughput(Throughput::Elements(1));

    let u8_cases = [
        ("u8_0", 0u8),
        ("u8_1", 1u8),
        ("u8_127", 127u8),
        ("u8_255", 255u8),
    ];
    for (name, value) in u8_cases {
        group_u8.bench_function(name, |b| {
            let mut buf = BytesMut::with_capacity(3);
            b.iter(|| {
                buf.clear();
                encode_u8_with_tag(&mut buf, 7, black_box(value));
                black_box(buf.as_ref());
            });
        });
    }

    group_u8.finish();

    let mut group_u64 = c.benchmark_group("hotpath_tlv_encode_u64_tag");
    group_u64.sampling_mode(SamplingMode::Flat);
    group_u64.throughput(Throughput::Elements(1));

    let u64_cases = [
        ("u64_0", 0u64),
        ("u64_1000000", 1_000_000u64),
        ("u64_9223372036854775807", i64::MAX as u64),
        ("u64_18446744073709551615", u64::MAX),
    ];
    for (name, value) in u64_cases {
        group_u64.bench_function(name, |b| {
            let mut buf = BytesMut::with_capacity(10);
            b.iter(|| {
                buf.clear();
                encode_u64_with_tag(&mut buf, 9, black_box(value));
                black_box(buf.as_ref());
            });
        });
    }

    group_u64.finish();

    let mut group_bytes = c.benchmark_group("hotpath_tlv_encode_bytes_tag");
    group_bytes.sampling_mode(SamplingMode::Flat);
    group_bytes.throughput(Throughput::Elements(1));

    let small = [0u8; 8];
    let medium = [1u8; 64];
    let large = [2u8; 256];
    let bytes_cases = [
        ("small_8b", small.as_slice()),
        ("medium_64b", medium.as_slice()),
        ("large_256b", large.as_slice()),
    ];

    for (name, data) in bytes_cases {
        group_bytes.bench_function(name, |b| {
            let mut buf = BytesMut::with_capacity(1 + 5 + data.len());
            b.iter(|| {
                buf.clear();
                encode_bytes_with_tag(&mut buf, 11, black_box(data)).unwrap();
                black_box(buf.as_ref());
            });
        });
    }

    group_bytes.finish();
}

/// Benchmark decoding a tagged field from serialized TLV data.
fn bench_tlv_field_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_decode_field");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let small = [0u8; 8];
    let medium = [1u8; 64];
    let large = [2u8; 256];
    let decode_cases = [
        ("small_8b", small.as_slice()),
        ("medium_64b", medium.as_slice()),
        ("large_256b", large.as_slice()),
    ];

    for (name, data) in decode_cases {
        let mut buf = BytesMut::with_capacity(1 + 5 + data.len());
        encode_bytes_with_tag(&mut buf, 11, data).unwrap();
        let encoded = buf.freeze();

        group.bench_function(name, |b| {
            b.iter(|| black_box(decode_tlv_field(black_box(encoded.as_ref())).unwrap()));
        });
    }

    group.finish();
}

// ============================================================================
// Batch Encoding Benchmarks (Realistic SST/WAL Pattern)
// ============================================================================

/// Benchmark encoding multiple fields in sequence (realistic SST entry encoding pattern).
fn bench_batch_field_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_batch_encode");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1)); // Per complete entry

    let key_delta = black_box(b"mykey");
    let value = black_box(b"myvalue");
    let seq = black_box(12345u64);
    let entry_type = black_box(0u8);

    group.bench_function("sst_entry_full", |b| {
        let mut buf = BytesMut::with_capacity(256);
        b.iter(|| {
            buf.clear();
            // Simulate SST entry encoding: shared_len + key_delta + value + sequence + entry_type
            encode_varint_with_tag(&mut buf, 1, 0);
            encode_bytes_with_tag(&mut buf, 2, key_delta).unwrap();
            encode_bytes_with_tag(&mut buf, 3, value).unwrap();
            encode_u64_with_tag(&mut buf, 4, seq);
            encode_u8_with_tag(&mut buf, 5, entry_type);
            black_box(&buf);
        });
    });

    group.finish();
}

// ============================================================================
// Criterion Main
// ============================================================================

criterion_group!(
    name = benches;
    config = criterion_config_for_tier1();
    targets =
        bench_varint32_encode,
        bench_varint32_decode,
        bench_tagged_field_encoding,
        bench_tlv_field_decode,
        bench_batch_field_encoding
);

criterion_main!(benches);
