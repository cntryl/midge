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

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};

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
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(5);
                encode_varint32(&mut buf, black_box(value));
                black_box(buf);
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
            b.iter(|| decode_varint32(black_box(data.as_ref())).unwrap());
        });
    }

    group.finish();
}

// ============================================================================
// Tagged Field Encoding Benchmarks
// ============================================================================

/// Benchmark encoding bytes with a tag prefix.
/// Used extensively in SST entry encoding (KEY_DELTA, VALUE fields).
fn bench_encode_bytes_with_tag(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_encode_bytes_tag");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(256));

    let small_bytes = black_box(b"key");
    let medium_bytes = black_box(&[0u8; 64][..]);
    let large_bytes = black_box(&[0u8; 256][..]);

    group.bench_function("small_8b", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(32);
            encode_bytes_with_tag(&mut buf, 2, small_bytes);
            black_box(buf);
        });
    });

    group.bench_function("medium_64b", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(128);
            encode_bytes_with_tag(&mut buf, 2, medium_bytes);
            black_box(buf);
        });
    });

    group.bench_function("large_256b", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(512);
            encode_bytes_with_tag(&mut buf, 3, large_bytes);
            black_box(buf);
        });
    });

    group.finish();
}

/// Benchmark encoding u64 with a tag prefix.
/// Used in SST for sequence numbers and expiration timestamps.
fn bench_encode_u64_with_tag(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_encode_u64_tag");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let test_values = [0u64, 1_000_000, u64::MAX / 2, u64::MAX];

    for value in test_values {
        group.bench_function(format!("u64_{}", value), |b| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(16);
                encode_u64_with_tag(&mut buf, 4, black_box(value));
                black_box(buf);
            });
        });
    }

    group.finish();
}

/// Benchmark encoding u8 with a tag prefix.
/// Used in SST for entry type flags.
fn bench_encode_u8_with_tag(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_encode_u8_tag");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    for value in [0u8, 1, 127, 255] {
        group.bench_function(format!("u8_{}", value), |b| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(8);
                encode_u8_with_tag(&mut buf, 5, black_box(value));
                black_box(buf);
            });
        });
    }

    group.finish();
}

// ============================================================================
// Field Decoding Benchmarks
// ============================================================================

/// Benchmark decoding a TLV field (tag + length + value).
/// Used in hot loops when parsing SST entries.
fn bench_decode_tlv_field(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_decode_field");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(256));

    // Pre-encode test fields
    let mut buf_small = BytesMut::with_capacity(32);
    encode_bytes_with_tag(&mut buf_small, 2, b"key");
    let buf_small = buf_small.freeze();

    let mut buf_medium = BytesMut::with_capacity(128);
    encode_bytes_with_tag(&mut buf_medium, 3, &[0u8; 64][..]);
    let buf_medium = buf_medium.freeze();

    let mut buf_large = BytesMut::with_capacity(512);
    encode_bytes_with_tag(&mut buf_large, 2, &[0u8; 256][..]);
    let buf_large = buf_large.freeze();

    group.bench_function("small_8b", |b| {
        b.iter(|| decode_tlv_field(black_box(buf_small.as_ref())).unwrap());
    });

    group.bench_function("medium_64b", |b| {
        b.iter(|| decode_tlv_field(black_box(buf_medium.as_ref())).unwrap());
    });

    group.bench_function("large_256b", |b| {
        b.iter(|| decode_tlv_field(black_box(buf_large.as_ref())).unwrap());
    });

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
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            // Simulate SST entry encoding: shared_len + key_delta + value + sequence + entry_type
            encode_varint_with_tag(&mut buf, 1, 0);
            encode_bytes_with_tag(&mut buf, 2, key_delta);
            encode_bytes_with_tag(&mut buf, 3, value);
            encode_u64_with_tag(&mut buf, 4, seq);
            encode_u8_with_tag(&mut buf, 5, entry_type);
            black_box(buf);
        });
    });

    group.finish();
}

// ============================================================================
// Criterion Main
// ============================================================================

criterion_group!(
    name = benches;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets =
        bench_varint32_encode,
        bench_varint32_decode,
        bench_encode_bytes_with_tag,
        bench_encode_u64_with_tag,
        bench_encode_u8_with_tag,
        bench_decode_tlv_field,
        bench_batch_field_encoding
);

criterion_main!(benches);
