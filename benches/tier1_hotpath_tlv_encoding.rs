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
    decode_varint32, encode_bytes_with_tag, encode_u64_with_tag, encode_u8_with_tag,
    encode_varint32, encode_varint_with_tag,
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
    config = criterion_config_for_tier1();
    targets =
        bench_varint32_encode,
        bench_varint32_decode,
        bench_batch_field_encoding
);

criterion_main!(benches);
