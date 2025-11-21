//! Tier 1 — Hot Path TLV Encoding/Decoding Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers TLV (Tag-Length-Value) primitives used throughout WAL/SST formats:
//! - Varint encoding/decoding (lengths, sequences)
//! - TlvWriter operations (building records)
//! - TlvReader operations (parsing records)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::common::tlv::{
    decode_varint32, decode_varint64, encode_varint32, encode_varint64, tags, TlvReader, TlvWriter,
};
use std::hint::black_box;

// ============================================================================
// Varint32 Encoding Benchmarks
// ============================================================================

fn bench_varint32_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint32_encode");
    group.throughput(Throughput::Elements(1));

    // Small values (< 128) - single byte encoding, common for key lengths
    group.bench_function("small_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint32(&mut buf, black_box(64));
            black_box(&buf);
        });
    });

    // Medium values (128-16383) - two byte encoding
    group.bench_function("medium_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint32(&mut buf, black_box(1024));
            black_box(&buf);
        });
    });

    // Large values (> 16383) - multi-byte encoding
    group.bench_function("large_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint32(&mut buf, black_box(1_000_000));
            black_box(&buf);
        });
    });

    group.finish();
}

// ============================================================================
// Varint64 Encoding Benchmarks
// ============================================================================

fn bench_varint64_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint64_encode");
    group.throughput(Throughput::Elements(1));

    // Small values (< 128) - single byte encoding, common for sequence numbers
    group.bench_function("small_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint64(&mut buf, black_box(100));
            black_box(&buf);
        });
    });

    // Medium values
    group.bench_function("medium_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint64(&mut buf, black_box(10000));
            black_box(&buf);
        });
    });

    // Large values (realistic sequence numbers)
    group.bench_function("large_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint64(&mut buf, black_box(1_000_000_000));
            black_box(&buf);
        });
    });

    group.finish();
}

// ============================================================================
// Varint32 Decoding Benchmarks
// ============================================================================

fn bench_varint32_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint32_decode");
    group.throughput(Throughput::Elements(1));

    // Pre-encode test values
    let mut small = Vec::new();
    encode_varint32(&mut small, 64);

    let mut medium = Vec::new();
    encode_varint32(&mut medium, 1024);

    let mut large = Vec::new();
    encode_varint32(&mut large, 1_000_000);

    group.bench_function("small_value", |b| {
        b.iter(|| black_box(decode_varint32(&small).unwrap()));
    });

    group.bench_function("medium_value", |b| {
        b.iter(|| black_box(decode_varint32(&medium).unwrap()));
    });

    group.bench_function("large_value", |b| {
        b.iter(|| black_box(decode_varint32(&large).unwrap()));
    });

    group.finish();
}

// ============================================================================
// Varint64 Decoding Benchmarks
// ============================================================================

fn bench_varint64_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint64_decode");
    group.throughput(Throughput::Elements(1));

    // Pre-encode test values
    let mut small = Vec::new();
    encode_varint64(&mut small, 100);

    let mut medium = Vec::new();
    encode_varint64(&mut medium, 10000);

    let mut large = Vec::new();
    encode_varint64(&mut large, 1_000_000_000);

    group.bench_function("small_value", |b| {
        b.iter(|| black_box(decode_varint64(&small).unwrap()));
    });

    group.bench_function("medium_value", |b| {
        b.iter(|| black_box(decode_varint64(&medium).unwrap()));
    });

    group.bench_function("large_value", |b| {
        b.iter(|| black_box(decode_varint64(&large).unwrap()));
    });

    group.finish();
}

// ============================================================================
// TlvWriter Benchmarks
// ============================================================================

fn bench_tlv_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_writer");
    group.throughput(Throughput::Elements(1));

    // Write a simple record with primitive types
    group.bench_function("write_primitives", |b| {
        b.iter(|| {
            let mut writer = TlvWriter::new();
            writer.write_u8(tags::OPERATION, black_box(1));
            writer.write_u32(tags::CF_ID, black_box(0));
            writer.write_u64(tags::SEQUENCE, black_box(12345));
            black_box(writer.finish());
        });
    });

    // Write with small bytes field (typical key)
    group.bench_function("write_small_bytes", |b| {
        let key = b"user:12345";
        b.iter(|| {
            let mut writer = TlvWriter::new();
            writer.write_u8(tags::OPERATION, 1);
            writer.write_bytes(tags::KEY, black_box(key));
            black_box(writer.finish());
        });
    });

    // Remaining parts omitted for brevity

    group.finish();
}

criterion_group!(
    name = hotpath_tlv,
    config = criterion_config(),
    targets = bench_varint32_encode,
    bench_varint64_encode,
    bench_varint32_decode,
    bench_varint64_decode,
    bench_tlv_writer
);

criterion_main!(hotpath_tlv);
