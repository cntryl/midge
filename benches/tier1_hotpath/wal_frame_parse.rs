//! Tier 1 — WAL record parsing hot path
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers WAL record encoding/decoding for different frame sizes

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::wal::encoding::{decode, decode_borrowed, encode};
use cntryl_midge::wal::{WalOpKind, WalRecord};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Create a WAL record with specified key/value sizes
fn make_wal_record(key_size: usize, value_size: usize, seq: u64) -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from(vec![b'k'; key_size]),
        Some(Bytes::from(vec![b'v'; value_size])),
        seq,
    )
}

/// Encode a WAL record to bytes
fn make_encoded_frame(key_size: usize, value_size: usize) -> Bytes {
    let record = make_wal_record(key_size, value_size, 42);
    encode(&record).expect("encode failed").into()
}

/// Benchmark small WAL record parsing (16-byte key, 64-byte value)
fn bench_wal_frame_parse_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_frame_parse_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let encoded = make_encoded_frame(16, 64);

    group.bench_function("parse_small_frame", |b| {
        b.iter(|| {
            let record = decode(&encoded).expect("decode failed");
            black_box(record);
        })
    });

    group.finish();
}

/// Benchmark medium WAL record parsing (64-byte key, 1KB value)
fn bench_wal_frame_parse_medium(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_frame_parse_medium");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let encoded = make_encoded_frame(64, 1024);

    group.bench_function("parse_medium_frame", |b| {
        b.iter(|| {
            let record = decode(&encoded).expect("decode failed");
            black_box(record);
        })
    });

    group.finish();
}

/// Benchmark large WAL record parsing (256-byte key, 4KB value)
fn bench_wal_frame_parse_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_frame_parse_large");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let encoded = make_encoded_frame(256, 4096);

    group.bench_function("parse_large_frame", |b| {
        b.iter(|| {
            let record = decode(&encoded).expect("decode failed");
            black_box(record);
        })
    });

    group.finish();
}

/// Benchmark partial decode (extract operation type without full parse)
/// Simulates scanning WAL for specific operation types
fn bench_wal_header_scan_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_header_scan_only");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let encoded = make_encoded_frame(64, 1024);

    group.bench_function("header_scan_only", |b| {
        b.iter(|| {
            // Decode just enough to get operation type
            let record = decode(&encoded).expect("decode failed");
            let op_kind = record.op;
            black_box(op_kind);
        })
    });

    group.finish();
}

/// Benchmark zero-copy decode vs allocating decode
fn bench_wal_zero_copy_vs_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_zero_copy");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let encoded = make_encoded_frame(64, 1024);

    // Allocating decode (current default)
    group.bench_function("decode_allocating", |b| {
        b.iter(|| {
            let record = decode(&encoded).expect("decode failed");
            black_box(record);
        })
    });

    // Zero-copy decode (no allocation)
    group.bench_function("decode_borrowed", |b| {
        b.iter(|| {
            let record = decode_borrowed(&encoded).expect("decode failed");
            black_box(record);
        })
    });

    group.finish();
}

criterion_group! {
    name = wal_frame_parse_group;
    config = criterion_config();
    targets = bench_wal_frame_parse_small, bench_wal_frame_parse_medium, bench_wal_frame_parse_large, bench_wal_header_scan_only, bench_wal_zero_copy_vs_alloc
}
criterion_main!(wal_frame_parse_group);
