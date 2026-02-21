//! Tier 1 — Hot Path WAL Encoding Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers WAL record encoding/decoding hot paths:
//! - TLV format serialization/deserialization
//! - Fast path optimizations (encode_delete, encode_put_simple)
//! - Parallel encoding for batches
//!
//! Note: I/O benchmarks are in tier2_subsystem/wal_io.rs

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier1;

use cntryl_midge::wal::encoding::{decode, encode};
use cntryl_midge::wal::{WalOpKind, WalRecord};
use std::hint::black_box;

// ============================================================================
// Encoding Benchmarks
// ============================================================================

/// Benchmark WAL record encoding (TLV format) with different sizes.
fn bench_wal_encode_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_encode");
    group.sampling_mode(SamplingMode::Flat);

    let small_key = Bytes::from_static(b"key");
    let small_value = Bytes::from_static(b"value");
    let medium_key: Bytes = Bytes::from_static(&[0u8; 64]);
    let medium_value: Bytes = Bytes::from_static(&[0u8; 256]);
    let delete_key = Bytes::from_static(b"deleted_key");

    let test_cases = [
        (
            "small_put",
            WalRecord::new(
                WalOpKind::Put,
                small_key.clone(),
                Some(small_value.clone()),
                1,
                1,
            ),
        ),
        (
            "medium_put",
            WalRecord::new(
                WalOpKind::Put,
                medium_key.clone(),
                Some(medium_value.clone()),
                1,
                1,
            ),
        ),
        (
            "delete",
            WalRecord::new(WalOpKind::Delete, delete_key.clone(), None, 1, 1),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, record) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| black_box(encode(&record).unwrap()));
        });
    }

    group.finish();
}

/// Benchmark WAL record decoding (TLV format).
fn bench_wal_decode_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_decode");

    let small_key = Bytes::from_static(b"key");
    let small_value = Bytes::from_static(b"value");
    let medium_key: Bytes = Bytes::from_static(&[0u8; 64]);
    let medium_value: Bytes = Bytes::from_static(&[0u8; 256]);
    let delete_key = Bytes::from_static(b"deleted_key");

    let test_cases = [
        (
            "small_put",
            encode(&WalRecord::new(
                WalOpKind::Put,
                small_key.clone(),
                Some(small_value.clone()),
                1,
                1,
            ))
            .unwrap(),
        ),
        (
            "medium_put",
            encode(&WalRecord::new(
                WalOpKind::Put,
                medium_key.clone(),
                Some(medium_value.clone()),
                1,
                1,
            ))
            .unwrap(),
        ),
        (
            "delete",
            encode(&WalRecord::new(
                WalOpKind::Delete,
                delete_key.clone(),
                None,
                1,
                1,
            ))
            .unwrap(),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, encoded) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| black_box(decode(encoded.clone()).unwrap()));
        });
    }

    group.finish();
}

/// Benchmark full encode -> decode round-trip.
fn bench_wal_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_roundtrip");

    let small_key = Bytes::from_static(b"key");
    let small_value = Bytes::from_static(b"value");
    let medium_key: Bytes = Bytes::from_static(&[0u8; 64]);
    let medium_value: Bytes = Bytes::from_static(&[0u8; 256]);

    let test_cases = [
        (
            "small",
            WalRecord::new(
                WalOpKind::Put,
                small_key.clone(),
                Some(small_value.clone()),
                1,
                1,
            ),
        ),
        (
            "medium",
            WalRecord::new(
                WalOpKind::Put,
                medium_key.clone(),
                Some(medium_value.clone()),
                1,
                1,
            ),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, record) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| {
                let encoded = encode(&record).unwrap();
                black_box(decode(encoded).unwrap())
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_wal;
    config = criterion_config_for_tier1();
    targets =
        bench_wal_encode_record,
        bench_wal_decode_record,
        bench_wal_roundtrip
}
criterion_main!(tier1_hotpath_wal);
