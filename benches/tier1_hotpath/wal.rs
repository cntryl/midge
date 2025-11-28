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

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::wal::encode_pipeline::{EncodeConfig, WalEncoder};
use cntryl_midge::wal::encoding::{decode, encode, encode_delete, encode_put_simple};
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
            ),
        ),
        (
            "medium_put",
            WalRecord::new(
                WalOpKind::Put,
                medium_key.clone(),
                Some(medium_value.clone()),
                1,
            ),
        ),
        (
            "delete",
            WalRecord::new(
                WalOpKind::Delete,
                delete_key.clone(),
                None,
                1,
            ),
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
            ))
            .unwrap(),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, encoded) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| black_box(decode(&encoded).unwrap()));
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
            ),
        ),
        (
            "medium",
            WalRecord::new(
                WalOpKind::Put,
                medium_key.clone(),
                Some(medium_value.clone()),
                1,
            ),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, record) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| {
                let encoded = encode(&record).unwrap();
                black_box(decode(&encoded).unwrap())
            });
        });
    }

    group.finish();
}

// ============================================================================
// Fast Path Optimizations
// ============================================================================

/// Benchmark specialized encode_delete fast path vs normal encode.
fn bench_wal_encode_delete_fast_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_encode_delete");

    let cf_id = 0;
    let seq = 1;
    let key = b"deleted_key";
    let delete_record = WalRecord::new(WalOpKind::Delete, Bytes::from_static(key), None, seq);

    group.throughput(Throughput::Elements(1));

    group.bench_function("normal_encode", |b| {
        b.iter(|| black_box(encode(&delete_record).unwrap()));
    });

    group.bench_function("fast_path", |b| {
        b.iter(|| black_box(encode_delete(cf_id, seq, key)));
    });

    group.finish();
}

/// Benchmark specialized encode_put_simple fast path vs normal encode.
fn bench_wal_encode_put_fast_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_encode_put");

    let tiny_key: &[u8] = b"k";
    let tiny_value: &[u8] = b"v";
    let small_key: &[u8] = b"test_key";
    let small_value: &[u8] = b"test_value";
    let medium_key: &[u8] = &[0u8; 32];
    let medium_value: &[u8] = &[0u8; 128];

    let test_cases = [
        ("tiny", tiny_key, tiny_value),
        ("small", small_key, small_value),
        ("medium", medium_key, medium_value),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, key, value) in test_cases {
        let cf_id = 0;
        let seq = 1;
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
        );

        group.bench_function(format!("{}_normal", name), |b| {
            b.iter(|| black_box(encode(&put_record).unwrap()));
        });

        group.bench_function(format!("{}_fast", name), |b| {
            b.iter(|| black_box(encode_put_simple(cf_id, seq, key, value)));
        });
    }

    group.finish();
}

// ============================================================================
// Parallel Encoding
// ============================================================================

/// Benchmark sequential vs parallel batch encoding.
fn bench_wal_batch_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_parallel_encode");

    // Test smaller batch sizes for tier1
    for batch_size in [50, 100] {
        let records: Vec<WalRecord> = (0..batch_size)
            .map(|i| {
                WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from(format!("key{:08}", i)),
                    Some(Bytes::from(vec![0u8; 256])), // Smaller values for faster tier1
                    i as u64,
                )
            })
            .collect();

        group.throughput(Throughput::Elements(batch_size as u64));

        let encoder_seq = WalEncoder::with_config(EncodeConfig {
            parallelism: 1,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        })
        .unwrap();

        group.bench_with_input(
            BenchmarkId::new("sequential", batch_size),
            &records,
            |b, recs| {
                b.iter(|| encoder_seq.encode_batch(recs).unwrap());
            },
        );

        let encoder_par = WalEncoder::with_config(EncodeConfig {
            parallelism: 4,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        })
        .unwrap();

        group.bench_with_input(
            BenchmarkId::new("parallel_4", batch_size),
            &records,
            |b, recs| {
                b.iter(|| encoder_par.encode_batch(recs).unwrap());
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_wal;
    config = criterion_config();
    targets =
        bench_wal_encode_record,
        bench_wal_decode_record,
        bench_wal_roundtrip,
        bench_wal_encode_delete_fast_path,
        bench_wal_encode_put_fast_path,
        bench_wal_batch_encode
}
criterion_main!(tier1_hotpath_wal);
