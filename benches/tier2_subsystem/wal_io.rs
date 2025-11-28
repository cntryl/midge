//! Tier 2 — WAL I/O Subsystem Benchmarks
//!
//! **Target Runtime:** < 8 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers WAL I/O operations:
//! - Append throughput with different sync modes
//! - I/O sequential throughput
//! - Append + sync latency
//! - Raw I/O baseline (pre-encoded)
//! - Platform-specific optimizations (io_uring vs fallback)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use tempfile::tempdir;

use cntryl_midge::fs::{io as fs_io, write_vectored};
use cntryl_midge::wal::encode_pipeline::WalEncoder;
use cntryl_midge::wal::{WalOpKind, WalRecord, WalSyncMode, WalWriter};
use std::hint::black_box;

// ============================================================================
// Append Throughput
// ============================================================================

/// Benchmark WAL append throughput for individual record appends.
fn bench_wal_append_individual(c: &mut Criterion) {
    let sync_modes = [
        ("nosync", WalSyncMode::NoSync),
        ("batchedsync", WalSyncMode::BatchedSync),
    ];

    for (mode_name, sync_mode) in &sync_modes {
        let mut group = c.benchmark_group(format!("subsystem_wal_append_individual_{}", mode_name));

        // Only test smaller batch sizes for tier2
        for batch_size in &[10, 100] {
            group.throughput(Throughput::Elements(*batch_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(batch_size),
                batch_size,
                |b, &size| {
                    let tmp = tempdir().expect("tempdir");
                    let dir = tmp.path();

                    let mut writer = cntryl_midge::wal::fs::Wal::open_with_mode(dir, *sync_mode)
                        .expect("open WAL");

                    let records: Vec<WalRecord> = (0..size)
                        .map(|i| {
                            WalRecord::new(
                                WalOpKind::Put,
                                Bytes::copy_from_slice(format!("key{:08}", i).as_bytes()),
                                Some(Bytes::copy_from_slice(b"value_data_payload")),
                                i as u64,
                            )
                        })
                        .collect();

                    b.iter(|| {
                        let total_ops: usize = if *sync_mode == WalSyncMode::NoSync {
                            500
                        } else {
                            50
                        };
                        let rounds = total_ops.div_ceil(size);
                        for _ in 0..rounds {
                            for record in &records {
                                writer.append(record).expect("append");
                            }
                        }
                    })
                },
            );
        }

        group.finish();
    }
}

/// Benchmark WAL batch append throughput.
fn bench_wal_append_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_append_batch");

    for batch_size in &[10, 100, 500] {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                let tmp = tempdir().expect("tempdir");
                let dir = tmp.path();

                let writer =
                    cntryl_midge::wal::fs::Wal::open_with_mode(dir, WalSyncMode::NoSync)
                        .expect("open WAL");

                let records: Vec<WalRecord> = (0..size)
                    .map(|i| {
                        WalRecord::new(
                            WalOpKind::Put,
                            Bytes::copy_from_slice(format!("key{:08}", i).as_bytes()),
                            Some(Bytes::copy_from_slice(b"value_data_payload")),
                            i as u64,
                        )
                    })
                    .collect();

                b.iter(|| {
                    let rounds = 500usize.div_ceil(size);
                    for _ in 0..rounds {
                        writer.append_batch(&records).expect("append_batch");
                    }
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// I/O Sequential Throughput
// ============================================================================

/// Sequential append throughput using NoSync mode (buffered writes)
fn bench_wal_io_seq_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_io_seq_throughput");

    for (name, value_size) in &[("small", 64usize), ("medium", 512usize)] {
        group.throughput(Throughput::Bytes(*value_size as u64 * 100));
        group.bench_with_input(BenchmarkId::from_parameter(name), value_size, |b, &size| {
            let tmp = tempdir().expect("tempdir");
            let dir = tmp.path();

            let mut writer =
                cntryl_midge::wal::fs::Wal::open_with_mode(dir, WalSyncMode::NoSync)
                    .expect("open WAL");

            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key_template"),
                Some(Bytes::from(vec![0u8; size])),
                1,
            );

            b.iter(|| {
                for _ in 0..100_usize {
                    writer.append(&record).expect("append");
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Raw I/O Baseline
// ============================================================================

/// Benchmark raw I/O using pre-encoded WAL fragments
fn bench_wal_io_preencoded(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_io_preencoded");

    group.throughput(Throughput::Bytes(1024));

    group.bench_function("preencoded_append_nosync", |b| {
        // Pre-encode outside the benchmark
        let encoder = WalEncoder::with_defaults().expect("encoder");
        let rec = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from(vec![0u8; 1024])),
            1,
        );
        let frag = encoder.encode_one(&rec).expect("encode");

        b.iter(|| {
            let tmp = tempdir().expect("tempdir");
            let path = tmp.path().join("raw_wal_test.wal");
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("open file");

            write_vectored(&mut file, &[&frag.header, &frag.body]).expect("write_vectored");
            black_box(&file);
        })
    });

    group.finish();
}

// ============================================================================
// Platform Optimizations
// ============================================================================

/// Compare fallback writev vs io_uring-backed writev
fn bench_wal_io_platform(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_io_platform");

    let size = 1024usize;
    group.throughput(Throughput::Bytes(size as u64));

    // Pre-encode
    let encoder = WalEncoder::with_defaults().expect("encoder");
    let rec = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"k"),
        Some(Bytes::from(vec![0u8; size])),
        1,
    );
    let frag = encoder.encode_one(&rec).expect("encode");

    group.bench_function("fallback_writev", |b| {
        b.iter(|| {
            let tmp = tempdir().expect("tempdir");
            let path = tmp.path().join("raw_wal_cmp.wal");
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("open");

            fs_io::write_vectored_fallback(&mut file, &[&frag.header, &frag.body])
                .expect("fallback write");
            black_box(&file);
        })
    });

    group.bench_function("dispatch_writev", |b| {
        b.iter(|| {
            let tmp = tempdir().expect("tempdir");
            let path = tmp.path().join("raw_wal_cmp.wal");
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("open");

            write_vectored(&mut file, &[&frag.header, &frag.body]).expect("write");
            black_box(&file);
        })
    });

    group.finish();
}

criterion_group! {
    name = wal_io_group;
    config = criterion_config();
    targets =
        bench_wal_append_individual,
        bench_wal_append_batch,
        bench_wal_io_seq_throughput,
        bench_wal_io_preencoded,
        bench_wal_io_platform
}
criterion_main!(wal_io_group);
