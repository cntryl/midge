//! Tier 2 — WAL replay bench
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers WAL replay operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::core::memtable::MemTable;
use cntryl_midge::wal::{WalOpKind, WalRecord};

/// Benchmark WAL replay small file
fn bench_wal_replay_small_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_replay_small_file");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1_000));

    // Pre-create 1k WAL records
    let records: Vec<WalRecord> = (0..1_000)
        .map(|i| WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from(format!("key_{:03}", i)),
            value: Some(Bytes::from(format!("value_{:03}", i))),
            seq: i as u64,
            cf_id: 0,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        })
        .collect();

    group.bench_function("replay_small", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            memtable.load_from_wal(records.clone()).unwrap();
            black_box(memtable);
        })
    });

    group.finish();
}

/// Benchmark WAL replay large file
fn bench_wal_replay_large_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_replay_large_file");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100_000));

    // Pre-create 100k WAL records
    let records: Vec<WalRecord> = (0..100_000)
        .map(|i| WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from(format!("key_{:05}", i)),
            value: Some(Bytes::from(format!("value_{:05}", i))),
            seq: i as u64,
            cf_id: 0,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        })
        .collect();

    group.bench_function("replay_large", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            memtable.load_from_wal(records.clone()).unwrap();
            black_box(memtable);
        })
    });

    group.finish();
}

/// Benchmark WAL replay corrupted tail
fn bench_wal_replay_corrupted_tail(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_replay_corrupted_tail");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1_000));

    // Pre-create 1k valid WAL records
    let records: Vec<WalRecord> = (0..1_000)
        .map(|i| WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from(format!("key_{:03}", i)),
            value: Some(Bytes::from(format!("value_{:03}", i))),
            seq: i as u64,
            cf_id: 0,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        })
        .collect();

    group.bench_function("replay_corrupted", |b| {
        b.iter(|| {
            let memtable = MemTable::new();

            // Simulate replay with corruption detection
            // In practice, this would involve trying to decode records and handling errors
            let mut valid_count = 0;
            for record in &records {
                // Simulate corruption check (e.g., checksum validation)
                let is_corrupted = record.seq % 100 == 99; // Simulate 1% corruption rate

                if !is_corrupted {
                    // Only replay valid records
                    memtable.put(&record.key, record.value.as_ref().unwrap());
                    valid_count += 1;
                } else {
                    // Handle corruption (in real implementation, this would log/truncate)
                    break; // Stop at first corruption
                }
            }

            black_box((memtable, valid_count));
        })
    });

    group.finish();
}

criterion_group! {
    name = wal_replay_group;
    config = criterion_config();
    targets = bench_wal_replay_small_file, bench_wal_replay_large_file, bench_wal_replay_corrupted_tail
}
criterion_main!(wal_replay_group);
