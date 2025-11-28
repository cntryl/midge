//! Tier 2 — WAL replay bench
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers WAL replay operations (applying WAL records to memtable)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::core::memtable::MemTable;
use cntryl_midge::wal::{WalOpKind, WalRecord};

/// Pre-generate WAL records (Bytes are ref-counted, so cloning is cheap)
fn make_wal_records(count: usize) -> Vec<WalRecord> {
    (0..count)
        .map(|i| WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from(format!("key_{:010}", i)),
            value: Some(Bytes::from(format!("value_{:010}", i))),
            seq: i as u64,
            cf_id: 0,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        })
        .collect()
}

/// Benchmark WAL replay small file (1k records)
fn bench_wal_replay_small_file(c: &mut Criterion) {
    let records = make_wal_records(1_000);

    let mut group = c.benchmark_group("subsystem_wal_replay_small_file");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("replay_small", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            // Clone is cheap since Bytes uses Arc internally
            memtable.load_from_wal(records.clone()).unwrap();
            black_box(memtable)
        })
    });

    group.finish();
}

/// Benchmark WAL replay large file (100k records)
fn bench_wal_replay_large_file(c: &mut Criterion) {
    let records = make_wal_records(100_000);

    let mut group = c.benchmark_group("subsystem_wal_replay_large_file");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100_000));
    group.sample_size(10); // Fewer samples for long benchmark

    group.bench_function("replay_large", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            memtable.load_from_wal(records.clone()).unwrap();
            black_box(memtable)
        })
    });

    group.finish();
}

/// Benchmark WAL replay with early termination (simulates corruption detection)
fn bench_wal_replay_early_terminate(c: &mut Criterion) {
    let records = make_wal_records(1_000);

    let mut group = c.benchmark_group("subsystem_wal_replay_early_terminate");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(99)); // Stop at record 99

    group.bench_function("replay_partial", |b| {
        b.iter(|| {
            let memtable = MemTable::new();

            // Simulate replay with early termination at record 99
            let mut count = 0u32;
            for record in &records {
                if record.seq == 99 {
                    break; // Stop at "corrupt" record
                }
                memtable.put(&record.key, record.value.as_ref().unwrap());
                count += 1;
            }

            black_box((memtable, count))
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_wal_replay;
    config = criterion_config();
    targets = bench_wal_replay_small_file, bench_wal_replay_large_file, bench_wal_replay_early_terminate
}
criterion_main!(tier2_subsystem_wal_replay);
