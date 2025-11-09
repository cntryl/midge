//! Overhead Analysis: WAL → MemTable → Engine
//!
//! This benchmark measures each layer to identify where the 15x overhead comes from:
//! - Raw WAL batch: 7.53M ops/sec
//! - Engine clean: 505K ops/sec
//! - Gap: 15x slower
//!
//! We'll test:
//! 1. WAL batch append (baseline)
//! 2. WAL + sequence number allocation
//! 3. WAL + MemTable insert
//! 4. Full engine write path

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::core::memtable::MemTable;
use cntryl_midge::wal::{WalOpKind, WalRecord, WalSyncMode, WalWriter};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

const BATCH_SIZE: usize = 100;

// ============================================================================
// Layer 1: Raw WAL Batch Append (Baseline)
// ============================================================================

fn bench_layer1_wal_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    group.bench_function("layer1_wal_batch_append", |b| {
        let tmp = tempdir().expect("tempdir");
        let writer =
            cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");

        // Pre-create records
        let records: Vec<WalRecord> = (0..BATCH_SIZE)
            .map(|i| {
                WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from(format!("key{:016}", i)),
                    Some(Bytes::from(vec![42u8; 1000])),
                    i as u64,
                )
            })
            .collect();

        b.iter(|| {
            writer.append_batch(&records).expect("append_batch");
            black_box(&writer);
        });
    });

    group.finish();
}

// ============================================================================
// Layer 2: WAL + Sequence Number Allocation
// ============================================================================

fn bench_layer2_wal_with_seq(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    group.bench_function("layer2_wal_plus_sequence", |b| {
        let tmp = tempdir().expect("tempdir");
        let writer =
            cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");
        let seq = Arc::new(AtomicU64::new(0));

        // Pre-create record templates (without seq)
        let key_values: Vec<(Bytes, Bytes)> = (0..BATCH_SIZE)
            .map(|i| {
                (
                    Bytes::from(format!("key{:016}", i)),
                    Bytes::from(vec![42u8; 1000]),
                )
            })
            .collect();

        b.iter(|| {
            // Allocate sequence numbers
            let mut records = Vec::with_capacity(BATCH_SIZE);
            for (key, value) in &key_values {
                let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                records.push(WalRecord::new(
                    WalOpKind::Put,
                    key.clone(),
                    Some(value.clone()),
                    s,
                ));
            }

            // Write to WAL
            writer.append_batch(&records).expect("append_batch");
            black_box(&writer);
        });
    });

    group.finish();
}

// ============================================================================
// Layer 3: WAL + MemTable Insert
// ============================================================================

fn bench_layer3_wal_plus_memtable(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    group.bench_function("layer3_wal_plus_memtable", |b| {
        let tmp = tempdir().expect("tempdir");
        let writer =
            cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");
        let memtable = MemTable::new();
        let seq = Arc::new(AtomicU64::new(0));

        // Pre-create record templates
        let key_values: Vec<(Bytes, Bytes)> = (0..BATCH_SIZE)
            .map(|i| {
                (
                    Bytes::from(format!("key{:016}", i)),
                    Bytes::from(vec![42u8; 1000]),
                )
            })
            .collect();

        b.iter(|| {
            // Allocate sequence numbers and create records
            let mut records = Vec::with_capacity(BATCH_SIZE);
            for (key, value) in &key_values {
                let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                records.push(WalRecord::new(
                    WalOpKind::Put,
                    key.clone(),
                    Some(value.clone()),
                    s,
                ));
            }

            // Write to WAL
            writer.append_batch(&records).expect("append_batch");

            // Insert into MemTable
            for record in &records {
                memtable.put_with_seq(&record.key, record.value.as_ref().unwrap(), record.seq);
            }

            black_box(&writer);
            black_box(&memtable);
        });
    });

    group.finish();
}

// ============================================================================
// Layer 4: Full WriteBatch Processing (matches engine path)
// ============================================================================

fn bench_layer4_write_batch_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    group.bench_function("layer4_writebatch_construction", |b| {
        let tmp = tempdir().expect("tempdir");
        let writer =
            cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");
        let memtable = MemTable::new();
        let seq = Arc::new(AtomicU64::new(0));

        b.iter(|| {
            // Manually construct records (simulating WriteBatch overhead)
            let mut records = Vec::with_capacity(BATCH_SIZE);

            for i in 0..BATCH_SIZE {
                let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                let key = Bytes::from(format!("key{:016}", i));
                let value = Bytes::from(vec![42u8; 1000]);

                // Simulate the overhead of record construction
                records.push(WalRecord {
                    cf_id: 0,
                    op: WalOpKind::Put,
                    key: key.clone(),
                    value: Some(value.clone()),
                    seq: s,
                    expiration: None,
                    range_end: None,
                    txn_id: None,
                    compression: None,
                });
            }

            // Write to WAL
            writer.append_batch(&records).expect("append_batch");

            // Insert into MemTable
            for record in &records {
                memtable.put_with_seq(&record.key, record.value.as_ref().unwrap(), record.seq);
            }

            black_box(&writer);
            black_box(&memtable);
        });
    });

    group.finish();
}

criterion_group! {
    name = hotpath_overhead_analysis;
    config = criterion_config();
    targets =
        bench_layer1_wal_only,
        bench_layer2_wal_with_seq,
        bench_layer3_wal_plus_memtable,
        bench_layer4_write_batch_construction
}
criterion_main!(hotpath_overhead_analysis);
