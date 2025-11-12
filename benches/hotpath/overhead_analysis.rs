//! Overhead Analysis: WAL → MemTable → Engine
//!
//! **Target Runtime:** < 2 seconds
//! **Run Frequency:** Every PR (CI gate)
//!
//! This benchmark isolates overhead at each layer of the storage stack:
//! 1. **Layer 1:** Raw WAL batch append (baseline, no seq/memtable)
//! 2. **Layer 2:** WAL + sequence number allocation (atomic overhead)
//! 3. **Layer 3:** WAL + MemTable insert (skiplist insertion)
//! 4. **Layer 4:** Full WriteBatch processing (record construction)
//! 5. **Layer 5:** Multi-CF writes (column family routing overhead)
//! 6. **Layer 6:** Concurrent multi-CF (thread coordination overhead)
//!
//! **Goal:** Quantify the cost of each layer to identify optimization targets.
//! Expected overhead: seq allocation (1-2%), memtable (5-10%), CF routing (1-3%)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::core::memtable::MemTable;
use cntryl_midge::wal::{WalOpKind, WalRecord, WalSyncMode, WalWriter};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

// ============================================================================
// Layer 1: Raw WAL Batch Append (Baseline)
// ============================================================================

/// Baseline: measure raw WAL append without any higher-level overhead.
/// This is the theoretical minimum for write throughput.
fn bench_layer1_wal_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis_layer1");
    
    for &batch_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let tmp = tempdir().expect("tempdir");
                let writer =
                    cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");

                // Pre-create records
                let records: Vec<WalRecord> = (0..size)
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
            },
        );
    }

    group.finish();
}

// ============================================================================
// Layer 2: WAL + Sequence Number Allocation
// ============================================================================

/// Measure the cost of atomic sequence number allocation.
/// This shows the overhead of SeqCst ordering for transaction ordering guarantees.
fn bench_layer2_wal_with_seq(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis_layer2");

    for &batch_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let tmp = tempdir().expect("tempdir");
                let writer =
                    cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let seq = Arc::new(AtomicU64::new(0));

                // Pre-create record templates (without seq)
                let key_values: Vec<(Bytes, Bytes)> = (0..size)
                    .map(|i| {
                        (
                            Bytes::from(format!("key{:016}", i)),
                            Bytes::from(vec![42u8; 1000]),
                        )
                    })
                    .collect();

                b.iter(|| {
                    // Allocate sequence numbers
                    let mut records = Vec::with_capacity(size);
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
            },
        );
    }

    group.finish();
}

// ============================================================================
// Layer 3: WAL + MemTable Insert
// ============================================================================

/// Measure the cost of in-memory MemTable insertion after WAL.
/// This isolates skiplist insertion overhead.
fn bench_layer3_wal_plus_memtable(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis_layer3");

    for &batch_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let tmp = tempdir().expect("tempdir");
                let writer =
                    cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let memtable = MemTable::new();
                let seq = Arc::new(AtomicU64::new(0));

                // Pre-create record templates
                let key_values: Vec<(Bytes, Bytes)> = (0..size)
                    .map(|i| {
                        (
                            Bytes::from(format!("key{:016}", i)),
                            Bytes::from(vec![42u8; 1000]),
                        )
                    })
                    .collect();

                b.iter(|| {
                    // Allocate sequence numbers and create records
                    let mut records = Vec::with_capacity(size);
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
                        memtable.put_with_seq(
                            &record.key,
                            record.value.as_ref().unwrap(),
                            record.seq,
                        );
                    }

                    black_box(&writer);
                    black_box(&memtable);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Layer 4: Full WriteBatch Processing (matches engine path)
// ============================================================================

/// Measure full WriteBatch record construction and processing.
/// This includes vector allocation and explicit field setup.
fn bench_layer4_write_batch_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis_layer4");

    for &batch_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let tmp = tempdir().expect("tempdir");
                let writer =
                    cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let memtable = MemTable::new();
                let seq = Arc::new(AtomicU64::new(0));

                b.iter(|| {
                    // Manually construct records (simulating WriteBatch overhead)
                    let mut records = Vec::with_capacity(size);

                    for i in 0..size {
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
                        memtable.put_with_seq(
                            &record.key,
                            record.value.as_ref().unwrap(),
                            record.seq,
                        );
                    }

                    black_box(&writer);
                    black_box(&memtable);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Layer 5: Column Family Routing Overhead
// ============================================================================

/// Measure the cost of column family ID lookup and routing.
/// This isolates CF-specific overhead from the main write path.
fn bench_layer5_column_family_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis_layer5_cf_routing");

    for &num_cfs in &[1, 4, 16] {
        let batch_size = 100;
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("cf_count_{}", num_cfs)),
            &num_cfs,
            |b, &cf_count| {
                let tmp = tempdir().expect("tempdir");
                let writer =
                    cntryl_midge::wal::fs::Wal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let memtables: Vec<MemTable> = (0..cf_count).map(|_| MemTable::new()).collect();
                let seq = Arc::new(AtomicU64::new(0));

                // Pre-create records
                let records: Vec<WalRecord> = (0..batch_size)
                    .map(|i| {
                        WalRecord::new(
                            WalOpKind::Put,
                            Bytes::from(format!("key{:016}", i)),
                            Some(Bytes::from(vec![42u8; 1000])),
                            seq.fetch_add(1, Ordering::SeqCst) + 1,
                        )
                    })
                    .collect();

                b.iter(|| {
                    // Simulate CF routing: assign each record to a CF
                    let mut cf_batches: Vec<Vec<_>> = vec![vec![]; cf_count];
                    for (i, record) in records.iter().enumerate() {
                        let cf_idx = i % cf_count;
                        cf_batches[cf_idx].push(record.clone());
                    }

                    // Write to all CFs
                    for (cf_idx, batch) in cf_batches.iter().enumerate() {
                        if !batch.is_empty() {
                            writer
                                .append_batch(batch)
                                .expect("append_batch");
                            for record in batch {
                                memtables[cf_idx].put_with_seq(
                                    &record.key,
                                    record.value.as_ref().unwrap(),
                                    record.seq,
                                );
                            }
                        }
                    }

                    black_box(&writer);
                    black_box(&memtables);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Layer 6: Concurrent Multi-CF Writes
// ============================================================================

/// Measure multi-threaded MemTable insertion overhead.
/// This quantifies the cost of concurrent skiplist operations and synchronization.
fn bench_layer6_concurrent_multi_cf(c: &mut Criterion) {
    use std::thread;
    
    let mut group = c.benchmark_group("overhead_analysis_layer6_concurrent");
    
    for &num_threads in &[1, 2, 4] {
        for &num_cfs in &[1, 4, 8] {
            let batch_size = 100;
            group.throughput(Throughput::Elements((batch_size * num_threads) as u64));

            group.bench_with_input(
                BenchmarkId::new(format!("threads_{}", num_threads), format!("cf_{}", num_cfs)),
                &(num_threads, num_cfs),
                |b, &(threads, cf_count)| {
                    let memtables: Vec<Arc<MemTable>> = (0..cf_count)
                        .map(|_| Arc::new(MemTable::new()))
                        .collect();
                    let seq = Arc::new(AtomicU64::new(0));

                    b.iter(|| {
                        let handles: Vec<_> = (0..threads)
                            .map(|thread_id| {
                                let memtables = memtables.iter().map(Arc::clone).collect::<Vec<_>>();
                                let seq = Arc::clone(&seq);
                                thread::spawn(move || {
                                    // Each thread generates its own records and writes to multiple CFs
                                    for i in 0..batch_size {
                                        let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                                        let key = Bytes::from(format!(
                                            "key_t{}_i{:016}",
                                            thread_id, i
                                        ));
                                        let value = Bytes::from(vec![42u8; 1000]);

                                        // Route to a CF based on key hash
                                        let cf_idx = i % cf_count;
                                        memtables[cf_idx].put_with_seq(&key, &value, s);
                                    }
                                })
                            })
                            .collect();

                        for handle in handles {
                            handle.join().unwrap();
                        }
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = hotpath_overhead_analysis;
    config = criterion_config();
    targets =
        bench_layer1_wal_only,
        bench_layer2_wal_with_seq,
        bench_layer3_wal_plus_memtable,
        bench_layer4_write_batch_construction,
        bench_layer5_column_family_routing,
        bench_layer6_concurrent_multi_cf
}
criterion_main!(hotpath_overhead_analysis);
