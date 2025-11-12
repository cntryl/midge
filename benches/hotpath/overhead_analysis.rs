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
use cntryl_midge::wal::fs::Wal as FsWal;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;
use std::time::Instant;
// Instant was added for aggregation but is not required now.

// Helper: format fixed-width key "key{:016}" into Bytes without using `format!`.
fn key_fixed(i: u64) -> Bytes {
    // "key" + 16 digits = 19 bytes
    let mut buf = [b'0'; 19];
    buf[0] = b'k';
    buf[1] = b'e';
    buf[2] = b'y';
    let mut n = i;
    for pos in (3..19).rev() {
        buf[pos] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    Bytes::copy_from_slice(&buf)
}

// Helper: create threaded key like "key_t{thread}_i{016}". Uses a small Vec to assemble bytes.
fn key_threaded(thread_id: u32, i: u64) -> Bytes {
    // Produce a threaded key without format! and return Bytes.
    let mut v: Vec<u8> = Vec::with_capacity(32);
    v.extend_from_slice(b"key_t");
    // append thread_id in decimal
    let tid = thread_id.to_string();
    v.extend_from_slice(tid.as_bytes());
    v.extend_from_slice(b"_i");
    // append zero-padded 16-digit i
    let mut num_buf = [b'0'; 16];
    let mut n = i;
    for pos in (0..16).rev() {
        num_buf[pos] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    v.extend_from_slice(&num_buf);
    Bytes::from(v)
}
// key_threaded is now used in the concurrent multi-CF bench to avoid format!.

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
                // Create tempdir once, outside the timing loop
                let tmp = tempdir().expect("tempdir");
                let writer =
                    FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");

                // Pre-create record templates (matches Layer 2 for fair comparison)
                let key_values: Vec<(Bytes, Bytes)> = (0..size)
                    .map(|i| {
                        (
                            key_fixed(i as u64),
                            Bytes::from(vec![42u8; 1000]),
                        )
                    })
                    .collect();

                b.iter(|| {
                    // Create records from pre-allocated templates (minimal formatting overhead)
                    let records: Vec<WalRecord> = key_values
                        .iter()
                        .enumerate()
                        .map(|(i, (k, v))| {
                            WalRecord::new(
                                WalOpKind::Put,
                                k.clone(),
                                Some(v.clone()),
                                i as u64,
                            )
                        })
                        .collect();

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
                // Create WAL outside the timing loop
                let tmp = tempdir().expect("tempdir");
                let writer =
                    FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let seq = Arc::new(AtomicU64::new(0));

                // Pre-create record templates (without seq)
                let key_values: Vec<(Bytes, Bytes)> = (0..size)
                    .map(|i| {
                        (
                            key_fixed(i as u64),
                            Bytes::from(vec![42u8; 1000]),
                        )
                    })
                    .collect();

                b.iter(|| {
                    // Reset sequence counter for each iteration
                    seq.store(0, Ordering::Relaxed);
                    
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
                    FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let memtable = MemTable::new();
                let seq = Arc::new(AtomicU64::new(0));

                // Pre-create record templates
                let key_values: Vec<(Bytes, Bytes)> = (0..size)
                    .map(|i| {
                        (
                            key_fixed(i as u64),
                            Bytes::from(vec![42u8; 1000]),
                        )
                    })
                    .collect();

                b.iter(|| {
                    // Reset sequence counter for clean iterations
                    seq.store(0, Ordering::Relaxed);
                    
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
                            memtable.put_owned_with_seq(record.key.clone(), record.value.as_ref().unwrap().clone(), record.seq);
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
                    FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let memtable = MemTable::new();
                let seq = Arc::new(AtomicU64::new(0));

                // Pre-create key/value templates (matches Layer 2 for fair comparison)
                let key_values: Vec<(Bytes, Bytes)> = (0..size)
                    .map(|i| {
                        (
                            key_fixed(i as u64),
                            Bytes::from(vec![42u8; 1000]),
                        )
                    })
                    .collect();

                b.iter(|| {
                    // Reset state for clean iterations
                    seq.store(0, Ordering::Relaxed);
                    
                    // Construct records with pre-allocated templates (simulating WriteBatch::with_capacity)
                    let mut records = Vec::with_capacity(size);

                    for (key, value) in key_values.iter() {
                        let s = seq.fetch_add(1, Ordering::SeqCst) + 1;

                        // Simulate the overhead of record construction (field assignment only)
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
                    FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync)
                        .expect("open WAL");
                let memtables: Vec<MemTable> = (0..cf_count).map(|_| MemTable::new()).collect();
                let seq = Arc::new(AtomicU64::new(0));

                b.iter(|| {
                    // Reset state for clean iterations
                    seq.store(0, Ordering::Relaxed);
                    
                    // Create records inside timing loop for fair measurement
                    let records: Vec<WalRecord> = (0..batch_size)
                        .map(|i| {
                            WalRecord::new(
                                WalOpKind::Put,
                                key_fixed(i as u64),
                                Some(Bytes::from(vec![42u8; 1000])),
                                seq.fetch_add(1, Ordering::SeqCst) + 1,
                            )
                        })
                        .collect();

                    // Simulate CF routing: assign each record to a CF
                    let mut cf_batches: Vec<Vec<_>> = vec![vec![]; cf_count];
                    for record in records.iter() {
                        // Hash the key bytes for a realistic CF distribution
                        let mut hasher = DefaultHasher::new();
                        record.key.as_ref().hash(&mut hasher);
                        let cf_idx = (hasher.finish() as usize) % cf_count;
                        cf_batches[cf_idx].push(record.clone());
                    }

                    // Write to all CFs
                    for (cf_idx, batch) in cf_batches.iter().enumerate() {
                        if !batch.is_empty() {
                            writer
                                .append_batch(batch)
                                .expect("append_batch");
                            for record in batch {
                                memtables[cf_idx].put_owned_with_seq(record.key.clone(), record.value.as_ref().unwrap().clone(), record.seq);
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
                        // Reset state for clean iterations
                        seq.store(0, Ordering::Relaxed);
                        
                        let handles: Vec<_> = (0..threads)
                            .map(|thread_id| {
                                let memtables = memtables.iter().map(Arc::clone).collect::<Vec<_>>();
                                let seq = Arc::clone(&seq);
                                thread::spawn(move || {
                                    // Each thread generates its own records and writes to multiple CFs
                                    for i in 0..batch_size {
                                        let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                                        // ensure types match helper signature
                                        let key = key_threaded(thread_id as u32, i as u64);
                                        let value = Bytes::from(vec![42u8; 1000]);

                                        // Route to a CF based on simple modulo (cast to usize for indexing)
                                        let cf_idx = (i as usize) % (cf_count as usize);
                            memtables[cf_idx].put_owned_with_seq(key.clone(), value.clone(), s);
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

// ============================================================================
// Layer 7: WAL Durability Mode Comparison
// ============================================================================

/// Measure the cost of different WAL synchronization modes.
/// This quantifies the throughput trade-off for durability guarantees:
/// - NoSync: Maximum throughput, async flush (data loss on crash)
/// - BatchedSync: Batched fsync, balanced throughput/safety (default)
/// - EveryWrite: Per-write fsync, minimum throughput (maximum safety)
fn bench_layer7_wal_durability_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("overhead_analysis_layer7_durability");
    
    let durability_modes = vec![
        ("NoSync", WalSyncMode::NoSync),
        ("BatchedSync", WalSyncMode::BatchedSync),
        ("EveryWrite", WalSyncMode::EveryWrite),
    ];
    
    for (mode_name, sync_mode) in durability_modes {
        let batch_size = 100;
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(mode_name),
            &sync_mode,
            |b, &sync_mode| {
                let tmp = tempdir().expect("tempdir");
                let writer =
                    FsWal::open_with_mode(tmp.path(), sync_mode)
                        .expect("open WAL");
                let seq = Arc::new(AtomicU64::new(0));

                // Pre-create record templates using key_fixed to avoid format! allocations
                let key_values: Vec<(Bytes, Bytes)> = (0..batch_size)
                    .map(|i| (key_fixed(i as u64), Bytes::from(vec![42u8; 1000])))
                    .collect();

                b.iter(|| {
                    // Reset sequence counter for clean iterations
                    seq.store(0, Ordering::Relaxed);
                    
                    // Create records with sequence numbers
                    let mut records = Vec::with_capacity(batch_size);
                    for (key, value) in &key_values {
                        let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                        records.push(WalRecord::new(
                            WalOpKind::Put,
                            key.clone(),
                            Some(value.clone()),
                            s,
                        ));
                    }

                    // Write to WAL with specified durability mode
                    writer.append_batch(&records).expect("append_batch");
                    black_box(&writer);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Manual Summary Aggregation (developer helper, not run by Criterion)
// ---------------------------------------------------------------------------
// This helper runs lightweight, manual timings for each layer and prints a
// simple table of mean elapsed ms and percent delta relative to Layer 1.
// It's intentionally standalone (not wired into Criterion) so you can invoke
// it from a small dev harness or REPL while iterating.
#[allow(dead_code)]
fn overhead_aggregate_summary_once() {
    let batch_size = 100usize;
    let reps = 5usize;

    // Helper to run a closure `reps` times and return mean ms.
    fn mean_ms<F: Fn()>(reps: usize, f: F) -> f64 {
        let mut totals: Vec<f64> = Vec::with_capacity(reps);
        for _ in 0..reps {
            let start = Instant::now();
            f();
            let elapsed = start.elapsed();
            totals.push(elapsed.as_secs_f64() * 1000.0);
        }
        let sum: f64 = totals.iter().sum();
        sum / (totals.len() as f64)
    }

    // Layer implementations mirror the Criterion benches but are simplified
    // and run in-process to produce a quick, comparable number.
    let layer1 = mean_ms(reps, || {
        let tmp = tempdir().expect("tempdir");
        let writer =
            FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");

        let key_values: Vec<(Bytes, Bytes)> = (0..batch_size)
            .map(|i| (key_fixed(i as u64), Bytes::from(vec![42u8; 1000])))
            .collect();

        // Build records and append
        let records: Vec<WalRecord> = key_values
            .iter()
            .enumerate()
            .map(|(i, (k, v))| WalRecord::new(WalOpKind::Put, k.clone(), Some(v.clone()), i as u64))
            .collect();
        writer.append_batch(&records).ok();
        black_box(&writer);
    });

    let layer2 = mean_ms(reps, || {
        let tmp = tempdir().expect("tempdir");
        let writer =
            FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");
        let seq = Arc::new(AtomicU64::new(0));

        let key_values: Vec<(Bytes, Bytes)> = (0..batch_size)
            .map(|i| (key_fixed(i as u64), Bytes::from(vec![42u8; 1000])))
            .collect();

        let mut records = Vec::with_capacity(batch_size);
        for (key, value) in &key_values {
            let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
            records.push(WalRecord::new(WalOpKind::Put, key.clone(), Some(value.clone()), s));
        }
        writer.append_batch(&records).ok();
        black_box(&writer);
    });

    let layer3 = mean_ms(reps, || {
        let tmp = tempdir().expect("tempdir");
        let writer =
            FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");
        let memtable = MemTable::new();
        let seq = Arc::new(AtomicU64::new(0));

        let key_values: Vec<(Bytes, Bytes)> = (0..batch_size)
            .map(|i| (key_fixed(i as u64), Bytes::from(vec![42u8; 1000])))
            .collect();

        let mut records = Vec::with_capacity(batch_size);
        for (key, value) in &key_values {
            let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
            records.push(WalRecord::new(WalOpKind::Put, key.clone(), Some(value.clone()), s));
        }

        writer.append_batch(&records).ok();
        for record in &records {
            memtable.put_with_seq(&record.key, record.value.as_ref().unwrap(), record.seq);
        }
        black_box(&memtable);
    });

    let layer4 = mean_ms(reps, || {
        let tmp = tempdir().expect("tempdir");
        let writer =
            FsWal::open_with_mode(tmp.path(), WalSyncMode::NoSync).expect("open WAL");
        let memtable = MemTable::new();
        let seq = Arc::new(AtomicU64::new(0));

        let key_values: Vec<(Bytes, Bytes)> = (0..batch_size)
            .map(|i| (key_fixed(i as u64), Bytes::from(vec![42u8; 1000])))
            .collect();

        let mut records = Vec::with_capacity(batch_size);
        for (key, value) in key_values.iter() {
            let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
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

        writer.append_batch(&records).ok();
        for record in &records {
            memtable.put_with_seq(&record.key, record.value.as_ref().unwrap(), record.seq);
        }
        black_box(&memtable);
    });

    // Simple printout
    println!("Overhead summary (batch_size = {}, reps = {})", batch_size, reps);
    println!("Layer | mean (ms) | % delta vs L1");
    println!("1     | {:8.3}  | 0.00%", layer1);
    println!("2     | {:8.3}  | {:+.2}%", layer2, (layer2 - layer1) / layer1 * 100.0);
    println!("3     | {:8.3}  | {:+.2}%", layer3, (layer3 - layer1) / layer1 * 100.0);
    println!("4     | {:8.3}  | {:+.2}%", layer4, (layer4 - layer1) / layer1 * 100.0);

    // Note: Layers 5/6 are more environment-dependent; include if needed later.
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
        bench_layer6_concurrent_multi_cf,
        bench_layer7_wal_durability_modes
}
criterion_main!(hotpath_overhead_analysis);
