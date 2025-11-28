//! Tier 2 — Core Primitives Benchmarks
//!
//! Covers foundational data structures and coordination primitives:
//!
//! - WAL write (buffered via in-memory WAL)
//! - WriteBatch creation
//! - MergeIterator over N sources
//! - Concurrent SkipList access
//!
//! Runtime target: < 2–3 seconds
//! Run frequency: nightly + on performance-critical PRs

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion_helper::criterion_config;

use cntryl_midge::{
    api::{column_family::ColumnFamilyId, write_batch::WriteBatch},
    core::data_structures::merge_iterator::{IteratorSource, MergingIterator, VecSource},
    core::skiplist::SkipList,
    wal::mem::WalMem,
    wal::{traits::WalWriter, WalOpKind, WalRecord},
};

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

// ============================================================================
// Helpers
// ============================================================================

fn fixed_kv(n: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);

    for i in 0..n {
        let mut k = [0u8; 16];
        k[8..16].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(Bytes::copy_from_slice(&k));

        vals.push(Bytes::copy_from_slice(&[0xCD; 32]));
    }
    (keys, vals)
}

// ============================================================================
// WAL Write Benchmarks
// ============================================================================

fn bench_wal_write(c: &mut Criterion) {
    let mut g = c.benchmark_group("subsystem_wal_write");
    g.sampling_mode(SamplingMode::Flat);

    const OPS: usize = 5000;

    let (keys, vals) = fixed_kv(OPS);
    let records: Vec<WalRecord> = (0..OPS)
        .map(|i| WalRecord {
            cf_id: 1,
            op: WalOpKind::Put,
            key: keys[i].clone(),
            value: Some(vals[i].clone()),
            seq: i as u64,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        })
        .collect();

    g.bench_function("buffered_write", |b| {
        b.iter(|| {
            let wal = WalMem::new();

            for rec in &records {
                wal.append_record(rec).unwrap();
            }

            black_box(&wal);
        });
    });

    g.finish();
}

// ============================================================================
// WriteBatch Creation Benchmarks
// ============================================================================

fn bench_writebatch_apply(c: &mut Criterion) {
    let mut g = c.benchmark_group("subsystem_writebatch");
    g.sampling_mode(SamplingMode::Flat);

    const N: usize = 5000;
    let (keys, vals) = fixed_kv(N);

    g.bench_function("create_5k", |b| {
        b.iter(|| {
            let mut batch = WriteBatch::new();
            for i in 0..N {
                batch.put(ColumnFamilyId::new(1), keys[i].clone(), vals[i].clone());
            }
            black_box(batch);
        });
    });

    g.finish();
}

// ============================================================================
// MergeIterator Benchmarks
// ============================================================================

fn bench_merge_iterator(c: &mut Criterion) {
    let mut g = c.benchmark_group("subsystem_merge_iter");
    g.sampling_mode(SamplingMode::Flat);

    const N: usize = 2000;
    let (_, vals) = fixed_kv(N);

    // Create 3 sources with non-overlapping keys
    // (sources created inside benchmark to avoid clone issues)

    g.bench_function("merge_iter_3sources", |b| {
        b.iter(|| {
            // Create sources inside the benchmark to avoid clone issues
            let mut sources = Vec::new();
            for shard in 0..3 {
                let mut data = Vec::new();
                for (i, val) in vals.iter().enumerate().take(N) {
                    let k_i = (i + shard * 10_000) as u64;
                    let mut k = [0u8; 16];
                    k[8..16].copy_from_slice(&k_i.to_be_bytes());
                    data.push((Bytes::copy_from_slice(&k), Some(val.clone()), k_i));
                }
                sources.push(Box::new(VecSource::new(data)) as Box<dyn IteratorSource>);
            }

            let mut it = MergingIterator::new(sources, None);
            let mut count = 0;
            while it.next().is_some() {
                count += 1;
            }
            black_box(count);
        });
    });

    g.finish();
}

// ============================================================================
// SkipList — Concurrent (moved from tier1)
// ============================================================================

fn bench_skiplist_concurrent(c: &mut Criterion) {
    let mut g = c.benchmark_group("subsystem_skiplist_concurrent");
    g.sampling_mode(SamplingMode::Flat);

    const THREADS: usize = 4;
    const OPS: usize = 500;

    // Precompute thread-specific K/V batches
    let mut kvs = Vec::new();
    for _t in 0..THREADS {
        let (keys, vals) = fixed_kv(OPS);
        kvs.push((keys, vals));
    }

    g.bench_function("4_threads_500_ops", |b| {
        // Reusable barrier + threads
        let barrier = Arc::new(Barrier::new(THREADS + 1));
        let sl = Arc::new(SkipList::new());
        let exit_signal = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for (keys, vals) in kvs.clone() {
            let sl_clone = sl.clone();
            let barrier_clone = barrier.clone();
            let exit_clone = exit_signal.clone();

            handles.push(thread::spawn(move || loop {
                // wait for instruction
                barrier_clone.wait();

                // Exit signal check
                if exit_clone.load(Ordering::Acquire) {
                    return;
                }

                // do 500 ops
                for i in 0..OPS {
                    sl_clone.upsert(keys[i].clone(), Some(vals[i].clone()), i as u64);
                }

                barrier_clone.wait();
            }));
        }

        b.iter(|| {
            // Signal threads to run
            barrier.wait();
            // Wait for them to finish
            barrier.wait();
            black_box(&sl)
        });

        // clean shutdown
        exit_signal.store(true, Ordering::Release);
        barrier.wait();
        for h in handles {
            let _ = h.join();
        }
    });

    g.finish();
}

// ============================================================================
// Criterion Entry Point
// ============================================================================

criterion_group! {
    name = tier2_subsystem_core_primitives;
    config = criterion_config();
    targets =
        bench_wal_write,
        bench_writebatch_apply,
        bench_merge_iterator,
        bench_skiplist_concurrent
}

criterion_main!(tier2_subsystem_core_primitives);
