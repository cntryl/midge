//! Tier 1 — Hot Path Storage Benchmarks (A++ Optimized)
//!
//! • Zero allocations inside measured loop
//! • No thread spawn inside measured loop
//! • SkipList/MemTable hot-path isolation
//! • Compression + decompression
//! • Flat sampling mode
//!
//! Runtime target: < 1 second
//! Run frequency: Every PR (CI gate)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::criterion_config;

use cntryl_midge::common::codec::{Compressor, Lz4Codec};
use cntryl_midge::core::memtable::MemTable;
use cntryl_midge::core::skiplist::SkipList;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::{hint::black_box, thread};

// =============================================================================
// Helpers: Precompute Data
// =============================================================================

fn make_fixed_kv(size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    // Keys: 16-byte fixed payload: 8 bytes prefix + 8 byte counter
    // Values: fixed-length 32-byte payload
    let mut keys = Vec::with_capacity(size);
    let mut vals = Vec::with_capacity(size);

    for i in 0..size {
        let mut key = [0u8; 16];
        key[8..16].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(Bytes::copy_from_slice(&key));

        vals.push(Bytes::copy_from_slice(&[0xAB; 32]));
    }

    (keys, vals)
}

fn shuffle_indices(len: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..len).collect();
    let mut seed = 0xDEADBEEFCAFEBABEu64;

    for i in (1..len).rev() {
        // Fast, deterministic xorshift64
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let j = (seed as usize) % (i + 1);
        idx.swap(i, j);
    }
    idx
}

// =============================================================================
// SkipList — Sequential
// =============================================================================

fn bench_skiplist_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_skiplist_sequential");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &[1_000, 5_000] {
        let (keys, vals) = make_fixed_kv(size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let sl = SkipList::new();
                for i in 0..size {
                    sl.upsert(keys[i].clone(), Some(vals[i].clone()), i as u64);
                }
                black_box(sl)
            });
        });
    }
    group.finish();
}

// =============================================================================
// SkipList — Random
// =============================================================================

fn bench_skiplist_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_skiplist_random");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &[1_000, 5_000] {
        let (keys, vals) = make_fixed_kv(size);
        let order = shuffle_indices(size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let sl = SkipList::new();
                for (i, idx) in order.iter().enumerate() {
                    sl.upsert(keys[*idx].clone(), Some(vals[i].clone()), i as u64);
                }
                black_box(sl);
            });
        });
    }
    group.finish();
}

// =============================================================================
// SkipList — Concurrent (no thread spawn per iter)
// =============================================================================

fn bench_skiplist_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_skiplist_concurrent");
    group.sampling_mode(SamplingMode::Flat);

    const THREADS: usize = 4;
    const OPS: usize = 500;

    // Precompute thread-specific K/V batches
    let mut kvs = Vec::new();
    for t in 0..THREADS {
        let (keys, vals) = make_fixed_kv(OPS);
        kvs.push((t, keys, vals));
    }

    group.bench_function("4_threads_500_ops", |b| {
        // Reusable barrier + threads
        let barrier = Arc::new(Barrier::new(THREADS + 1));
        let sl = Arc::new(SkipList::new());
        let exit_signal = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for (_tid, keys, vals) in kvs.clone() {
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

    group.finish();
}

// =============================================================================
// MemTable — Sequential / Random / Concurrent
// =============================================================================

fn bench_memtable_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_sequential");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &[1_000, 5_000] {
        let (keys, vals) = make_fixed_kv(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let mt = MemTable::new();
                for i in 0..size {
                    mt.put_owned_with_seq(keys[i].clone(), vals[i].clone(), i as u64);
                }
                black_box(mt);
            });
        });
    }
    group.finish();
}

fn bench_memtable_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_random");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &[1_000, 5_000] {
        let (keys, vals) = make_fixed_kv(size);
        let order = shuffle_indices(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let mt = MemTable::new();
                for (i, idx) in order.iter().enumerate() {
                    mt.put_owned_with_seq(keys[*idx].clone(), vals[i].clone(), i as u64);
                }
                black_box(mt);
            });
        });
    }
    group.finish();
}

// =============================================================================
// MemTable — Reads
// =============================================================================

fn bench_memtable_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_read");
    group.sampling_mode(SamplingMode::Flat);

    const SIZE: usize = 5_000;
    let (keys, vals) = make_fixed_kv(SIZE);
    let mt = MemTable::new();
    for i in 0..SIZE {
        mt.put_owned_with_seq(keys[i].clone(), vals[i].clone(), i as u64);
    }

    group.bench_function("5000_reads", |b| {
        b.iter(|| {
            let mut count = 0;
            for key in keys.iter().take(SIZE) {
                if mt.get(key).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    group.finish();
}

// =============================================================================
// Compression — LZ4 (compress + decompress)
// =============================================================================

fn bench_compression_lz4(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_compression_lz4");
    group.sampling_mode(SamplingMode::Flat);

    let codec = Lz4Codec::new();

    for &size in &[4_096, 16_384] {
        let data = vec![0xAB; size];
        let compressed = codec.compress(&data).unwrap();

        group.bench_with_input(BenchmarkId::new("compress", size), &size, |b, _| {
            b.iter(|| black_box(codec.compress(&data).unwrap()));
        });

        group.bench_with_input(BenchmarkId::new("decompress", size), &size, |b, _| {
            b.iter(|| black_box(codec.decompress(&compressed).unwrap()));
        });
    }

    group.finish();
}

// =============================================================================

criterion_group! {
    name = tier1_hotpath_storage;
    config = criterion_config();
    targets =
        bench_skiplist_sequential,
        bench_skiplist_random,
        bench_skiplist_concurrent,
        bench_memtable_sequential,
        bench_memtable_random,
        bench_memtable_read,
        bench_compression_lz4
}
criterion_main!(tier1_hotpath_storage);
