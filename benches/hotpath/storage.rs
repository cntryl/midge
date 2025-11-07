//! Tier 1 — Hot Path Storage Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers core storage primitives:
//! - SkipList (sequential, random, concurrent inserts)
//! - Compression codecs (Snappy, LZ4)
//! - Merge iterator (multi-SST reads)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;

use midge::common::codec::{Compressor, Lz4Codec};
use midge::core::memtable::MemTable;
use midge::core::skiplist::SkipList;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

// ============================================================================
// SkipList Benchmarks
// ============================================================================

fn bench_skiplist_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_skiplist_sequential");

    for size in [1_000, 5_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let sl = SkipList::new();
                for i in 0..size {
                    let key = format!("key_{:08}", i);
                    let val = format!("value_{:08}", i);
                    sl.upsert(Bytes::from(key), Some(Bytes::from(val)), i as u64);
                }
                black_box(sl);
            });
        });
    }

    group.finish();
}

fn bench_skiplist_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_skiplist_random");

    for size in [1_000, 5_000] {
        group.throughput(Throughput::Elements(size as u64));

        // Generate shuffled keys
        let mut keys: Vec<String> = (0..size).map(|i| format!("key_{:08}", i)).collect();
        let mut seed = 12345u64;
        for i in (1..keys.len()).rev() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let j = (seed as usize) % (i + 1);
            keys.swap(i, j);
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let sl = SkipList::new();
                for (i, key) in keys.iter().enumerate() {
                    let val = format!("value_{:08}", i);
                    sl.upsert(Bytes::from(key.clone()), Some(Bytes::from(val)), i as u64);
                }
                black_box(sl);
            });
        });
    }

    group.finish();
}

fn bench_skiplist_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_skiplist_concurrent");

    let num_threads = 4;
    let ops_per_thread = 500;
    group.throughput(Throughput::Elements((num_threads * ops_per_thread) as u64));

    group.bench_function("4_threads_500_ops", |b| {
        b.iter(|| {
            let sl: Arc<SkipList> = Arc::new(SkipList::new());
            let mut handles = vec![];

            for t in 0..num_threads {
                let sl_clone = Arc::clone(&sl);
                let handle = thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{}_{:08}", t, i);
                        let val = format!("val_{}_{:08}", t, i);
                        sl_clone.upsert(Bytes::from(key), Some(Bytes::from(val)), i as u64);
                    }
                });
                handles.push(handle);
            }

            for h in handles {
                h.join().unwrap();
            }

            black_box(sl);
        });
    });

    group.finish();
}

// ============================================================================
// MemTable Benchmarks (SkipList + wrapper overhead)
// ============================================================================

fn bench_memtable_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_sequential");

    for size in [1_000, 5_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mt = MemTable::new();
                for i in 0..size {
                    let key = format!("key_{:08}", i);
                    let val = format!("value_{:08}", i);
                    mt.put_with_seq(key.as_bytes(), val.as_bytes(), i as u64);
                }
                black_box(mt);
            });
        });
    }

    group.finish();
}

fn bench_memtable_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_random");

    for size in [1_000, 5_000] {
        group.throughput(Throughput::Elements(size as u64));

        // Generate shuffled keys
        let mut keys: Vec<String> = (0..size).map(|i| format!("key_{:08}", i)).collect();
        let mut seed = 12345u64;
        for i in (1..keys.len()).rev() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let j = (seed as usize) % (i + 1);
            keys.swap(i, j);
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let mt = MemTable::new();
                for (i, key) in keys.iter().enumerate() {
                    let val = format!("value_{:08}", i);
                    mt.put_with_seq(key.as_bytes(), val.as_bytes(), i as u64);
                }
                black_box(mt);
            });
        });
    }

    group.finish();
}

fn bench_memtable_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_concurrent");

    let num_threads = 4;
    let ops_per_thread = 500;
    group.throughput(Throughput::Elements((num_threads * ops_per_thread) as u64));

    group.bench_function("4_threads_500_ops", |b| {
        b.iter(|| {
            let mt: Arc<MemTable> = Arc::new(MemTable::new());
            let mut handles = vec![];

            for t in 0..num_threads {
                let mt_clone = Arc::clone(&mt);
                let handle = thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{}_{:08}", t, i);
                        let val = format!("val_{}_{:08}", t, i);
                        mt_clone.put_with_seq(key.as_bytes(), val.as_bytes(), i as u64);
                    }
                });
                handles.push(handle);
            }

            for h in handles {
                h.join().unwrap();
            }

            black_box(mt);
        });
    });

    group.finish();
}

fn bench_memtable_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_read");

    let size = 5_000;
    let mt = MemTable::new();

    // Populate memtable
    for i in 0..size {
        let key = format!("key_{:08}", i);
        let val = format!("value_{:08}", i);
        mt.put_with_seq(key.as_bytes(), val.as_bytes(), i as u64);
    }

    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("5000_reads", |b| {
        b.iter(|| {
            let mut count = 0;
            for i in 0..size {
                let key = format!("key_{:08}", i);
                if mt.get(key.as_bytes()).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    group.finish();
}

// ============================================================================
// Compression Codec Benchmarks
// ============================================================================

fn make_data(size: usize, compressibility: f32) -> Vec<u8> {
    // compressibility: 0.0 = random, 1.0 = highly compressible
    if compressibility > 0.9 {
        vec![b'x'; size] // Highly compressible
    } else if compressibility > 0.5 {
        // Medium compressibility - pattern
        b"abcdefghijklmnopqrstuvwxyz0123456789"
            .iter()
            .cycle()
            .take(size)
            .copied()
            .collect()
    } else {
        // Low compressibility - deterministic random
        (0..size).map(|i| ((i * 2654435761) % 256) as u8).collect()
    }
}

fn bench_compression_lz4(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_compression_lz4");
    let codec = Lz4Codec::new();

    for &size in &[4_096, 16_384] {
        let data = make_data(size, 0.7); // Medium compressibility

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let compressed = codec.compress(&data).unwrap();
                black_box(compressed);
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = hotpath_storage;
    config = criterion_config();
    targets =
        bench_skiplist_sequential,
        bench_skiplist_random,
        bench_skiplist_concurrent,
        bench_memtable_sequential,
        bench_memtable_random,
        bench_memtable_concurrent,
        bench_memtable_read,
        bench_compression_lz4
}
criterion_main!(hotpath_storage);
