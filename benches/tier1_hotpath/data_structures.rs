//! Tier 1 — Hot Path Data Structure Benchmarks
//!
//! Covers core in-memory data structures:
//! • SkipList (sequential, random insertion)
//! • MemTable (sequential, random, reads)
//! • LZ4 compression/decompression
//!
//! • Zero allocations inside measured loop
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
use std::hint::black_box;

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

// Note: Concurrent skiplist benchmark moved to tier2_subsystem/storage.rs
// Thread-based benchmarks belong in tier2 even with barrier-based thread reuse.

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
    name = tier1_hotpath_data_structures;
    config = criterion_config();
    targets =
        bench_skiplist_sequential,
        bench_skiplist_random,
        bench_memtable_sequential,
        bench_memtable_random,
        bench_memtable_read,
        bench_compression_lz4
}
criterion_main!(tier1_hotpath_data_structures);
