//! Tier 2 — Compression Subsystem Benchmarks
//!
//! **Target Runtime:** < 8 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers compression subsystem behaviour across realistic scenarios:
//! - Multiple block sizes (16 KB, 64 KB)
//! - Adaptive policy: LZ4 vs Zstd3 auto-selection
//! - Compressible vs incompressible data (worst-case)
//! - Batch throughput: consecutive block compress/decompress
//! - WAL batch: many record-level compress/decompress ops

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

use cntryl_midge::sst::compression::{
    compress_block_with_trailer, compress_wal_value, decompress_block_with_trailer,
    decompress_wal_value, CompressionAlgo, CompressionPolicy,
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Build a compressible payload (repeating pattern — high redundancy).
fn compressible_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 64) as u8).collect()
}

/// Build an incompressible payload (every byte distinct mod 256).
fn incompressible_payload(size: usize) -> Vec<u8> {
    // Pseudo-random via simple LCG to defeat pattern matching while remaining deterministic
    let mut v = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        v.push((state >> 24) as u8);
    }
    v
}

// ─── Block Compress — Multi-Size ─────────────────────────────────────────────

/// Benchmark compress_block_with_trailer for multiple block sizes and policies.
fn bench_block_compress_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/block_compress");
    group.sampling_mode(SamplingMode::Flat);

    let sizes: &[usize] = &[16 * 1024, 64 * 1024];
    let policies: &[(&str, CompressionPolicy)] = &[
        ("lz4", CompressionPolicy::Fixed(CompressionAlgo::Lz4)),
        ("zstd3", CompressionPolicy::Fixed(CompressionAlgo::Zstd3)),
        ("zstd9", CompressionPolicy::Fixed(CompressionAlgo::Zstd9)),
        (
            "adaptive",
            CompressionPolicy::Adaptive {
                min_savings_bytes: 256,
                min_ratio: 1.05,
                check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
            },
        ),
    ];

    for &size in sizes {
        let data = compressible_payload(size);
        group.throughput(Throughput::Bytes(size as u64));

        for (policy_name, policy) in policies {
            group.bench_with_input(BenchmarkId::new(*policy_name, size), &data, |b, data| {
                b.iter(|| {
                    let out = compress_block_with_trailer(black_box(data), policy).unwrap();
                    black_box(out)
                })
            });
        }
    }

    group.finish();
}

// ─── Block Decompress — Multi-Size ───────────────────────────────────────────

/// Benchmark decompress_block_with_trailer for multiple block sizes and codecs.
fn bench_block_decompress_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/block_decompress");
    group.sampling_mode(SamplingMode::Flat);

    let sizes: &[usize] = &[16 * 1024, 64 * 1024];
    let algos: &[(&str, CompressionPolicy)] = &[
        ("lz4", CompressionPolicy::Fixed(CompressionAlgo::Lz4)),
        ("zstd3", CompressionPolicy::Fixed(CompressionAlgo::Zstd3)),
        ("zstd9", CompressionPolicy::Fixed(CompressionAlgo::Zstd9)),
    ];

    for &size in sizes {
        let data = compressible_payload(size);
        group.throughput(Throughput::Bytes(size as u64));

        for (name, policy) in algos {
            // Precompute compressed block
            let compressed = compress_block_with_trailer(&data, policy).unwrap();

            group.bench_with_input(
                BenchmarkId::new(*name, size),
                &compressed,
                |b, compressed| {
                    b.iter(|| {
                        let out =
                            decompress_block_with_trailer(black_box(compressed.as_ref())).unwrap();
                        black_box(out)
                    })
                },
            );
        }
    }

    group.finish();
}

// ─── Incompressible Worst-Case ───────────────────────────────────────────────

/// Benchmark compression on worst-case (incompressible) data to measure overhead.
fn bench_compress_incompressible(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/incompressible");
    group.sampling_mode(SamplingMode::Flat);

    let size = 16 * 1024;
    let data = incompressible_payload(size);
    group.throughput(Throughput::Bytes(size as u64));

    let policies: &[(&str, CompressionPolicy)] = &[
        ("lz4", CompressionPolicy::Fixed(CompressionAlgo::Lz4)),
        ("zstd3", CompressionPolicy::Fixed(CompressionAlgo::Zstd3)),
        (
            "adaptive",
            CompressionPolicy::Adaptive {
                min_savings_bytes: 256,
                min_ratio: 1.05,
                check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
            },
        ),
    ];

    for (name, policy) in policies {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let out = compress_block_with_trailer(black_box(&data), policy).unwrap();
                black_box(out)
            })
        });
    }

    group.finish();
}

// ─── Batch Throughput ────────────────────────────────────────────────────────

/// Benchmark compressing a batch of 32 blocks (simulates SST build).
fn bench_batch_block_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/batch_compress");
    group.sampling_mode(SamplingMode::Flat);

    let block_size = 16 * 1024;
    let block_count = 32;
    let total_bytes = block_size * block_count;
    group.throughput(Throughput::Bytes(total_bytes as u64));

    // Precompute 32 blocks with varied data
    let blocks: Vec<Vec<u8>> = (0..block_count)
        .map(|i| compressible_payload(block_size + i * 7))
        .collect();

    let policy = CompressionPolicy::Fixed(CompressionAlgo::Lz4);

    group.bench_function("lz4_32x16kb", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for block in &blocks {
                let out = compress_block_with_trailer(black_box(block), &policy).unwrap();
                total += out.len();
            }
            black_box(total)
        })
    });

    let policy_zstd = CompressionPolicy::Fixed(CompressionAlgo::Zstd3);

    group.bench_function("zstd3_32x16kb", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for block in &blocks {
                let out = compress_block_with_trailer(black_box(block), &policy_zstd).unwrap();
                total += out.len();
            }
            black_box(total)
        })
    });

    group.finish();
}

/// Benchmark decompressing a batch of 32 blocks (simulates SST read).
fn bench_batch_block_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/batch_decompress");
    group.sampling_mode(SamplingMode::Flat);

    let block_size = 16 * 1024;
    let block_count = 32;
    let total_bytes = block_size * block_count;
    group.throughput(Throughput::Bytes(total_bytes as u64));

    let policy = CompressionPolicy::Fixed(CompressionAlgo::Lz4);

    // Precompute 32 compressed blocks
    let compressed_blocks: Vec<cntryl_midge::Bytes> = (0..block_count)
        .map(|i| {
            let block = compressible_payload(block_size + i * 7);
            compress_block_with_trailer(&block, &policy).unwrap()
        })
        .collect();

    group.bench_function("lz4_32x16kb", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for block in &compressed_blocks {
                let out = decompress_block_with_trailer(black_box(block.as_ref())).unwrap();
                total += out.len();
            }
            black_box(total)
        })
    });

    let policy_zstd = CompressionPolicy::Fixed(CompressionAlgo::Zstd3);
    let compressed_zstd: Vec<cntryl_midge::Bytes> = (0..block_count)
        .map(|i| {
            let block = compressible_payload(block_size + i * 7);
            compress_block_with_trailer(&block, &policy_zstd).unwrap()
        })
        .collect();

    group.bench_function("zstd3_32x16kb", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for block in &compressed_zstd {
                let out = decompress_block_with_trailer(black_box(block.as_ref())).unwrap();
                total += out.len();
            }
            black_box(total)
        })
    });

    group.finish();
}

// ─── WAL Batch ───────────────────────────────────────────────────────────────

/// Benchmark compressing a batch of 100 WAL record values (simulates WAL write burst).
fn bench_wal_batch_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/wal_batch_compress");
    group.sampling_mode(SamplingMode::Flat);

    let record_count = 100;
    let record_size = 512;
    group.throughput(Throughput::Bytes((record_count * record_size) as u64));

    // Precompute 100 compressible WAL records
    let records: Vec<Vec<u8>> = (0..record_count)
        .map(|i| compressible_payload(record_size + i * 3))
        .collect();

    group.bench_function("lz4_100x512b", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for rec in &records {
                let (out, _) = compress_wal_value(black_box(rec));
                total += out.len();
            }
            black_box(total)
        })
    });

    group.finish();
}

/// Benchmark decompressing a batch of 100 WAL record values.
fn bench_wal_batch_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/wal_batch_decompress");
    group.sampling_mode(SamplingMode::Flat);

    let record_count = 100;
    let record_size = 512;
    group.throughput(Throughput::Bytes((record_count * record_size) as u64));

    // Precompute compressed WAL records
    let compressed_records: Vec<(cntryl_midge::Bytes, Option<u8>)> = (0..record_count)
        .map(|i| {
            let rec = compressible_payload(record_size + i * 3);
            compress_wal_value(&rec)
        })
        .collect();

    group.bench_function("lz4_100x512b", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for (data, comp) in &compressed_records {
                let out = decompress_wal_value(black_box(data.as_ref()), *comp).unwrap();
                total += out.len();
            }
            black_box(total)
        })
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier2_subsystem_compression;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets =
        bench_block_compress_sizes,
        bench_block_decompress_sizes,
        bench_compress_incompressible,
        bench_batch_block_compress,
        bench_batch_block_decompress,
        bench_wal_batch_compress,
        bench_wal_batch_decompress
}
criterion_main!(tier2_subsystem_compression);
