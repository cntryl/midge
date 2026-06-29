//! Tier 1 — Hot Path Compression Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers compression hot paths:
//! - Raw LZ4 / Zstd compress and decompress per block
//! - Block trailer compress + CRC and decompress + verify
//! - WAL value compress / decompress (LZ4 only, per-record)

#[path = "./criterion_config.rs"]
mod criterion_config;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier1;
use std::hint::black_box;

use cntryl_midge::sst::compression::{
    compress_block, compress_block_with_trailer, compress_wal_value, decompress_block,
    decompress_block_with_trailer, decompress_wal_value, CompressionAlgo, CompressionPolicy,
};

const TRAILER_COMPRESS_BATCH_SIZE: usize = 128;

// ============================================================================
// Raw Compress Benchmarks
// ============================================================================

/// Benchmark raw `compress_block` for each algorithm on a 16 KB compressible block.
fn bench_compress_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_compress_raw");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(16 * 1024));

    // Precompute a 16 KB compressible payload (repeated pattern)
    let data: Vec<u8> = (0..16 * 1024).map(|i| (i % 64) as u8).collect();

    let policies = [
        ("lz4", CompressionPolicy::Fixed(CompressionAlgo::Lz4)),
        ("zstd3", CompressionPolicy::Fixed(CompressionAlgo::Zstd3)),
        ("zstd9", CompressionPolicy::Fixed(CompressionAlgo::Zstd9)),
        ("none", CompressionPolicy::None),
    ];

    for (name, policy) in &policies {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let out = compress_block(black_box(&data), black_box(policy)).unwrap();
                black_box(out)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Raw Decompress Benchmarks
// ============================================================================

/// Benchmark raw `decompress_block` for each algorithm.
fn bench_decompress_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_decompress_raw");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(16 * 1024));

    let data: Vec<u8> = (0..16 * 1024).map(|i| (i % 64) as u8).collect();

    // Precompute compressed payloads for each algo
    let lz4_compressed =
        compress_block(&data, &CompressionPolicy::Fixed(CompressionAlgo::Lz4)).unwrap();
    let zstd3_compressed =
        compress_block(&data, &CompressionPolicy::Fixed(CompressionAlgo::Zstd3)).unwrap();
    let zstd9_compressed =
        compress_block(&data, &CompressionPolicy::Fixed(CompressionAlgo::Zstd9)).unwrap();

    let cases: [(&str, &[u8], CompressionAlgo); 3] = [
        ("lz4", &lz4_compressed.0, CompressionAlgo::Lz4),
        ("zstd3", &zstd3_compressed.0, CompressionAlgo::Zstd3),
        ("zstd9", &zstd9_compressed.0, CompressionAlgo::Zstd9),
    ];

    for (name, compressed, algo) in &cases {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let out = decompress_block(black_box(compressed), *algo).unwrap();
                black_box(out)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Block Trailer (compress + CRC / decompress + verify)
// ============================================================================

/// Benchmark `compress_block_with_trailer` (compress + append algo + CRC32C).
fn bench_compress_trailer(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_compress_trailer");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(
        (16 * 1024 * TRAILER_COMPRESS_BATCH_SIZE) as u64,
    ));

    let data: Vec<u8> = (0..16 * 1024).map(|i| (i % 64) as u8).collect();
    let policy_lz4 = CompressionPolicy::Fixed(CompressionAlgo::Lz4);
    let policy_zstd3 = CompressionPolicy::Fixed(CompressionAlgo::Zstd3);

    group.bench_function("lz4", |b| {
        b.iter(|| {
            let mut bytes = 0usize;
            for _ in 0..TRAILER_COMPRESS_BATCH_SIZE {
                let out = compress_block_with_trailer(black_box(&data), &policy_lz4).unwrap();
                bytes = bytes.wrapping_add(out.len());
            }
            black_box(bytes)
        });
    });

    group.bench_function("zstd3", |b| {
        b.iter(|| {
            let mut bytes = 0usize;
            for _ in 0..TRAILER_COMPRESS_BATCH_SIZE {
                let out = compress_block_with_trailer(black_box(&data), &policy_zstd3).unwrap();
                bytes = bytes.wrapping_add(out.len());
            }
            black_box(bytes)
        });
    });

    group.finish();
}

/// Benchmark `decompress_block_with_trailer` (CRC verify + decompress).
fn bench_decompress_trailer(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_decompress_trailer");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(16 * 1024));

    let data: Vec<u8> = (0..16 * 1024).map(|i| (i % 64) as u8).collect();

    let lz4_block =
        compress_block_with_trailer(&data, &CompressionPolicy::Fixed(CompressionAlgo::Lz4))
            .unwrap();
    let zstd3_block =
        compress_block_with_trailer(&data, &CompressionPolicy::Fixed(CompressionAlgo::Zstd3))
            .unwrap();

    group.bench_function("lz4", |b| {
        b.iter(|| {
            let out = decompress_block_with_trailer(black_box(&lz4_block)).unwrap();
            black_box(out)
        });
    });

    group.bench_function("zstd3", |b| {
        b.iter(|| {
            let out = decompress_block_with_trailer(black_box(&zstd3_block)).unwrap();
            black_box(out)
        });
    });

    group.finish();
}

// ============================================================================
// WAL Value Compress / Decompress
// ============================================================================

/// Benchmark WAL per-record compression (LZ4, skip-below-256B policy).
fn bench_wal_value_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_compress");
    group.sampling_mode(SamplingMode::Flat);

    // Small value (< MIN_COMPRESS_SIZE) — no-op path
    let small_val = vec![0xABu8; 128];
    // Medium compressible value
    let medium_val: Vec<u8> = (0..1024).map(|i| (i % 64) as u8).collect();

    group.throughput(Throughput::Elements(1));

    group.bench_function("skip_128b", |b| {
        b.iter(|| {
            let out = compress_wal_value(black_box(&small_val));
            black_box(out)
        });
    });

    group.throughput(Throughput::Bytes(1024));

    group.bench_function("lz4_1kb", |b| {
        b.iter(|| {
            let out = compress_wal_value(black_box(&medium_val));
            black_box(out)
        });
    });

    group.finish();
}

/// Benchmark WAL per-record decompression.
fn bench_wal_value_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_decompress");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute compressed WAL values
    let medium_val: Vec<u8> = (0..1024).map(|i| (i % 64) as u8).collect();
    let (compressed, comp_byte) = compress_wal_value(&medium_val);

    // Uncompressed passthrough
    let small_val = vec![0xABu8; 128];
    let (passthrough, pass_byte) = compress_wal_value(&small_val);

    group.throughput(Throughput::Elements(1));

    group.bench_function("passthrough_128b", |b| {
        b.iter(|| {
            let out = decompress_wal_value(black_box(&passthrough), pass_byte).unwrap();
            black_box(out)
        });
    });

    group.throughput(Throughput::Bytes(1024));

    group.bench_function("lz4_1kb", |b| {
        b.iter(|| {
            let out = decompress_wal_value(black_box(&compressed), comp_byte).unwrap();
            black_box(out)
        });
    });

    group.finish();
}

// ============================================================================
// Criterion Main
// ============================================================================

criterion_group!(
    name = benches;
    config = criterion_config_for_tier1();
    targets =
        bench_compress_raw,
        bench_decompress_raw,
        bench_compress_trailer,
        bench_decompress_trailer,
        bench_wal_value_compress,
        bench_wal_value_decompress
);

criterion_main!(benches);
