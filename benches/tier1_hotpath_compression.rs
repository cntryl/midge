//! Tier 1 — Hot Path Compression Benchmarks
//!
//! Covers raw block compression/decompression, trailer variants, and WAL values.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::compression::{
    compress_block, compress_block_with_trailer, compress_wal_value, decompress_block,
    decompress_block_with_trailer, decompress_wal_value, CompressionAlgo, CompressionPolicy,
};
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const BLOCK_SIZE: usize = 16 * 1024;
const RAW_COMPRESS_BATCH_SIZE: usize = 256;
const RAW_ZSTD_COMPRESS_WINDOW_BLOCKS: usize = 32;
const RAW_ZSTD_COMPRESS_WINDOW_REPEATS: usize =
    RAW_COMPRESS_BATCH_SIZE / RAW_ZSTD_COMPRESS_WINDOW_BLOCKS;
const RAW_NONE_BATCH_SIZE: usize = 4096;
const TRAILER_COMPRESS_BATCH_SIZE: usize = 1024;
const TRAILER_ZSTD_COMPRESS_BATCH_SIZE: usize = 256;
const TRAILER_ZSTD_COMPRESS_WINDOW_REPEATS: usize =
    TRAILER_ZSTD_COMPRESS_BATCH_SIZE / RAW_ZSTD_COMPRESS_WINDOW_BLOCKS;
const RAW_NONE_PREWARM_REPEATS: usize = 4096;
const WAL_SKIP_BATCH_SIZE: usize = 1024;
const WAL_SKIP_BATCH_OPS: u64 = 1024;

cntryl_stress::stress_allocator!();

fn compressible_block() -> Vec<u8> {
    (0u8..64).cycle().take(BLOCK_SIZE).collect()
}

fn compressible_block_window() -> Vec<Vec<u8>> {
    (0..RAW_ZSTD_COMPRESS_WINDOW_BLOCKS)
        .map(|i| (0u8..64).cycle().take(BLOCK_SIZE + i * 7).collect())
        .collect()
}

fn run_compress_raw(ctx: &mut StressContext, name: &'static str, policy: &CompressionPolicy) {
    if matches!(name, "zstd3" | "zstd9") {
        run_compress_raw_zstd_window(ctx, name, policy);
        return;
    }

    let data = compressible_block();
    ctx.parameter("codec", name);
    ctx.parameter("block_size", data.len());
    ctx.parameter("batch_size", RAW_COMPRESS_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, RAW_COMPRESS_BATCH_SIZE as u64, || {
        let mut bytes = 0usize;
        for _ in 0..RAW_COMPRESS_BATCH_SIZE {
            let out = compress_block(black_box(&data), black_box(policy)).unwrap();
            bytes = bytes.wrapping_add(out.0.len());
        }
        black_box(bytes);
    });
}

fn run_compress_raw_zstd_window(
    ctx: &mut StressContext,
    name: &'static str,
    policy: &CompressionPolicy,
) {
    let blocks = compressible_block_window();
    ctx.parameter("codec", name);
    ctx.parameter("block_size", BLOCK_SIZE);
    ctx.parameter("batch_size", RAW_COMPRESS_BATCH_SIZE);
    ctx.parameter("block_window", RAW_ZSTD_COMPRESS_WINDOW_BLOCKS);
    ctx.parameter("window_repeats", RAW_ZSTD_COMPRESS_WINDOW_REPEATS);

    stress_config::measure_micro_batch(ctx, RAW_COMPRESS_BATCH_SIZE as u64, || {
        let mut bytes = 0usize;
        for _ in 0..RAW_ZSTD_COMPRESS_WINDOW_REPEATS {
            for block in &blocks {
                let out = compress_block(black_box(block), black_box(policy)).unwrap();
                bytes = bytes.wrapping_add(out.0.len());
            }
        }
        black_box(bytes);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "compress_raw_lz4")
)]
fn compress_raw_lz4(ctx: &mut StressContext) {
    run_compress_raw(ctx, "lz4", &CompressionPolicy::Fixed(CompressionAlgo::Lz4));
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "compress_raw_zstd3")
)]
fn compress_raw_zstd3(ctx: &mut StressContext) {
    run_compress_raw(
        ctx,
        "zstd3",
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
    );
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "compress_raw_zstd9")
)]
fn compress_raw_zstd9(ctx: &mut StressContext) {
    run_compress_raw(
        ctx,
        "zstd9",
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
    );
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "compress_raw_none")
)]
fn compress_raw_none(ctx: &mut StressContext) {
    let data = compressible_block();
    ctx.parameter("codec", "none");
    ctx.parameter("block_size", data.len());
    ctx.parameter("batch_size", RAW_NONE_BATCH_SIZE);
    ctx.parameter("prewarm_repeats", RAW_NONE_PREWARM_REPEATS);

    let mut prewarm_bytes = 0usize;
    for _ in 0..RAW_NONE_PREWARM_REPEATS {
        let out = compress_block(black_box(&data), black_box(&CompressionPolicy::None))
            .expect("none compression should succeed");
        prewarm_bytes = prewarm_bytes.wrapping_add(out.0.len());
    }
    black_box(prewarm_bytes);

    stress_config::measure_micro_batch(ctx, RAW_NONE_BATCH_SIZE as u64, || {
        let mut bytes = 0usize;
        for _ in 0..RAW_NONE_BATCH_SIZE {
            let out = compress_block(black_box(&data), black_box(&CompressionPolicy::None))
                .expect("none compression should succeed");
            bytes = bytes.wrapping_add(out.0.len());
        }
        black_box(bytes);
    });
}

fn run_decompress_raw(ctx: &mut StressContext, name: &'static str, algo: CompressionAlgo) {
    let data = compressible_block();
    let compressed = compress_block(&data, &CompressionPolicy::Fixed(algo)).unwrap();
    ctx.parameter("codec", name);
    ctx.parameter("block_size", data.len());

    ctx.measure_micro(|| {
        let out = decompress_block(black_box(&compressed.0), algo).unwrap();
        black_box(out);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "decompress_raw_lz4")
)]
fn decompress_raw_lz4(ctx: &mut StressContext) {
    run_decompress_raw(ctx, "lz4", CompressionAlgo::Lz4);
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "decompress_raw_zstd3")
)]
fn decompress_raw_zstd3(ctx: &mut StressContext) {
    run_decompress_raw(ctx, "zstd3", CompressionAlgo::Zstd3);
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "decompress_raw_zstd9")
)]
fn decompress_raw_zstd9(ctx: &mut StressContext) {
    run_decompress_raw(ctx, "zstd9", CompressionAlgo::Zstd9);
}

fn run_compress_trailer(ctx: &mut StressContext, name: &'static str, policy: &CompressionPolicy) {
    if name == "zstd3" {
        run_compress_trailer_zstd_window(ctx, policy);
        return;
    }

    let data = compressible_block();
    ctx.parameter("codec", name);
    ctx.parameter("block_size", data.len());
    ctx.parameter("batch_size", TRAILER_COMPRESS_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TRAILER_COMPRESS_BATCH_SIZE as u64, || {
        let mut bytes = 0usize;
        for _ in 0..TRAILER_COMPRESS_BATCH_SIZE {
            let out = compress_block_with_trailer(black_box(&data), policy).unwrap();
            bytes = bytes.wrapping_add(out.len());
        }
        black_box(bytes);
    });
}

fn run_compress_trailer_zstd_window(ctx: &mut StressContext, policy: &CompressionPolicy) {
    let blocks = compressible_block_window();
    ctx.parameter("codec", "zstd3");
    ctx.parameter("block_size", BLOCK_SIZE);
    ctx.parameter("batch_size", TRAILER_ZSTD_COMPRESS_BATCH_SIZE);
    ctx.parameter("block_window", RAW_ZSTD_COMPRESS_WINDOW_BLOCKS);
    ctx.parameter("window_repeats", TRAILER_ZSTD_COMPRESS_WINDOW_REPEATS);

    stress_config::measure_micro_batch(ctx, TRAILER_ZSTD_COMPRESS_BATCH_SIZE as u64, || {
        let mut bytes = 0usize;
        for _ in 0..TRAILER_ZSTD_COMPRESS_WINDOW_REPEATS {
            for block in &blocks {
                let out = compress_block_with_trailer(black_box(block), policy).unwrap();
                bytes = bytes.wrapping_add(out.len());
            }
        }
        black_box(bytes);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "compress_trailer_lz4")
)]
fn compress_trailer_lz4(ctx: &mut StressContext) {
    run_compress_trailer(ctx, "lz4", &CompressionPolicy::Fixed(CompressionAlgo::Lz4));
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "compress_trailer_zstd3")
)]
fn compress_trailer_zstd3(ctx: &mut StressContext) {
    run_compress_trailer(
        ctx,
        "zstd3",
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
    );
}

fn run_decompress_trailer(ctx: &mut StressContext, name: &'static str, policy: &CompressionPolicy) {
    let data = compressible_block();
    let block = compress_block_with_trailer(&data, policy).unwrap();
    ctx.parameter("codec", name);
    ctx.parameter("block_size", data.len());

    ctx.measure_micro(|| {
        let out = decompress_block_with_trailer(black_box(&block)).unwrap();
        black_box(out);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "decompress_trailer_lz4")
)]
fn decompress_trailer_lz4(ctx: &mut StressContext) {
    run_decompress_trailer(ctx, "lz4", &CompressionPolicy::Fixed(CompressionAlgo::Lz4));
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "decompress_trailer_zstd3")
)]
fn decompress_trailer_zstd3(ctx: &mut StressContext) {
    run_decompress_trailer(
        ctx,
        "zstd3",
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
    );
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "wal_compress_skip_128b")
)]
fn wal_compress_skip_128b(ctx: &mut StressContext) {
    let small_val = vec![0xABu8; 128];
    ctx.parameter("value_size", small_val.len());
    ctx.parameter("batch_size", WAL_SKIP_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, WAL_SKIP_BATCH_OPS, || {
        let mut bytes = 0usize;
        for _ in 0..WAL_SKIP_BATCH_SIZE {
            let (out, marker) = compress_wal_value(black_box(&small_val));
            bytes = bytes.wrapping_add(out.len() + usize::from(marker.is_some()));
        }
        black_box(bytes);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "wal_compress_lz4_1kb")
)]
fn wal_compress_lz4_1kb(ctx: &mut StressContext) {
    let medium_val: Vec<u8> = (0u8..64).cycle().take(1024).collect();
    ctx.parameter("value_size", medium_val.len());

    ctx.measure_micro(|| {
        let out = compress_wal_value(black_box(&medium_val));
        black_box(out);
    });
}

#[stress_test(
    tier = 1,
    metadata(
        component = "compression",
        scenario = "wal_decompress_passthrough_128b"
    )
)]
fn wal_decompress_passthrough_128b(ctx: &mut StressContext) {
    let small_val = vec![0xABu8; 128];
    let (passthrough, pass_byte) = compress_wal_value(&small_val);
    ctx.parameter("value_size", small_val.len());

    ctx.measure_micro(|| {
        let out = decompress_wal_value(black_box(&passthrough), pass_byte).unwrap();
        black_box(out);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "compression", scenario = "wal_decompress_lz4_1kb")
)]
fn wal_decompress_lz4_1kb(ctx: &mut StressContext) {
    let medium_val: Vec<u8> = (0u8..64).cycle().take(1024).collect();
    let (compressed, comp_byte) = compress_wal_value(&medium_val);
    ctx.parameter("value_size", medium_val.len());

    ctx.measure_micro(|| {
        let out = decompress_wal_value(black_box(&compressed), comp_byte).unwrap();
        black_box(out);
    });
}

stress_main!();
