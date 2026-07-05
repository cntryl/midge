//! Tier 2 — Compression Subsystem Benchmarks
//!
//! Covers block compression/decompression, incompressible inputs, and WAL batches.

use cntryl_midge::sst::compression::{
    compress_block_with_trailer, compress_wal_value, decompress_block_with_trailer,
    decompress_wal_value, CompressionAlgo, CompressionPolicy,
};
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const BLOCK_OPERATION_REPEATS: usize = 1024;
const BLOCK_OPERATION_REPEAT_OPS: u64 = 1024;
const LZ4_64K_DECOMPRESS_REPEATS: usize = 4096;
const LZ4_64K_DECOMPRESS_REPEAT_OPS: u64 = 4096;
const INCOMPRESSIBLE_LZ4_PREWARM_REPEATS: usize = 8192;
const ZSTD3_64K_OPERATION_REPEATS: usize = 1024;
const ZSTD3_64K_OPERATION_REPEAT_OPS: u64 = 1024;
const ZSTD9_64K_WINDOW_BLOCKS: usize = 32;
const ZSTD9_64K_WINDOW_REPEATS: usize = 32;
const BATCH_BLOCK_OPERATION_REPEATS: usize = 256;
const BATCH_BLOCK_OPERATION_PREWARM_REPEATS: usize = 16;
const WAL_BATCH_OPERATION_REPEATS: usize = 256;

fn compressible_payload(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from(i % 64).expect("pattern byte fits in u8"))
        .collect()
}

fn mixed_compressible_payload(size: usize) -> Vec<u8> {
    let mut state: u32 = 0xC0FF_EE31;
    (0..size)
        .map(|i| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            if i % 4 == 0 {
                u8::try_from(state >> 24).expect("shifted LCG byte fits in u8")
            } else {
                u8::try_from(i % 64).expect("pattern byte fits in u8")
            }
        })
        .collect()
}

fn zstd9_64k_block_window(size: usize) -> Vec<Vec<u8>> {
    (0..ZSTD9_64K_WINDOW_BLOCKS)
        .map(|i| mixed_compressible_payload(size + i * 7))
        .collect()
}

fn incompressible_payload(size: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        v.push(u8::try_from(state >> 24).expect("shifted LCG byte fits in u8"));
    }
    v
}

fn adaptive_policy() -> CompressionPolicy {
    CompressionPolicy::Adaptive {
        min_savings_bytes: 256,
        min_ratio: 1.05,
        check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
    }
}

fn prewarm_block_compress(data: &[u8], policy: &CompressionPolicy, repeats: usize) {
    let mut total = 0usize;
    for _ in 0..repeats {
        let out = compress_block_with_trailer(black_box(data), policy).unwrap();
        total = total.wrapping_add(out.len());
    }
    black_box(total);
}

fn run_block_compress(
    ctx: &mut StressContext,
    size: usize,
    policy_name: &'static str,
    policy: &CompressionPolicy,
) {
    if policy_name == "zstd3" && size == 64 * 1024 {
        run_repeated_block_compress(
            ctx,
            size,
            policy_name,
            policy,
            ZSTD3_64K_OPERATION_REPEATS,
            ZSTD3_64K_OPERATION_REPEAT_OPS,
        );
        return;
    }

    if policy_name == "zstd9" && size == 64 * 1024 {
        run_zstd9_64k_windowed_compress(ctx, policy);
        return;
    }

    run_repeated_block_compress(
        ctx,
        size,
        policy_name,
        policy,
        BLOCK_OPERATION_REPEATS,
        BLOCK_OPERATION_REPEAT_OPS,
    );
}

fn run_repeated_block_compress(
    ctx: &mut StressContext,
    size: usize,
    policy_name: &'static str,
    policy: &CompressionPolicy,
    repeats: usize,
    repeat_ops: u64,
) {
    let uses_mixed_payload = policy_name == "zstd3" && size == 64 * 1024;
    let data = if uses_mixed_payload {
        mixed_compressible_payload(size)
    } else {
        compressible_payload(size)
    };
    ctx.parameter("block_size", size);
    ctx.parameter("policy", policy_name);
    ctx.parameter("block_repeats", repeats);
    if uses_mixed_payload {
        ctx.parameter("data_shape", "mixed");
    }

    let measurement_name = format!("block_compress_{policy_name}_{}k", size / 1024);
    let _completed = ctx.measure_batch(measurement_name, (size as u64) * repeat_ops, || {
        let mut total = 0usize;
        for _ in 0..repeats {
            let out = compress_block_with_trailer(black_box(&data), policy).unwrap();
            total = total.wrapping_add(out.len());
        }
        black_box(total);
    });
}

fn run_zstd9_64k_windowed_compress(ctx: &mut StressContext, policy: &CompressionPolicy) {
    let size = 64 * 1024;
    let blocks = zstd9_64k_block_window(size);
    let bytes_per_window: usize = blocks.iter().map(Vec::len).sum();
    ctx.parameter("block_size", size);
    ctx.parameter("policy", "zstd9");
    ctx.parameter("data_shape", "mixed");
    ctx.parameter(
        "block_repeats",
        ZSTD9_64K_WINDOW_BLOCKS * ZSTD9_64K_WINDOW_REPEATS,
    );
    ctx.parameter("block_window", ZSTD9_64K_WINDOW_BLOCKS);
    ctx.parameter("window_repeats", ZSTD9_64K_WINDOW_REPEATS);

    let _completed = ctx.measure_batch(
        "block_compress_zstd9_64k",
        (bytes_per_window * ZSTD9_64K_WINDOW_REPEATS) as u64,
        || {
            let mut total = 0usize;
            for _ in 0..ZSTD9_64K_WINDOW_REPEATS {
                for block in &blocks {
                    let out = compress_block_with_trailer(black_box(block), policy).unwrap();
                    total = total.wrapping_add(out.len());
                }
            }
            black_box(total);
        },
    );
}

macro_rules! block_compress_case {
    ($fn_name:ident, $size:expr, $policy_name:literal, $policy:expr) => {
        #[stress(tier = 2, metadata(component = "compression"))]
        fn $fn_name(ctx: &mut StressContext) {
            run_block_compress(ctx, $size, $policy_name, &$policy);
        }
    };
}

block_compress_case!(
    block_compress_lz4_16k,
    16 * 1024,
    "lz4",
    CompressionPolicy::Fixed(CompressionAlgo::Lz4)
);
block_compress_case!(
    block_compress_zstd3_16k,
    16 * 1024,
    "zstd3",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd3)
);
block_compress_case!(
    block_compress_zstd9_16k,
    16 * 1024,
    "zstd9",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd9)
);
block_compress_case!(
    block_compress_adaptive_16k,
    16 * 1024,
    "adaptive",
    adaptive_policy()
);
block_compress_case!(
    block_compress_lz4_64k,
    64 * 1024,
    "lz4",
    CompressionPolicy::Fixed(CompressionAlgo::Lz4)
);
block_compress_case!(
    block_compress_zstd3_64k,
    64 * 1024,
    "zstd3",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd3)
);
block_compress_case!(
    block_compress_zstd9_64k,
    64 * 1024,
    "zstd9",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd9)
);
block_compress_case!(
    block_compress_adaptive_64k,
    64 * 1024,
    "adaptive",
    adaptive_policy()
);

fn run_block_decompress(
    ctx: &mut StressContext,
    size: usize,
    policy_name: &'static str,
    policy: &CompressionPolicy,
) {
    let data = compressible_payload(size);
    let compressed = compress_block_with_trailer(&data, policy).unwrap();
    let (repeats, repeat_ops) = if policy_name == "lz4" && size == 64 * 1024 {
        (LZ4_64K_DECOMPRESS_REPEATS, LZ4_64K_DECOMPRESS_REPEAT_OPS)
    } else {
        (BLOCK_OPERATION_REPEATS, BLOCK_OPERATION_REPEAT_OPS)
    };
    ctx.parameter("block_size", size);
    ctx.parameter("policy", policy_name);
    ctx.parameter("block_repeats", repeats);

    let measurement_name = format!("block_decompress_{policy_name}_{}k", size / 1024);
    let _completed = ctx.measure_batch(measurement_name, (size as u64) * repeat_ops, || {
        let mut total = 0usize;
        for _ in 0..repeats {
            let out = decompress_block_with_trailer(black_box(compressed.as_ref())).unwrap();
            total = total.wrapping_add(out.len());
        }
        black_box(total);
    });
}

macro_rules! block_decompress_case {
    ($fn_name:ident, $size:expr, $policy_name:literal, $policy:expr) => {
        #[stress(tier = 2, metadata(component = "compression"))]
        fn $fn_name(ctx: &mut StressContext) {
            run_block_decompress(ctx, $size, $policy_name, &$policy);
        }
    };
}

block_decompress_case!(
    block_decompress_lz4_16k,
    16 * 1024,
    "lz4",
    CompressionPolicy::Fixed(CompressionAlgo::Lz4)
);
block_decompress_case!(
    block_decompress_zstd3_16k,
    16 * 1024,
    "zstd3",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd3)
);
block_decompress_case!(
    block_decompress_zstd9_16k,
    16 * 1024,
    "zstd9",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd9)
);
block_decompress_case!(
    block_decompress_lz4_64k,
    64 * 1024,
    "lz4",
    CompressionPolicy::Fixed(CompressionAlgo::Lz4)
);
block_decompress_case!(
    block_decompress_zstd3_64k,
    64 * 1024,
    "zstd3",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd3)
);
block_decompress_case!(
    block_decompress_zstd9_64k,
    64 * 1024,
    "zstd9",
    CompressionPolicy::Fixed(CompressionAlgo::Zstd9)
);

fn run_incompressible(
    ctx: &mut StressContext,
    policy_name: &'static str,
    policy: &CompressionPolicy,
) {
    let size = 16 * 1024;
    let data = incompressible_payload(size);
    ctx.parameter("block_size", size);
    ctx.parameter("policy", policy_name);
    ctx.parameter("data_shape", "incompressible");
    ctx.parameter("block_repeats", BLOCK_OPERATION_REPEATS);
    if policy_name == "lz4" {
        ctx.parameter("prewarm_repeats", INCOMPRESSIBLE_LZ4_PREWARM_REPEATS);
        prewarm_block_compress(&data, policy, INCOMPRESSIBLE_LZ4_PREWARM_REPEATS);
    }

    let measurement_name = format!("incompressible_{policy_name}");
    let _completed = ctx.measure_batch(
        measurement_name,
        (size as u64) * BLOCK_OPERATION_REPEAT_OPS,
        || {
            let mut total = 0usize;
            for _ in 0..BLOCK_OPERATION_REPEATS {
                let out = compress_block_with_trailer(black_box(&data), policy).unwrap();
                total = total.wrapping_add(out.len());
            }
            black_box(total);
        },
    );
}

#[stress(
    tier = 2,
    metadata(component = "compression", scenario = "incompressible_lz4")
)]
fn incompressible_lz4(ctx: &mut StressContext) {
    run_incompressible(ctx, "lz4", &CompressionPolicy::Fixed(CompressionAlgo::Lz4));
}

#[stress(
    tier = 2,
    metadata(component = "compression", scenario = "incompressible_zstd3")
)]
fn incompressible_zstd3(ctx: &mut StressContext) {
    run_incompressible(
        ctx,
        "zstd3",
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
    );
}

#[stress(
    tier = 2,
    metadata(component = "compression", scenario = "incompressible_adaptive")
)]
fn incompressible_adaptive(ctx: &mut StressContext) {
    run_incompressible(ctx, "adaptive", &adaptive_policy());
}

fn run_batch_block_compress(
    ctx: &mut StressContext,
    policy_name: &'static str,
    policy: &CompressionPolicy,
) {
    let block_size = 16 * 1024;
    let block_count = 32;
    let blocks: Vec<Vec<u8>> = (0..block_count)
        .map(|i| compressible_payload(block_size + i * 7))
        .collect();
    ctx.parameter("block_size", block_size);
    ctx.parameter("block_count", block_count);
    ctx.parameter("policy", policy_name);
    ctx.parameter("batch_repeats", BATCH_BLOCK_OPERATION_REPEATS);
    ctx.parameter(
        "batch_prewarm_repeats",
        BATCH_BLOCK_OPERATION_PREWARM_REPEATS,
    );
    let mut prewarm_total = 0usize;
    for _ in 0..BATCH_BLOCK_OPERATION_PREWARM_REPEATS {
        for block in &blocks {
            let out = compress_block_with_trailer(black_box(block), policy).unwrap();
            prewarm_total = prewarm_total.wrapping_add(out.len());
        }
    }
    black_box(prewarm_total);

    let _completed = ctx.measure_batch(
        format!("batch_compress_{policy_name}_32x16k"),
        (block_size * block_count * BATCH_BLOCK_OPERATION_REPEATS) as u64,
        || {
            let mut total = 0usize;
            for _ in 0..BATCH_BLOCK_OPERATION_REPEATS {
                for block in &blocks {
                    let out = compress_block_with_trailer(black_box(block), policy).unwrap();
                    total += out.len();
                }
            }
            black_box(total);
        },
    );
}

#[stress(
    tier = 2,
    metadata(component = "compression", scenario = "batch_compress_lz4_32x16k")
)]
fn batch_compress_lz4_32x16k(ctx: &mut StressContext) {
    run_batch_block_compress(ctx, "lz4", &CompressionPolicy::Fixed(CompressionAlgo::Lz4));
}

#[stress(
    tier = 2,
    metadata(component = "compression", scenario = "batch_compress_zstd3_32x16k")
)]
fn batch_compress_zstd3_32x16k(ctx: &mut StressContext) {
    run_batch_block_compress(
        ctx,
        "zstd3",
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
    );
}

fn run_batch_block_decompress(
    ctx: &mut StressContext,
    policy_name: &'static str,
    policy: &CompressionPolicy,
) {
    let block_size = 16 * 1024;
    let block_count = 32;
    let compressed_blocks: Vec<cntryl_midge::Bytes> = (0..block_count)
        .map(|i| {
            let block = compressible_payload(block_size + i * 7);
            compress_block_with_trailer(&block, policy).unwrap()
        })
        .collect();
    ctx.parameter("block_size", block_size);
    ctx.parameter("block_count", block_count);
    ctx.parameter("policy", policy_name);
    ctx.parameter("batch_repeats", BATCH_BLOCK_OPERATION_REPEATS);
    ctx.parameter(
        "batch_prewarm_repeats",
        BATCH_BLOCK_OPERATION_PREWARM_REPEATS,
    );
    let mut prewarm_total = 0usize;
    for _ in 0..BATCH_BLOCK_OPERATION_PREWARM_REPEATS {
        for block in &compressed_blocks {
            let out = decompress_block_with_trailer(black_box(block.as_ref())).unwrap();
            prewarm_total = prewarm_total.wrapping_add(out.len());
        }
    }
    black_box(prewarm_total);

    let _completed = ctx.measure_batch(
        format!("batch_decompress_{policy_name}_32x16k"),
        (block_size * block_count * BATCH_BLOCK_OPERATION_REPEATS) as u64,
        || {
            let mut total = 0usize;
            for _ in 0..BATCH_BLOCK_OPERATION_REPEATS {
                for block in &compressed_blocks {
                    let out = decompress_block_with_trailer(black_box(block.as_ref())).unwrap();
                    total += out.len();
                }
            }
            black_box(total);
        },
    );
}

#[stress(
    tier = 2,
    metadata(component = "compression", scenario = "batch_decompress_lz4_32x16k")
)]
fn batch_decompress_lz4_32x16k(ctx: &mut StressContext) {
    run_batch_block_decompress(ctx, "lz4", &CompressionPolicy::Fixed(CompressionAlgo::Lz4));
}

#[stress(
    tier = 2,
    metadata(component = "compression", scenario = "batch_decompress_zstd3_32x16k")
)]
fn batch_decompress_zstd3_32x16k(ctx: &mut StressContext) {
    run_batch_block_decompress(
        ctx,
        "zstd3",
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
    );
}

#[stress(
    tier = 2,
    metadata(
        component = "compression",
        scenario = "wal_batch_compress_lz4_100x512b"
    )
)]
fn wal_batch_compress_lz4_100x512b(ctx: &mut StressContext) {
    let record_count = 100;
    let record_size = 512;
    let records: Vec<Vec<u8>> = (0..record_count)
        .map(|i| compressible_payload(record_size + i * 3))
        .collect();
    ctx.parameter("record_count", record_count);
    ctx.parameter("record_size", record_size);
    ctx.parameter("batch_repeats", WAL_BATCH_OPERATION_REPEATS);

    let _completed = ctx.measure_batch(
        "wal_batch_compress_lz4_100x512b",
        (record_count * record_size * WAL_BATCH_OPERATION_REPEATS) as u64,
        || {
            let mut total = 0usize;
            for _ in 0..WAL_BATCH_OPERATION_REPEATS {
                for rec in &records {
                    let (out, _) = compress_wal_value(black_box(rec));
                    total += out.len();
                }
            }
            black_box(total);
        },
    );
}

#[stress(
    tier = 2,
    metadata(
        component = "compression",
        scenario = "wal_batch_decompress_lz4_100x512b"
    )
)]
fn wal_batch_decompress_lz4_100x512b(ctx: &mut StressContext) {
    let record_count = 100;
    let record_size = 512;
    let compressed_records: Vec<(cntryl_midge::Bytes, Option<u8>)> = (0..record_count)
        .map(|i| {
            let rec = compressible_payload(record_size + i * 3);
            compress_wal_value(&rec)
        })
        .collect();
    ctx.parameter("record_count", record_count);
    ctx.parameter("record_size", record_size);
    ctx.parameter("batch_repeats", WAL_BATCH_OPERATION_REPEATS);

    let _completed = ctx.measure_batch(
        "wal_batch_decompress_lz4_100x512b",
        (record_count * record_size * WAL_BATCH_OPERATION_REPEATS) as u64,
        || {
            let mut total = 0usize;
            for _ in 0..WAL_BATCH_OPERATION_REPEATS {
                for (data, comp) in &compressed_records {
                    let out = decompress_wal_value(black_box(data.as_ref()), *comp).unwrap();
                    total += out.len();
                }
            }
            black_box(total);
        },
    );
}

stress_main!();
