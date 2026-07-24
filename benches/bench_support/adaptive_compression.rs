use cntryl_midge::sst::compression::{
    compress_block, CompressionAlgo, CompressionPolicy, BLOCK_TRAILER_SIZE,
};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, StressContext};

const CORPUS_BLOCKS: usize = 16;
const CORPUS_REPEATS: usize = 8;
const MEASURED_SAMPLES: usize = 20;
const WARMUP_SAMPLES: usize = 3;
const MIN_SAVINGS_BYTES: usize = 256;
const MIN_RATIO: f32 = 0.95;
const ALGORITHMS: [CompressionAlgo; 2] = [CompressionAlgo::Lz4, CompressionAlgo::Zstd3];
const SCREEN_CORPUS_REPEATS: usize = 2;
const SCREEN_SHAPES: [CorpusShape; 6] = [
    CorpusShape::Repeated,
    CorpusShape::Structured,
    CorpusShape::Mixed,
    CorpusShape::PrefixRandomTail,
    CorpusShape::Uniform,
    CorpusShape::LowCardinality,
];
const SCREEN_BLOCK_SIZES: [usize; 2] = [16 * 1024, 64 * 1024];

#[derive(Clone, Copy)]
pub enum CorpusShape {
    Repeated,
    Structured,
    Mixed,
    PrefixRandomTail,
    Uniform,
    LowCardinality,
}

impl CorpusShape {
    const fn name(self) -> &'static str {
        match self {
            Self::Repeated => "repeated",
            Self::Structured => "structured",
            Self::Mixed => "mixed",
            Self::PrefixRandomTail => "prefix_random_tail",
            Self::Uniform => "uniform",
            Self::LowCardinality => "low_cardinality",
        }
    }
}

#[derive(Clone, Copy)]
pub enum SelectionRole {
    Candidate,
    Oracle,
}

#[derive(Default)]
struct SelectionStats {
    physical_bytes: usize,
    none_blocks: usize,
    lz4_blocks: usize,
    zstd3_blocks: usize,
    lz4_attempts: usize,
    zstd3_attempts: usize,
}

impl SelectionStats {
    fn add_assign(&mut self, other: &Self) {
        self.physical_bytes += other.physical_bytes;
        self.none_blocks += other.none_blocks;
        self.lz4_blocks += other.lz4_blocks;
        self.zstd3_blocks += other.zstd3_blocks;
        self.lz4_attempts += other.lz4_attempts;
        self.zstd3_attempts += other.zstd3_attempts;
    }

    fn record_codec(&mut self, algorithm: CompressionAlgo) {
        match algorithm {
            CompressionAlgo::None => self.none_blocks += 1,
            CompressionAlgo::Lz4 => self.lz4_blocks += 1,
            CompressionAlgo::Zstd3 => self.zstd3_blocks += 1,
            CompressionAlgo::Zstd9 | CompressionAlgo::Zlib | CompressionAlgo::Snappy => {
                unreachable!("diagnostic policy only emits None, LZ4, or Zstd3")
            }
        }
    }

    fn codec_counts(&self) -> String {
        format!(
            "none={},lz4={},zstd3={}",
            self.none_blocks, self.lz4_blocks, self.zstd3_blocks
        )
    }
}

fn lcg_bytes(size: usize, seed: u32) -> Vec<u8> {
    let mut state = seed;
    (0..size)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            u8::try_from(state >> 24).expect("shifted LCG byte fits in u8")
        })
        .collect()
}

fn repeated_block(size: usize, seed: usize) -> Vec<u8> {
    let byte = b'A' + u8::try_from(seed % 23).expect("repeated seed fits in u8");
    vec![byte; size]
}

fn structured_block(size: usize, seed: usize) -> Vec<u8> {
    let pattern = format!(
        "account={seed:04}|region={}|status=active|segment={}|",
        ["east", "west", "north", "south"][seed % 4],
        ["consumer", "business", "public"][seed % 3],
    );
    pattern
        .as_bytes()
        .iter()
        .copied()
        .cycle()
        .take(size)
        .collect()
}

fn mixed_block(size: usize, seed: usize) -> Vec<u8> {
    let structured = structured_block(size, seed);
    let random = lcg_bytes(
        size,
        0xc0ff_ee31 ^ u32::try_from(seed).expect("seed fits in u32"),
    );
    structured
        .into_iter()
        .zip(random)
        .enumerate()
        .map(|(index, (structured_byte, random_byte))| {
            if index.is_multiple_of(4) {
                random_byte
            } else {
                structured_byte
            }
        })
        .collect()
}

fn prefix_random_tail_block(size: usize, seed: usize) -> Vec<u8> {
    let prefix_len = size * 3 / 4;
    let mut block = structured_block(prefix_len, seed);
    block.extend(lcg_bytes(
        size - prefix_len,
        0x51a7_1e5d ^ u32::try_from(seed).expect("seed fits in u32"),
    ));
    block
}

fn low_cardinality_block(size: usize, seed: usize) -> Vec<u8> {
    let symbols = [
        b'A' + u8::try_from(seed % 13).expect("low-cardinality seed fits in u8"),
        b'a' + u8::try_from(seed % 17).expect("low-cardinality seed fits in u8"),
        b'0' + u8::try_from(seed % 10).expect("low-cardinality seed fits in u8"),
        b'|',
    ];
    let random = lcg_bytes(
        size,
        0xa11c_e5ed ^ u32::try_from(seed).expect("seed fits in u32"),
    );
    random
        .into_iter()
        .map(|byte| symbols[usize::from(byte) % symbols.len()])
        .collect()
}

fn corpus(shape: CorpusShape, size: usize) -> Vec<Vec<u8>> {
    (0..CORPUS_BLOCKS)
        .map(|seed| match shape {
            CorpusShape::Repeated => repeated_block(size, seed),
            CorpusShape::Structured => structured_block(size, seed),
            CorpusShape::Mixed => mixed_block(size, seed),
            CorpusShape::PrefixRandomTail => prefix_random_tail_block(size, seed),
            CorpusShape::Uniform => lcg_bytes(
                size,
                0xdead_beef ^ u32::try_from(seed).expect("seed fits in u32"),
            ),
            CorpusShape::LowCardinality => low_cardinality_block(size, seed),
        })
        .collect()
}

#[allow(clippy::cast_precision_loss)]
fn qualifies(original_size: usize, compressed_size: usize) -> bool {
    compressed_size < original_size
        && original_size.saturating_sub(compressed_size) >= MIN_SAVINGS_BYTES
        && compressed_size as f32 / original_size as f32 <= MIN_RATIO
}

#[allow(clippy::cast_precision_loss)]
fn meets_fast_accept(original_size: usize, compressed_size: usize, fast_accept_ratio: f32) -> bool {
    compressed_size as f32 / original_size as f32 <= MIN_RATIO.min(fast_accept_ratio)
}

fn append_trailer(compressed: &[u8], algorithm: CompressionAlgo) -> Bytes {
    let mut block = Vec::with_capacity(compressed.len() + BLOCK_TRAILER_SIZE);
    block.extend_from_slice(compressed);
    block.push(algorithm.to_u8());
    let crc = crc32c::crc32c(&block);
    block.extend_from_slice(&crc.to_le_bytes());
    Bytes::from(block)
}

fn exhaustive_oracle(block: &[u8]) -> (Bytes, CompressionAlgo) {
    let mut best: Option<(Bytes, CompressionAlgo)> = None;

    for algorithm in ALGORITHMS {
        let Ok((compressed, _)) = compress_block(block, &CompressionPolicy::Fixed(algorithm))
        else {
            continue;
        };
        if qualifies(block.len(), compressed.len())
            && best
                .as_ref()
                .is_none_or(|(current, _)| compressed.len() < current.len())
        {
            best = Some((compressed, algorithm));
        }
    }

    best.map_or_else(
        || {
            (
                append_trailer(block, CompressionAlgo::None),
                CompressionAlgo::None,
            )
        },
        |(compressed, algorithm)| (append_trailer(&compressed, algorithm), algorithm),
    )
}

fn ordered_cascade_candidate(
    block: &[u8],
    fast_accept_ratio: f32,
) -> (Bytes, CompressionAlgo, usize) {
    let mut attempts = 0;
    let mut best: Option<(Bytes, CompressionAlgo)> = None;
    for algorithm in ALGORITHMS {
        attempts += 1;
        let Ok((compressed, _)) = compress_block(block, &CompressionPolicy::Fixed(algorithm))
        else {
            continue;
        };
        if qualifies(block.len(), compressed.len())
            && meets_fast_accept(block.len(), compressed.len(), fast_accept_ratio)
        {
            return (append_trailer(&compressed, algorithm), algorithm, attempts);
        }
        if qualifies(block.len(), compressed.len())
            && best
                .as_ref()
                .is_none_or(|(current, _)| compressed.len() < current.len())
        {
            best = Some((compressed, algorithm));
        }
    }
    best.map_or_else(
        || {
            (
                append_trailer(block, CompressionAlgo::None),
                CompressionAlgo::None,
                attempts,
            )
        },
        |(compressed, algorithm)| (append_trailer(&compressed, algorithm), algorithm, attempts),
    )
}

fn candidate_stats(blocks: &[Vec<u8>], fast_accept_ratio: f32) -> SelectionStats {
    let mut stats = SelectionStats::default();
    for block in blocks {
        let (encoded, algorithm, attempts) = ordered_cascade_candidate(block, fast_accept_ratio);
        stats.lz4_attempts += 1;
        stats.zstd3_attempts += usize::from(attempts == 2);
        stats.physical_bytes += encoded.len();
        stats.record_codec(algorithm);
    }
    stats
}

fn oracle_stats(blocks: &[Vec<u8>]) -> SelectionStats {
    let mut stats = SelectionStats::default();
    for block in blocks {
        let (encoded, algorithm) = exhaustive_oracle(block);
        stats.lz4_attempts += 1;
        stats.zstd3_attempts += 1;
        stats.physical_bytes += encoded.len();
        stats.record_codec(algorithm);
    }
    stats
}

fn set_common_parameters(
    ctx: &mut StressContext,
    shape: CorpusShape,
    block_size: usize,
    logical_bytes: usize,
    role: &str,
) {
    ctx.parameter("data_shape", shape.name());
    ctx.parameter("block_size", block_size);
    ctx.parameter("corpus_blocks", CORPUS_BLOCKS);
    ctx.parameter("corpus_repeats", CORPUS_REPEATS);
    ctx.parameter("logical_bytes", logical_bytes);
    ctx.parameter("logical_unit", "block_byte");
    ctx.parameter("comparison_role", role);
    ctx.metadata("trust_class", "diagnostic");
    ctx.metadata("diagnostic_reason", "adaptive_cascade_failed_promotion");
}

fn record_stats(
    ctx: &mut StressContext,
    stats: &SelectionStats,
    oracle_physical_bytes: usize,
    logical_bytes: usize,
) {
    let byte_regret = stats.physical_bytes.saturating_sub(oracle_physical_bytes);
    let logical_regret_bps = byte_regret.saturating_mul(10_000) / logical_bytes;
    let oracle_block_footprint_overhead_bps = stats
        .physical_bytes
        .saturating_sub(oracle_physical_bytes)
        .saturating_mul(10_000)
        / oracle_physical_bytes;
    ctx.parameter("physical_bytes", stats.physical_bytes);
    ctx.parameter("codec_counts", stats.codec_counts());
    ctx.parameter("attempts_lz4", stats.lz4_attempts);
    ctx.parameter("attempts_zstd3", stats.zstd3_attempts);
    ctx.parameter("attempts_total", stats.lz4_attempts + stats.zstd3_attempts);
    ctx.parameter("oracle_physical_bytes", oracle_physical_bytes);
    ctx.parameter("oracle_byte_regret", byte_regret);
    ctx.parameter("logical_regret_bps", logical_regret_bps);
    ctx.parameter(
        "oracle_block_footprint_overhead_bps",
        oracle_block_footprint_overhead_bps,
    );
}

fn measure_candidate(
    ctx: &mut StressContext,
    shape: CorpusShape,
    block_size: usize,
    blocks: &[Vec<u8>],
    stats: &SelectionStats,
    oracle: &SelectionStats,
    fast_accept_ratio: f32,
) {
    let logical_bytes = blocks.iter().map(Vec::len).sum::<usize>();
    set_common_parameters(ctx, shape, block_size, logical_bytes, "candidate");
    ctx.parameter("fast_accept_ratio", format!("{fast_accept_ratio:.2}"));
    let row = format!("adaptive_candidate_{}_{}k", shape.name(), block_size / 1024);
    let _completed = ctx
        .benchmark(row)
        .samples(MEASURED_SAMPLES)
        .warmup(WARMUP_SAMPLES)
        .measure_batch(
            u64::try_from(logical_bytes * CORPUS_REPEATS).expect("logical corpus bytes fit in u64"),
            || {
                let mut physical_bytes = 0_usize;
                for _ in 0..CORPUS_REPEATS {
                    for block in blocks {
                        let (encoded, _, _) =
                            ordered_cascade_candidate(black_box(block), fast_accept_ratio);
                        physical_bytes = physical_bytes.wrapping_add(encoded.len());
                    }
                }
                black_box(physical_bytes);
            },
        );
    record_stats(ctx, stats, oracle.physical_bytes, logical_bytes);
}

fn measure_oracle(
    ctx: &mut StressContext,
    shape: CorpusShape,
    block_size: usize,
    blocks: &[Vec<u8>],
    stats: &SelectionStats,
) {
    let logical_bytes = blocks.iter().map(Vec::len).sum::<usize>();
    set_common_parameters(ctx, shape, block_size, logical_bytes, "exhaustive_oracle");
    let row = format!("adaptive_oracle_{}_{}k", shape.name(), block_size / 1024);
    let _completed = ctx
        .benchmark(row)
        .samples(MEASURED_SAMPLES)
        .warmup(WARMUP_SAMPLES)
        .measure_batch(
            u64::try_from(logical_bytes * CORPUS_REPEATS).expect("logical corpus bytes fit in u64"),
            || {
                let mut physical_bytes = 0_usize;
                for _ in 0..CORPUS_REPEATS {
                    for block in blocks {
                        let (encoded, _) = exhaustive_oracle(black_box(block));
                        physical_bytes = physical_bytes.wrapping_add(encoded.len());
                    }
                }
                black_box(physical_bytes);
            },
        );
    record_stats(ctx, stats, stats.physical_bytes, logical_bytes);
}

pub fn run_case(
    ctx: &mut StressContext,
    block_size: usize,
    shape: CorpusShape,
    role: SelectionRole,
    fast_accept_ratio: f32,
) {
    let blocks = corpus(shape, block_size);
    let candidate = candidate_stats(&blocks, fast_accept_ratio);
    let oracle = oracle_stats(&blocks);
    match role {
        SelectionRole::Candidate => {
            measure_candidate(
                ctx,
                shape,
                block_size,
                &blocks,
                &candidate,
                &oracle,
                fast_accept_ratio,
            );
        }
        SelectionRole::Oracle => {
            measure_oracle(ctx, shape, block_size, &blocks, &oracle);
        }
    }
}

pub fn run_threshold_screen(ctx: &mut StressContext, fast_accept_ratio: f32) {
    let cases: Vec<_> = SCREEN_BLOCK_SIZES
        .into_iter()
        .flat_map(|block_size| {
            SCREEN_SHAPES
                .into_iter()
                .map(move |shape| (shape, block_size, corpus(shape, block_size)))
        })
        .collect();
    let mut candidate_total = SelectionStats::default();
    let mut oracle_total = SelectionStats::default();
    let mut logical_bytes = 0_usize;
    let mut storage_gate_passed = true;
    let mut workload_regret = Vec::with_capacity(cases.len());

    for (shape, block_size, blocks) in &cases {
        let candidate = candidate_stats(blocks, fast_accept_ratio);
        let oracle = oracle_stats(blocks);
        let case_logical_bytes = blocks.iter().map(Vec::len).sum::<usize>();
        let regret = candidate
            .physical_bytes
            .saturating_sub(oracle.physical_bytes);
        storage_gate_passed &= regret.saturating_mul(200) <= case_logical_bytes;
        workload_regret.push(format!(
            "{}_{}k={}/{}/{}",
            shape.name(),
            block_size / 1024,
            case_logical_bytes,
            candidate.physical_bytes,
            regret
        ));
        candidate_total.add_assign(&candidate);
        oracle_total.add_assign(&oracle);
        logical_bytes += case_logical_bytes;
    }

    ctx.parameter("comparison_role", "threshold_screen");
    ctx.parameter("fast_accept_ratio", format!("{fast_accept_ratio:.2}"));
    ctx.parameter("logical_bytes", logical_bytes);
    ctx.parameter("logical_unit", "block_byte");
    ctx.parameter("storage_gate_passed", storage_gate_passed);
    ctx.parameter(
        "workload_logical_physical_regret",
        workload_regret.join(","),
    );
    ctx.metadata("trust_class", "diagnostic");
    ctx.metadata("diagnostic_reason", "adaptive_cascade_threshold_screen");

    let row = format!("adaptive_threshold_screen_{:.0}", fast_accept_ratio * 100.0);
    let _completed = ctx
        .benchmark(row)
        .samples(MEASURED_SAMPLES)
        .warmup(WARMUP_SAMPLES)
        .measure_batch(
            u64::try_from(logical_bytes * SCREEN_CORPUS_REPEATS)
                .expect("screen logical bytes fit in u64"),
            || {
                let mut physical_bytes = 0_usize;
                for _ in 0..SCREEN_CORPUS_REPEATS {
                    for (_, _, blocks) in &cases {
                        for block in blocks {
                            let (encoded, _, _) =
                                ordered_cascade_candidate(black_box(block), fast_accept_ratio);
                            physical_bytes = physical_bytes.wrapping_add(encoded.len());
                        }
                    }
                }
                black_box(physical_bytes);
            },
        );
    record_stats(
        ctx,
        &candidate_total,
        oracle_total.physical_bytes,
        logical_bytes,
    );
}
