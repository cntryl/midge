//! Tier 4 - production-default grandparent overlap compaction workload.
//!
//! Setup compacts a deterministic 371 MiB logical dataset into the default
//! L2 target. The measured phase overwrites a fixed uniform-random sample,
//! flushes, and compacts it. The output-byte metric is the compaction output
//! counter delta only; it excludes flush bytes and is not total physical write
//! amplification.

use cntryl_midge::__internal::diagnostics::compaction_grandparent_overlap_roll_count;
use cntryl_midge::{
    ColumnFamilyHandle, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::path::PathBuf;
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_64_with_seed;

const BENCHMARK: &str = "tier4_system_compaction_grandparent_cuts";
const SEED: u64 = 0x9480_2026_1006;
const BASE_KEYS: usize = 2_700_000;
const UPDATE_KEYS: usize = 550_000;
const KEY_BYTES: usize = 16;
const VALUE_BYTES: usize = 128;
const LOGICAL_BYTES_PER_KEY: u64 = (KEY_BYTES + VALUE_BYTES) as u64;
const WRITE_BATCH: usize = 2_000;
const MIN_GRANDPARENT_OVERLAP_BYTES: u64 = 320 * 1024 * 1024;
const WRITE_STALL_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_SETUP_COMPACTION_PASSES: usize = 8;
const SETUP_COMPACTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

fn data_path() -> PathBuf {
    let root = PathBuf::from("target/tmp");
    std::fs::create_dir_all(&root).expect("create benchmark temp directory");
    root.join(format!("{BENCHMARK}-{}-{SEED}", std::process::id()))
}

fn key(id: u64) -> [u8; KEY_BYTES] {
    let mut key = [0; KEY_BYTES];
    key[..8].copy_from_slice(b"midge948");
    key[8..].copy_from_slice(&id.to_be_bytes());
    key
}

fn value(id: u64, generation: u64) -> [u8; VALUE_BYTES] {
    let mut value = [0; VALUE_BYTES];
    let mut state = id ^ generation.rotate_left(23) ^ SEED;
    for chunk in value.as_chunks_mut::<8>().0 {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut word = state;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        *chunk = (word ^ (word >> 31)).to_le_bytes();
    }
    value
}

fn shuffled_ids() -> Vec<u32> {
    shuffled_id_range(BASE_KEYS)
}

fn shuffled_id_range(count: usize) -> Vec<u32> {
    let mut ids: Vec<u32> = (0..count)
        .map(|id| u32::try_from(id).expect("benchmark key id fits in u32"))
        .collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
    ids.shuffle(&mut rng);
    ids
}

fn write_keys(engine: &Engine, cf_id: u32, ids: &[u32], generation: u64) {
    for batch in ids.chunks(WRITE_BATCH) {
        loop {
            let mut transaction = engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin benchmark write batch");
            for id in batch {
                let id = u64::from(*id);
                transaction
                    .put(key(id).to_vec(), value(id, generation).to_vec(), None)
                    .expect("stage benchmark value");
            }
            match transaction.commit(WriteOptions::buffered()) {
                Ok(()) => break,
                Err(MidgeError::WriteStall(_)) => {
                    engine
                        .wait_for_write_stall_clear(cf_id, WRITE_STALL_WAIT)
                        .expect("wait for production write stall to clear");
                }
                Err(error) => panic!("commit benchmark write batch: {error}"),
            }
        }
    }
}

fn layout_fingerprint(engine: &Engine) -> Vec<(u32, u32, String, u64)> {
    let mut files: Vec<_> = engine
        .metrics()
        .get_storage_layout()
        .expect("read benchmark storage layout")
        .levels
        .into_iter()
        .flat_map(|level| {
            level
                .files
                .into_iter()
                .map(move |file| (level.level, file.cf_id, file.name, file.size_bytes))
        })
        .collect();
    files.sort_unstable();
    files
}

fn describe_layout(engine: &Engine) -> String {
    let mut files: Vec<_> = engine
        .metrics()
        .get_storage_layout()
        .expect("read benchmark storage layout")
        .levels
        .into_iter()
        .flat_map(|level| {
            level.files.into_iter().map(move |file| {
                (
                    level.level,
                    file.cf_id,
                    file.name,
                    file.size_bytes,
                    file.smallest_key,
                    file.largest_key,
                )
            })
        })
        .collect();
    files.sort_unstable_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.4.cmp(&right.4))
            .then_with(|| left.5.cmp(&right.5))
            .then_with(|| left.2.cmp(&right.2))
    });
    files
        .into_iter()
        .map(|(level, cf_id, name, bytes, smallest, largest)| {
            format!(
                "L{level}:CF{cf_id}:{name}:{bytes}:{}:{}",
                smallest.as_deref().map(hex::encode).unwrap_or_default(),
                largest.as_deref().map(hex::encode).unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn compact_to_stable_layout(engine: &Engine) {
    let deadline = Instant::now() + SETUP_COMPACTION_TIMEOUT;
    let mut compaction_passes = 0;
    loop {
        let before = layout_fingerprint(engine);
        engine.compact_all().expect("compact benchmark data");
        let after = layout_fingerprint(engine);
        let runtime = engine
            .metrics()
            .get_runtime_metrics()
            .expect("read benchmark runtime metrics");
        let layout_is_idle = runtime.active_compactions == 0
            && runtime.compacting_ssts == 0
            && runtime.flush_inflight == 0
            && runtime.flush_queue_depth == 0;
        if before == after && layout_is_idle {
            assert!(
                after.iter().all(|(level, _, _, _)| *level != 0),
                "stable benchmark layout must not retain L0 files"
            );
            return;
        }
        if before != after {
            compaction_passes += 1;
            assert!(
                compaction_passes <= MAX_SETUP_COMPACTION_PASSES,
                "benchmark storage layout did not stabilize after compaction passes"
            );
        }
        assert!(
            Instant::now() < deadline,
            "benchmark compaction did not become idle before the setup timeout"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[derive(Debug, Default)]
struct L2Overlap {
    bytes: u64,
    files: usize,
    largest_file_bytes: u64,
}

fn overlapping_l2(engine: &Engine, cf_id: u32, lower: &[u8], upper: &[u8]) -> L2Overlap {
    engine
        .metrics()
        .get_storage_layout()
        .expect("read benchmark storage layout")
        .levels
        .into_iter()
        .filter(|level| level.level == 2)
        .flat_map(|level| level.files)
        .filter(|file| {
            file.cf_id == cf_id
                && file
                    .smallest_key
                    .as_deref()
                    .is_some_and(|smallest| smallest <= upper)
                && file
                    .largest_key
                    .as_deref()
                    .is_some_and(|largest| largest >= lower)
        })
        .fold(L2Overlap::default(), |mut overlap, file| {
            overlap.bytes = overlap.bytes.saturating_add(file.size_bytes);
            overlap.files = overlap.files.saturating_add(1);
            overlap.largest_file_bytes = overlap.largest_file_bytes.max(file.size_bytes);
            overlap
        })
}

fn prepare_fixture(engine: &Engine, cf: &ColumnFamilyHandle) -> (Vec<u32>, L2Overlap, String) {
    let ids = shuffled_ids();

    // Populate and compact the base outside the measured window.
    write_keys(engine, cf.id(), &ids, 0);
    engine.flush_cf(cf).expect("flush base dataset");
    compact_to_stable_layout(engine);

    let update_ids = &ids[..UPDATE_KEYS];
    let lower = key(u64::from(
        *update_ids.iter().min().expect("nonempty updates"),
    ));
    let upper = key(u64::from(
        *update_ids.iter().max().expect("nonempty updates"),
    ));
    let l2_overlap = overlapping_l2(engine, cf.id(), &lower, &upper);
    assert!(
        l2_overlap.bytes > MIN_GRANDPARENT_OVERLAP_BYTES
            && l2_overlap.files >= 2
            && l2_overlap.largest_file_bytes < MIN_GRANDPARENT_OVERLAP_BYTES,
        "workload invalid: update span must overlap more than 320 MiB across at least two individually smaller L2 files; observed {l2_overlap:?}"
    );
    let layout = describe_layout(engine);
    (ids, l2_overlap, layout)
}

fn measure_updates(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    update_ids: &[u32],
) -> (std::time::Duration, u64, u64, usize) {
    let metrics_before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("read metrics before measured compaction");
    let measured_started = Instant::now();

    write_keys(engine, cf.id(), update_ids, 1);
    engine.flush_cf(cf).expect("flush measured updates");
    compact_to_stable_layout(engine);

    let elapsed = measured_started.elapsed();
    let metrics_after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("read metrics after measured compaction");
    let compaction_output_bytes = metrics_after
        .compaction_bytes_rewritten
        .saturating_sub(metrics_before.compaction_bytes_rewritten);
    let compactions_run = metrics_after
        .compactions_run
        .saturating_sub(metrics_before.compactions_run);
    (
        elapsed,
        compaction_output_bytes,
        compactions_run,
        metrics_after.sst_count,
    )
}

fn u64_as_f64(value: u64) -> f64 {
    let upper = u32::try_from(value >> u32::BITS).expect("upper half fits in u32");
    let lower = u32::try_from(value & u64::from(u32::MAX)).expect("lower half fits in u32");
    f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
}

fn usize_as_f64(value: usize) -> f64 {
    u64_as_f64(u64::try_from(value).expect("benchmark observation fits in u64"))
}

fn record_measurement(
    ctx: &mut StressContext,
    l2_overlap: &L2Overlap,
    elapsed: std::time::Duration,
    compaction_output_bytes: u64,
    compactions_run: u64,
    final_sst_count: usize,
    initial_layout: &str,
) {
    let logical_input_bytes = u64::try_from(UPDATE_KEYS)
        .expect("update count fits in u64")
        .saturating_mul(LOGICAL_BYTES_PER_KEY);
    let output_bytes_per_input_byte =
        u64_as_f64(compaction_output_bytes) / u64_as_f64(logical_input_bytes);

    ctx.parameter("storage", "local");
    ctx.parameter("production_defaults", "true");
    ctx.parameter("seed", SEED);
    ctx.parameter("base_keys", BASE_KEYS);
    ctx.parameter("updated_keys", UPDATE_KEYS);
    ctx.parameter("key_bytes", KEY_BYTES);
    ctx.parameter("value_bytes", VALUE_BYTES);
    ctx.parameter("write_batch_keys", WRITE_BATCH);
    ctx.parameter(
        "level2_grandparent_limit_bytes",
        MIN_GRANDPARENT_OVERLAP_BYTES,
    );
    ctx.record_observation(
        "pre_measurement_l2_overlap_bytes",
        u64_as_f64(l2_overlap.bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "pre_measurement_l2_overlap_files",
        usize_as_f64(l2_overlap.files),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "largest_pre_measurement_l2_overlap_file_bytes",
        u64_as_f64(l2_overlap.largest_file_bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
    ctx.parameter("compaction_metric", "published compaction output SST bytes");
    ctx.parameter("flush_bytes_in_compaction_metric", "false");
    let layout_fingerprint = xxh3_64_with_seed(initial_layout.as_bytes(), SEED);
    ctx.record_observation(
        "pre_measurement_layout_fingerprint_high",
        f64::from(u32::try_from(layout_fingerprint >> u32::BITS).expect("upper half fits in u32")),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "pre_measurement_layout_fingerprint_low",
        f64::from(
            u32::try_from(layout_fingerprint & u64::from(u32::MAX))
                .expect("lower half fits in u32"),
        ),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    eprintln!("midge948_pre_measurement_layout={initial_layout}");
    ctx.record_external_outcome(
        "sustained_write_throughput_under_compaction",
        elapsed,
        LogicalUnit::new("logical_input_byte"),
        OperationOutcome::success(logical_input_bytes),
    );
    ctx.record_observation(
        "compaction_output_bytes",
        u64_as_f64(compaction_output_bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "compaction_output_bytes_per_logical_input_byte",
        output_bytes_per_input_byte,
        ObservationUnit::Ratio,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "measured_compactions_run",
        u64_as_f64(compactions_run),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "final_sst_count",
        usize_as_f64(final_sst_count),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
}

fn run(ctx: &mut StressContext) {
    let path = data_path();
    if path.exists() {
        std::fs::remove_dir_all(&path).expect("remove previous benchmark database");
    }
    let mut engine = Engine::open(
        OpenOptions::local(&path)
            .build()
            .expect("build default options"),
    )
    .expect("open local benchmark engine with default production settings");
    let cf = engine
        .create_column_family("grandparent-cuts")
        .expect("create benchmark column family");
    let (ids, l2_overlap, initial_layout) = prepare_fixture(&engine, &cf);
    let setup_rolls = compaction_grandparent_overlap_roll_count(&engine);
    let (elapsed, compaction_output_bytes, compactions_run, final_sst_count) =
        measure_updates(&engine, &cf, &ids[..UPDATE_KEYS]);
    let measured_rolls =
        compaction_grandparent_overlap_roll_count(&engine).saturating_sub(setup_rolls);
    for (name, rolls) in [
        ("setup_grandparent_overlap_rolls", setup_rolls),
        ("measured_grandparent_overlap_rolls", measured_rolls),
    ] {
        ctx.record_observation(
            name,
            u64_as_f64(rolls),
            ObservationUnit::Count,
            ObservationDirection::Informational,
        );
    }
    record_measurement(
        ctx,
        &l2_overlap,
        elapsed,
        compaction_output_bytes,
        compactions_run,
        final_sst_count,
        &initial_layout,
    );
    engine
        .shutdown(std::time::Duration::from_secs(60))
        .expect("shutdown compaction benchmark engine");
    drop(engine);
    std::fs::remove_dir_all(path).expect("remove benchmark database");
}

#[stress(
    tier = 4,
    metadata(
        component = "engine_compaction",
        scenario = "grandparent_output_cuts",
        measurement_shape = "fixed_workload"
    )
)]
fn tier4_compaction_grandparent_cuts(ctx: &mut StressContext) {
    run(ctx);
}

/// Three-level fixture: small level targets so L1, L2 and L3 are all populated.
///
/// The SST size stays at the production default (128 MiB for the default
/// goal); it is only tunable through the `failpoints` feature. The levels are
/// therefore scaled instead: L1 64 MiB, L2 256 MiB, L3 1 GiB. The base dataset
/// fills L1 and L2 and spills the remainder into L3.
mod three_level {
    use super::{
        compact_to_stable_layout, describe_layout, key, u64_as_f64, write_keys, ColumnFamilyHandle,
        Engine, Instant, StressContext, TransactionMode, KEY_BYTES, SEED, VALUE_BYTES, WRITE_BATCH,
    };
    use cntryl_midge::CompactionLevelConfig;

    pub const BASE_KEYS_3L: usize = 3_500_000;
    pub const ROUNDS: usize = 8;
    pub const KEYS_PER_ROUND: usize = 300_000;
    pub const TOP_UP_KEYS: usize = 150_000;
    pub const READ_SAMPLE_KEYS: usize = 20_000;
    pub const L1_TARGET_BYTES: u64 = 64 * 1024 * 1024;
    pub const LEVEL_MULTIPLIER: u64 = 4;
    pub const MAX_LEVELS: usize = 7;
    /// 1.5x the production-default 128 MiB SST. Any two adjacent L3 files exceed
    /// it, so an output may overlap at most one full grandparent file; a
    /// 10x-file-size limit would exceed all of L3 and never fire.
    pub const POLICY_ON_OVERLAP_BYTES: u64 = 192 * 1024 * 1024;

    pub fn levels(max_grandparent_overlap_bytes: u64) -> CompactionLevelConfig {
        CompactionLevelConfig {
            l1_target_bytes: L1_TARGET_BYTES,
            level_multiplier: LEVEL_MULTIPLIER,
            max_levels: MAX_LEVELS,
            max_grandparent_overlap_bytes,
        }
    }

    /// Files and bytes at `level`; zero when the level is empty.
    pub fn level_shape(engine: &Engine, level: u32) -> (usize, u64) {
        engine
            .metrics()
            .get_storage_layout()
            .expect("read benchmark storage layout")
            .levels
            .iter()
            .filter(|entry| entry.level == level)
            .fold((0, 0), |(files, bytes), entry| {
                (files + entry.file_count, bytes + entry.total_bytes)
            })
    }

    pub fn assert_three_inner_levels(engine: &Engine) {
        for level in 1..=3 {
            let (files, _) = level_shape(engine, level);
            assert!(
                files >= 1,
                "workload invalid: L{level} holds no SSTs after setup: {}",
                describe_layout(engine)
            );
        }
    }

    pub struct Measured {
        pub elapsed: std::time::Duration,
        pub output_bytes: u64,
        pub compactions_run: u64,
        pub final_sst_count: usize,
    }

    pub fn measure_rounds(engine: &Engine, cf: &ColumnFamilyHandle, ids: &[u32]) -> Measured {
        let before = engine
            .metrics()
            .get_runtime_metrics()
            .expect("read metrics before measured rounds");
        let started = Instant::now();
        for round in 0..ROUNDS {
            let slice = &ids[round * KEYS_PER_ROUND..(round + 1) * KEYS_PER_ROUND];
            write_keys(
                engine,
                cf.id(),
                slice,
                1 + u64::try_from(round).expect("round fits in u64"),
            );
            engine.flush_cf(cf).expect("flush measured round");
            compact_to_stable_layout(engine);
        }

        let elapsed = started.elapsed();
        let after = engine
            .metrics()
            .get_runtime_metrics()
            .expect("read metrics after measured rounds");
        Measured {
            elapsed,
            output_bytes: after
                .compaction_bytes_rewritten
                .saturating_sub(before.compaction_bytes_rewritten),
            compactions_run: after.compactions_run.saturating_sub(before.compactions_run),
            final_sst_count: after.sst_count,
        }
    }

    /// Average SSTs consulted per point read over a fixed key sample.
    pub fn read_ssts_per_point_read(engine: &Engine, cf: &ColumnFamilyHandle, ids: &[u32]) -> f64 {
        let before = engine
            .metrics()
            .get_read_amp_metrics()
            .expect("read amplification metrics before reads");
        let transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read-only transaction");
        for id in ids.iter().step_by(ids.len() / READ_SAMPLE_KEYS).copied() {
            let found = transaction
                .get(&key(u64::from(id)))
                .expect("point read benchmark key");
            assert!(found.is_some(), "benchmark key {id} must exist");
        }
        drop(transaction);
        let after = engine
            .metrics()
            .get_read_amp_metrics()
            .expect("read amplification metrics after reads");
        let reads = after.reads_total.saturating_sub(before.reads_total);
        assert!(reads > 0, "read amplification metrics recorded no reads");
        u64_as_f64(
            after
                .ssts_touched_total
                .saturating_sub(before.ssts_touched_total),
        ) / u64_as_f64(reads)
    }

    pub fn record_parameters(ctx: &mut StressContext, overlap_limit: u64) {
        ctx.parameter("storage", "local");
        ctx.parameter("seed", SEED);
        ctx.parameter("base_keys", BASE_KEYS_3L);
        ctx.parameter("measured_rounds", ROUNDS);
        ctx.parameter("updated_keys_per_round", KEYS_PER_ROUND);
        ctx.parameter("key_bytes", KEY_BYTES);
        ctx.parameter("value_bytes", VALUE_BYTES);
        ctx.parameter("write_batch_keys", WRITE_BATCH);
        ctx.parameter("read_sample_keys", READ_SAMPLE_KEYS);
        ctx.parameter("l1_target_bytes", L1_TARGET_BYTES);
        ctx.parameter("level_multiplier", LEVEL_MULTIPLIER);
        ctx.parameter("max_levels", MAX_LEVELS);
        ctx.parameter("max_grandparent_overlap_bytes", overlap_limit);
        ctx.parameter(
            "compaction_during_measurement",
            "compact_all_after_each_round",
        );
        ctx.parameter("compaction_metric", "published compaction output SST bytes");
        ctx.parameter("flush_bytes_in_compaction_metric", "false");
    }
}

fn record_obs(ctx: &mut StressContext, name: &str, value: f64, unit: ObservationUnit) {
    ctx.record_observation(name, value, unit, ObservationDirection::Informational);
}

fn record_three_level(
    ctx: &mut StressContext,
    engine: &Engine,
    measured: &three_level::Measured,
    rolls: (u64, u64),
    read_ssts_per_read: f64,
    initial_layout: &str,
) {
    let logical_input_bytes = u64::try_from(three_level::ROUNDS * three_level::KEYS_PER_ROUND)
        .expect("update count fits in u64")
        .saturating_mul(LOGICAL_BYTES_PER_KEY);
    ctx.record_external_outcome(
        "sustained_write_throughput_under_compaction",
        measured.elapsed,
        LogicalUnit::new("logical_input_byte"),
        OperationOutcome::success(logical_input_bytes),
    );
    let bytes = ObservationUnit::Bytes;
    let count = ObservationUnit::Count;
    record_obs(
        ctx,
        "compaction_output_bytes",
        u64_as_f64(measured.output_bytes),
        bytes,
    );
    record_obs(
        ctx,
        "compaction_output_bytes_per_logical_input_byte",
        u64_as_f64(measured.output_bytes) / u64_as_f64(logical_input_bytes),
        ObservationUnit::Ratio,
    );
    record_obs(
        ctx,
        "measured_compactions_run",
        u64_as_f64(measured.compactions_run),
        count,
    );
    record_obs(
        ctx,
        "setup_grandparent_overlap_rolls",
        u64_as_f64(rolls.0),
        count,
    );
    record_obs(
        ctx,
        "measured_grandparent_overlap_rolls",
        u64_as_f64(rolls.1),
        count,
    );
    record_obs(
        ctx,
        "final_sst_count",
        usize_as_f64(measured.final_sst_count),
        count,
    );
    record_obs(
        ctx,
        "read_ssts_touched_per_point_read",
        read_ssts_per_read,
        count,
    );
    for (level, name) in [(0, "l0"), (1, "l1"), (2, "l2"), (3, "l3")] {
        let (files, level_bytes) = three_level::level_shape(engine, level);
        record_obs(
            ctx,
            &format!("final_{name}_files"),
            usize_as_f64(files),
            count,
        );
        record_obs(
            ctx,
            &format!("final_{name}_bytes"),
            u64_as_f64(level_bytes),
            bytes,
        );
    }
    let fingerprint = xxh3_64_with_seed(initial_layout.as_bytes(), SEED);
    for (name, half) in [
        (
            "pre_measurement_layout_fingerprint_high",
            fingerprint >> u32::BITS,
        ),
        (
            "pre_measurement_layout_fingerprint_low",
            fingerprint & u64::from(u32::MAX),
        ),
    ] {
        let half = u32::try_from(half).expect("fingerprint half fits in u32");
        record_obs(ctx, name, f64::from(half), count);
    }
    eprintln!("midge948_three_level_pre_measurement_layout={initial_layout}");
}

const THREE_LEVEL_CF: &str = "grandparent-cuts-three-level";

fn open_three_level(path: &std::path::Path, overlap_limit: u64) -> Engine {
    Engine::open(
        OpenOptions::local(path)
            .background_compaction(false)
            .compaction_levels(three_level::levels(overlap_limit))
            .build()
            .expect("build three-level options"),
    )
    .expect("open local three-level benchmark engine")
}

/// Builds the shared starting layout under a zero overlap limit, so both rows
/// begin from identical files. Returns the shuffled ids, the setup roll count
/// and the layout description.
fn build_three_level_fixture(path: &std::path::Path) -> (Vec<u32>, u64, String) {
    let mut engine = open_three_level(path, 0);
    let cf = engine
        .create_column_family(THREE_LEVEL_CF)
        .expect("create benchmark column family");
    let ids = shuffled_id_range(three_level::BASE_KEYS_3L);
    write_keys(&engine, cf.id(), &ids, 0);
    engine.flush_cf(&cf).expect("flush base dataset");
    compact_to_stable_layout(&engine);
    // A small top-up of keys the measured rounds never touch leaves a partly
    // filled L1 behind, so all three inner levels hold data at the start.
    let top_up = &ids[ids.len() - three_level::TOP_UP_KEYS..];
    write_keys(&engine, cf.id(), top_up, 100);
    engine.flush_cf(&cf).expect("flush top-up dataset");
    compact_to_stable_layout(&engine);
    three_level::assert_three_inner_levels(&engine);
    let layout = describe_layout(&engine);
    let rolls = compaction_grandparent_overlap_roll_count(&engine);
    engine
        .shutdown(std::time::Duration::from_secs(60))
        .expect("shutdown fixture engine");
    drop(engine);
    (ids, rolls, layout)
}

fn run_three_level(ctx: &mut StressContext, overlap_limit: u64) {
    let path = data_path();
    if path.exists() {
        std::fs::remove_dir_all(&path).expect("remove previous benchmark database");
    }
    let (ids, setup_rolls, built_layout) = build_three_level_fixture(&path);
    let mut engine = open_three_level(&path, overlap_limit);
    let cf = engine
        .get_column_family(THREE_LEVEL_CF)
        .expect("reopened benchmark column family");
    let initial_layout = describe_layout(&engine);
    if initial_layout != built_layout {
        eprintln!("midge948_three_level_reopen_changed_layout=true");
    }
    let rolls_before = compaction_grandparent_overlap_roll_count(&engine);
    let measured = three_level::measure_rounds(&engine, &cf, &ids);
    let measured_rolls =
        compaction_grandparent_overlap_roll_count(&engine).saturating_sub(rolls_before);
    if overlap_limit > 0 {
        assert!(
            measured_rolls > 0,
            "policy-on row must roll outputs at grandparent overlap limits"
        );
    } else {
        assert_eq!(
            measured_rolls, 0,
            "policy-off row must never roll on overlap"
        );
    }
    let read_ssts = three_level::read_ssts_per_point_read(&engine, &cf, &ids);
    three_level::record_parameters(ctx, overlap_limit);
    record_three_level(
        ctx,
        &engine,
        &measured,
        (setup_rolls, measured_rolls),
        read_ssts,
        &initial_layout,
    );
    engine
        .shutdown(std::time::Duration::from_secs(60))
        .expect("shutdown compaction benchmark engine");
    drop(engine);
    std::fs::remove_dir_all(path).expect("remove benchmark database");
}

#[stress(
    tier = 4,
    metadata(
        component = "engine_compaction",
        scenario = "grandparent_output_cuts_three_level_policy_on",
        measurement_shape = "fixed_workload"
    )
)]
fn tier4_compaction_grandparent_cuts_three_level_policy_on(ctx: &mut StressContext) {
    run_three_level(ctx, three_level::POLICY_ON_OVERLAP_BYTES);
}

#[stress(
    tier = 4,
    metadata(
        component = "engine_compaction",
        scenario = "grandparent_output_cuts_three_level_policy_off",
        measurement_shape = "fixed_workload"
    )
)]
fn tier4_compaction_grandparent_cuts_three_level_policy_off(ctx: &mut StressContext) {
    run_three_level(ctx, 0);
}

stress_main!();
