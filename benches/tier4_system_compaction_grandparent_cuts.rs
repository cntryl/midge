//! Tier 4 - production-default grandparent overlap compaction workload.
//!
//! Setup compacts a deterministic 371 MiB logical dataset into the default
//! L2 target. The measured phase overwrites a fixed uniform-random sample,
//! flushes, and compacts it. The output-byte metric is the compaction output
//! counter delta only; it excludes flush bytes and is not total physical write
//! amplification.

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
    let mut ids: Vec<u32> = (0..BASE_KEYS)
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

fn compact_to_stable_layout(engine: &Engine) {
    for _ in 0..MAX_SETUP_COMPACTION_PASSES {
        let before = layout_fingerprint(engine);
        engine.compact_all().expect("compact benchmark data");
        let after = layout_fingerprint(engine);
        if before == after {
            assert!(
                after.iter().all(|(level, _, _, _)| *level != 0),
                "stable benchmark layout must not retain L0 files"
            );
            return;
        }
    }
    panic!("benchmark storage layout did not stabilize after compaction passes");
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

fn prepare_fixture(engine: &Engine, cf: &ColumnFamilyHandle) -> (Vec<u32>, L2Overlap) {
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
    (ids, l2_overlap)
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
    let (ids, l2_overlap) = prepare_fixture(&engine, &cf);
    let (elapsed, compaction_output_bytes, compactions_run, final_sst_count) =
        measure_updates(&engine, &cf, &ids[..UPDATE_KEYS]);
    record_measurement(
        ctx,
        &l2_overlap,
        elapsed,
        compaction_output_bytes,
        compactions_run,
        final_sst_count,
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

stress_main!();
