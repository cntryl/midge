//! Tier 4 — Recovery & Reopen Behavior
//!
//! Measures: repeated engine reopen throughput after state-dependent lifecycle events.
//! NOT: single primitive cost (Tier 3)
//!
//! Tier 4 OWNS:
//! - Reopen after flush (manifest replay cost)
//! - Reopen after compaction (multi-level state complexity)
//! - Replay of unflushed WAL records into fresh memtables
//! - State-dependent recovery cost (recovery depends on manifests, WALs, levels)
//!
//! NOT measured:
//! - Clean reopen (Tier 3: `tier3_system_lifecycle.rs`)

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
use std::time::{Duration, Instant};

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use cntryl_midge_core::io::{FsPath, RealFs};
use cntryl_midge_durability::wal::{
    ledger_store::LedgerStore,
    recovery::{
        streaming::{replay_wal_with_options, ReplayOptions, StreamingReplayLimits},
        ReplayPolicy, ReplaySegments,
    },
    replay::ReplaySelection,
    segment_file_name,
};
use rand::{RngExt, SeedableRng};
use stress_config::MidgeOptions;

const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;
const RECOVERY_FIXTURE_MEMTABLE_SIZE_BYTES: usize = 2 * 1024 * 1024;
const REOPENS_PER_SAMPLE: u64 = 10;
const WAL_REPLAY_BATCHES: u64 = 32;
const WAL_REPLAY_WRITES_PER_BATCH: u64 = 512;
const WAL_REPLAY_VALUE_BYTES: usize = 1024;
const WAL_REPLAYS_PER_SAMPLE: u64 = 8;
const COLD_START_INVENTORY_ENTRIES: usize = 10_000;
const COLD_START_MOCK_HEAD_DELAY: Duration = Duration::from_millis(1);

fn setup_engine(opts: MidgeOptions) -> Engine {
    stress_config::bench_stress::open_engine_no_compaction(opts)
}

fn recovery_opts_for_mode(mode: &str) -> MidgeOptions {
    let mut opts = stress_config::opts_for_mode(mode);
    // Recovery rows create their durable state at explicit flush boundaries.
    // Keep each setup batch in one memtable instead of inheriting the tiny
    // generic local profile and stalling before the explicit flush.
    opts.memtable_size = opts.memtable_size.max(RECOVERY_FIXTURE_MEMTABLE_SIZE_BYTES);
    opts
}

fn write_some(engine: &Engine, num_keys: usize) {
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::best_effort(); // Fast setup: skip WAL I/O
    let total = num_keys;

    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in start..end {
            let k = stress_config::bench_stress::key16_u64_be(i as u64);
            let v = vec![u8::try_from(i % 251).expect("value byte fits in u8"); VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
}

fn measure_reopens(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: &MidgeOptions,
    failure_context: &'static str,
) {
    stress_config::mark_capped_probe(ctx, "ten_reopen_recovery_throughput_probe");
    stress_config::measure_tier4_cycles(ctx, scenario, "engine_reopen", || {
        let started_at = std::time::Instant::now();
        for _ in 0..REOPENS_PER_SAMPLE {
            let mut engine = setup_engine(opts.clone());
            engine
                .shutdown(Duration::from_secs(10))
                .expect(failure_context);
        }
        (REOPENS_PER_SAMPLE, started_at.elapsed())
    });
}

fn run_reopen_after_flush_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: &MidgeOptions,
) {
    ctx.parameter("fixture_memtable_size_bytes", opts.memtable_size);

    // All setup outside measurement: create flushed state
    {
        let e = setup_engine(opts.clone());
        let cf = e.create_column_family("cf1").unwrap();
        write_some(&e, 5_000);
        e.flush_cf(&cf).unwrap(); // Ensure durability before measurement
        let mut e = e;
        e.shutdown(Duration::from_secs(10))
            .expect("prepare flushed recovery benchmark");
    }

    // Measure repeated reopen throughput under flushed manifest state.
    measure_reopens(ctx, scenario, opts, "complete flushed recovery measurement");
}

fn run_reopen_after_compaction_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: &MidgeOptions,
) {
    ctx.parameter("fixture_memtable_size_bytes", opts.memtable_size);

    // All setup outside measurement: create multi-level compacted state
    {
        let e = setup_engine(opts.clone());
        let cf = e.create_column_family("cf1").unwrap();
        write_some(&e, 3_000);
        e.flush_cf(&cf).unwrap();
        write_some(&e, 3_000);
        e.flush_cf(&cf).unwrap(); // Ensure durability before compaction
        e.compact_all().unwrap();
        let mut e = e;
        e.shutdown(Duration::from_secs(10))
            .expect("prepare compacted recovery benchmark");
    }

    // Measure repeated reopen throughput under compacted multi-level state.
    measure_reopens(
        ctx,
        scenario,
        opts,
        "complete compacted recovery measurement",
    );
}

fn run_cloud_inventory_case(ctx: &mut StressContext) {
    use cntryl_stress::{LogicalUnit, OperationOutcome};

    stress_config::mark_capped_probe(ctx, "fixed_10000_sst_inventory");
    let expected_entries = u64::try_from(COLD_START_INVENTORY_ENTRIES).expect("10,000 fits in u64");
    ctx.parameter("inventory_entries", COLD_START_INVENTORY_ENTRIES);
    ctx.parameter("provider_mock", "filesystem_object_store");
    ctx.parameter("fixture_setup_measured", false);
    ctx.parameter("mock_head_delay_ms", COLD_START_MOCK_HEAD_DELAY.as_millis());
    let directory = tempfile::tempdir().expect("create cloud inventory benchmark root");
    let fixture = cntryl_midge::__internal::recovery::prepare_inventory_progress_fixture(
        directory.path(),
        COLD_START_INVENTORY_ENTRIES,
        COLD_START_MOCK_HEAD_DELAY,
    )
    .expect("prepare 10,000-SST inventory outside measured recovery");
    let started_at = std::time::Instant::now();
    let result = fixture
        .run()
        .expect("validate 10,000-SST cloud inventory through production recovery");
    let elapsed = started_at.elapsed();
    assert_eq!(result.expected_inventory_entries, expected_entries);
    assert_eq!(result.completed_inventory_heads, expected_entries);
    assert_eq!(
        result.completed_inventory_size_validations,
        expected_entries
    );
    assert_eq!(result.retained_inventory_entries, expected_entries);
    assert_eq!(result.mismatches, 0);
    ctx.record_external_outcome(
        "tier4_recovery_cloud_inventory_10k",
        elapsed,
        LogicalUnit::new("manifest_sst"),
        OperationOutcome::success(expected_entries),
    );
}

fn prepare_unflushed_wal(root: &std::path::Path) -> u32 {
    let options = OpenOptions::local(root)
        .background_compaction(false)
        .with_memtable_size_limit(64 * 1024 * 1024)
        .build()
        .expect("build local WAL replay fixture options");
    let mut engine = Engine::open(options).expect("open WAL replay fixture");
    let cf = engine.get_column_family("default").expect("default family");
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    for batch in 0..WAL_REPLAY_BATCHES {
        let start = batch * WAL_REPLAY_WRITES_PER_BATCH;
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin WAL replay fixture batch");
        for i in start..start + WAL_REPLAY_WRITES_PER_BATCH {
            let key = stress_config::bench_stress::key16_u64_be(i);
            let mut value = vec![0_u8; WAL_REPLAY_VALUE_BYTES];
            rng.fill(value.as_mut_slice());
            tx.put(key.to_vec(), value, None)
                .expect("write fixture key");
        }
        tx.commit(WriteOptions::sync())
            .expect("persist fixture batch");
    }
    engine
        .shutdown(Duration::from_secs(10))
        .expect("close WAL fixture without a flush");
    cf.id()
}

fn replay_unflushed_wal(fs: &RealFs, segments: &ReplaySegments, family: u32) -> Duration {
    let mut tables = std::collections::HashMap::new();
    let mut options = ReplayOptions::new(ReplaySelection::All);
    options.spill_pending_txns = true;
    let started = Instant::now();
    let stats = replay_wal_with_options(
        fs,
        segments,
        &mut tables,
        ReplayPolicy::Strict,
        StreamingReplayLimits::local(),
        options,
        &mut |_, _| unreachable!("unbounded local replay cannot checkpoint"),
    )
    .expect("replay fixture WAL");
    let elapsed = started.elapsed();

    assert!(!stats.had_corruption);
    let table = tables.get(&family).expect("recovered family memtable");
    let writes = WAL_REPLAY_BATCHES * WAL_REPLAY_WRITES_PER_BATCH;
    assert_eq!(table.iter_all().len() as u64, writes);
    elapsed
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_recovery_replay_unflushed_local(ctx: &mut StressContext) {
    use cntryl_stress::{LogicalUnit, OperationOutcome};

    let dir = tempfile::tempdir().expect("create WAL replay fixture root");
    let family = prepare_unflushed_wal(dir.path());
    let fs = RealFs::open_existing(dir.path().join("wal")).expect("open fixture WAL root");
    let (ledger, _) = LedgerStore::load(&fs)
        .expect("read fixture WAL ledger")
        .expect("fixture WAL ledger exists");
    let segments = ReplaySegments::from_ledger(&ledger, |id| FsPath::new(segment_file_name(id)));
    ctx.parameter("fixture_setup_measured", false);
    ctx.parameter("cache_state", "warm_after_fixture_write");
    ctx.parameter("replays_per_sample", WAL_REPLAYS_PER_SAMPLE);
    ctx.parameter("writes_per_batch", WAL_REPLAY_WRITES_PER_BATCH);
    ctx.parameter("value_bytes", WAL_REPLAY_VALUE_BYTES);
    stress_config::mark_capped_probe(ctx, "fixed_unflushed_local_wal");

    let elapsed = (0..WAL_REPLAYS_PER_SAMPLE)
        .map(|_| replay_unflushed_wal(&fs, &segments, family))
        .sum();
    ctx.record_external_outcome(
        "tier4_recovery_replay_unflushed_local",
        elapsed,
        LogicalUnit::new("recovered_write"),
        OperationOutcome::success(
            WAL_REPLAYS_PER_SAMPLE * WAL_REPLAY_BATCHES * WAL_REPLAY_WRITES_PER_BATCH,
        ),
    );
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_recovery_reopen_after_flush_local(ctx: &mut StressContext) {
    let opts = recovery_opts_for_mode("local");
    run_reopen_after_flush_case(ctx, "tier4_recovery_reopen_after_flush_local", &opts);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_recovery_reopen_after_flush_cloud(ctx: &mut StressContext) {
    let opts = recovery_opts_for_mode("cloud");
    run_reopen_after_flush_case(ctx, "tier4_recovery_reopen_after_flush_cloud", &opts);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_recovery_reopen_after_compaction_local(ctx: &mut StressContext) {
    let opts = recovery_opts_for_mode("local");
    run_reopen_after_compaction_case(ctx, "tier4_recovery_reopen_after_compaction_local", &opts);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_recovery_reopen_after_compaction_cloud(ctx: &mut StressContext) {
    let opts = recovery_opts_for_mode("cloud");
    run_reopen_after_compaction_case(ctx, "tier4_recovery_reopen_after_compaction_cloud", &opts);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_recovery_cloud_inventory_10k(ctx: &mut StressContext) {
    run_cloud_inventory_case(ctx);
}

stress_main!();
