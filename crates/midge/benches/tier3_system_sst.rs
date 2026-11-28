//! Tier 3 — SST primitives
//!
//! Measures: cost of point seek, iterator construction, first advance, and
//! bounded-cache SST point reads with cold admissions.
//! NOT: full scans, iteration, payload processing

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};

use cntryl_midge::{
    BlockCachePolicy, ColumnFamilyHandle, ColumnFamilyId, Engine, MemoryBudget, OpenOptions,
    TransactionMode, WriteOptions,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use stress_config::MidgeOptions;
use tempfile::TempDir;

const KEY_SIZE: usize = stress_config::bench_stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;
const SST_POINT_SEEK_BATCH_SIZE: usize = 1;
const SST_RANGE_SEEK_BATCH_SIZE: usize = 64;
const SST_FIXTURE_MEMTABLE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const SST_POINT_SEEK_SAMPLE_COUNT: usize = 12;
const CHURN_MEMORY_BUDGET_BYTES: usize = 32 * 1024 * 1024;
const CHURN_MEMTABLE_BYTES: usize = 8 * 1024 * 1024;
const CHURN_KEYS_PER_SST: usize = 16_384;
const CHURN_VALUE_BYTES: usize = 256;
const CHURN_PROBES: usize = 512;
const CHURN_KEYS_PER_PROBE: usize = 64;
const CHURN_HOT_PROBES: usize = 64;
const CHURN_MEASURED: Duration = Duration::from_secs(5);
const CHURN_WARMUP: Duration = Duration::from_secs(1);

struct ChurnProbe {
    key: [u8; KEY_SIZE],
    expected: [u8; CHURN_VALUE_BYTES],
}

#[derive(Clone, Copy, Debug)]
struct ChurnReadCounters {
    block_hits: u64,
    block_misses: u64,
    data_blocks_read: u64,
    candidate_ssts: u64,
    candidate_blocks: u64,
}

impl ChurnReadCounters {
    fn capture(engine: &Engine) -> Self {
        let snapshot = engine.read_path_diagnostics_snapshot_for_benchmarks();
        Self {
            block_hits: snapshot.sst_block_cache_hits,
            block_misses: snapshot.sst_block_cache_misses,
            data_blocks_read: snapshot.data_blocks_read,
            candidate_ssts: snapshot.candidate_sst_files_checked,
            candidate_blocks: snapshot.candidate_blocks_checked,
        }
    }

    fn since(self, before: Self) -> Self {
        let delta = |end: u64, start: u64| end.checked_sub(start).expect("read counter decreased");
        Self {
            block_hits: delta(self.block_hits, before.block_hits),
            block_misses: delta(self.block_misses, before.block_misses),
            data_blocks_read: delta(self.data_blocks_read, before.data_blocks_read),
            candidate_ssts: delta(self.candidate_ssts, before.candidate_ssts),
            candidate_blocks: delta(self.candidate_blocks, before.candidate_blocks),
        }
    }

    fn record(self, ctx: &mut StressContext) {
        let as_f64 = |value| f64::from(u32::try_from(value).expect("counter fits in u32"));
        for (name, value) in [
            ("sst_block_cache_hits", self.block_hits),
            ("sst_block_cache_misses", self.block_misses),
            ("data_blocks_read", self.data_blocks_read),
            ("candidate_sst_files_checked", self.candidate_ssts),
            ("candidate_blocks_checked", self.candidate_blocks),
        ] {
            ctx.record_observation(
                name,
                as_f64(value),
                ObservationUnit::Count,
                ObservationDirection::Informational,
            );
        }
        ctx.record_observation(
            "sst_block_cache_hit_ratio",
            as_f64(self.block_hits) / as_f64(self.block_hits + self.block_misses),
            ObservationUnit::Ratio,
            ObservationDirection::Informational,
        );
    }
}

fn churn_value(index: usize) -> [u8; CHURN_VALUE_BYTES] {
    // Incompressible per-key values keep the flushed data footprint above the
    // bounded block-cache budget under the default LZ4 SST policy.
    let mut state = (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut value = [0; CHURN_VALUE_BYTES];
    for chunk in value.as_chunks_mut::<8>().0 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    value
}

fn read_churn_probe(engine: &Engine, cf_id: ColumnFamilyId, probe: &ChurnProbe) {
    let tx = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin SST point read");
    let value = tx.get(&probe.key).expect("read SST point value");
    assert_eq!(
        value.as_deref(),
        Some(probe.expected.as_slice()),
        "SST point read returned the wrong value"
    );
}

fn churn_probe_index(client_id: usize, operation: u64) -> usize {
    let operation = usize::try_from(operation).expect("benchmark operation index fits in usize");
    let client_offset = client_id.wrapping_mul(31);
    if operation % 5 == 0 {
        // One rotating probe for four hot reads. The 73-step walk is
        // coprime to 512, so every spaced probe appears before it repeats.
        ((operation / 5).wrapping_mul(73).wrapping_add(client_offset)) % CHURN_PROBES
    } else {
        (operation.wrapping_add(client_offset)) % CHURN_HOT_PROBES
    }
}

fn run_churn_clients(
    engine: &Arc<Engine>,
    probes: &Arc<Vec<ChurnProbe>>,
    clients: usize,
    duration: Duration,
) -> stress_config::ycsb::MultiClientRunStats {
    stress_config::ycsb::run_multi_client_for_duration_with_stats(
        engine,
        clients,
        duration,
        |client_id, _stop| {
            let probes = Arc::clone(probes);
            move |engine, cf, operation| {
                let index = churn_probe_index(client_id, operation);
                read_churn_probe(engine, cf.id(), &probes[index]);
            }
        },
    )
}

fn write_churn_ssts(engine: &Engine, cf: &ColumnFamilyHandle) {
    for generation in 0..2 {
        let start = generation * CHURN_KEYS_PER_SST;
        let end = start + CHURN_KEYS_PER_SST;
        for batch_start in (start..end).step_by(TARGET_BATCH) {
            let batch_end = (batch_start + TARGET_BATCH).min(end);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin SST churn fixture write");
            for index in batch_start..batch_end {
                tx.put(
                    stress_config::bench_stress::key16_u64_be(index as u64).to_vec(),
                    churn_value(index).to_vec(),
                    None,
                )
                .expect("write SST churn fixture value");
            }
            tx.commit(WriteOptions::best_effort())
                .expect("commit SST churn fixture batch");
        }
        engine.flush_cf(cf).expect("flush SST churn generation");
    }
}

fn record_churn_fixture_layout(ctx: &mut StressContext, engine: &Engine, cache_capacity: usize) {
    let layout = engine
        .metrics()
        .get_storage_layout()
        .expect("capture flushed SST churn fixture layout");
    let file_count: usize = layout.levels.iter().map(|level| level.file_count).sum();
    let sst_bytes: u64 = layout.levels.iter().map(|level| level.total_bytes).sum();
    assert_eq!(file_count, 2, "fixture should have two flushed SSTs");
    assert!(
        sst_bytes > u64::try_from(cache_capacity).expect("cache capacity fits in u64"),
        "stored SST bytes must exceed block-cache capacity"
    );
    ctx.parameter("fixture_ssts", file_count);
    ctx.parameter("fixture_stored_sst_bytes", sst_bytes);
}

fn run_sst_cold_admission_churn(ctx: &mut StressContext) {
    let directory = TempDir::new().expect("create SST churn database directory");
    let options = OpenOptions::local(directory.path())
        .memory_budget(MemoryBudget::Bytes(CHURN_MEMORY_BUDGET_BYTES))
        .with_memtable_size_limit(CHURN_MEMTABLE_BYTES)
        .with_memtable_flush_threshold(CHURN_MEMTABLE_BYTES)
        .background_compaction(false)
        .block_cache_policy(BlockCachePolicy::Lru)
        .build()
        .expect("build bounded SST churn options");
    let logical_data_bytes = (CHURN_KEYS_PER_SST * 2) * (KEY_SIZE + CHURN_VALUE_BYTES);
    // ReadResources reserves one quarter of the configured read pool for SST
    // metadata and uses the remaining three quarters for resident blocks.
    let read_pool_bytes = options.block_cache_size();
    let cache_capacity = read_pool_bytes.saturating_sub(read_pool_bytes / 4);
    assert!(
        logical_data_bytes > cache_capacity,
        "flushed data must exceed block-cache capacity"
    );
    ctx.parameter("storage_profile", "local");
    ctx.parameter("logical_unit", "engine_point_read");
    ctx.parameter("operation_surface", "begin_tx_plus_sst_get");
    ctx.parameter("fixture_keys", CHURN_KEYS_PER_SST * 2);
    ctx.parameter("fixture_logical_data_bytes", logical_data_bytes);
    ctx.parameter("sst_read_pool_bytes", read_pool_bytes);
    ctx.parameter("block_cache_capacity_bytes", cache_capacity);
    ctx.parameter("probe_keys", CHURN_PROBES);
    ctx.parameter("probe_stride_keys", CHURN_KEYS_PER_PROBE);
    ctx.parameter("hot_probe_keys", CHURN_HOT_PROBES);
    ctx.parameter("rotating_probe_fraction", "1/5");
    ctx.metadata("diagnostic_reason", "controlled_engine_sst_cache_churn");

    let engine = Arc::new(Engine::open(options).expect("open SST churn engine"));
    let cf = engine
        .create_column_family("cf1")
        .expect("create SST churn column family");
    write_churn_ssts(&engine, &cf);
    record_churn_fixture_layout(ctx, &engine, cache_capacity);

    let probes = Arc::new(
        (0..CHURN_PROBES)
            .map(|index| {
                let key_index = ((index * 73) % CHURN_PROBES) * CHURN_KEYS_PER_PROBE;
                ChurnProbe {
                    key: stress_config::bench_stress::key16_u64_be(key_index as u64),
                    expected: churn_value(key_index),
                }
            })
            .collect::<Vec<_>>(),
    );

    // This pass starts with no data-block reads through the Engine, so its
    // measured misses include actual SST block reads and cache admissions.
    let before = ChurnReadCounters::capture(&engine);
    let mut latencies = Vec::with_capacity(CHURN_PROBES);
    let started_at = Instant::now();
    for probe in probes.iter() {
        let read_started_at = Instant::now();
        read_churn_probe(&engine, cf.id(), probe);
        latencies.push(read_started_at.elapsed());
    }
    let elapsed = started_at.elapsed();
    let cold = ChurnReadCounters::capture(&engine).since(before);
    assert!(cold.candidate_ssts > 0 && cold.candidate_blocks > 0);
    assert!(
        cold.block_misses >= CHURN_PROBES as u64 / 4,
        "cold probe pass did not visit enough uncached blocks: {cold:?}"
    );
    assert!(cold.data_blocks_read > 0);
    ctx.record_external_outcome(
        "tier3_sst_cold_point_admission_local",
        elapsed,
        LogicalUnit::new("engine_point_read"),
        OperationOutcome::success(CHURN_PROBES as u64),
    );
    for latency in latencies {
        ctx.record_latency(latency);
    }
    cold.record(ctx);

    for clients in [1, 16] {
        // Re-warm after the prior read phase so both client rows start with
        // resident hot blocks, while the spaced cold rotation still churns.
        for probe in probes.iter().take(CHURN_HOT_PROBES) {
            read_churn_probe(&engine, cf.id(), probe);
        }
        let _warmup = run_churn_clients(&engine, &probes, clients, CHURN_WARMUP);
        let before = ChurnReadCounters::capture(&engine);
        let name = format!("tier3_sst_hot_cold_churn_local_{clients}_clients");
        let measured = stress_config::measure_counted(ctx, name, "engine_point_read", || {
            let measured = run_churn_clients(&engine, &probes, clients, CHURN_MEASURED);
            let operations = measured.operations;
            (measured, operations)
        });
        let delta = ChurnReadCounters::capture(&engine).since(before);
        assert!(delta.block_hits > 0 && delta.block_misses > 0);
        assert!(
            delta.block_misses >= measured.operations / 20,
            "measured reads did not sustain cache churn: {delta:?} over {} reads",
            measured.operations
        );
        assert!(delta.data_blocks_read > 0 && delta.candidate_blocks > 0);
        measured.record_latencies(ctx);
        delta.record(ctx);
    }
}

fn setup_engine(opts: MidgeOptions) -> Engine {
    stress_config::bench_stress::open_engine_no_compaction(opts)
}

fn precompute_keys(num: usize) -> Vec<[u8; KEY_SIZE]> {
    stress_config::bench_stress::precompute_keys16_u64_be(num)
}

fn run_sst_point_seek_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mut opts: MidgeOptions,
    num_keys: usize,
) {
    // Build one deliberate SST at the explicit flush boundary. The generic
    // local profile's 64 KiB memtable is smaller than this fixture and can
    // trigger write stalls or background flushes during setup.
    opts.memtable_size = opts.memtable_size.max(SST_FIXTURE_MEMTABLE_SIZE_BYTES);

    ctx.parameter("logical_batch_size", SST_POINT_SEEK_BATCH_SIZE);
    ctx.parameter("logical_unit", "sst_point_seek");
    ctx.parameter("operation_surface", "sst_point_seek");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_key_count", num_keys);
    ctx.parameter("fixture_memtable_size_bytes", opts.memtable_size);
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

    let write_opts = stress_config::measured_write_options(&opts);
    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement: create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    let total = keys.len();
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for (i, k) in keys[start..end].iter().enumerate() {
            let idx = start + i;
            let v = vec![u8::try_from(idx % 251).expect("value byte fits in u8"); VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    let mut key_index = num_keys / 2;
    let read_path_before = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let mut validation_failures = 0_u64;

    let _ = ctx
        .benchmark(scenario)
        .samples(SST_POINT_SEEK_SAMPLE_COUNT)
        .measure_batch(SST_POINT_SEEK_BATCH_SIZE as u64, || {
            for _ in 0..SST_POINT_SEEK_BATCH_SIZE {
                let key = keys[key_index % keys.len()];
                key_index = key_index.wrapping_add(1);
                let expected = vec![
                    u8::try_from(key_index.wrapping_sub(1) % keys.len() % 251)
                        .expect("value byte fits in u8");
                    VALUE_SIZE
                ];
                match tx.get(&key[..]) {
                    Ok(Some(value)) if value.as_ref() == expected.as_slice() => {}
                    _ => validation_failures += 1,
                }
            }
        });

    let read_path_after = engine.read_path_diagnostics_snapshot_for_benchmarks();
    assert_eq!(
        validation_failures, 0,
        "measured SST point reads must validate"
    );
    assert!(
        read_path_after.candidate_sst_files_checked > read_path_before.candidate_sst_files_checked
            && read_path_after.candidate_blocks_checked > read_path_before.candidate_blocks_checked,
        "SST point row must exercise candidate SST and block work"
    );

    // Engine shutdown waits for active transaction guards. Release the
    // read snapshot before dropping the engine so this benchmark can finish.
    drop(tx);
    drop(engine);
}

fn run_sst_range_seek_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mut opts: MidgeOptions,
    num_keys: usize,
) {
    // Keep setup in one memtable until the explicit fixture flush below.
    opts.memtable_size = opts.memtable_size.max(SST_FIXTURE_MEMTABLE_SIZE_BYTES);

    ctx.parameter("logical_batch_size", SST_RANGE_SEEK_BATCH_SIZE);
    ctx.parameter("logical_unit", "sst_range_seek");
    ctx.parameter("operation_surface", "sst_range_seek_first_row");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_key_count", num_keys);
    ctx.parameter("fixture_memtable_size_bytes", opts.memtable_size);
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

    let write_opts = stress_config::measured_write_options(&opts);
    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement: create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    let total = keys.len();
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for (i, k) in keys[start..end].iter().enumerate() {
            let idx = start + i;
            let v = vec![u8::try_from(idx % 251).expect("value byte fits in u8"); VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    let mut key_index = 0usize;
    let read_path_before = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let mut validation_failures = 0_u64;

    let _ = ctx.measure_batch(scenario, SST_RANGE_SEEK_BATCH_SIZE as u64, || {
        for _ in 0..SST_RANGE_SEEK_BATCH_SIZE {
            let start_index = key_index % (keys.len() - 33);
            let start = keys[start_index];
            let end = keys[start_index + 32];
            key_index = key_index.wrapping_add(1);
            let query = cntryl_midge::Query::new()
                .start_key(cntryl_midge::Bytes::copy_from_slice(&start[..]))
                .end_key(cntryl_midge::Bytes::copy_from_slice(&end[..]));
            let mut it = tx.scan(&query).expect("scan failed");
            let expected_value =
                vec![u8::try_from(start_index % 251).expect("value byte fits"); VALUE_SIZE];
            match it.next() {
                Some(Ok((key, value)))
                    if key.as_ref() == start.as_slice()
                        && value.as_ref() == expected_value.as_slice() => {}
                _ => validation_failures += 1,
            }
        }
    });

    let read_path_after = engine.read_path_diagnostics_snapshot_for_benchmarks();
    assert_eq!(
        validation_failures, 0,
        "measured SST range reads must validate"
    );
    assert!(
        read_path_after.candidate_sst_files_checked > read_path_before.candidate_sst_files_checked
            && read_path_after.candidate_blocks_checked > read_path_before.candidate_blocks_checked,
        "SST range row must exercise candidate SST and block work"
    );

    // Engine shutdown waits for active transaction guards. Release the
    // read snapshot before dropping the engine so this benchmark can finish.
    drop(tx);
    drop(engine);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_point_seek_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_sst_point_seek_case(ctx, "tier3_sst_point_seek_local", opts, 5_000);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_point_seek_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_sst_point_seek_case(ctx, "tier3_sst_point_seek_cloud", opts, 5_000);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_range_seek_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_sst_range_seek_case(ctx, "tier3_sst_range_seek_local", opts, 10_000);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_range_seek_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_sst_range_seek_case(ctx, "tier3_sst_range_seek_cloud", opts, 10_000);
}

#[stress(
    tier = 3,
    role = "diagnostic",
    metadata(component = "engine_sst_cache", scenario = "cold_admission_churn")
)]
fn tier3_sst_cold_admission_churn_local(ctx: &mut StressContext) {
    run_sst_cold_admission_churn(ctx);
}

stress_main!();
