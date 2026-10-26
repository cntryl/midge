//! Tier 2 - Durability commit latency
//!
//! Measures fixed-work strict commit throughput and latency on durable local storage.
//! The 1-writer row owns the direct durability floor. The 16- and 64-writer rows
//! expose physical WAL append/fsync sharing across independent transactions.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{ColumnFamilyId, Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use hdrhistogram::Histogram;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const VALUE_SIZE: usize = 128;
const ROTATION_VALUE_SIZE: usize = 4 * 1024;
const ROTATION_MEMTABLE_BYTES: usize = 128 * 1024;
const ROTATION_WARMUP_TRANSACTIONS: usize = 64;
const ROTATION_TRANSACTIONS_PER_RUN: usize = 512;
const ROTATION_RUNS: usize = 5;
const SHARED_HOT_KEY: &[u8; 16] = b"strict-hot-key01";

#[derive(Clone, Copy)]
enum StrictKeyDistribution {
    Distinct,
    SharedHotKey,
}

impl StrictKeyDistribution {
    fn label(self) -> &'static str {
        match self {
            Self::Distinct => "distinct",
            Self::SharedHotKey => "single_shared_hot_key",
        }
    }
}

struct StrictCommitSample {
    ledger: LedgerSyncSample,
    elapsed: Duration,
    completed: u64,
    wal_appends: u64,
    physical_fsyncs: u64,
    commit_latencies_us: Vec<u64>,
}

struct RotationRunSample {
    ledger: LedgerSyncSample,
    elapsed: Duration,
    event_loop_write_service_ns_total: u64,
    wal_encode_ns_total: u64,
    wal_write_syscall_ns_total: u64,
    commit_p50_us: u64,
    commit_p95_us: u64,
    commit_p99_us: u64,
    rotation_commit_p95_us: u64,
    non_rotation_commit_p95_us: u64,
    rotation_commits: u64,
    wal_fsyncs: u64,
    wal_fsync_ns_total: u64,
    wal_fsync_ns_max: u64,
    flush_build_count: u64,
    flush_build_ns_total: u64,
    flush_publish_count: u64,
    flush_publish_ns_total: u64,
}

#[derive(Default)]
struct LedgerSyncSample {
    file_attempts: u64,
    file_completed: u64,
    directory_attempts: u64,
    directory_completed: u64,
    accounted: u64,
}

impl LedgerSyncSample {
    fn between(start: &RuntimeMetricsSnapshot, end: &RuntimeMetricsSnapshot) -> Self {
        Self {
            file_attempts: end
                .wal_ledger_file_sync_attempts
                .saturating_sub(start.wal_ledger_file_sync_attempts),
            file_completed: end
                .wal_ledger_file_syncs
                .saturating_sub(start.wal_ledger_file_syncs),
            directory_attempts: end
                .wal_ledger_directory_sync_attempts
                .saturating_sub(start.wal_ledger_directory_sync_attempts),
            directory_completed: end
                .wal_ledger_directory_syncs
                .saturating_sub(start.wal_ledger_directory_syncs),
            accounted: end
                .wal_accounted_syncs
                .saturating_sub(start.wal_accounted_syncs),
        }
    }

    fn add(&mut self, other: &Self) {
        self.file_attempts += other.file_attempts;
        self.file_completed += other.file_completed;
        self.directory_attempts += other.directory_attempts;
        self.directory_completed += other.directory_completed;
        self.accounted += other.accounted;
    }

    fn record(&self, ctx: &mut StressContext) {
        ctx.parameter("physical_fsync_scope", "successful_writer_runner_barriers");
        ctx.parameter(
            "accounted_sync_scope",
            "writer_barriers_plus_ledger_files_and_directories_excludes_other_syncs",
        );
        for (name, count) in [
            ("ledger_file_sync_attempts", self.file_attempts),
            ("ledger_file_syncs", self.file_completed),
            ("ledger_directory_sync_attempts", self.directory_attempts),
            ("ledger_directory_syncs", self.directory_completed),
            ("wal_and_ledger_accounted_syncs", self.accounted),
        ] {
            ctx.record_observation(
                name,
                u64_to_f64(count),
                ObservationUnit::Count,
                ObservationDirection::Informational,
            );
        }
        assert_eq!(
            self.file_attempts, self.file_completed,
            "ledger file sync errors invalidate cost measurements"
        );
        assert_eq!(
            self.directory_attempts, self.directory_completed,
            "ledger directory sync errors invalidate cost measurements"
        );
    }
}

fn metrics_delta(start: &RuntimeMetricsSnapshot, end: &RuntimeMetricsSnapshot) -> (u64, u64) {
    (
        end.wal_append_count.saturating_sub(start.wal_append_count),
        end.wal_fsync_count.saturating_sub(start.wal_fsync_count),
    )
}

fn median_u64(values: impl IntoIterator<Item = u64>) -> u64 {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn u64_to_f64(value: u64) -> f64 {
    let upper = u32::try_from(value >> 32).expect("upper half fits in u32");
    let lower = u32::try_from(value & u64::from(u32::MAX)).expect("lower half fits in u32");
    f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
}

fn record_success(
    ctx: &mut StressContext,
    name: impl Into<String>,
    duration: Duration,
    logical_unit: &'static str,
    completed: u64,
) {
    ctx.record_external_outcome(
        name,
        duration,
        LogicalUnit::new(logical_unit),
        OperationOutcome::success(completed),
    );
}

fn commit_rotation_value(engine: &Engine, cf_id: ColumnFamilyId, ordinal: usize, key_prefix: &str) {
    let key = format!("{key_prefix}-{ordinal:08}").into_bytes();
    let value =
        vec![u8::try_from(ordinal % 251).expect("value byte fits in u8"); ROTATION_VALUE_SIZE];
    loop {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin rotation transaction");
        tx.put(key.clone(), value.clone(), None)
            .expect("put rotation value");
        match tx.commit(WriteOptions::sync()) {
            Ok(()) => return,
            Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                assert!(
                    engine
                        .wait_for_write_stall_clear(cf_id, Duration::from_secs(5))
                        .expect("wait for rotation benchmark stall"),
                    "rotation benchmark write stall did not clear"
                );
            }
            Err(error) => panic!("rotation benchmark commit failed: {error}"),
        }
    }
}

fn execute_rotation_run() -> RotationRunSample {
    let mut opts = stress_config::write_coordination_opts_for_mode("local");
    // Sustained rotation needs compaction to reclaim bounded L0 slots.
    opts.enable_compaction = true;
    opts.memtable_size = ROTATION_MEMTABLE_BYTES;
    let engine = Engine::open(opts.to_open_options()).expect("open rotation benchmark engine");
    let cf = engine
        .create_column_family("rotation")
        .expect("create rotation benchmark column family");
    for ordinal in 0..ROTATION_WARMUP_TRANSACTIONS {
        commit_rotation_value(&engine, cf.id(), ordinal, "warmup");
    }
    engine.flush_cf(&cf).expect("flush rotation warmup");

    let start_metrics = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture starting rotation metrics");
    let mut all = Histogram::<u64>::new(3).expect("create rotation commit histogram");
    let mut rotation = Histogram::<u64>::new(3).expect("create rotating commit histogram");
    let mut non_rotation = Histogram::<u64>::new(3).expect("create non-rotating commit histogram");
    let mut rotation_commits = 0_u64;
    let started_at = Instant::now();
    for ordinal in 0..ROTATION_TRANSACTIONS_PER_RUN {
        let before = engine
            .metrics()
            .get_runtime_metrics()
            .expect("capture pre-commit rotation metrics");
        let commit_started_at = Instant::now();
        commit_rotation_value(&engine, cf.id(), ordinal, "measured");
        let latency_us = u64::try_from(commit_started_at.elapsed().as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        let after = engine
            .metrics()
            .get_runtime_metrics()
            .expect("capture post-commit rotation metrics");
        all.record(latency_us).expect("record commit latency");
        if after.flush_enqueued_total > before.flush_enqueued_total {
            rotation
                .record(latency_us)
                .expect("record rotating commit latency");
            rotation_commits = rotation_commits.saturating_add(1);
        } else {
            non_rotation
                .record(latency_us)
                .expect("record non-rotating commit latency");
        }
    }
    let elapsed = started_at.elapsed();
    engine
        .flush_cf(&cf)
        .expect("drain measured rotation flushes");
    let end_metrics = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture ending rotation metrics");

    RotationRunSample {
        ledger: LedgerSyncSample::between(&start_metrics, &end_metrics),
        elapsed,
        event_loop_write_service_ns_total: end_metrics
            .event_loop_write_service_ns_total
            .saturating_sub(start_metrics.event_loop_write_service_ns_total),
        wal_encode_ns_total: end_metrics
            .wal_encode_ns_total
            .saturating_sub(start_metrics.wal_encode_ns_total),
        wal_write_syscall_ns_total: end_metrics
            .wal_write_syscall_ns_total
            .saturating_sub(start_metrics.wal_write_syscall_ns_total),
        commit_p50_us: all.value_at_quantile(0.50),
        commit_p95_us: all.value_at_quantile(0.95),
        commit_p99_us: all.value_at_quantile(0.99),
        rotation_commit_p95_us: rotation.value_at_quantile(0.95),
        non_rotation_commit_p95_us: non_rotation.value_at_quantile(0.95),
        rotation_commits,
        wal_fsyncs: end_metrics
            .wal_fsync_count
            .saturating_sub(start_metrics.wal_fsync_count),
        wal_fsync_ns_total: end_metrics
            .wal_fsync_ns_total
            .saturating_sub(start_metrics.wal_fsync_ns_total),
        wal_fsync_ns_max: end_metrics.wal_fsync_ns_max,
        flush_build_count: end_metrics
            .flush_build_count
            .saturating_sub(start_metrics.flush_build_count),
        flush_build_ns_total: end_metrics
            .flush_build_ns_total
            .saturating_sub(start_metrics.flush_build_ns_total),
        flush_publish_count: end_metrics
            .flush_publish_count
            .saturating_sub(start_metrics.flush_publish_count),
        flush_publish_ns_total: end_metrics
            .flush_publish_ns_total
            .saturating_sub(start_metrics.flush_publish_ns_total),
    }
}

fn writer_workload(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    writer: usize,
    transactions: usize,
    key_distribution: StrictKeyDistribution,
    barrier: &Barrier,
) -> Vec<u64> {
    let mut commit_latencies_us = Vec::with_capacity(transactions);
    barrier.wait();
    for index in 0..transactions {
        let ordinal = writer * transactions + index;
        let key = match key_distribution {
            StrictKeyDistribution::Distinct => stress_config::bench_stress::key16_u64_be(
                u64::try_from(ordinal).expect("transaction ordinal fits in u64"),
            )
            .to_vec(),
            StrictKeyDistribution::SharedHotKey => SHARED_HOT_KEY.to_vec(),
        };
        let value = vec![u8::try_from(ordinal % 251).expect("value byte fits in u8"); VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin durability latency transaction");
        tx.put(key, value, None)
            .expect("put durability latency value");
        let commit_started_at = Instant::now();
        tx.commit(WriteOptions::sync())
            .expect("commit durability latency transaction");
        commit_latencies_us
            .push(u64::try_from(commit_started_at.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    commit_latencies_us
}

fn execute_strict_commit_case(
    writers: usize,
    key_distribution: StrictKeyDistribution,
) -> StrictCommitSample {
    let transactions_per_sample: usize = match writers {
        1 => 2_048,
        16 => 8_192,
        64 => 32_768,
        _ => panic!("unsupported strict commit writer count"),
    };
    assert!(transactions_per_sample.is_multiple_of(writers));
    let mut opts = stress_config::write_coordination_opts_for_mode("local");
    opts.enable_compaction = false;
    stress_config::init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let engine =
        Arc::new(Engine::open(opts.to_open_options()).expect("open durability latency engine"));
    let cf = engine
        .create_column_family("cf1")
        .expect("create durability latency column family");
    let cf_id = cf.id();
    let start_metrics = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture starting durability metrics");
    let transactions_per_writer = transactions_per_sample / writers;
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            writer_workload(
                &engine,
                cf_id,
                writer,
                transactions_per_writer,
                key_distribution,
                &barrier,
            )
        }));
    }

    let started_at = Instant::now();
    barrier.wait();
    let mut commit_latencies_us = Vec::with_capacity(transactions_per_sample);
    for handle in handles {
        for latency_us in handle.join().expect("join strict commit writer") {
            commit_latencies_us.push(latency_us);
        }
    }
    let elapsed = started_at.elapsed();
    let end_metrics = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture ending durability metrics");
    let (wal_appends, physical_fsyncs) = metrics_delta(&start_metrics, &end_metrics);
    let mut engine = Arc::try_unwrap(engine).unwrap_or_else(|_| {
        panic!("all strict commit benchmark engine references must be released")
    });
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shut down strict commit benchmark engine");

    StrictCommitSample {
        ledger: LedgerSyncSample::between(&start_metrics, &end_metrics),
        elapsed,
        completed: u64::try_from(transactions_per_sample).expect("transaction count fits in u64"),
        wal_appends,
        physical_fsyncs,
        commit_latencies_us,
    }
}

fn run_strict_commit_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    writers: usize,
    key_distribution: StrictKeyDistribution,
) {
    let transactions = match writers {
        1 => 2_048,
        16 => 8_192,
        64 => 32_768,
        _ => panic!("unsupported strict commit writer count"),
    };
    ctx.parameter("logical_batch_size", transactions);
    ctx.parameter("logical_unit", "transaction");
    ctx.parameter("storage_profile", "local");
    ctx.parameter("commit_mode", "sync");
    ctx.parameter("writers", writers);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("key_distribution", key_distribution.label());
    ctx.parameter("operation_surface", "single_put_single_commit");
    let sample = execute_strict_commit_case(writers, key_distribution);
    sample.ledger.record(ctx);
    record_success(
        ctx,
        scenario,
        sample.elapsed,
        "transaction",
        sample.completed,
    );
    for latency_us in &sample.commit_latencies_us {
        ctx.record_latency(Duration::from_micros((*latency_us).max(1)));
    }
    ctx.record_observation(
        "wal_appends",
        u64_to_f64(sample.wal_appends),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "physical_fsyncs",
        u64_to_f64(sample.physical_fsyncs),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    assert!(
        sample.wal_appends > 0,
        "strict transactions must produce at least one physical WAL append"
    );
    ctx.record_observation(
        "commits_per_fsync",
        u64_to_f64(sample.completed) / u64_to_f64(sample.physical_fsyncs.max(1)),
        ObservationUnit::Ratio,
        ObservationDirection::HigherIsBetter,
    );
    ctx.record_observation(
        "avg_txn_records_per_append",
        u64_to_f64(sample.completed) / u64_to_f64(sample.wal_appends),
        ObservationUnit::Ratio,
        ObservationDirection::HigherIsBetter,
    );
}

fn set_rotation_parameters(ctx: &mut StressContext) {
    ctx.parameter("independent_runs", ROTATION_RUNS);
    ctx.parameter("logical_batch_size", ROTATION_TRANSACTIONS_PER_RUN);
    ctx.parameter("storage_profile", "local_durable");
    ctx.parameter("commit_mode", "sync");
    ctx.parameter("writers", 1);
    ctx.parameter("value_size_bytes", ROTATION_VALUE_SIZE);
    ctx.parameter("memtable_flush_threshold_bytes", ROTATION_MEMTABLE_BYTES);
    ctx.parameter("background_compaction", true);
    ctx.parameter("warmup_transactions_per_run", ROTATION_WARMUP_TRANSACTIONS);
    ctx.parameter(
        "measured_transactions_per_run",
        ROTATION_TRANSACTIONS_PER_RUN,
    );
}

fn record_rotation_observations(
    ctx: &mut StressContext,
    samples: &[RotationRunSample],
    elapsed: Duration,
) {
    let mut ledger = LedgerSyncSample::default();
    for sample in samples {
        ledger.add(&sample.ledger);
    }
    ledger.record(ctx);
    let wal_fsyncs = samples.iter().map(|sample| sample.wal_fsyncs).sum::<u64>();
    let wal_fsync_ns_total = samples
        .iter()
        .map(|sample| sample.wal_fsync_ns_total)
        .sum::<u64>();
    let wal_fsync_ns_max = samples
        .iter()
        .map(|sample| sample.wal_fsync_ns_max)
        .max()
        .unwrap_or(0);
    let rotation_commits = samples
        .iter()
        .map(|sample| sample.rotation_commits)
        .sum::<u64>();
    let flush_build_count = samples
        .iter()
        .map(|sample| sample.flush_build_count)
        .sum::<u64>();
    let flush_build_ns_total = samples
        .iter()
        .map(|sample| sample.flush_build_ns_total)
        .sum::<u64>();
    let flush_publish_count = samples
        .iter()
        .map(|sample| sample.flush_publish_count)
        .sum::<u64>();
    let flush_publish_ns_total = samples
        .iter()
        .map(|sample| sample.flush_publish_ns_total)
        .sum::<u64>();
    for latency_us in [
        median_u64(samples.iter().map(|sample| sample.commit_p50_us)),
        median_u64(samples.iter().map(|sample| sample.commit_p95_us)),
        median_u64(samples.iter().map(|sample| sample.commit_p99_us)),
    ] {
        ctx.record_latency(Duration::from_micros(latency_us.max(1)));
    }
    for (name, value, unit) in [
        (
            "rotation_commit_p95_us",
            median_u64(samples.iter().map(|s| s.rotation_commit_p95_us)),
            ObservationUnit::Microseconds,
        ),
        (
            "non_rotation_commit_p95_us",
            median_u64(samples.iter().map(|s| s.non_rotation_commit_p95_us)),
            ObservationUnit::Microseconds,
        ),
        ("wal_fsyncs", wal_fsyncs, ObservationUnit::Count),
        (
            "wal_fsync_ns_total",
            wal_fsync_ns_total,
            ObservationUnit::Nanoseconds,
        ),
        (
            "wal_fsync_ns_max",
            wal_fsync_ns_max,
            ObservationUnit::Nanoseconds,
        ),
        ("rotation_commits", rotation_commits, ObservationUnit::Count),
        (
            "flush_build_count",
            flush_build_count,
            ObservationUnit::Count,
        ),
        (
            "flush_build_ns_total",
            flush_build_ns_total,
            ObservationUnit::Nanoseconds,
        ),
        (
            "flush_publish_count",
            flush_publish_count,
            ObservationUnit::Count,
        ),
        (
            "flush_publish_ns_total",
            flush_publish_ns_total,
            ObservationUnit::Nanoseconds,
        ),
    ] {
        ctx.record_observation(
            name,
            u64_to_f64(value),
            unit,
            ObservationDirection::Informational,
        );
    }
    ctx.record_observation(
        "elapsed_ns",
        elapsed.as_secs_f64() * 1_000_000_000.0,
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    record_rotation_service_observations(ctx, samples);
}

fn record_rotation_service_observations(ctx: &mut StressContext, samples: &[RotationRunSample]) {
    let event_loop_write_service_ns_total = samples
        .iter()
        .map(|sample| sample.event_loop_write_service_ns_total)
        .sum::<u64>();
    let wal_encode_ns_total = samples
        .iter()
        .map(|sample| sample.wal_encode_ns_total)
        .sum::<u64>();
    let wal_write_syscall_ns_total = samples
        .iter()
        .map(|sample| sample.wal_write_syscall_ns_total)
        .sum::<u64>();
    let measured_commits = u64::try_from(samples.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(ROTATION_TRANSACTIONS_PER_RUN).unwrap_or(u64::MAX));
    let per_commit_ns = |total: u64| u64_to_f64(total) / u64_to_f64(measured_commits.max(1));

    for (name, value) in [
        (
            "event_loop_write_service_ns_per_4k_commit",
            per_commit_ns(event_loop_write_service_ns_total),
        ),
        (
            "wal_encode_ns_per_4k_commit",
            per_commit_ns(wal_encode_ns_total),
        ),
        (
            "wal_append_service_ns_per_4k_commit",
            per_commit_ns(wal_write_syscall_ns_total),
        ),
        (
            "wal_encode_and_append_service_ns_per_4k_commit",
            per_commit_ns(wal_encode_ns_total.saturating_add(wal_write_syscall_ns_total)),
        ),
    ] {
        ctx.record_observation(
            name,
            value,
            ObservationUnit::Nanoseconds,
            ObservationDirection::Informational,
        );
    }
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local_4k_rotation(ctx: &mut StressContext) {
    stress_config::init_benchmark_telemetry().expect("initialize benchmark telemetry");
    set_rotation_parameters(ctx);
    let samples = (0..ROTATION_RUNS)
        .map(|_| execute_rotation_run())
        .collect::<Vec<_>>();
    let elapsed = samples
        .iter()
        .map(|sample| sample.elapsed)
        .sum::<Duration>();
    let completed = u64::try_from(ROTATION_RUNS * ROTATION_TRANSACTIONS_PER_RUN)
        .expect("rotation transaction count fits u64");
    record_success(
        ctx,
        "tier2_durability_commit_sync_local_4k_rotation",
        elapsed,
        "transaction",
        completed,
    );
    record_rotation_observations(ctx, &samples, elapsed);
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local(ctx: &mut StressContext) {
    run_strict_commit_case(
        ctx,
        "tier2_durability_commit_sync_local",
        1,
        StrictKeyDistribution::Distinct,
    );
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local_16_writers(ctx: &mut StressContext) {
    run_strict_commit_case(
        ctx,
        "tier2_durability_commit_sync_local_16_writers",
        16,
        StrictKeyDistribution::Distinct,
    );
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local_16_writers_hot_key(ctx: &mut StressContext) {
    run_strict_commit_case(
        ctx,
        "tier2_durability_commit_sync_local_16_writers_hot_key",
        16,
        StrictKeyDistribution::SharedHotKey,
    );
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local_64_writers(ctx: &mut StressContext) {
    run_strict_commit_case(
        ctx,
        "tier2_durability_commit_sync_local_64_writers",
        64,
        StrictKeyDistribution::Distinct,
    );
}

stress_main!();
