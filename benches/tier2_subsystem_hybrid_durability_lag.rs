//! Tier 2 - Hybrid durability lag
//!
//! Measures the two numbers the hybrid-cloud claim depends on, side by side:
//! commit latency at the caller and how far cloud durability trails it. One
//! client commits fixed work against the in-memory object-store simulator
//! while a sampler reads the durability frontiers every few milliseconds, and
//! a probe commit waits for cloud durability every `PROBE_EVERY` commits to
//! turn the sequence lag into a time lag. Provider requests and uploaded bytes
//! come from the simulator, so every row also reports requests per commit and
//! bytes uploaded per committed byte. The local `sync()` row is the floor the
//! cloud rows are compared against.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::__internal::simulation::{Operation, Schedule, SimulatedObjectStore};
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, ColumnFamilyId, Engine, OpenOptions,
    TransactionMode, WriteOptions,
};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TRANSACTIONS: usize = 2_048;
const VALUE_SIZE: usize = 128;
const KEY_SIZE: usize = 24;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
/// The sampler's metrics round trip queues behind the event loop, so under a
/// commit flood it achieves fewer ticks than the nominal interval allows. A
/// row still reports when it got at least this fraction of them, and always
/// reports the period it achieved.
const MIN_SAMPLER_TICK_FRACTION: u64 = 4;
const PROBE_EVERY: usize = 64;
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(60);
const OBJECT_PREFIX: &str = "hybrid-lag/";

#[derive(Clone, Copy, PartialEq, Eq)]
enum CommitMode {
    CloudAsync,
    CloudStrict,
    LocalSync,
}

impl CommitMode {
    fn write_options(self) -> WriteOptions {
        match self {
            Self::CloudAsync => WriteOptions::cloud_async(),
            Self::CloudStrict => WriteOptions::cloud_strict(),
            Self::LocalSync => WriteOptions::sync(),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::CloudAsync => "cloud_async",
            Self::CloudStrict => "cloud_strict",
            Self::LocalSync => "sync",
        }
    }

    fn is_cloud(self) -> bool {
        self != Self::LocalSync
    }
}

struct LagSample {
    elapsed: Duration,
    completed: u64,
    commit_latencies_us: Vec<u64>,
    /// `wal_local_durable_seq - wal_cloud_durable_seq` at each sampler tick.
    durable_lag_sequences: Vec<u64>,
    /// `current_sequence - wal_cloud_durable_seq` at each sampler tick.
    visible_lag_sequences: Vec<u64>,
    /// Time from a probe commit's return until the cloud frontier covered it.
    probe_wait_us: Vec<u64>,
    probe_timeouts: u64,
    /// Write stalls the client waited through; their time is inside the
    /// commit latencies.
    write_stalls: u64,
    provider_puts: u64,
    upload_bytes: u64,
    committed_bytes: u64,
    sampler_ticks: u64,
}

fn provider_options(cache: &std::path::Path, store: &SimulatedObjectStore) -> OpenOptions {
    OpenOptions::cloud(
        cache,
        CloudStorageLocation::new(
            CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
            OBJECT_PREFIX,
        ),
    )
    // A cloud seal drains memtables, so strict commits publish one L0 file
    // each; without compaction the L0 ceiling stalls the writer within a few
    // hundred commits. Production runs compaction, so the rows do too.
    .background_compaction(true)
    .object_store_for_testing(Arc::new(store.process()))
    .build()
    .expect("hybrid durability lag options")
}

fn open_engine(
    mode: CommitMode,
    store: &SimulatedObjectStore,
    cache: &tempfile::TempDir,
) -> Engine {
    if mode.is_cloud() {
        Engine::open(provider_options(cache.path(), store)).expect("open simulated cloud engine")
    } else {
        let mut opts = stress_config::write_coordination_opts_for_mode("local");
        opts.enable_compaction = false;
        Engine::open(opts.to_open_options()).expect("open local engine")
    }
}

fn spawn_sampler(
    engine: &Arc<Engine>,
    stop: &Arc<AtomicBool>,
) -> std::thread::JoinHandle<(Vec<u64>, Vec<u64>, u64)> {
    let engine = Arc::clone(engine);
    let stop = Arc::clone(stop);
    std::thread::spawn(move || {
        let mut durable = Vec::new();
        let mut visible = Vec::new();
        let mut ticks = 0_u64;
        while !stop.load(Ordering::Acquire) {
            if let Ok(metrics) = engine.metrics().get_runtime_metrics() {
                durable.push(
                    metrics
                        .wal_local_durable_seq
                        .saturating_sub(metrics.wal_cloud_durable_seq),
                );
                visible.push(
                    metrics
                        .current_sequence
                        .saturating_sub(metrics.wal_cloud_durable_seq),
                );
                ticks += 1;
            }
            std::thread::sleep(SAMPLE_INTERVAL);
        }
        (durable, visible, ticks)
    })
}

/// Commit one value, waiting out write stalls like a production caller would.
/// Returns the assigned sequence and how many stalls the commit waited through;
/// the stall time stays inside the measured commit latency.
fn commit_value(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    ordinal: usize,
    mode: CommitMode,
) -> (u64, u64) {
    let key = format!("{ordinal:0KEY_SIZE$}").into_bytes();
    let value = vec![u8::try_from(ordinal % 251).expect("value byte fits in u8"); VALUE_SIZE];
    let mut stalls = 0_u64;
    loop {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin hybrid lag transaction");
        tx.put(key.clone(), value.clone(), None)
            .expect("put hybrid lag value");
        match tx.commit_with_sequence(mode.write_options()) {
            Ok(sequence) => return (sequence, stalls),
            Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                stalls += 1;
                assert!(
                    engine
                        .wait_for_write_stall_clear(cf_id, Duration::from_secs(30))
                        .expect("wait for hybrid lag write stall"),
                    "hybrid lag write stall did not clear"
                );
            }
            Err(error) => panic!("commit hybrid lag value: {error}"),
        }
    }
}

fn wal_bytes(store: &SimulatedObjectStore) -> u64 {
    store.stored_bytes_under(&format!("{OBJECT_PREFIX}wal/"))
}

fn put_count(store: &SimulatedObjectStore) -> u64 {
    store
        .request_counts()
        .get(&Operation::Put)
        .copied()
        .unwrap_or(0)
}

fn execute_case(mode: CommitMode) -> LagSample {
    let cache = tempfile::tempdir().expect("local cache directory");
    let store = SimulatedObjectStore::new(Schedule::Eager);
    let engine = Arc::new(open_engine(mode, &store, &cache));
    let cf_id = engine
        .create_column_family("hybrid-lag")
        .expect("create hybrid lag column family")
        .id();
    let puts_before = put_count(&store);
    let bytes_before = wal_bytes(&store);
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = mode.is_cloud().then(|| spawn_sampler(&engine, &stop));

    let started_at = Instant::now();
    let mut commit_latencies_us = Vec::with_capacity(TRANSACTIONS);
    let mut probe_wait_us = Vec::new();
    let mut probe_timeouts = 0_u64;
    let mut write_stalls = 0_u64;
    let mut last_sequence = 0_u64;
    for ordinal in 0..TRANSACTIONS {
        let commit_started = Instant::now();
        let (sequence, stalls) = commit_value(&engine, cf_id, ordinal, mode);
        last_sequence = sequence;
        write_stalls += stalls;
        commit_latencies_us.push(duration_micros(commit_started.elapsed()));
        if mode == CommitMode::CloudAsync && ordinal % PROBE_EVERY == PROBE_EVERY - 1 {
            let wait_started = Instant::now();
            match engine.wait_cloud_durable(last_sequence, PROBE_TIMEOUT) {
                Ok(true) => probe_wait_us.push(duration_micros(wait_started.elapsed())),
                Ok(false) => probe_timeouts += 1,
                Err(error) => panic!("hybrid lag probe wait failed: {error}"),
            }
        }
    }
    let elapsed = started_at.elapsed();
    if mode.is_cloud() {
        // Drain so the request and byte totals cover every commit's upload.
        assert!(
            engine
                .wait_cloud_durable(last_sequence, DRAIN_TIMEOUT)
                .expect("drain cloud durability"),
            "cloud durability did not drain within {DRAIN_TIMEOUT:?}"
        );
    }
    stop.store(true, Ordering::Release);
    let (durable_lag_sequences, visible_lag_sequences, sampler_ticks) = sampler
        .map(|handle| handle.join().expect("join lag sampler"))
        .unwrap_or_default();
    let provider_puts = put_count(&store).saturating_sub(puts_before);
    let upload_bytes = wal_bytes(&store).saturating_sub(bytes_before);
    let mut engine = Arc::try_unwrap(engine)
        .unwrap_or_else(|_| panic!("all hybrid lag engine references must be released"));
    engine
        .shutdown(Duration::from_secs(30))
        .expect("shut down hybrid lag engine");

    LagSample {
        elapsed,
        completed: u64::try_from(TRANSACTIONS).expect("transaction count fits in u64"),
        commit_latencies_us,
        durable_lag_sequences,
        visible_lag_sequences,
        probe_wait_us,
        probe_timeouts,
        write_stalls,
        provider_puts,
        upload_bytes,
        committed_bytes: u64::try_from(TRANSACTIONS * (KEY_SIZE + VALUE_SIZE))
            .expect("committed bytes fit in u64"),
        sampler_ticks,
    }
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn u64_to_f64(value: u64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    {
        value as f64
    }
}

fn percentile(values: &[u64], numerator: usize, denominator: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = (sorted.len() * numerator).div_ceil(denominator).max(1);
    sorted[rank - 1]
}

/// Refuse a cloud row whose sampler fell too far behind to describe the lag,
/// and report the sample period the sampler actually achieved.
fn sampler_period_ms(sample: &LagSample) -> u64 {
    let elapsed_ms = u64::try_from(sample.elapsed.as_millis()).unwrap_or(u64::MAX);
    let nominal_ms = u64::try_from(SAMPLE_INTERVAL.as_millis())
        .unwrap_or(1)
        .max(1);
    let expected_ticks = elapsed_ms / nominal_ms;
    assert!(
        sample
            .sampler_ticks
            .saturating_mul(MIN_SAMPLER_TICK_FRACTION)
            >= expected_ticks,
        "lag sampler took {} ticks for a {:?} window; expected at least {}",
        sample.sampler_ticks,
        sample.elapsed,
        expected_ticks / MIN_SAMPLER_TICK_FRACTION
    );
    elapsed_ms / sample.sampler_ticks.max(1)
}

fn record_lag_observations(ctx: &mut StressContext, sample: &LagSample, period_ms: u64) {
    let observations = [
        (
            "cloud_durable_lag_p50_seq",
            u64_to_f64(percentile(&sample.durable_lag_sequences, 50, 100)),
            ObservationUnit::Count,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "cloud_durable_lag_p99_seq",
            u64_to_f64(percentile(&sample.durable_lag_sequences, 99, 100)),
            ObservationUnit::Count,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "cloud_durable_lag_max_seq",
            u64_to_f64(
                sample
                    .durable_lag_sequences
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0),
            ),
            ObservationUnit::Count,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "visible_ahead_of_cloud_p99_seq",
            u64_to_f64(percentile(&sample.visible_lag_sequences, 99, 100)),
            ObservationUnit::Count,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "cloud_durable_probe_wait_p50_us",
            u64_to_f64(percentile(&sample.probe_wait_us, 50, 100)),
            ObservationUnit::Microseconds,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "cloud_durable_probe_wait_p99_us",
            u64_to_f64(percentile(&sample.probe_wait_us, 99, 100)),
            ObservationUnit::Microseconds,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "cloud_durable_probe_timeouts",
            u64_to_f64(sample.probe_timeouts),
            ObservationUnit::Count,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "provider_puts_per_commit",
            u64_to_f64(sample.provider_puts) / u64_to_f64(sample.completed.max(1)),
            ObservationUnit::Ratio,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "upload_bytes_per_committed_byte",
            u64_to_f64(sample.upload_bytes) / u64_to_f64(sample.committed_bytes.max(1)),
            ObservationUnit::Ratio,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "lag_sampler_ticks",
            u64_to_f64(sample.sampler_ticks),
            ObservationUnit::Count,
            ObservationDirection::Informational,
        ),
        (
            "lag_sampler_period_ms",
            u64_to_f64(period_ms),
            ObservationUnit::Count,
            ObservationDirection::Informational,
        ),
        (
            "write_stalls",
            u64_to_f64(sample.write_stalls),
            ObservationUnit::Count,
            ObservationDirection::LowerIsBetter,
        ),
    ];
    for (name, value, unit, direction) in observations {
        ctx.record_observation(name, value, unit, direction);
    }
}

fn run_case(ctx: &mut StressContext, scenario: &'static str, mode: CommitMode) {
    ctx.parameter("logical_batch_size", TRANSACTIONS);
    ctx.parameter("logical_unit", "transaction");
    ctx.parameter(
        "storage_profile",
        if mode.is_cloud() {
            "simulated_cloud"
        } else {
            "local"
        },
    );
    ctx.parameter("commit_mode", mode.label());
    ctx.parameter("writers", 1);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("sample_interval_ms", SAMPLE_INTERVAL.as_millis());
    ctx.parameter("probe_every_commits", PROBE_EVERY);
    let sample = execute_case(mode);
    let period_ms = if mode.is_cloud() {
        sampler_period_ms(&sample)
    } else {
        0
    };
    ctx.record_external_outcome(
        scenario,
        sample.elapsed,
        LogicalUnit::new("transaction"),
        OperationOutcome::success(sample.completed),
    );
    for latency_us in &sample.commit_latencies_us {
        ctx.record_latency(Duration::from_micros((*latency_us).max(1)));
    }
    record_lag_observations(ctx, &sample, period_ms);
}

#[stress(tier = 2)]
fn tier2_hybrid_cloud_async_commit_vs_durable_lag_1_client(ctx: &mut StressContext) {
    run_case(
        ctx,
        "tier2_hybrid_cloud_async_commit_vs_durable_lag_1_client",
        CommitMode::CloudAsync,
    );
}

#[stress(tier = 2)]
fn tier2_hybrid_cloud_strict_commit_vs_durable_lag_1_client(ctx: &mut StressContext) {
    run_case(
        ctx,
        "tier2_hybrid_cloud_strict_commit_vs_durable_lag_1_client",
        CommitMode::CloudStrict,
    );
}

#[stress(tier = 2)]
fn tier2_hybrid_local_sync_commit_floor_1_client(ctx: &mut StressContext) {
    run_case(
        ctx,
        "tier2_hybrid_local_sync_commit_floor_1_client",
        CommitMode::LocalSync,
    );
}

stress_main!();
