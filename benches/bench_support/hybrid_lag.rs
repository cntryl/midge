//! Tier 2 - Hybrid durability lag
//!
//! Measures the two numbers the hybrid-cloud claim depends on, side by side:
//! commit latency at the caller and how far cloud durability trails it. One
//! client commits fixed work against the in-memory object-store simulator
//! while a sampler reads durability frontiers every few milliseconds, and
//! a probe commit waits for cloud durability every `PROBE_EVERY` commits to
//! turn the sequence lag into a time lag. Provider requests and uploaded bytes
//! come from the simulator, so every row also reports requests per commit and
//! bytes uploaded per committed byte. The local `sync()` row is the floor the
//! cloud rows are compared against.

#![allow(dead_code)]

#[cfg(feature = "stress-soak")]
#[path = "hybrid_lag/provider_observation.rs"]
mod provider_observation;
#[path = "../stress_config.rs"]
mod stress_config;

use cntryl_midge::__internal::simulation::{Operation, Schedule, SimulatedObjectStore};
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, ColumnFamilyId, Engine, OpenOptions,
    TransactionMode, WriteOptions,
};
use cntryl_stress::{
    LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome, StressContext,
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
pub(crate) enum CommitMode {
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
    /// Time between successive sampled increases in the cloud-durable
    /// frontier. This is quantized by the sampler period, not a catalog CAS
    /// completion timestamp.
    frontier_advance_intervals_us: Vec<u64>,
    frontier_advances: u64,
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
    sampler_max_gap_ms: u64,
    max_pending_uploads: usize,
    provider_requests: u64,
    native_transport: Option<Traffic>,
}

struct ClientSample {
    commit_latencies_us: Vec<u64>,
    probe_wait_us: Vec<u64>,
    probe_timeouts: u64,
    write_stalls: u64,
    last_sequence: u64,
}

struct SamplerSample {
    durable_lag_sequences: Vec<u64>,
    visible_lag_sequences: Vec<u64>,
    frontier_advance_intervals_us: Vec<u64>,
    frontier_advances: u64,
    ticks: u64,
    max_gap_ms: u64,
    max_pending_uploads: usize,
}

#[derive(Clone, Copy, Default)]
struct Traffic {
    requests: u64,
    puts: u64,
    bytes: u64,
    response_bytes: u64,
    transport_errors: u64,
    cancelled: u64,
    malformed: u64,
}

#[derive(Default)]
struct FrontierAdvanceIntervals {
    last_frontier: Option<u64>,
    last_advance_at: Option<Instant>,
    intervals_us: Vec<u64>,
    advances: u64,
}

impl FrontierAdvanceIntervals {
    fn observe(&mut self, frontier: u64, observed_at: Instant) {
        match self.last_frontier {
            Some(previous) if frontier > previous => {
                if let Some(previous_advance_at) = self.last_advance_at {
                    self.intervals_us.push(duration_micros(
                        observed_at.saturating_duration_since(previous_advance_at),
                    ));
                }
                self.last_frontier = Some(frontier);
                self.last_advance_at = Some(observed_at);
                self.advances = self.advances.saturating_add(1);
            }
            Some(previous) if frontier < previous => {
                self.last_frontier = Some(frontier);
                self.last_advance_at = None;
            }
            None => self.last_frontier = Some(frontier),
            _ => {}
        }
    }
}

impl Traffic {
    fn since(self, before: Self) -> Self {
        Self {
            requests: self.requests.saturating_sub(before.requests),
            puts: self.puts.saturating_sub(before.puts),
            bytes: self.bytes.saturating_sub(before.bytes),
            response_bytes: self.response_bytes.saturating_sub(before.response_bytes),
            transport_errors: self
                .transport_errors
                .saturating_sub(before.transport_errors),
            cancelled: self.cancelled.saturating_sub(before.cancelled),
            malformed: self.malformed.saturating_sub(before.malformed),
        }
    }
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
        Engine::open(local_floor_options()).expect("open local engine")
    }
}

pub(crate) fn local_floor_options() -> OpenOptions {
    let mut opts = stress_config::write_coordination_opts_for_mode("local");
    opts.enable_compaction = false;
    opts.to_open_options()
}

#[cfg(feature = "stress-soak")]
pub(crate) fn sqrzl_options(cache: &std::path::Path, namespace: &str) -> OpenOptions {
    let endpoint = std::env::var("MIDGE_STRESS_SQRZL_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let provider = CloudProviderConfig::sqrzl_s3(namespace)
        .with_endpoint(endpoint)
        .expect("Sqrzl endpoint");
    OpenOptions::cloud(cache, CloudStorageLocation::new(provider, OBJECT_PREFIX))
        .background_compaction(true)
        .build()
        .expect("provider lag options")
}

fn spawn_sampler(
    engine: &Arc<Engine>,
    stop: &Arc<AtomicBool>,
) -> std::thread::JoinHandle<SamplerSample> {
    let engine = Arc::clone(engine);
    let stop = Arc::clone(stop);
    std::thread::spawn(move || {
        let mut durable = Vec::new();
        let mut visible = Vec::new();
        let mut ticks = 0_u64;
        let mut previous = Instant::now();
        let mut max_gap = Duration::ZERO;
        let mut max_pending_uploads = 0;
        let mut frontier_advances = FrontierAdvanceIntervals::default();
        while !stop.load(Ordering::Acquire) {
            let metrics = engine.metrics();
            let frontiers = metrics.get_durability_frontiers();
            if stop.load(Ordering::Acquire) {
                break;
            }
            let now = Instant::now();
            max_gap = max_gap.max(now.saturating_duration_since(previous));
            previous = now;
            frontier_advances.observe(frontiers.wal_cloud_durable_seq, now);
            durable.push(
                frontiers
                    .wal_local_durable_seq
                    .saturating_sub(frontiers.wal_cloud_durable_seq),
            );
            visible.push(
                frontiers
                    .current_sequence
                    .saturating_sub(frontiers.wal_cloud_durable_seq),
            );
            max_pending_uploads = max_pending_uploads.max(metrics.get_pending_cloud_uploads());
            ticks += 1;
            std::thread::sleep(SAMPLE_INTERVAL);
        }
        max_gap = max_gap.max(previous.elapsed());
        SamplerSample {
            durable_lag_sequences: durable,
            visible_lag_sequences: visible,
            frontier_advance_intervals_us: frontier_advances.intervals_us,
            frontier_advances: frontier_advances.advances,
            ticks,
            max_gap_ms: u64::try_from(max_gap.as_millis()).unwrap_or(u64::MAX),
            max_pending_uploads,
        }
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

fn execute_writers(
    engine: &Arc<Engine>,
    cf_id: ColumnFamilyId,
    mode: CommitMode,
    writers: usize,
) -> Vec<ClientSample> {
    assert!(writers > 0, "hybrid lag rows need at least one writer");
    assert_eq!(
        TRANSACTIONS % writers,
        0,
        "the fixed transaction count must divide evenly across writers"
    );
    let transactions_per_writer = TRANSACTIONS / writers;
    std::thread::scope(|scope| {
        let handles = (0..writers)
            .map(|writer| {
                let engine = Arc::clone(engine);
                scope.spawn(move || {
                    let mut sample = ClientSample {
                        commit_latencies_us: Vec::with_capacity(transactions_per_writer),
                        probe_wait_us: Vec::new(),
                        probe_timeouts: 0,
                        write_stalls: 0,
                        last_sequence: 0,
                    };
                    for offset in 0..transactions_per_writer {
                        let ordinal = writer * transactions_per_writer + offset;
                        let started = Instant::now();
                        let (sequence, stalls) = commit_value(&engine, cf_id, ordinal, mode);
                        sample.last_sequence = sample.last_sequence.max(sequence);
                        sample.write_stalls += stalls;
                        sample
                            .commit_latencies_us
                            .push(duration_micros(started.elapsed()));
                        if mode == CommitMode::CloudAsync
                            && ordinal % PROBE_EVERY == PROBE_EVERY - 1
                        {
                            let wait_started = Instant::now();
                            match engine.wait_cloud_durable(sample.last_sequence, PROBE_TIMEOUT) {
                                Ok(true) => sample
                                    .probe_wait_us
                                    .push(duration_micros(wait_started.elapsed())),
                                Ok(false) => sample.probe_timeouts += 1,
                                Err(error) => panic!("hybrid lag probe wait failed: {error}"),
                            }
                        }
                    }
                    sample
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("hybrid lag writer thread"))
            .collect()
    })
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

fn execute_case(mode: CommitMode, writers: usize) -> LagSample {
    let cache = tempfile::tempdir().expect("local cache directory");
    let store = SimulatedObjectStore::new(Schedule::Eager);
    let engine = Arc::new(open_engine(mode, &store, &cache));
    execute_open_engine(mode, engine, false, writers, || Traffic {
        requests: store.request_counts().values().copied().sum(),
        puts: put_count(&store),
        bytes: wal_bytes(&store),
        ..Default::default()
    })
}

fn execute_open_engine(
    mode: CommitMode,
    engine: Arc<Engine>,
    native: bool,
    writers: usize,
    counters: impl Fn() -> Traffic,
) -> LagSample {
    let cf_id = engine
        .create_column_family("hybrid-lag")
        .expect("create hybrid lag column family")
        .id();
    let before = counters();
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = mode.is_cloud().then(|| spawn_sampler(&engine, &stop));

    let started_at = Instant::now();
    let client_samples = execute_writers(&engine, cf_id, mode, writers);
    let elapsed = started_at.elapsed();
    let mut commit_latencies_us = Vec::with_capacity(TRANSACTIONS);
    let mut probe_wait_us = Vec::new();
    let mut probe_timeouts = 0_u64;
    let mut write_stalls = 0_u64;
    let mut last_sequence = 0_u64;
    for sample in client_samples {
        commit_latencies_us.extend(sample.commit_latencies_us);
        probe_wait_us.extend(sample.probe_wait_us);
        probe_timeouts += sample.probe_timeouts;
        write_stalls += sample.write_stalls;
        last_sequence = last_sequence.max(sample.last_sequence);
    }
    // A final drain must not manufacture sampler health for the measured
    // commit window.
    stop.store(true, Ordering::Release);
    let sampler = sampler.map_or(
        SamplerSample {
            durable_lag_sequences: Vec::new(),
            visible_lag_sequences: Vec::new(),
            frontier_advance_intervals_us: Vec::new(),
            frontier_advances: 0,
            ticks: 0,
            max_gap_ms: 0,
            max_pending_uploads: 0,
        },
        |handle| handle.join().expect("join lag sampler"),
    );
    if mode.is_cloud() {
        // Drain so the request and byte totals cover every commit's upload.
        assert!(
            engine
                .wait_cloud_durable(last_sequence, DRAIN_TIMEOUT)
                .expect("drain cloud durability"),
            "cloud durability did not drain within {DRAIN_TIMEOUT:?}"
        );
    }
    let mut engine = Arc::try_unwrap(engine)
        .unwrap_or_else(|_| panic!("all hybrid lag engine references must be released"));
    engine
        .shutdown(Duration::from_secs(30))
        .expect("shut down hybrid lag engine");
    // Worker join settles transport observations. Counter scope includes the
    // final durability drain and shutdown, while latency excludes both.
    let traffic = counters().since(before);

    LagSample {
        elapsed,
        completed: u64::try_from(commit_latencies_us.len()).expect("transaction count fits in u64"),
        commit_latencies_us,
        durable_lag_sequences: sampler.durable_lag_sequences,
        visible_lag_sequences: sampler.visible_lag_sequences,
        frontier_advance_intervals_us: sampler.frontier_advance_intervals_us,
        frontier_advances: sampler.frontier_advances,
        probe_wait_us,
        probe_timeouts,
        write_stalls,
        provider_puts: traffic.puts,
        upload_bytes: traffic.bytes,
        committed_bytes: u64::try_from(TRANSACTIONS * (KEY_SIZE + VALUE_SIZE))
            .expect("committed bytes fit in u64"),
        sampler_ticks: sampler.ticks,
        sampler_max_gap_ms: sampler.max_gap_ms,
        max_pending_uploads: sampler.max_pending_uploads,
        provider_requests: traffic.requests,
        native_transport: native.then_some(traffic),
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
        sample.sampler_ticks > 0,
        "cloud lag sampler produced no observations"
    );
    let gap_limit = nominal_ms.saturating_mul(MIN_SAMPLER_TICK_FRACTION);
    assert!(
        sample.sampler_max_gap_ms <= gap_limit,
        "cloud lag sampler gap {}ms exceeds {gap_limit}ms; row is inconclusive",
        sample.sampler_max_gap_ms
    );
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
            "provider_requests_per_commit",
            u64_to_f64(sample.provider_requests) / u64_to_f64(sample.completed.max(1)),
            ObservationUnit::Ratio,
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
    record_queue_observations(ctx, sample);
}

fn record_queue_observations(ctx: &mut StressContext, sample: &LagSample) {
    ctx.record_observation(
        "pending_cloud_uploads_sampled_max",
        u64_to_f64(u64::try_from(sample.max_pending_uploads).unwrap_or(u64::MAX)),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
}

pub(crate) fn run_case(ctx: &mut StressContext, scenario: &'static str, mode: CommitMode) {
    run_case_with_writers(ctx, scenario, mode, 1);
}

pub(crate) fn run_case_with_writers(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: CommitMode,
    writers: usize,
) {
    run_measured_case(
        ctx,
        scenario,
        mode,
        writers,
        if mode.is_cloud() {
            "simulated_cloud"
        } else {
            "local"
        },
        || execute_case(mode, writers),
    );
}

#[cfg(feature = "stress-soak")]
pub(crate) fn run_provider_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: CommitMode,
    options: OpenOptions,
) {
    run_provider_case_with_writers(ctx, scenario, mode, options, 1);
}

#[cfg(feature = "stress-soak")]
pub(crate) fn run_provider_case_with_writers(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: CommitMode,
    options: OpenOptions,
    writers: usize,
) {
    let counters = provider_observation::install();
    let window = counters.begin();
    run_measured_case(
        ctx,
        scenario,
        mode,
        writers,
        if mode.is_cloud() { "sqrzl_s3" } else { "local" },
        || {
            let engine = Arc::new(Engine::open(options).expect("open provider lag engine"));
            execute_open_engine(mode, engine, mode.is_cloud(), writers, || {
                counters.snapshot()
            })
        },
    );
    drop(window);
}

fn run_measured_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: CommitMode,
    writers: usize,
    storage: &'static str,
    execute: impl FnOnce() -> LagSample,
) {
    ctx.parameter("logical_batch_size", TRANSACTIONS);
    ctx.parameter("logical_unit", "transaction");
    ctx.parameter("storage_profile", storage);
    ctx.parameter(
        "counter_scope",
        if storage == "local" {
            "none_local_floor"
        } else if storage == "sqrzl_s3" {
            "all_engine_http_attempts_and_request_body_bytes_including_retries"
        } else {
            "simulator_requests_and_resident_wal_bytes"
        },
    );
    ctx.parameter("commit_mode", mode.label());
    ctx.parameter("writers", writers);
    ctx.parameter("transactions_per_writer", TRANSACTIONS / writers);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("sample_interval_ms", SAMPLE_INTERVAL.as_millis());
    record_cloud_measurement_parameters(ctx, mode);
    ctx.parameter("probe_every_commits", PROBE_EVERY);
    let sample = execute();
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
    if mode.is_cloud() {
        record_frontier_advance_observations(ctx, &sample);
    }
    ctx.record_observation(
        "lag_sampler_max_gap_ms",
        u64_to_f64(sample.sampler_max_gap_ms),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    for (name, value) in [
        (
            "commit_p50_us",
            percentile(&sample.commit_latencies_us, 50, 100),
        ),
        (
            "commit_p99_us",
            percentile(&sample.commit_latencies_us, 99, 100),
        ),
    ] {
        ctx.record_observation(
            name,
            u64_to_f64(value),
            ObservationUnit::Microseconds,
            ObservationDirection::LowerIsBetter,
        );
    }
    assert_eq!(
        sample.probe_timeouts, 0,
        "cloud durability probes timed out; row is inconclusive"
    );
    if let Some(traffic) = sample.native_transport {
        for (name, value) in [
            ("provider_response_body_bytes", traffic.response_bytes),
            ("provider_transport_errors", traffic.transport_errors),
            ("provider_cancelled_attempts", traffic.cancelled),
            ("provider_malformed_observations", traffic.malformed),
        ] {
            ctx.record_observation(
                name,
                u64_to_f64(value),
                ObservationUnit::Count,
                ObservationDirection::Informational,
            );
        }
        assert!(
            traffic.requests > 0 && traffic.puts > 0 && traffic.bytes > 0,
            "provider cost observations unavailable; row is inconclusive"
        );
        assert_eq!(
            traffic.malformed, 0,
            "provider attempt fields incomplete; row is inconclusive"
        );
        assert_eq!(
            traffic.cancelled, 0,
            "provider attempts did not settle; row is inconclusive"
        );
    }
}

fn record_cloud_measurement_parameters(ctx: &mut StressContext, mode: CommitMode) {
    if !mode.is_cloud() {
        return;
    }
    ctx.parameter(
        "frontier_interval_measurement",
        "observed_cloud_durable_frontier_advance_interarrival",
    );
    ctx.parameter(
        "frontier_interval_nominal_sample_period_ms",
        SAMPLE_INTERVAL.as_millis(),
    );
    let upload_policy = cntryl_midge::CloudWritePolicy::default();
    ctx.parameter(
        "cloud_wal_upload_concurrency",
        upload_policy.wal_upload_concurrency,
    );
    ctx.parameter(
        "cloud_wal_upload_memory_cap_bytes",
        upload_policy.wal_upload_memory_bytes,
    );
}

fn record_frontier_advance_observations(ctx: &mut StressContext, sample: &LagSample) {
    assert!(
        !sample.frontier_advance_intervals_us.is_empty(),
        "cloud durable frontier produced fewer than two sampled advances; row is inconclusive"
    );
    ctx.record_observation(
        "observed_cloud_durable_frontier_advances",
        u64_to_f64(sample.frontier_advances),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    for (name, numerator) in [
        ("observed_cloud_frontier_interarrival_p50_us", 50),
        ("observed_cloud_frontier_interarrival_p99_us", 99),
    ] {
        ctx.record_observation(
            name,
            u64_to_f64(percentile(
                &sample.frontier_advance_intervals_us,
                numerator,
                100,
            )),
            ObservationUnit::Microseconds,
            ObservationDirection::Informational,
        );
    }
    let maximum = sample
        .frontier_advance_intervals_us
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    ctx.record_observation(
        "observed_cloud_frontier_interarrival_max_us",
        u64_to_f64(maximum),
        ObservationUnit::Microseconds,
        ObservationDirection::Informational,
    );
}

#[cfg(test)]
#[allow(
    unused_imports,
    reason = "Harnessless benches compile cfg(test) helpers"
)]
mod tests {
    use super::*;

    #[test]
    fn should_record_intervals_between_observed_cloud_frontier_advances() {
        // Arrange
        let started_at = Instant::now();
        let mut intervals = FrontierAdvanceIntervals::default();

        // Act
        intervals.observe(4, started_at);
        intervals.observe(4, started_at + Duration::from_millis(10));
        intervals.observe(7, started_at + Duration::from_millis(25));
        intervals.observe(9, started_at + Duration::from_millis(40));

        // Assert
        assert_eq!(intervals.advances, 2);
        assert_eq!(intervals.intervals_us, vec![15_000]);
    }

    #[test]
    fn should_reset_interarrival_baseline_when_cloud_frontier_regresses() {
        // Arrange
        let started_at = Instant::now();
        let mut intervals = FrontierAdvanceIntervals::default();

        // Act
        intervals.observe(8, started_at);
        intervals.observe(9, started_at + Duration::from_millis(10));
        intervals.observe(10, started_at + Duration::from_millis(20));
        intervals.observe(0, started_at + Duration::from_millis(30));
        intervals.observe(1, started_at + Duration::from_millis(40));
        intervals.observe(2, started_at + Duration::from_millis(55));

        // Assert
        assert_eq!(intervals.advances, 4);
        assert_eq!(intervals.intervals_us, vec![10_000, 15_000]);
    }
}
