//! Shared Tier 5 and Tier 6 workloads for Midge's isolated stress benches.

use cntryl_midge::{
    Bytes, CloudProviderConfig, CloudStorageLocation, CloudWritePolicy, ColumnFamilyHandle, Engine,
    MemoryBudget, MidgeError, OpenOptions, Query, RuntimeMetricsSnapshot, TransactionMode,
    WorkloadProfile, WriteOptions,
};
use cntryl_stress::{
    LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome, ProgressHandle,
    StressContext,
};
use hdrhistogram::Histogram;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions as FileOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "stress_scenarios/client_progress.rs"]
mod client_progress;
use client_progress::{
    run_client_with, write_atomic_json, ClientClock, ClientControl, ClientReporter, WorkerResult,
    CLIENT_REPORT_PERIOD,
};
#[path = "stress_scenarios/recovery_progress.rs"]
mod recovery_progress;
use recovery_progress::{is_recovery_work, RecoveryProgressLayer, RecoveryScope};
#[path = "stress_scenarios/checkpoint_accounting.rs"]
mod checkpoint_accounting;
use checkpoint_accounting::CheckpointAccounting;
#[path = "stress_scenarios/admission.rs"]
mod admission;
#[cfg(all(test, feature = "failpoints"))]
#[path = "stress_scenarios/final_flush_watchdog.rs"]
mod final_flush_watchdog;
#[path = "stress_scenarios/latency.rs"]
mod latency;
#[cfg(test)]
#[path = "stress_scenarios/watchdog_preparation.rs"]
mod watchdog_preparation;
#[cfg(all(test, feature = "failpoints"))]
#[allow(
    unused_imports,
    reason = "Only the watchdog integration target uses these shared fixture exports"
)]
pub(super) use final_flush_watchdog::{
    prepare_final_flush_watchdog_fixture, run_final_flush_terminal_policy_fixture,
    run_final_flush_watchdog_fixture, FlushFixtureKind,
};
#[cfg(test)]
pub(super) use watchdog_preparation::PreparationGuard;

const VALUE_SIZE: usize = 128;
const SEED_ROWS: usize = 512;
const SEED_BATCH: usize = 64;
const WRITE_BATCH_ROWS: usize = 32;
const MIXED_ROWS: [usize; 5] = [1, 8, 128, 1_024, 4_096];
const WRITE_ROW_COUNTS: [usize; 11] = [32, 1, 1, 1, 8, 128, 1_024, 4_096, 1, 1, 128];
const RESOURCE_SAMPLE_PERIOD: Duration = Duration::from_secs(10);
const SATURATION_BACKOFF_MAX_SHIFT: u32 = 8;
const SATURATION_BACKOFF_RECOVERY_SUCCESSES: u32 = 32;
const CLOUD_STRESS_WAL_SEAL_MAX_FLUSH_DELAY: Duration = Duration::from_secs(5);
const SHUTDOWN_CALLER_BUDGET: Duration = Duration::from_secs(45);
const FINAL_FLUSH_CALLER_SLICE: Duration = Duration::from_secs(1);
const FINAL_FLUSH_INITIAL_BACKOFF: Duration = Duration::from_millis(25);
const FINAL_FLUSH_MAX_BACKOFF: Duration = Duration::from_millis(200);

fn enable_phase_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer as _;

    static INITIALIZE: std::sync::Once = std::sync::Once::new();
    INITIALIZE.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            tracing_subscriber::EnvFilter::new(
                "warn,cntryl_midge::runtime::event_loop::shutdown=info,cntryl_midge::engine::lease_state=info,cntryl_midge::lease=info,midge::recovery=info,midge::recovery::work=off",
            )
        });
        tracing_subscriber::registry()
            .with(RecoveryProgressLayer.with_filter(tracing_subscriber::filter::filter_fn(
                is_recovery_work,
            )))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_ansi(false)
                    .with_filter(filter),
            )
            .try_init()
            .expect("install stress phase tracing and recovery progress listener");
    });
}

#[derive(Clone, Copy)]
pub(super) struct WorkloadCase {
    pub(super) benchmark: &'static str,
    pub(super) scenario: &'static str,
    pub(super) backend: &'static str,
    pub(super) tier: u8,
    pub(super) workload: &'static str,
    pub(super) stages: &'static [usize],
}

#[derive(Debug, Clone, Copy, Default)]
struct SaturationCounts {
    resource_limit: u64,
    write_stall: u64,
    backoff_ms: u64,
}

#[derive(Default)]
struct SaturationBackoff {
    level: u32,
    successful_operations_since_stall: u32,
}

impl SaturationBackoff {
    fn on_saturation(&mut self) -> u64 {
        self.level = self
            .level
            .saturating_add(1)
            .min(SATURATION_BACKOFF_MAX_SHIFT + 1);
        self.successful_operations_since_stall = 0;
        saturation_backoff_ms(self.level)
    }

    fn on_success(&mut self) {
        if self.level == 0 {
            return;
        }
        self.successful_operations_since_stall =
            self.successful_operations_since_stall.saturating_add(1);
        if self.successful_operations_since_stall >= SATURATION_BACKOFF_RECOVERY_SUCCESSES {
            self.level = self.level.saturating_sub(1);
            self.successful_operations_since_stall = 0;
        }
    }
}

#[derive(Debug)]
struct StageStats {
    attempts: u64,
    acknowledged: u64,
    logical_operations: u64,
    acknowledged_rows: u64,
    saturation: SaturationCounts,
    latency_us: Histogram<u64>,
    latency: latency::LatencyMetrics,
}

impl Default for StageStats {
    fn default() -> Self {
        Self {
            attempts: 0,
            acknowledged: 0,
            logical_operations: 0,
            acknowledged_rows: 0,
            saturation: SaturationCounts::default(),
            latency_us: Histogram::new(3).expect("create transaction latency histogram"),
            latency: latency::LatencyMetrics::default(),
        }
    }
}

impl StageStats {
    fn merge(&mut self, other: &Self) {
        self.attempts = self.attempts.saturating_add(other.attempts);
        self.acknowledged = self.acknowledged.saturating_add(other.acknowledged);
        self.logical_operations = self
            .logical_operations
            .saturating_add(other.logical_operations);
        self.acknowledged_rows = self
            .acknowledged_rows
            .saturating_add(other.acknowledged_rows);
        self.saturation.resource_limit = self
            .saturation
            .resource_limit
            .saturating_add(other.saturation.resource_limit);
        self.saturation.write_stall = self
            .saturation
            .write_stall
            .saturating_add(other.saturation.write_stall);
        self.saturation.backoff_ms = self
            .saturation
            .backoff_ms
            .saturating_add(other.saturation.backoff_ms);
        self.latency_us
            .add(&other.latency_us)
            .expect("merge transaction latency histogram");
        self.latency.merge(&other.latency);
    }

    fn record_latency(&mut self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        self.latency_us
            .record(micros)
            .expect("record transaction latency");
    }
}

#[derive(Default, serde::Serialize)]
struct FinalFlushReport {
    attempts: u64,
    completed: bool,
    operation_in_flight: bool,
    timeout_responses: u64,
    busy_responses: u64,
    write_stall_responses: u64,
    backoff_ms: u64,
    last_error: Option<String>,
    last_error_kind: Option<&'static str>,
    observed_primary_lease_healthy: Option<bool>,
}

struct WorkloadArtifacts {
    path: PathBuf,
    started: Instant,
    started_unix_ms: u128,
    phase_started_elapsed_ms: u128,
    resource_phase: Arc<AtomicU64>,
    benchmark: &'static str,
    git_commit: String,
    binary_sha256: Option<String>,
    source_worktree_clean: bool,
    stage_index: Option<usize>,
    shutdown_results: Vec<serde_json::Value>,
    terminal_error: Option<String>,
    final_flush: FinalFlushReport,
    scenario: &'static str,
    backend: &'static str,
    configured_seconds: u64,
    phase: &'static str,
    stage: String,
    attempts: u64,
    acknowledged: u64,
    acknowledged_rows: u64,
    resource_limit: u64,
    write_stall: u64,
    saturation_backoff_ms: u64,
    latency_stats: StageStats,
    progress_units: u64,
    checks: Vec<serde_json::Value>,
    complete: bool,
}

impl WorkloadArtifacts {
    fn begin(case: WorkloadCase, duration: Duration) -> Self {
        let started = Instant::now();
        let base = std::env::var_os("MIDGE_STRESS_ARTIFACT_DIR")
            .map_or_else(|| PathBuf::from("target/midge-stress"), PathBuf::from);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_millis();
        let path = base.join(format!(
            "{}-{}-{}-{time}",
            case.scenario,
            case.backend,
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create workload artifact directory");
        let artifacts = Self {
            path,
            started,
            started_unix_ms: time,
            phase_started_elapsed_ms: 0,
            resource_phase: Arc::new(AtomicU64::new(0)),
            benchmark: case.benchmark,
            git_commit: current_commit(),
            binary_sha256: std::env::var("MIDGE_STRESS_BINARY_SHA256").ok(),
            source_worktree_clean: std::process::Command::new("git")
                .args(["diff", "HEAD", "--quiet"])
                .status()
                .is_ok_and(|status| status.success()),
            stage_index: None,
            shutdown_results: Vec::new(),
            terminal_error: None,
            final_flush: FinalFlushReport::default(),
            scenario: case.scenario,
            backend: case.backend,
            configured_seconds: duration.as_secs(),
            phase: "setup",
            stage: String::new(),
            attempts: 0,
            acknowledged: 0,
            acknowledged_rows: 0,
            resource_limit: 0,
            write_stall: 0,
            saturation_backoff_ms: 0,
            latency_stats: StageStats::default(),
            progress_units: 0,
            checks: Vec::new(),
            complete: false,
        };
        fs::write(
            artifacts.path.join("phase-events.csv"),
            "elapsed_ms,unix_ms,phase,stage\n",
        )
        .expect("write phase event header");
        artifacts.append_phase_event();
        artifacts.persist("running");
        artifacts
    }

    fn record_stage(&mut self, stage: &str, stats: &StageStats, progress: &ProgressHandle) {
        self.latency_stats.merge(stats);
        self.phase = "workload";
        self.stage = stage.to_string();
        self.stage_index = None;
        self.attempts = self.attempts.saturating_add(stats.attempts);
        self.acknowledged = self.acknowledged.saturating_add(stats.acknowledged);
        self.acknowledged_rows = self
            .acknowledged_rows
            .saturating_add(stats.acknowledged_rows);
        self.resource_limit = self
            .resource_limit
            .saturating_add(stats.saturation.resource_limit);
        self.write_stall = self
            .write_stall
            .saturating_add(stats.saturation.write_stall);
        self.saturation_backoff_ms = self
            .saturation_backoff_ms
            .saturating_add(stats.saturation.backoff_ms);
        self.progress_units = progress.completed_units();
        append_stage_csv(&self.path, stage, stats);
        self.persist("running");
    }

    fn record_terminal_errors(&mut self, outcomes: &[WorkerResult], progress: &ProgressHandle) {
        let errors: Vec<_> = outcomes
            .iter()
            .enumerate()
            .filter_map(|(client_index, outcome)| {
                outcome.terminal_error.as_ref().map(
                    |error| json!({ "client_index": client_index, "error": error.to_string() }),
                )
            })
            .collect();
        self.terminal_error = errors
            .first()
            .and_then(|error| error["error"].as_str())
            .map(str::to_string);
        self.progress_units = progress.completed_units();
        write_atomic_json(
            &self.path.join("stage-failure.json"),
            &json!({ "phase": self.phase, "stage": self.stage, "errors": errors }),
        )
        .expect("publish failed stage details");
        self.persist("failed");
    }

    fn verification(
        &mut self,
        phase: &str,
        expected_rows: u64,
        actual_rows: u64,
        mismatches: u64,
        progress: &ProgressHandle,
    ) {
        self.phase = "verification";
        self.stage = phase.to_string();
        let passed = expected_rows == actual_rows && mismatches == 0;
        self.checks.push(json!({
            "phase": phase,
            "expected_rows": expected_rows,
            "actual_rows": actual_rows,
            "value_mismatches": mismatches,
            "passed": passed,
        }));
        write_atomic_json(
            &self.path.join("verification-summary.json"),
            &json!({
                "scenario": self.scenario,
                "backend": self.backend,
                "passed": self.checks.iter().all(|check| check["passed"] == true),
                "checks": self.checks,
            }),
        )
        .expect("write verification summary");
        if passed {
            progress.advance();
        }
        self.progress_units = progress.completed_units();
        self.persist(if passed { "running" } else { "failed" });
        assert!(
            passed,
            "{phase} verification failed; see artifacts: {}",
            self.path.display()
        );
    }

    fn enter_phase(&mut self, phase: &'static str, stage: &str, progress: &ProgressHandle) {
        // Bit zero permits storage progress; higher bits identify the phase.
        // A new generation invalidates any sampler baseline from earlier work.
        let generation = self.resource_phase.load(Ordering::Relaxed).wrapping_add(2) & !1;
        self.resource_phase.store(
            generation | u64::from(storage_progress_phase(phase)),
            Ordering::Release,
        );
        self.phase = phase;
        self.stage = stage.to_string();
        self.stage_index = None;
        self.phase_started_elapsed_ms = self.started.elapsed().as_millis();
        self.append_phase_event();
        self.progress_units = progress.completed_units();
        self.persist("running");
    }

    fn enter_stage(&mut self, index: usize, stage: &str, progress: &ProgressHandle) {
        self.enter_phase("workload", stage, progress);
        self.stage_index = Some(index);
        self.persist("running");
    }

    fn append_phase_event(&self) {
        let mut output = FileOptions::new()
            .append(true)
            .open(self.path.join("phase-events.csv"))
            .expect("open phase event report");
        writeln!(
            output,
            "{},{},{},{}",
            self.phase_started_elapsed_ms,
            self.started_unix_ms + self.phase_started_elapsed_ms,
            self.phase,
            self.stage,
        )
        .expect("write phase event");
    }

    fn record_shutdown(&mut self, stage: &str, result: &Result<(), MidgeError>, elapsed: Duration) {
        let caller_result = match result {
            Ok(()) => "ok",
            Err(MidgeError::Timeout(_)) => "timeout",
            Err(MidgeError::Fenced(_)) => "fenced",
            Err(_) => "error",
        };
        self.shutdown_results.push(json!({
            "stage": stage,
            "caller_result": caller_result,
            "caller_budget_ms": SHUTDOWN_CALLER_BUDGET.as_millis(),
            "elapsed_ms": elapsed.as_millis(),
            "recorded_elapsed_ms": self.started.elapsed().as_millis(),
            "error": result.as_ref().err().map(|error| format!("{error:?}")),
        }));
        self.persist(if result.is_ok() { "running" } else { "failed" });
    }

    fn finish(&mut self, progress: &ProgressHandle) {
        self.enter_phase("complete", "flush-reopen-recovery", progress);
        self.complete = true;
        self.persist("passed");
    }

    fn persist(&self, status: &str) {
        write_atomic_json(
            &self.path.join("workload-status.json"),
            &json!({
                "benchmark_workload": self.benchmark,
                "git_commit": self.git_commit,
                "binary_sha256": self.binary_sha256,
                "source_worktree_clean": self.source_worktree_clean,
                "process_id": std::process::id(),
                "scenario": self.scenario,
                "backend": self.backend,
                "status": status,
                "phase": self.phase,
                "stage": self.stage,
                "stage_index": self.stage_index,
                "phase_started_elapsed_ms": self.phase_started_elapsed_ms,
                "phase_started_unix_ms": self.started_unix_ms + self.phase_started_elapsed_ms,
                "shutdown_results": self.shutdown_results,
                "configured_duration_seconds": self.configured_seconds,
                "measurement_topology": if std::env::var_os("MIDGE_STRESS_COMPARISON_CLIENTS").is_some() { "independent_fresh_process" } else { "cumulative_client_ramp" },
                "comparison_repeat": std::env::var("MIDGE_COMPARISON_REPEAT").ok(),
                "attempted_transactions": self.attempts,
                "acknowledged_transactions": self.acknowledged,
                "acknowledged_rows": self.acknowledged_rows,
                "resource_limit_responses": self.resource_limit,
                "write_stall_responses": self.write_stall,
                "saturation_backoff_ms": self.saturation_backoff_ms,
                "latency": self.latency_stats.latency.summary(&self.latency_stats.latency_us),
                "progress_completed_units": self.progress_units,
                "verification_checks": self.checks.len(),
                "terminal_error": self.terminal_error,
                "final_flush": self.final_flush,
            }),
        )
        .expect("write workload status");
    }
}

impl Drop for WorkloadArtifacts {
    fn drop(&mut self) {
        if !self.complete {
            self.persist("failed");
        }
    }
}

fn current_commit() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map_or_else(|| "unknown".to_string(), |commit| commit.trim().to_string())
}

fn storage_progress_phase(phase: &str) -> bool {
    matches!(phase, "flush" | "recovery")
}

fn resource_progress_observed(
    previous_phase: u64,
    before_phase: u64,
    after_phase: u64,
    previous_bytes: u64,
    current_bytes: u64,
) -> bool {
    before_phase & 1 != 0
        && previous_phase == before_phase
        && before_phase == after_phase
        && previous_bytes != current_bytes
}

struct ResourceProgress {
    phase: u64,
    database_bytes: u64,
}

impl ResourceProgress {
    fn observe(&mut self, before_phase: u64, after_phase: u64, database_bytes: u64) -> bool {
        let observed = resource_progress_observed(
            self.phase,
            before_phase,
            after_phase,
            self.database_bytes,
            database_bytes,
        );
        // A phase change during a scan also invalidates the next comparison.
        self.phase = before_phase;
        self.database_bytes = database_bytes;
        observed
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ResourcePoint {
    elapsed_ms: u128,
    rss_bytes: Option<u64>,
    database_bytes: u64,
}

struct ResourceSampler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    started: Instant,
    database: PathBuf,
    report: PathBuf,
    start: ResourcePoint,
    peak: Arc<Mutex<Option<u64>>>,
}

impl ResourceSampler {
    fn start(artifacts: &WorkloadArtifacts, database: &Path, progress: ProgressHandle) -> Self {
        let interval = std::env::var("MIDGE_STRESS_RESOURCE_SAMPLE_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map_or(RESOURCE_SAMPLE_PERIOD, Duration::from_secs);
        Self::start_with_interval(artifacts, database, progress, interval)
    }

    fn start_with_interval(
        artifacts: &WorkloadArtifacts,
        database: &Path,
        progress: ProgressHandle,
        interval: Duration,
    ) -> Self {
        let started = artifacts.started;
        let report = artifacts.path.join("resource-samples.csv");
        fs::write(&report, "elapsed_ms,rss_bytes,database_disk_bytes\n")
            .expect("write resource sample header");
        let start_phase = artifacts.resource_phase.load(Ordering::Acquire);
        let start = resource_point(started, database);
        append_resource_point(&report, start);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_database = database.to_path_buf();
        let thread_report = report.clone();
        let thread_progress = progress;
        let thread_resource_phase = Arc::clone(&artifacts.resource_phase);
        let peak = Arc::new(Mutex::new(start.rss_bytes));
        let thread_peak = Arc::clone(&peak);
        let handle = thread::spawn(move || {
            let mut resource_progress = ResourceProgress {
                phase: start_phase,
                database_bytes: start.database_bytes,
            };
            while !thread_stop.load(Ordering::Acquire) {
                thread::park_timeout(interval);
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                let before_phase = thread_resource_phase.load(Ordering::Acquire);
                let point = resource_point(started, &thread_database);
                let after_phase = thread_resource_phase.load(Ordering::Acquire);
                if resource_progress.observe(before_phase, after_phase, point.database_bytes) {
                    thread_progress.advance();
                }
                if let Some(rss) = point.rss_bytes {
                    let mut current = thread_peak.lock().expect("RSS peak lock");
                    *current = Some(current.map_or(rss, |observed| observed.max(rss)));
                }
                append_resource_point(&thread_report, point);
            }
        });
        Self {
            stop,
            handle: Some(handle),
            started,
            database: database.to_path_buf(),
            report,
            start,
            peak,
        }
    }

    fn finish(mut self) -> (ResourcePoint, ResourcePoint, Option<u64>) {
        self.stop_sampler()
            .expect("resource sampler thread completes");
        let end = resource_point(self.started, &self.database);
        append_resource_point(&self.report, end);
        let sampled_peak = *self.peak.lock().expect("RSS peak lock");
        let peak = match (sampled_peak, end.rss_bytes) {
            (Some(sampled), Some(end)) => Some(sampled.max(end)),
            (Some(sampled), None) => Some(sampled),
            (None, current) => current,
        };
        (self.start, end, peak)
    }

    fn stop_sampler(&mut self) -> std::thread::Result<()> {
        self.stop.store(true, Ordering::Release);
        self.handle.take().map_or(Ok(()), |handle| {
            handle.thread().unpark();
            handle.join()
        })
    }
}

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        let _ = self.stop_sampler();
    }
}

struct OpenedCase {
    engine: Engine,
    checkpoint_accounting: CheckpointAccounting,
    family: ColumnFamilyHandle,
    database: PathBuf,
    backend: &'static str,
    namespace: String,
    object_prefix: String,
    cloud: bool,
    has_reads: bool,
}

#[derive(Clone, Copy)]
struct ClientConfig {
    workload: &'static str,
    stage: usize,
    client: usize,
    seed_count: usize,
    cloud: bool,
}

#[derive(Clone, Copy)]
struct StageReport<'a> {
    case: WorkloadCase,
    stage_index: usize,
    clients: usize,
    elapsed: Duration,
    stats: &'a StageStats,
    before: &'a RuntimeMetricsSnapshot,
    after: &'a RuntimeMetricsSnapshot,
    admission_before: &'a cntryl_midge::__internal::diagnostics::WriteAdmissionSnapshot,
    admission_after: &'a cntryl_midge::__internal::diagnostics::WriteAdmissionSnapshot,
}

pub(super) fn run_case(ctx: &mut StressContext, case: WorkloadCase) {
    let case = comparison_case(
        case,
        std::env::var("MIDGE_STRESS_COMPARISON_CLIENTS")
            .ok()
            .as_deref(),
    )
    .expect("valid isolated comparison client count");
    enable_phase_tracing();
    let duration = case_duration(case.tier);
    let progress = ctx.progress_handle();
    let mut artifacts = WorkloadArtifacts::begin(case, duration);
    let run_dir = tempfile::tempdir().expect("create isolated stress directory");
    let mut opened = open_case(case, run_dir.path(), &progress, &artifacts);
    let sampler = ResourceSampler::start(&artifacts, &opened.database, progress.clone());
    let maintenance =
        admission::MaintenanceSampler::start(&opened.engine, &artifacts.path, artifacts.started);
    let expected = run_stages(ctx, case, duration, &mut opened, &progress, &mut artifacts);
    if let Some(maintenance) = maintenance {
        maintenance.finish();
    }
    finish_case(opened, &expected, &progress, &mut artifacts);
    let (resource_start, resource_end, peak_rss) = sampler.finish();
    record_resources(ctx, resource_start, resource_end, peak_rss);
    artifacts.finish(&progress);
}

fn comparison_case(
    mut case: WorkloadCase,
    requested: Option<&str>,
) -> Result<WorkloadCase, String> {
    if let Some(requested) = requested {
        let clients = requested
            .parse::<usize>()
            .map_err(|_| "comparison clients must be an integer")?;
        if !case.stages.contains(&clients) {
            return Err("comparison client count must be an original workload stage".into());
        }
        case.stages = match clients {
            1 => &[1],
            2 => &[2],
            4 => &[4],
            8 => &[8],
            16 => &[16],
            32 => &[32],
            _ => return Err("unsupported comparison client count".into()),
        };
    }
    Ok(case)
}

fn open_case(
    case: WorkloadCase,
    root: &Path,
    progress: &ProgressHandle,
    artifacts: &WorkloadArtifacts,
) -> OpenedCase {
    let database = root.join("database");
    let cloud = case.backend != "local";
    let namespace = unique_namespace(case.scenario);
    let object_prefix = format!("midge-stress/{namespace}/{}/", case.scenario);
    if cloud {
        ensure_namespace(case.backend, &namespace)
            .unwrap_or_else(|error| panic!("create Sqrzl {} namespace: {error}", case.backend));
        progress.advance();
    }
    let options = if cloud {
        cloud_options(&database, case.backend, &namespace, &object_prefix)
    } else {
        local_options(&database)
    };
    write_atomic_json(
        &artifacts.path.join("resolved-options.json"),
        &json!({
            "requested_memory_budget": format!("{:?}", options.memory_budget()),
            "resolved_memory_budget_bytes": options.memory_budget_bytes(),
            "sst_read_budget_bytes": options.block_cache_size(),
            "memtable_size_limit_bytes": options.memtable_size_limit(),
            "memtable_flush_threshold_bytes": options.memtable_flush_threshold(),
            "transaction_memory_pool_bytes": options.transaction_memory_pool_size(),
            "block_size_bytes": options.block_size(),
            "target_sst_size_bytes": options.target_sst_size(),
        }),
    )
    .expect("persist resolved stress options");
    let (engine, family) = open_family(options, true);
    let checkpoint_accounting = CheckpointAccounting::attach(&engine, artifacts, "original", None);
    progress.advance();
    let has_reads = matches!(
        case.workload,
        "read-heavy" | "balanced" | "mixed-workload-soak"
    );
    if has_reads {
        seed(&engine, &family, progress, cloud);
    }
    checkpoint_accounting.capture("after-setup-and-seed", artifacts, None);
    OpenedCase {
        engine,
        checkpoint_accounting,
        family,
        database,
        backend: case.backend,
        namespace,
        object_prefix,
        cloud,
        has_reads,
    }
}

fn run_stages(
    ctx: &mut StressContext,
    case: WorkloadCase,
    duration: Duration,
    opened: &mut OpenedCase,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
) -> BTreeMap<(usize, usize, usize), u64> {
    let ingestion_before =
        opened
            .checkpoint_accounting
            .capture("before-ingestion", artifacts, None);
    let mut expected = BTreeMap::new();
    let budgets = stage_budgets(duration, case.stages.len());
    for (stage_index, (&clients, budget)) in case.stages.iter().zip(budgets).enumerate() {
        let stage_label = format!("{clients}-clients");
        artifacts.enter_stage(stage_index, &stage_label, progress);
        let started = Instant::now();
        let before = runtime_metrics(&opened.engine);
        capture_prestate(case, stage_index, clients, opened, artifacts, &before);
        let admission_before =
            cntryl_midge::__internal::diagnostics::write_admission_snapshot(&opened.engine);
        let (stage, outcomes) = run_stage(
            case,
            stage_index,
            clients,
            budget,
            opened,
            progress,
            artifacts,
        );
        // Preserve a failed client's partial statistics before querying a
        // runtime that may already be fenced or propagating the fatal result.
        artifacts.record_stage(&stage_label, &stage, progress);
        if let Some(error) = outcomes
            .iter()
            .find_map(|outcome| outcome.terminal_error.as_ref())
        {
            artifacts.record_terminal_errors(&outcomes, progress);
            panic!(
                "{} workload client failed: {error}",
                workload_name(case.workload)
            );
        }
        for (client, outcome) in outcomes.into_iter().enumerate() {
            stage_expected_rows(&mut expected, stage_index, client, &outcome);
        }
        let elapsed = started.elapsed().max(Duration::from_nanos(1));
        let after = runtime_metrics(&opened.engine);
        let admission_after =
            cntryl_midge::__internal::diagnostics::write_admission_snapshot(&opened.engine);
        write_atomic_json(
            &artifacts
                .path
                .join(format!("stage-{stage_index:02}-endstate.json")),
            &json!({ "stage_index": stage_index, "runtime": after, "write_admission": admission_after }),
        )
        .expect("persist measured end-stage runtime state");
        record_stage(
            ctx,
            StageReport {
                case,
                stage_index,
                clients,
                elapsed,
                stats: &stage,
                before: &before,
                after: &after,
                admission_before: &admission_before,
                admission_after: &admission_after,
            },
        );
    }
    opened
        .checkpoint_accounting
        .window("ingestion", &ingestion_before, artifacts, None);
    expected
}

fn capture_prestate(
    case: WorkloadCase,
    stage_index: usize,
    clients: usize,
    opened: &OpenedCase,
    artifacts: &WorkloadArtifacts,
    before: &RuntimeMetricsSnapshot,
) {
    let isolated = std::env::var_os("MIDGE_STRESS_COMPARISON_CLIENTS").is_some();
    let initial_rows = if isolated {
        assert_eq!(stage_index, 0, "comparison cannot inherit earlier stages");
        let transaction = opened
            .engine
            .begin_tx(opened.family.id(), TransactionMode::ReadOnly)
            .expect("capture isolated initial rows");
        let mut scan = transaction
            .scan(&Query::new())
            .expect("scan isolated initial state");
        let mut rows = 0;
        for row in scan.by_ref() {
            row.expect("read isolated initial row");
            rows += 1;
        }
        assert!(scan.exhausted(), "isolated initial scan exhausted normally");
        assert_eq!(
            rows,
            seed_rows(case.workload),
            "comparison initial cardinality differs from fixed seed"
        );
        Some(rows)
    } else {
        None
    };
    let layout = opened
        .engine
        .metrics()
        .get_storage_layout()
        .expect("capture pre-stage storage layout");
    write_atomic_json(
        &artifacts
            .path
            .join(format!("stage-{stage_index:02}-prestate.json")),
        &json!({
            "schema_version": 1,
            "git_commit": artifacts.git_commit,
            "stage_index": stage_index,
            "clients": clients,
            "isolated_comparison": isolated,
            "verified_initial_rows": initial_rows,
            "warmup_operations": 0,
            "seed_rows": seed_rows(case.workload),
            "prior_stage_count": stage_index,
            "database_logical_file_bytes": directory_bytes(&opened.database),
            "runtime": before,
            "write_admission": cntryl_midge::__internal::diagnostics::write_admission_snapshot(&opened.engine),
            "storage_layout": layout,
        }),
    )
    .expect("persist pre-stage comparison state");
}

fn run_stage(
    case: WorkloadCase,
    stage_index: usize,
    clients: usize,
    budget: Duration,
    opened: &OpenedCase,
    progress: &ProgressHandle,
    artifacts: &WorkloadArtifacts,
) -> (StageStats, Vec<WorkerResult>) {
    let stop = AtomicBool::new(false);
    let snapshots = client_snapshot_directory(artifacts, stage_index);
    let stage_label = format!("{clients}-clients");
    let outcomes = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(clients);
        for client in 0..clients {
            let config = ClientConfig {
                workload: case.workload,
                stage: stage_index,
                client,
                seed_count: seed_rows(case.workload),
                cloud: opened.cloud,
            };
            let heartbeat = progress.clone();
            let engine = &opened.engine;
            let family_id = opened.family.id();
            let reporter = ClientReporter::new(
                &snapshots,
                config,
                artifacts.started,
                CLIENT_REPORT_PERIOD,
                &stage_label,
            );
            let stop = &stop;
            handles.push(scope.spawn(move || {
                run_client(
                    engine, family_id, config, budget, &heartbeat, reporter, stop,
                )
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("stress workload client completes"))
            .collect::<Vec<_>>()
    });
    let mut stage = StageStats::default();
    for outcome in &outcomes {
        stage.merge(&outcome.stats);
    }
    (stage, outcomes)
}

fn client_snapshot_directory(artifacts: &WorkloadArtifacts, stage_index: usize) -> PathBuf {
    let directory = artifacts
        .path
        .join("client-snapshots")
        .join(format!("stage-{stage_index:02}"));
    fs::create_dir_all(&directory).expect("create stage client snapshot directory");
    directory
}

fn stage_expected_rows(
    expected: &mut BTreeMap<(usize, usize, usize), u64>,
    stage_index: usize,
    client: usize,
    outcome: &WorkerResult,
) {
    for (kind, &count) in outcome.kind_counts.iter().enumerate() {
        expected.insert((stage_index, client, kind), count);
    }
    if outcome.account_updates != 0 {
        expected.insert((stage_index, client, 11), outcome.account_updates);
    }
}

fn flush_and_verify_original(
    opened: &OpenedCase,
    expected: &BTreeMap<(usize, usize, usize), u64>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
) -> u64 {
    let flush_before =
        opened
            .checkpoint_accounting
            .capture("before-explicit-flush", artifacts, None);
    let flush_result = flush_acknowledged_data_with(
        &opened.engine,
        &opened.family,
        opened.cloud,
        opened.cloud.then_some(FINAL_FLUSH_CALLER_SLICE),
        progress,
        artifacts,
        |engine, family| {
            if opened.cloud {
                cntryl_midge::__internal::maintenance::flush_cf_with_timeout(
                    engine,
                    family,
                    FINAL_FLUSH_CALLER_SLICE,
                )
            } else {
                engine.flush_cf(family)
            }
        },
    );
    opened.checkpoint_accounting.window(
        "explicit-flush",
        &flush_before,
        artifacts,
        Some(&flush_result),
    );
    flush_result.expect("flush acknowledged stress data");
    let expected_seed =
        u64::try_from(if opened.has_reads { SEED_ROWS } else { 0 }).expect("seed count fits u64");
    verify_database(
        &opened.engine,
        &opened.family,
        expected_seed,
        expected,
        progress,
        artifacts,
        "flushed",
    );
    expected_seed
}

fn finish_case(
    mut opened: OpenedCase,
    expected: &BTreeMap<(usize, usize, usize), u64>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
) {
    let expected_seed = flush_and_verify_original(&opened, expected, progress, artifacts);
    shutdown_engine(
        &mut opened.engine,
        "shutdown-before-recovery",
        &opened.checkpoint_accounting,
        progress,
        artifacts,
    );
    drop(opened.engine);
    if opened.cloud {
        artifacts.enter_phase("cache-loss", "remove-local-cloud-cache", progress);
        fs::remove_dir_all(&opened.database).expect("remove local cloud cache before recovery");
        progress.advance();
        let options = cloud_options(
            &opened.database,
            opened.backend,
            &opened.namespace,
            &opened.object_prefix,
        );
        artifacts.enter_phase("recovery", "open-cloud-from-empty-cache", progress);
        let (recovered, family) = {
            let _scope = RecoveryScope::enter(progress, &artifacts.resource_phase, artifacts.phase);
            open_family(options, false)
        };
        finish_recovered_engine(
            recovered,
            &family,
            expected_seed,
            expected,
            progress,
            artifacts,
            RecoveryObservation {
                predecessor: opened.checkpoint_accounting.owner_id(),
                verification_phase: "cloud-recovered",
                shutdown_phase: "shutdown-recovered-cloud-engine",
            },
        );
    } else {
        artifacts.enter_phase("recovery", "reopen-local-engine", progress);
        let (recovered, family) = {
            let _scope = RecoveryScope::enter(progress, &artifacts.resource_phase, artifacts.phase);
            open_family(local_options(&opened.database), false)
        };
        finish_recovered_engine(
            recovered,
            &family,
            expected_seed,
            expected,
            progress,
            artifacts,
            RecoveryObservation {
                predecessor: opened.checkpoint_accounting.owner_id(),
                verification_phase: "reopened",
                shutdown_phase: "shutdown-reopened-local-engine",
            },
        );
    }
    opened
        .checkpoint_accounting
        .after_reopened_shutdown(artifacts);
}

#[derive(Clone, Copy)]
struct RecoveryObservation {
    predecessor: u64,
    verification_phase: &'static str,
    shutdown_phase: &'static str,
}

fn finish_recovered_engine(
    mut recovered: Engine,
    family: &ColumnFamilyHandle,
    expected_seed: u64,
    expected: &BTreeMap<(usize, usize, usize), u64>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
    observation: RecoveryObservation,
) {
    let recovered_accounting = CheckpointAccounting::attach(
        &recovered,
        artifacts,
        "reopened",
        Some(observation.predecessor),
    );
    progress.advance();
    verify_database(
        &recovered,
        family,
        expected_seed,
        expected,
        progress,
        artifacts,
        observation.verification_phase,
    );
    shutdown_engine(
        &mut recovered,
        observation.shutdown_phase,
        &recovered_accounting,
        progress,
        artifacts,
    );
}

fn flush_acknowledged_data_with(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    cloud: bool,
    caller_budget: Option<Duration>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
    mut flush: impl FnMut(&Engine, &ColumnFamilyHandle) -> Result<(), MidgeError>,
) -> Result<(), MidgeError> {
    artifacts.enter_phase("flush", "flush-acknowledged-data", progress);
    let mut backoff = FINAL_FLUSH_INITIAL_BACKOFF;
    loop {
        let before_health = cloud.then(|| engine.is_primary_lease_healthy());
        artifacts.final_flush.observed_primary_lease_healthy = before_health;
        if before_health == Some(false) {
            let error =
                MidgeError::Fenced("primary lease became unhealthy before final flush".into());
            artifacts.final_flush.last_error = Some(format!("{error:?}"));
            artifacts.final_flush.last_error_kind = Some("fenced");
            artifacts.terminal_error = Some(format!("{error:?}"));
            artifacts.persist("failed");
            return Err(error);
        }

        artifacts.final_flush.attempts = artifacts.final_flush.attempts.saturating_add(1);
        artifacts.final_flush.operation_in_flight = true;
        artifacts.progress_units = progress.completed_units();
        artifacts.persist("running");
        let started = Instant::now();
        let result = flush(engine, family);
        let after_health = cloud.then(|| engine.is_primary_lease_healthy());
        artifacts.final_flush.operation_in_flight = false;
        artifacts.final_flush.observed_primary_lease_healthy = after_health;
        let retry = cloud
            && before_health == Some(true)
            && after_health == Some(true)
            && result.as_ref().err().is_some_and(|error| {
                matches!(
                    error,
                    MidgeError::Timeout(_) | MidgeError::Busy(_) | MidgeError::WriteStall(_)
                )
            });
        let error_kind = result.as_ref().err().map(final_flush_error_kind);
        if let Err(error) = &result {
            record_final_flush_error(&mut artifacts.final_flush, error);
        }
        if retry {
            artifacts.final_flush.backoff_ms = artifacts.final_flush.backoff_ms.saturating_add(
                u64::try_from(backoff.as_millis()).expect("bounded flush backoff fits u64"),
            );
        }
        append_final_flush_attempt(
            artifacts,
            &json!({
                "attempt": artifacts.final_flush.attempts,
                "recorded_elapsed_ms": artifacts.started.elapsed().as_millis(),
                "elapsed_ms": started.elapsed().as_millis(),
                "caller_budget_ms": caller_budget.map(|budget| budget.as_millis()),
                "success": result.is_ok(),
                "error_kind": error_kind,
                "error": result.as_ref().err().map(|error| format!("{error:?}")),
                "lease_healthy_before": before_health,
                "lease_healthy_after": after_health,
                "retry": retry,
                "backoff_ms": if retry { backoff.as_millis() } else { 0 },
                "progress_completed_units": progress.completed_units(),
            }),
        );
        match result {
            Ok(()) => {
                artifacts.final_flush.completed = true;
                progress.advance();
                artifacts.progress_units = progress.completed_units();
                artifacts.persist("running");
                return Ok(());
            }
            Err(error) if !retry => {
                artifacts.terminal_error = Some(format!("{error:?}"));
                artifacts.progress_units = progress.completed_units();
                artifacts.persist("failed");
                return Err(error);
            }
            Err(_) => {
                artifacts.progress_units = progress.completed_units();
                artifacts.persist("running");
                // Retry only the caller's barrier wait. Submission, rejection,
                // timeout and sleeping never advance the progress watchdog.
                thread::sleep(backoff);
                backoff = backoff.saturating_mul(2).min(FINAL_FLUSH_MAX_BACKOFF);
            }
        }
    }
}

fn record_final_flush_error(report: &mut FinalFlushReport, error: &MidgeError) {
    report.last_error = Some(format!("{error:?}"));
    report.last_error_kind = Some(final_flush_error_kind(error));
    match error {
        MidgeError::Timeout(_) => {
            report.timeout_responses = report.timeout_responses.saturating_add(1);
        }
        MidgeError::Busy(_) => {
            report.busy_responses = report.busy_responses.saturating_add(1);
        }
        MidgeError::WriteStall(_) => {
            report.write_stall_responses = report.write_stall_responses.saturating_add(1);
        }
        _ => {}
    }
}

fn final_flush_error_kind(error: &MidgeError) -> &'static str {
    match error {
        MidgeError::Timeout(_) => "timeout",
        MidgeError::Busy(_) => "busy",
        MidgeError::WriteStall(_) => "write_stall",
        MidgeError::NoSpace(_) => "no_space",
        MidgeError::Fenced(_) => "fenced",
        MidgeError::Corruption(_) => "corruption",
        MidgeError::ResourceLimit(_) => "resource_limit",
        MidgeError::Internal(_) => "internal",
        _ => "other_terminal",
    }
}

fn append_final_flush_attempt(artifacts: &WorkloadArtifacts, attempt: &serde_json::Value) {
    let mut output = FileOptions::new()
        .create(true)
        .append(true)
        .open(artifacts.path.join("final-flush-attempts.jsonl"))
        .expect("open final-flush attempt report");
    writeln!(output, "{attempt}").expect("retain actual final-flush result");
}

fn shutdown_engine(
    engine: &mut Engine,
    stage: &str,
    checkpoint_accounting: &CheckpointAccounting,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
) {
    artifacts.enter_phase("shutdown", stage, progress);
    checkpoint_accounting.capture("before-shutdown", artifacts, None);
    let started = Instant::now();
    let result = engine.shutdown(SHUTDOWN_CALLER_BUDGET);
    artifacts.record_shutdown(stage, &result, started.elapsed());
    checkpoint_accounting.capture("after-shutdown-caller-result", artifacts, Some(&result));
    result.unwrap_or_else(|error| panic!("{stage} failed: {error}"));
    checkpoint_accounting.finalized(artifacts);
    progress.advance();
}

fn verify_database(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    expected_seed: u64,
    expected: &BTreeMap<(usize, usize, usize), u64>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
    phase: &str,
) {
    artifacts.enter_phase("verification", phase, progress);
    verify_seed(
        engine,
        family,
        expected_seed,
        progress,
        artifacts,
        &format!("{phase}-seed"),
    );
    verify_writes(
        engine,
        family,
        expected,
        progress,
        artifacts,
        &format!("{phase}-workload"),
    );
    verify_accounts(
        engine,
        family,
        expected,
        progress,
        artifacts,
        &format!("{phase}-accounts"),
    );
}

fn case_duration(tier: u8) -> Duration {
    let key = if tier == 5 {
        "MIDGE_TIER5_DURATION_SECS"
    } else {
        "MIDGE_TIER6_DURATION_SECS"
    };
    let seconds = std::env::var(key)
        .ok()
        .or_else(|| std::env::var("MIDGE_STRESS_DURATION_SECS").ok())
        .map_or(3_600, |value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("{key} must be a positive number of seconds"))
        });
    assert!((1..=86_400).contains(&seconds), "{key} must be 1..=86400");
    Duration::from_secs(seconds)
}

fn stage_budgets(total: Duration, count: usize) -> Vec<Duration> {
    assert!(count > 0, "a workload sweep needs at least one stage");
    let nanos = total.as_nanos() / u128::try_from(count).expect("stage count fits u128");
    let budget = Duration::from_nanos(u64::try_from(nanos.max(1)).unwrap_or(u64::MAX));
    vec![budget; count]
}

fn stage_name(case: WorkloadCase, stage: usize) -> String {
    format!("{}-{}-stage-{stage:02}", case.scenario, case.backend)
}

fn local_options(path: &Path) -> OpenOptions {
    OpenOptions::local(path)
        .memory_budget(MemoryBudget::Auto)
        .workload(WorkloadProfile::default())
        .with_memtable_size_limit(8 * 1024 * 1024)
        .with_memtable_flush_threshold(4 * 1024 * 1024)
        .build()
        .expect("build local stress options")
}

fn cloud_options(path: &Path, backend: &str, namespace: &str, object_prefix: &str) -> OpenOptions {
    let endpoint = std::env::var("MIDGE_STRESS_SQRZL_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let provider = match backend {
        "s3" => CloudProviderConfig::sqrzl_s3(namespace),
        "azure" => CloudProviderConfig::sqrzl_azure(namespace),
        "gcs-xml" => CloudProviderConfig::sqrzl_gcs(namespace),
        "gcs-json" => CloudProviderConfig::sqrzl_gcs_json(namespace),
        _ => panic!("unsupported Sqrzl protocol: {backend}"),
    }
    .with_endpoint(endpoint)
    .unwrap_or_else(|error| panic!("configure Sqrzl endpoint: {error}"));
    let cloud_write_policy = CloudWritePolicy {
        // Batch asynchronous WAL into fewer objects during one-hour sweeps.
        wal_seal_max_flush_delay: CLOUD_STRESS_WAL_SEAL_MAX_FLUSH_DELAY,
        ..CloudWritePolicy::default()
    };
    OpenOptions::cloud(
        path,
        CloudStorageLocation::new(provider, object_prefix.to_string()),
    )
    .cloud_write_policy(cloud_write_policy)
    .memory_budget(MemoryBudget::Auto)
    .workload(WorkloadProfile::default())
    .with_memtable_size_limit(8 * 1024 * 1024)
    .with_memtable_flush_threshold(4 * 1024 * 1024)
    .build()
    .expect("build Sqrzl stress options")
}

fn open_family(options: OpenOptions, create: bool) -> (Engine, ColumnFamilyHandle) {
    let engine = Engine::open(options).expect("open stress engine");
    let family = if create {
        engine
            .create_column_family("workload")
            .expect("create workload column family")
    } else {
        engine
            .get_column_family("workload")
            .expect("recover workload column family")
    };
    (engine, family)
}

fn runtime_metrics(engine: &Engine) -> RuntimeMetricsSnapshot {
    engine
        .metrics()
        .get_runtime_metrics()
        .expect("read Midge runtime metrics")
}

fn seed_rows(workload: &str) -> usize {
    if matches!(workload, "read-heavy" | "balanced" | "mixed-workload-soak") {
        SEED_ROWS
    } else {
        0
    }
}

fn seed(engine: &Engine, family: &ColumnFamilyHandle, progress: &ProgressHandle, cloud: bool) {
    for batch in 0..SEED_ROWS.div_ceil(SEED_BATCH) {
        let mut transaction = engine
            .begin_tx(family.id(), TransactionMode::ReadWrite)
            .expect("begin workload seed transaction");
        let start = batch * SEED_BATCH;
        let end = (start + SEED_BATCH).min(SEED_ROWS);
        for index in start..end {
            let key = format!("midge:seed:{index:08}");
            transaction
                .put(key.as_bytes().to_vec(), workload_value(&key), None)
                .expect("stage workload seed row");
        }
        transaction
            .commit(if cloud {
                WriteOptions::cloud_async()
            } else {
                WriteOptions::best_effort()
            })
            .expect("commit workload seed transaction");
        progress.advance();
    }
    engine.flush_cf(family).expect("flush workload seed rows");
}

fn run_client(
    engine: &Engine,
    family_id: u32,
    config: ClientConfig,
    budget: Duration,
    progress: &ProgressHandle,
    mut reporter: ClientReporter,
    stop: &AtomicBool,
) -> WorkerResult {
    let check_health = || engine.is_primary_lease_healthy();
    let control = ClientControl {
        stop,
        lease_health: config.cloud.then_some(&check_health as &dyn Fn() -> bool),
        origin: reporter.origin(),
    };
    run_client_with(
        config,
        budget,
        &control,
        ClientClock {
            now: Instant::now,
            sleep: thread::sleep,
        },
        |attempt, sequences, account| {
            run_operation(engine, family_id, &config, attempt, sequences, account)
        },
        || progress.advance(),
        |result, report| {
            reporter.report(result, report);
        },
    )
}

fn saturation_backoff_ms(level: u32) -> u64 {
    let shift = level.saturating_sub(1).min(SATURATION_BACKOFF_MAX_SHIFT);
    1_u64 << shift
}

type OperationResult = Result<(Option<usize>, u64, u64, bool), MidgeError>;

fn run_operation(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    attempt: u64,
    sequences: &mut [u64; 11],
    account: &mut u64,
) -> OperationResult {
    match config.workload {
        "read-heavy" => read_heavy_operation(engine, family_id, config, attempt),
        "balanced" => balanced_operation(engine, family_id, config, attempt, sequences),
        "many-small-transactions" => {
            small_transaction_operation(engine, family_id, config, sequences, account)
        }
        "mixed-transaction-sizes" => {
            mixed_size_operation(engine, family_id, config, attempt, sequences)
        }
        "write-heavy" | "sqrzl-write-pressure" => {
            write_pressure_operation(engine, family_id, config, sequences)
        }
        "mixed-workload-soak" => composite_operation(engine, family_id, config, attempt, sequences),
        _ => panic!("unregistered Midge stress workload: {}", config.workload),
    }
}

fn read_heavy_operation(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    attempt: u64,
) -> OperationResult {
    read_seed(engine, family_id, config.client, attempt, config.seed_count)?;
    if attempt.is_multiple_of(16) {
        scan_seed(engine, family_id)?;
    }
    Ok((None, 1, 0, false))
}

fn balanced_operation(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    attempt: u64,
    sequences: &mut [u64; 11],
) -> OperationResult {
    let read_only = !attempt.is_multiple_of(5);
    let mode = if read_only {
        TransactionMode::ReadOnly
    } else {
        TransactionMode::ReadWrite
    };
    let mut transaction = engine.begin_tx(family_id, mode)?;
    for offset in 0..4 {
        let attempt_index = usize::try_from(
            attempt % u64::try_from(config.seed_count).expect("seed size fits u64"),
        )
        .expect("bounded seed index fits usize");
        let seed_index = (config.client * 13 + attempt_index + offset) % config.seed_count;
        let key = format!("midge:seed:{seed_index:08}");
        transaction.get(key.as_bytes())?;
    }
    if read_only {
        return Ok((None, 4, 0, false));
    }
    append_generated_row(
        &mut transaction,
        config.stage,
        config.client,
        1,
        sequences[1],
        0,
    )?;
    transaction.commit(write_options(config.cloud))?;
    sequences[1] = sequences[1].saturating_add(1);
    Ok((Some(1), 5, 1, false))
}

fn small_transaction_operation(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    sequences: &mut [u64; 11],
    account: &mut u64,
) -> OperationResult {
    let account_key = format!("midge:account:{:02}:{:02}", config.stage, config.client);
    let ledger_key = write_key(config.stage, config.client, 2, sequences[2], 0);
    let mut transaction = engine.begin_tx(family_id, TransactionMode::ReadWrite)?;
    let actual = transaction.get(account_key.as_bytes())?;
    let current = actual.as_deref().map_or(0, |bytes| {
        std::str::from_utf8(bytes)
            .expect("account balance is UTF-8")
            .parse::<u64>()
            .expect("account balance is numeric")
    });
    assert_eq!(current, *account, "small transaction account is consistent");
    let next = current.saturating_add(1);
    transaction.put(
        account_key.into_bytes(),
        next.to_string().into_bytes(),
        None,
    )?;
    transaction.put(
        ledger_key.as_bytes().to_vec(),
        workload_value(&ledger_key),
        None,
    )?;
    transaction.commit(write_options(config.cloud))?;
    *account = next;
    sequences[2] = sequences[2].saturating_add(1);
    Ok((Some(2), 2, 1, true))
}

fn mixed_size_operation(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    attempt: u64,
    sequences: &mut [u64; 11],
) -> OperationResult {
    let size_index = usize::try_from(attempt % 5).expect("mixed size index fits usize");
    let kind = 3 + size_index;
    let rows = MIXED_ROWS[size_index];
    let sequence = sequences[kind];
    let mut transaction = engine.begin_tx(family_id, TransactionMode::ReadWrite)?;
    for row in 0..rows {
        append_generated_row(
            &mut transaction,
            config.stage,
            config.client,
            kind,
            sequence,
            row,
        )?;
    }
    transaction.commit(write_options(config.cloud))?;
    sequences[kind] = sequences[kind].saturating_add(1);
    let row_count = u64::try_from(rows).expect("mixed transaction row count fits u64");
    Ok((Some(kind), row_count, row_count, false))
}

fn write_pressure_operation(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    sequences: &mut [u64; 11],
) -> OperationResult {
    write_batch(engine, family_id, config, 0, sequences[0], WRITE_BATCH_ROWS)?;
    sequences[0] = sequences[0].saturating_add(1);
    let rows = u64::try_from(WRITE_BATCH_ROWS).expect("write batch size fits u64");
    Ok((Some(0), rows, rows, false))
}

fn composite_operation(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    attempt: u64,
    sequences: &mut [u64; 11],
) -> OperationResult {
    match attempt % 100 {
        0..=59 => {
            read_seed(engine, family_id, config.client, attempt, config.seed_count)?;
            Ok((None, 1, 0, false))
        }
        60..=84 => {
            write_batch(engine, family_id, config, 8, sequences[8], 1)?;
            sequences[8] = sequences[8].saturating_add(1);
            Ok((Some(8), 1, 1, false))
        }
        85..=94 => {
            let mut transaction = engine.begin_tx(family_id, TransactionMode::ReadWrite)?;
            for offset in 0..4 {
                let seed_index =
                    (config.client * 13 + offset + usize::try_from(attempt).unwrap_or(0))
                        % config.seed_count;
                let key = format!("midge:seed:{seed_index:08}");
                transaction.get(key.as_bytes())?;
            }
            append_generated_row(
                &mut transaction,
                config.stage,
                config.client,
                9,
                sequences[9],
                0,
            )?;
            transaction.commit(write_options(config.cloud))?;
            sequences[9] = sequences[9].saturating_add(1);
            Ok((Some(9), 5, 1, false))
        }
        _ => {
            write_batch(engine, family_id, config, 10, sequences[10], 128)?;
            sequences[10] = sequences[10].saturating_add(1);
            Ok((Some(10), 128, 128, false))
        }
    }
}

fn write_batch(
    engine: &Engine,
    family_id: u32,
    config: &ClientConfig,
    kind: usize,
    sequence: u64,
    rows: usize,
) -> Result<(), MidgeError> {
    let mut transaction = engine.begin_tx(family_id, TransactionMode::ReadWrite)?;
    for row in 0..rows {
        append_generated_row(
            &mut transaction,
            config.stage,
            config.client,
            kind,
            sequence,
            row,
        )?;
    }
    transaction.commit(write_options(config.cloud))
}

fn append_generated_row(
    transaction: &mut cntryl_midge::Transaction,
    stage: usize,
    client: usize,
    kind: usize,
    sequence: u64,
    row: usize,
) -> Result<(), MidgeError> {
    let key = write_key(stage, client, kind, sequence, row);
    transaction.put(key.as_bytes().to_vec(), workload_value(&key), None)
}

fn write_key(stage: usize, client: usize, kind: usize, sequence: u64, row: usize) -> String {
    format!("midge:w:{stage:02}:{client:02}:{kind:02}:{sequence:012}:{row:05}")
}

fn write_options(cloud: bool) -> WriteOptions {
    if cloud {
        WriteOptions::cloud_async()
    } else {
        WriteOptions::sync()
    }
}

fn read_seed(
    engine: &Engine,
    family_id: u32,
    client: usize,
    attempt: u64,
    seed_count: usize,
) -> Result<(), MidgeError> {
    let attempt_index =
        usize::try_from(attempt % u64::try_from(seed_count).expect("seed size fits u64"))
            .expect("bounded seed index fits usize");
    let index = (client * 17 + attempt_index) % seed_count;
    let key = format!("midge:seed:{index:08}");
    let transaction = engine.begin_tx(family_id, TransactionMode::ReadOnly)?;
    let value = transaction.get(key.as_bytes())?;
    assert_eq!(value.as_deref(), Some(workload_value(&key).as_slice()));
    Ok(())
}

fn scan_seed(engine: &Engine, family_id: u32) -> Result<(), MidgeError> {
    let transaction = engine.begin_tx(family_id, TransactionMode::ReadOnly)?;
    let mut scan = transaction.scan(&Query::new().prefix(Bytes::from_static(b"midge:seed:")))?;
    if let Some(row) = scan.next() {
        let (key, value) = row?;
        let label = std::str::from_utf8(&key).expect("seed key is UTF-8");
        assert_eq!(value.as_ref(), workload_value(label));
    }
    Ok(())
}

fn workload_name(workload: &str) -> &'static str {
    match workload {
        "write-heavy" => "write-heavy",
        "read-heavy" => "read-heavy",
        "balanced" => "balanced",
        "many-small-transactions" => "many small transactions",
        "mixed-transaction-sizes" => "mixed transaction sizes",
        "sqrzl-write-pressure" => "Sqrzl write-pressure",
        "mixed-workload-soak" => "mixed-workload soak",
        _ => "unknown workload",
    }
}

fn record_stage(ctx: &mut StressContext, report: StageReport<'_>) {
    let StageReport {
        case,
        stage_index,
        clients,
        elapsed,
        stats,
        before,
        after,
        admission_before,
        admission_after,
    } = report;
    let name = format!(
        "{}/{}/{}",
        case.scenario,
        case.backend,
        stage_name(case, stage_index)
    );
    let outcome = OperationOutcome {
        // Correctness counters and throughput normalization cover acknowledged
        // work; rejected attempts are recorded separately as saturation.
        attempted: stats.acknowledged,
        completed: stats.acknowledged,
        ..OperationOutcome::default()
    };
    ctx.record_external_outcome(name, elapsed, LogicalUnit::new("transaction"), outcome);
    ctx.parameter("scenario", case.scenario)
        .parameter("storage_backend", case.backend)
        .parameter("concurrent_clients", clients)
        .parameter("attempted_transactions", stats.attempts)
        .parameter("acknowledged_transactions", stats.acknowledged)
        .parameter("acknowledged_rows", stats.acknowledged_rows)
        .parameter("resource_limit_responses", stats.saturation.resource_limit)
        .parameter("write_stall_responses", stats.saturation.write_stall)
        .parameter("saturation_backoff_ms", stats.saturation.backoff_ms)
        .parameter("legacy_transaction_latency_scope", "all_attempts_excluding_report_and_retry_sleep")
        .parameter("inter_ack_latency_scope", "worker_loop_start_or_previous_success_to_next_success_including_reporting_and_actual_sleep")
        .parameter("latency_accounting_valid", stats.latency.valid)
        .parameter("attempt_latency_samples", stats.latency_us.len())
        .parameter("successful_call_latency_samples", stats.latency.successful_us.len())
        .parameter("inter_ack_latency_samples", stats.latency.inter_ack_us.len())
        .parameter("actual_sleep_ns", stats.latency.actual_sleep_ns)
        .parameter("censored_inter_ack_samples", stats.latency.censored_samples)
        .parameter("censored_inter_ack_ns", stats.latency.censored_ns)
        .parameter("midge_write_stalls_total", after.write_stalls_total)
        .parameter("midge_write_stalls_memory", after.write_stalls_memory_total)
        .parameter(
            "midge_write_stalls_compaction",
            after.write_stalls_compaction_total,
        )
        .parameter("midge_write_stalls_cloud", after.write_stalls_cloud_total)
        .parameter(
            "midge_write_stalls_no_space",
            after.write_stalls_no_space_total,
        );
    record_stage_diagnostics(ctx, stats, before, after);
    record_admission_diagnostics(ctx, stats, *admission_before, *admission_after);
    record_legacy_latency(ctx, stats);
    ctx.record_observation(
        "logical_operations",
        observation_value(stats.logical_operations),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    )
    .record_observation(
        "resource_limit_responses",
        observation_value(stats.saturation.resource_limit),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    )
    .record_observation(
        "write_stall_responses",
        observation_value(stats.saturation.write_stall),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    )
    .record_observation(
        "saturation_backoff_us",
        observation_value(stats.saturation.backoff_ms.saturating_mul(1_000)),
        ObservationUnit::Microseconds,
        ObservationDirection::Informational,
    )
    .record_observation(
        "midge_write_stalls_delta",
        observation_value(
            after
                .write_stalls_total
                .saturating_sub(before.write_stalls_total),
        ),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
}

fn record_legacy_latency(ctx: &mut StressContext, stats: &StageStats) {
    for (name, rank) in [
        ("transaction_latency_p50_us", 0.50),
        ("transaction_latency_p95_us", 0.95),
        ("transaction_latency_p99_us", 0.99),
    ] {
        ctx.record_observation(
            name,
            observation_value(quantile(&stats.latency_us, rank)),
            ObservationUnit::Microseconds,
            ObservationDirection::LowerIsBetter,
        );
    }
}

fn record_stage_diagnostics(
    ctx: &mut StressContext,
    stats: &StageStats,
    before: &RuntimeMetricsSnapshot,
    after: &RuntimeMetricsSnapshot,
) {
    for (name, initial, final_value) in [
        (
            "flush_build_count",
            before.flush_build_count,
            after.flush_build_count,
        ),
        (
            "flush_build_ns",
            before.flush_build_ns_total,
            after.flush_build_ns_total,
        ),
        (
            "flush_publish_count",
            before.flush_publish_count,
            after.flush_publish_count,
        ),
        (
            "flush_publish_ns",
            before.flush_publish_ns_total,
            after.flush_publish_ns_total,
        ),
        ("compactions", before.compactions_run, after.compactions_run),
        (
            "compaction_bytes",
            before.compaction_bytes_rewritten,
            after.compaction_bytes_rewritten,
        ),
        (
            "write_stall_ns",
            before.write_stall_ns_total,
            after.write_stall_ns_total,
        ),
    ] {
        let delta = final_value.checked_sub(initial);
        ctx.parameter(format!("midge_{name}_delta_valid"), delta.is_some())
            .parameter(format!("midge_{name}_delta"), delta.unwrap_or(0));
    }
    for (name, initial, final_value) in [
        (
            "memory",
            before.write_stalls_memory_total,
            after.write_stalls_memory_total,
        ),
        (
            "compaction",
            before.write_stalls_compaction_total,
            after.write_stalls_compaction_total,
        ),
        (
            "cloud",
            before.write_stalls_cloud_total,
            after.write_stalls_cloud_total,
        ),
        (
            "no_space",
            before.write_stalls_no_space_total,
            after.write_stalls_no_space_total,
        ),
    ] {
        let delta = final_value.checked_sub(initial);
        ctx.parameter(
            format!("midge_write_stalls_{name}_delta_valid"),
            delta.is_some(),
        )
        .parameter(
            format!("midge_write_stalls_{name}_delta"),
            delta.unwrap_or(0),
        );
    }
    for (prefix, histogram) in [
        ("attempt_latency", &stats.latency_us),
        ("successful_call_latency", &stats.latency.successful_us),
        ("inter_ack_latency", &stats.latency.inter_ack_us),
    ] {
        for (suffix, rank) in [("p50", 0.50), ("p95", 0.95), ("p99", 0.99)] {
            ctx.record_observation(
                format!("{prefix}_{suffix}_us"),
                observation_value(quantile(histogram, rank)),
                ObservationUnit::Microseconds,
                ObservationDirection::LowerIsBetter,
            );
        }
    }
}

fn observation_value(value: u64) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("integer observations fit in f64")
}

fn record_admission_diagnostics(
    ctx: &mut StressContext,
    stats: &StageStats,
    before: cntryl_midge::__internal::diagnostics::WriteAdmissionSnapshot,
    after: cntryl_midge::__internal::diagnostics::WriteAdmissionSnapshot,
) {
    let mut origins = Some(0_u64);
    for (name, initial, final_value) in [
        ("queue", before.queue_total, after.queue_total),
        ("l0", before.l0_total, after.l0_total),
        (
            "cloud_generation",
            before.cloud_generation_total,
            after.cloud_generation_total,
        ),
        ("cloud_wal", before.cloud_wal_total, after.cloud_wal_total),
        (
            "ingest_hint",
            before.ingest_hint_total,
            after.ingest_hint_total,
        ),
    ] {
        let delta = final_value.checked_sub(initial);
        origins = origins
            .zip(delta)
            .and_then(|(sum, count)| sum.checked_add(count));
        ctx.parameter(
            format!("midge_admission_{name}_delta_valid"),
            delta.is_some(),
        )
        .parameter(format!("midge_admission_{name}_delta"), delta.unwrap_or(0));
    }
    let commits = after
        .commit_write_stall_total
        .checked_sub(before.commit_write_stall_total);
    ctx.parameter("midge_admission_commit_delta_valid", commits.is_some())
        .parameter("midge_admission_commit_delta", commits.unwrap_or(0))
        .parameter(
            "midge_admission_counts_reconcile",
            commits == Some(stats.saturation.write_stall) && origins == commits,
        );
    for (name, initial, final_value) in [
        (
            "runtime",
            before.hint_runtime_total,
            after.hint_runtime_total,
        ),
        ("l0", before.hint_l0_total, after.hint_l0_total),
        ("memory", before.hint_memory_total, after.hint_memory_total),
        (
            "cloud_pending",
            before.hint_cloud_pending_total,
            after.hint_cloud_pending_total,
        ),
        (
            "upload_stalled",
            before.hint_upload_stalled_total,
            after.hint_upload_stalled_total,
        ),
        (
            "unknown",
            before.hint_unknown_total,
            after.hint_unknown_total,
        ),
    ] {
        let delta = final_value.checked_sub(initial);
        ctx.parameter(format!("midge_hint_{name}_delta_valid"), delta.is_some())
            .parameter(format!("midge_hint_{name}_delta"), delta.unwrap_or(0));
    }
}

fn append_stage_csv(path: &Path, stage: &str, stats: &StageStats) {
    let report = path.join("stages.csv");
    let needs_header = !report.exists();
    let mut file = FileOptions::new()
        .create(true)
        .append(true)
        .open(report)
        .expect("open stage report");
    if needs_header {
        writeln!(file,"stage,attempted_transactions,acknowledged_transactions,logical_operations,acknowledged_rows,resource_limit_responses,write_stall_responses,saturation_backoff_ms,latency_p50_us,latency_p95_us,latency_p99_us,{}", latency::CSV_FIELDS.join(","))
            .expect("write stage report header");
    }
    let summary = stats.latency.summary(&stats.latency_us);
    let additional = latency::CSV_FIELDS
        .map(|key| summary[key].to_string())
        .join(",");
    writeln!(
        file,
        "{stage},{},{},{},{},{},{},{},{},{},{},{additional}",
        stats.attempts,
        stats.acknowledged,
        stats.logical_operations,
        stats.acknowledged_rows,
        stats.saturation.resource_limit,
        stats.saturation.write_stall,
        stats.saturation.backoff_ms,
        quantile(&stats.latency_us, 0.50),
        quantile(&stats.latency_us, 0.95),
        quantile(&stats.latency_us, 0.99),
    )
    .expect("write stage report row");
}

fn quantile(histogram: &Histogram<u64>, quantile: f64) -> u64 {
    if histogram.is_empty() {
        0
    } else {
        histogram.value_at_quantile(quantile)
    }
}

fn verify_seed(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    expected: u64,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
    phase: &str,
) {
    let transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadOnly)
        .expect("begin seed verification scan");
    let mut scan = transaction
        .scan(&Query::new().prefix(Bytes::from_static(b"midge:seed:")))
        .expect("start seed verification scan");
    let (mut rows, mut mismatches) = (0_u64, 0_u64);
    for row in scan.by_ref() {
        let (key, value) = row.expect("read seed verification row");
        let label = std::str::from_utf8(&key).expect("seed key is UTF-8");
        let valid_key = label
            .strip_prefix("midge:seed:")
            .and_then(|index| index.parse::<usize>().ok())
            .is_some_and(|index| index < SEED_ROWS && label == format!("midge:seed:{index:08}"));
        if !valid_key || value.as_ref() != workload_value(label) {
            mismatches = mismatches.saturating_add(1);
        }
        rows = rows.saturating_add(1);
        if rows % 128 == 0 {
            progress.advance();
        }
    }
    assert!(
        scan.exhausted(),
        "seed verification scan exhausted normally"
    );
    artifacts.verification(phase, expected, rows, mismatches, progress);
}

fn verify_writes(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    expected: &BTreeMap<(usize, usize, usize), u64>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
    phase: &str,
) {
    let expected_rows = expected
        .iter()
        .filter(|((_, _, kind), _)| *kind < WRITE_ROW_COUNTS.len())
        .fold(0_u64, |total, ((_, _, kind), count)| {
            let rows_per_transaction =
                u64::try_from(WRITE_ROW_COUNTS[*kind]).expect("row count fits u64");
            total.saturating_add(count.saturating_mul(rows_per_transaction))
        });
    let transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadOnly)
        .expect("begin acknowledged-write scan");
    let mut scan = transaction
        .scan(&Query::new().prefix(Bytes::from_static(b"midge:w:")))
        .expect("start acknowledged-write scan");
    let (mut rows, mut mismatches) = (0_u64, 0_u64);
    for row in scan.by_ref() {
        let (key, value) = row.expect("read acknowledged-write row");
        let label = std::str::from_utf8(&key).expect("workload key is UTF-8");
        let fields = label.split(':').collect::<Vec<_>>();
        let valid = fields.len() == 7
            && fields[0] == "midge"
            && fields[1] == "w"
            && fields[2]
                .parse::<usize>()
                .ok()
                .zip(fields[3].parse::<usize>().ok())
                .zip(fields[4].parse::<usize>().ok())
                .zip(fields[5].parse::<u64>().ok())
                .zip(fields[6].parse::<usize>().ok())
                .is_some_and(|((((stage, client), kind), sequence), row)| {
                    kind < WRITE_ROW_COUNTS.len()
                        && row < WRITE_ROW_COUNTS[kind]
                        && expected
                            .get(&(stage, client, kind))
                            .is_some_and(|count| sequence < *count)
                        && value.as_ref() == workload_value(label)
                });
        if !valid {
            mismatches = mismatches.saturating_add(1);
        }
        rows = rows.saturating_add(1);
        if rows % 128 == 0 {
            progress.advance();
        }
    }
    assert!(
        scan.exhausted(),
        "write verification scan exhausted normally"
    );
    artifacts.verification(phase, expected_rows, rows, mismatches, progress);
}

fn verify_accounts(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    expected: &BTreeMap<(usize, usize, usize), u64>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
    phase: &str,
) {
    let expected_accounts = expected
        .iter()
        .filter_map(|(&(stage, client, kind), &count)| {
            (kind == 11).then_some(((stage, client), count))
        })
        .collect::<BTreeMap<_, _>>();
    let transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadOnly)
        .expect("begin account verification");
    let mut scan = transaction
        .scan(&Query::new().prefix(Bytes::from_static(b"midge:account:")))
        .expect("start account verification scan");
    let (mut rows, mut mismatches) = (0_u64, 0_u64);
    for row in scan.by_ref() {
        let (key, value) = row.expect("read acknowledged account");
        let label = std::str::from_utf8(&key).expect("account key is UTF-8");
        let fields = label.split(':').collect::<Vec<_>>();
        let valid = if fields.len() == 4 && fields[0] == "midge" && fields[1] == "account" {
            if let (Ok(stage), Ok(client)) =
                (fields[2].parse::<usize>(), fields[3].parse::<usize>())
            {
                let canonical_key = format!("midge:account:{stage:02}:{client:02}");
                let balance = std::str::from_utf8(&value)
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok());
                label == canonical_key
                    && expected_accounts
                        .get(&(stage, client))
                        .is_some_and(|expected| balance == Some(*expected))
            } else {
                false
            }
        } else {
            false
        };
        if !valid {
            mismatches = mismatches.saturating_add(1);
        }
        rows = rows.saturating_add(1);
        progress.advance();
    }
    assert!(
        scan.exhausted(),
        "account verification scan exhausted normally"
    );
    artifacts.verification(
        phase,
        u64::try_from(expected_accounts.len()).expect("account count fits u64"),
        rows,
        mismatches,
        progress,
    );
}

fn record_resources(
    ctx: &mut StressContext,
    start: ResourcePoint,
    end: ResourcePoint,
    peak_rss: Option<u64>,
) {
    if let (Some(start_rss), Some(end_rss)) = (start.rss_bytes, end.rss_bytes) {
        ctx.record_observation(
            "rss_start_bytes",
            observation_value(start_rss),
            ObservationUnit::Bytes,
            ObservationDirection::Informational,
        )
        .record_observation(
            "rss_end_bytes",
            observation_value(end_rss),
            ObservationUnit::Bytes,
            ObservationDirection::Informational,
        )
        .record_observation(
            "rss_drift_bytes",
            observation_value(end_rss) - observation_value(start_rss),
            ObservationUnit::Bytes,
            ObservationDirection::Informational,
        )
        .record_observation(
            "rss_peak_bytes",
            observation_value(peak_rss.unwrap_or(end_rss)),
            ObservationUnit::Bytes,
            ObservationDirection::Informational,
        );
    }
    ctx.record_observation(
        "database_disk_start_bytes",
        observation_value(start.database_bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    )
    .record_observation(
        "database_disk_end_bytes",
        observation_value(end.database_bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    )
    .record_observation(
        "database_disk_drift_bytes",
        observation_value(end.database_bytes) - observation_value(start.database_bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
}

fn resource_point(started: Instant, database: &Path) -> ResourcePoint {
    ResourcePoint {
        elapsed_ms: started.elapsed().as_millis(),
        rss_bytes: current_rss_bytes(),
        database_bytes: directory_bytes(database),
    }
}

fn append_resource_point(path: &Path, point: ResourcePoint) {
    let mut file = FileOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open resource sample report");
    writeln!(
        file,
        "{},{},{}",
        point.elapsed_ms,
        point
            .rss_bytes
            .map_or_else(String::new, |rss| rss.to_string()),
        point.database_bytes
    )
    .expect("write resource sample");
}

fn current_rss_bytes() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map(sysinfo::Process::memory)
}

fn directory_bytes(root: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(root) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => directory_bytes(&entry.path()),
            Ok(kind) if kind.is_file() => entry.metadata().map_or(0, |metadata| metadata.len()),
            _ => 0,
        })
        .fold(0_u64, u64::saturating_add)
}

fn workload_value(key: &str) -> Vec<u8> {
    let mut state = 0xcbf2_9ce4_8422_2325_u64;
    for byte in key.bytes() {
        state ^= u64::from(byte);
        state = state.wrapping_mul(0x100_0000_01b3);
    }
    let mut value = Vec::with_capacity(VALUE_SIZE);
    while value.len() < VALUE_SIZE {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        value.extend_from_slice(&state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes());
    }
    value.truncate(VALUE_SIZE);
    value
}

fn unique_namespace(scenario: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_millis();
    let simple = scenario
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .collect::<String>()
        .to_ascii_lowercase();
    format!("midge-{simple}-{}-{timestamp}", std::process::id())
}

fn ensure_namespace(protocol: &str, namespace: &str) -> Result<(), String> {
    match protocol {
        "s3" => signed_s3_namespace(namespace),
        "azure" => signed_azure_namespace(namespace),
        "gcs-xml" => signed_gcs_xml_namespace(namespace),
        "gcs-json" => signed_gcs_json_namespace(namespace),
        _ => Err(format!("unsupported Sqrzl protocol {protocol}")),
    }
}

fn sqrzl_endpoint() -> String {
    std::env::var("MIDGE_STRESS_SQRZL_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string())
}

fn sqrzl_secret() -> Result<String, String> {
    let secret = std::env::var("SQRZL_SECRET_ACCESS_KEY")
        .map_err(|_| "SQRZL_SECRET_ACCESS_KEY is required for Sqrzl namespace setup".to_string())?;
    if secret.is_empty() {
        return Err("SQRZL_SECRET_ACCESS_KEY must not be empty".to_string());
    }
    Ok(secret)
}

fn sqrzl_client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())
}

fn accept_namespace_response(response: reqwest::blocking::Response) -> Result<(), String> {
    let status = response.status();
    let body = response.bytes().map_err(|error| error.to_string())?;
    if status.is_success() || matches!(status.as_u16(), 409 | 500) {
        Ok(())
    } else {
        Err(format!(
            "Sqrzl namespace setup returned {status}: {}",
            String::from_utf8_lossy(&body)
        ))
    }
}

fn signed_s3_namespace(namespace: &str) -> Result<(), String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::{Digest, Sha256};

    let endpoint = sqrzl_endpoint();
    let url = reqwest::Url::parse(&endpoint).map_err(|error| error.to_string())?;
    let host = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or("127.0.0.1")),
        None => url.host_str().unwrap_or("127.0.0.1").to_string(),
    };
    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let payload_hash = hex::encode(Sha256::digest(b""));
    let path = format!("/{namespace}");
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request =
        format!("PUT\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let mac = |key: &[u8], data: &[u8]| -> Result<Vec<u8>, String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|error| error.to_string())?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    };
    let aws_key = format!("AWS4{}", sqrzl_secret()?);
    let date_key = mac(aws_key.as_bytes(), date.as_bytes())?;
    let region_key = mac(&date_key, b"us-east-1")?;
    let service_key = mac(&region_key, b"s3")?;
    let signing_key = mac(&service_key, b"aws4_request")?;
    let signature = hex::encode(mac(&signing_key, string_to_sign.as_bytes())?);
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential=admin/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    let response = sqrzl_client()?
        .put(format!("{endpoint}{path}"))
        .header("host", host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .body(Vec::new())
        .send()
        .map_err(|error| error.to_string())?;
    accept_namespace_response(response)
}

fn signed_azure_namespace(container: &str) -> Result<(), String> {
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let endpoint = sqrzl_endpoint();
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let path = format!("/admin/{container}");
    let canonical_headers = format!("x-ms-date:{date}\nx-ms-version:2024-11-04\n");
    let canonical_resource = format!("/admin{path}\nrestype:container");
    let string_to_sign =
        format!("PUT\n\n\n\n\n\n\n\n\n\n\n\n{canonical_headers}{canonical_resource}");
    let key = sqrzl_secret()?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).map_err(|error| error.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let response = sqrzl_client()?
        .put(format!("{endpoint}{path}?restype=container"))
        .header("authorization", format!("SharedKey admin:{signature}"))
        .header("x-ms-date", date)
        .header("x-ms-version", "2024-11-04")
        .body(Vec::new())
        .send()
        .map_err(|error| error.to_string())?;
    accept_namespace_response(response)
}

fn signed_gcs_xml_namespace(bucket: &str) -> Result<(), String> {
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    use sha1::Sha1;

    let endpoint = sqrzl_endpoint();
    let path = format!("/{bucket}");
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let string_to_sign = format!("PUT\n\n\n{date}\n{path}");
    let secret = sqrzl_secret()?;
    let mut mac =
        Hmac::<Sha1>::new_from_slice(secret.as_bytes()).map_err(|error| error.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let response = sqrzl_client()?
        .put(format!("{endpoint}{path}"))
        .header("date", date)
        .header("authorization", format!("GOOG1 admin:{signature}"))
        .body(Vec::new())
        .send()
        .map_err(|error| error.to_string())?;
    accept_namespace_response(response)
}

fn signed_gcs_json_namespace(bucket: &str) -> Result<(), String> {
    let response = sqrzl_client()?
        .post(format!("{}/storage/v1/b?project=sqrzl", sqrzl_endpoint()))
        .header("authorization", "Bearer admin")
        .header("content-type", "application/json")
        .body(format!("{{\"name\":\"{bucket}\"}}"))
        .send()
        .map_err(|error| error.to_string())?;
    accept_namespace_response(response)
}

#[cfg(test)]
static PREPARED_RECOVERY: watchdog_preparation::PreparedSlot<(
    WorkloadArtifacts,
    cntryl_midge::__internal::recovery::PreparedRecoveryProgressFixture,
)> = watchdog_preparation::PreparedSlot::new();

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
pub(super) fn run_watchdog_fixture(ctx: &mut StressContext, resume_successes: bool) {
    let duration = Duration::from_secs(if resume_successes { 2 } else { 4 });
    let case = WorkloadCase {
        benchmark: if resume_successes {
            "resumed_successes_with_disk_churn"
        } else {
            "rejected_clients_with_disk_churn"
        },
        scenario: "watchdog-client-stage",
        backend: "local",
        tier: 5,
        workload: "write-heavy",
        stages: &[1],
    };
    let progress = ctx.progress_handle();
    let mut artifacts = WorkloadArtifacts::begin(case, duration);
    artifacts.enter_stage(0, "1-clients", &progress);
    let database = artifacts.path.join("fixture-database");
    fs::create_dir(&database).expect("create disk-change fixture");
    let _sampler = ResourceSampler::start_with_interval(
        &artifacts,
        &database,
        progress.clone(),
        Duration::from_millis(100),
    );
    let mut growth = FileOptions::new()
        .create(true)
        .append(true)
        .open(database.join("changing.bin"))
        .expect("create changing database file");
    let config = ClientConfig {
        workload: "write-heavy",
        stage: 0,
        client: 0,
        seed_count: 0,
        cloud: false,
    };
    let snapshots = client_snapshot_directory(&artifacts, 0);
    let mut reporter = ClientReporter::new(
        &snapshots,
        config,
        artifacts.started,
        Duration::from_millis(100),
        "1-clients",
    );
    let stop = AtomicBool::new(false);
    let control = ClientControl {
        stop: &stop,
        lease_health: None,
        origin: artifacts.started,
    };
    // This is the production reporter and shared loop. Only operation results,
    // disk churn and shorter report/sample cadences belong to this test fixture.
    let outcome = ctx.measure("worker stage with disk churn", || {
        run_client_with(
            config,
            duration,
            &control,
            ClientClock {
                now: Instant::now,
                sleep: thread::sleep,
            },
            |attempt, _sequences, _account| {
                watchdog_fixture_operation(&mut growth, attempt, resume_successes)
            },
            || progress.advance(),
            |result, report| {
                reporter.report(result, report);
            },
        )
    });
    ctx.metadata(
        "fixture_acknowledged_transactions",
        outcome.stats.acknowledged,
    );
    ctx.metadata("fixture_progress_units", progress.completed_units());
    artifacts.record_stage("1-clients", &outcome.stats, &progress);
    artifacts.enter_phase("complete", "watchdog-client-stage", &progress);
    artifacts.complete = true;
    artifacts.persist("passed");
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
pub(super) fn prepare_recovery_watchdog_fixture(
    mode: cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode,
    samples: usize,
    setup_delay: Duration,
) -> impl PreparationGuard {
    use cntryl_midge::__internal::recovery::{
        prepare_recovery_progress_fixture, RecoveryProgressFixtureMode,
    };

    enable_phase_tracing();
    let benchmark = match mode {
        RecoveryProgressFixtureMode::DelayedRanges => "delayed_recovery_with_flat_cache",
        RecoveryProgressFixtureMode::HeldFirstRange => "held_recovery_with_flat_cache",
        RecoveryProgressFixtureMode::CachedCoverage => "cached_recovery_with_flat_cache",
        RecoveryProgressFixtureMode::MetadataInventory => {
            "metadata_inventory_recovery_with_flat_cache"
        }
        RecoveryProgressFixtureMode::HeldInventory => "held_inventory_recovery_with_flat_cache",
    };
    let case = WorkloadCase {
        benchmark,
        scenario: "watchdog-recovery-stage",
        backend: "local",
        tier: 5,
        workload: "recovery-fixture",
        stages: &[],
    };
    let scope = PREPARED_RECOVERY.install(Vec::new(), |(mut artifacts, _)| {
        artifacts.terminal_error = Some("prepared fixture was not invoked".into());
        // Seal the cancellation receipt before the fallback Drop marks unfinished work failed.
        artifacts.complete = true;
        artifacts.persist("canceled");
    });
    for _ in 0..samples {
        let mut artifacts = WorkloadArtifacts::begin(case, Duration::from_secs(8));
        let fixture_root = artifacts.path.join("recovery-fixture");
        let database = fixture_root.join("local");
        fs::create_dir_all(&database).expect("create empty recovery fixture database");
        let prepared = prepare_recovery_progress_fixture(&fixture_root, mode, setup_delay)
            .unwrap_or_else(|error| {
                artifacts.terminal_error = Some(format!("fixture preparation failed: {error}"));
                artifacts.persist("failed");
                panic!("recovery fixture preparation failed: {error}");
            });
        write_atomic_json(
            &artifacts.path.join("fixture-preparation.json"),
            &json!({
                "setup_elapsed_ms": prepared.setup_elapsed().as_millis(),
                "mode": prepared.mode(),
            }),
        )
        .expect("retain actual component preparation time");
        scope.push((artifacts, prepared));
    }
    scope
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
pub(super) fn run_recovery_watchdog_fixture(
    ctx: &mut StressContext,
    mode: cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode,
) {
    let (mut artifacts, prepared) = PREPARED_RECOVERY.take();
    assert_eq!(
        prepared.mode(),
        mode,
        "run the workload whose input was prepared"
    );
    let database = artifacts.path.join("recovery-fixture/local");
    let progress = ctx.progress_handle();
    ctx.record_observation(
        "fixture_setup_elapsed_ns",
        prepared.setup_elapsed().as_secs_f64() * 1e9,
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    artifacts.enter_phase("recovery", "actual-planner-and-replay", &progress);
    let _sampler = ResourceSampler::start_with_interval(
        &artifacts,
        &database,
        progress.clone(),
        Duration::from_millis(100),
    );
    // The real planner and replay own successful work. The fixture delays or
    // holds their provider responses and retains independent observations.
    let before = progress.completed_units();
    let result = {
        let _scope = RecoveryScope::enter(&progress, &artifacts.resource_phase, artifacts.phase);
        prepared
            .run()
            .expect("actual planner and replay fixture completes")
    };
    let recovery_units = progress.completed_units() - before;
    // This is a complete fixture invocation, not a repeatable timing closure.
    // Native warmup/measured/cooldown samples each consume their own input.
    ctx.record_external_outcome(
        "actual cloud WAL recovery",
        Duration::from_millis(result.elapsed_ms),
        LogicalUnit::new("recovery"),
        OperationOutcome::success(1),
    );
    write_atomic_json(
        &artifacts.path.join("recovery-result.json"),
        &json!({
            "progress_units": recovery_units,
            "expected_records": result.expected_records,
            "verified_records": result.verified_records,
            "mismatches": result.mismatches,
            "max_sequence": result.max_sequence,
            "max_epoch": result.max_epoch,
            "completed_range_reads": result.completed_range_reads,
            "completed_range_bytes": result.completed_range_bytes,
            "maximum_range_bytes": result.maximum_range_bytes,
            "local_wal_bytes": result.local_wal_bytes,
            "staged_wal_count": result.staged_wal_count,
            "elapsed_ms": result.elapsed_ms,
            "coverage_checks": result.coverage_checks,
            "replay_completed_range_reads": result.replay_completed_range_reads,
            "expected_inventory_entries": result.expected_inventory_entries,
            "retained_inventory_entries": result.retained_inventory_entries,
            "completed_inventory_heads": result.completed_inventory_heads,
            "completed_inventory_size_validations": result.completed_inventory_size_validations,
        }),
    )
    .expect("persist successful actual recovery fixture result");
    ctx.metadata("fixture_recovery_verified_records", result.verified_records);
    progress.advance();
    artifacts.enter_phase("complete", "actual-planner-and-replay", &progress);
    artifacts.complete = true;
    artifacts.persist("passed");
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
pub(super) fn run_journal_recovery_watchdog_fixture(ctx: &mut StressContext) {
    use cntryl_midge::__internal::recovery::run_journal_recovery_progress_fixture;

    enable_phase_tracing();
    let case = WorkloadCase {
        benchmark: "journal_recovery_with_flat_cache",
        scenario: "watchdog-recovery-stage",
        backend: "local",
        tier: 5,
        workload: "journal-recovery-fixture",
        stages: &[],
    };
    let progress = ctx.progress_handle();
    let mut artifacts = WorkloadArtifacts::begin(case, Duration::from_secs(8));
    let fixture_root = artifacts.path.join("recovery-fixture");
    let database = fixture_root.join("local");
    fs::create_dir_all(&database).expect("create empty journal fixture database");
    artifacts.enter_phase("recovery", "actual-manifest-journal-replay", &progress);
    let _sampler = ResourceSampler::start_with_interval(
        &artifacts,
        &database,
        progress.clone(),
        Duration::from_millis(100),
    );
    let (result, recovery_units) = ctx
        .measure("actual manifest journal recovery", || {
            let before = progress.completed_units();
            let _scope =
                RecoveryScope::enter(&progress, &artifacts.resource_phase, artifacts.phase);
            run_journal_recovery_progress_fixture(&fixture_root)
                .map(|result| (result, progress.completed_units() - before))
        })
        .expect("actual manifest journal fixture completes");
    write_atomic_json(
        &artifacts.path.join("recovery-result.json"),
        &json!({
            "progress_units": recovery_units,
            "expected_edits": result.expected_edits,
            "verified_edits": result.verified_edits,
            "max_edit_id": result.max_edit_id,
            "manifest_edit_checkpoint_id": result.manifest_edit_checkpoint_id,
            "restored_cf_count": result.restored_cf_count,
            "mismatches": result.mismatches,
            "completed_local_reads": result.completed_local_reads,
            "completed_local_read_bytes": result.completed_local_read_bytes,
            "journal_bytes": result.journal_bytes,
            "local_wal_bytes": result.local_wal_bytes,
            "staged_wal_count": result.staged_wal_count,
            "elapsed_ms": result.elapsed_ms,
        }),
    )
    .expect("persist successful actual journal recovery result");
    ctx.metadata("fixture_recovery_elapsed_ms", result.elapsed_ms);
    ctx.metadata("fixture_recovery_verified_edits", result.verified_edits);
    ctx.metadata("fixture_recovery_progress_units", recovery_units);
    progress.advance();
    artifacts.enter_phase("complete", "actual-manifest-journal-replay", &progress);
    artifacts.complete = true;
    artifacts.persist("passed");
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
pub(super) fn run_recovery_listener_fixture(ctx: &mut StressContext) {
    let progress = ctx.progress_handle();
    let listener_units = ctx.measure("recovery listener isolation", || {
        let before = progress.completed_units();
        recovery_progress::assert_listener_isolation(&progress);
        progress.completed_units() - before
    });
    ctx.metadata("fixture_listener_progress_units", listener_units);
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
fn watchdog_fixture_operation(
    growth: &mut std::fs::File,
    attempt: u64,
    resume_successes: bool,
) -> OperationResult {
    growth.write_all(&[0_u8; 1_024]).expect("grow fixture file");
    growth.flush().expect("flush changing file");
    if resume_successes {
        thread::sleep(Duration::from_millis(100));
        return match attempt {
            0 => Err(MidgeError::WriteStall("initial pressure".into())),
            3 => Err(MidgeError::ResourceLimit("temporary allowance".into())),
            _ => Ok((Some(0), 32, 32, false)),
        };
    }
    if attempt.is_multiple_of(2) {
        Err(MidgeError::WriteStall("scripted pressure".into()))
    } else {
        Err(MidgeError::ResourceLimit("scripted budget".into()))
    }
}

#[cfg(test)]
mod artifact_tests {

    #[test]
    fn should_select_one_original_stage_when_comparison_starts_in_a_fresh_process() {
        use super::*;
        // Arrange
        let case = WorkloadCase {
            benchmark: "fixture",
            scenario: "write-heavy",
            backend: "local",
            tier: 5,
            workload: "write-heavy",
            stages: &[1, 2, 4, 8, 16],
        };
        // Act and Assert
        for clients in ["1", "2", "4", "8", "16"] {
            let selected = comparison_case(case, Some(clients)).unwrap();
            assert_eq!(selected.stages, &[clients.parse::<usize>().unwrap()]);
        }
        assert_eq!(comparison_case(case, None).unwrap().stages, case.stages);
        assert!(comparison_case(case, Some("3")).is_err());
        assert!(comparison_case(case, Some("invalid")).is_err());
    }

    #[test]
    fn should_count_disk_changes_only_within_one_eligible_storage_phase() {
        use super::*;
        // Arrange
        for phase in ["setup", "workload", "verification", "shutdown", "complete"] {
            assert!(!storage_progress_phase(phase));
        }
        assert!(storage_progress_phase("flush"));
        assert!(storage_progress_phase("recovery"));

        // Act and Assert: disabled work, a cross-phase delta, and a phase
        // change during sampling must all leave the external heartbeat idle.
        assert!(!resource_progress_observed(2, 2, 2, 100, 200));
        assert!(!resource_progress_observed(2, 5, 5, 100, 200));
        assert!(!resource_progress_observed(5, 5, 7, 100, 200));
        assert!(!resource_progress_observed(5, 5, 5, 100, 100));
        assert!(resource_progress_observed(5, 5, 5, 100, 200));
    }

    #[test]
    fn should_preserve_typed_shutdown_timeout_when_artifacts_drop_after_failure() {
        use super::*;
        // Arrange
        let mut artifacts = WorkloadArtifacts::begin(
            WorkloadCase {
                benchmark: "shutdown_artifact_test",
                scenario: "shutdown-artifact-test",
                backend: "local",
                tier: 5,
                workload: "write-heavy",
                stages: &[1],
            },
            Duration::from_secs(1),
        );
        let status_path = artifacts.path.join("workload-status.json");
        let result = Err(MidgeError::Timeout("owned join still running".to_string()));

        // Act
        artifacts.record_shutdown("shutdown-before-recovery", &result, SHUTDOWN_CALLER_BUDGET);
        let before_drop: serde_json::Value =
            serde_json::from_slice(&fs::read(&status_path).unwrap()).unwrap();
        drop(artifacts);
        let after_drop: serde_json::Value =
            serde_json::from_slice(&fs::read(status_path).unwrap()).unwrap();

        // Assert
        assert_eq!(before_drop["status"], "failed");
        assert_eq!(
            before_drop["shutdown_results"],
            after_drop["shutdown_results"]
        );
        let shutdown = &after_drop["shutdown_results"][0];
        assert_eq!(shutdown["caller_result"], "timeout");
        assert_eq!(shutdown["caller_budget_ms"], 45_000);
        assert!(shutdown["error"].as_str().unwrap().starts_with("Timeout("));
    }

    #[test]
    fn should_reestablish_baseline_when_phase_changes_during_resource_sampling() {
        use super::*;

        // Arrange: the first scan overlaps disabled workload and enabled flush.
        let mut resource_progress = ResourceProgress {
            phase: 2,
            database_bytes: 0,
        };

        // Act and Assert: the overlapping sample and its successor cannot
        // attribute workload growth to the newly entered storage phase.
        assert!(!resource_progress.observe(2, 5, 100));
        assert!(!resource_progress.observe(5, 5, 200));
        assert!(resource_progress.observe(5, 5, 300));
    }
}
