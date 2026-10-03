//! Shared Tier 5 and Tier 6 workloads for Midge's isolated stress benches.

use cntryl_midge::{
    Bytes, CloudProviderConfig, CloudStorageLocation, ColumnFamilyHandle, Engine, MemoryBudget,
    MidgeError, OpenOptions, Query, RuntimeMetricsSnapshot, TransactionMode, WorkloadProfile,
    WriteOptions,
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const VALUE_SIZE: usize = 128;
const SEED_ROWS: usize = 512;
const SEED_BATCH: usize = 64;
const WRITE_BATCH_ROWS: usize = 32;
const MIXED_ROWS: [usize; 5] = [1, 8, 128, 1_024, 4_096];
const WRITE_ROW_COUNTS: [usize; 11] = [32, 1, 1, 1, 8, 128, 1_024, 4_096, 1, 1, 128];
const RESOURCE_SAMPLE_PERIOD: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub(super) struct WorkloadCase {
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
}

#[derive(Debug)]
struct StageStats {
    attempts: u64,
    acknowledged: u64,
    logical_operations: u64,
    acknowledged_rows: u64,
    saturation: SaturationCounts,
    latency_us: Histogram<u64>,
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
        self.latency_us
            .add(&other.latency_us)
            .expect("merge transaction latency histogram");
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

struct WorkloadArtifacts {
    path: PathBuf,
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
    progress_units: u64,
    checks: Vec<serde_json::Value>,
    complete: bool,
}

impl WorkloadArtifacts {
    fn begin(case: WorkloadCase, duration: Duration) -> Self {
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
            progress_units: 0,
            checks: Vec::new(),
            complete: false,
        };
        artifacts.persist("running");
        artifacts
    }

    fn record_stage(&mut self, stage: &str, stats: &StageStats, progress: &ProgressHandle) {
        self.phase = "workload";
        self.stage = stage.to_string();
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
        self.progress_units = progress.completed_units();
        append_stage_csv(&self.path, stage, stats);
        self.persist("running");
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
        fs::write(
            self.path.join("verification-summary.json"),
            serde_json::to_vec_pretty(&json!({
                "scenario": self.scenario,
                "backend": self.backend,
                "passed": self.checks.iter().all(|check| check["passed"] == true),
                "checks": self.checks,
            }))
            .expect("serialize verification summary"),
        )
        .expect("write verification summary");
        self.progress_units = progress.completed_units();
        self.persist(if passed { "running" } else { "failed" });
        assert!(
            passed,
            "{phase} verification failed: expected {expected_rows} rows, found {actual_rows}, mismatches {mismatches}; artifacts: {}",
            self.path.display()
        );
    }

    fn finish(&mut self, progress: &ProgressHandle) {
        self.phase = "complete";
        self.stage = "flush-reopen-recovery".to_string();
        self.progress_units = progress.completed_units();
        self.complete = true;
        self.persist("passed");
    }

    fn persist(&self, status: &str) {
        fs::write(
            self.path.join("workload-status.json"),
            serde_json::to_vec_pretty(&json!({
                "scenario": self.scenario,
                "backend": self.backend,
                "status": status,
                "phase": self.phase,
                "stage": self.stage,
                "configured_duration_seconds": self.configured_seconds,
                "attempted_transactions": self.attempts,
                "acknowledged_transactions": self.acknowledged,
                "acknowledged_rows": self.acknowledged_rows,
                "resource_limit_responses": self.resource_limit,
                "write_stall_responses": self.write_stall,
                "progress_completed_units": self.progress_units,
                "verification_checks": self.checks.len(),
            }))
            .expect("serialize workload status"),
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

#[derive(Default)]
struct WorkerResult {
    stats: StageStats,
    kind_counts: [u64; 11],
    account_updates: u64,
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
    fn start(artifacts: &WorkloadArtifacts, database: &Path) -> Self {
        let started = Instant::now();
        let report = artifacts.path.join("resource-samples.csv");
        fs::write(&report, "elapsed_ms,rss_bytes,database_disk_bytes\n")
            .expect("write resource sample header");
        let start = resource_point(started, database);
        append_resource_point(&report, start);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_database = database.to_path_buf();
        let thread_report = report.clone();
        let peak = Arc::new(Mutex::new(start.rss_bytes));
        let thread_peak = Arc::clone(&peak);
        let interval = std::env::var("MIDGE_STRESS_RESOURCE_SAMPLE_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map_or(RESOURCE_SAMPLE_PERIOD, Duration::from_secs);
        let handle = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                thread::park_timeout(interval);
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                let point = resource_point(started, &thread_database);
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
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            handle.join().expect("resource sampler thread completes");
        }
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
}

struct OpenedCase {
    engine: Engine,
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
}

pub(super) fn run_case(ctx: &mut StressContext, case: WorkloadCase) {
    let duration = case_duration(case.tier);
    let progress = ctx.progress_handle();
    let mut artifacts = WorkloadArtifacts::begin(case, duration);
    let run_dir = tempfile::tempdir().expect("create isolated stress directory");
    let mut opened = open_case(case, run_dir.path(), &progress);
    let sampler = ResourceSampler::start(&artifacts, &opened.database);
    let expected = run_stages(ctx, case, duration, &mut opened, &progress, &mut artifacts);
    let (resource_start, resource_end, peak_rss) = sampler.finish();
    record_resources(ctx, resource_start, resource_end, peak_rss);
    finish_case(opened, &expected, &progress, &mut artifacts);
    artifacts.finish(&progress);
}

fn open_case(case: WorkloadCase, root: &Path, progress: &ProgressHandle) -> OpenedCase {
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
    let (engine, family) = open_family(options, true);
    progress.advance();
    let has_reads = matches!(
        case.workload,
        "read-heavy" | "balanced" | "mixed-workload-soak"
    );
    if has_reads {
        seed(&engine, &family, progress, cloud);
    }
    OpenedCase {
        engine,
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
    let mut expected = BTreeMap::new();
    let budgets = stage_budgets(duration, case.stages.len());
    for (stage_index, (&clients, budget)) in case.stages.iter().zip(budgets).enumerate() {
        let started = Instant::now();
        let before = runtime_metrics(&opened.engine);
        let (stage, outcomes) = run_stage(case, stage_index, clients, budget, opened, progress);
        for (client, outcome) in outcomes.into_iter().enumerate() {
            stage_expected_rows(&mut expected, stage_index, client, &outcome);
        }
        let elapsed = started.elapsed().max(Duration::from_nanos(1));
        let after = runtime_metrics(&opened.engine);
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
            },
        );
        artifacts.record_stage(&format!("{clients}-clients"), &stage, progress);
    }
    expected
}

fn run_stage(
    case: WorkloadCase,
    stage_index: usize,
    clients: usize,
    budget: Duration,
    opened: &OpenedCase,
    progress: &ProgressHandle,
) -> (StageStats, Vec<WorkerResult>) {
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
            handles.push(
                scope.spawn(move || run_client(engine, family_id, config, budget, &heartbeat)),
            );
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

fn finish_case(
    mut opened: OpenedCase,
    expected: &BTreeMap<(usize, usize, usize), u64>,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
) {
    opened
        .engine
        .flush_cf(&opened.family)
        .expect("flush acknowledged stress data");
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
    opened
        .engine
        .shutdown(Duration::from_secs(60))
        .expect("shutdown stress engine before recovery check");
    drop(opened.engine);
    if opened.cloud {
        fs::remove_dir_all(&opened.database).expect("remove local cloud cache before recovery");
        let options = cloud_options(
            &opened.database,
            opened.backend,
            &opened.namespace,
            &opened.object_prefix,
        );
        let (mut recovered, family) = open_family(options, false);
        verify_database(
            &recovered,
            &family,
            expected_seed,
            expected,
            progress,
            artifacts,
            "cloud-recovered",
        );
        recovered
            .shutdown(Duration::from_secs(60))
            .expect("shutdown recovered cloud engine");
    } else {
        let (mut recovered, family) = open_family(local_options(&opened.database), false);
        verify_database(
            &recovered,
            &family,
            expected_seed,
            expected,
            progress,
            artifacts,
            "reopened",
        );
        recovered
            .shutdown(Duration::from_secs(60))
            .expect("shutdown reopened local engine");
    }
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
    OpenOptions::cloud(
        path,
        CloudStorageLocation::new(provider, object_prefix.to_string()),
    )
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
) -> WorkerResult {
    let deadline = Instant::now() + budget;
    let mut result = WorkerResult::default();
    let mut sequences = [0_u64; 11];
    let mut account = 0_u64;
    let mut attempt = 0_u64;
    while Instant::now() < deadline {
        let started = Instant::now();
        result.stats.attempts = result.stats.attempts.saturating_add(1);
        match run_operation(
            engine,
            family_id,
            &config,
            attempt,
            &mut sequences,
            &mut account,
        ) {
            Ok((kind, operations, rows, account_updated)) => {
                result.stats.acknowledged = result.stats.acknowledged.saturating_add(1);
                result.stats.logical_operations =
                    result.stats.logical_operations.saturating_add(operations);
                result.stats.acknowledged_rows =
                    result.stats.acknowledged_rows.saturating_add(rows);
                if let Some(kind) = kind {
                    result.kind_counts[kind] = result.kind_counts[kind].saturating_add(1);
                }
                result.account_updates = result
                    .account_updates
                    .saturating_add(u64::from(account_updated));
            }
            Err(MidgeError::ResourceLimit(_)) => {
                result.stats.saturation.resource_limit =
                    result.stats.saturation.resource_limit.saturating_add(1);
            }
            Err(MidgeError::WriteStall(_)) => {
                result.stats.saturation.write_stall =
                    result.stats.saturation.write_stall.saturating_add(1);
            }
            Err(error) => panic!(
                "{} workload transaction failed: {error}",
                workload_name(config.workload)
            ),
        }
        result.stats.record_latency(started.elapsed());
        progress.advance();
        attempt = attempt.saturating_add(1);
    }
    result
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
    for (name, quantile) in [
        ("transaction_latency_p50_us", 0.50),
        ("transaction_latency_p95_us", 0.95),
        ("transaction_latency_p99_us", 0.99),
    ] {
        let value = if stats.latency_us.is_empty() {
            0.0
        } else {
            observation_value(stats.latency_us.value_at_quantile(quantile))
        };
        ctx.record_observation(
            name,
            value,
            ObservationUnit::Microseconds,
            ObservationDirection::LowerIsBetter,
        );
    }
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

fn observation_value(value: u64) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("integer observations fit in f64")
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
        writeln!(file,"stage,attempted_transactions,acknowledged_transactions,logical_operations,acknowledged_rows,resource_limit_responses,write_stall_responses,latency_p50_us,latency_p95_us,latency_p99_us")
            .expect("write stage report header");
    }
    writeln!(
        file,
        "{stage},{},{},{},{},{},{},{},{},{}",
        stats.attempts,
        stats.acknowledged,
        stats.logical_operations,
        stats.acknowledged_rows,
        stats.saturation.resource_limit,
        stats.saturation.write_stall,
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
    let date_key = mac(b"AWS4easy-peasy", date.as_bytes())?;
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
    let key = base64::engine::general_purpose::STANDARD
        .decode("easy-peasy")
        .unwrap_or_else(|_| b"easy-peasy".to_vec());
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|error| error.to_string())?;
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
    let mut mac = Hmac::<Sha1>::new_from_slice(b"easy-peasy").map_err(|error| error.to_string())?;
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
