//! Tier 4 - bounded, partitioned compaction qualification.
//!
//! This is an explicit scale harness rather than a duration-based benchmark.
//! It models one primary address entry plus postal, locality, and street lookup
//! entries for every base record, crashes the writer process after compaction,
//! and verifies the complete ordered digest after crash recovery and clean reopen.

use bytes::Bytes;
use cntryl_midge::{
    init_benchmark_telemetry, Engine, Goal, MemoryBudget, MidgeError, MidgeResult, OpenOptions,
    Query, TransactionMode, WorkloadProfile, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
use xxhash_rust::xxh3::Xxh3;

const LOGICAL_ENTRIES_PER_BASE: u64 = 4;
const LOGICAL_BYTES_PER_BASE: u64 = 88;
const DISK_SAFETY_MULTIPLIER: u64 = 5;
const DISK_HEADROOM_BYTES: u64 = 20 * 1024 * 1024 * 1024;
const INGEST_BASE_BATCH: u64 = 2_500;
const FLUSH_BASE_INTERVAL: u64 = 250_000;
const POINT_SAMPLES: u64 = 1_000;
const PREFIX_SAMPLES: u32 = 200;
const PREFIX_SCAN_LIMIT: usize = 100;
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const MIB: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct DigestEvidence {
    entries: u64,
    xxh3_128: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LatencyEvidence {
    samples: usize,
    p95_micros: u64,
    p99_micros: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerEvidence {
    base_records: u64,
    logical_entries: u64,
    logical_dataset_bytes: u64,
    ingest_seconds: f64,
    compaction_seconds: f64,
    pre_compaction_sst_bytes: u64,
    post_compaction_sst_bytes: u64,
    compaction_bytes_rewritten: u64,
    write_amplification: f64,
    target_sst_size: usize,
    output_count: usize,
    output_sizes: Vec<u64>,
    remaining_l0_file_count: usize,
    remaining_l0_bytes: u64,
    peak_rss_bytes: u64,
    write_stalls_total: u64,
    pending_compactions: usize,
    obsolete_file_backlog: usize,
    digest_before_crash: DigestEvidence,
}

#[derive(Debug, Serialize)]
struct QualificationEvidence {
    schema_version: u32,
    git_sha: String,
    command: String,
    database_path: String,
    hardware_model: String,
    cpu_model: String,
    cpu_arch: String,
    physical_cores: usize,
    total_memory_bytes: u64,
    available_disk_bytes: u64,
    required_disk_bytes: u64,
    worker: WorkerEvidence,
    crash_recovery_seconds: f64,
    clean_reopen_seconds: f64,
    cold_point: LatencyEvidence,
    warm_point: LatencyEvidence,
    cold_prefix_scan: LatencyEvidence,
    warm_prefix_scan: LatencyEvidence,
    digest_after_crash: DigestEvidence,
    digest_after_clean_reopen: DigestEvidence,
}

fn json_error(error: &serde_json::Error) -> MidgeError {
    MidgeError::Internal(format!("qualification evidence JSON failed: {error}"))
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> MidgeResult<Self> {
        let pid = sysinfo::get_current_pid()
            .map_err(|error| MidgeError::Internal(format!("resolve process id: {error}")))?;
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(0));
        let worker_stop = Arc::clone(&stop);
        let worker_peak = Arc::clone(&peak);
        let handle = thread::spawn(move || {
            let mut system = System::new();
            while !worker_stop.load(Ordering::Acquire) {
                system.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[pid]),
                    false,
                    ProcessRefreshKind::nothing().with_memory(),
                );
                if let Some(process) = system.process(pid) {
                    worker_peak.fetch_max(process.memory(), Ordering::AcqRel);
                }
                thread::sleep(RSS_SAMPLE_INTERVAL);
            }
        });
        Ok(Self {
            stop,
            peak,
            handle: Some(handle),
        })
    }

    fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("RSS sampler thread");
        }
        self.peak.load(Ordering::Acquire)
    }
}

fn primary_key(id: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = b'a';
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}

fn lookup_key(tag: u8, bucket: u32, id: u64) -> [u8; 13] {
    let mut key = [0; 13];
    key[0] = tag;
    key[1..5].copy_from_slice(&bucket.to_be_bytes());
    key[5..].copy_from_slice(&id.to_be_bytes());
    key
}

fn primary_value(id: u64) -> [u8; 16] {
    let mut value = [0; 16];
    value[..4].copy_from_slice(
        &u32::try_from(id % 100_000)
            .expect("postal bucket")
            .to_be_bytes(),
    );
    value[4..8].copy_from_slice(
        &u32::try_from(id % 10_000)
            .expect("locality bucket")
            .to_be_bytes(),
    );
    value[8..12].copy_from_slice(
        &u32::try_from(id % 1_000_000)
            .expect("street bucket")
            .to_be_bytes(),
    );
    value[12..].copy_from_slice(
        &u32::try_from(id % 100_000)
            .expect("house number")
            .to_be_bytes(),
    );
    value
}

fn qualification_target_sst_size(base_records: u64) -> usize {
    match base_records {
        0..=1_000_000 => 16 * MIB,
        1_000_001..=10_000_000 => 64 * MIB,
        _ => 128 * MIB,
    }
}

fn options(path: &Path, base_records: u64) -> MidgeResult<OpenOptions> {
    OpenOptions::local(path)
        .goal(Goal::Throughput)
        .workload(WorkloadProfile::WriteHeavy)
        .memory_budget(MemoryBudget::Bytes(4 * 1024 * 1024 * 1024))
        .with_memtable_size_limit(256 * 1024 * 1024)
        .target_sst_size_for_testing(qualification_target_sst_size(base_records))
        .runtime_response_timeout(Duration::from_hours(4))
        .lease_ttl(Duration::from_secs(2))
        .lease_clock_skew_tolerance(Duration::ZERO)
        .background_compaction(false)
        .build()
}

fn open_after_crash(path: &Path, base_records: u64) -> MidgeResult<Engine> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Engine::open(options(path, base_records)?) {
            Ok(engine) => return Ok(engine),
            Err(MidgeError::LeaseHeld(_)) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(error),
        }
    }
}

fn add_base_record(transaction: &mut cntryl_midge::Transaction, id: u64) -> MidgeResult<()> {
    let id_value = id.to_be_bytes();
    transaction.put(primary_key(id).to_vec(), primary_value(id).to_vec(), None)?;
    transaction.put(
        lookup_key(
            b'p',
            u32::try_from(id % 100_000).expect("postal bucket"),
            id,
        )
        .to_vec(),
        id_value.to_vec(),
        None,
    )?;
    transaction.put(
        lookup_key(
            b'l',
            u32::try_from(id % 10_000).expect("locality bucket"),
            id,
        )
        .to_vec(),
        id_value.to_vec(),
        None,
    )?;
    transaction.put(
        lookup_key(
            b's',
            u32::try_from(id % 1_000_000).expect("street bucket"),
            id,
        )
        .to_vec(),
        id_value.to_vec(),
        None,
    )
}

fn total_sst_bytes(engine: &Engine) -> MidgeResult<u64> {
    Ok(engine
        .get_storage_layout()?
        .levels
        .iter()
        .map(|level| level.total_bytes)
        .sum())
}

fn digest_engine(engine: &Engine, cf_id: u32) -> MidgeResult<DigestEvidence> {
    let transaction = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let iterator = transaction.scan(&Query::new())?;
    let mut hasher = Xxh3::new();
    let mut entries = 0_u64;
    for result in iterator {
        let (key, value) = result?;
        hasher.update(&u64::try_from(key.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(&key);
        hasher.update(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(&value);
        entries = entries.saturating_add(1);
    }
    Ok(DigestEvidence {
        entries,
        xxh3_128: format!("{:032x}", hasher.digest128()),
    })
}

fn percentile(mut samples: Vec<u64>) -> LatencyEvidence {
    samples.sort_unstable();
    let select = |percent: usize| {
        let index = samples
            .len()
            .saturating_mul(percent)
            .div_ceil(100)
            .saturating_sub(1)
            .min(samples.len().saturating_sub(1));
        samples.get(index).copied().unwrap_or(0)
    };
    LatencyEvidence {
        samples: samples.len(),
        p95_micros: select(95),
        p99_micros: select(99),
    }
}

fn sample_point_latency(
    engine: &Engine,
    cf_id: u32,
    base_records: u64,
) -> MidgeResult<LatencyEvidence> {
    let samples = POINT_SAMPLES.min(base_records);
    let transaction = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let mut latencies = Vec::with_capacity(usize::try_from(samples).unwrap_or(usize::MAX));
    for sample in 0..samples {
        let id = sample.saturating_mul(base_records / samples.max(1));
        let started = Instant::now();
        let value = transaction.get(&primary_key(id))?;
        latencies.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        if value.as_deref() != Some(primary_value(id).as_slice()) {
            return Err(MidgeError::Corruption(format!(
                "point sample {id} returned incorrect value"
            )));
        }
    }
    Ok(percentile(latencies))
}

fn sample_prefix_latency(engine: &Engine, cf_id: u32) -> MidgeResult<LatencyEvidence> {
    let transaction = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let mut latencies = Vec::with_capacity(PREFIX_SAMPLES as usize);
    for postal in 0..PREFIX_SAMPLES {
        let mut prefix = [0; 5];
        prefix[0] = b'p';
        prefix[1..].copy_from_slice(&postal.to_be_bytes());
        let query = Query::new()
            .prefix(Bytes::copy_from_slice(&prefix))
            .limit(PREFIX_SCAN_LIMIT);
        let started = Instant::now();
        let values = transaction.scan(&query)?.try_collect()?;
        latencies.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        if values.iter().any(|(key, _)| !key.starts_with(&prefix)) {
            return Err(MidgeError::Corruption(
                "prefix scan escaped its requested postal bucket".to_string(),
            ));
        }
    }
    Ok(percentile(latencies))
}

fn validate_partition_layout(
    engine: &Engine,
    cf_id: u32,
    target: usize,
    block_size: usize,
) -> MidgeResult<(Vec<u64>, u64, usize, u64)> {
    let layout = engine.get_storage_layout()?;
    let files: Vec<_> = layout
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .filter(|file| file.cf_id == cf_id && file.level > 0)
        .collect();
    let unique: HashSet<_> = files.iter().map(|file| file.name.as_str()).collect();
    if unique.len() != files.len() {
        return Err(MidgeError::Corruption(
            "compaction manifest contains duplicate output names".to_string(),
        ));
    }
    if files.len() < 2 {
        return Err(MidgeError::ResourceLimit(format!(
            "qualification expected multiple compaction outputs at target {target}, observed {}",
            files.len()
        )));
    }
    let allowance =
        u64::try_from(block_size.saturating_add(16 * 1024).saturating_add(64)).unwrap_or(u64::MAX);
    let target = u64::try_from(target).unwrap_or(u64::MAX);
    if let Some(file) = files
        .iter()
        .find(|file| file.size_bytes > target.saturating_add(allowance))
    {
        return Err(MidgeError::ResourceLimit(format!(
            "partition {} is {} bytes, above target plus allowance {}",
            file.name,
            file.size_bytes,
            target.saturating_add(allowance)
        )));
    }
    let sizes = files.iter().map(|file| file.size_bytes).collect::<Vec<_>>();
    let total = layout.levels.iter().map(|level| level.total_bytes).sum();
    let l0_file_count = layout
        .levels
        .iter()
        .find(|level| level.level == 0)
        .map_or(0, |level| level.file_count);
    let l0_bytes = layout
        .levels
        .iter()
        .find(|level| level.level == 0)
        .map_or(0, |level| level.total_bytes);
    Ok((sizes, total, l0_file_count, l0_bytes))
}

#[allow(clippy::cast_precision_loss)]
fn write_amplification(compaction_bytes_rewritten: u64, pre_compaction_sst_bytes: u64) -> f64 {
    if pre_compaction_sst_bytes == 0 {
        0.0
    } else {
        compaction_bytes_rewritten as f64 / pre_compaction_sst_bytes as f64
    }
}

fn run_worker(base_records: u64, path: &Path, partial_path: &Path) -> MidgeResult<()> {
    init_benchmark_telemetry()?;
    let sampler = RssSampler::start()?;
    let open_options = options(path, base_records)?;
    let target_sst_size = open_options.target_sst_size();
    let block_size = open_options.block_size();
    let engine = Engine::open(open_options)?;
    let cf = engine.create_column_family("addresses")?;

    let ingest_started = Instant::now();
    let mut next_flush = FLUSH_BASE_INTERVAL;
    let mut start = 0_u64;
    while start < base_records {
        let end = start.saturating_add(INGEST_BASE_BATCH).min(base_records);
        let mut transaction = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        for id in start..end {
            add_base_record(&mut transaction, id)?;
        }
        transaction.commit(WriteOptions::best_effort())?;
        start = end;
        if start >= next_flush {
            engine.flush_cf(&cf)?;
            next_flush = next_flush.saturating_add(FLUSH_BASE_INTERVAL);
        }
        if start.is_multiple_of(1_000_000) || start == base_records {
            eprintln!("bounded-compaction load {start}/{base_records}");
        }
    }
    engine.flush_cf(&cf)?;
    let ingest_seconds = ingest_started.elapsed().as_secs_f64();
    let pre_compaction_sst_bytes = total_sst_bytes(&engine)?;
    let metrics_before = engine.get_runtime_metrics()?;

    let compaction_started = Instant::now();
    engine.compact_all()?;
    let compaction_seconds = compaction_started.elapsed().as_secs_f64();
    let metrics_after = engine.get_runtime_metrics()?;
    let (output_sizes, post_compaction_sst_bytes, remaining_l0_file_count, remaining_l0_bytes) =
        validate_partition_layout(&engine, cf.id(), target_sst_size, block_size)?;
    let digest_before_crash = digest_engine(&engine, cf.id())?;
    let expected_entries = base_records.saturating_mul(LOGICAL_ENTRIES_PER_BASE);
    if digest_before_crash.entries != expected_entries {
        return Err(MidgeError::Corruption(format!(
            "expected {expected_entries} logical entries, observed {}",
            digest_before_crash.entries
        )));
    }
    let compaction_bytes_rewritten = metrics_after
        .compaction_bytes_rewritten
        .saturating_sub(metrics_before.compaction_bytes_rewritten);
    let write_amplification =
        write_amplification(compaction_bytes_rewritten, pre_compaction_sst_bytes);
    let peak_rss_bytes = sampler.finish();
    let evidence = WorkerEvidence {
        base_records,
        logical_entries: expected_entries,
        logical_dataset_bytes: base_records.saturating_mul(LOGICAL_BYTES_PER_BASE),
        ingest_seconds,
        compaction_seconds,
        pre_compaction_sst_bytes,
        post_compaction_sst_bytes,
        compaction_bytes_rewritten,
        write_amplification,
        target_sst_size,
        output_count: output_sizes.len(),
        output_sizes,
        remaining_l0_file_count,
        remaining_l0_bytes,
        peak_rss_bytes,
        write_stalls_total: metrics_after.write_stalls_total,
        pending_compactions: metrics_after.pending_compactions,
        obsolete_file_backlog: metrics_after.obsolete_file_backlog,
        digest_before_crash,
    };
    std::fs::write(
        partial_path,
        serde_json::to_vec_pretty(&evidence).map_err(|error| json_error(&error))?,
    )?;
    // Deliberately bypass Engine::drop and shutdown. The parent must reconcile
    // the durable manifest/WAL/intent state as a crashed process would leave it.
    std::process::exit(0);
}

#[cfg(unix)]
fn available_disk_bytes(path: &Path) -> MidgeResult<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        MidgeError::InvalidArgument("qualification path contains a NUL byte".to_string())
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` points to writable storage.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(MidgeError::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: successful statvfs initializes the output structure.
    let stats = unsafe { stats.assume_init() };
    Ok(u64::from(stats.f_bavail).saturating_mul(stats.f_frsize))
}

#[cfg(not(unix))]
fn available_disk_bytes(_path: &Path) -> MidgeResult<u64> {
    Err(MidgeError::NotSupported(
        "bounded compaction qualification disk preflight is not implemented on this platform"
            .to_string(),
    ))
}

fn parse_args() -> MidgeResult<(bool, u64, PathBuf, PathBuf)> {
    let mut worker = false;
    let mut base_records = None;
    let mut path = None;
    let mut output = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--worker" => worker = true,
            "--base-records" => {
                base_records = args.next().and_then(|value| value.parse().ok());
            }
            "--path" => path = args.next().map(PathBuf::from),
            "--output" => output = args.next().map(PathBuf::from),
            other if other.starts_with("--bench") => {}
            other => {
                return Err(MidgeError::InvalidArgument(format!(
                    "unknown qualification argument: {other}"
                )));
            }
        }
    }
    let base_records = base_records
        .ok_or_else(|| MidgeError::InvalidArgument("--base-records is required".to_string()))?;
    let path = path.ok_or_else(|| MidgeError::InvalidArgument("--path is required".to_string()))?;
    let output =
        output.ok_or_else(|| MidgeError::InvalidArgument("--output is required".to_string()))?;
    Ok((worker, base_records, path, output))
}

fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_string(),
        )
}

fn command_value(program: &str, args: &[&str]) -> Option<String> {
    std::process::Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}

fn preflight_disk(base_records: u64, path: &Path) -> MidgeResult<(u64, u64)> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let available = available_disk_bytes(parent)?;
    let logical_dataset_bytes = base_records
        .checked_mul(LOGICAL_BYTES_PER_BASE)
        .ok_or_else(|| MidgeError::ResourceLimit("logical dataset size overflow".to_string()))?;
    let required = logical_dataset_bytes
        .checked_mul(DISK_SAFETY_MULTIPLIER)
        .and_then(|bytes| bytes.checked_add(DISK_HEADROOM_BYTES))
        .ok_or_else(|| MidgeError::ResourceLimit("disk preflight size overflow".to_string()))?;
    if available < required {
        return Err(MidgeError::NoSpace(format!(
            "qualification requires {required} free bytes, only {available} are available"
        )));
    }
    Ok((available, required))
}

fn run_parent(base_records: u64, path: &Path, output: &Path) -> MidgeResult<()> {
    if path.exists() {
        return Err(MidgeError::InvalidArgument(format!(
            "qualification database path already exists: {}",
            path.display()
        )));
    }
    let (available_disk_bytes, required_disk_bytes) = preflight_disk(base_records, path)?;

    let partial_path = output.with_extension("worker.json");
    if let Some(output_parent) = output.parent() {
        std::fs::create_dir_all(output_parent)?;
    }
    let executable = std::env::current_exe()?;
    let status = std::process::Command::new(&executable)
        .arg("--worker")
        .arg("--base-records")
        .arg(base_records.to_string())
        .arg("--path")
        .arg(path)
        .arg("--output")
        .arg(&partial_path)
        .status()?;
    if !status.success() {
        return Err(MidgeError::Internal(format!(
            "qualification crash worker exited with {status}"
        )));
    }
    let worker: WorkerEvidence = serde_json::from_slice(&std::fs::read(&partial_path)?)
        .map_err(|error| json_error(&error))?;

    let recovery_started = Instant::now();
    let mut recovered = open_after_crash(path, base_records)?;
    let recovered_cf = recovered.get_column_family("addresses").ok_or_else(|| {
        MidgeError::RecoveryFailed("addresses column family missing after crash".to_string())
    })?;
    let crash_recovery_seconds = recovery_started.elapsed().as_secs_f64();
    let cold_point = sample_point_latency(&recovered, recovered_cf.id(), base_records)?;
    let cold_prefix_scan = sample_prefix_latency(&recovered, recovered_cf.id())?;
    let warm_point = sample_point_latency(&recovered, recovered_cf.id(), base_records)?;
    let warm_prefix_scan = sample_prefix_latency(&recovered, recovered_cf.id())?;
    let digest_after_crash = digest_engine(&recovered, recovered_cf.id())?;
    if digest_after_crash.xxh3_128 != worker.digest_before_crash.xxh3_128
        || digest_after_crash.entries != worker.digest_before_crash.entries
    {
        return Err(MidgeError::RecoveryFailed(
            "crash-recovery digest differs from pre-crash authority".to_string(),
        ));
    }
    recovered.shutdown(Duration::from_secs(30))?;

    let clean_started = Instant::now();
    let mut reopened = Engine::open(options(path, base_records)?)?;
    let reopened_cf = reopened.get_column_family("addresses").ok_or_else(|| {
        MidgeError::RecoveryFailed("addresses column family missing after clean reopen".to_string())
    })?;
    let clean_reopen_seconds = clean_started.elapsed().as_secs_f64();
    let digest_after_clean_reopen = digest_engine(&reopened, reopened_cf.id())?;
    if digest_after_clean_reopen.xxh3_128 != worker.digest_before_crash.xxh3_128
        || digest_after_clean_reopen.entries != worker.digest_before_crash.entries
    {
        return Err(MidgeError::RecoveryFailed(
            "clean-reopen digest differs from pre-crash authority".to_string(),
        ));
    }
    reopened.shutdown(Duration::from_secs(30))?;

    let mut system = System::new_all();
    system.refresh_memory();
    let evidence = QualificationEvidence {
        schema_version: 1,
        git_sha: git_sha(),
        command: std::env::args().collect::<Vec<_>>().join(" "),
        database_path: path.display().to_string(),
        hardware_model: command_value("sysctl", &["-n", "hw.model"])
            .or_else(System::name)
            .unwrap_or_else(|| "unknown".to_string()),
        cpu_model: system
            .cpus()
            .first()
            .map_or_else(|| "unknown".to_string(), |cpu| cpu.brand().to_string()),
        cpu_arch: System::cpu_arch(),
        physical_cores: System::physical_core_count().unwrap_or(0),
        total_memory_bytes: system.total_memory(),
        available_disk_bytes,
        required_disk_bytes,
        worker,
        crash_recovery_seconds,
        clean_reopen_seconds,
        cold_point,
        warm_point,
        cold_prefix_scan,
        warm_prefix_scan,
        digest_after_crash,
        digest_after_clean_reopen,
    };
    let json = serde_json::to_vec_pretty(&evidence).map_err(|error| json_error(&error))?;
    std::fs::write(output, &json)?;
    println!("{}", String::from_utf8_lossy(&json));
    Ok(())
}

fn main() {
    let result = parse_args().and_then(|(worker, base_records, path, output)| {
        if worker {
            run_worker(base_records, &path, &output)
        } else {
            run_parent(base_records, &path, &output)
        }
    });
    if let Err(error) = result {
        eprintln!("bounded compaction qualification failed: {error}");
        std::process::exit(1);
    }
}
