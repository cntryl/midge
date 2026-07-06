#![allow(rustdoc::broken_intra_doc_links)]

//! Utilities intended for benchmarks (and other perf-style harnesses).
//!
//! The goal of this module is to keep benchmark targets thin by centralizing:
//! - deterministic temp path generation
//! - storage-mode parameterization
//! - engine setup from high-level knobs (via `OpenOptions`)

use crate::testkit::{MidgeOptions, StorageMode};
use crate::{Engine, Goal, MemoryBudget, RuntimeMetricsSnapshot, WorkloadProfile};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use crate::diagnostics::{TransactionCommitTimingGuard, TransactionCommitTimingSample};

pub const DEFAULT_MEMTABLE_SWEEP_SIZE_BYTES: [usize; 5] = [
    128 * 1024,
    512 * 1024,
    2 * 1024 * 1024,
    8 * 1024 * 1024,
    32 * 1024 * 1024,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemtableSweepSize {
    pub label: String,
    pub bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeCounterSnapshot {
    pub write_stalls_total: u64,
    pub write_stalls_memory_total: u64,
    pub compactions_run: u64,
    pub compaction_bytes_rewritten: u64,
    pub compaction_failures: u64,
    pub wal_append_count: u64,
    pub wal_flush_count: u64,
    pub wal_fsync_count: u64,
}

impl RuntimeCounterSnapshot {
    #[must_use]
    pub fn from_runtime_metrics(metrics: &RuntimeMetricsSnapshot) -> Self {
        Self {
            write_stalls_total: metrics.write_stalls_total,
            write_stalls_memory_total: metrics.write_stalls_memory_total,
            compactions_run: metrics.compactions_run,
            compaction_bytes_rewritten: metrics.compaction_bytes_rewritten,
            compaction_failures: metrics.compaction_failures,
            wal_append_count: metrics.wal_append_count,
            wal_flush_count: metrics.wal_flush_count,
            wal_fsync_count: metrics.wal_fsync_count,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeCounterDeltas {
    pub write_stalls_total: u64,
    pub write_stalls_memory_total: u64,
    pub compactions_run: u64,
    pub compaction_bytes_rewritten: u64,
    pub compaction_failures: u64,
    pub wal_append_count: u64,
    pub wal_flush_count: u64,
    pub wal_fsync_count: u64,
}

impl RuntimeCounterDeltas {
    #[must_use]
    pub fn between(start: RuntimeCounterSnapshot, end: RuntimeCounterSnapshot) -> Self {
        Self {
            write_stalls_total: end
                .write_stalls_total
                .saturating_sub(start.write_stalls_total),
            write_stalls_memory_total: end
                .write_stalls_memory_total
                .saturating_sub(start.write_stalls_memory_total),
            compactions_run: end.compactions_run.saturating_sub(start.compactions_run),
            compaction_bytes_rewritten: end
                .compaction_bytes_rewritten
                .saturating_sub(start.compaction_bytes_rewritten),
            compaction_failures: end
                .compaction_failures
                .saturating_sub(start.compaction_failures),
            wal_append_count: end.wal_append_count.saturating_sub(start.wal_append_count),
            wal_flush_count: end.wal_flush_count.saturating_sub(start.wal_flush_count),
            wal_fsync_count: end.wal_fsync_count.saturating_sub(start.wal_fsync_count),
        }
    }
}

impl MemtableSweepSize {
    #[must_use]
    pub fn default_derived() -> Self {
        Self {
            label: "default".to_string(),
            bytes: None,
        }
    }

    #[must_use]
    pub fn explicit(bytes: usize) -> Self {
        Self {
            label: format_memtable_size_label(bytes),
            bytes: Some(bytes),
        }
    }
}

/// Global counter for unique benchmark directory names.
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate unique path for benchmark database.
/// Uses PID + atomic counter to ensure isolation across iterations and parallel runs.
pub fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let base_dir = std::env::var("MIDGE_BENCH_DIR")
        .ok()
        .filter(|val| !val.trim().is_empty())
        .map_or_else(std::env::temp_dir, PathBuf::from);
    base_dir.join(format!("midge_bench_{prefix}_{pid}_{counter}"))
}

#[must_use]
pub fn default_memtable_sweep_sizes() -> Vec<MemtableSweepSize> {
    DEFAULT_MEMTABLE_SWEEP_SIZE_BYTES
        .into_iter()
        .map(MemtableSweepSize::explicit)
        .chain(std::iter::once(MemtableSweepSize::default_derived()))
        .collect()
}

/// Parse a comma-separated memtable size list.
///
/// Empty input returns the default sweep sizes. Entries may be byte counts or
/// the literal `default`.
///
/// # Errors
///
/// Returns an error when the list contains an empty entry, a non-numeric byte
/// count other than `default`, or a zero byte count.
pub fn parse_memtable_sweep_sizes(input: Option<&str>) -> Result<Vec<MemtableSweepSize>, String> {
    let Some(input) = input.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default_memtable_sweep_sizes());
    };

    let mut sizes = Vec::new();
    for raw_part in input.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            return Err("memtable sweep sizes must not contain empty entries".to_string());
        }

        if part.eq_ignore_ascii_case("default") {
            sizes.push(MemtableSweepSize::default_derived());
            continue;
        }

        let bytes = part
            .parse::<usize>()
            .map_err(|_| format!("invalid memtable sweep size `{part}`; use bytes or `default`"))?;
        if bytes == 0 {
            return Err("memtable sweep size must be greater than zero".to_string());
        }
        sizes.push(MemtableSweepSize::explicit(bytes));
    }

    Ok(sizes)
}

#[must_use]
pub fn format_memtable_size_label(bytes: usize) -> String {
    if bytes.is_multiple_of(1024 * 1024) {
        format!("{}MiB", bytes / (1024 * 1024))
    } else if bytes.is_multiple_of(1024) {
        format!("{}KiB", bytes / 1024)
    } else {
        format!("{bytes}B")
    }
}

/// Enable or disable runtime compaction for a benchmark engine.
///
/// # Errors
///
/// Returns an error if the engine cannot apply the runtime setting.
pub fn set_runtime_compaction_enabled(engine: &Engine, enabled: bool) -> crate::MidgeResult<()> {
    engine.set_runtime_compaction_enabled(enabled)
}

/// Request one runtime compaction pass for a benchmark engine.
///
/// # Errors
///
/// Returns an error if the engine cannot enqueue or run the compaction request.
pub fn kick_runtime_compaction_once(engine: &Engine) -> crate::MidgeResult<()> {
    engine.kick_runtime_compaction_once()
}

/// Initialize telemetry settings used by benchmark binaries.
///
/// Repeated initialization is treated as success when telemetry is already
/// globally available.
///
/// # Errors
///
/// Returns an error if telemetry initialization fails for a reason other than
/// an already initialized global telemetry instance.
pub fn init_benchmark_telemetry() -> crate::MidgeResult<()> {
    let mut config = crate::telemetry::TelemetryConfig::new()
        .with_enabled(true)
        .with_service_name("midge-bench".to_string());
    config.features.enable_logging = false;
    config.features.enable_tracing = false;
    config.features.enable_metrics = true;

    match crate::telemetry::Telemetry::init(config) {
        Ok(()) => Ok(()),
        Err(crate::common::MidgeError::Internal(message))
            if message == "Telemetry already initialized"
                && crate::telemetry::Telemetry::global().is_some() =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Storage mode variant for benchmark parameterization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchStorageMode {
    /// In-memory only (no persistence)
    Memory,
    /// Local disk storage
    LocalDisk,
    /// Cloud-backed with mock backend (configurable latency)
    CloudBacked,
}

impl std::fmt::Display for BenchStorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl BenchStorageMode {
    /// Return string representation for benchmark IDs.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            BenchStorageMode::Memory => "memory",
            BenchStorageMode::LocalDisk => "disk",
            BenchStorageMode::CloudBacked => "cloud",
        }
    }
}

/// All storage modes for comprehensive benchmarking.
pub const ALL_STORAGE_MODES: [BenchStorageMode; 2] =
    [BenchStorageMode::Memory, BenchStorageMode::LocalDisk];

/// Fast storage modes (excludes cloud for quick iteration).
pub const FAST_STORAGE_MODES: [BenchStorageMode; 2] =
    [BenchStorageMode::Memory, BenchStorageMode::LocalDisk];

/// Durable storage modes (excludes memory).
pub const DURABLE_STORAGE_MODES: [BenchStorageMode; 1] = [BenchStorageMode::LocalDisk];

/// Configuration for benchmark engine setup.
#[derive(Clone, Debug)]
pub struct BenchEngineConfig {
    pub storage_mode: BenchStorageMode,
    /// High-level tuning knobs (use the public `OpenOptions` builder).
    pub goal: Goal,
    pub workload: WorkloadProfile,
    pub memory_budget: MemoryBudget,
    /// Optional WAL batch config to control group commit behavior for benches.
    pub wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    pub enable_compaction: bool,
    /// Optional override for memtable size (bytes). If None, derived from `OpenOptions`.
    pub memtable_size: Option<usize>,
}

impl Default for BenchEngineConfig {
    fn default() -> Self {
        Self {
            storage_mode: BenchStorageMode::LocalDisk,
            goal: Goal::default(),
            workload: WorkloadProfile::default(),
            memory_budget: MemoryBudget::default(),
            wal_batch_config: None,
            enable_compaction: false,
            memtable_size: None,
        }
    }
}

impl BenchEngineConfig {
    #[must_use]
    pub fn memory() -> Self {
        Self {
            storage_mode: BenchStorageMode::Memory,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn local_disk() -> Self {
        Self {
            storage_mode: BenchStorageMode::LocalDisk,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    #[must_use]
    pub fn with_workload(mut self, workload: WorkloadProfile) -> Self {
        self.workload = workload;
        self
    }

    #[must_use]
    pub fn with_memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = budget;
        self
    }

    #[must_use]
    pub fn with_wal_batch_config(mut self, cfg: crate::wal::policy::BatchConfig) -> Self {
        self.wal_batch_config = Some(cfg);
        self
    }

    #[must_use]
    pub fn with_compaction(mut self, enabled: bool) -> Self {
        self.enable_compaction = enabled;
        self
    }

    #[must_use]
    pub fn with_memtable_size(mut self, size: usize) -> Self {
        self.memtable_size = Some(size);
        self
    }

    /// Build a `MidgeOptions` object from this bench config.
    ///
    /// `db_path` is required for `LocalDisk` storage mode and should be the
    /// filesystem path the engine will use. Pass `None` for `Memory` mode.
    ///
    /// # Panics
    ///
    /// Panics when `LocalDisk` is selected without a `db_path`, or when the
    /// currently unsupported `CloudBacked` mode is selected.
    #[must_use]
    pub fn build_midge_options(&self, db_path: Option<PathBuf>) -> MidgeOptions {
        let storage_mode = match self.storage_mode {
            BenchStorageMode::Memory => StorageMode::Memory,
            BenchStorageMode::LocalDisk => {
                let p = db_path.expect("LocalDisk bench requires a db_path");
                StorageMode::LocalDisk { db_path: p }
            }
            BenchStorageMode::CloudBacked => {
                panic!("CloudBacked mode not yet supported in benchmarks")
            }
        };

        MidgeOptions {
            storage_mode,
            memtable_size: self.memtable_size.unwrap_or(64 * 1024 * 1024),
            enable_compaction: self.enable_compaction,
            // WAL sync is determined at commit time via WriteOptions, not OpenOptions
            wal_sync: false,
            wal_batch_config: self.wal_batch_config,
            ..Default::default()
        }
    }
}

/// Setup a benchmark engine with the given configuration.
///
/// # Panics
///
/// Panics if the benchmark engine cannot be opened with the derived options.
#[must_use]
pub fn setup_engine(prefix: &str, config: &BenchEngineConfig) -> Engine {
    let path = unique_bench_path(prefix);

    // Only clean up filesystem paths (memory mode doesn't need cleanup).
    if config.storage_mode != BenchStorageMode::Memory {
        let _ = std::fs::remove_dir_all(&path);
    }

    let opts = config.build_midge_options(Some(path));
    Engine::open_with_options(&opts).expect("failed to open engine")
}

/// Setup engine with storage mode (convenience wrapper with defaults).
#[must_use]
pub fn setup_engine_with_mode(prefix: &str, mode: BenchStorageMode) -> Engine {
    let config = BenchEngineConfig {
        storage_mode: mode,
        ..Default::default()
    };
    setup_engine(prefix, &config)
}

/// Setup a benchmark engine at a specific path with the given configuration.
/// This creates a NEW database at the path (deletes any existing data).
#[must_use]
pub fn setup_engine_at_path(path: &Path, config: &BenchEngineConfig) -> Engine {
    let _ = std::fs::remove_dir_all(path);
    reopen_engine_at_path(path, config)
}

/// Reopen an existing database at a specific path.
/// Does NOT delete existing data - use for recovery/reopen tests.
///
/// # Panics
///
/// Panics if `config` uses memory storage, or if the engine cannot be opened at
/// the requested path.
#[must_use]
pub fn reopen_engine_at_path(path: &Path, config: &BenchEngineConfig) -> Engine {
    if let BenchStorageMode::Memory = config.storage_mode {
        panic!("setup_engine_at_path requires persistent storage");
    }

    let opts = config.build_midge_options(Some(path.to_path_buf()));
    Engine::open_with_options(&opts).expect("failed to open engine")
}

/// Setup Arc-wrapped engine for concurrent benchmarks.
#[must_use]
pub fn setup_engine_arc(prefix: &str, mode: BenchStorageMode) -> Arc<Engine> {
    Arc::new(setup_engine_with_mode(prefix, mode))
}

// ============================================================================
// Seed/restore single-shot helpers (not Criterion-specific)
// ============================================================================

/// Recursively copy a directory tree from `src` to `dst`. Creates `dst` if needed.
fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    let start = std::time::Instant::now();
    let mut bytes_copied: u64 = 0;
    let mut files_copied: u64 = 0;

    if !dst.exists() {
        std::fs::create_dir_all(dst)?;
    }
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else {
            let bytes = std::fs::copy(&src_path, &dst_path)?;
            bytes_copied = bytes_copied.saturating_add(bytes);
            files_copied += 1;
        }
    }

    let elapsed = start.elapsed();
    tracing::info!(
        src = ?src,
        dst = ?dst,
        files = files_copied,
        bytes = bytes_copied,
        elapsed_ms = elapsed.as_secs_f64() * 1000.0,
        "seed clone completed"
    );

    Ok(())
}

/// Create an on-disk "seed" directory by invoking a builder closure once.
/// The builder receives the path where it should materialize the database.
///
/// # Panics
///
/// Panics if the provided builder panics.
pub fn create_seed_dir<F>(seed_prefix: &str, builder: F) -> PathBuf
where
    F: FnOnce(&Path),
{
    let seed_path = unique_bench_path(seed_prefix);
    // Ensure a clean slate.
    let _ = std::fs::remove_dir_all(&seed_path);
    builder(&seed_path);
    seed_path
}

/// Run a single-shot measurement from a previously created seed directory.
///
/// This clones the seed directory to a unique temp path, reopens the engine at
/// that path using `config`, invokes `measure_fn` exactly once with ownership of
/// the opened engine, measures elapsed time, then cleans up.
///
/// # Panics
///
/// Panics if the seed directory cannot be cloned, the engine cannot be opened,
/// or `measure_fn` panics.
pub fn run_single_shot_from_seed<F>(
    seed_path: &Path,
    config: &BenchEngineConfig,
    measure_fn: F,
) -> std::time::Duration
where
    F: FnOnce(Engine),
{
    let tmp_path = unique_bench_path("tier3_case");
    let _ = std::fs::remove_dir_all(&tmp_path);
    copy_dir_all(seed_path, &tmp_path).expect("failed to clone seed dir");

    let engine = reopen_engine_at_path(&tmp_path, config);

    let start = std::time::Instant::now();
    measure_fn(engine);
    let mut elapsed = start.elapsed();

    // Many perf harnesses/assertions assume non-zero sample durations.
    if elapsed.as_nanos() == 0 {
        elapsed = std::time::Duration::from_nanos(1);
    }

    let _ = std::fs::remove_dir_all(&tmp_path);
    elapsed
}

/// Run a single-shot measurement from a seed but allow a pre-timed "restore" step.
///
/// Useful when per-sample restore is expensive but must not be included in the
/// timed critical section (e.g., creating multiple L0 files before `compact_all`).
///
/// # Panics
///
/// Panics if the seed directory cannot be cloned, the engine cannot be opened,
/// or either closure panics.
pub fn run_single_shot_with_restore<R, T>(
    seed_path: &Path,
    config: &BenchEngineConfig,
    restore_fn: R,
    timed_fn: T,
) -> std::time::Duration
where
    R: FnOnce(&Engine),
    T: FnOnce(&Engine),
{
    let tmp_path = unique_bench_path("tier3_case_restore");
    let _ = std::fs::remove_dir_all(&tmp_path);
    copy_dir_all(seed_path, &tmp_path).expect("failed to clone seed dir");

    let engine = reopen_engine_at_path(&tmp_path, config);

    restore_fn(&engine);

    let start = std::time::Instant::now();
    timed_fn(&engine);
    let mut elapsed = start.elapsed();

    if elapsed.as_nanos() == 0 {
        elapsed = std::time::Duration::from_nanos(1);
    }

    let _ = std::fs::remove_dir_all(&tmp_path);
    elapsed
}

/// Run a single-shot measurement where the *timed* work includes opening the engine.
///
/// The seed directory is cloned to a unique temp directory outside the timed window.
/// The provided closure is executed inside the timed window and receives the temp
/// directory path and the config.
///
/// Return a value from the closure if you want its `Drop` to be excluded from timing.
///
/// # Panics
///
/// Panics if the seed directory cannot be cloned or if the provided closure
/// panics.
pub fn run_single_shot_open_from_seed<F, R>(
    seed_path: &Path,
    config: &BenchEngineConfig,
    f: F,
) -> std::time::Duration
where
    F: FnOnce(&Path, &BenchEngineConfig) -> R,
{
    let tmp_path = unique_bench_path("tier3_open_case");
    let _ = std::fs::remove_dir_all(&tmp_path);
    copy_dir_all(seed_path, &tmp_path).expect("failed to clone seed dir");

    let start = std::time::Instant::now();
    let result = f(&tmp_path, config);
    let mut elapsed = start.elapsed();

    if elapsed.as_nanos() == 0 {
        elapsed = std::time::Duration::from_nanos(1);
    }

    drop(result);
    let _ = std::fs::remove_dir_all(&tmp_path);
    elapsed
}

/// Consume an entire iterator and return the count of elements.
///
/// This helper ensures proper iteration consumption in benchmarks and
/// uses `black_box` to prevent the compiler from optimizing away the loop.
///
/// # Example
/// ```no_run
/// use cntryl_midge::testkit::bench::consume_iterator;
/// # use cntryl_midge::{Engine, Query};
/// # let engine = Engine::open_with_options(&cntryl_midge::testkit::memory_opts()).unwrap();
/// # let cf = engine.create_column_family("cf1").unwrap();
/// # let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
/// let iter = tx.scan(&Query::new()).unwrap();
/// let count = consume_iterator(iter);
/// println!("Scanned {} items", count);
/// ```
#[must_use]
pub fn consume_iterator(mut iter: crate::engine::api::iterator::Iterator) -> usize {
    let mut count = 0;
    while iter.next().is_some() {
        count += 1;
    }
    std::hint::black_box(count);
    count
}

/// Consume up to N elements from an iterator and return the actual count consumed.
///
/// This is useful for benchmarks that want to measure partial scan performance
/// without consuming the entire result set.
///
/// # Example
/// ```no_run
/// use cntryl_midge::testkit::bench::consume_n_from_iterator;
/// # use cntryl_midge::{Engine, Query};
/// # let engine = Engine::open_with_options(&cntryl_midge::testkit::memory_opts()).unwrap();
/// # let cf = engine.create_column_family("cf1").unwrap();
/// # let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
/// let iter = tx.scan(&Query::new()).unwrap();
/// let count = consume_n_from_iterator(iter, 100);
/// println!("Scanned {} items (max 100)", count);
/// ```
#[must_use]
pub fn consume_n_from_iterator(
    mut iter: crate::engine::api::iterator::Iterator,
    n: usize,
) -> usize {
    let mut count = 0;
    while count < n && iter.next().is_some() {
        count += 1;
    }
    std::hint::black_box(count);
    count
}

#[cfg(test)]
mod tests {
    use super::{
        create_seed_dir, default_memtable_sweep_sizes, parse_memtable_sweep_sizes,
        run_single_shot_from_seed, run_single_shot_with_restore, setup_engine_at_path,
        unique_bench_path, BenchEngineConfig, MemtableSweepSize, RuntimeCounterDeltas,
        RuntimeCounterSnapshot,
    };

    #[test]
    fn should_return_unique_paths_when_called_twice() {
        let p1 = unique_bench_path("test");
        let p2 = unique_bench_path("test");
        assert_ne!(p1, p2);
    }

    #[test]
    fn should_run_single_shot_helpers_when_seed_dir_exists() {
        // Arrange
        let seed = create_seed_dir("test_single_shot_seed", |p| {
            let cfg = BenchEngineConfig::default();
            let _ = setup_engine_at_path(p, &cfg);
        });

        // Act
        let duration =
            run_single_shot_from_seed(&seed, &BenchEngineConfig::default(), |_engine| {});

        // Assert
        // Just ensure no panic and a sane duration.
        assert!(duration.as_nanos() > 0);
    }

    #[test]
    fn should_run_restore_before_timed_phase_when_using_restore_helper() {
        use std::sync::{Arc, Mutex};

        // Arrange
        let seed = create_seed_dir("test_restore_seed", |p| {
            let cfg = BenchEngineConfig::default();
            let _ = setup_engine_at_path(p, &cfg);
        });

        let seq = Arc::new(Mutex::new(Vec::new()));
        let seq_restore = seq.clone();
        let seq_timed = seq.clone();

        // Act
        let duration = run_single_shot_with_restore(
            &seed,
            &BenchEngineConfig::default(),
            move |_engine| {
                seq_restore.lock().unwrap().push("restore");
            },
            move |_engine| {
                seq_timed.lock().unwrap().push("timed");
            },
        );

        // Assert
        let captured = seq.lock().unwrap().clone();
        assert_eq!(captured, vec!["restore", "timed"]);
        assert!(duration.as_nanos() > 0);
    }

    #[test]
    fn should_use_default_memtable_sweep_sizes_when_env_is_empty() {
        let parsed = parse_memtable_sweep_sizes(Some("  ")).expect("parse default sizes");

        assert_eq!(parsed, default_memtable_sweep_sizes());
    }

    #[test]
    fn should_parse_comma_separated_memtable_sweep_byte_values() {
        // Arrange
        let parsed =
            parse_memtable_sweep_sizes(Some("131072, 524288, default")).expect("parse sizes");

        // Act
        // Assert
        assert_eq!(
            parsed,
            vec![
                MemtableSweepSize::explicit(128 * 1024),
                MemtableSweepSize::explicit(512 * 1024),
                MemtableSweepSize::default_derived()
            ]
        );
    }

    #[test]
    fn should_reject_invalid_memtable_sweep_size() {
        let result = parse_memtable_sweep_sizes(Some("131072, nope"));

        assert!(result.is_err());
    }

    #[test]
    fn should_reject_zero_memtable_sweep_size() {
        let result = parse_memtable_sweep_sizes(Some("0"));

        assert!(result.is_err());
    }

    #[test]
    fn should_calculate_runtime_counter_deltas() {
        // Arrange
        let start = RuntimeCounterSnapshot {
            write_stalls_total: 10,
            write_stalls_memory_total: 4,
            compactions_run: 3,
            compaction_bytes_rewritten: 1_000,
            compaction_failures: 1,
            wal_append_count: 20,
            wal_flush_count: 5,
            wal_fsync_count: 2,
        };
        let end = RuntimeCounterSnapshot {
            write_stalls_total: 12,
            write_stalls_memory_total: 7,
            compactions_run: 8,
            compaction_bytes_rewritten: 2_500,
            compaction_failures: 1,
            wal_append_count: 25,
            wal_flush_count: 8,
            wal_fsync_count: 3,
        };

        // Act
        // Assert
        assert_eq!(
            RuntimeCounterDeltas::between(start, end),
            RuntimeCounterDeltas {
                write_stalls_total: 2,
                write_stalls_memory_total: 3,
                compactions_run: 5,
                compaction_bytes_rewritten: 1_500,
                compaction_failures: 0,
                wal_append_count: 5,
                wal_flush_count: 3,
                wal_fsync_count: 1,
            }
        );
    }

    #[test]
    fn should_saturate_runtime_counter_deltas_when_snapshot_regresses() {
        // Arrange
        let start = RuntimeCounterSnapshot {
            write_stalls_total: 10,
            write_stalls_memory_total: 10,
            compactions_run: 10,
            compaction_bytes_rewritten: 10,
            compaction_failures: 10,
            wal_append_count: 10,
            wal_flush_count: 10,
            wal_fsync_count: 10,
        };
        let end = RuntimeCounterSnapshot::default();

        // Act
        // Assert
        assert_eq!(
            RuntimeCounterDeltas::between(start, end),
            RuntimeCounterDeltas::default()
        );
    }
}
