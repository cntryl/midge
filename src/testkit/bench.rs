//! Utilities intended for benchmarks (and other perf-style harnesses).
//!
//! The goal of this module is to keep benchmark targets thin by centralizing:
//! - deterministic temp path generation
//! - storage-mode parameterization
//! - engine setup from high-level knobs (via `OpenOptions`)

use crate::testkit::{MidgeOptions, StorageMode};
use crate::{Goal, MemoryBudget, MidgeEngine, OpenOptions, WorkloadProfile};
use crate::common::AckPolicy;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Global counter for unique benchmark directory names.
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate unique path for benchmark database.
/// Uses PID + atomic counter to ensure isolation across iterations and parallel runs.
pub fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_{}_{}_{}", prefix, pid, counter))
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
    /// High-level tuning knobs (use the public OpenOptions builder).
    pub goal: Goal,
    pub workload: WorkloadProfile,
    pub memory_budget: MemoryBudget,
    /// Optional WAL batch config to control group commit behavior for benches.
    pub wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    pub enable_compaction: bool,
    /// Optional override for memtable size (bytes). If None, derived from OpenOptions.
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
    pub fn memory() -> Self {
        Self {
            storage_mode: BenchStorageMode::Memory,
            ..Default::default()
        }
    }

    pub fn local_disk() -> Self {
        Self {
            storage_mode: BenchStorageMode::LocalDisk,
            ..Default::default()
        }
    }

    pub fn with_goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    pub fn with_workload(mut self, workload: WorkloadProfile) -> Self {
        self.workload = workload;
        self
    }

    pub fn with_memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = budget;
        self
    }

    pub fn with_wal_batch_config(mut self, cfg: crate::wal::policy::BatchConfig) -> Self {
        self.wal_batch_config = Some(cfg);
        self
    }

    pub fn with_compaction(mut self, enabled: bool) -> Self {
        self.enable_compaction = enabled;
        self
    }

    pub fn with_memtable_size(mut self, size: usize) -> Self {
        self.memtable_size = Some(size);
        self
    }

    /// Build a `MidgeOptions` object from this bench config.
    ///
    /// `db_path` is required for `LocalDisk` storage mode and should be the
    /// filesystem path the engine will use. Pass `None` for `Memory` mode.
    pub fn build_midge_options(&self, db_path: Option<PathBuf>) -> MidgeOptions {
        // Use the public OpenOptions builder as the source of truth for derived knobs.
        // This keeps benches aligned with user-facing configuration semantics.
        let open_opts = OpenOptions::new()
            .goal(self.goal)
            .workload(self.workload)
            .memory_budget(self.memory_budget)
            .build();

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
            memtable_size: self
                .memtable_size
                .unwrap_or(open_opts.memtable_size_limit()),
            enable_compaction: self.enable_compaction,
            // WAL sync is determined at commit time via WriteOptions, not OpenOptions
            wal_sync: false,
            ack_policy: AckPolicy::Immediate,
            wal_batch_config: self.wal_batch_config,
            ..Default::default()
        }
    }
}

/// Setup a benchmark engine with the given configuration.
pub fn setup_engine(prefix: &str, config: &BenchEngineConfig) -> MidgeEngine {
    let path = unique_bench_path(prefix);

    // Only clean up filesystem paths (memory mode doesn't need cleanup).
    if config.storage_mode != BenchStorageMode::Memory {
        let _ = std::fs::remove_dir_all(&path);
    }

    let opts = config.build_midge_options(Some(path));
    MidgeEngine::open(opts).expect("failed to open engine")
}

/// Setup engine with storage mode (convenience wrapper with defaults).
pub fn setup_engine_with_mode(prefix: &str, mode: BenchStorageMode) -> MidgeEngine {
    let config = BenchEngineConfig {
        storage_mode: mode,
        ..Default::default()
    };
    setup_engine(prefix, &config)
}

/// Setup a benchmark engine at a specific path with the given configuration.
/// This creates a NEW database at the path (deletes any existing data).
pub fn setup_engine_at_path(path: &Path, config: &BenchEngineConfig) -> MidgeEngine {
    let _ = std::fs::remove_dir_all(path);
    reopen_engine_at_path(path, config)
}

/// Reopen an existing database at a specific path.
/// Does NOT delete existing data - use for recovery/reopen tests.
pub fn reopen_engine_at_path(path: &Path, config: &BenchEngineConfig) -> MidgeEngine {
    if let BenchStorageMode::Memory = config.storage_mode {
        panic!("setup_engine_at_path requires persistent storage");
    }

    let opts = config.build_midge_options(Some(path.to_path_buf()));
    MidgeEngine::open(opts).expect("failed to open engine")
}



/// Setup Arc-wrapped engine for concurrent benchmarks.
pub fn setup_engine_arc(prefix: &str, mode: BenchStorageMode) -> Arc<MidgeEngine> {
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
pub fn run_single_shot_from_seed<F>(
    seed_path: &Path,
    config: &BenchEngineConfig,
    measure_fn: F,
) -> std::time::Duration
where
    F: FnOnce(MidgeEngine),
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
pub fn run_single_shot_with_restore<R, T>(
    seed_path: &Path,
    config: &BenchEngineConfig,
    restore_fn: R,
    timed_fn: T,
) -> std::time::Duration
where
    R: FnOnce(&MidgeEngine),
    T: FnOnce(&MidgeEngine),
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

#[cfg(test)]
mod tests {
    use super::{
        create_seed_dir, run_single_shot_from_seed, run_single_shot_with_restore,
        setup_engine_at_path, unique_bench_path, BenchEngineConfig,
    };

    #[test]
    fn test_unique_paths_are_unique() {
        let p1 = unique_bench_path("test");
        let p2 = unique_bench_path("test");
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_single_shot_helpers_run() {
        // Arrange
        let seed = create_seed_dir("test_single_shot_seed", |p| {
            let cfg = BenchEngineConfig::default();
            let _ = setup_engine_at_path(p, &cfg);
        });

        // Act
        let _d = run_single_shot_from_seed(&seed, &BenchEngineConfig::default(), |_engine| {});

        // Assert
        // Just ensure no panic and a sane duration.
        assert!(_d.as_nanos() > 0);
    }

    #[test]
    fn test_restore_then_timed_order() {
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
        let _d = run_single_shot_with_restore(
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
        assert!(_d.as_nanos() > 0);
    }
}
