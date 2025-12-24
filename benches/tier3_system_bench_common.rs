//! Common utilities for benchmarks across all tiers.
//!
//! This module provides:
//! - Storage mode configurations for testing Memory, LocalDisk, and CloudBacked
//! - Unique path generation for isolated benchmark directories
//! - Precomputed key/value generation (no allocations in hot loops)
//! - Standard constants for consistent benchmarking
//!
//! ## Usage
//!
//! Include in benchmark files via:
//! ```ignore
//! #[path = "../bench_common.rs"]
//! mod bench_common;
//! use bench_common::*;
//! ```

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// Constants
// ============================================================================

/// Default key size in bytes (14 bytes: "key_" + 10 digits)
#[allow(dead_code)]
pub const KEY_SIZE: usize = 14;

/// Default value size for benchmarks
#[allow(dead_code)]
pub const VALUE_SIZE: usize = 128;

/// Bytes per operation (key + value)
#[allow(dead_code)]
pub const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Default memtable size for benchmarks (4MB)
#[allow(dead_code)]
pub const BENCH_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

/// Whether to use BenchFast mode for cloud backends (no filesystem IO).
/// Set to false to test with real filesystem for debugging.
#[allow(dead_code)]
pub const USE_BENCH_FAST_CLOUD: bool = true;

// ============================================================================
// Unique Path Generation
// ============================================================================

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate unique path for benchmark database.
/// Uses PID + atomic counter to ensure isolation across iterations and parallel runs.
#[allow(dead_code)]
pub fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_{}_{}_{}", prefix, pid, counter))
}

// ============================================================================
// Key/Value Generation
// ============================================================================

/// Generate a fixed-size key without format! allocations.
/// Format: "key_" + 10-digit zero-padded number
#[inline]
#[allow(dead_code)]
pub fn make_key(i: usize) -> Bytes {
    let mut key = vec![0u8; KEY_SIZE];
    key[..4].copy_from_slice(b"key_");
    let mut n = i;
    for j in (4..KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    Bytes::from(key)
}

/// Generate a fixed-size value (filled with 'x' bytes).
#[inline]
#[allow(dead_code)]
pub fn make_value_fixed(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Precompute keys and values for benchmark.
/// Call this outside the timed section to avoid allocations during measurement.
#[allow(dead_code)]
pub fn precompute_kv(n: usize, value_size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value_fixed(value_size));
    }
    (keys, vals)
}

/// Precompute deterministic random indices for reads.
#[allow(dead_code)]
pub fn precompute_read_indices(n: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(count);
    let mut state = seed;
    for _ in 0..count {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        indices.push((state as usize) % n);
    }
    indices
}

// ============================================================================
// Storage Mode Definitions
// ============================================================================

/// Storage mode variant for benchmark parameterization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
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
    /// Return string representation for benchmark IDs
    pub fn as_str(&self) -> &'static str {
        match self {
            BenchStorageMode::Memory => "memory",
            BenchStorageMode::LocalDisk => "disk",
            BenchStorageMode::CloudBacked => "cloud",
        }
    }
}

/// All storage modes for comprehensive benchmarking.
#[allow(dead_code)]
pub const ALL_STORAGE_MODES: [BenchStorageMode; 2] =
    [BenchStorageMode::Memory, BenchStorageMode::LocalDisk];

/// Fast storage modes (excludes cloud for quick iteration).
#[allow(dead_code)]
pub const FAST_STORAGE_MODES: [BenchStorageMode; 2] =
    [BenchStorageMode::Memory, BenchStorageMode::LocalDisk];

/// Durable storage modes (excludes memory).
#[allow(dead_code)]
pub const DURABLE_STORAGE_MODES: [BenchStorageMode; 1] = [BenchStorageMode::LocalDisk];

// ============================================================================
// Engine Setup Functions
// ============================================================================

/// Configuration for benchmark engine setup.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct BenchEngineConfig {
    pub storage_mode: BenchStorageMode,
    pub wal_sync: bool,
    /// Optional WAL batch config to control group commit behavior for benches
    pub wal_batch_config: Option<cntryl_midge::wal::policy::BatchConfig>,
    pub enable_compaction: bool,
    pub memtable_size: usize,
}

impl Default for BenchEngineConfig {
    fn default() -> Self {
        Self {
            storage_mode: BenchStorageMode::LocalDisk,
            wal_sync: false,
            wal_batch_config: None,
            enable_compaction: false,
            memtable_size: BENCH_MEMTABLE_SIZE,
        }
    }
}

#[allow(dead_code)]
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

    pub fn with_wal_sync(mut self, sync: bool) -> Self {
        self.wal_sync = sync;
        self
    }

    pub fn with_wal_batch_config(mut self, cfg: cntryl_midge::wal::policy::BatchConfig) -> Self {
        self.wal_batch_config = Some(cfg);
        self
    }

    pub fn with_compaction(mut self, enabled: bool) -> Self {
        self.enable_compaction = enabled;
        self
    }

    pub fn with_memtable_size(mut self, size: usize) -> Self {
        self.memtable_size = size;
        self
    }
}

/// Setup a benchmark engine with the given configuration.
/// Returns the engine and optionally a path (for LocalDisk) that should be cleaned up.
#[allow(dead_code)]
pub fn setup_engine(prefix: &str, config: &BenchEngineConfig) -> MidgeEngine {
    let path = unique_bench_path(prefix);

    // Only clean up filesystem paths (memory mode doesn't need cleanup)
    if config.storage_mode != BenchStorageMode::Memory {
        let _ = std::fs::remove_dir_all(&path);
    }

    let storage_mode = match config.storage_mode {
        BenchStorageMode::Memory => StorageMode::Memory,
        BenchStorageMode::LocalDisk => StorageMode::LocalDisk { db_path: path },
        BenchStorageMode::CloudBacked => panic!("CloudBacked mode not yet supported in benchmarks"),
    };

    let opts = MidgeOptions {
        storage_mode,
        memtable_size: config.memtable_size,
        enable_compaction: config.enable_compaction,
        wal_sync: config.wal_sync,
        wal_batch_config: config.wal_batch_config,
        ..Default::default()
    };

    MidgeEngine::open(opts).expect("failed to open engine")
}

/// Setup engine with storage mode (convenience wrapper with defaults).
#[allow(dead_code)]
pub fn setup_engine_with_mode(prefix: &str, mode: BenchStorageMode) -> MidgeEngine {
    let config = BenchEngineConfig {
        storage_mode: mode,
        ..Default::default()
    };
    setup_engine(prefix, &config)
}

/// Setup a benchmark engine at a specific path with the given configuration.
/// This creates a NEW database at the path (deletes any existing data).
/// Use `reopen_engine_at_path` for recovery/reopen tests.
#[allow(dead_code)]
pub fn setup_engine_at_path(path: &std::path::Path, config: &BenchEngineConfig) -> MidgeEngine {
    let _ = std::fs::remove_dir_all(path);
    reopen_engine_at_path(path, config)
}

/// Reopen an existing database at a specific path.
/// Does NOT delete existing data - use for recovery/reopen tests.
#[allow(dead_code)]
pub fn reopen_engine_at_path(path: &std::path::Path, config: &BenchEngineConfig) -> MidgeEngine {
    let storage_mode = match config.storage_mode {
        BenchStorageMode::Memory => panic!("setup_engine_at_path requires persistent storage"),
        BenchStorageMode::LocalDisk => StorageMode::LocalDisk {
            db_path: path.to_path_buf(),
        },
        BenchStorageMode::CloudBacked => panic!("CloudBacked mode not yet supported in benchmarks"),
    };

    let opts = MidgeOptions {
        storage_mode,
        memtable_size: config.memtable_size,
        enable_compaction: config.enable_compaction,
        wal_sync: config.wal_sync,
        ..Default::default()
    };

    MidgeEngine::open(opts).expect("failed to open engine")
}

/// Setup engine with storage mode and WAL sync option.
#[allow(dead_code)]
pub fn setup_engine_with_mode_and_sync(
    prefix: &str,
    mode: BenchStorageMode,
    wal_sync: bool,
) -> MidgeEngine {
    let config = BenchEngineConfig {
        storage_mode: mode,
        wal_sync,
        ..Default::default()
    };
    setup_engine(prefix, &config)
}

/// Setup Arc-wrapped engine for concurrent benchmarks.
#[allow(dead_code)]
pub fn setup_engine_arc(prefix: &str, mode: BenchStorageMode) -> Arc<MidgeEngine> {
    Arc::new(setup_engine_with_mode(prefix, mode))
}

// ============================================================================
// Tier-3 harness helpers
// ============================================================================

/// Recursively copy a directory tree from `src` to `dst`. Creates `dst` if needed.
fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
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

/// Tier-3 harness helpers (typed) 🛡️
///
/// These helpers live in a dedicated module to avoid emitting dead-code lints
/// in benches that don't use the typed harness. Include them from benches that
/// need them with:
///
/// ```ignore
/// #[path = "./common/tier3_harness.rs"]
/// mod tier3;
/// ```
/// When included, use `tier3::Tier3Case` or `tier3::Tier3RestoreCase` and the
/// `tier3_bench!` / `tier3_bench_restore!` macros.
/// Create an on-disk "seed" directory by invoking a builder closure once.
/// The builder receives the path where it should materialize the database. Use
/// `setup_engine_at_path` inside the builder to construct the desired SST layout.
#[allow(dead_code)]
pub fn create_seed_dir<F>(seed_prefix: &str, builder: F) -> std::path::PathBuf
where
    F: FnOnce(&std::path::Path),
{
    let seed_path = unique_bench_path(seed_prefix);
    // Ensure a clean slate
    let _ = std::fs::remove_dir_all(&seed_path);
    // Let the builder create the DB at `seed_path` (it may call setup_engine_at_path)
    builder(&seed_path);
    seed_path
}

/// Run a single-shot measurement from a previously created seed directory.
/// This function clones the seed directory to a unique temp path, reopens the
/// engine at that path using `config`, invokes `measure_fn` exactly once with
/// ownership of the opened engine, measures the elapsed time, then cleans up.
#[allow(dead_code)]
pub fn run_single_shot_from_seed<F>(
    seed_path: &std::path::Path,
    config: &BenchEngineConfig,
    measure_fn: F,
) -> std::time::Duration
where
    F: FnOnce(MidgeEngine),
{
    // Clone seed into a unique temp path so each sample gets an isolated engine
    let tmp_path = unique_bench_path("tier3_case");
    let _ = std::fs::remove_dir_all(&tmp_path);
    copy_dir_all(seed_path, &tmp_path).expect("failed to clone seed dir");

    // Reopen engine at tmp_path with requested config
    let engine = reopen_engine_at_path(&tmp_path, config);

    // Timed single-shot invocation
    let start = std::time::Instant::now();
    measure_fn(engine);
    let mut elapsed = start.elapsed();
    // Criterion asserts sample durations > 0; ensure a tiny non-zero minimum to avoid panics
    if elapsed.as_nanos() == 0 {
        elapsed = std::time::Duration::from_nanos(1);
    }

    // Engine dropped here; remove temp dir
    let _ = std::fs::remove_dir_all(&tmp_path);
    elapsed
}

/// Run a single-shot measurement from a seed but allow a pre-timed "restore" step.
/// Useful for cases where the per-sample restore is expensive but must not be included
/// in the timed critical section (e.g., creating multiple L0 files before compact_all).
#[allow(dead_code)]
pub fn run_single_shot_with_restore<R, T>(
    seed_path: &std::path::Path,
    config: &BenchEngineConfig,
    restore_fn: R,
    timed_fn: T,
) -> std::time::Duration
where
    R: FnOnce(&MidgeEngine),
    T: FnOnce(&MidgeEngine),
{
    // Clone seed into a unique temp path so each sample gets an isolated engine
    let tmp_path = unique_bench_path("tier3_case_restore");
    let _ = std::fs::remove_dir_all(&tmp_path);
    copy_dir_all(seed_path, &tmp_path).expect("failed to clone seed dir");

    // Reopen engine at tmp_path with requested config
    let engine = reopen_engine_at_path(&tmp_path, config);

    // Perform restore steps outside timed window
    restore_fn(&engine);

    // Timed single-shot invocation
    let start = std::time::Instant::now();
    timed_fn(&engine);
    let mut elapsed = start.elapsed();
    // Criterion asserts sample durations > 0; ensure a tiny non-zero minimum to avoid panics
    if elapsed.as_nanos() == 0 {
        elapsed = std::time::Duration::from_nanos(1);
    }

    // Engine dropped here; remove temp dir
    let _ = std::fs::remove_dir_all(&tmp_path);
    elapsed
}

// ============================================================================
// Benchmark Group Helpers
// ============================================================================

/// Helper macro for iterating over storage modes in benchmarks.
///
/// Usage:
/// ```ignore
/// for_each_storage_mode!(group, FAST_STORAGE_MODES, |mode| {
///     group.bench_with_input(
///         BenchmarkId::new("operation", mode),
///         &mode,
///         |b, &mode| { ... }
///     );
/// });
/// ```
#[macro_export]
macro_rules! for_each_storage_mode {
    ($modes:expr, $closure:expr) => {
        for mode in $modes {
            $closure(mode);
        }
    };
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{
        create_seed_dir, make_key, run_single_shot_from_seed, run_single_shot_with_restore,
        setup_engine_at_path, unique_bench_path, BenchEngineConfig, BenchStorageMode, KEY_SIZE,
    };

    #[test]
    fn test_unique_paths_are_unique() {
        let p1 = unique_bench_path("test");
        let p2 = unique_bench_path("test");
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_make_key_format() {
        let key = make_key(123);
        assert_eq!(key.len(), KEY_SIZE);
        assert_eq!(&key[..4], b"key_");
    }

    #[test]
    fn test_storage_mode_display() {
        assert_eq!(format!("{}", BenchStorageMode::Memory), "memory");
        assert_eq!(format!("{}", BenchStorageMode::LocalDisk), "disk");
    }

    #[test]
    fn test_tier3_case_run() {
        let seed = create_seed_dir("test_tier3_case", |p| {
            let cfg = BenchEngineConfig::default();
            let _ = setup_engine_at_path(p, &cfg);
        });

        // Should run without panicking.
        let _d = run_single_shot_from_seed(&seed, &BenchEngineConfig::default(), |_engine| {});
    }

    #[test]
    fn test_tier3_restore_case_phases() {
        use std::sync::{Arc, Mutex};
        let seed = create_seed_dir("test_tier3_restore", |p| {
            let cfg = BenchEngineConfig::default();
            let _ = setup_engine_at_path(p, &cfg);
        });

        let seq = Arc::new(Mutex::new(Vec::new()));
        // Clone handles for each closure so `seq` remains available after call.
        let seq_restore = seq.clone();
        let seq_timed = seq.clone();

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

        let captured = seq.lock().unwrap().clone();
        assert_eq!(captured, vec!["restore", "timed"]);
    }
}
