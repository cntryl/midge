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
pub const ALL_STORAGE_MODES: [BenchStorageMode; 2] = [
    BenchStorageMode::Memory,
    BenchStorageMode::LocalDisk,
];

/// Fast storage modes (excludes cloud for quick iteration).
#[allow(dead_code)]
pub const FAST_STORAGE_MODES: [BenchStorageMode; 2] =
    [BenchStorageMode::Memory, BenchStorageMode::LocalDisk];

/// Durable storage modes (excludes memory).
#[allow(dead_code)]
pub const DURABLE_STORAGE_MODES: [BenchStorageMode; 1] =
    [BenchStorageMode::LocalDisk];

// ============================================================================
// Engine Setup Functions
// ============================================================================

/// Configuration for benchmark engine setup.
#[derive(Clone)]
#[allow(dead_code)]
pub struct BenchEngineConfig {
    pub storage_mode: BenchStorageMode,
    pub wal_sync: bool,
    pub enable_compaction: bool,
    pub memtable_size: usize,
}

impl Default for BenchEngineConfig {
    fn default() -> Self {
        Self {
            storage_mode: BenchStorageMode::LocalDisk,
            wal_sync: false,
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
    use super::{make_key, unique_bench_path, BenchStorageMode, KEY_SIZE};

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
}
