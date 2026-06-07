//! Test configuration primitives and helpers.
//!
//! This module defines `StorageMode` + `MidgeOptions` (used by `MidgeEngine::open_with_options`)
//! and provides common helpers for parameterizing integration tests across backends.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Storage mode configuration for the engine.
#[derive(Clone)]
pub enum StorageMode {
    /// In-memory storage (no persistence).
    Memory,
    /// Local filesystem storage.
    LocalDisk { db_path: PathBuf },
    /// Cloud-backed storage with local cache.
    CloudBacked { local_cache_path: PathBuf },
}

/// Configuration options for opening a `MidgeEngine` in tests.
#[derive(Clone)]
pub struct MidgeOptions {
    /// Storage mode.
    pub storage_mode: StorageMode,
    /// WAL sync enabled.
    pub wal_sync: bool,

    /// Batch config for WAL group commit (optional).
    pub wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    /// Maximum memtable size before flush.
    pub memtable_size: usize,
    /// Compression enabled.
    pub compression: bool,
    /// Enable automatic background compaction.
    pub enable_compaction: bool,
    /// Memory budget for spilling (in bytes).
    pub memory_budget: Option<usize>,
}

impl Default for MidgeOptions {
    fn default() -> Self {
        Self {
            storage_mode: StorageMode::Memory,
            wal_sync: false,
            wal_batch_config: None,
            memtable_size: 64 * 1024 * 1024, // 64 MB
            compression: false,
            enable_compaction: true,
            memory_budget: None,
        }
    }
}

impl MidgeOptions {
    /// Set memory budget for transaction spilling (in bytes).
    ///
    /// When a transaction exceeds this memory limit, it will spill to disk.
    /// Set to `None` for unlimited memory.
    pub fn memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget = Some(bytes);
        self
    }

    /// Convert MidgeOptions to OpenOptions for use with Engine::open.
    pub fn to_open_options(&self) -> crate::OpenOptions {
        let storage = match &self.storage_mode {
            StorageMode::Memory => crate::Storage::InMemory,
            StorageMode::LocalDisk { db_path } => crate::Storage::Local {
                path: db_path.clone(),
            },
            StorageMode::CloudBacked { local_cache_path } => crate::Storage::CloudSimulated {
                local_cache_path: local_cache_path.clone(),
                bucket: "test-bucket".to_string(),
                prefix: "test-prefix/".to_string(),
            },
        };

        // Build OpenOptions via constructors so defaults are sensible
        let mut open_opts = match storage {
            crate::Storage::InMemory => crate::OpenOptions::in_memory(),
            crate::Storage::Local { path } => crate::OpenOptions::local(path),
            crate::Storage::CloudSimulated {
                local_cache_path,
                bucket,
                prefix,
            } => crate::OpenOptions::cloud_simulated(local_cache_path, bucket, prefix),
            crate::Storage::Cloud { .. } => unreachable!("testkit uses simulated cloud storage"),
        };

        // Apply user-specified high-level knobs
        open_opts = open_opts
            .memory_budget(match self.memory_budget {
                Some(n) => crate::MemoryBudget::Bytes(n),
                None => crate::MemoryBudget::Auto,
            })
            .workload(crate::WorkloadProfile::default())
            .goal(crate::Goal::default())
            .build();

        // Pass through WAL batch configuration if specified by testkit
        open_opts.wal_batch_config = self.wal_batch_config;

        open_opts
    }
}

// ===== Mode lists =====

/// All available storage modes for integration tests (uppercase: backward-compatible).
pub fn all_storage_modes() -> Vec<&'static str> {
    vec!["Memory", "LocalDisk"]
}

/// All supported storage modes for parametrized tests (lowercase: new convention).
/// Includes: memory, local (disk), cloud (backed).
pub fn all_storage_modes_new() -> Vec<&'static str> {
    vec!["memory", "local", "cloud"]
}

/// Durable storage modes only: local disk and cloud.
/// Use this for tests that require persistence (SST, WAL, recovery, durability).
pub fn durable_storage_modes() -> &'static [&'static str] {
    &["local", "cloud"]
}

/// Memory-only storage mode.
/// Use this for tests that explicitly need non-persistent storage.
pub fn memory_storage_modes() -> Vec<&'static str> {
    vec!["memory"]
}

/// Filesystem-only storage mode.
/// Use this for tests that require filesystem-specific behavior.
pub fn filesystem_storage_modes() -> Vec<&'static str> {
    vec!["local"]
}

/// Disk storage modes for testing (uppercase: backward-compatible).
pub fn disk_storage_modes() -> Vec<&'static str> {
    vec!["LocalDisk"]
}

// ===== Option constructors =====

/// Create memory-only options for testing.
pub fn memory_opts() -> MidgeOptions {
    opts_for_mode("memory")
}

/// Generate appropriate `MidgeOptions` for the given storage mode (lowercase convention).
///
/// # Arguments
/// * `mode` - Storage mode name: "memory", "local", or "cloud"
///
/// # Panics
/// Panics if mode is not recognized.
pub fn opts_for_mode(mode: &str) -> MidgeOptions {
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    let unique_id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);

    match mode {
        "memory" => MidgeOptions {
            storage_mode: StorageMode::Memory,
            wal_sync: false,
            wal_batch_config: None,
            // Keep test default small to reduce runtime.
            memtable_size: 64 * 1024,
            compression: false,
            enable_compaction: false,
            memory_budget: None,
        },
        "local" => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let test_dir = PathBuf::from(format!(
                "target/tmp/midge_test_local_{}_{}_{}",
                std::process::id(),
                unique_id,
                timestamp
            ));
            let _ = std::fs::create_dir_all(&test_dir);
            MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: test_dir },
                wal_sync: false,
                wal_batch_config: None,
                memtable_size: 64 * 1024,
                compression: false,
                enable_compaction: false,
                memory_budget: None,
            }
        }
        "cloud" => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let test_dir = PathBuf::from(format!(
                "target/tmp/midge_test_cloud_{}_{}_{}",
                std::process::id(),
                unique_id,
                timestamp
            ));
            let _ = std::fs::create_dir_all(&test_dir);
            MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: test_dir,
                },
                wal_sync: true,
                wal_batch_config: None,
                // Benchmark-safe: larger memtable delays flushes, allowing CloudAsync
                // to batch WAL segments efficiently. CloudAsync uses background uploads
                // with thresholds (16MB bytes / 10k writes / 500ms delay) to avoid
                // per-commit upload overhead. Single commits never trigger uploads.
                memtable_size: 2 * 1024 * 1024, // 2MB (was 64KB)
                compression: false,
                enable_compaction: false,
                memory_budget: None,
            }
        }
        _ => panic!("unknown storage mode: {}", mode),
    }
}

/// Run a test across selected storage modes, applying a test function to each.
///
/// # Arguments
/// * `modes` - Slice of mode names ("memory", "local", "cloud")
/// * `test_fn` - Closure that receives (mode_name, opts) for each mode
pub fn for_each_storage_mode<F>(modes: &[&str], test_fn: F)
where
    F: Fn(&str, MidgeOptions),
{
    for mode in modes {
        if let Some(only) = std::env::var_os("MIDGE_TEST_ONLY_MODE") {
            if only.as_os_str() != std::ffi::OsStr::new(mode) {
                continue;
            }
        }

        if std::env::var_os("MIDGE_TEST_TRACE_MODES").is_some() {
            eprintln!("[midge-test] mode={mode}");
        }
        test_fn(mode, opts_for_mode(mode));
    }
}

/// Create a temporary directory for tests.
pub fn test_temp_dir() -> tempfile::TempDir {
    tempfile::TempDir::new().expect("Failed to create temp dir")
}

/// Options for compaction tests.
pub fn compaction_test_opts() -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: test_temp_dir().path().to_path_buf(),
        },
        wal_sync: true,
        wal_batch_config: None,
        memtable_size: 1024 * 1024, // 1 MB for faster flushing in tests
        compression: false,
        enable_compaction: true,
        memory_budget: None,
    }
}

/// Options for manual compaction tests.
pub fn manual_compaction_test_opts() -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: test_temp_dir().path().to_path_buf(),
        },
        wal_sync: true,
        wal_batch_config: None,
        memtable_size: 512 * 1024, // 512 KB for even faster flushing
        compression: false,
        enable_compaction: false,
        memory_budget: None,
    }
}

/// Options for durability tests.
pub fn durability_opts() -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: test_temp_dir().path().to_path_buf(),
        },
        wal_sync: true,
        wal_batch_config: None,
        memtable_size: 64 * 1024,
        compression: false,
        enable_compaction: false,
        memory_budget: None,
    }
}
