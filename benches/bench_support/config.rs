//! Benchmark configuration primitives and helpers.
//!
//! This module defines local benchmark storage profiles without exposing a
//! crate-root testkit API.

#![allow(dead_code)]

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

#[derive(Debug, Clone, Default)]
pub struct SimulatedCloudOverrides {
    pub local_storage_budget_bytes: Option<u64>,
}

#[derive(Clone)]
pub struct MidgeOptions {
    /// Storage mode.
    pub storage_mode: StorageMode,
    /// WAL sync enabled.
    pub wal_sync: bool,

    /// Batch config for WAL group commit (optional).
    pub wal_batch_config: Option<cntryl_midge::wal::policy::BatchConfig>,
    /// Maximum memtable size before flush.
    pub memtable_size: usize,
    /// Compression enabled.
    pub compression: bool,
    /// Enable automatic background compaction.
    pub enable_compaction: bool,
    /// Memory budget for spilling (in bytes).
    pub memory_budget: Option<usize>,
    /// Cloud runtime tuning used by benchmark profiles.
    pub cloud_write_policy: Option<cntryl_midge::CloudWritePolicy>,
    /// Internal simulated-cloud storage tuning used only by tests.
    pub simulated_cloud_overrides: Option<SimulatedCloudOverrides>,
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
            cloud_write_policy: None,
            simulated_cloud_overrides: None,
        }
    }
}

impl MidgeOptions {
    /// Set memory budget for transaction spilling (in bytes).
    ///
    /// When a transaction exceeds this memory limit, it will spill to disk.
    /// Set to `None` for unlimited memory.
    #[must_use]
    pub fn memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget = Some(bytes);
        self
    }

    #[must_use]
    pub fn with_cloud_write_policy(mut self, policy: cntryl_midge::CloudWritePolicy) -> Self {
        self.cloud_write_policy = Some(policy);
        self
    }

    #[must_use]
    pub fn with_simulated_cloud_local_storage_budget(mut self, bytes: u64) -> Self {
        let overrides = self
            .simulated_cloud_overrides
            .get_or_insert_with(SimulatedCloudOverrides::default);
        overrides.local_storage_budget_bytes = Some(bytes);
        self
    }

    /// Convert `MidgeOptions` to `OpenOptions` for use with `Engine::open`.
    #[must_use]
    pub fn to_open_options(&self) -> cntryl_midge::OpenOptions {
        let storage = match &self.storage_mode {
            StorageMode::Memory => cntryl_midge::Storage::InMemory,
            StorageMode::LocalDisk { db_path } => cntryl_midge::Storage::Local {
                path: db_path.clone(),
            },
            StorageMode::CloudBacked { local_cache_path } => {
                cntryl_midge::Storage::CloudSimulated {
                    local_cache_path: local_cache_path.clone(),
                    bucket: "test-bucket".to_string(),
                    prefix: "test-prefix/".to_string(),
                }
            }
        };

        // Build OpenOptions via constructors so defaults are sensible
        let mut open_opts = match storage {
            cntryl_midge::Storage::InMemory => cntryl_midge::OpenOptions::in_memory(),
            cntryl_midge::Storage::Local { path } => cntryl_midge::OpenOptions::local(path),
            cntryl_midge::Storage::CloudSimulated {
                local_cache_path,
                bucket,
                prefix,
            } => cntryl_midge::OpenOptions::cloud_simulated(local_cache_path, bucket, prefix),
            cntryl_midge::Storage::Cloud { .. } => {
                unreachable!("bench support uses simulated cloud storage")
            }
        };

        // Apply user-specified high-level knobs
        open_opts = open_opts
            .memory_budget(match self.memory_budget {
                Some(n) => cntryl_midge::MemoryBudget::Bytes(n),
                None => cntryl_midge::MemoryBudget::Auto,
            })
            .workload(cntryl_midge::WorkloadProfile::default())
            .goal(cntryl_midge::Goal::default())
            .background_compaction(self.enable_compaction)
            .with_memtable_size_limit(self.memtable_size)
            .with_memtable_flush_threshold(self.memtable_size)
            .build();

        if let Some(policy) = self.cloud_write_policy.clone() {
            open_opts = open_opts.cloud_write_policy(policy);
        }

        open_opts
    }
}

// ===== Mode lists =====

/// All available storage modes for integration tests (uppercase: backward-compatible).
#[must_use]
pub fn all_storage_modes() -> Vec<&'static str> {
    vec!["Memory", "LocalDisk"]
}

/// All supported storage modes for parametrized tests (lowercase: new convention).
/// Includes: memory, local (disk), cloud (backed).
#[must_use]
pub fn all_storage_modes_new() -> Vec<&'static str> {
    vec!["memory", "local", "cloud"]
}

/// Durable storage modes only: local disk and cloud.
/// Use this for tests that require persistence (SST, WAL, recovery, durability).
#[must_use]
pub fn durable_storage_modes() -> &'static [&'static str] {
    &["local", "cloud"]
}

/// Memory-only storage mode.
/// Use this for tests that explicitly need non-persistent storage.
#[must_use]
pub fn memory_storage_modes() -> Vec<&'static str> {
    vec!["memory"]
}

/// Filesystem-only storage mode.
/// Use this for tests that require filesystem-specific behavior.
#[must_use]
pub fn filesystem_storage_modes() -> Vec<&'static str> {
    vec!["local"]
}

/// Disk storage modes for testing (uppercase: backward-compatible).
#[must_use]
pub fn disk_storage_modes() -> Vec<&'static str> {
    vec!["LocalDisk"]
}

// ===== Option constructors =====

/// Create memory-only options for testing.
#[must_use]
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
            cloud_write_policy: None,
            simulated_cloud_overrides: None,
        },
        "local" => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
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
                cloud_write_policy: None,
                simulated_cloud_overrides: None,
            }
        }
        "cloud" => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
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
                cloud_write_policy: None,
                simulated_cloud_overrides: None,
            }
        }
        "hybrid" => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let test_dir = PathBuf::from(format!(
                "target/tmp/midge_test_hybrid_{}_{}_{}",
                std::process::id(),
                unique_id,
                timestamp
            ));
            let _ = std::fs::create_dir_all(&test_dir);
            let local_storage_budget_bytes = std::env::var("MIDGE_BENCH_HYBRID_LOCAL_BUDGET_BYTES")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(8 * 1024 * 1024);
            MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: test_dir,
                },
                wal_sync: true,
                wal_batch_config: None,
                memtable_size: 2 * 1024 * 1024,
                compression: false,
                enable_compaction: false,
                memory_budget: None,
                cloud_write_policy: None,
                simulated_cloud_overrides: Some(SimulatedCloudOverrides {
                    local_storage_budget_bytes: Some(local_storage_budget_bytes),
                }),
            }
        }
        _ => panic!("unknown storage mode: {mode}"),
    }
}

/// Run a test across selected storage modes, applying a test function to each.
///
/// # Arguments
/// * `modes` - Slice of mode names ("memory", "local", "cloud")
/// * `test_fn` - Closure that receives (`mode_name`, opts) for each mode
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
#[must_use]
///
/// # Panics
///
/// Panics if the temporary directory cannot be created.
pub fn test_temp_dir() -> tempfile::TempDir {
    tempfile::TempDir::new().expect("Failed to create temp dir")
}

/// Options for compaction tests.
#[must_use]
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
        cloud_write_policy: None,
        simulated_cloud_overrides: None,
    }
}

/// Options for manual compaction tests.
#[must_use]
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
        cloud_write_policy: None,
        simulated_cloud_overrides: None,
    }
}

/// Options for durability tests.
#[must_use]
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
        cloud_write_policy: None,
        simulated_cloud_overrides: None,
    }
}
