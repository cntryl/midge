//! Production-grade configuration system for Midge.
//!
//! This module implements the specification in `docs/specs/configuration_spec.md`.
//!
//! # Design Philosophy
//!
//! Midge's configuration is designed around **three core questions**:
//!
//! 1. What is the performance goal? (`Goal::Latency` | `Goal::Throughput` | `Goal::Cost`)
//! 2. What durability guarantee is required? (`Durability::Strict` | `Durability::Steady` | `Durability::CloudReplicated`)
//! 3. How much memory is available? (`MemoryBudget::Auto` | `MemoryBudget::Bytes(n)`)
//!
//! All other parameters (buffer sizes, compaction triggers, cache allocation, etc.)
//! are **derived automatically** from these inputs plus optional workload profile and cloud mode.
//!
//! # Example
//!
//! ```rust,no_run
//! use cntryl_midge::config::{ConfigBuilder, Goal, Durability};
//!
//! // Simple latency-optimized configuration
//! let config = ConfigBuilder::new("./my_db")
//!     .goal(Goal::Latency)
//!     .durability(Durability::Steady)
//!     .build()
//!     .expect("valid configuration");
//!
//! // Inspect derived parameters
//! let plan = config.plan();
//! println!("Block size: {} bytes", plan.block_size);
//! println!("Memtable size: {} MB", plan.memtable_size / 1024 / 1024);
//! ```

pub mod autotune;
pub mod builder;
pub mod cloud;
pub mod cloud_builder;
pub mod column_family;
pub mod derivation;
pub mod profile;
pub mod validation;

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// Re-export commonly used types
pub use autotune::{Autotuner, ObservedMetrics};
pub use builder::ConfigBuilder;
pub use cloud_builder::CloudConfigBuilder;
pub use column_family::{CompactionStyle, CompressionType};

/// Performance optimization goal.
///
/// Determines the primary optimization target for derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Goal {
    /// Optimize for low latency (p99 < 10ms for point queries).
    ///
    /// - Smaller block sizes (16 KiB)
    /// - More aggressive bloom filters
    /// - Lower compaction trigger thresholds
    /// - Higher cache allocation
    #[default]
    Latency,

    /// Optimize for high throughput (MB/s for bulk operations).
    ///
    /// - Larger block sizes (64 KiB)
    /// - Larger memtables (256 MB)
    /// - Higher compaction concurrency
    /// - Larger SST files
    Throughput,

    /// Optimize for cost (minimize memory/CPU usage).
    ///
    /// - Minimal cache allocation
    /// - Lower compaction concurrency
    /// - Smaller bloom filters
    /// - Higher compression
    Cost,
}

/// Durability guarantee level.
///
/// Determines when writes are considered durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Durability {
    /// Fsync on every write commit.
    ///
    /// **Guarantee:** Data survives process crash and power loss.
    /// **Latency:** Highest (per-write fsync overhead).
    /// **Use case:** Critical data, financial transactions.
    Strict,

    /// Fsync at controlled intervals (default: 20ms, auto-tuned 10-40ms).
    ///
    /// **Guarantee:** Data loss window ≤ sync interval on crash.
    /// **Latency:** Low (amortized fsync cost).
    /// **Use case:** Most applications, balanced durability/performance.
    #[default]
    Steady,

    /// Durability confirmed via local fsync + verified cloud copy.
    ///
    /// **Guarantee:** Data survives node failure (cloud durability 11+ nines).
    /// **Latency:** Medium (local fsync + async cloud verification).
    /// **Use case:** Distributed systems, high availability.
    CloudReplicated,
}

/// Memory budget specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MemoryBudget {
    /// Automatically determine memory budget from available system memory.
    ///
    /// Uses ~50% of available RAM for cache + memtables.
    #[default]
    Auto,

    /// Explicit memory budget in bytes.
    ///
    /// All allocations (cache + memtables + overhead) must fit within this budget.
    Bytes(usize),
}

/// Workload profile for optimizing derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum WorkloadProfile {
    /// Balanced read/write workload (default).
    #[default]
    Mixed,

    /// Write-heavy workload (>70% writes).
    ///
    /// - Larger memtables
    /// - More aggressive compaction
    /// - Lower bloom filter priority
    WriteHeavy,

    /// Read-mostly workload (>70% reads).
    ///
    /// - More aggressive bloom filters
    /// - Higher cache allocation
    /// - Lower compaction priority
    ReadMostly,

    /// Range scan workload.
    ///
    /// - Larger block sizes
    /// - Sequential access optimization
    /// - Lower bloom filter priority (not useful for ranges)
    RangeScan,

    /// TTL-heavy workload with frequent expirations.
    ///
    /// - More aggressive compaction
    /// - Higher tombstone cleanup priority
    TtlHeavy,

    /// Tiny keys (< 16 bytes average).
    ///
    /// - Smaller block sizes
    /// - More entries per memtable
    TinyKeys,

    /// Large values (> 1 KB average).
    ///
    /// - Larger block sizes
    /// - Larger memtables
    /// - Higher compression priority
    LargeValues,
}

/// Cloud storage mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CloudMode {
    /// Fully local storage (no cloud backend).
    #[default]
    Off,

    /// Keep all SSTs locally; upload asynchronously for durability.
    ///
    /// - Reads: Always local (no cloud latency)
    /// - Writes: Local + async cloud upload
    /// - Durability: Cloud-backed after upload verification
    Cache,

    /// Keep hot SSTs local; evict cold SSTs to cloud.
    ///
    /// - Reads: Local for hot data, cloud fetch for cold data
    /// - Writes: Local + async cloud upload
    /// - Eviction: When local cache exceeds 80% of budget
    Tiered,

    /// Same as Cache + asynchronous replication to secondary region.
    ///
    /// - Reads: Always local
    /// - Writes: Local + dual-region cloud upload
    /// - Failover: ≤ 60s to secondary region
    Replicated,
}

/// Complete configuration for a Midge instance.
///
/// Built via `ConfigBuilder` and validated before construction.
#[derive(Debug, Clone)]
pub struct Config {
    // Core inputs
    pub(crate) path: PathBuf,
    pub(crate) goal: Goal,
    pub(crate) durability: Durability,
    pub(crate) memory_budget: MemoryBudget,
    pub(crate) workload_profile: WorkloadProfile,
    pub(crate) cloud_mode: CloudMode,

    // Derived parameters (frozen after build)
    pub(crate) plan: ConfigPlan,

    // Optional extensions
    pub(crate) autotune_enabled: bool,
    pub(crate) cloud_config: Option<cloud::CloudConfig>,
}

impl Config {
    /// Get the configuration plan with all derived parameters.
    pub fn plan(&self) -> &ConfigPlan {
        &self.plan
    }

    /// Get the database path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get the performance goal.
    pub fn goal(&self) -> Goal {
        self.goal
    }

    /// Get the durability level.
    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// Get the workload profile.
    pub fn workload_profile(&self) -> WorkloadProfile {
        self.workload_profile
    }

    /// Get the memory budget specification.
    pub fn memory_budget(&self) -> MemoryBudget {
        self.memory_budget
    }

    /// Get the cloud mode.
    pub fn cloud_mode(&self) -> CloudMode {
        self.cloud_mode
    }

    /// Check if autotuning is enabled.
    pub fn autotune_enabled(&self) -> bool {
        self.autotune_enabled
    }

    /// Get cloud configuration if cloud mode is enabled.
    pub fn cloud_config(&self) -> Option<&cloud::CloudConfig> {
        self.cloud_config.as_ref()
    }

    /// Convert configuration to legacy `MidgeOptions`.
    ///
    /// This method bridges the new high-level Config API with the existing
    /// low-level MidgeOptions interface used internally by MidgeEngine.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use cntryl_midge::config::{ConfigBuilder, Goal, Durability};
    ///
    /// let config = ConfigBuilder::new("./my_db")
    ///     .goal(Goal::Latency)
    ///     .durability(Durability::Steady)
    ///     .build()
    ///     .expect("valid configuration");
    ///
    /// // Convert to MidgeOptions for engine initialization
    /// let opts = config.to_options();
    /// println!("Block size: {}", opts.block_size);
    /// ```
    pub fn to_options(&self) -> crate::MidgeOptions {
        use crate::common::codec::CompressionType;
        use crate::{MidgeOptions, StorageMode};

        // Determine storage mode based on cloud configuration
        let storage_mode = if let Some(cloud_cfg) = &self.cloud_config {
            StorageMode::CloudBacked {
                local_cache_path: self.path.clone(),
                cloud_backend: cloud_cfg.backend.clone(),
                storage_context: crate::config::cloud::StorageContext::new(&cloud_cfg.bucket),
                local_wal_sync: matches!(self.durability, Durability::Strict | Durability::Steady),
                wal_batch_size: cloud_cfg.upload_params.chunk_size,
                sst_cache_capacity: 100, // Reasonable default
            }
        } else {
            StorageMode::LocalDisk {
                db_path: self.path.clone(),
            }
        };

        // Map Goal to compression strategy
        // Per codec optimization table:
        // - L0 (hot data): LZ4 for ultra-low latency
        // - L1-L3 (warm data): Zstd1-3 for balanced ratio vs CPU
        // - L4+ (cold data): Zstd5-9 for maximum density
        // For simplicity, use a single default per goal and let per-level
        // compression be configured later when implementing tiered compression.
        let compression = match self.goal {
            Goal::Latency => CompressionType::Lz4, // Fast decompression, low latency
            Goal::Throughput => CompressionType::Zstd3, // Balanced ratio/speed
            Goal::Cost => CompressionType::Zstd5,  // Better compression for cost savings
        };

        MidgeOptions {
            storage_mode,
            memtable_size: self.plan.memtable_size,
            max_levels: self.plan.max_levels,
            level_multiplier: self.plan.level_multiplier,
            block_size: self.plan.block_size,
            compression,
            enable_compaction: true, // Always enabled with new config
            compaction_sst_threshold: self.plan.l0_compaction_trigger,
            compaction_check_interval_ms: 200, // Fixed for now
            read_only: false,                  // Config system doesn't support read-only yet
            bloom_filter_fp_rate: self.bloom_fp_rate_from_bits(),
            wal_buffer_size: self.plan.wal_buffer_size,
            wal_sync: self.plan.wal_sync_per_write,
            cache_size_mb: self.plan.block_cache_size / (1024 * 1024),
            table_cache_size: 100,                      // Reasonable default
            max_open_files: 1000,                       // Reasonable default
            txn_spill_threshold_bytes: 8 * 1024 * 1024, // 8MB default
            ttl_seconds: 0,                             // Not configured via Config yet
            tombstone_density_threshold: 50.0,          // Reasonable default
            max_tombstone_compaction_files: 3,
            wal_recovery_mode: crate::WalRecoveryMode::default(), // Strict by default
            // Map cloud config upload bandwidth cap (if provided) to the legacy
            // MidgeOptions global upload limiter fields. Bandwidth cap is an
            // optional value on CloudConfig::upload_params.bandwidth_cap and is
            // interpreted as bytes/sec. For burst, use multipart chunk size as a
            // reasonable default when a cap is present.
            cloud_upload_bytes_per_sec: self
                .cloud_config
                .as_ref()
                .and_then(|c| c.upload_params.bandwidth_cap)
                .map(|v| v as u64)
                .unwrap_or(0),
            cloud_upload_max_burst_bytes: self
                .cloud_config
                .as_ref()
                .map(|c| c.upload_params.chunk_size as u64)
                .unwrap_or(0),
            test_hooks: None,          // No test hooks when using Config API
            paranoid_checksums: false, // Default to false for performance
        }
    }

    /// Calculate bloom filter false positive rate from bits per key.
    ///
    /// Formula: FPR ≈ (0.6185)^(bits_per_key)
    fn bloom_fp_rate_from_bits(&self) -> f64 {
        let bits = self.plan.bloom_bits_per_key as f64;
        0.6185_f64.powf(bits)
    }
}

/// Configuration plan containing all derived parameters.
///
/// This structure exposes all internal parameters derived from high-level knobs.
/// It provides complete transparency and enables reproducibility for diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigPlan {
    // Memory allocation
    pub total_memory_budget: usize,
    pub block_cache_size: usize,
    pub memtable_size: usize,
    pub memtable_count: usize,
    pub overhead_budget: usize,

    // Storage parameters
    pub block_size: usize,
    pub bloom_bits_per_key: u32,
    pub target_sst_size_l1: usize,
    pub level_multiplier: usize,
    pub max_levels: usize,

    // Compaction parameters
    pub l0_compaction_trigger: usize,
    pub compaction_concurrency: usize,

    // WAL parameters
    pub wal_sync_per_write: bool,
    pub wal_sync_interval: Option<Duration>,
    pub wal_buffer_size: usize,

    // Cloud parameters (if applicable)
    pub upload_concurrency: Option<usize>,
    pub multipart_chunk_size: Option<usize>,
    pub prefetch_depth: Option<usize>,

    // Validation metadata
    pub memory_utilization: f64, // Percentage of budget used
    pub validated: bool,
}

impl ConfigPlan {
    /// Check if configuration passed validation.
    pub fn is_valid(&self) -> bool {
        self.validated
    }

    /// Get memory utilization as a percentage (0.0 - 1.0).
    pub fn memory_utilization_pct(&self) -> f64 {
        self.memory_utilization
    }
}

/// Configuration error types.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Memory overcommit: requested {requested} bytes but budget is {budget} bytes (utilization: {utilization:.1}%)")]
    MemoryOvercommit {
        requested: usize,
        budget: usize,
        utilization: f64,
    },

    #[error("Unsafe WAL sync interval: {interval_ms}ms exceeds safe limit of 250ms")]
    UnsafeWalInterval { interval_ms: u64 },

    #[error("Cloud mode {mode:?} requires bucket configuration")]
    CloudBucketRequired { mode: CloudMode },

    #[error("Invalid path: {path}")]
    InvalidPath { path: String },

    #[error("Invalid memory budget: {budget} bytes (minimum: {minimum} bytes)")]
    InvalidMemoryBudget { budget: usize, minimum: usize },

    #[error("Validation failed: {reason}")]
    ValidationFailed { reason: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type ConfigResult<T> = Result<T, ConfigError>;
