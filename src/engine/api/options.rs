//! Database Configuration Options
//!
//! Smart configuration system with **automatic parameter derivation**.
//!
//! # Design Philosophy
//!
//! Instead of exposing hundreds of low-level tuning knobs, Midge asks **two core questions**:
//!
//! 1. **What's the performance goal?** (`Goal::Latency` | `Goal::Throughput` | `Goal::Economy`)
//! 2. **How much memory?** (`MemoryBudget::Auto` | `MemoryBudget::Bytes(n)`)
//!
//! All other parameters (block sizes, buffer sizes, compaction triggers, cache allocation, etc.)
//! are **derived automatically** from these three inputs plus optional workload hints.
//! Advanced callers can also override runtime memtable sizing explicitly while
//! leaving `MemoryBudget` semantics unchanged.
//!
//! # Example
//!
//! ```rust,no_run
//! use cntryl_midge::{MidgeEngine, OpenOptions};
//!
//! // Open a database with default options
//! let opts = OpenOptions::local("./my_db").build();
//! let engine = MidgeEngine::open(opts)?;
//! # Ok::<(), cntryl_midge::MidgeError>(())
//! ```

use std::path::PathBuf;
use std::time::Duration;

pub use crate::config::{RecoveryPolicy, Storage};
use crate::sst::cache::CachePolicyType;
use crate::sst::compression::{CompressionAlgo, CompressionPolicy};
use crate::storage::providers::CloudProviderConfig;
#[cfg(test)]
use crate::storage::providers::{
    AzureCredentialSource, GcsApiStyle, GcsCredentialSource, S3CredentialSource,
};

/// Performance optimization goal.
///
/// Determines the primary optimization target for derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
    Economy,
}

/// Memory budget specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryBudget {
    /// Automatically determine memory budget from available system memory.
    ///
    /// Uses ~50% of the effective memory limit (cgroup-aware when possible).
    #[default]
    Auto,

    /// Explicit memory budget in bytes.
    ///
    /// All allocations (cache + memtables + overhead) must fit within this budget.
    Bytes(usize),
}

/// Workload profile for optimizing derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
}

/// Eviction policy for the block cache used by SST reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockCachePolicy {
    /// Least-recently-used eviction. This is the stable default.
    #[default]
    Lru,
    /// Window TinyLFU-style admission/eviction for frequency-biased workloads.
    TinyLfu,
    /// CLOCK-Pro eviction for scan-resistant workloads.
    ClockPro,
}

impl From<BlockCachePolicy> for CachePolicyType {
    fn from(policy: BlockCachePolicy) -> Self {
        match policy {
            BlockCachePolicy::Lru => Self::Lru,
            BlockCachePolicy::TinyLfu => Self::TinyLfu,
            BlockCachePolicy::ClockPro => Self::ClockPro,
        }
    }
}

/// Cloud-backed write tuning used by `OpenOptions`.
///
/// Defaults preserve the engine's production behavior:
/// - flush a WAL-backed memtable after a segment gap of 128
/// - seal cloud WAL segments after 16 MiB, 500 ms, or 10,000 pending writes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudWritePolicy {
    pub eventual_flush_segment_gap: u64,
    pub wal_seal_min_segment_bytes: usize,
    pub wal_seal_max_flush_delay: Duration,
    pub wal_seal_max_pending_writes: usize,
}

impl Default for CloudWritePolicy {
    fn default() -> Self {
        Self {
            eventual_flush_segment_gap: 128,
            wal_seal_min_segment_bytes: 16 * 1024 * 1024,
            wal_seal_max_flush_delay: Duration::from_millis(500),
            wal_seal_max_pending_writes: 10_000,
        }
    }
}

impl From<CloudWritePolicy> for crate::runtime::CloudRuntimePolicy {
    fn from(policy: CloudWritePolicy) -> Self {
        Self {
            eventual_flush_segment_gap: policy.eventual_flush_segment_gap,
            wal_seal: crate::runtime::CloudWalSealPolicy {
                min_segment_bytes: policy.wal_seal_min_segment_bytes,
                max_flush_delay: policy.wal_seal_max_flush_delay,
                max_pending_writes: policy.wal_seal_max_pending_writes,
            },
        }
    }
}

/// Database open options with smart defaults.
///
/// Use the builder pattern to configure high-level knobs, and all low-level
/// parameters will be derived automatically.
///
/// Storage backend MUST be explicitly specified via constructors:
/// - `OpenOptions::in_memory()`
/// - `OpenOptions::local(path)`
/// - `OpenOptions::cloud(cache_path`, provider, prefix)
/// - `OpenOptions::cloud_simulated(cache_path`, bucket, prefix)
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Storage backend (REQUIRED - no default)
    pub storage: Storage,

    /// Performance goal
    pub goal: Goal,

    /// Memory budget
    pub memory_budget: MemoryBudget,

    /// Workload profile hint
    pub workload: WorkloadProfile,

    /// Recovery policy used during engine open.
    pub recovery_policy: RecoveryPolicy,

    /// Explicit override for memtable size limit in bytes.
    explicit_memtable_size_limit: Option<usize>,

    /// Explicit override for memtable flush threshold in bytes.
    explicit_memtable_flush_threshold: Option<usize>,

    /// Derived memory budget in bytes (from `build()`)
    pub(crate) derived_memory_budget: usize,

    // Derived parameters (populated by build())
    /// Block size in bytes (derived)
    pub(crate) block_size: usize,

    /// Memtable size limit (derived)
    pub(crate) memtable_size_limit: usize,

    /// Memtable flush threshold (derived)
    pub(crate) memtable_flush_threshold: usize,

    /// Target SST file size (derived)
    pub(crate) target_sst_size: usize,

    /// Block cache size (derived)
    pub(crate) block_cache_size: usize,

    /// Block cache eviction policy.
    block_cache_policy: BlockCachePolicy,

    /// Cloud write tuning policy.
    cloud_write_policy: CloudWritePolicy,

    /// Whether automatic background compaction scheduling is enabled.
    background_compaction: bool,

    /// WAL buffer size (derived)
    pub(crate) wal_buffer_size: usize,

    /// L0 compaction trigger (derived)
    pub(crate) l0_compaction_trigger: usize,

    /// Compression policy for SST blocks (derived from Goal)
    pub(crate) compression_policy: CompressionPolicy,

    /// Optional WAL batch configuration (from testkit for batched durability mode)
    pub(crate) wal_batch_config: Option<crate::wal::policy::BatchConfig>,
}

impl OpenOptions {
    /// Create in-memory database instance
    ///
    /// Data is NOT persisted and will be lost when engine is dropped.
    /// Ideal for: testing, caching, ephemeral workloads
    ///
    /// Memtable sizing is derived automatically by default. Advanced callers can
    /// override the runtime memtable size limit and flush threshold explicitly.
    ///
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            storage: Storage::InMemory,
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            block_cache_policy: BlockCachePolicy::default(),
            cloud_write_policy: CloudWritePolicy::default(),
            background_compaction: true,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
        }
    }

    /// Create local filesystem database instance
    ///
    /// Data persists to the specified path on local disk.
    /// Ideal for: traditional deployments, single-node databases
    ///
    pub fn local<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            storage: Storage::Local { path: path.into() },
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            block_cache_policy: BlockCachePolicy::default(),
            cloud_write_policy: CloudWritePolicy::default(),
            background_compaction: true,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
        }
    }

    /// Create cloud-backed database instance using a real object-store provider.
    ///
    /// Data persists to cloud object storage (S3, Azure, GCS, OCI, etc.).
    /// Uses hybrid model with local cache for performance.
    /// Ideal for: cloud-native deployments, serverless, distributed systems
    ///
    /// # Arguments
    /// * `local_cache_path` - Local directory for caching/staging
    /// * `provider` - Cloud provider, bucket/container, credentials, and endpoint
    /// * `prefix` - Object key prefix
    pub fn cloud<P: Into<PathBuf>, S: Into<String>>(
        local_cache_path: P,
        provider: CloudProviderConfig,
        prefix: S,
    ) -> Self {
        Self {
            storage: Storage::Cloud {
                local_cache_path: local_cache_path.into(),
                provider,
                prefix: prefix.into(),
            },
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            block_cache_policy: BlockCachePolicy::default(),
            cloud_write_policy: CloudWritePolicy::default(),
            background_compaction: true,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
        }
    }

    /// Create a filesystem-backed cloud simulation.
    ///
    /// This keeps deterministic `CloudAsync` tests available without pretending to
    /// connect to a real provider.
    pub fn cloud_simulated<P: Into<PathBuf>, S: Into<String>>(
        local_cache_path: P,
        bucket: S,
        prefix: S,
    ) -> Self {
        Self {
            storage: Storage::CloudSimulated {
                local_cache_path: local_cache_path.into(),
                bucket: bucket.into(),
                prefix: prefix.into(),
            },
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            block_cache_policy: BlockCachePolicy::default(),
            cloud_write_policy: CloudWritePolicy::default(),
            background_compaction: true,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
        }
    }

    /// Set performance goal.
    #[must_use]
    pub fn goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    /// Set memory budget.
    #[must_use]
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = budget;
        self
    }

    /// Override the derived memtable size limit in bytes.
    ///
    /// By default, Midge derives memtable sizing from the goal, workload, and
    /// memory budget. This override applies the exact requested runtime size
    /// limit without changing `MemoryBudget`.
    #[must_use]
    pub fn with_memtable_size_limit(mut self, bytes: usize) -> Self {
        self.explicit_memtable_size_limit = Some(Self::sanitize_memtable_bytes(bytes));
        self
    }

    /// Override the runtime memtable flush threshold in bytes.
    ///
    /// By default, the flush threshold follows the derived memtable sizing. If
    /// only `with_memtable_size_limit` is set, the flush threshold uses that
    /// same value unless this override is also provided.
    #[must_use]
    pub fn with_memtable_flush_threshold(mut self, bytes: usize) -> Self {
        self.explicit_memtable_flush_threshold = Some(Self::sanitize_memtable_bytes(bytes));
        self
    }

    /// Set workload profile hint.
    #[must_use]
    pub fn workload(mut self, profile: WorkloadProfile) -> Self {
        self.workload = profile;
        self
    }

    /// Set the block cache eviction policy.
    #[must_use]
    pub fn block_cache_policy(mut self, policy: BlockCachePolicy) -> Self {
        self.block_cache_policy = policy;
        self
    }

    /// Set the cloud write tuning policy.
    #[must_use]
    pub fn cloud_write_policy(mut self, policy: CloudWritePolicy) -> Self {
        self.cloud_write_policy = policy;
        self
    }

    /// Enable or disable automatic background compaction scheduling.
    ///
    /// Manual `Engine::compact_all()` remains available when background
    /// scheduling is disabled.
    #[must_use]
    pub fn background_compaction(mut self, enabled: bool) -> Self {
        self.background_compaction = enabled;
        self
    }

    /// Set recovery policy.
    #[must_use]
    pub fn recovery_policy(mut self, policy: RecoveryPolicy) -> Self {
        self.recovery_policy = policy;
        self
    }

    /// Build options with derived parameters.
    ///
    /// This automatically computes all low-level parameters based on the
    /// high-level knobs (goal, memory, workload) plus optional explicit
    /// memtable overrides for advanced callers.
    #[must_use]
    pub fn build(mut self) -> Self {
        // Derive memory budget
        let total_memory = match self.memory_budget {
            MemoryBudget::Auto => memory::auto_memory_budget_bytes().unwrap_or(512 * 1024 * 1024),
            MemoryBudget::Bytes(n) => n,
        };
        self.derived_memory_budget = total_memory;

        // Derive block size based on goal and workload
        self.block_size = match (self.goal, self.workload) {
            (Goal::Latency, _) => 16 * 1024, // 16KB for low latency
            (Goal::Economy, _) => 32 * 1024, // 32KB balanced
            (Goal::Throughput, WorkloadProfile::RangeScan) => 128 * 1024, // 128KB for bulk scans
            (Goal::Throughput, _) => 64 * 1024, // 64KB for throughput
        };

        // Derive memtable size based on goal and workload
        let base_memtable = match self.goal {
            Goal::Latency => 64 * 1024 * 1024,     // 64MB for latency
            Goal::Throughput => 256 * 1024 * 1024, // 256MB for throughput
            Goal::Economy => 32 * 1024 * 1024,     // 32MB for cost
        };

        self.memtable_size_limit = match self.workload {
            WorkloadProfile::WriteHeavy => base_memtable * 2, // Double for write-heavy
            WorkloadProfile::ReadMostly => base_memtable / 2, // Half for read-heavy (more cache)
            _ => base_memtable,
        };

        // Clamp memtable size to keep total memory usage within budget.
        let min_memtable = 4 * 1024 * 1024;
        let max_memtable = total_memory / 2;
        let max_allowed = max_memtable.max(min_memtable.min(total_memory));
        self.memtable_size_limit = self.memtable_size_limit.min(max_allowed).max(1);

        if let Some(explicit_memtable_size_limit) = self.explicit_memtable_size_limit {
            self.memtable_size_limit = Self::sanitize_memtable_bytes(explicit_memtable_size_limit);
        }

        self.memtable_flush_threshold = if let Some(explicit_memtable_flush_threshold) =
            self.explicit_memtable_flush_threshold
        {
            Self::sanitize_memtable_bytes(explicit_memtable_flush_threshold)
        } else if self.explicit_memtable_size_limit.is_some() {
            self.memtable_size_limit
        } else if let MemoryBudget::Bytes(n) = self.memory_budget {
            Self::sanitize_memtable_bytes(n / 2)
        } else {
            self.memtable_size_limit
        };

        // Derive target SST size
        self.target_sst_size = match self.goal {
            Goal::Latency => 128 * 1024 * 1024,    // 128MB
            Goal::Throughput => 512 * 1024 * 1024, // 512MB
            Goal::Economy => 256 * 1024 * 1024,    // 256MB
        };

        // Allocate remaining memory to block cache
        let cache_ratio_percent: usize = match self.workload {
            WorkloadProfile::ReadMostly => 70,
            WorkloadProfile::WriteHeavy => 20,
            _ => 50,
        };

        let usable_memory = total_memory.saturating_sub(self.memtable_size_limit * 2); // 2 memtables
        self.block_cache_size = usable_memory.saturating_mul(cache_ratio_percent) / 100;

        // Cap cache size for Economy goal to minimize resource usage
        if self.goal == Goal::Economy {
            self.block_cache_size = self.block_cache_size.min(256 * 1024 * 1024);
            // 256MB max
        }

        // Derive WAL buffer size
        self.wal_buffer_size = match self.goal {
            Goal::Latency => 128 * 1024,     // 128KB
            Goal::Throughput => 1024 * 1024, // 1MB
            Goal::Economy => 256 * 1024,     // 256KB
        };
        self.wal_buffer_size = self.wal_buffer_size.min(total_memory.max(32 * 1024));

        // Derive compaction trigger
        self.l0_compaction_trigger = match (self.goal, self.workload) {
            (Goal::Latency, _) => 3,               // Aggressive
            (_, WorkloadProfile::WriteHeavy) => 8, // Relaxed for write-heavy
            (Goal::Throughput, _) => 6,            // Moderate
            _ => 4,                                // Default
        };

        // Derive compression policy from goal
        //   Latency  → fast codec, minimal CPU overhead
        //   Throughput → adaptive, try a few codecs per block
        //   Economy  → max compression ratio
        self.compression_policy = match self.goal {
            Goal::Latency => CompressionPolicy::Fixed(CompressionAlgo::Lz4),
            Goal::Throughput => CompressionPolicy::Adaptive {
                min_savings_bytes: 256,
                min_ratio: 1.05,
                check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
            },
            Goal::Economy => CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
        };

        self
    }

    // Getters for derived parameters

    /// Get derived block size
    #[must_use]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Get derived memtable size limit
    #[must_use]
    pub fn memtable_size_limit(&self) -> usize {
        self.memtable_size_limit
    }

    /// Get derived target SST size
    #[must_use]
    pub fn target_sst_size(&self) -> usize {
        self.target_sst_size
    }

    /// Get derived block cache size
    #[must_use]
    pub fn block_cache_size(&self) -> usize {
        self.block_cache_size
    }

    /// Get the configured block cache eviction policy.
    #[must_use]
    pub fn block_cache_policy_value(&self) -> BlockCachePolicy {
        self.block_cache_policy
    }

    /// Get the configured cloud write policy.
    #[must_use]
    pub fn cloud_write_policy_value(&self) -> &CloudWritePolicy {
        &self.cloud_write_policy
    }

    /// Get derived WAL buffer size
    #[must_use]
    pub fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    /// Get derived L0 compaction trigger
    #[must_use]
    pub fn l0_compaction_trigger(&self) -> usize {
        self.l0_compaction_trigger
    }

    /// Get derived compression policy
    #[must_use]
    pub fn compression_policy(&self) -> &CompressionPolicy {
        &self.compression_policy
    }

    pub(crate) fn runtime_memtable_size_limit(&self) -> usize {
        if self.explicit_memtable_size_limit.is_some() {
            self.memtable_size_limit
        } else if let MemoryBudget::Bytes(n) = self.memory_budget {
            Self::sanitize_memtable_bytes(n / 2)
        } else {
            self.memtable_size_limit
        }
    }

    pub(crate) fn runtime_memtable_flush_threshold(&self) -> usize {
        self.memtable_flush_threshold
    }

    pub(crate) fn block_cache_policy_type(&self) -> CachePolicyType {
        self.block_cache_policy.into()
    }

    pub(crate) fn cloud_runtime_policy(&self) -> crate::runtime::CloudRuntimePolicy {
        self.cloud_write_policy.clone().into()
    }

    pub(crate) fn background_compaction_enabled(&self) -> bool {
        self.background_compaction
    }

    fn sanitize_memtable_bytes(bytes: usize) -> usize {
        bytes.max(1)
    }
}

mod memory {
    #[cfg(target_os = "linux")]
    use std::fs;
    #[cfg(target_os = "linux")]
    use std::path::Path;

    pub fn auto_memory_budget_bytes() -> Option<usize> {
        let limit = effective_memory_limit_bytes()?;
        Some(budget_from_limit_bytes(limit))
    }

    fn budget_from_limit_bytes(limit: u64) -> usize {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let mut budget = limit / 2;
        let min_budget: usize = 64 * 1024 * 1024;
        if limit >= min_budget.saturating_mul(2) {
            budget = budget.max(min_budget);
        } else {
            budget = budget.max(1024 * 1024);
        }
        budget.min(limit).max(1)
    }

    fn effective_memory_limit_bytes() -> Option<u64> {
        let host_total = host_total_memory_bytes();
        let cgroup_limit = cgroup_memory_limit_bytes();

        match (host_total, cgroup_limit) {
            (Some(host), Some(limit)) => Some(host.min(limit)),
            (Some(host), None) => Some(host),
            (None, Some(limit)) => Some(limit),
            (None, None) => None,
        }
    }

    fn host_total_memory_bytes() -> Option<u64> {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let total_kb = system.total_memory();
        if total_kb == 0 {
            return None;
        }
        Some(total_kb.saturating_mul(1024))
    }

    #[cfg(target_os = "linux")]
    fn cgroup_memory_limit_bytes() -> Option<u64> {
        let v2_limit = cgroup_v2_limit_bytes();
        if v2_limit.is_some() {
            return v2_limit;
        }
        cgroup_v1_limit_bytes()
    }

    #[cfg(not(target_os = "linux"))]
    fn cgroup_memory_limit_bytes() -> Option<u64> {
        None
    }

    #[cfg(target_os = "linux")]
    fn cgroup_v2_limit_bytes() -> Option<u64> {
        let controllers = Path::new("/sys/fs/cgroup/cgroup.controllers");
        if !controllers.exists() {
            return None;
        }
        let max_path = Path::new("/sys/fs/cgroup/memory.max");
        let value = fs::read_to_string(max_path).ok()?;
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("max") {
            return None;
        }
        trimmed.parse::<u64>().ok().filter(|v| *v > 0)
    }

    #[cfg(target_os = "linux")]
    fn cgroup_v1_limit_bytes() -> Option<u64> {
        let max_path = Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        let value = fs::read_to_string(max_path).ok()?;
        let limit = value.trim().parse::<u64>().ok()?;
        if limit == 0 {
            return None;
        }
        Some(limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Goal Enum Tests ==========

    #[test]
    fn should_have_latency_as_default_goal() {
        assert_eq!(Goal::default(), Goal::Latency);
    }

    #[test]
    fn should_create_throughput_goal() {
        assert_eq!(Goal::Throughput, Goal::Throughput);
    }

    #[test]
    fn should_create_cost_goal() {
        assert_eq!(Goal::Economy, Goal::Economy);
    }

    #[test]
    fn should_distinguish_different_goals() {
        assert_ne!(Goal::Latency, Goal::Throughput);
        assert_ne!(Goal::Throughput, Goal::Economy);
        assert_ne!(Goal::Economy, Goal::Latency);
    }

    // ========== MemoryBudget Enum Tests ==========

    #[test]
    fn should_have_auto_as_default_memory_budget() {
        assert_eq!(MemoryBudget::default(), MemoryBudget::Auto);
    }

    #[test]
    fn should_create_explicit_memory_budget() {
        let budget = MemoryBudget::Bytes(4 * 1024 * 1024 * 1024);
        assert_eq!(budget, MemoryBudget::Bytes(4 * 1024 * 1024 * 1024));
    }

    #[test]
    fn should_distinguish_memory_budgets() {
        assert_ne!(MemoryBudget::Auto, MemoryBudget::Bytes(1000));
    }

    // ========== WorkloadProfile Enum Tests ==========

    #[test]
    fn should_have_mixed_as_default_workload() {
        assert_eq!(WorkloadProfile::default(), WorkloadProfile::Mixed);
    }

    #[test]
    fn should_create_write_heavy_workload() {
        assert_eq!(WorkloadProfile::WriteHeavy, WorkloadProfile::WriteHeavy);
    }

    #[test]
    fn should_create_read_mostly_workload() {
        assert_eq!(WorkloadProfile::ReadMostly, WorkloadProfile::ReadMostly);
    }

    #[test]
    fn should_create_range_scan_workload() {
        assert_eq!(WorkloadProfile::RangeScan, WorkloadProfile::RangeScan);
    }

    #[test]
    fn should_create_ttl_heavy_workload() {
        assert_eq!(WorkloadProfile::TtlHeavy, WorkloadProfile::TtlHeavy);
    }

    #[test]
    fn should_distinguish_workload_profiles() {
        // Arrange
        // (no setup required)

        // Act
        // (compare variants)

        // Assert
        assert_ne!(WorkloadProfile::Mixed, WorkloadProfile::WriteHeavy);
        assert_ne!(WorkloadProfile::WriteHeavy, WorkloadProfile::ReadMostly);
        assert_ne!(WorkloadProfile::ReadMostly, WorkloadProfile::RangeScan);
        assert_ne!(WorkloadProfile::RangeScan, WorkloadProfile::TtlHeavy);
    }

    // ========== Cloud Provider Constructor Tests ==========

    #[test]
    fn should_create_aws_s3_with_default_chain() {
        // Arrange
        let provider = CloudProviderConfig::aws_s3("bucket", "us-east-1");

        // Act
        // Assert
        assert_eq!(
            provider,
            CloudProviderConfig::AwsS3 {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                credentials: S3CredentialSource::AwsDefaultChain,
            }
        );
    }

    #[test]
    fn should_create_s3_compatible_env_with_safe_defaults() {
        // Arrange
        let provider = CloudProviderConfig::s3_compatible_env("bucket", "http://localhost:9000");

        // Act
        // Assert
        assert_eq!(
            provider,
            CloudProviderConfig::S3Compatible {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "http://localhost:9000".to_string(),
                path_style: true,
                credentials: S3CredentialSource::Environment,
            }
        );
    }

    #[test]
    fn should_create_s3_family_static_configs() {
        // Arrange
        let minio =
            CloudProviderConfig::minio_static("bucket", "http://minio:9000", "key", "secret");
        let wasabi = CloudProviderConfig::wasabi_static("bucket", "us-east-2", "key", "secret");
        let oci = CloudProviderConfig::oci_s3_compatible_static(
            "namespace",
            "bucket",
            "us-phoenix-1",
            "key",
            "secret",
        );

        // Act
        // Assert
        assert!(matches!(minio, CloudProviderConfig::Minio { .. }));
        assert!(matches!(
            wasabi,
            CloudProviderConfig::Wasabi {
                credentials: S3CredentialSource::Static { .. },
                endpoint: None,
                ..
            }
        ));
        assert!(matches!(
            oci,
            CloudProviderConfig::OciS3Compatible {
                path_style: false,
                credentials: S3CredentialSource::Static { .. },
                ..
            }
        ));
    }

    #[test]
    fn should_create_azure_configs_for_supported_credentials() {
        // Arrange
        let identity = CloudProviderConfig::azure_blob("account", "container");
        let shared_key =
            CloudProviderConfig::azure_blob_shared_key("account", "container", "account-key");
        let sas = CloudProviderConfig::azure_blob_sas("account", "container", "?sig=token");
        let conn = CloudProviderConfig::azure_blob_connection_string(
            "container",
            "AccountName=account;AccountKey=key",
        );

        // Act
        // Assert
        assert!(matches!(
            identity,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::LightweightDefaultChain,
                ..
            }
        ));
        assert!(matches!(
            shared_key,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::SharedKey { .. },
                ..
            }
        ));
        assert!(matches!(
            sas,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::SasToken { .. },
                ..
            }
        ));
        assert!(matches!(
            conn,
            CloudProviderConfig::AzureBlob {
                account,
                credential: AzureCredentialSource::ConnectionString { .. },
                ..
            } if account == "account"
        ));
    }

    #[test]
    fn should_create_gcs_configs_with_matching_api_styles() {
        // Arrange
        let adc = CloudProviderConfig::gcs("bucket");
        let hmac = CloudProviderConfig::gcs_hmac("bucket", "access", "secret");
        let bearer = CloudProviderConfig::gcs_bearer_token("bucket", "token");

        // Act
        // Assert
        assert!(matches!(
            adc,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::ApplicationDefault,
                ..
            }
        ));
        assert!(matches!(
            hmac,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Xml,
                credential: GcsCredentialSource::HmacKey { .. },
                ..
            }
        ));
        assert!(matches!(
            bearer,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::BearerToken { .. },
                ..
            }
        ));
    }

    #[test]
    fn should_apply_fluent_cloud_modifiers() {
        // Arrange
        let s3 = CloudProviderConfig::s3_compatible_env("bucket", "http://old")
            .with_endpoint("http://new")
            .expect("endpoint override")
            .with_s3_region("eu-west-1")
            .expect("region override")
            .with_path_style(false)
            .expect("path-style override")
            .with_s3_credentials(S3CredentialSource::access_key("key", "secret"))
            .expect("s3 credentials");
        let gcs = CloudProviderConfig::gcs_hmac("bucket", "access", "secret")
            .with_gcs_credentials(GcsCredentialSource::application_default())
            .expect("gcs credentials");

        // Act
        // Assert
        assert_eq!(
            s3,
            CloudProviderConfig::S3Compatible {
                bucket: "bucket".to_string(),
                region: "eu-west-1".to_string(),
                endpoint: "http://new".to_string(),
                path_style: false,
                credentials: S3CredentialSource::access_key("key", "secret"),
            }
        );
        assert!(matches!(
            gcs,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::ApplicationDefault,
                ..
            }
        ));
    }

    #[test]
    fn should_reject_mismatched_cloud_credentials() {
        // Arrange
        let result = CloudProviderConfig::gcs("bucket")
            .try_with_credentials(S3CredentialSource::access_key("key", "secret"));

        // Act
        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_unsupported_cloud_modifiers() {
        // Arrange
        // Act
        // Assert
        assert!(CloudProviderConfig::aws_s3("bucket", "us-east-1")
            .with_endpoint("http://localhost:9000")
            .is_err());
        assert!(CloudProviderConfig::gcs("bucket")
            .with_path_style(true)
            .is_err());
        assert!(
            CloudProviderConfig::minio_static("bucket", "http://minio:9000", "key", "secret")
                .with_s3_region("us-west-2")
                .is_err()
        );
    }

    #[test]
    fn should_parse_azure_account_from_connection_string_config() {
        // Arrange
        let provider = CloudProviderConfig::azure_blob_connection_string(
            "container",
            "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key",
        );

        // Act
        // Assert
        assert!(matches!(
            provider,
            CloudProviderConfig::AzureBlob { account, .. } if account == "myaccount"
        ));
    }

    #[test]
    fn should_reject_connection_string_credential_override_without_account() {
        // Arrange
        let provider = CloudProviderConfig::AzureBlob {
            account: String::new(),
            container: "container".to_string(),
            endpoint: None,
            credential: AzureCredentialSource::connection_string("AccountKey=key"),
        };

        let result = provider.with_azure_credentials(AzureCredentialSource::shared_key("key"));

        // Act
        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_create_workload_identity_credentials_without_none_annotations() {
        // Arrange
        let client = AzureCredentialSource::workload_identity_for_client("client-id");
        let file = AzureCredentialSource::workload_identity_from_file("/var/run/token");
        let full = AzureCredentialSource::workload_identity_with(
            Some("tenant-id".to_string()),
            None,
            Some(PathBuf::from("/var/run/token")),
        );

        // Act
        // Assert
        assert!(matches!(
            client,
            AzureCredentialSource::WorkloadIdentity {
                client_id: Some(_),
                ..
            }
        ));
        assert!(matches!(
            file,
            AzureCredentialSource::WorkloadIdentity {
                token_file: Some(_),
                ..
            }
        ));
        assert!(matches!(
            full,
            AzureCredentialSource::WorkloadIdentity {
                tenant_id: Some(_),
                client_id: None,
                token_file: Some(_),
            }
        ));
    }

    #[test]
    fn should_clamp_memtable_for_small_explicit_budget() {
        // Arrange
        let budget = 64 * 1024 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .memory_budget(MemoryBudget::Bytes(budget))
            .build();

        // Assert
        assert_eq!(opts.derived_memory_budget, budget);
        assert!(opts.memtable_size_limit() <= budget / 2);
        assert!(opts.block_cache_size() <= budget);
    }

    #[test]
    fn should_use_explicit_memtable_size_for_flush_threshold_when_only_size_override_is_set() {
        // Arrange
        let size_limit = 128 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(size_limit)
            .build();

        // Assert
        assert_eq!(opts.memtable_size_limit(), size_limit);
        assert_eq!(opts.memtable_flush_threshold, size_limit);
    }

    #[test]
    fn should_preserve_explicit_memtable_limits_when_both_are_set() {
        // Arrange
        let size_limit = 256 * 1024;
        let flush_threshold = 128 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(size_limit)
            .with_memtable_flush_threshold(flush_threshold)
            .build();

        // Assert
        assert_eq!(opts.memtable_size_limit(), size_limit);
        assert_eq!(opts.memtable_flush_threshold, flush_threshold);
    }

    #[test]
    fn should_clamp_zero_memtable_overrides_to_one() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(0)
            .with_memtable_flush_threshold(0)
            .build();

        // Assert
        assert_eq!(opts.memtable_size_limit(), 1);
        assert_eq!(opts.memtable_flush_threshold, 1);
    }

    // ========== OpenOptions Builder Tests ==========

    #[test]
    fn should_create_in_memory_options() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory();

        // Assert
        assert_eq!(opts.storage, Storage::InMemory);
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.memory_budget, MemoryBudget::Auto);
        assert_eq!(opts.workload, WorkloadProfile::Mixed);
    }

    #[test]
    fn should_create_local_options_with_path() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::local("./test_db");

        // Assert
        assert_eq!(
            opts.storage,
            Storage::Local {
                path: PathBuf::from("./test_db")
            }
        );
    }

    #[test]
    fn should_set_goal_when_calling_goal() {
        // Arrange
        // Act
        let opts = OpenOptions::in_memory().goal(Goal::Throughput);

        // Assert
        assert_eq!(opts.goal, Goal::Throughput);
    }

    #[test]
    fn should_set_memory_budget_when_calling_memory_budget() {
        // Arrange
        let budget = MemoryBudget::Bytes(2 * 1024 * 1024 * 1024);

        // Act
        let opts = OpenOptions::in_memory().memory_budget(budget);

        // Assert
        assert_eq!(opts.memory_budget, budget);
    }

    #[test]
    fn should_set_workload_when_calling_workload() {
        let opts = OpenOptions::in_memory().workload(WorkloadProfile::WriteHeavy);
        assert_eq!(opts.workload, WorkloadProfile::WriteHeavy);
    }

    #[test]
    fn should_support_fluent_builder_chain() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::local("./db")
            .goal(Goal::Latency)
            .workload(WorkloadProfile::ReadMostly)
            .build();

        // Assert
        assert_eq!(
            opts.storage,
            Storage::Local {
                path: PathBuf::from("./db")
            }
        );
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.workload, WorkloadProfile::ReadMostly);
    }

    #[test]
    fn should_derive_parameters_when_building() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory().goal(Goal::Latency).build();

        // Assert
        assert!(opts.block_size > 0);
        assert!(opts.memtable_size_limit > 0);
        assert!(opts.target_sst_size > 0);
        assert!(opts.block_cache_size > 0);
    }

    #[test]
    fn should_use_different_block_sizes_for_different_goals() {
        // Arrange
        // (no setup required)

        // Act
        let latency_opts = OpenOptions::in_memory().goal(Goal::Latency).build();
        let throughput_opts = OpenOptions::in_memory().goal(Goal::Throughput).build();

        // Assert
        assert_ne!(latency_opts.block_size, throughput_opts.block_size);
    }

    #[test]
    fn should_use_different_memtable_sizes_for_different_workloads() {
        // Arrange
        // (no setup required)

        // Act
        let normal = OpenOptions::in_memory()
            .workload(WorkloadProfile::Mixed)
            .build();
        let write_heavy = OpenOptions::in_memory()
            .workload(WorkloadProfile::WriteHeavy)
            .build();

        // Assert
        assert!(write_heavy.memtable_size_limit >= normal.memtable_size_limit);
    }

    #[test]
    fn should_provide_getter_methods() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory().build();

        // Assert - getters should be callable
        let _ = opts.block_size();
        let _ = opts.memtable_size_limit();
        let _ = opts.target_sst_size();
        let _ = opts.block_cache_size();
        let _ = opts.wal_buffer_size();
        let _ = opts.l0_compaction_trigger();
    }

    #[test]
    fn should_respect_explicit_memory_budget() {
        // Arrange
        // Use a realistic budget larger than 2x memtable size to have cache allocation
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024); // 512MB

        // Act
        let opts = OpenOptions::in_memory().memory_budget(budget).build();

        // Assert
        assert!(opts.block_cache_size > 0);
    }

    #[test]
    fn should_clone_options() {
        // Arrange
        let original = OpenOptions::in_memory().goal(Goal::Throughput);

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.goal, original.goal);
    }
}
