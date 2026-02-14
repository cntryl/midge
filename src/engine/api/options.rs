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

use crate::sst::compression::{CompressionAlgo, CompressionPolicy};

/// Storage backend specification - MUST be explicit
///
/// This enum enforces unambiguous storage selection. There are NO defaults,
/// NO inference, and NO magic switching between backends.
///
/// Each variant clearly answers: "Where does this database live?"
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    /// In-memory storage - no persistence
    ///
    /// Data is lost when the engine is dropped or process exits.
    /// Use for: testing, caching, ephemeral workloads
    InMemory,

    /// Local filesystem storage
    ///
    /// Data persists to local disk at the specified path.
    /// Use for: traditional deployments, single-node databases
    Local {
        /// Filesystem path to database directory
        path: PathBuf,
    },

    /// Cloud object storage (provider-agnostic)
    ///
    /// Data persists to cloud object storage. Supports:
    /// - AWS S3
    /// - Azure Blob Storage  
    /// - Google Cloud Storage
    /// - Cloudflare R2
    /// - MinIO and other S3-compatible services
    /// - Any object storage with appropriate credentials
    ///
    /// Uses a hybrid model with local cache for performance.
    ///
    /// Use for: cloud-native deployments, serverless, distributed systems
    Cloud {
        /// Local cache/staging path for performance
        local_cache_path: PathBuf,
        /// Bucket/container name (provider-specific terminology)
        bucket: String,
        /// Object key prefix (e.g., "databases/myapp/")
        prefix: String,
        /// Optional endpoint override (for custom cloud providers or regional endpoints)
        endpoint: Option<String>,
        /// Optional region/location override
        region: Option<String>,
    },
}

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

/// Database open options with smart defaults.
///
/// Use the builder pattern to configure high-level knobs, and all low-level
/// parameters will be derived automatically.
///
/// Storage backend MUST be explicitly specified via constructors:
/// - OpenOptions::in_memory()
/// - OpenOptions::local(path)
/// - OpenOptions::cloud(cache_path, bucket, prefix)
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

    /// Derived memory budget in bytes (from build())
    pub(crate) derived_memory_budget: usize,

    // Derived parameters (populated by build())
    /// Block size in bytes (derived)
    pub(crate) block_size: usize,

    /// Memtable size limit (derived)
    pub(crate) memtable_size_limit: usize,

    /// Target SST file size (derived)
    pub(crate) target_sst_size: usize,

    /// Block cache size (derived)
    pub(crate) block_cache_size: usize,

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
    pub fn in_memory() -> Self {
        Self {
            storage: Storage::InMemory,
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
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
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
        }
    }

    /// Create cloud-backed database instance
    ///
    /// Data persists to cloud object storage (S3, Azure, GCS, R2, etc.).
    /// Uses hybrid model with local cache for performance.
    /// Ideal for: cloud-native deployments, serverless, distributed systems
    ///
    /// # Arguments
    /// * `local_cache_path` - Local directory for caching/staging
    /// * `bucket` - Cloud bucket/container name
    /// * `prefix` - Object key prefix
    pub fn cloud<P: Into<PathBuf>, S: Into<String>>(
        local_cache_path: P,
        bucket: S,
        prefix: S,
    ) -> Self {
        Self {
            storage: Storage::Cloud {
                local_cache_path: local_cache_path.into(),
                bucket: bucket.into(),
                prefix: prefix.into(),
                endpoint: None,
                region: None,
            },
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
        }
    }

    /// Set performance goal.
    pub fn goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    /// Set memory budget.
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = budget;
        self
    }

    /// Set workload profile hint.
    pub fn workload(mut self, profile: WorkloadProfile) -> Self {
        self.workload = profile;
        self
    }

    /// Build options with derived parameters.
    ///
    /// This automatically computes all low-level parameters based on the
    /// high-level knobs (goal, durability, memory, workload).
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

        // Derive target SST size
        self.target_sst_size = match self.goal {
            Goal::Latency => 128 * 1024 * 1024,    // 128MB
            Goal::Throughput => 512 * 1024 * 1024, // 512MB
            Goal::Economy => 256 * 1024 * 1024,    // 256MB
        };

        // Allocate remaining memory to block cache
        let cache_ratio = match self.workload {
            WorkloadProfile::ReadMostly => 0.7, // 70% to cache
            WorkloadProfile::WriteHeavy => 0.2, // 20% to cache
            _ => 0.5,                           // 50% to cache
        };

        let usable_memory = total_memory.saturating_sub(self.memtable_size_limit * 2); // 2 memtables
        self.block_cache_size = ((usable_memory as f64) * cache_ratio) as usize;

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
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Get derived memtable size limit
    pub fn memtable_size_limit(&self) -> usize {
        self.memtable_size_limit
    }

    /// Get derived target SST size
    pub fn target_sst_size(&self) -> usize {
        self.target_sst_size
    }

    /// Get derived block cache size
    pub fn block_cache_size(&self) -> usize {
        self.block_cache_size
    }

    /// Get derived WAL buffer size
    pub fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    /// Get derived L0 compaction trigger
    pub fn l0_compaction_trigger(&self) -> usize {
        self.l0_compaction_trigger
    }

    /// Get derived compression policy
    pub fn compression_policy(&self) -> &CompressionPolicy {
        &self.compression_policy
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

/// Durability level for runtime use
///
/// NOTE: This enum is for INTERNAL runtime durability tracking only.
/// It should NOT be exposed in OpenOptions or any user-facing configuration.
/// Write-time durability decisions use WriteOptions::DurabilityPolicy instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Strict - fsync on every write
    Strict,
    /// Steady - fsync every N ms
    Steady,
    /// CloudPersisted - wait for cloud backup
    CloudPersisted,
}
