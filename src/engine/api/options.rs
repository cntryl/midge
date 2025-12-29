//! Database Configuration Options
//!
//! Smart configuration system with **automatic parameter derivation**.
//!
//! # Design Philosophy
//!
//! Instead of exposing hundreds of low-level tuning knobs, Midge asks **three core questions**:
//!
//! 1. **What's the performance goal?** (`Goal::Latency` | `Goal::Throughput` | `Goal::Cost`)
//! 2. **What durability guarantee?** (`Durability::Strict` | `Durability::Steady` | `Durability::CloudPersisted`)
//! 3. **How much memory?** (`MemoryBudget::Auto` | `MemoryBudget::Bytes(n)`)
//!
//! All other parameters (block sizes, buffer sizes, compaction triggers, cache allocation, etc.)
//! are **derived automatically** from these three inputs plus optional workload hints.
//!
//! # Example
//!
//! ```rust,no_run
//! use cntryl_midge::MidgeEngine;
//! use std::path::PathBuf;
//!
//! // Open a database with default options
//! let engine = MidgeEngine::open(PathBuf::from("./my_db"))?;
//! # Ok::<(), cntryl_midge::MidgeError>(())
//! ```

use std::path::PathBuf;

use crate::common::AckPolicy;

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

/// Durability guarantee level.
///
/// Determines when writes are considered durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
    CloudPersisted,
}

/// Memory budget specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Database path
    pub path: PathBuf,

    /// Performance goal
    pub goal: Goal,

    /// Durability level
    pub durability: Durability,

    /// Derived acknowledgment policy (internal).
    ///
    /// Users choose durability; the system derives acknowledgment semantics.
    pub(crate) ack_policy: AckPolicy,

    /// Memory budget
    pub memory_budget: MemoryBudget,

    /// Workload profile hint
    pub workload: WorkloadProfile,

    // Derived parameters (populated by build())
    /// Block size in bytes (derived)
    pub(crate) block_size: usize,

    /// Memtable size limit (derived)
    pub(crate) memtable_size_limit: usize,

    /// Target SST file size (derived)
    pub(crate) target_sst_size: usize,

    /// Block cache size (derived)
    pub(crate) block_cache_size: usize,

    /// WAL sync on every write (derived)
    pub(crate) wal_sync_on_write: bool,

    /// WAL buffer size (derived)
    pub(crate) wal_buffer_size: usize,

    /// L0 compaction trigger (derived)
    pub(crate) l0_compaction_trigger: usize,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// Create new options with default values.
    ///
    /// Defaults:
    /// - Goal: Latency
    /// - Durability: Steady
    /// - Memory: Auto (50% of system RAM)
    /// - Workload: Mixed
    pub fn new() -> Self {
        Self {
            path: PathBuf::from("./midge_db"),
            goal: Goal::default(),
            durability: Durability::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            // Defaults align with Durability::Steady.
            ack_policy: AckPolicy::Immediate,
            // Temporary defaults until build() derives them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            wal_sync_on_write: false,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
        }
    }

    /// Set database path.
    pub fn path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.path = path.into();
        self
    }

    /// Set performance goal.
    pub fn goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    /// Set durability level.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
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
            MemoryBudget::Auto => {
                // Use 50% of available system memory
                // TODO: Query actual system memory
                512 * 1024 * 1024 // Default to 512MB for now
            }
            MemoryBudget::Bytes(n) => n,
        };

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

        // Derive WAL settings
        self.wal_sync_on_write = matches!(self.durability, Durability::Strict);

        // Derive acknowledgment semantics from durability.
        //
        // Principle: users choose durability; the system chooses ack semantics.
        self.ack_policy = match self.durability {
            Durability::Strict => AckPolicy::AfterLocalDurable,
            Durability::Steady => AckPolicy::Immediate,
            Durability::CloudPersisted => AckPolicy::Immediate,
        };
        self.wal_buffer_size = match self.goal {
            Goal::Latency => 128 * 1024,     // 128KB
            Goal::Throughput => 1024 * 1024, // 1MB
            Goal::Economy => 256 * 1024,     // 256KB
        };

        // Derive compaction trigger
        self.l0_compaction_trigger = match (self.goal, self.workload) {
            (Goal::Latency, _) => 3,               // Aggressive
            (_, WorkloadProfile::WriteHeavy) => 8, // Relaxed for write-heavy
            (Goal::Throughput, _) => 6,            // Moderate
            _ => 4,                                // Default
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

    /// Check if WAL should sync on every write
    pub fn wal_sync_on_write(&self) -> bool {
        self.wal_sync_on_write
    }

    /// Get the derived acknowledgment policy.
    ///
    /// This is derived from `durability` during `build()`.
    pub fn ack_policy(&self) -> AckPolicy {
        self.ack_policy
    }

    /// Get derived WAL buffer size
    pub fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    /// Get derived L0 compaction trigger
    pub fn l0_compaction_trigger(&self) -> usize {
        self.l0_compaction_trigger
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

    // ========== Durability Enum Tests ==========

    #[test]
    fn should_have_steady_as_default_durability() {
        assert_eq!(Durability::default(), Durability::Steady);
    }

    #[test]
    fn should_create_strict_durability() {
        assert_eq!(Durability::Strict, Durability::Strict);
    }

    #[test]
    fn should_create_cloud_persisted_durability() {
        assert_eq!(Durability::CloudPersisted, Durability::CloudPersisted);
    }

    #[test]
    fn should_distinguish_different_durabilities() {
        assert_ne!(Durability::Strict, Durability::Steady);
        assert_ne!(Durability::Steady, Durability::CloudPersisted);
        assert_ne!(Durability::CloudPersisted, Durability::Strict);
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
        assert_ne!(WorkloadProfile::Mixed, WorkloadProfile::WriteHeavy);
        assert_ne!(WorkloadProfile::WriteHeavy, WorkloadProfile::ReadMostly);
        assert_ne!(WorkloadProfile::ReadMostly, WorkloadProfile::RangeScan);
        assert_ne!(WorkloadProfile::RangeScan, WorkloadProfile::TtlHeavy);
    }

    // ========== OpenOptions Builder Tests ==========

    #[test]
    fn should_create_options_with_defaults() {
        let opts = OpenOptions::new();
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.durability, Durability::Steady);
        assert_eq!(opts.memory_budget, MemoryBudget::Auto);
        assert_eq!(opts.workload, WorkloadProfile::Mixed);
    }

    #[test]
    fn should_set_path_when_calling_path() {
        let opts = OpenOptions::new().path("./test_db");
        assert_eq!(opts.path, PathBuf::from("./test_db"));
    }

    #[test]
    fn should_set_goal_when_calling_goal() {
        let opts = OpenOptions::new().goal(Goal::Throughput);
        assert_eq!(opts.goal, Goal::Throughput);
    }

    #[test]
    fn should_set_durability_when_calling_durability() {
        let opts = OpenOptions::new().durability(Durability::Strict);
        assert_eq!(opts.durability, Durability::Strict);
    }

    #[test]
    fn should_set_memory_budget_when_calling_memory_budget() {
        let budget = MemoryBudget::Bytes(2 * 1024 * 1024 * 1024);
        let opts = OpenOptions::new().memory_budget(budget);
        assert_eq!(opts.memory_budget, budget);
    }

    #[test]
    fn should_set_workload_when_calling_workload() {
        let opts = OpenOptions::new().workload(WorkloadProfile::WriteHeavy);
        assert_eq!(opts.workload, WorkloadProfile::WriteHeavy);
    }

    #[test]
    fn should_support_fluent_builder_chain() {
        let opts = OpenOptions::new()
            .path("./db")
            .goal(Goal::Latency)
            .durability(Durability::Strict)
            .workload(WorkloadProfile::ReadMostly)
            .build();

        assert_eq!(opts.path, PathBuf::from("./db"));
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.durability, Durability::Strict);
        assert_eq!(opts.workload, WorkloadProfile::ReadMostly);
    }

    #[test]
    fn should_derive_parameters_when_building() {
        let opts = OpenOptions::new().goal(Goal::Latency).build();

        assert!(opts.block_size > 0);
        assert!(opts.memtable_size_limit > 0);
        assert!(opts.target_sst_size > 0);
        assert!(opts.block_cache_size > 0);
    }

    #[test]
    fn should_set_wal_sync_for_strict_durability() {
        let opts = OpenOptions::new().durability(Durability::Strict).build();

        assert!(opts.wal_sync_on_write);
    }

    #[test]
    fn should_not_set_wal_sync_for_steady_durability() {
        let opts = OpenOptions::new().durability(Durability::Steady).build();

        assert!(!opts.wal_sync_on_write);
    }

    #[test]
    fn should_derive_ack_policy_after_local_durable_for_strict() {
        let opts = OpenOptions::new().durability(Durability::Strict).build();
        assert_eq!(opts.ack_policy, AckPolicy::AfterLocalDurable);
    }

    #[test]
    fn should_derive_ack_policy_immediate_for_steady() {
        let opts = OpenOptions::new().durability(Durability::Steady).build();
        assert_eq!(opts.ack_policy, AckPolicy::Immediate);
    }

    #[test]
    fn should_derive_ack_policy_immediate_for_cloud_persisted() {
        let opts = OpenOptions::new()
            .durability(Durability::CloudPersisted)
            .build();
        assert_eq!(opts.ack_policy, AckPolicy::Immediate);
    }

    #[test]
    fn should_use_different_block_sizes_for_different_goals() {
        let latency_opts = OpenOptions::new().goal(Goal::Latency).build();
        let throughput_opts = OpenOptions::new().goal(Goal::Throughput).build();

        assert_ne!(latency_opts.block_size, throughput_opts.block_size);
    }

    #[test]
    fn should_use_different_memtable_sizes_for_different_workloads() {
        let normal = OpenOptions::new().workload(WorkloadProfile::Mixed).build();
        let write_heavy = OpenOptions::new()
            .workload(WorkloadProfile::WriteHeavy)
            .build();

        assert!(write_heavy.memtable_size_limit >= normal.memtable_size_limit);
    }

    #[test]
    fn should_derive_parameters_from_default() {
        let opts = OpenOptions::default().build();

        assert_eq!(opts.goal, Goal::Latency);
        assert!(opts.block_size > 0);
        assert!(opts.memtable_size_limit > 0);
    }

    #[test]
    fn should_provide_getter_methods() {
        let opts = OpenOptions::new().build();

        let _ = opts.block_size();
        let _ = opts.memtable_size_limit();
        let _ = opts.target_sst_size();
        let _ = opts.block_cache_size();
        let _ = opts.wal_sync_on_write();
        let _ = opts.wal_buffer_size();
        let _ = opts.l0_compaction_trigger();
    }

    #[test]
    fn should_handle_path_conversion() {
        let opts = OpenOptions::new().path("/tmp/db");
        assert_eq!(opts.path, PathBuf::from("/tmp/db"));
    }

    #[test]
    fn should_respect_explicit_memory_budget() {
        // Use a realistic budget larger than 2x memtable size to have cache allocation
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024); // 512MB
        let opts = OpenOptions::new().memory_budget(budget).build();

        // With explicit budget, cache size should be derived from it
        assert!(opts.block_cache_size > 0);
    }

    #[test]
    fn should_clone_options() {
        let original = OpenOptions::new()
            .goal(Goal::Throughput)
            .durability(Durability::Strict);
        let cloned = original.clone();

        assert_eq!(cloned.goal, original.goal);
        assert_eq!(cloned.durability, original.durability);
    }
}
