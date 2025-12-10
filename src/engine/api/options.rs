//! Database Configuration Options
//!
//! Smart configuration system with **automatic parameter derivation**.
//!
//! # Design Philosophy
//!
//! Instead of exposing hundreds of low-level tuning knobs, Midge asks **three core questions**:
//!
//! 1. **What's the performance goal?** (`Goal::Latency` | `Goal::Throughput` | `Goal::Cost`)
//! 2. **What durability guarantee?** (`Durability::Strict` | `Durability::Steady` | `Durability::CloudReplicated`)
//! 3. **How much memory?** (`MemoryBudget::Auto` | `MemoryBudget::Bytes(n)`)
//!
//! All other parameters (block sizes, buffer sizes, compaction triggers, cache allocation, etc.)
//! are **derived automatically** from these three inputs plus optional workload hints.
//!
//! # Example
//!
//! ```rust,no_run
//! use cntryl_midge::{OpenOptions, Goal, Durability};
//!
//! // Simple: just specify path and goal
//! let opts = OpenOptions::new()
//!     .path("./my_db")
//!     .goal(Goal::Latency)
//!     .build();
//!
//! // Advanced: tune for specific workload
//! let opts = OpenOptions::new()
//!     .path("./my_db")
//!     .goal(Goal::Throughput)
//!     .durability(Durability::Steady)
//!     .memory_budget(MemoryBudget::Bytes(4 * 1024 * 1024 * 1024)) // 4GB
//!     .workload(WorkloadProfile::WriteHeavy)
//!     .build();
//! ```

use std::path::PathBuf;

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
    Cost,
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
    CloudReplicated,
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
            (Goal::Latency, _) => 16 * 1024,           // 16KB for low latency
            (Goal::Cost, _) => 32 * 1024,              // 32KB balanced
            (Goal::Throughput, WorkloadProfile::RangeScan) => 128 * 1024, // 128KB for bulk scans
            (Goal::Throughput, _) => 64 * 1024,        // 64KB for throughput
        };

        // Derive memtable size based on goal and workload
        let base_memtable = match self.goal {
            Goal::Latency => 64 * 1024 * 1024,    // 64MB for latency
            Goal::Throughput => 256 * 1024 * 1024, // 256MB for throughput
            Goal::Cost => 32 * 1024 * 1024,       // 32MB for cost
        };

        self.memtable_size_limit = match self.workload {
            WorkloadProfile::WriteHeavy => base_memtable * 2, // Double for write-heavy
            WorkloadProfile::ReadMostly => base_memtable / 2, // Half for read-heavy (more cache)
            _ => base_memtable,
        };

        // Derive target SST size
        self.target_sst_size = match self.goal {
            Goal::Latency => 128 * 1024 * 1024,   // 128MB
            Goal::Throughput => 512 * 1024 * 1024, // 512MB
            Goal::Cost => 256 * 1024 * 1024,       // 256MB
        };

        // Allocate remaining memory to block cache
        let cache_ratio = match self.workload {
            WorkloadProfile::ReadMostly => 0.7,  // 70% to cache
            WorkloadProfile::WriteHeavy => 0.2,  // 20% to cache
            _ => 0.5,                             // 50% to cache
        };
        
        let usable_memory = total_memory.saturating_sub(self.memtable_size_limit * 2); // 2 memtables
        self.block_cache_size = ((usable_memory as f64) * cache_ratio) as usize;

        // Derive WAL settings
        self.wal_sync_on_write = matches!(self.durability, Durability::Strict);
        self.wal_buffer_size = match self.goal {
            Goal::Latency => 128 * 1024,    // 128KB
            Goal::Throughput => 1024 * 1024, // 1MB
            Goal::Cost => 256 * 1024,       // 256KB
        };

        // Derive compaction trigger
        self.l0_compaction_trigger = match (self.goal, self.workload) {
            (Goal::Latency, _) => 3,                        // Aggressive
            (_, WorkloadProfile::WriteHeavy) => 8,          // Relaxed for write-heavy
            (Goal::Throughput, _) => 6,                     // Moderate
            _ => 4,                                         // Default
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

    /// Get derived WAL buffer size
    pub fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    /// Get derived L0 compaction trigger
    pub fn l0_compaction_trigger(&self) -> usize {
        self.l0_compaction_trigger
    }
}
