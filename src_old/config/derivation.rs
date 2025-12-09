//! Parameter derivation from high-level configuration knobs.
//!
//! This module implements the deterministic derivation logic described
//! in the configuration specification.

use std::time::Duration;

use super::{ConfigPlan, Durability, Goal, MemoryBudget, WorkloadProfile};

/// Intermediate derived parameters before final validation.
#[derive(Debug, Clone)]
pub struct DerivedParams {
    // Memory
    pub total_memory_budget: usize,
    pub block_cache_size: usize,
    pub memtable_size: usize,
    pub memtable_count: usize,
    pub overhead_budget: usize,

    // Storage
    pub block_size: usize,
    pub bloom_bits_per_key: u32,
    pub target_sst_size_l1: usize,
    pub level_multiplier: usize,
    pub max_levels: usize,

    // Compaction
    pub l0_compaction_trigger: usize,
    pub compaction_concurrency: usize,

    // WAL
    pub wal_sync_per_write: bool,
    pub wal_sync_interval: Option<Duration>,
    pub wal_buffer_size: usize,
}

impl DerivedParams {
    /// Derive parameters from high-level configuration.
    pub fn derive(
        goal: Goal,
        durability: Durability,
        memory_budget: MemoryBudget,
        _profile: WorkloadProfile,
    ) -> Self {
        // Resolve memory budget
        let total_memory = Self::resolve_memory_budget(memory_budget);

        // Derive memory allocations
        let cache_fraction = Self::cache_fraction_for_goal(goal);
        let block_cache_size = (total_memory as f64 * cache_fraction) as usize;

        let memtable_size = Self::memtable_size_for_goal(goal, total_memory);
        let memtable_count = Self::memtable_count(total_memory, memtable_size);

        // Reserve 8% for overhead (internal buffers, metadata, etc.)
        let overhead_budget = (total_memory as f64 * 0.08) as usize;

        // Derive storage parameters
        let block_size = Self::block_size_for_goal(goal);
        let bloom_bits_per_key = Self::bloom_bits_for_goal(goal);
        let target_sst_size_l1 = Self::target_sst_size_for_goal(goal);
        let level_multiplier = Self::level_multiplier_for_goal(goal);
        let max_levels = 7; // Standard LSM depth

        // Derive compaction parameters
        let l0_compaction_trigger = Self::l0_trigger_for_goal(goal);
        let compaction_concurrency = Self::compaction_threads_for_goal(goal);

        // Derive WAL parameters
        let (wal_sync_per_write, wal_sync_interval) = Self::wal_sync_for_durability(durability);
        let wal_buffer_size = Self::wal_buffer_size();

        Self {
            total_memory_budget: total_memory,
            block_cache_size,
            memtable_size,
            memtable_count,
            overhead_budget,
            block_size,
            bloom_bits_per_key,
            target_sst_size_l1,
            level_multiplier,
            max_levels,
            l0_compaction_trigger,
            compaction_concurrency,
            wal_sync_per_write,
            wal_sync_interval,
            wal_buffer_size,
        }
    }

    /// Convert to ConfigPlan with validation metadata.
    pub fn into_plan(self, validated: bool) -> ConfigPlan {
        let memory_used = self.block_cache_size
            + (self.memtable_size * self.memtable_count)
            + self.overhead_budget;
        let memory_utilization = (memory_used as f64 / self.total_memory_budget as f64).min(1.0);

        ConfigPlan {
            total_memory_budget: self.total_memory_budget,
            block_cache_size: self.block_cache_size,
            memtable_size: self.memtable_size,
            memtable_count: self.memtable_count,
            overhead_budget: self.overhead_budget,
            block_size: self.block_size,
            bloom_bits_per_key: self.bloom_bits_per_key,
            target_sst_size_l1: self.target_sst_size_l1,
            level_multiplier: self.level_multiplier,
            max_levels: self.max_levels,
            l0_compaction_trigger: self.l0_compaction_trigger,
            compaction_concurrency: self.compaction_concurrency,
            wal_sync_per_write: self.wal_sync_per_write,
            wal_sync_interval: self.wal_sync_interval,
            wal_buffer_size: self.wal_buffer_size,
            upload_concurrency: None,
            multipart_chunk_size: None,
            prefetch_depth: None,
            memory_utilization,
            validated,
        }
    }

    /// Resolve memory budget (auto or explicit).
    fn resolve_memory_budget(budget: MemoryBudget) -> usize {
        match budget {
            MemoryBudget::Auto => {
                // Use sysinfo to get available memory
                #[cfg(not(test))]
                {
                    use sysinfo::System;
                    let mut sys = System::new_all();
                    sys.refresh_memory();
                    let available = sys.available_memory() as usize;
                    // Use 50% of available memory
                    available / 2
                }
                #[cfg(test)]
                {
                    // Default for tests
                    512 * 1024 * 1024 // 512 MB
                }
            }
            MemoryBudget::Bytes(bytes) => bytes,
        }
    }

    /// Cache fraction based on goal.
    fn cache_fraction_for_goal(goal: Goal) -> f64 {
        match goal {
            Goal::Latency => 0.45,    // 45% for cache
            Goal::Throughput => 0.35, // 35% for cache, more for memtables
            Goal::Cost => 0.28,       // 28% for cache, minimize memory
        }
    }

    /// Memtable size based on goal and total memory.
    fn memtable_size_for_goal(goal: Goal, total_memory: usize) -> usize {
        let base_size = match goal {
            Goal::Latency => 64 * 1024 * 1024,     // 64 MB
            Goal::Throughput => 256 * 1024 * 1024, // 256 MB
            Goal::Cost => 32 * 1024 * 1024,        // 32 MB
        };

        // Cap at 12% of total memory per memtable
        let max_size = (total_memory as f64 * 0.12) as usize;
        base_size.min(max_size)
    }

    /// Number of memtables to maintain.
    fn memtable_count(_total_memory: usize, _memtable_size: usize) -> usize {
        // 1 active + 2 immutable (being flushed)
        3
    }

    /// Block size based on goal.
    fn block_size_for_goal(goal: Goal) -> usize {
        match goal {
            Goal::Latency => 16 * 1024,    // 16 KiB
            Goal::Throughput => 64 * 1024, // 64 KiB
            Goal::Cost => 32 * 1024,       // 32 KiB
        }
    }

    /// Bloom filter bits per key.
    fn bloom_bits_for_goal(goal: Goal) -> u32 {
        match goal {
            Goal::Latency => 12,    // ~0.5% FPR
            Goal::Throughput => 10, // ~1% FPR
            Goal::Cost => 8,        // ~2% FPR
        }
    }

    /// Target SST size for L1.
    fn target_sst_size_for_goal(goal: Goal) -> usize {
        match goal {
            Goal::Latency => 128 * 1024 * 1024,    // 128 MB
            Goal::Throughput => 512 * 1024 * 1024, // 512 MB
            Goal::Cost => 256 * 1024 * 1024,       // 256 MB
        }
    }

    /// Level size multiplier.
    fn level_multiplier_for_goal(goal: Goal) -> usize {
        match goal {
            Goal::Latency => 8,     // Smaller levels, faster compaction
            Goal::Throughput => 12, // Larger levels, more write throughput
            Goal::Cost => 10,       // Balanced
        }
    }

    /// L0 compaction trigger threshold.
    fn l0_trigger_for_goal(goal: Goal) -> usize {
        match goal {
            Goal::Latency => 4,     // Aggressive compaction
            Goal::Throughput => 16, // Defer compaction
            Goal::Cost => 8,        // Balanced
        }
    }

    /// Compaction thread count based on goal and CPU cores.
    fn compaction_threads_for_goal(goal: Goal) -> usize {
        let cpu_count = num_cpus::get();

        let max_threads = match goal {
            Goal::Latency => 4,
            Goal::Throughput => 8,
            Goal::Cost => 2,
        };

        // Use at most half of available CPUs, capped by goal
        (cpu_count / 2).min(max_threads).max(1)
    }

    /// WAL sync behavior based on durability.
    fn wal_sync_for_durability(durability: Durability) -> (bool, Option<Duration>) {
        match durability {
            Durability::Strict => (true, None), // Sync on every write
            Durability::Steady => (false, Some(Duration::from_millis(20))), // Sync every 20ms
            Durability::CloudReplicated => (true, None), // Local sync + cloud verification
        }
    }

    /// WAL buffer size.
    fn wal_buffer_size() -> usize {
        4 * 1024 * 1024 // 4 MB
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_derive_latency_goal_parameters() {
        // Arrange
        let goal = Goal::Latency;
        let durability = Durability::Steady;
        let budget = MemoryBudget::Bytes(1024 * 1024 * 1024); // 1 GB
        let profile = WorkloadProfile::Mixed;

        // Act
        let params = DerivedParams::derive(goal, durability, budget, profile);

        // Assert
        assert_eq!(params.block_size, 16 * 1024); // 16 KiB
        assert_eq!(params.bloom_bits_per_key, 12);
        assert_eq!(params.l0_compaction_trigger, 4);
        assert_eq!(params.level_multiplier, 8);
    }

    #[test]
    fn should_derive_throughput_goal_parameters() {
        // Arrange
        let goal = Goal::Throughput;
        let durability = Durability::Steady;
        let budget = MemoryBudget::Bytes(1024 * 1024 * 1024); // 1 GB
        let profile = WorkloadProfile::Mixed;

        // Act
        let params = DerivedParams::derive(goal, durability, budget, profile);

        // Assert
        assert_eq!(params.block_size, 64 * 1024); // 64 KiB
        assert_eq!(params.l0_compaction_trigger, 16);
        assert_eq!(params.level_multiplier, 12);
    }

    #[test]
    fn should_derive_cost_goal_parameters() {
        // Arrange
        let goal = Goal::Cost;
        let durability = Durability::Steady;
        let budget = MemoryBudget::Bytes(1024 * 1024 * 1024); // 1 GB
        let profile = WorkloadProfile::Mixed;

        // Act
        let params = DerivedParams::derive(goal, durability, budget, profile);

        // Assert
        assert_eq!(params.block_size, 32 * 1024); // 32 KiB
        assert_eq!(params.bloom_bits_per_key, 8);
        assert!(params.compaction_concurrency <= 2);
    }

    #[test]
    fn should_configure_strict_durability() {
        // Arrange
        let goal = Goal::Latency;
        let durability = Durability::Strict;
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024);
        let profile = WorkloadProfile::Mixed;

        // Act
        let params = DerivedParams::derive(goal, durability, budget, profile);

        // Assert
        assert!(params.wal_sync_per_write);
        assert!(params.wal_sync_interval.is_none());
    }

    #[test]
    fn should_configure_steady_durability() {
        // Arrange
        let goal = Goal::Latency;
        let durability = Durability::Steady;
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024);
        let profile = WorkloadProfile::Mixed;

        // Act
        let params = DerivedParams::derive(goal, durability, budget, profile);

        // Assert
        assert!(!params.wal_sync_per_write);
        assert_eq!(params.wal_sync_interval, Some(Duration::from_millis(20)));
    }

    #[test]
    fn should_allocate_memory_across_components() {
        // Arrange
        let goal = Goal::Latency;
        let durability = Durability::Steady;
        let budget = MemoryBudget::Bytes(1024 * 1024 * 1024); // 1 GB
        let profile = WorkloadProfile::Mixed;

        // Act
        let params = DerivedParams::derive(goal, durability, budget, profile);
        let total_allocated = params.block_cache_size
            + (params.memtable_size * params.memtable_count)
            + params.overhead_budget;

        // Assert
        assert!(total_allocated <= params.total_memory_budget);
        assert!(total_allocated >= (params.total_memory_budget as f64 * 0.70) as usize);
    }
}
