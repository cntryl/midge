//! Workload profile definitions and adjustments.
//!
//! Each profile modifies the baseline derived configuration to optimize
//! for specific access patterns.

use super::{derivation::DerivedParams, WorkloadProfile};

/// Profile-specific adjustments to derived parameters.
pub struct ProfileAdjustments {
    /// Memtable size multiplier (1.0 = no change).
    pub memtable_multiplier: f64,

    /// Block cache size multiplier (1.0 = no change).
    pub cache_multiplier: f64,

    /// Bloom filter bits per key adjustment (additive).
    pub bloom_bits_adjustment: i32,

    /// Block size multiplier (1.0 = no change).
    pub block_size_multiplier: f64,

    /// Compaction concurrency adjustment (additive).
    pub compaction_threads_adjustment: i32,
}

impl ProfileAdjustments {
    /// Get adjustments for a specific workload profile.
    pub fn for_profile(profile: WorkloadProfile) -> Self {
        match profile {
            WorkloadProfile::Mixed => Self {
                memtable_multiplier: 1.0,
                cache_multiplier: 1.0,
                bloom_bits_adjustment: 0,
                block_size_multiplier: 1.0,
                compaction_threads_adjustment: 0,
            },

            WorkloadProfile::WriteHeavy => Self {
                // Larger memtables to amortize flush cost
                memtable_multiplier: 1.5,
                // Lower cache allocation (writes don't benefit from cache)
                cache_multiplier: 0.7,
                // Fewer bloom bits (writes don't use blooms)
                bloom_bits_adjustment: -2,
                // Larger blocks for better write throughput
                block_size_multiplier: 1.25,
                // More compaction threads to keep up with writes
                compaction_threads_adjustment: 2,
            },

            WorkloadProfile::ReadMostly => Self {
                // Smaller memtables (fewer writes)
                memtable_multiplier: 0.75,
                // Larger cache for read performance
                cache_multiplier: 1.3,
                // More bloom bits to reduce false positives
                bloom_bits_adjustment: 2,
                // Standard block size
                block_size_multiplier: 1.0,
                // Fewer compaction threads (less write activity)
                compaction_threads_adjustment: -1,
            },

            WorkloadProfile::RangeScan => Self {
                // Standard memtables
                memtable_multiplier: 1.0,
                // Higher cache for sequential reads
                cache_multiplier: 1.2,
                // Bloom filters not useful for range scans
                bloom_bits_adjustment: -4,
                // Larger blocks for sequential access
                block_size_multiplier: 2.0,
                // Standard compaction
                compaction_threads_adjustment: 0,
            },

            WorkloadProfile::TtlHeavy => Self {
                // Standard memtables
                memtable_multiplier: 1.0,
                // Standard cache
                cache_multiplier: 1.0,
                // Standard bloom filters
                bloom_bits_adjustment: 0,
                // Standard block size
                block_size_multiplier: 1.0,
                // More compaction threads for tombstone cleanup
                compaction_threads_adjustment: 2,
            },

            WorkloadProfile::TinyKeys => Self {
                // Can fit more entries per memtable
                memtable_multiplier: 0.8,
                // Standard cache
                cache_multiplier: 1.0,
                // More bloom bits (more entries = more false positives)
                bloom_bits_adjustment: 1,
                // Smaller blocks for tiny keys
                block_size_multiplier: 0.5,
                // Standard compaction
                compaction_threads_adjustment: 0,
            },

            WorkloadProfile::LargeValues => Self {
                // Larger memtables for large values
                memtable_multiplier: 1.5,
                // Standard cache (values dominate memory)
                cache_multiplier: 1.0,
                // Standard bloom filters
                bloom_bits_adjustment: 0,
                // Larger blocks for large values
                block_size_multiplier: 2.0,
                // Standard compaction
                compaction_threads_adjustment: 0,
            },
        }
    }

    /// Apply adjustments to derived parameters.
    pub fn apply(&self, params: &mut DerivedParams) {
        // Apply memtable multiplier
        params.memtable_size = (params.memtable_size as f64 * self.memtable_multiplier) as usize;

        // Apply cache multiplier
        params.block_cache_size = (params.block_cache_size as f64 * self.cache_multiplier) as usize;

        // Apply bloom bits adjustment
        params.bloom_bits_per_key =
            (params.bloom_bits_per_key as i32 + self.bloom_bits_adjustment).clamp(4, 20) as u32;

        // Apply block size multiplier
        params.block_size = (params.block_size as f64 * self.block_size_multiplier) as usize;

        // Apply compaction threads adjustment
        params.compaction_concurrency = (params.compaction_concurrency as i32
            + self.compaction_threads_adjustment)
            .clamp(1, 16) as usize;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_configure_write_heavy_profile() {
        // Arrange
        let profile = WorkloadProfile::WriteHeavy;

        // Act
        let adj = ProfileAdjustments::for_profile(profile);

        // Assert
        assert!(adj.memtable_multiplier > 1.0);
        assert!(adj.cache_multiplier < 1.0);
        assert!(adj.bloom_bits_adjustment < 0);
    }

    #[test]
    fn should_configure_read_mostly_profile() {
        // Arrange
        let profile = WorkloadProfile::ReadMostly;

        // Act
        let adj = ProfileAdjustments::for_profile(profile);

        // Assert
        assert!(adj.cache_multiplier > 1.0);
        assert!(adj.bloom_bits_adjustment > 0);
    }

    #[test]
    fn should_configure_range_scan_profile() {
        // Arrange
        let profile = WorkloadProfile::RangeScan;

        // Act
        let adj = ProfileAdjustments::for_profile(profile);

        // Assert
        assert!(adj.block_size_multiplier > 1.0);
        assert!(adj.bloom_bits_adjustment < 0); // Blooms not useful for ranges
    }
}
