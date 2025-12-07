//! Configuration for the block cache.
//!
//! `BlockCacheOptions` controls capacity, sharding, eviction policy, and
//! accounting behavior.

/// Eviction policy selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// CLOCK-Pro (O(1), scan-resistant, adaptive). Default and recommended.
    #[default]
    Clock,
    /// Windowed TinyLFU (maps to CLOCK-Pro internally).
    WTinyLfu,
    /// Simple LRU (least recently used). O(1) but not scan-resistant.
    Lru,
}

/// How to charge block size against cache capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SizeAccounting {
    /// Charge uncompressed size (reflects memory when block is in use).
    #[default]
    Uncompressed,
    /// Charge compressed size (matches on-disk footprint).
    Compressed,
}

/// Configuration options for the block cache.
#[derive(Debug, Clone)]
pub struct BlockCacheOptions {
    /// Total cache capacity in bytes.
    pub capacity_bytes: usize,

    /// Number of shards (must be a power of two). More shards reduce
    /// contention at the cost of slightly less efficient capacity usage.
    /// Default: 16.
    pub num_shards: usize,

    /// Eviction policy to use. Default: `WTinyLfu`.
    pub eviction_policy: EvictionPolicy,

    /// How to account block sizes against capacity. Default: `Uncompressed`.
    pub size_accounting: SizeAccounting,

    /// If `true`, per-column-family statistics are tracked separately.
    /// Default: `false`.
    pub per_cf_stats: bool,
}

impl Default for BlockCacheOptions {
    fn default() -> Self {
        Self {
            capacity_bytes: 64 * 1024 * 1024, // 64 MiB
            num_shards: 16,
            eviction_policy: EvictionPolicy::default(),
            size_accounting: SizeAccounting::default(),
            per_cf_stats: false,
        }
    }
}

impl BlockCacheOptions {
    /// Create options with the given capacity and default settings.
    pub fn with_capacity(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            ..Default::default()
        }
    }

    /// Builder: set number of shards.
    pub fn num_shards(mut self, n: usize) -> Self {
        // Round up to next power of two.
        self.num_shards = n.next_power_of_two();
        self
    }

    /// Builder: set eviction policy.
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Builder: set size accounting mode.
    pub fn size_accounting(mut self, mode: SizeAccounting) -> Self {
        self.size_accounting = mode;
        self
    }

    /// Builder: enable per-CF stats.
    pub fn per_cf_stats(mut self, enable: bool) -> Self {
        self.per_cf_stats = enable;
        self
    }

    /// Capacity per shard (capacity / num_shards).
    pub fn capacity_per_shard(&self) -> usize {
        self.capacity_bytes / self.num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_use_defaults_given_default_options_when_created() {
        // Arrange
        // (no setup needed)

        // Act
        let opts = BlockCacheOptions::default();

        // Assert
        assert_eq!(opts.capacity_bytes, 64 * 1024 * 1024);
        assert_eq!(opts.num_shards, 16);
        assert_eq!(opts.eviction_policy, EvictionPolicy::Clock);
        assert_eq!(opts.size_accounting, SizeAccounting::Uncompressed);
        assert!(!opts.per_cf_stats);
    }

    #[test]
    fn should_round_shards_to_power_of_two_given_non_power_when_set() {
        let opts = BlockCacheOptions::with_capacity(1024).num_shards(10);
        assert_eq!(opts.num_shards, 16);
    }

    #[test]
    fn should_compute_per_shard_capacity_given_options_when_queried() {
        let opts = BlockCacheOptions::with_capacity(1024).num_shards(4);
        assert_eq!(opts.capacity_per_shard(), 256);
    }
}
