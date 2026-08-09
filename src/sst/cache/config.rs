use super::CachePolicyType;

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

/// Finalized SST block-cache configuration owned by the cache subsystem.
#[derive(Debug, Clone)]
pub(crate) struct CachePolicyConfig {
    pub(crate) block_size: usize,
    pub(crate) capacity_bytes: usize,
    pub(crate) policy: BlockCachePolicy,
}

impl CachePolicyConfig {
    pub(crate) fn new(block_size: usize, capacity_bytes: usize, policy: BlockCachePolicy) -> Self {
        Self {
            block_size,
            capacity_bytes,
            policy,
        }
    }
}
