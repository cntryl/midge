//! Cache eviction policies
//!
//! Pluggable eviction policies determining which blocks to evict when cache is full.
//! - **LRU**: Least Recently Used
//! - **TinyLFU**: Frequency + recency (W-TinyLFU)
//! - **CLOCK-Pro**: Strong scan resistance

pub mod lru;
pub mod tinylfu;
pub mod clockpro;

pub use lru::LruPolicy;
pub use tinylfu::TinyLfuPolicy;
pub use clockpro::ClockProPolicy;

use crate::sst::cache::key::CacheKey;

/// Eviction policy trait
pub trait CachePolicy: Send + Sync {
    /// Record an access to a key (for policy state tracking)
    fn on_access(&self, key: CacheKey);

    /// Pick a key to evict (returns None if nothing should be evicted)
    fn pick_victim(&self) -> Option<CacheKey>;

    /// Remove key from policy tracking
    fn remove(&self, key: CacheKey);

    /// Clear all tracked keys
    fn clear(&self);
}

/// Factory for creating cache policies
#[derive(Clone, Copy)]
pub enum CachePolicyType {
    Lru,
    TinyLfu,
    ClockPro,
}

impl CachePolicyType {
    /// Create a policy instance
    pub fn create(&self) -> Box<dyn CachePolicy> {
        match self {
            CachePolicyType::Lru => Box::new(LruPolicy::new()),
            CachePolicyType::TinyLfu => Box::new(TinyLfuPolicy::new()),
            CachePolicyType::ClockPro => Box::new(ClockProPolicy::new()),
        }
    }
}
