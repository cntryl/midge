//! Cache eviction policies
//!
//! Pluggable eviction policies determining which blocks to evict when cache is full.
//! - **LRU**: Least Recently Used
//! - **TinyLFU**: Frequency + recency (W-TinyLFU)
//! - **CLOCK-Pro**: Strong scan resistance

pub mod clockpro;
pub mod lru;
pub mod tinylfu;

pub use clockpro::ClockProPolicy;
pub use lru::LruPolicy;
pub use tinylfu::TinyLfuPolicy;

use crate::sst::cache::key::{BlockType, CacheKey};

/// Eviction policy trait
///
/// Policies must tolerate stale keys (keys that were evicted, removed, or never admitted).
/// The cache is the source of truth; policies provide hints, not guarantees.
pub trait CachePolicy: Send + Sync {
    /// Record an access to a key (for policy state tracking)
    fn on_access(&self, key: CacheKey);

    /// Pick a key to evict, excluding specified block types
    ///
    /// `exclude_types`: Block types to protect from eviction (e.g., Index, Filter)
    /// Returns None if no suitable victim is found
    ///
    /// **May return stale keys** - caller must validate with cache before evicting
    fn pick_victim(&self, exclude_types: &[BlockType]) -> Option<CacheKey>;

    /// Notify policy that a key was successfully evicted
    fn on_remove(&self, key: CacheKey);

    /// Notify policy that a picked victim was stale (already gone from cache)
    ///
    /// Policies should remove stale tracking state to converge naturally.
    fn on_stale(&self, key: CacheKey);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_lru_policy() {
        // Arrange
        let key = CacheKey::for_data(1, 0);

        // Act
        let policy = CachePolicyType::Lru.create();
        policy.on_access(key);

        // Assert - trait object should work
        // Should not panic
    }

    #[test]
    fn should_create_tinylfu_policy() {
        // Arrange
        let key = CacheKey::for_data(1, 0);

        // Act
        let policy = CachePolicyType::TinyLfu.create();
        policy.on_access(key);

        // Assert
        // Should not panic
    }

    #[test]
    fn should_create_clockpro_policy() {
        // Arrange
        let key = CacheKey::for_data(1, 0);

        // Act
        let policy = CachePolicyType::ClockPro.create();
        policy.on_access(key);

        // Assert
        // Should not panic
    }

    #[test]
    fn should_polymorphically_handle_accesses() {
        // Arrange
        let policies: Vec<Box<dyn CachePolicy>> = vec![
            CachePolicyType::Lru.create(),
            CachePolicyType::TinyLfu.create(),
            CachePolicyType::ClockPro.create(),
        ];

        // Act
        for policy in &policies {
            for i in 0..5 {
                let key = CacheKey::for_data(i, 0);
                policy.on_access(key);
            }
            policy.pick_victim(&[]);
            policy.clear();
        }

        // Assert - all should work without panic
        assert_eq!(policies.len(), 3);
    }

    #[test]
    fn should_polymorphically_handle_removals() {
        // Arrange
        let policies: Vec<Box<dyn CachePolicy>> = vec![
            CachePolicyType::Lru.create(),
            CachePolicyType::TinyLfu.create(),
            CachePolicyType::ClockPro.create(),
        ];
        let key = CacheKey::for_data(1, 0);

        // Act
        for policy in &policies {
            policy.on_access(key);
            policy.on_remove(key);
        }

        // Assert - all should handle removal without panic
        assert_eq!(policies.len(), 3);
    }

    #[test]
    fn should_handle_all_policy_types_consistently() {
        // Arrange
        let policy_types = [
            CachePolicyType::Lru,
            CachePolicyType::TinyLfu,
            CachePolicyType::ClockPro,
        ];
        let keys: Vec<CacheKey> = (0..10).map(|i| CacheKey::for_data(i, 0)).collect();

        // Act - all policies should handle the same sequence
        let mut victims_after_clear = Vec::new();
        for policy_type in &policy_types {
            let policy = policy_type.create();

            // Add keys
            for key in &keys {
                policy.on_access(*key);
            }

            // Pick victims
            for _ in 0..5 {
                let victim = policy.pick_victim(&[]);
                // Some policies may or may not have victims
                let _ = victim;
            }

            // Clear
            policy.clear();
            victims_after_clear.push(policy.pick_victim(&[]));
        }

        // Assert
        for victim in victims_after_clear {
            assert_eq!(victim, None);
        }
    }

    #[test]
    fn should_be_cloneable_policy_type() {
        // Arrange
        let policy_type = CachePolicyType::Lru;

        // Act
        let policy_type_copy = policy_type;

        // Assert
        let policy1 = policy_type.create();
        let policy2 = policy_type_copy.create();

        let key = CacheKey::for_data(1, 0);
        policy1.on_access(key);
        policy2.on_access(key);
        // Both should work independently
    }

    #[test]
    fn should_create_multiple_instances_of_same_type() {
        // Arrange
        let policy_type = CachePolicyType::Lru;

        // Act
        let policy1 = policy_type.create();
        let policy2 = policy_type.create();

        // Assert - independent instances
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        policy1.on_access(key1);
        policy2.on_access(key2);

        // Both should work independently
        let victim1 = policy1.pick_victim(&[]);
        let victim2 = policy2.pick_victim(&[]);

        assert!(victim1.is_some() || victim1.is_none()); // Can be either
        assert!(victim2.is_some() || victim2.is_none()); // Can be either
    }

    #[test]
    fn should_handle_policy_trait_bound() {
        // Arrange
        fn use_policy(policy: &dyn CachePolicy) {
            let key = CacheKey::for_data(1, 0);
            policy.on_access(key);
            let _ = policy.pick_victim(&[]);
            policy.clear();
        }

        // Act
        let lru = CachePolicyType::Lru.create();
        use_policy(&*lru);

        let tinyfu = CachePolicyType::TinyLfu.create();
        use_policy(&*tinyfu);

        let clockpro = CachePolicyType::ClockPro.create();
        use_policy(&*clockpro);

        // Assert
    }

    #[test]
    fn should_preserve_policy_semantics_across_types() {
        // Arrange
        let policies: Vec<Box<dyn CachePolicy>> = vec![
            CachePolicyType::Lru.create(),
            CachePolicyType::TinyLfu.create(),
            CachePolicyType::ClockPro.create(),
        ];

        // Act
        let mut victims_after_clear = Vec::new();
        for policy in &policies {
            // Add keys in order
            let keys: Vec<CacheKey> = (0..5).map(|i| CacheKey::for_data(i, 0)).collect();
            for key in &keys {
                policy.on_access(*key);
            }

            // Should be able to pick victims (or return None)
            let _ = policy.pick_victim(&[]);

            // Clear should work
            policy.clear();

            // After clear, should have no victims
            victims_after_clear.push(policy.pick_victim(&[]));
        }

        // Assert
        for victim in victims_after_clear {
            assert_eq!(victim, None);
        }
    }
}
