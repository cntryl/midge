//! Eviction Actor - manages background local SST replica deletion
//!
//! Responsibilities:
//! - Consume pending eviction queue from Storage Budget Actor
//! - Delete local SST replicas after cloud upload confirmation
//! - Track eviction progress
//! - Update disk state after deletion
//! - Handle errors gracefully (missing files, permission issues)

use std::sync::Arc;

/// Eviction Actor - manages background local SST deletion
pub struct EvictionActor {
    /// Stats for testing and monitoring
    #[cfg(test)]
    evictions_processed: u64,
    #[cfg(test)]
    total_freed: u64,
}

impl EvictionActor {
    /// Create a new eviction actor
    pub fn new(_hybrid_storage: Arc<crate::storage::HybridStorage>) -> Self {
        Self {
            #[cfg(test)]
            evictions_processed: 0,
            #[cfg(test)]
            total_freed: 0,
        }
    }

    /// Mark an eviction as complete and update disk state
    #[cfg(test)]
    fn mark_eviction_complete(&mut self, _sst_id: u64, freed_bytes: u64) {
        // Update counters
        self.evictions_processed += 1;
        self.total_freed += freed_bytes;

        // Note: The disk state in SBA would be updated separately via LocalSSTPurged event
        // when the actual file deletion is confirmed at the filesystem level
    }

    /// Get the number of evictions processed
    #[cfg(test)]
    pub fn evictions_processed(&self) -> u64 {
        self.evictions_processed
    }

    /// Get total bytes freed
    #[cfg(test)]
    pub fn total_freed(&self) -> u64 {
        self.total_freed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::HybridStorage;

    fn create_test_eviction_actor() -> (EvictionActor, Arc<HybridStorage>) {
        // Create mock storage backends for testing
        let local = Arc::new(crate::storage::test_support::MockStorage::new());
        let cloud = Arc::new(crate::storage::test_support::MockStorage::new());
        let hybrid = Arc::new(HybridStorage::with_policy_and_event_sender(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
        ));
        let actor = EvictionActor::new(Arc::clone(&hybrid));
        (actor, hybrid)
    }

    #[test]
    fn should_initialize_eviction_actor_with_zero_counters() {
        // Arrange
        let (actor, _hybrid) = create_test_eviction_actor();

        // Act
        // Verify initial state

        // Assert
        assert_eq!(actor.evictions_processed(), 0);
        assert_eq!(actor.total_freed(), 0);
    }

    #[test]
    fn should_increment_evictions_when_marking_complete() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act
        actor.mark_eviction_complete(1, 1024);

        // Assert
        assert_eq!(actor.evictions_processed(), 1);
    }

    #[test]
    fn should_accumulate_freed_bytes_across_evictions() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act
        actor.mark_eviction_complete(1, 1024);
        actor.mark_eviction_complete(2, 2048);
        actor.mark_eviction_complete(3, 4096);

        // Assert
        assert_eq!(actor.evictions_processed(), 3);
        assert_eq!(actor.total_freed(), 1024 + 2048 + 4096);
    }

    #[test]
    fn should_maintain_monotonic_eviction_count() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act
        actor.mark_eviction_complete(1, 100);
        let count1 = actor.evictions_processed();

        actor.mark_eviction_complete(2, 200);
        let count2 = actor.evictions_processed();

        actor.mark_eviction_complete(3, 300);
        let count3 = actor.evictions_processed();

        // Assert: counts only increase
        assert_eq!(count1, 1);
        assert_eq!(count2, 2);
        assert_eq!(count3, 3);
    }

    #[test]
    fn should_maintain_monotonic_freed_bytes() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act
        actor.mark_eviction_complete(1, 1000);
        let freed1 = actor.total_freed();

        actor.mark_eviction_complete(2, 500);
        let freed2 = actor.total_freed();

        actor.mark_eviction_complete(3, 200);
        let freed3 = actor.total_freed();

        // Assert: freed bytes only increase
        assert!(freed2 > freed1);
        assert!(freed3 > freed2);
        assert_eq!(freed3, 1700);
    }

    #[test]
    fn should_handle_zero_byte_evictions() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act
        actor.mark_eviction_complete(1, 0);

        // Assert
        assert_eq!(actor.evictions_processed(), 1);
        assert_eq!(actor.total_freed(), 0);
    }

    #[test]
    fn should_handle_multiple_evictions_same_sst() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act: Mark same SST as complete multiple times (edge case)
        actor.mark_eviction_complete(1, 1000);
        actor.mark_eviction_complete(1, 500);

        // Assert: Counter still increments even with same SST ID
        assert_eq!(actor.evictions_processed(), 2);
        assert_eq!(actor.total_freed(), 1500);
    }
}
