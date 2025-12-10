//! Eviction Actor - manages background local SST replica deletion
//!
//! Responsibilities:
//! - Consume pending eviction queue from Storage Budget Actor
//! - Delete local SST replicas after cloud upload confirmation
//! - Track eviction progress
//! - Update disk state after deletion
//! - Handle errors gracefully (missing files, permission issues)

use crate::storage::HybridStorage;
use std::sync::Arc;

/// Events for the Eviction Actor
#[derive(Debug, Clone)]
pub enum EvictionEvent {
    /// Process the next pending eviction
    ProcessNext,
    /// Mark eviction as complete and update disk state
    Complete { sst_id: u64, freed_bytes: u64 },
}

/// Eviction Actor - manages background local SST deletion
pub struct EvictionActor {
    hybrid_storage: Arc<HybridStorage>,
    /// Stats for testing and monitoring
    evictions_processed: u64,
    total_freed: u64,
}

impl EvictionActor {
    /// Create a new eviction actor
    pub fn new(hybrid_storage: Arc<HybridStorage>) -> Self {
        Self {
            hybrid_storage,
            evictions_processed: 0,
            total_freed: 0,
        }
    }

    /// Handle an eviction event
    pub fn handle_event(&mut self, event: EvictionEvent) -> Result<(), String> {
        match event {
            EvictionEvent::ProcessNext => self.process_next_eviction(),
            EvictionEvent::Complete {
                sst_id,
                freed_bytes,
            } => self.mark_eviction_complete(sst_id, freed_bytes),
        }
    }

    /// Process the next eviction from the queue
    fn process_next_eviction(&mut self) -> Result<(), String> {
        // Get next eviction from SBA
        let next_eviction = {
            let mut actor = self
                .hybrid_storage
                .budget_actor()
                .map_err(|e| format!("Failed to lock SBA: {}", e))?;
            actor.next_eviction()
        };

        if let Some((sst_id, size)) = next_eviction {
            // In a real implementation, we would:
            // 1. Construct the local SST file path
            // 2. Call local backend to delete the replica
            // 3. Handle errors gracefully
            //
            // For now, we track that we processed it and mark as complete
            self.mark_eviction_complete(sst_id, size)?;
        }

        Ok(())
    }

    /// Mark an eviction as complete and update disk state
    fn mark_eviction_complete(&mut self, _sst_id: u64, freed_bytes: u64) -> Result<(), String> {
        // Update counters
        self.evictions_processed += 1;
        self.total_freed += freed_bytes;

        // Note: The disk state in SBA would be updated separately via LocalSSTPurged event
        // when the actual file deletion is confirmed at the filesystem level

        Ok(())
    }

    /// Get the number of evictions processed
    pub fn evictions_processed(&self) -> u64 {
        self.evictions_processed
    }

    /// Get total bytes freed
    pub fn total_freed(&self) -> u64 {
        self.total_freed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_eviction_actor() -> (EvictionActor, Arc<HybridStorage>) {
        // Create mock storage backends for testing
        let local = Arc::new(crate::testkit::MockStorage::new());
        let cloud = Arc::new(crate::testkit::MockStorage::new());
        let hybrid = Arc::new(HybridStorage::new(local, cloud));
        let actor = EvictionActor::new(Arc::clone(&hybrid));
        (actor, hybrid)
    }

    #[test]
    fn should_create_eviction_actor_when_initialized() {
        // Arrange
        let (actor, _hybrid) = create_test_eviction_actor();

        // Assert
        assert_eq!(actor.evictions_processed(), 0);
        assert_eq!(actor.total_freed(), 0);
    }

    #[test]
    fn should_increment_counters_when_marking_eviction_complete() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act
        let result = actor.mark_eviction_complete(1, 1024);

        // Assert
        assert!(result.is_ok());
        assert_eq!(actor.evictions_processed(), 1);
        assert_eq!(actor.total_freed(), 1024);
    }

    #[test]
    fn should_accumulate_freed_bytes_across_evictions() {
        // Arrange
        let (mut actor, _hybrid) = create_test_eviction_actor();

        // Act
        let _ = actor.mark_eviction_complete(1, 1024);
        let _ = actor.mark_eviction_complete(2, 2048);
        let _ = actor.mark_eviction_complete(3, 4096);

        // Assert
        assert_eq!(actor.evictions_processed(), 3);
        assert_eq!(actor.total_freed(), 1024 + 2048 + 4096);
    }
}
