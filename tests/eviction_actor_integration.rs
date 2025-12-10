//! Integration tests for Eviction Actor with Storage Budget Actor coordination

#[cfg(test)]
mod tests {
    use cntryl_midge::storage::HybridStorage;
    use cntryl_midge::runtime::actors::EvictionActor;
    use cntryl_midge::testkit::MockStorage;
    use std::sync::Arc;

    fn setup_eviction_scenario() -> (EvictionActor, Arc<HybridStorage>) {
        let local = Arc::new(MockStorage::new());
        let cloud = Arc::new(MockStorage::new());
        let hybrid = Arc::new(HybridStorage::new(local, cloud));
        let eviction_actor = EvictionActor::new(Arc::clone(&hybrid));
        (eviction_actor, hybrid)
    }

    #[test]
    fn should_track_single_eviction_completion() {
        // Arrange
        let (mut actor, _hybrid) = setup_eviction_scenario();

        // Act
        let result = actor.handle_event(cntryl_midge::runtime::actors::EvictionEvent::Complete {
            sst_id: 1,
            freed_bytes: 1024,
        });

        // Assert
        assert!(result.is_ok());
        assert_eq!(actor.evictions_processed(), 1);
        assert_eq!(actor.total_freed(), 1024);
    }

    #[test]
    fn should_accumulate_multiple_evictions_over_time() {
        // Arrange
        let (mut actor, _hybrid) = setup_eviction_scenario();

        // Act
        for i in 1..=5 {
            let result = actor.handle_event(cntryl_midge::runtime::actors::EvictionEvent::Complete {
                sst_id: i,
                freed_bytes: i as u64 * 1024,
            });
            assert!(result.is_ok());
        }

        // Assert
        assert_eq!(actor.evictions_processed(), 5);
        // 1*1024 + 2*1024 + 3*1024 + 4*1024 + 5*1024 = 15*1024
        assert_eq!(actor.total_freed(), 15 * 1024);
    }

    #[test]
    fn should_handle_large_evictions() {
        // Arrange
        let (mut actor, _hybrid) = setup_eviction_scenario();
        let large_size = 100 * 1024 * 1024; // 100 MB

        // Act
        let result = actor.handle_event(cntryl_midge::runtime::actors::EvictionEvent::Complete {
            sst_id: 1,
            freed_bytes: large_size,
        });

        // Assert
        assert!(result.is_ok());
        assert_eq!(actor.evictions_processed(), 1);
        assert_eq!(actor.total_freed(), large_size);
    }

    #[test]
    fn should_initialize_eviction_actor_with_hybrid_storage() {
        // Arrange
        let local = Arc::new(MockStorage::new());
        let cloud = Arc::new(MockStorage::new());
        let hybrid = Arc::new(HybridStorage::new(local, cloud));

        // Act
        let actor = EvictionActor::new(Arc::clone(&hybrid));

        // Assert
        assert_eq!(actor.evictions_processed(), 0);
        assert_eq!(actor.total_freed(), 0);
    }
}
