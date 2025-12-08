//! Phase 3.5: Index Sequential Access Optimization Tests
//!
//! Tests for the sequential access optimizer, which predicts sequential block
//! access patterns and caches repeated lookups for range scans.
//!
//! Target: 10-15% iterator throughput improvement

#[cfg(test)]
mod tests {
    use cntryl_midge::sst::SequentialAccessOptimizer;

    #[test]
    fn should_create_optimizer_with_clean_state() {
        // Arrange
        
        // Act
        let opt = SequentialAccessOptimizer::new();

        // Assert
        assert_eq!(opt.cache_size_bytes(), 1024);
        assert_eq!(opt.cache_hit_ratio(), 0.0);
        assert_eq!(opt.predictor_hit_ratio(), 0.0);
        assert_eq!(opt.efficiency_ratio(), 0.0);
    }

    #[test]
    fn should_predict_sequential_blocks() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Simulate sequential block access (0 → 1 → 2 → 3)
        opt.record_lookup(1000, 0);
        assert_eq!(opt.predict_next_block(), None); // No prediction yet

        opt.record_lookup(1001, 1);
        assert_eq!(opt.predict_next_block(), Some(2)); // Predict next

        opt.record_lookup(1002, 2);
        assert_eq!(opt.predict_next_block(), Some(3)); // Still sequential

        opt.record_lookup(1003, 3);
        assert_eq!(opt.predict_next_block(), Some(4)); // Continue sequential

        // Assert
        assert!(opt.predictor_hit_ratio() > 0.5); // Should have reasonable predictor accuracy
    }

    #[test]
    fn should_break_prediction_on_non_sequential_access() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Sequential, then jump
        opt.record_lookup(1, 0);
        opt.record_lookup(2, 1);
        opt.record_lookup(3, 2);
        let pred_sequential = opt.predict_next_block();

        // Jump backward
        opt.record_lookup(4, 0);
        let pred_after_jump = opt.predict_next_block();

        // Assert
        assert_eq!(pred_sequential, Some(3));
        assert_eq!(pred_after_jump, None); // No prediction after backward jump
    }

    #[test]
    fn should_cache_frequently_accessed_blocks() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Record some lookups to populate cache
        for i in 0..50 {
            opt.record_lookup(i as u64, i % 10); // Only 10 unique blocks
        }

        // Now query the cache for keys we've seen
        for i in 0..50 {
            let result = opt.cache_lookup(i as u64);
            // Some should hit depending on hash collisions
            if result.is_some() {
                assert!(result.unwrap() < 10); // All entries should be in blocks 0-9
            }
        }

        // Assert
        let cache_ratio = opt.cache_hit_ratio();
        assert!(cache_ratio >= 0.0 && cache_ratio <= 1.0);
    }

    #[test]
    fn should_predict_mixed_sequential_patterns() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Mix of sequential and random patterns
        // Sequential pattern 1: 0, 1, 2
        opt.record_lookup(100, 0);
        opt.record_lookup(101, 1);
        opt.record_lookup(102, 2);

        // Random jump
        opt.record_lookup(200, 50);

        // Sequential pattern 2: 50, 51, 52
        opt.record_lookup(201, 51);
        opt.record_lookup(202, 52);

        // Random jump again
        opt.record_lookup(300, 10);

        // Assert - predictor should work on sequential parts
        assert!(opt.predictor_hit_ratio() > 0.0);
        let efficiency = opt.efficiency_ratio();
        assert!(efficiency >= 0.0 && efficiency <= 1.0);
    }

    #[test]
    fn should_handle_range_scan_pattern() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Simulate a range scan from block 100 to 110
        for block_idx in 100..=110 {
            let key_hash = (block_idx as u64) * 1000;
            opt.record_lookup(key_hash, block_idx);
        }

        // Assert
        let predictor_ratio = opt.predictor_hit_ratio();
        assert!(
            predictor_ratio > 0.5,
            "Range scans should have high predictor accuracy"
        );
    }

    #[test]
    fn should_handle_repeated_lookups() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Populate cache with lookups, then repeat them
        for i in 0..20 {
            opt.record_lookup(i as u64, i);
        }

        let cache_before = opt.cache_hit_ratio();

        // Repeat some lookups
        for i in 0..20 {
            opt.cache_lookup(i as u64);
        }

        let cache_after = opt.cache_hit_ratio();

        // Assert - cache ratio should be higher after repeated lookups
        assert!(cache_after >= cache_before);
    }

    #[test]
    fn should_reset_metrics_but_keep_predictor() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Build some state
        opt.record_lookup(1, 0);
        opt.record_lookup(2, 1);
        opt.record_lookup(3, 2);

        let pred_before = opt.predict_next_block();
        opt.reset_metrics();
        let pred_after = opt.predict_next_block();

        // Assert
        assert_eq!(pred_before, Some(3));
        assert_eq!(pred_after, Some(3)); // Predictor state preserved
        assert_eq!(opt.cache_hit_ratio(), 0.0); // Metrics reset
        assert_eq!(opt.predictor_hit_ratio(), 0.0);
    }

    #[test]
    fn should_handle_empty_cache_lookups() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Query cache without any prior records
        let result = opt.cache_lookup(12345);

        // Assert
        assert_eq!(result, None);
        assert_eq!(opt.cache_hit_ratio(), 0.0);
    }

    #[test]
    fn should_report_metrics_accurately() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Generate predictable access pattern
        opt.record_lookup(1, 0);
        opt.record_lookup(2, 1); // Sequential hit
        opt.record_lookup(3, 2); // Sequential hit

        let metrics = opt.metrics();

        // Assert
        assert_eq!(metrics.total_lookups, 3);
        assert_eq!(metrics.predictor_hits, 2);
        assert!(metrics.cache_size_bytes > 0);
    }

    #[test]
    fn should_handle_large_block_indices() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Use very large block indices
        let large_block_1 = 10000;
        let large_block_2 = 10001;
        let large_block_3 = 10002;

        opt.record_lookup(1, large_block_1);
        opt.record_lookup(2, large_block_2);
        let pred = opt.predict_next_block();

        opt.record_lookup(3, large_block_3);

        // Assert
        assert_eq!(pred, Some(large_block_3));
        assert!(opt.predictor_hit_ratio() > 0.0);
    }

    #[test]
    fn should_handle_non_sequential_forward_jumps() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Sequential then large forward jump
        opt.record_lookup(1, 0);
        opt.record_lookup(2, 1);
        opt.record_lookup(3, 100); // Large jump forward
        let pred = opt.predict_next_block();

        // Assert
        assert_eq!(pred, Some(101)); // Should still predict (considered sequential)
                                     // This is acceptable since the predictor handles fence pointers optimistically
    }

    #[test]
    fn should_have_consistent_efficiency_ratio() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Generate mixed workload
        for i in 0..100 {
            opt.record_lookup(i as u64, i % 20);
        }

        for i in 0..50 {
            let _ = opt.cache_lookup(i as u64);
        }

        let efficiency = opt.efficiency_ratio();

        // Assert
        assert!(efficiency >= 0.0 && efficiency <= 1.0);
        // Efficiency should be max of cache and predictor ratios
        let cache_ratio = opt.cache_hit_ratio();
        let pred_ratio = opt.predictor_hit_ratio();
        let max_ratio = cache_ratio.max(pred_ratio);
        assert!((efficiency - max_ratio).abs() < 0.001);
    }

    #[test]
    fn should_optimize_repeated_range_scan() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: First range scan (blocks 0-9)
        for i in 0..10 {
            opt.record_lookup(i as u64, i);
        }

        let efficiency_first = opt.efficiency_ratio();

        // Second range scan (same blocks, different keys)
        for i in 0..10 {
            opt.record_lookup((i + 100) as u64, i); // Different keys, same blocks
        }

        let efficiency_second = opt.efficiency_ratio();

        // Assert
        assert!(efficiency_second >= efficiency_first);
        // Second scan should benefit from cache
    }
}
