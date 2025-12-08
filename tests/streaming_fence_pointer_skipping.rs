//! Phase 2.5: Fence-Pointer Range Skipping in Iterators
//!
//! Tests for:
//! - Fence pointer logic in range iterations
//! - Block skipping metrics  
//! - Wide range scan performance
//! - Correct correctness (no lost keys)

#![allow(unused_imports)]

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cntryl_midge::sst::block_meta::{BlockMeta, IndexTable};
    use cntryl_midge::sst::format::BlockHandle;

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 2.5.1: Fence Pointer Logic Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_skip_block_entirely_before_range_start() {
        // Arrange: Block [100, 200], Range [300, 400]
        let block = BlockMeta::new(
            Bytes::from("100"),
            Bytes::from("200"),
            BlockHandle::new(0, 1024),
        );

        // Act: Check range intersection
        let intersects = block.range_intersects(b"300", b"400");

        // Assert: Should NOT intersect (block entirely before range start)
        assert!(!intersects);
    }

    #[test]
    fn should_skip_block_entirely_after_range_end() {
        // Arrange: Block [500, 600], Range [100, 200]
        let block = BlockMeta::new(
            Bytes::from("500"),
            Bytes::from("600"),
            BlockHandle::new(0, 1024),
        );

        // Act: Check range intersection
        let intersects = block.range_intersects(b"100", b"200");

        // Assert: Should NOT intersect (block entirely after range end)
        assert!(!intersects);
    }

    #[test]
    fn should_not_skip_block_that_partially_overlaps_range() {
        // Arrange: Block [150, 250], Range [200, 350]
        let block = BlockMeta::new(
            Bytes::from("150"),
            Bytes::from("250"),
            BlockHandle::new(0, 1024),
        );

        // Act: Check range intersection
        let intersects = block.range_intersects(b"200", b"350");

        // Assert: Should intersect (blocks overlap)
        assert!(intersects);
    }

    #[test]
    fn should_handle_range_exactly_at_block_boundaries() {
        // Arrange: Block [100, 200], Range [100, 200]
        let block = BlockMeta::new(
            Bytes::from("100"),
            Bytes::from("200"),
            BlockHandle::new(0, 1024),
        );

        // Act: Check range intersection (exact match)
        let intersects_exact = block.range_intersects(b"100", b"200");

        // Assert: Should intersect
        assert!(intersects_exact);
    }

    #[test]
    fn should_skip_blocks_in_sequential_read() {
        // Arrange: Create 10 blocks with 1000-unit spans
        let mut blocks = vec![];
        for i in 0..10 {
            let min = format!("{:04}", i * 1000);
            let max = format!("{:04}", (i + 1) * 1000 - 1);
            blocks.push(BlockMeta::new(
                Bytes::from(min),
                Bytes::from(max),
                BlockHandle::new((i * 1024) as u64, 1024),
            ));
        }

        // Act: Find blocks in range [3500, 6500] (should include blocks 3, 4, 5, 6)
        let range_blocks = blocks
            .iter()
            .filter(|b| b.range_intersects(b"3500", b"6500"))
            .collect::<Vec<_>>();

        // Assert: Should find exactly 4 blocks
        assert_eq!(range_blocks.len(), 4);
        // Verify they're the right ones (blocks 3-6)
        assert_eq!(range_blocks[0].min_key, Bytes::from("3000"));
        assert_eq!(range_blocks[3].min_key, Bytes::from("6000"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 2.5.2: Block Skip Ratio Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_measure_block_skip_ratio_for_narrow_range() {
        // Arrange: 100 blocks, but query only middle 10%
        let mut blocks = vec![];
        for i in 0..100 {
            let min_key = format!("key_{:06}", i * 1000);
            let max_key = format!("key_{:06}", (i + 1) * 1000 - 1);
            blocks.push(BlockMeta::new(
                Bytes::from(min_key),
                Bytes::from(max_key),
                BlockHandle::new((i * 1024) as u64, 1024),
            ));
        }

        // Act: Query range [45000, 55000) (covers blocks 45-54, 10 blocks)
        let intersecting: Vec<_> = blocks
            .iter()
            .filter(|b| b.range_intersects(b"key_045000", b"key_055000"))
            .collect();

        let skip_ratio = (100 - intersecting.len()) as f64 / 100.0;

        // Assert: Should skip 90 blocks, hit 10
        assert_eq!(intersecting.len(), 10);
        assert!(
            skip_ratio > 0.85 && skip_ratio < 0.95,
            "Skip ratio should be ~90%"
        );
    }

    #[test]
    fn should_measure_block_skip_ratio_for_wide_range() {
        // Arrange: 100 simple blocks numbered 0-99
        let mut blocks = vec![];
        for i in 0..100 {
            let min_key = format!("{:03}", i);
            let max_key = format!("{:03}", i + 1);
            blocks.push(BlockMeta::new(
                Bytes::from(min_key),
                Bytes::from(max_key),
                BlockHandle::new((i * 1024) as u64, 1024),
            ));
        }

        // Act: Query range [25, 75) which should hit blocks 25-74
        let intersecting: Vec<_> = blocks
            .iter()
            .filter(|b| b.range_intersects(b"025", b"075"))
            .collect();

        // Assert: Should include approximately 50 blocks (50%)
        println!("Intersecting blocks: {}", intersecting.len());
        assert!(
            intersecting.len() >= 45 && intersecting.len() <= 55,
            "Expected ~50 blocks to intersect, got {}",
            intersecting.len()
        );

        // Verify skip ratio
        let skip_ratio = (100 - intersecting.len()) as f64 / 100.0;
        assert!(
            skip_ratio > 0.40 && skip_ratio < 0.60,
            "Skip ratio should be ~50%"
        );
    }

    #[test]
    fn should_skip_all_blocks_for_range_before_all_blocks() {
        // Arrange: Blocks [100, 200], [200, 300], [300, 400]
        let blocks = vec![
            BlockMeta::new(
                Bytes::from("100"),
                Bytes::from("200"),
                BlockHandle::new(0, 1024),
            ),
            BlockMeta::new(
                Bytes::from("200"),
                Bytes::from("300"),
                BlockHandle::new(1024, 1024),
            ),
            BlockMeta::new(
                Bytes::from("300"),
                Bytes::from("400"),
                BlockHandle::new(2048, 1024),
            ),
        ];

        // Act: Query range [001, 050] (entirely before all blocks)
        let intersecting: Vec<_> = blocks
            .iter()
            .filter(|b| b.range_intersects(b"001", b"050"))
            .collect();

        // Assert: Should skip all blocks
        assert_eq!(intersecting.len(), 0);
    }

    #[test]
    fn should_skip_all_blocks_for_range_after_all_blocks() {
        // Arrange: Blocks [100, 200], [200, 300], [300, 400]
        let blocks = vec![
            BlockMeta::new(
                Bytes::from("100"),
                Bytes::from("200"),
                BlockHandle::new(0, 1024),
            ),
            BlockMeta::new(
                Bytes::from("200"),
                Bytes::from("300"),
                BlockHandle::new(1024, 1024),
            ),
            BlockMeta::new(
                Bytes::from("300"),
                Bytes::from("400"),
                BlockHandle::new(2048, 1024),
            ),
        ];

        // Act: Query range [500, 600] (entirely after all blocks)
        let intersecting: Vec<_> = blocks
            .iter()
            .filter(|b| b.range_intersects(b"500", b"600"))
            .collect();

        // Assert: Should skip all blocks
        assert_eq!(intersecting.len(), 0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 2.5.3: Correctness Tests (No Lost Keys)
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_not_lose_keys_at_range_boundaries() {
        // Arrange: Create blocks with carefully chosen boundary keys
        let blocks = vec![
            BlockMeta::new(
                Bytes::from("apple"),
                Bytes::from("apricot"),
                BlockHandle::new(0, 1024),
            ),
            BlockMeta::new(
                Bytes::from("banana"),
                Bytes::from("blueberry"),
                BlockHandle::new(1024, 1024),
            ),
            BlockMeta::new(
                Bytes::from("cherry"),
                Bytes::from("coconut"),
                BlockHandle::new(2048, 1024),
            ),
        ];

        // Act: Query [apricot, coconut) - should include blocks with overlapping ranges
        // Range is [apricot, coconut)
        // Block 0: [apple, apricot] - min_key=apple, max_key=apricot
        //   apricot <= apricot? YES. apple < coconut? YES. Intersects!
        // Block 1: [banana, blueberry] - intersects
        // Block 2: [cherry, coconut] - intersects
        let intersecting: Vec<_> = blocks
            .iter()
            .filter(|b| b.range_intersects(b"apricot", b"coconut"))
            .collect();

        // Assert: Should include apple block (because max_key=apricot is in range), banana, and cherry
        assert_eq!(intersecting.len(), 3, "All three blocks should intersect");
        assert_eq!(intersecting[0].min_key, Bytes::from("apple"));
        assert_eq!(intersecting[1].min_key, Bytes::from("banana"));
        assert_eq!(intersecting[2].min_key, Bytes::from("cherry"));
    }

    #[test]
    fn should_include_block_containing_only_range_start_key() {
        // Arrange
        let block = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("apricot"),
            BlockHandle::new(0, 1024),
        );

        // Act: Query [apple, zebra)
        let intersects = block.range_intersects(b"apple", b"zebra");

        // Assert: Should intersect
        assert!(intersects);
    }

    #[test]
    fn should_handle_single_key_range() {
        // Arrange: Block containing a single key
        let block = BlockMeta::new(
            Bytes::from("key_100"),
            Bytes::from("key_100"),
            BlockHandle::new(0, 1024),
        );

        // Act: Query exactly that key
        let intersects = block.range_intersects(b"key_100", b"key_101");

        // Assert: Should intersect
        assert!(intersects);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 2.5.4: Range Scan Pattern Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_handle_streaming_window_scan_pattern() {
        // Arrange: Simulate a time-series dataset with 256 blocks (hours of data)
        let mut blocks = vec![];
        for hour in 0..256 {
            let min_key = format!("ts_{:06}:00", hour * 3600);
            let max_key = format!("ts_{:06}:59", (hour + 1) * 3600 - 1);
            blocks.push(BlockMeta::new(
                Bytes::from(min_key),
                Bytes::from(max_key),
                BlockHandle::new((hour * 1024) as u64, 1024),
            ));
        }

        // Act: Consumer queries a 4-hour window
        let start_hour = 100;
        let end_hour = 104;
        let min_query = format!("ts_{:06}:00", start_hour * 3600);
        let max_query = format!("ts_{:06}:59", (end_hour) * 3600 - 1);

        let intersecting: Vec<_> = blocks
            .iter()
            .filter(|b| b.range_intersects(min_query.as_bytes(), max_query.as_bytes()))
            .collect();

        // Assert: Should include 4-5 blocks (hours 100-103/104)
        println!(
            "Streaming window: intersecting {} blocks",
            intersecting.len()
        );
        assert!(
            intersecting.len() >= 4 && intersecting.len() <= 5,
            "Should include 4-5 blocks for 4-hour window, got {}",
            intersecting.len()
        );

        // Verify skip ratio
        let skip_ratio = (256 - intersecting.len()) as f64 / 256.0;
        assert!(
            skip_ratio > 0.97,
            "Should skip ~98% of blocks for 4-hour window from 256 total (skip ratio: {:.1}%)",
            skip_ratio * 100.0
        );
    }

    #[test]
    fn should_handle_overlapping_queries() {
        // Arrange: Create 50 blocks
        let mut blocks = vec![];
        for i in 0..50 {
            let min = format!("key_{:04}", i * 1000);
            let max = format!("key_{:04}", (i + 1) * 1000 - 1);
            blocks.push(BlockMeta::new(
                Bytes::from(min),
                Bytes::from(max),
                BlockHandle::new((i * 1024) as u64, 1024),
            ));
        }

        // Act: Multiple overlapping queries
        let queries = vec![
            ("key_0000", "key_5000"),   // First 5 blocks
            ("key_10000", "key_20000"), // Blocks 10-19
            ("key_15000", "key_35000"), // Blocks 15-34 (overlaps with previous)
        ];

        for (start, end) in queries {
            let intersecting: Vec<_> = blocks
                .iter()
                .filter(|b| b.range_intersects(start.as_bytes(), end.as_bytes()))
                .collect();
            
            // Assert
            assert!(
                intersecting.len() > 0,
                "Query [{}, {}) should find blocks",
                start,
                end
            );
        }
    }

    #[test]
    fn should_correctly_order_results_when_using_fence_pointers() {
        // Arrange: Create ordered blocks
        let mut blocks = vec![];
        for i in 0..10 {
            let min = format!("{:03}", i);
            let max = format!("{:03}", i + 1);
            blocks.push(BlockMeta::new(
                Bytes::from(min),
                Bytes::from(max),
                BlockHandle::new((i * 1024) as u64, 1024),
            ));
        }

        // Act: Query middle range
        let intersecting: Vec<_> = blocks
            .iter()
            .filter(|b| b.range_intersects(b"003", b"007"))
            .collect();

        // Assert: Results should be in order
        for i in 0..intersecting.len() - 1 {
            assert!(
                intersecting[i].min_key < intersecting[i + 1].min_key,
                "Blocks should be in order"
            );
        }
    }
}
