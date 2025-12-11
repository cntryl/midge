//! Phase 4: Range Tombstone Indexing Tests
//!
//! Validates that tombstone indexes enable efficient tombstone lookups
//! without reading data blocks, and properly handle compaction scenarios.

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cntryl_midge::sst::format::BlockHandle;
    use cntryl_midge::sst::traits::RangeTombstone;
    use cntryl_midge::sst::{TombstoneIndex, TombstoneIndexBuilder, TombstoneIndexEntry};

    fn create_tombstone(start: &[u8], end: &[u8], seq: u64) -> RangeTombstone {
        RangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            seq,
        }
    }

    #[test]
    fn should_create_empty_tombstone_index_when_no_tombstones() {
        // Arrange
        // (no setup needed)

        // Act
        let index = TombstoneIndex::empty();

        // Assert
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn should_build_tombstone_index_from_single_block() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        let tombstones = vec![
            create_tombstone(b"apple", b"banana", 10),
            create_tombstone(b"cherry", b"date", 20),
        ];

        // Act
        builder.add_block(&tombstones, BlockHandle::new(100, 256));
        let index = builder.finish();

        // Assert
        assert_eq!(index.len(), 1);
        assert_eq!(index.entries()[0].count, 2);
    }

    #[test]
    fn should_build_tombstone_index_from_multiple_blocks() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();

        // Act
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"m", b"p", 20)],
            BlockHandle::new(100, 150),
        );
        builder.add_block(
            &[create_tombstone(b"x", b"z", 30)],
            BlockHandle::new(250, 200),
        );
        let index = builder.finish();

        // Assert
        assert_eq!(index.len(), 3);
    }

    #[test]
    fn should_find_tombstone_blocks_for_key_in_range() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"e", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"m", b"p", 20)],
            BlockHandle::new(100, 150),
        );
        let index = builder.finish();

        // Act
        let blocks_for_b: Vec<_> = index.find_blocks_for_key(b"b").collect();
        let blocks_for_n: Vec<_> = index.find_blocks_for_key(b"n").collect();

        // Assert
        assert_eq!(blocks_for_b.len(), 1);
        assert_eq!(blocks_for_b[0].min_key, Bytes::from("a"));
        assert_eq!(blocks_for_n.len(), 1);
        assert_eq!(blocks_for_n[0].min_key, Bytes::from("m"));
    }

    #[test]
    fn should_return_no_blocks_when_key_not_in_any_range() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"m", b"p", 20)],
            BlockHandle::new(100, 150),
        );
        let index = builder.finish();

        // Act
        let blocks: Vec<_> = index.find_blocks_for_key(b"x").collect();

        // Assert
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn should_find_blocks_intersecting_range() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"d", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"e", b"h", 20)],
            BlockHandle::new(100, 150),
        );
        builder.add_block(
            &[create_tombstone(b"m", b"p", 30)],
            BlockHandle::new(250, 200),
        );
        let index = builder.finish();

        // Act
        let blocks: Vec<_> = index.find_blocks_in_range(b"c", b"f").collect();

        // Assert
        assert_eq!(blocks.len(), 2); // First two blocks intersect [c, f)
    }

    #[test]
    fn should_return_no_blocks_when_range_disjoint() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        let index = builder.finish();

        // Act
        let blocks: Vec<_> = index.find_blocks_in_range(b"x", b"z").collect();

        // Assert
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn should_detect_potential_deletion_when_key_in_range() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"m", 10)],
            BlockHandle::new(0, 100),
        );
        let index = builder.finish();

        // Act
        let might_be_deleted = index.might_be_deleted(b"f");

        // Assert
        assert!(might_be_deleted);
    }

    #[test]
    fn should_detect_no_deletion_when_key_outside_range() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"m", 10)],
            BlockHandle::new(0, 100),
        );
        let index = builder.finish();

        // Act
        let might_be_deleted = index.might_be_deleted(b"z");

        // Assert
        assert!(!might_be_deleted);
    }

    #[test]
    fn should_handle_overlapping_tombstones_in_same_block() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        let tombstones = vec![
            create_tombstone(b"a", b"e", 10),
            create_tombstone(b"c", b"g", 20),
            create_tombstone(b"b", b"f", 30),
        ];

        // Act
        builder.add_block(&tombstones, BlockHandle::new(0, 100));
        let index = builder.finish();

        // Assert
        assert_eq!(index.len(), 1);
        let entry = &index.entries()[0];
        assert_eq!(entry.min_key, Bytes::from("a"));
        assert_eq!(entry.max_key, Bytes::from("g"));
        assert_eq!(entry.count, 3);
    }

    #[test]
    fn should_handle_tombstones_with_identical_ranges() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        let tombstones = vec![
            create_tombstone(b"x", b"z", 10),
            create_tombstone(b"x", b"z", 20),
        ];

        // Act
        builder.add_block(&tombstones, BlockHandle::new(0, 100));
        let index = builder.finish();

        // Assert
        assert_eq!(index.len(), 1);
        assert_eq!(index.entries()[0].count, 2);
    }

    #[test]
    fn should_skip_empty_tombstone_blocks() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();

        // Act
        builder.add_block(&[], BlockHandle::new(0, 100));
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(100, 150),
        );
        let index = builder.finish();

        // Assert
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn should_handle_tombstones_with_empty_start_key() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        let tombstones = vec![create_tombstone(b"", b"end", 10)];

        // Act
        builder.add_block(&tombstones, BlockHandle::new(0, 100));
        let index = builder.finish();

        // Assert
        assert_eq!(index.len(), 1);
        assert_eq!(index.entries()[0].min_key, Bytes::from(""));
    }

    #[test]
    fn should_handle_tombstones_spanning_entire_keyspace() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        let tombstones = vec![create_tombstone(b"", b"\xff\xff\xff\xff", 10)];

        // Act
        builder.add_block(&tombstones, BlockHandle::new(0, 100));
        let index = builder.finish();

        // Assert
        let might_delete_any = index.might_be_deleted(b"any_key");
        assert!(might_delete_any);
    }

    #[test]
    fn should_maintain_block_order_for_sequential_ranges() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"c", b"e", 20)],
            BlockHandle::new(100, 150),
        );
        builder.add_block(
            &[create_tombstone(b"e", b"g", 30)],
            BlockHandle::new(250, 200),
        );
        let index = builder.finish();

        // Act
        let entries = index.entries();

        // Assert
        assert_eq!(entries.len(), 3);
        assert!(entries[0].max_key <= entries[1].min_key);
        assert!(entries[1].max_key <= entries[2].min_key);
    }

    #[test]
    fn should_check_entry_coverage_with_boundary_keys() {
        // Arrange
        let entry = TombstoneIndexEntry::new(
            Bytes::from("b"),
            Bytes::from("e"),
            BlockHandle::new(0, 100),
            1,
        );

        // Act
        let result_a = entry.might_cover(b"a");
        let result_b = entry.might_cover(b"b");
        let result_c = entry.might_cover(b"c");
        let result_e = entry.might_cover(b"e");
        let result_f = entry.might_cover(b"f");

        // Assert
        assert!(!result_a); // Before range
        assert!(result_b); // Start boundary (inclusive)
        assert!(result_c); // Middle
        assert!(!result_e); // End boundary (exclusive)
        assert!(!result_f); // After range
    }

    #[test]
    fn should_check_entry_range_intersection_with_boundaries() {
        // Arrange
        let entry = TombstoneIndexEntry::new(
            Bytes::from("c"),
            Bytes::from("f"),
            BlockHandle::new(0, 100),
            1,
        );

        // Act
        let result_ad = entry.range_intersects(b"a", b"d");
        let result_eh = entry.range_intersects(b"e", b"h");
        let result_cf = entry.range_intersects(b"c", b"f");
        let result_de = entry.range_intersects(b"d", b"e");
        let result_ac = entry.range_intersects(b"a", b"c");

        // Assert
        assert!(result_ad); // Overlaps start
        assert!(result_eh); // Overlaps end
        assert!(result_cf); // Exact match
        assert!(result_de); // Contained
        assert!(!result_ac); // Ends at start
        assert!(!entry.range_intersects(b"f", b"h")); // Starts at end
    }

    #[test]
    fn should_handle_large_number_of_tombstone_blocks() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        for i in 0..1000 {
            let start = format!("key_{:06}_a", i);
            let end = format!("key_{:06}_z", i);
            builder.add_block(
                &[create_tombstone(start.as_bytes(), end.as_bytes(), i as u64)],
                BlockHandle::new((i as u64) * 1024, 1024),
            );
        }

        // Act
        let index = builder.finish();

        // Assert
        assert_eq!(index.len(), 1000);
        let blocks: Vec<_> = index.find_blocks_for_key(b"key_000500_m").collect();
        assert_eq!(blocks.len(), 1);
    }

    #[test]
    fn should_find_multiple_overlapping_blocks_for_key() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"m", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"e", b"p", 20)],
            BlockHandle::new(100, 150),
        );
        builder.add_block(
            &[create_tombstone(b"j", b"z", 30)],
            BlockHandle::new(250, 200),
        );
        let index = builder.finish();

        // Act
        let blocks: Vec<_> = index.find_blocks_for_key(b"k").collect();

        // Assert
        assert_eq!(blocks.len(), 3); // All three blocks might contain tombstones covering 'k'
    }

    #[test]
    fn should_iterate_through_all_entries() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"m", b"p", 20)],
            BlockHandle::new(100, 150),
        );
        let index = builder.finish();

        // Act
        let count = index.entries().len();

        // Assert
        assert_eq!(count, 2);
    }
}
