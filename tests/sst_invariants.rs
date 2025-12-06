/// SST Index Baseline Invariant Tests
///
/// This test suite codifies and validates the invariants specified in INDEX_SPEC.md.
/// These tests form the locked contract: all future SST enhancements must preserve
/// these properties or explicitly update the format version.

#[cfg(test)]
mod sst_invariants {
    use bytes::Bytes;
    use cntryl_midge::sst::{BlockMeta, IndexTable};
    use cntryl_midge::sst::format::BlockHandle;

    /// Test BlockMeta struct

    #[test]
    fn should_create_block_meta() {
        // Arrange & Act
        let meta = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("apricot"),
            BlockHandle::new(100, 1024),
        );

        // Assert
        assert_eq!(meta.min_key, Bytes::from("apple"));
        assert_eq!(meta.max_key, Bytes::from("apricot"));
        assert!(!meta.has_tombstones);
    }

    #[test]
    fn should_validate_key_containment() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("banana"),
            BlockHandle::new(100, 1024),
        );

        // Act & Assert
        assert!(meta.contains_key(b"apple"));
        assert!(meta.contains_key(b"apricot"));
        assert!(meta.contains_key(b"banana"));
        assert!(!meta.contains_key(b"aardvark"));
        assert!(!meta.contains_key(b"cherry"));
    }

    #[test]
    fn should_validate_range_intersection() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("b"),
            Bytes::from("d"),
            BlockHandle::new(100, 1024),
        );

        // Act & Assert (range [start, end) intersects block [b, d])
        assert!(meta.range_intersects(b"a", b"c")); // [a, c) intersects [b, d]
        assert!(meta.range_intersects(b"c", b"e")); // [c, e) intersects [b, d]
        assert!(meta.range_intersects(b"b", b"d")); // [b, d) intersects [b, d]
        assert!(!meta.range_intersects(b"a", b"b")); // [a, b) doesn't intersect [b, d]
        assert!(!meta.range_intersects(b"e", b"f")); // [e, f) doesn't intersect [b, d]
    }

    #[test]
    fn should_build_index_table() {
        // Arrange
        let metas = vec![
            BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(100, 100),
            ),
            BlockMeta::new(
                Bytes::from("g"),
                Bytes::from("z"),
                BlockHandle::new(200, 100),
            ),
        ];

        // Act
        let table = IndexTable::new(metas);

        // Assert
        assert_eq!(table.len(), 3);
        assert!(!table.is_empty());
    }

    #[test]
    fn should_find_block_by_key() {
        // Arrange
        let metas = vec![
            BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(100, 100),
            ),
            BlockMeta::new(
                Bytes::from("g"),
                Bytes::from("z"),
                BlockHandle::new(200, 100),
            ),
        ];
        let table = IndexTable::new(metas);

        // Act & Assert
        assert_eq!(table.find_block(b"b").unwrap().min_key, Bytes::from("a"));
        assert_eq!(table.find_block(b"e").unwrap().min_key, Bytes::from("d"));
        assert_eq!(table.find_block(b"x").unwrap().min_key, Bytes::from("g"));
    }

    #[test]
    fn should_find_blocks_in_range() {
        // Arrange
        let metas = vec![
            BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(100, 100),
            ),
            BlockMeta::new(
                Bytes::from("g"),
                Bytes::from("z"),
                BlockHandle::new(200, 100),
            ),
        ];
        let table = IndexTable::new(metas);

        // Act
        let blocks = table.find_blocks_in_range(b"b", b"h");

        // Assert
        assert_eq!(blocks.len(), 3); // All blocks intersect [b, h)
    }

    #[test]
    fn should_maintain_fence_pointer_invariants() {
        // Arrange: Create metas that respect fence pointer constraints
        let metas = vec![
            BlockMeta::new(Bytes::from("apple"), Bytes::from("apricot"), BlockHandle::new(0, 100)),
            BlockMeta::new(Bytes::from("banana"), Bytes::from("blueberry"), BlockHandle::new(100, 100)),
            BlockMeta::new(Bytes::from("cherry"), Bytes::from("date"), BlockHandle::new(200, 100)),
        ];

        // Assert: Non-overlapping blocks (all metas properly ordered)
        for i in 0..metas.len() - 1 {
            assert!(
                metas[i].max_key < metas[i + 1].min_key,
                "Blocks must not overlap: block {} max_key >= block {} min_key",
                i,
                i + 1
            );
        }
    }

    #[test]
    fn should_handle_tombstone_metadata() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("cherry"),
            BlockHandle::new(100, 1024),
        )
        .with_tombstones(
            true,
            Some(Bytes::from("apple")),
            Some(Bytes::from("cherry")),
        );

        // Assert
        assert!(meta.has_tombstones);
        assert_eq!(meta.tombstone_min, Some(Bytes::from("apple")));
        assert_eq!(meta.tombstone_max, Some(Bytes::from("cherry")));
        assert!(meta.might_be_fully_covered());
    }

    #[test]
    fn should_support_bloom_offset_metadata() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("a"),
            Bytes::from("z"),
            BlockHandle::new(100, 1024),
        )
        .with_bloom_offset(5000);

        // Assert
        assert_eq!(meta.bloom_offset, Some(5000));
    }

    #[test]
    fn should_handle_empty_index_table() {
        // Arrange & Act
        let table = IndexTable::new(vec![]);

        // Assert
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert!(table.find_block(b"key").is_none());
        assert_eq!(table.find_blocks_in_range(b"a", b"z").len(), 0);
    }
}
