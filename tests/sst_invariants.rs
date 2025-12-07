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
        // Arrange
        let min_key = Bytes::from("apple");
        let max_key = Bytes::from("apricot");
        let handle = BlockHandle::new(100, 1024);

        // Act
        let meta = BlockMeta::new(min_key.clone(), max_key.clone(), handle);

        // Assert
        assert_eq!(meta.min_key, min_key);
        assert_eq!(meta.max_key, max_key);
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

        // Act
        let contains_apple = meta.contains_key(b"apple");
        let contains_apricot = meta.contains_key(b"apricot");
        let contains_banana = meta.contains_key(b"banana");
        let contains_aardvark = meta.contains_key(b"aardvark");
        let contains_cherry = meta.contains_key(b"cherry");

        // Assert
        assert!(contains_apple);
        assert!(contains_apricot);
        assert!(contains_banana);
        assert!(!contains_aardvark);
        assert!(!contains_cherry);
    }

    #[test]
    fn should_validate_range_intersection() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("b"),
            Bytes::from("d"),
            BlockHandle::new(100, 1024),
        );

        // Act: Check various range intersections
        let intersects_ac = meta.range_intersects(b"a", b"c");
        let intersects_ce = meta.range_intersects(b"c", b"e");
        let intersects_bd = meta.range_intersects(b"b", b"d");
        let intersects_ab = meta.range_intersects(b"a", b"b");
        let intersects_ef = meta.range_intersects(b"e", b"f");

        // Assert (range [start, end) intersects block [b, d])
        assert!(intersects_ac); // [a, c) intersects [b, d]
        assert!(intersects_ce); // [c, e) intersects [b, d]
        assert!(intersects_bd); // [b, d) intersects [b, d]
        assert!(!intersects_ab); // [a, b) doesn't intersect [b, d]
        assert!(!intersects_ef); // [e, f) doesn't intersect [b, d]
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

        // Act
        let block_b = table.find_block(b"b");
        let block_e = table.find_block(b"e");
        let block_x = table.find_block(b"x");

        // Assert
        assert_eq!(block_b.unwrap().min_key, Bytes::from("a"));
        assert_eq!(block_e.unwrap().min_key, Bytes::from("d"));
        assert_eq!(block_x.unwrap().min_key, Bytes::from("g"));
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

        // Act: Check ordering
        let all_ordered = (0..metas.len() - 1).all(|i| metas[i].max_key < metas[i + 1].min_key);

        // Assert: Non-overlapping blocks (all metas properly ordered)
        assert!(all_ordered, "All blocks must maintain fence pointer ordering");
    }

    #[test]
    fn should_handle_tombstone_metadata() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("cherry"),
            BlockHandle::new(100, 1024),
        );

        // Act
        let meta = meta.with_tombstones(
            true,
            Some(Bytes::from("apple")),
            Some(Bytes::from("cherry")),
        );
        let has_tombstones = meta.has_tombstones;
        let fully_covered = meta.might_be_fully_covered();

        // Assert
        assert!(has_tombstones);
        assert_eq!(meta.tombstone_min, Some(Bytes::from("apple")));
        assert_eq!(meta.tombstone_max, Some(Bytes::from("cherry")));
        assert!(fully_covered);
    }

    #[test]
    fn should_support_bloom_offset_metadata() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("a"),
            Bytes::from("z"),
            BlockHandle::new(100, 1024),
        );

        // Act
        let meta = meta.with_bloom_offset(5000);
        let bloom_offset = meta.bloom_offset;

        // Assert
        assert_eq!(bloom_offset, Some(5000));
    }

    #[test]
    fn should_handle_empty_index_table() {
        // Arrange
        let empty_table = IndexTable::new(vec![]);

        // Act
        let is_empty = empty_table.is_empty();
        let len = empty_table.len();
        let find_result = empty_table.find_block(b"key");
        let range_results = empty_table.find_blocks_in_range(b"a", b"z");

        // Assert
        assert!(is_empty);
        assert_eq!(len, 0);
        assert!(find_result.is_none());
        assert_eq!(range_results.len(), 0);
    }
}
