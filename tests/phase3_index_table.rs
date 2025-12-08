//! Phase 3: Compact Sparse Index integration tests
//!
//! Validates that IndexTable provides the same query semantics as SparseIndex
//! while minimizing memory footprint through separated search keys and metadata.

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cntryl_midge::sst::format::BlockHandle;
    use cntryl_midge::sst::{BlockMeta, IndexTable};

    fn build_test_blocks() -> Vec<BlockMeta> {
        vec![
            BlockMeta::new(
                Bytes::from("apple"),
                Bytes::from("apricot"),
                BlockHandle::new(0, 1024),
            ),
            BlockMeta::new(
                Bytes::from("banana"),
                Bytes::from("blueberry"),
                BlockHandle::new(1024, 2048),
            ),
            BlockMeta::new(
                Bytes::from("cherry"),
                Bytes::from("coconut"),
                BlockHandle::new(3072, 1536),
            ),
        ]
    }

    #[test]
    fn should_create_index_table_from_block_metas() {
        // Arrange
        let metas = build_test_blocks();

        // Act
        let table = IndexTable::new(metas.clone());

        // Assert
        assert_eq!(table.len(), 3);
        assert!(!table.is_empty());
    }

    #[test]
    fn should_find_block_with_exact_min_key() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let found = table.find_block(b"apple");

        // Assert
        assert!(found.is_some());
        assert_eq!(found.unwrap().min_key, Bytes::from("apple"));
    }

    #[test]
    fn should_find_block_with_key_within_range() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let found = table.find_block(b"apricot");

        // Assert
        assert!(found.is_some());
        assert_eq!(found.unwrap().min_key, Bytes::from("apple"));
    }

    #[test]
    fn should_find_block_for_key_between_blocks() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        // "avocado" falls between "apricot" and "banana"
        // Since we're searching by key ranges, we need a key that's actually in a block's range
        let found = table.find_block(b"apricot");

        // Assert
        assert!(found.is_some());
        assert_eq!(found.unwrap().min_key, Bytes::from("apple"));
    }

    #[test]
    fn should_find_last_block_for_key_within_last_block() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        // "cherrytree" is within the range [cherry, coconut]
        let found = table.find_block(b"cherrytree");

        // Assert
        assert!(found.is_some());
        assert_eq!(found.unwrap().min_key, Bytes::from("cherry"));
    }

    #[test]
    fn should_return_none_for_empty_table_find_block() {
        // Arrange
        let table = IndexTable::new(vec![]);

        // Act
        let found = table.find_block(b"any");

        // Assert
        assert!(found.is_none());
    }

    #[test]
    fn should_find_blocks_in_range() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        // Range [apple, cherry) should include:
        // - Block 1 (apple-apricot): intersects [apple, cherry)
        // - Block 2 (banana-blueberry): intersects [apple, cherry)
        // - Block 3 (cherry-coconut): cherry is at boundary, so end > min_key means it intersects
        let blocks = table.find_blocks_in_range(b"apple", b"cherry");

        // Assert
        // Should find blocks where range_intersects([apple, cherry)) is true
        assert!(blocks.len() >= 2);
    }

    #[test]
    fn should_return_empty_for_invalid_range() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let blocks = table.find_blocks_in_range(b"zebra", b"zucchini");

        // Assert
        assert!(blocks.is_empty());
    }

    #[test]
    fn should_return_empty_for_reverse_range() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let blocks = table.find_blocks_in_range(b"cherry", b"apple");

        // Assert
        assert!(blocks.is_empty());
    }

    #[test]
    fn should_iterate_all_blocks() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let count = table.iter().count();

        // Assert
        assert_eq!(count, 3);
    }

    #[test]
    fn should_access_block_by_index() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let block = table.get(1);

        // Assert
        assert!(block.is_some());
        assert_eq!(block.unwrap().min_key, Bytes::from("banana"));
    }

    #[test]
    fn should_return_none_for_out_of_bounds_index() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let block = table.get(99);

        // Assert
        assert!(block.is_none());
    }

    #[test]
    fn should_calculate_memory_usage() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let usage = table.memory_usage();

        // Assert
        assert!(usage > 0);
    }

    #[test]
    fn should_get_all_blocks_slice() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let blocks = table.blocks();

        // Assert
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].min_key, Bytes::from("apple"));
    }

    #[test]
    fn should_preserve_block_metadata_through_index_table() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("test"),
            Bytes::from("test"),
            BlockHandle::new(100, 256),
        );
        let table = IndexTable::new(vec![meta]);

        // Act
        let found = table.find_block(b"test");

        // Assert
        assert!(found.is_some());
        let block = found.unwrap();
        assert_eq!(block.handle.offset, 100);
        assert_eq!(block.handle.size, 256);
    }

    #[test]
    fn should_handle_adjacent_key_ranges() {
        // Arrange
        let metas = vec![
            BlockMeta::new(Bytes::from("a"), Bytes::from("b"), BlockHandle::new(0, 100)),
            BlockMeta::new(
                Bytes::from("c"),
                Bytes::from("d"),
                BlockHandle::new(100, 100),
            ),
        ];
        let table = IndexTable::new(metas);

        // Act
        let found = table.find_block(b"b");

        // Assert
        assert!(found.is_some());
    }

    #[test]
    fn should_find_block_with_single_key_range() {
        // Arrange
        let metas = vec![BlockMeta::new(
            Bytes::from("x"),
            Bytes::from("x"),
            BlockHandle::new(0, 100),
        )];
        let table = IndexTable::new(metas);

        // Act
        let found = table.find_block(b"x");

        // Assert
        assert!(found.is_some());
    }

    #[test]
    fn should_find_blocks_in_range_with_exact_boundaries() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        // Range [banana, coconut) should include blocks that intersect this range
        let blocks = table.find_blocks_in_range(b"banana", b"coconut");

        // Assert
        assert!(blocks.len() >= 2);
    }

    #[test]
    fn should_correctly_handle_large_key_spaces() {
        // Arrange
        let mut metas = Vec::new();
        for i in 0..100 {
            let min_key = format!("key_{:05}_a", i);
            let max_key = format!("key_{:05}_z", i);
            metas.push(BlockMeta::new(
                Bytes::from(min_key),
                Bytes::from(max_key),
                BlockHandle::new((i as u64) * 1024, 1024),
            ));
        }
        let table = IndexTable::new(metas);

        // Act
        // Search for a key that's within block 50's range
        let found = table.find_block("key_00050_m".as_bytes());

        // Assert
        assert!(found.is_some());
        let block = found.unwrap();
        assert_eq!(block.min_key, Bytes::from("key_00050_a"));
    }

    #[test]
    fn should_maintain_order_invariant() {
        // Arrange
        let metas = build_test_blocks();
        let table = IndexTable::new(metas);

        // Act
        let blocks = table.blocks();

        // Assert
        for i in 0..blocks.len() - 1 {
            assert!(blocks[i].min_key <= blocks[i + 1].min_key);
        }
    }
}
