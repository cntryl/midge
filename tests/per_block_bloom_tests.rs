/// Per-Block Bloom Filter Tests (TDD Approach)
///
/// These tests define the contract for Phase 1: Per-Block Bloom Filters.
/// Implementation follows after all tests are passing.

#[cfg(test)]
mod per_block_bloom_tests {
    use bytes::Bytes;
    use cntryl_midge::sst::block_meta::BlockMeta;
    use cntryl_midge::sst::format::BlockHandle;

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 1: BlockBloom Type & Basic Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: BlockBloom should be created with a capacity
    #[test]
    fn should_create_block_bloom_with_capacity() {
        // Arrange
        let capacity = 1024;

        // Act
        let bloom = cntryl_midge::sst::block_meta::BlockBloom::new(capacity);

        // Assert
        assert_eq!(bloom.capacity_bytes(), 1024);
    }

    /// Test: BlockBloom should support add and maybe_contains operations
    #[test]
    fn should_add_keys_to_block_bloom() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(1024);

        // Act
        bloom.add(b"key1");
        bloom.add(b"key2");
        bloom.add(b"key3");

        // Assert: Keys that were added should be found
        assert!(bloom.maybe_contains(b"key1"));
        assert!(bloom.maybe_contains(b"key2"));
        assert!(bloom.maybe_contains(b"key3"));
    }

    /// Test: BlockBloom should not have false negatives
    #[test]
    fn should_not_have_false_negatives() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(1024);
        let keys: Vec<&[u8]> = vec![b"apple".as_ref(), b"banana".as_ref(), b"cherry".as_ref(), b"date".as_ref()];

        // Act: Add all keys
        for key in &keys {
            bloom.add(key);
        }

        // Assert: All keys must be found
        for key in &keys {
            assert!(
                bloom.maybe_contains(key),
                "Key {:?} should be in bloom (no false negatives)",
                std::str::from_utf8(key).unwrap_or("?")
            );
        }
    }

    /// Test: BlockBloom should be serializable to bytes
    #[test]
    fn should_encode_block_bloom_to_bytes() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(1024);
        bloom.add(b"key1");
        bloom.add(b"key2");

        // Act
        let encoded = bloom.encode();

        // Assert
        assert!(!encoded.is_empty());
        assert!(encoded.len() <= 1024 + 16); // payload + metadata
    }

    /// Test: BlockBloom should be deserializable from bytes
    #[test]
    fn should_decode_block_bloom_from_bytes() {
        // Arrange
        let mut bloom1 = cntryl_midge::sst::block_meta::BlockBloom::new(1024);
        bloom1.add(b"key1");
        bloom1.add(b"key2");
        let encoded = bloom1.encode();

        // Act
        let bloom2 = cntryl_midge::sst::block_meta::BlockBloom::decode(&encoded)
            .expect("Decode should succeed");

        // Assert: Decoded bloom should find the same keys
        assert!(bloom2.maybe_contains(b"key1"));
        assert!(bloom2.maybe_contains(b"key2"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 2: BlockMeta Integration with Per-Block Blooms
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: BlockMeta should support per-block bloom offset
    #[test]
    fn should_store_block_bloom_offset_in_block_meta() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("a"),
            Bytes::from("z"),
            BlockHandle::new(100, 1024),
        );

        // Act
        let meta_with_offset = meta.with_bloom_offset(5000);

        // Assert
        assert_eq!(meta_with_offset.bloom_offset, Some(5000));
    }

    /// Test: BlockMeta should support querying with cached bloom
    #[test]
    fn should_query_block_bloom_from_block_meta() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(512);
        bloom.add(b"apple");
        bloom.add(b"banana");

        let meta_with_bloom = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("banana"),
            BlockHandle::new(100, 1024),
        )
        .with_bloom(bloom.clone());

        // Act
        let contains_apple = meta_with_bloom.bloom_maybe_contains(b"apple");
        let contains_banana = meta_with_bloom.bloom_maybe_contains(b"banana");
        let contains_cherry = meta_with_bloom.bloom_maybe_contains(b"cherry");

        // Assert
        assert!(contains_apple);
        assert!(contains_banana);
        assert!(!contains_cherry); // False positive ok, false negative not ok
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 3: Index Entry with Bloom Offset
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: BlockIndexEntry should include bloom_offset
    #[test]
    fn should_create_block_index_entry_with_bloom_offset() {
        // Arrange
        let min_key = Bytes::from("a");
        let max_key = Bytes::from("z");
        let block_offset = 100;
        let block_len = 1024;
        let bloom_offset = Some(5000);

        // Act
        let entry = cntryl_midge::sst::block_meta::BlockIndexEntry {
            min_key,
            max_key,
            block_offset,
            block_len,
            bloom_offset,
        };

        // Assert
        assert_eq!(entry.bloom_offset, Some(5000));
    }

    /// Test: BlockIndexEntry without bloom_offset should work
    #[test]
    fn should_create_block_index_entry_without_bloom() {
        // Arrange
        let min_key = Bytes::from("a");
        let max_key = Bytes::from("z");

        // Act
        let entry = cntryl_midge::sst::block_meta::BlockIndexEntry {
            min_key,
            max_key,
            block_offset: 100,
            block_len: 1024,
            bloom_offset: None,
        };

        // Assert
        assert_eq!(entry.bloom_offset, None);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 4: Format Versioning & Compatibility
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: Footer should support version flag for per-block blooms
    #[test]
    fn should_detect_per_block_bloom_format() {
        // Arrange
        let footer = cntryl_midge::sst::block_meta::SstFooter {
            metaindex_handle: BlockHandle::new(0, 100),
            index_handle: BlockHandle::new(100, 200),
            has_per_block_blooms: true,
        };

        // Act
        let has_blooms = footer.has_per_block_blooms;

        // Assert
        assert!(has_blooms);
    }

    /// Test: Old SSTs without per-block blooms should be readable
    #[test]
    fn should_read_old_sst_format_without_per_block_blooms() {
        // Arrange
        let footer = cntryl_midge::sst::block_meta::SstFooter {
            metaindex_handle: BlockHandle::new(0, 100),
            index_handle: BlockHandle::new(100, 200),
            has_per_block_blooms: false,
        };

        // Act
        let has_blooms = footer.has_per_block_blooms;

        // Assert
        assert!(!has_blooms);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 5: Bloom False Positive Rate
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: BlockBloom should maintain acceptable false positive rate
    #[test]
    fn should_maintain_acceptable_false_positive_rate() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(4096); // Larger bloom

        // Add 100 keys
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            bloom.add(key.as_bytes());
        }

        // Act: Check for keys that are NOT in the bloom
        let mut false_positives = 0;
        for i in 100..1000 {
            let key = format!("key_{:03}", i);
            if bloom.maybe_contains(key.as_bytes()) {
                false_positives += 1;
            }
        }

        // Assert: False positive rate should be reasonable (< 10% for simple hash)
        let fp_rate = false_positives as f64 / 900.0;
        assert!(
            fp_rate < 0.10,
            "False positive rate too high: {:.2}%",
            fp_rate * 100.0
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 6: Batch Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: BlockBloom should support batch add operations
    #[test]
    fn should_add_batch_of_keys_to_bloom() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(1024);
        let keys = vec![b"a".as_ref(), b"b".as_ref(), b"c".as_ref(), b"d".as_ref(), b"e".as_ref()];

        // Act
        for key in &keys {
            bloom.add(key);
        }

        // Assert
        for key in &keys {
            assert!(bloom.maybe_contains(key));
        }
    }

    /// Test: BlockBloom should support checking multiple keys
    #[test]
    fn should_check_multiple_keys_efficiently() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(1024);
        bloom.add(b"exists1");
        bloom.add(b"exists2");

        // Act
        let contains_1 = bloom.maybe_contains(b"exists1");
        let contains_2 = bloom.maybe_contains(b"exists2");

        // Assert
        assert!(contains_1);
        assert!(contains_2);
        // (not_exists may or may not be found due to false positives)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 7: Edge Cases
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: BlockBloom should handle empty bloom
    #[test]
    fn should_handle_empty_block_bloom() {
        // Arrange
        let bloom = cntryl_midge::sst::block_meta::BlockBloom::new(1024);

        // Act
        let contains_random = bloom.maybe_contains(b"unlikely_to_exist_random_key_12345");

        // Assert: Empty bloom should not find any keys (with high probability)
        // Note: Theoretically possible but extremely unlikely to find random keys
        assert!(!contains_random);
    }

    /// Test: BlockBloom should handle very large keys
    #[test]
    fn should_handle_large_keys_in_bloom() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(2048);
        let large_key = vec![b'x'; 1024];

        // Act
        bloom.add(&large_key);

        // Assert
        assert!(bloom.maybe_contains(&large_key));
    }

    /// Test: BlockBloom should handle very small size
    #[test]
    fn should_handle_small_bloom_size() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(16); // 16 bytes

        // Act
        bloom.add(b"key");
        let contains = bloom.maybe_contains(b"key");

        // Assert: Should still work, just with higher false positive rate
        assert!(contains);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test Suite 8: Serialization Round-Trip
    // ─────────────────────────────────────────────────────────────────────────

    /// Test: Bloom should survive round-trip through encode/decode
    #[test]
    fn should_survive_encode_decode_round_trip() {
        // Arrange
        let mut bloom1 = cntryl_midge::sst::block_meta::BlockBloom::new(1024);
        let keys: Vec<&[u8]> = vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref(), b"key4".as_ref(), b"key5".as_ref()];

        for key in &keys {
            bloom1.add(key);
        }

        // Act
        let encoded = bloom1.encode();
        let bloom2 = cntryl_midge::sst::block_meta::BlockBloom::decode(&encoded)
            .expect("Decode should succeed");

        // Assert: All keys should still be found
        for key in &keys {
            assert!(
                bloom2.maybe_contains(key),
                "Key {:?} lost in round-trip",
                std::str::from_utf8(key).unwrap_or("?")
            );
        }
    }

    /// Test: Encoded bloom should have reasonable size
    #[test]
    fn should_encode_bloom_efficiently() {
        // Arrange
        let mut bloom = cntryl_midge::sst::block_meta::BlockBloom::new(1024);

        for i in 0..100 {
            let key = format!("key_{:03}", i);
            bloom.add(key.as_bytes());
        }

        // Act
        let encoded = bloom.encode();

        // Assert: Encoded size should be close to allocated size + small header
        assert!(encoded.len() >= 1024); // At least the bloom bits
        assert!(encoded.len() <= 1024 + 32); // + reasonable header
    }
}
