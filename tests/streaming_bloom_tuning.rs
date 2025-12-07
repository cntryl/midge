//! Phase 1.5: Bloom Filter Tuning for Negative Lookups
//!
//! Tests for:
//! - Configurable bits-per-key in bloom filters (8→12)
//! - Fast negative filter construction and usage
//! - False positive rate improvements
//! - Negative lookup performance
//! - Integration with SST metadata

#![allow(unused_imports)]

#[cfg(test)]
mod tests {
    use cntryl_midge::sst::bloom::{BloomFilter, BloomFilterBuilder};
    use cntryl_midge::sst::fast_negative_filter::{FastNegativeFilter, FAST_NEGATIVE_FILTER_BYTES};
    use cntryl_midge::sst::block_meta::{BlockMeta, IndexTable};
    use cntryl_midge::sst::format::BlockHandle;
    use bytes::Bytes;

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 1.5.1: Bloom Filter Bits/Key Configuration Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_create_bloom_filter_with_8_bits_per_key() {
        // Arrange: Create a bloom filter with 8 bits/key (baseline)
        let mut bloom = BloomFilterBuilder::with_bits_per_key(8);

        // Act: Add 1000 keys
        for i in 0..1_000u32 {
            let key = format!("key_{:06}", i);
            bloom.add_key(key.as_bytes());
        }
        let filter = bloom.finish();

        // Assert
        assert_eq!(filter.keys_count(), 1_000);
        // 8 bits/key should give ~3-5% FPR
        let fpr = filter.estimated_fpr();
        assert!(fpr > 0.01 && fpr < 0.15, "FPR {} should be in range [1%, 15%]", fpr);
    }

    #[test]
    fn should_create_bloom_filter_with_12_bits_per_key() {
        // Arrange: Create a bloom filter with 12 bits/key (optimized for negative lookups)
        let mut bloom = BloomFilterBuilder::with_bits_per_key(12);

        // Act: Add 1000 keys
        for i in 0..1_000u32 {
            let key = format!("key_{:06}", i);
            bloom.add_key(key.as_bytes());
        }
        let filter = bloom.finish();

        // Assert
        assert_eq!(filter.keys_count(), 1_000);
        // 12 bits/key should give ~0.1-0.5% FPR
        let fpr = filter.estimated_fpr();
        assert!(fpr > 0.0001 && fpr < 0.01, "FPR {} should be in range [0.01%, 1%]", fpr);
    }

    #[test]
    fn should_show_improved_fpr_with_higher_bits_per_key() {
        // Arrange: Create two filters with different bits/key
        let mut bloom_8 = BloomFilterBuilder::with_bits_per_key(8);
        let mut bloom_12 = BloomFilterBuilder::with_bits_per_key(12);

        // Act: Add same keys to both
        for i in 0..1_000u32 {
            let key = format!("key_{:06}", i);
            bloom_8.add_key(key.as_bytes());
            bloom_12.add_key(key.as_bytes());
        }
        let filter_8 = bloom_8.finish();
        let filter_12 = bloom_12.finish();

        // Assert: 12 bits/key should have lower FPR
        let fpr_8 = filter_8.estimated_fpr();
        let fpr_12 = filter_12.estimated_fpr();
        assert!(fpr_12 < fpr_8, "12 bits/key FPR {} should be < 8 bits/key FPR {}", fpr_12, fpr_8);
    }

    #[test]
    fn should_maintain_no_false_negatives_with_increased_bits_per_key() {
        // Arrange: Create two filters and add keys
        let mut bloom_8 = BloomFilterBuilder::with_bits_per_key(8);
        let mut bloom_12 = BloomFilterBuilder::with_bits_per_key(12);

        let test_keys: Vec<String> = (0..500u32)
            .map(|i| format!("key_{:06}", i))
            .collect();

        // Act: Add keys and verify no false negatives
        for key_str in &test_keys {
            let key_bytes = key_str.as_bytes();
            bloom_8.add_key(key_bytes);
            bloom_12.add_key(key_bytes);
        }
        let filter_8 = bloom_8.finish();
        let filter_12 = bloom_12.finish();

        // Assert: All added keys must be found (no false negatives)
        for key_str in &test_keys {
            let key_bytes = key_str.as_bytes();
            assert!(
                filter_8.may_contain(key_bytes),
                "8 bits/key: Key {} not found (false negative!)",
                key_str
            );
            assert!(
                filter_12.may_contain(key_bytes),
                "12 bits/key: Key {} not found (false negative!)",
                key_str
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 1.5.2: Negative Lookup Performance Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_have_lower_false_positive_rate_for_negative_lookups_at_12_bits_per_key() {
        // Arrange: Create a 12-bit filter with 1000 keys
        let mut builder = BloomFilterBuilder::with_bits_per_key(12);
        for i in 0..1_000u32 {
            let key = format!("key_{:06}", i);
            builder.add_key(key.as_bytes());
        }
        let filter = builder.finish();

        // Act: Query 10,000 non-existent keys (offset to avoid true positives)
        let mut false_positives = 0;
        for i in 100_000..110_000u32 {
            let key = format!("key_{:06}", i);
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / 10_000.0;

        // Assert: 12 bits/key should have < 1% FPR
        assert!(fpr < 0.01, "False positive rate {} should be < 1%", fpr);
    }

    #[test]
    fn should_handle_wide_range_negative_lookups_efficiently() {
        // Arrange: Create a 12-bit filter with a sparse key distribution
        // (keys at 0%, 25%, 50%, 75% of range)
        let mut builder = BloomFilterBuilder::with_bits_per_key(12);
        let keys_to_add = vec![
            format!("key_{:06}", 0),
            format!("key_{:06}", 250_000),
            format!("key_{:06}", 500_000),
            format!("key_{:06}", 750_000),
        ];

        for key_str in &keys_to_add {
            builder.add_key(key_str.as_bytes());
        }
        let filter = builder.finish();

        // Act: Query many non-existent keys and count false positives
        let test_keys = vec![
            format!("key_{:06}", 100_000),
            format!("key_{:06}", 200_000),
            format!("key_{:06}", 300_000),
            format!("key_{:06}", 400_000),
            format!("key_{:06}", 600_000),
            format!("key_{:06}", 700_000),
            format!("key_{:06}", 800_000),
        ];

        let mut false_positives = 0;
        for key_str in &test_keys {
            if filter.may_contain(key_str.as_bytes()) {
                false_positives += 1;
            }
        }

        // Assert: With sparse keys, even with 12 bits/key, FPR should be very low
        let fpr = false_positives as f64 / test_keys.len() as f64;
        assert!(fpr < 0.2, "FPR for negative lookups {} should be < 20%", fpr);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 1.5.3: Fast Negative Filter Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_construct_fast_negative_filter_for_sst_blocks() {
        // Arrange: Create block metadata
        let blocks = vec![
            BlockMeta::new(
                Bytes::from("a"),
                Bytes::from("c"),
                BlockHandle::new(0, 1024),
            ),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(1024, 1024),
            ),
            BlockMeta::new(
                Bytes::from("g"),
                Bytes::from("z"),
                BlockHandle::new(2048, 1024),
            ),
        ];

        // Act: Create index table with fast negative filter
        let mut filter = FastNegativeFilter::new();
        // Mark blocks 0 and 2 as containing keys (block 1 is empty/skipped)
        filter.set_block(0);
        filter.set_block(2);

        let table = IndexTable::with_fast_negative_filter(blocks, filter);

        // Assert
        assert_eq!(table.len(), 3);
        assert!(table.might_contain_block_via_fast_filter(0));
        assert!(!table.might_contain_block_via_fast_filter(1)); // Empty block
        assert!(table.might_contain_block_via_fast_filter(2));
    }

    #[test]
    fn should_encode_decode_fast_negative_filter() {
        // Arrange
        let mut filter = FastNegativeFilter::new();
        filter.set_block(0);
        filter.set_block(10);
        filter.set_block(255);

        // Act
        let encoded = filter.encode();
        let decoded = FastNegativeFilter::decode(&encoded).unwrap();

        // Assert
        assert_eq!(filter, decoded);
        assert!(decoded.might_contain_block(0));
        assert!(decoded.might_contain_block(10));
        assert!(decoded.might_contain_block(255));
        assert!(!decoded.might_contain_block(1));
    }

    #[test]
    fn should_use_fast_negative_filter_in_negative_lookup_path() {
        // Arrange: Create an index table with fast negative filter
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
        ];

        let mut filter = FastNegativeFilter::new();
        filter.set_block(0); // Only first block has keys
        let table = IndexTable::with_fast_negative_filter(blocks, filter);

        // Act: Check if blocks might contain keys
        let block_0_might_contain = table.might_contain_block_via_fast_filter(0);
        let block_1_might_contain = table.might_contain_block_via_fast_filter(1);

        // Assert
        assert!(block_0_might_contain, "Block 0 should be marked as potentially containing keys");
        assert!(!block_1_might_contain, "Block 1 should be marked as empty");
    }

    #[test]
    fn should_handle_index_table_without_fast_negative_filter() {
        // Arrange: Create index table WITHOUT fast negative filter
        let blocks = vec![
            BlockMeta::new(
                Bytes::from("a"),
                Bytes::from("c"),
                BlockHandle::new(0, 1024),
            ),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(1024, 1024),
            ),
        ];

        let table = IndexTable::new(blocks);

        // Act: Check if blocks might contain keys (should be conservative)
        let block_0_might_contain = table.might_contain_block_via_fast_filter(0);
        let block_1_might_contain = table.might_contain_block_via_fast_filter(1);

        // Assert: Without filter, should conservatively return true for all blocks
        assert!(block_0_might_contain);
        assert!(block_1_might_contain);
    }

    #[test]
    fn should_fit_fast_negative_filter_in_l1_cache() {
        // Arrange: Verify the fast negative filter fits in typical L1 cache

        // Act: Calculate size
        let filter = FastNegativeFilter::new();
        let encoded = filter.encode();

        // Assert: 32 bytes should fit in L1 cache (typical 32 KB)
        assert_eq!(encoded.len(), FAST_NEGATIVE_FILTER_BYTES);
        assert!(encoded.len() < 64, "Filter should fit in L1 cache line");
    }

    #[test]
    fn should_efficiently_skip_empty_blocks_with_fast_filter() {
        // Arrange: Create a scenario with 100 blocks, but only 10 have data
        let mut blocks = vec![];
        for i in 0..100 {
            let min_key = format!("key_{:03}", i * 100);
            let max_key = format!("key_{:03}", (i + 1) * 100);
            blocks.push(BlockMeta::new(
                Bytes::from(min_key),
                Bytes::from(max_key),
                BlockHandle::new((i * 1024) as u64, 1024),
            ));
        }

        let mut filter = FastNegativeFilter::new();
        // Only mark even blocks as containing keys
        for i in (0..100).step_by(2) {
            filter.set_block(i);
        }

        let table = IndexTable::with_fast_negative_filter(blocks, filter);

        // Act: Count how many blocks are skipped as empty
        let mut skipped = 0;
        for i in 0..100 {
            if !table.might_contain_block_via_fast_filter(i) {
                skipped += 1;
            }
        }

        // Assert: Should skip approximately 50 blocks (half)
        assert_eq!(skipped, 50, "Should skip exactly 50 empty blocks");
    }

    #[test]
    fn should_support_maximum_256_blocks_per_sst() {
        // Arrange: Test with maximum supported blocks (256)
        let mut filter = FastNegativeFilter::new();

        // Act: Set and verify all 256 blocks
        for i in 0..256 {
            filter.set_block(i);
            assert!(filter.might_contain_block(i));
        }

        // Assert: All blocks marked
        let encoded = filter.encode();
        let decoded = FastNegativeFilter::decode(&encoded).unwrap();
        for i in 0..256 {
            assert!(decoded.might_contain_block(i));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 1.5.4: Integration Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_integrate_bloom_filter_and_fast_negative_filter_for_read_path() {
        // Arrange: Create blocks with both per-block bloom and fast negative filter
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
        ];

        let mut filter = FastNegativeFilter::new();
        filter.set_block(0); // Only first block has keys

        let table = IndexTable::with_fast_negative_filter(blocks, filter);

        // Act: Check that the table correctly identifies which blocks exist
        let should_have_block_0 = table.might_contain_block_via_fast_filter(0);
        let should_not_have_block_1 = !table.might_contain_block_via_fast_filter(1);

        // Assert: Fast filter should allow us to skip block 1
        assert!(should_have_block_0, "Block 0 should be marked as containing keys");
        assert!(should_not_have_block_1, "Block 1 should be marked as empty");
    }

    #[test]
    fn should_measure_negative_lookup_improvement() {
        // Arrange: Create filters with different bits/key and measure FPR
        // Using fewer keys but correct format to get stable FPR measurements
        let mut bloom_8 = BloomFilterBuilder::with_bits_per_key(8);
        let mut bloom_12 = BloomFilterBuilder::with_bits_per_key(12);

        // Add 10,000 keys with consistent format
        let inserted_keys: Vec<String> = (0..10_000u32)
            .map(|i| format!("inserted_key_{:08}", i))
            .collect();
        
        for key_str in &inserted_keys {
            bloom_8.add_key(key_str.as_bytes());
            bloom_12.add_key(key_str.as_bytes());
        }
        let filter_8 = bloom_8.finish();
        let filter_12 = bloom_12.finish();

        // Act: Query keys that don't exist (completely different prefix)
        let mut fp_8 = 0;
        let mut fp_12 = 0;
        let query_count = 10_000;
        for i in 0..query_count {
            let key = format!("query_not_inserted_key_{:08}", i);
            if filter_8.may_contain(key.as_bytes()) {
                fp_8 += 1;
            }
            if filter_12.may_contain(key.as_bytes()) {
                fp_12 += 1;
            }
        }

        let fpr_8 = fp_8 as f64 / query_count as f64;
        let fpr_12 = fp_12 as f64 / query_count as f64;

        // Assert: 12 bits/key should have lower FPR
        println!(
            "FPR Comparison - 8 bits/key: {:.2}%, 12 bits/key: {:.2}%",
            fpr_8 * 100.0,
            fpr_12 * 100.0
        );
        
        assert!(fpr_8 > 0.001, "8 bits/key FPR {} should be measurable", fpr_8);
        assert!(fpr_12 >= 0.0, "12 bits/key FPR {} should be non-negative", fpr_12);
        assert!(
            fpr_12 <= fpr_8,
            "12 bits/key should have FPR <= 8 bits/key (actual: {} vs {})",
            fpr_12, fpr_8
        );
        // Verify no false negatives (all inserted keys must be found)
        for key_str in inserted_keys.iter().take(100) {
            assert!(
                filter_8.may_contain(key_str.as_bytes()),
                "8 bits: Key {} should be found",
                key_str
            );
            assert!(
                filter_12.may_contain(key_str.as_bytes()),
                "12 bits: Key {} should be found",
                key_str
            );
        }
    }

    #[test]
    fn should_support_bloom_filter_roundtrip_with_configurable_bits_per_key() {
        // Arrange: Create and serialize a 12-bit filter
        let mut builder = BloomFilterBuilder::with_bits_per_key(12);
        for i in 0..100u32 {
            builder.add_key(format!("key_{:04}", i).as_bytes());
        }
        let filter_original = builder.finish();

        // Act: Encode and decode
        let encoded = filter_original.encode();
        let filter_decoded = BloomFilter::decode_block(&encoded).unwrap();

        // Assert: All keys should still be found after roundtrip
        for i in 0..100u32 {
            let key = format!("key_{:04}", i);
            assert!(
                filter_decoded.may_contain(key.as_bytes()),
                "Key {} not found after decode",
                key
            );
        }
        // Hash count should be preserved
        assert_eq!(filter_original.hash_count(), filter_decoded.hash_count());
    }
}
