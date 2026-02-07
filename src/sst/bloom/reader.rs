//! Bloom filter reader for querying filters during SST reads

use super::writer::{BloomFilterOps, BloomTestResult};
use crate::common::{MidgeError, MidgeResult};
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// Seeds for the two base hashes (must match writer)
const SEED1: u64 = 0x9E3779B185EBCA87;
const SEED2: u64 = 0xC2B2AE3D27D4EB4F;

/// Bloom filter reader for querying an existing filter
#[derive(Debug, Clone)]
pub struct BloomReader {
    pub(super) bits: Vec<u8>,
    pub(super) num_bits: usize,
    pub(super) key_count: usize,
    pub(super) k: u8,
}

impl BloomReader {
    /// Deserialize a bloom filter from bytes (format-aware with corruption checks).
    ///
    /// Format: [num_bits: u32][key_count: u32][k: u8][bits...]
    /// Validates: num_bits > 0, k ∈ [1,8], bits size matches
    pub fn deserialize(data: &[u8]) -> MidgeResult<Self> {
        if data.len() < 9 {
            return Err(MidgeError::Corruption(
                "Bloom filter data too short".to_string(),
            ));
        }

        let num_bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let key_count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let k = data[8];

        // Safety: num_bits=0 would cause mod-by-zero panic in contains()
        if num_bits == 0 {
            return Err(MidgeError::Corruption(
                "Bloom filter num_bits cannot be zero — data corruption detected".to_string(),
            ));
        }

        // Safety: k ∈ [1,8] (adaptive k range from writer)
        if !matches!(k, 1..=8) {
            return Err(MidgeError::Corruption(format!(
                "Bloom filter k={} out of range [1, 8] — data corruption detected",
                k
            )));
        }

        let bits = data[9..].to_vec();
        let expected_bytes = num_bits.div_ceil(8);

        if bits.len() != expected_bytes {
            return Err(MidgeError::Corruption(
                "Bloom filter size mismatch".to_string(),
            ));
        }

        Ok(Self {
            bits,
            num_bits,
            key_count,
            k,
        })
    }

    /// Get the number of keys that were added to this filter
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// Get the number of bits in the filter
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Calculate the false positive rate of this filter
    pub fn estimated_fpr(&self) -> f64 {
        if self.key_count == 0 {
            return 0.0;
        }

        // FPR = (1 - e^(-k*n/m))^k
        // where k = number of hash functions, n = key count, m = number of bits
        let exponent = -(self.k as f64) * (self.key_count as f64) / (self.num_bits as f64);
        (1.0 - exponent.exp()).powi(self.k as i32)
    }
}

impl BloomFilterOps for BloomReader {
    fn contains(&self, key: &[u8]) -> BloomTestResult {
        // Compute both hashes ONCE (Kirsch-Mitzenmacher optimization)
        let h1 = xxh3_64_with_seed(key, SEED1);
        let h2 = xxh3_64_with_seed(key, SEED2);
        let num_bits = self.num_bits as u64;

        for i in 0..(self.k as u64) {
            let combined = h1.wrapping_add(i.wrapping_mul(h2));
            let bit_index = (combined % num_bits) as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;

            // Safety: bit_index < num_bits, so byte_index < bits.len() always
            // when the filter was constructed correctly. But we keep the check
            // for defense against corruption.
            if byte_index >= self.bits.len() {
                return BloomTestResult::DefinitelyNotPresent;
            }

            let is_set = (self.bits[byte_index] & (1 << bit_offset)) != 0;
            if !is_set {
                return BloomTestResult::DefinitelyNotPresent;
            }
        }

        BloomTestResult::MightBePresent
    }

    fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Format (NEW): [num_bits: u32][key_count: u32][k: u8][bits...]
        result.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        result.extend_from_slice(&(self.key_count as u32).to_le_bytes());
        result.push(self.k);
        result.extend_from_slice(&self.bits);

        result
    }
}

impl BloomReader {
    /// Batch query multiple keys for better CPU cache locality
    ///
    /// Returns results without allocating per-key; enables vectorized processing
    /// and early termination for downstream pipelines.
    pub fn contains_batch(&self, keys: &[&[u8]]) -> super::batch::BatchBloomResults {
        use super::batch::BatchBloomResults;

        let mut results = BatchBloomResults::new(keys.len());
        let mut positive_count = 0;
        let mut negative_count = 0;

        for (idx, key) in keys.iter().enumerate() {
            let result = self.contains(key);
            results.results[idx] = match result {
                BloomTestResult::MightBePresent => {
                    positive_count += 1;
                    0
                }
                BloomTestResult::DefinitelyNotPresent => {
                    negative_count += 1;
                    1
                }
            };
        }

        results.positive_count = positive_count;
        results.negative_count = negative_count;
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::bloom::BloomWriter;

    #[test]
    fn should_deserialize_valid_filter() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"key1");
        let serialized = writer.serialize();

        // Act
        let reader = BloomReader::deserialize(&serialized);

        // Assert
        assert!(reader.is_ok());
        let reader = reader.unwrap();
        assert_eq!(reader.key_count(), 1);
    }

    #[test]
    fn should_return_error_on_truncated_data() {
        // Arrange
        let data = vec![1, 2, 3];

        // Act
        let result = BloomReader::deserialize(&data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_test_keys_consistently_after_deserialization() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"hello");
        let serialized = writer.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Act
        let result = reader.contains(b"hello");

        // Assert
        assert_eq!(result, BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_calculate_estimated_fpr() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        for i in 0..100 {
            writer.insert(format!("key{}", i).as_bytes());
        }
        let reader = writer.finish();

        // Act
        let fpr = reader.estimated_fpr();

        // Assert
        assert!(fpr > 0.0);
        assert!(fpr < 0.05); // Should be close to 1%
    }

    #[test]
    fn should_round_trip_serialization() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        for i in 0..50 {
            writer.insert(format!("key{}", i).as_bytes());
        }

        // Act
        let serialized1 = writer.serialize();
        let reader = BloomReader::deserialize(&serialized1).unwrap();
        let serialized2 = reader.serialize();

        // Assert
        assert_eq!(serialized1, serialized2);
    }

    // ===== New comprehensive invariant tests =====

    #[test]
    fn should_reject_empty_deserialization_data() {
        // Arrange
        let data = vec![];

        // Act
        let result = BloomReader::deserialize(&data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_data_with_size_mismatch() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();

        // Act - truncate more aggressively to create a definite mismatch
        // Current format is: [4 bytes num_bits][4 bytes key_count][1 byte k][bits...]
        // We truncate to a length that doesn't match either OLD or NEW format
        let bits_len = serialized.len() - 9;
        let invalid_len = 9 + bits_len - 2; // Remove 2 bytes from expected bits
        serialized.truncate(invalid_len);
        let result = BloomReader::deserialize(&serialized);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_preserve_key_count_through_serialization() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        for i in 0..25 {
            writer.insert(format!("key{}", i).as_bytes());
        }

        // Act
        let serialized = writer.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Assert
        assert_eq!(reader.key_count(), 25);
    }

    #[test]
    fn should_preserve_num_bits_through_serialization() {
        // Arrange
        let mut writer = BloomWriter::new(200, 0.01);
        writer.insert(b"test");
        let original_reader = writer.finish();
        let original_num_bits = original_reader.num_bits();

        // Act
        let serialized = original_reader.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Assert
        assert_eq!(reader.num_bits(), original_num_bits);
    }

    #[test]
    fn should_have_zero_fpr_for_empty_filter() {
        // Arrange
        let writer = BloomWriter::new(100, 0.01);
        let reader = writer.finish();

        // Act
        let fpr = reader.estimated_fpr();

        // Assert
        assert_eq!(fpr, 0.0);
    }

    #[test]
    fn should_have_bounded_fpr_value() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        for i in 0..100 {
            writer.insert(format!("key{}", i).as_bytes());
        }

        // Act
        let reader = writer.finish();
        let fpr = reader.estimated_fpr();

        // Assert
        assert!(fpr >= 0.0, "FPR must be non-negative");
        assert!(fpr <= 1.0, "FPR must be <= 1.0");
    }

    #[test]
    fn should_increase_fpr_with_more_keys() {
        // Arrange
        let mut writer_low = BloomWriter::new(100, 0.01);
        for i in 0..10 {
            writer_low.insert(format!("key{}", i).as_bytes());
        }

        let mut writer_high = BloomWriter::new(100, 0.01);
        for i in 0..100 {
            writer_high.insert(format!("key{}", i).as_bytes());
        }

        // Act
        let fpr_low = writer_low.finish().estimated_fpr();
        let fpr_high = writer_high.finish().estimated_fpr();

        // Assert
        assert!(fpr_high > fpr_low, "More keys should increase FPR");
    }

    #[test]
    fn should_test_missing_keys_consistently() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"present");
        let serialized = writer.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Act
        let result_missing = reader.contains(b"missing");
        let result_other = reader.contains(b"other");

        // Assert (missing keys should be definitely not present)
        assert_eq!(result_missing, BloomTestResult::DefinitelyNotPresent);
        assert_eq!(result_other, BloomTestResult::DefinitelyNotPresent);
    }

    #[test]
    fn should_handle_empty_key_in_reader() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"");
        let serialized = writer.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Act
        let result = reader.contains(b"");

        // Assert
        assert_eq!(result, BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_handle_large_key_in_reader() {
        // Arrange
        let large_key = vec![99u8; 5000]; // 5KB key
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(&large_key);
        let serialized = writer.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Act
        let result = reader.contains(&large_key);

        // Assert
        assert_eq!(result, BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_maintain_consistent_size_bytes() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"key1");
        writer.insert(b"key2");

        // Act
        let serialized = writer.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();
        let size_after_deser = reader.size_bytes();

        // Assert
        assert_eq!(size_after_deser, writer.size_bytes());
    }

    #[test]
    fn should_hash_consistently_across_instances() {
        // Arrange
        let key = b"consistent_key";

        // Act
        let mut writer1 = BloomWriter::new(100, 0.01);
        writer1.insert(key);
        let serialized = writer1.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Assert - Reader and Writer should give same result
        assert_eq!(
            writer1.contains(key),
            reader.contains(key),
            "Reader and Writer must agree on membership"
        );
    }

    #[test]
    fn should_handle_many_keys_in_filter() {
        // Arrange
        let mut writer = BloomWriter::new(10000, 0.01);
        for i in 0..10000 {
            writer.insert(format!("key{}", i).as_bytes());
        }

        // Act
        let serialized = writer.serialize();
        let reader = BloomReader::deserialize(&serialized).unwrap();

        // Assert
        assert_eq!(reader.key_count(), 10000);
        // Spot check some keys
        assert_eq!(reader.contains(b"key0"), BloomTestResult::MightBePresent);
        assert_eq!(reader.contains(b"key5000"), BloomTestResult::MightBePresent);
        assert_eq!(reader.contains(b"key9999"), BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_reject_incomplete_metadata() {
        // Arrange
        let incomplete = vec![1, 2, 3, 4]; // Only 4 bytes, need 9

        // Act
        let result = BloomReader::deserialize(&incomplete);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_preserve_adaptive_k_when_serialized() {
        // Arrange
        let mut writer_small = BloomWriter::new(10, 0.01);
        for i in 0..10 {
            writer_small.insert(format!("key{}", i).as_bytes());
        }

        let mut writer_large = BloomWriter::new(10000, 0.01);
        for i in 0..10000 {
            writer_large.insert(format!("key{}", i).as_bytes());
        }

        // Act
        let reader_small = writer_small.finish();
        let reader_large = writer_large.finish();

        // Assert - larger filter should have higher or equal k
        assert!(reader_small.k >= 1 && reader_small.k <= 8);
        assert!(reader_large.k >= 1 && reader_large.k <= 8);
        assert!(
            reader_large.k >= reader_small.k,
            "Larger filter should have higher k"
        );

        // Verify k is preserved through serialization
        let serialized_large = reader_large.serialize();
        let deserialized_large = BloomReader::deserialize(&serialized_large).unwrap();
        assert_eq!(
            deserialized_large.k, reader_large.k,
            "k must be preserved through serialization"
        );
    }

    #[test]
    fn should_deterministically_hash_same_keys() {
        // Arrange
        let mut writer1 = BloomWriter::new(100, 0.01);
        writer1.insert(b"key1");
        writer1.insert(b"key2");

        let mut writer2 = BloomWriter::new(100, 0.01);
        writer2.insert(b"key1");
        writer2.insert(b"key2");

        // Act
        let serialized1 = writer1.serialize();
        let serialized2 = writer2.serialize();

        // Assert - same keys should produce same serialized bytes (deterministic hash)
        assert_eq!(serialized1, serialized2, "Hashing must be deterministic");
    }

    #[test]
    fn should_have_reasonable_fpr_with_many_keys() {
        // Arrange - Build filter with deterministic keys and measure false positives
        let mut writer = BloomWriter::new(1000, 0.01);
        for i in 0..1000 {
            writer.insert(format!("key{:010}", i).as_bytes());
        }
        let reader = writer.finish();

        // Act
        let estimated_fpr = reader.estimated_fpr();

        // Assert - FPR should be reasonable (not pathological)
        // For 1000 keys with 1% target, should be bounded
        assert!(
            estimated_fpr > 0.0,
            "FPR should be positive for full filter"
        );
        assert!(estimated_fpr < 0.05, "FPR should be reasonable (bounded)");
    }

    #[test]
    fn should_query_batch_keys() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"key1");
        writer.insert(b"key2");
        writer.insert(b"key3");
        let reader = writer.finish();

        let key_refs = vec![
            b"key1".as_ref(),
            b"key2".as_ref(),
            b"key3".as_ref(),
            b"key4".as_ref(),
            b"key5".as_ref(),
        ];

        // Act
        let results = reader.contains_batch(&key_refs);

        // Assert
        assert_eq!(results.results.len(), 5);
        assert_eq!(results.positive_count, 3); // key1, key2, key3
        assert_eq!(results.negative_count, 2); // key4, key5
        assert!(results.is_positive(0)); // key1
        assert!(results.is_positive(1)); // key2
        assert!(results.is_positive(2)); // key3
        assert!(results.is_negative(3)); // key4
        assert!(results.is_negative(4)); // key5
    }

    #[test]
    fn should_batch_results_match_individual_queries() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        for i in 0..50 {
            writer.insert(format!("key{}", i).as_bytes());
        }
        let reader = writer.finish();

        let test_keys = ["key0", "key25", "key49", "unknown1", "unknown2"];
        let key_refs: Vec<&[u8]> = test_keys.iter().map(|k| k.as_bytes()).collect();

        // Act
        let batch_results = reader.contains_batch(&key_refs);
        let individual_results: Vec<_> = test_keys
            .iter()
            .map(|k| reader.contains(k.as_bytes()))
            .collect();

        // Assert - batch results should match individual queries
        for i in 0..test_keys.len() {
            let batch_is_present = batch_results.is_positive(i);
            let individual_is_present =
                matches!(individual_results[i], BloomTestResult::MightBePresent);
            assert_eq!(
                batch_is_present, individual_is_present,
                "Result mismatch at index {} for key '{}'",
                i, test_keys[i]
            );
        }
    }

    #[test]
    fn should_batch_with_empty_keys() {
        // Arrange
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"key1");
        let reader = writer.finish();

        // Act
        let results = reader.contains_batch(&[]);

        // Assert
        assert_eq!(results.results.len(), 0);
        assert_eq!(results.positive_count, 0);
        assert_eq!(results.negative_count, 0);
    }

    #[test]
    fn should_reject_k_out_of_bounds_zero() {
        // Arrange - Create corrupted data with k=0
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();
        serialized[8] = 0; // Set k=0 (invalid)

        // Act
        let result = BloomReader::deserialize(&serialized);

        // Assert - Should reject as corruption
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MidgeError::Corruption(_)));
    }

    #[test]
    fn should_reject_k_out_of_bounds_too_large() {
        // Arrange - Create corrupted data with k=255
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();
        serialized[8] = 255; // Set k=255 (invalid)

        // Act
        let result = BloomReader::deserialize(&serialized);

        // Assert - Should reject as corruption
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MidgeError::Corruption(_)));
    }

    #[test]
    fn should_reject_k_boundary_below_range() {
        // Arrange - Create corrupted data with k=0
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();
        serialized[8] = 0;

        // Act
        let result = BloomReader::deserialize(&serialized);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_accept_k_boundary_lower_valid() {
        // Arrange - Valid minimum k=1
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();
        serialized[8] = 1; // k=1 is valid minimum

        // Act
        let result = BloomReader::deserialize(&serialized);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_accept_k_boundary_upper_valid() {
        // Arrange - Valid maximum k=8
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();
        serialized[8] = 8; // k=8 is valid maximum

        // Act
        let result = BloomReader::deserialize(&serialized);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_k_boundary_above_range() {
        // Arrange - Invalid k=9
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();
        serialized[8] = 9; // k=9 is out of range

        // Act
        let result = BloomReader::deserialize(&serialized);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_num_bits_zero() {
        // Arrange - Create corrupted data with num_bits=0 (mod-by-zero safety)
        let mut writer = BloomWriter::new(100, 0.01);
        writer.insert(b"test");
        let mut serialized = writer.serialize();
        // Set num_bits to 0: bytes [0..4]
        serialized[0] = 0;
        serialized[1] = 0;
        serialized[2] = 0;
        serialized[3] = 0;

        // Act
        let result = BloomReader::deserialize(&serialized);

        // Assert - Should reject as corruption to prevent mod-by-zero panic
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MidgeError::Corruption(_)));
    }
}
