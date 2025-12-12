//! Bloom filter reader for querying filters during SST reads

use super::writer::{BloomFilterOps, BloomTestResult};
use crate::common::{MidgeError, MidgeResult};

/// Number of hash functions to use (must match writer)
const HASH_COUNT: usize = 2;

/// Bloom filter reader for querying an existing filter
#[derive(Debug, Clone)]
pub struct BloomReader {
    pub(super) bits: Vec<u8>,
    pub(super) num_bits: usize,
    pub(super) key_count: usize,
}

impl BloomReader {
    /// Deserialize a bloom filter from bytes
    pub fn deserialize(data: &[u8]) -> MidgeResult<Self> {
        if data.len() < 8 {
            return Err(MidgeError::Corruption(
                "Bloom filter data too short".to_string(),
            ));
        }

        let num_bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let key_count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

        let bits = data[8..].to_vec();
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
        let exponent = -(HASH_COUNT as f64) * (self.key_count as f64) / (self.num_bits as f64);
        (1.0 - exponent.exp()).powi(HASH_COUNT as i32)
    }
}

impl BloomFilterOps for BloomReader {
    fn contains(&self, key: &[u8]) -> BloomTestResult {
        for i in 0..HASH_COUNT {
            let hash = Self::hash(key, i);
            let bit_index = hash % self.num_bits;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;

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

        // Format: [num_bits: u32][key_count: u32][bits: variable]
        result.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        result.extend_from_slice(&(self.key_count as u32).to_le_bytes());
        result.extend_from_slice(&self.bits);

        result
    }
}

impl BloomReader {
    /// Hash function using simple bit mixing (must match writer)
    fn hash(key: &[u8], seed: usize) -> usize {
        let mut hash: u64 = seed as u64;

        for &byte in key {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }

        hash = hash ^ (hash >> 33);
        hash = hash.wrapping_mul(0xff51afd7ed558ccd);

        hash as usize
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

        // Act - truncate bits section
        serialized.truncate(serialized.len() - 1);
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
        // Arrange & Act
        let mut writer_low = BloomWriter::new(100, 0.01);
        for i in 0..10 {
            writer_low.insert(format!("key{}", i).as_bytes());
        }
        let fpr_low = writer_low.finish().estimated_fpr();

        let mut writer_high = BloomWriter::new(100, 0.01);
        for i in 0..100 {
            writer_high.insert(format!("key{}", i).as_bytes());
        }
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
        let incomplete = vec![1, 2, 3, 4]; // Only 4 bytes, need 8

        // Act
        let result = BloomReader::deserialize(&incomplete);

        // Assert
        assert!(result.is_err());
    }
}
