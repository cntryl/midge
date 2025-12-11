//! Bloom filter writer for building filters during SST creation

use super::reader::BloomReader;

/// Number of hash functions to use (double hashing)
const HASH_COUNT: usize = 2;

/// Bloom filter membership test result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomTestResult {
    /// Key is definitely not in the set (no false negatives)
    DefinitelyNotPresent,
    /// Key might be in the set (could be false positive)
    MightBePresent,
}

impl BloomTestResult {
    /// Returns true if the key might be present (useful for conditional logic)
    pub fn might_be_present(&self) -> bool {
        matches!(self, BloomTestResult::MightBePresent)
    }

    /// Returns true if the key is definitely not present
    pub fn definitely_not_present(&self) -> bool {
        matches!(self, BloomTestResult::DefinitelyNotPresent)
    }
}

/// Trait for bloom filter operations
pub trait BloomFilterOps: Send + Sync + std::fmt::Debug {
    /// Test if a key might be in the filter
    fn contains(&self, key: &[u8]) -> BloomTestResult;

    /// Get the size of the filter in bytes
    fn size_bytes(&self) -> usize;

    /// Serialize the filter to bytes
    fn serialize(&self) -> Vec<u8>;
}

/// Bloom filter writer that builds a filter from keys during SST creation
#[derive(Debug, Clone)]
pub struct BloomWriter {
    /// Bit vector (using u8 array for simplicity)
    bits: Vec<u8>,
    /// Number of bits in the filter
    num_bits: usize,
    /// False positive rate (default 1% = 0.01)
    #[allow(dead_code)]
    fpr: f64,
    /// Number of keys added
    key_count: usize,
}

impl BloomWriter {
    /// Create a new bloom filter writer with estimated key count and false positive rate
    ///
    /// # Arguments
    /// * `estimated_keys` - Expected number of keys in the SST
    /// * `false_positive_rate` - Target false positive rate (default 0.01 for 1%)
    pub fn new(estimated_keys: usize, false_positive_rate: f64) -> Self {
        let num_bits = Self::calculate_bit_size(estimated_keys, false_positive_rate);
        let num_bytes = (num_bits + 7) / 8;

        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            fpr: false_positive_rate,
            key_count: 0,
        }
    }

    /// Create a bloom filter with default parameters (1% FPR)
    pub fn with_defaults(estimated_keys: usize) -> Self {
        Self::new(estimated_keys, 0.01)
    }

    /// Add a key to the bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..HASH_COUNT {
            let hash = Self::hash(key, i);
            let bit_index = hash % self.num_bits;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;

            if byte_index < self.bits.len() {
                self.bits[byte_index] |= 1 << bit_offset;
            }
        }
        self.key_count += 1;
    }

    /// Calculate optimal bit size using standard bloom filter formula:
    /// m = -n * ln(p) / ln(2)^2
    /// where n = number of items, p = false positive rate, m = number of bits
    fn calculate_bit_size(n: usize, p: f64) -> usize {
        if n == 0 {
            return 0;
        }

        let ln_p = p.ln();
        let ln_2_sq = 2.0_f64.ln().powi(2);
        let m = -(n as f64) * ln_p / ln_2_sq;
        (m.ceil() as usize).max(64) // Minimum 64 bits
    }

    /// Hash function using simple bit mixing
    fn hash(key: &[u8], seed: usize) -> usize {
        let mut hash: u64 = seed as u64;

        for &byte in key {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }

        hash = hash ^ (hash >> 33);
        hash = hash.wrapping_mul(0xff51afd7ed558ccd);

        hash as usize
    }

    /// Finalize and return the bloom filter reader
    pub fn finish(self) -> BloomReader {
        BloomReader {
            bits: self.bits,
            num_bits: self.num_bits,
            key_count: self.key_count,
        }
    }

    /// Get the current size in bytes
    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    /// Get the actual key count
    pub fn key_count(&self) -> usize {
        self.key_count
    }
}

impl BloomFilterOps for BloomWriter {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_contain_key_after_insert() {
        // Arrange
        let mut filter = BloomWriter::new(100, 0.01);
        let key = b"hello";

        // Act
        filter.insert(&key[..]);
        let result = filter.contains(&key[..]);

        // Assert
        assert_eq!(result, BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_return_definitely_not_present_for_missing_keys() {
        // Arrange
        let mut filter = BloomWriter::new(100, 0.01);
        filter.insert(b"hello");

        // Act
        let result = filter.contains(b"world");

        // Assert
        assert_eq!(result, BloomTestResult::DefinitelyNotPresent);
    }

    #[test]
    fn should_handle_multiple_keys() {
        // Arrange
        let mut filter = BloomWriter::new(1000, 0.01);
        let keys = vec![b"key1", b"key2", b"key3"];

        // Act
        for key in &keys {
            filter.insert(&key[..]);
        }

        // Assert
        for key in &keys {
            assert_eq!(filter.contains(&key[..]), BloomTestResult::MightBePresent);
        }
        assert_eq!(
            filter.contains(b"nonexistent"),
            BloomTestResult::DefinitelyNotPresent
        );
    }

    #[test]
    fn should_calculate_correct_bit_size() {
        // Arrange & Act
        let size_100 = BloomWriter::calculate_bit_size(100, 0.01);
        let size_1000 = BloomWriter::calculate_bit_size(1000, 0.01);

        // Assert
        assert!(size_100 > 0);
        assert!(size_1000 > size_100);
        assert!(size_100 >= 64);
    }

    #[test]
    fn should_serialize_bloom_filter() {
        // Arrange
        let mut filter = BloomWriter::new(100, 0.01);
        filter.insert(b"test");

        // Act
        let serialized = filter.serialize();
        let expected_len = 8 + filter.bits.len();

        // Assert
        assert_eq!(serialized.len(), expected_len);
        assert!(serialized.len() > 8); // Has metadata + bits
    }
}
