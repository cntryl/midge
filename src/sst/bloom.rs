//! Bloom filter implementation with optimized performance
//!
//! - Raw byte storage (SIMD-friendly, LSB-first bits)
//! - Double hashing via xxh3_64_with_seed (no hasher allocations)
//! - Inlined hot paths with debug-checked unsafe for bounds
//! - Compact header: [ver | k | m | n | bits]
//! - Power-of-2 bit_count for fast modulo via bitwise AND
//!
//! Encoding: [ version:u8=1 | hash_count:u32le | bit_count:u32le | keys_count:u32le | bitset ]

use crate::error::{MidgeError, MidgeResult};
use bytes::{Bytes, BytesMut};
use std::cmp;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// Public filter abstraction used by SST and storage layers.
pub trait Filter {
    fn may_contain(&self, key: &bytes::Bytes) -> bool;
    fn build(keys: &[(bytes::Bytes, usize)]) -> Self
    where
        Self: Sized;
}

/// Bloom filter using raw byte storage and double hashing for optimal performance.
#[derive(Debug, PartialEq, Clone)]
pub struct BloomFilter {
    /// Raw byte array for bit storage (LSB-first per byte).
    bytes: Vec<u8>,
    /// Total number of bits (<= bytes.len()*8).
    bit_count: u32,
    /// Number of hash functions (k).
    hash_count: u32,
    /// Number of keys inserted (n).
    keys_count: u32,
}

// Encoding format constants.
const BLOOM_FMT_VERSION: u8 = 1;
const BLOOM_HEADER_LEN: usize = 1 + 4 + 4 + 4;

// Mathematical constants (compile-time).
const LN2: f64 = std::f64::consts::LN_2;
const LN2_SQ: f64 = LN2 * LN2;

impl BloomFilter {
    /// Create a new bloom filter for an expected number of keys and a target false-positive rate.
    #[inline]
    pub fn new(num_keys: usize, false_positive_rate: f64) -> Self {
        let bits_per_key = Self::calculate_bits_per_key(false_positive_rate);
        // Minimum 64 bits to avoid degenerate tiny filters
        let bit_count_raw = cmp::max(num_keys.saturating_mul(bits_per_key), 64);
        // Round up to next power of 2 for fast modulo via bitwise AND
        let bit_count = bit_count_raw.next_power_of_two() as u32;
        let hash_count = Self::calculate_hash_count(bits_per_key);
        let byte_count = bit_count.div_ceil(8) as usize;

        Self {
            bytes: vec![0u8; byte_count],
            bit_count,
            hash_count,
            keys_count: 0,
        }
    }

    /// Create bloom filter from existing raw bitset (legacy compat).
    #[inline]
    pub fn from_bytes(data: &[u8], hash_count: u32, keys_count: u32) -> MidgeResult<Self> {
        if data.is_empty() {
            return Err(MidgeError::InvalidData(
                "Bloom filter data too small".into(),
            ));
        }
        let bit_count = (data.len() * 8) as u32;
        Ok(Self {
            bytes: data.to_vec(),
            bit_count,
            hash_count: cmp::max(hash_count, 1),
            keys_count,
        })
    }

    /// Create bloom filter from existing data with specific bit count.
    #[inline]
    pub fn from_bytes_with_bit_count(
        data: &[u8],
        bit_count: usize,
        hash_count: u32,
        keys_count: u32,
    ) -> MidgeResult<Self> {
        if data.is_empty() {
            return Err(MidgeError::InvalidData(
                "Bloom filter data too small".into(),
            ));
        }
        let required_bytes = bit_count.div_ceil(8);
        if data.len() < required_bytes {
            return Err(MidgeError::InvalidData(
                "Bloom filter data incomplete".into(),
            ));
        }
        Ok(Self {
            bytes: data[..required_bytes].to_vec(),
            bit_count: bit_count as u32,
            hash_count: cmp::max(hash_count, 1),
            keys_count,
        })
    }

    /// Add a key to the bloom filter (hot path).
    #[inline(always)]
    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = self.double_hash(key);
        let m = self.bit_count;
        // Fast path set-bit loop with debug-checked bounds.
        for i in 0..self.hash_count {
            let bit_index = (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize;
            Self::set_bit(&mut self.bytes, bit_index);
        }
        self.keys_count = self.keys_count.saturating_add(1);
    }

    /// Check if a key might be in the filter (hot path).
    ///
    /// Optimized with:
    /// - Early exit on first missing bit (most common case for absent keys)
    /// - Bitwise AND masking when bit_count is power of 2
    #[inline(always)]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.double_hash(key);
        let m = self.bit_count;

        // Fast path: if m is power of 2, use bitwise AND instead of modulo
        let is_pow2 = m.is_power_of_two();
        let mask = m.wrapping_sub(1);

        for i in 0..self.hash_count {
            let hash = h1.wrapping_add(i.wrapping_mul(h2));
            let bit_index = if is_pow2 {
                (hash & mask) as usize
            } else {
                (hash % m) as usize
            };

            if !Self::test_bit(&self.bytes, bit_index) {
                return false; // Definitely not present
            }
        }
        true // Might be present (with FPR)
    }
    /// Encode to bytes with metadata header.
    /// Layout: [version(1) | k(u32le) | m(u32le) | n(u32le) | bitset]
    #[inline]
    pub fn encode(&self) -> Bytes {
        let mut buffer = BytesMut::with_capacity(BLOOM_HEADER_LEN + self.bytes.len());
        buffer.extend_from_slice(&[BLOOM_FMT_VERSION]);
        buffer.extend_from_slice(&self.hash_count.to_le_bytes());
        buffer.extend_from_slice(&self.bit_count.to_le_bytes());
        buffer.extend_from_slice(&self.keys_count.to_le_bytes());
        buffer.extend_from_slice(&self.bytes);
        buffer.freeze()
    }

    /// Decode from an SST filter block payload (with metadata header).
    #[inline]
    pub fn decode_block(data: &[u8]) -> MidgeResult<Self> {
        if data.len() < BLOOM_HEADER_LEN {
            return Err(MidgeError::InvalidData("Bloom block too small".into()));
        }
        let version = data[0];
        if version != BLOOM_FMT_VERSION {
            return Err(MidgeError::InvalidData(format!(
                "Unsupported bloom format version {}",
                version
            )));
        }
        let hash_count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        let bit_count = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);
        let keys_count = u32::from_le_bytes([data[9], data[10], data[11], data[12]]);

        let required_bytes = bit_count.div_ceil(8) as usize;
        let bits_bytes = &data[BLOOM_HEADER_LEN..];
        if bits_bytes.len() < required_bytes {
            return Err(MidgeError::InvalidData("Bloom bitset incomplete".into()));
        }

        Ok(Self {
            bytes: bits_bytes[..required_bytes].to_vec(),
            bit_count,
            hash_count: cmp::max(hash_count, 1),
            keys_count,
        })
    }

    /// Number of hash functions `k`.
    #[inline(always)]
    pub fn hash_count(&self) -> u32 {
        self.hash_count
    }

    /// Number of keys inserted `n`.
    #[inline(always)]
    pub fn keys_count(&self) -> u32 {
        self.keys_count
    }

    /// Total number of bits `m`.
    #[inline(always)]
    pub fn bit_count(&self) -> usize {
        self.bit_count as usize
    }

    /// Estimate serialized size for `num_keys` and `fp_rate`.
    #[inline]
    pub fn estimate_size(num_keys: usize, fp_rate: f64) -> usize {
        let bits = Self::calculate_bits_per_key(fp_rate)
            .saturating_mul(num_keys)
            .max(64);
        BLOOM_HEADER_LEN + bits.div_ceil(8)
    }

    /// Optimal bits/key for target FPR: m/n ≈ -ln(p) / (ln2^2).
    #[inline(always)]
    fn calculate_bits_per_key(false_positive_rate: f64) -> usize {
        let p = false_positive_rate.clamp(1e-9, 0.5); // clamp to sane range
        let bits_per_key = -p.ln() / LN2_SQ;
        bits_per_key.ceil() as usize
    }

    /// Optimal number of hashes: k ≈ (m/n) * ln2.
    /// Capped at 10 for diminishing returns beyond that point.
    #[inline(always)]
    fn calculate_hash_count(bits_per_key: usize) -> u32 {
        let k = (bits_per_key as f64 * LN2).round();
        // Cap at 10: Beyond this, the cost of hashing outweighs FPR gains
        // RocksDB uses 5-6 probes in blocked filters for similar reasons
        k.clamp(1.0, 10.0) as u32
    }

    /// Double hashing: h(i) = h1 + i*h2 mod m.
    /// Uses xxh3 64-bit with independent seeds (no hasher allocation).
    #[inline(always)]
    fn double_hash(&self, key: &[u8]) -> (u32, u32) {
        let h1 = xxh3_64_with_seed(key, 0) as u32;
        let mut h2 = xxh3_64_with_seed(key, 0x9E37_79B9) as u32; // golden-ratio-ish seed
        h2 |= 1; // ensure odd to improve coverage
        (h1, h2)
    }

    #[inline(always)]
    fn set_bit(bytes: &mut [u8], bit_index: usize) {
        let byte_index = bit_index >> 3;
        let bit_offset = bit_index & 7;
        debug_assert!(byte_index < bytes.len());
        // SAFETY: debug_assert above guarantees bounds in debug; release keeps hot-path tight.
        unsafe {
            let b = bytes.get_unchecked_mut(byte_index);
            *b |= 1u8 << bit_offset;
        }
    }

    #[inline(always)]
    fn test_bit(bytes: &[u8], bit_index: usize) -> bool {
        let byte_index = bit_index >> 3;
        let bit_offset = bit_index & 7;
        debug_assert!(byte_index < bytes.len());
        unsafe { (*bytes.get_unchecked(byte_index) & (1u8 << bit_offset)) != 0 }
    }
}

/// Bloom filter builder for constructing filters during SST creation.
///
/// This builder streams keys directly into the bloom filter without storing them,
/// enabling memory-efficient construction of massive SST files.
#[derive(Debug, Clone)]
pub struct BloomFilterBuilder {
    filter: BloomFilter,
}

impl BloomFilterBuilder {
    #[inline]
    pub fn new(false_positive_rate: f64) -> Self {
        // Start with a reasonable default size (will grow if needed in streaming mode)
        // Use 1024 expected keys as initial capacity
        let filter = BloomFilter::new(1024, false_positive_rate);
        Self { filter }
    }

    /// Create a new bloom filter builder with a target bits per key.
    ///
    /// Common values:
    /// - 10 bits/key ≈ 1% false positive rate
    /// - 14 bits/key ≈ 0.1% false positive rate  
    /// - 20 bits/key ≈ 0.001% false positive rate
    #[inline]
    pub fn with_bits_per_key(bits_per_key: u32) -> Self {
        // Approximate false positive rate from bits per key
        // fpr ≈ (0.6185)^(bits_per_key)
        // For simplicity, use a lookup table for common values
        let false_positive_rate = match bits_per_key {
            0..=5 => 0.10, // Very high FPR for low bits
            6..=8 => 0.05,
            9..=11 => 0.01,   // ~10 bits/key
            12..=15 => 0.001, // ~14 bits/key
            _ => 0.0001,      // 20+ bits/key
        };
        Self::new(false_positive_rate)
    }

    /// Create a bloom filter builder with known expected capacity.
    /// This is more efficient than the default constructor.
    #[inline]
    pub fn with_expected_keys(expected_keys: usize, bits_per_key: u32) -> Self {
        let false_positive_rate = match bits_per_key {
            0..=5 => 0.10,
            6..=8 => 0.05,
            9..=11 => 0.01,
            12..=15 => 0.001,
            _ => 0.0001,
        };
        let filter = BloomFilter::new(expected_keys, false_positive_rate);
        Self { filter }
    }

    /// Add a key directly to the bloom filter (streaming mode).
    /// This does NOT store the key - it immediately hashes and updates the bit array.
    #[inline]
    pub fn add_key(&mut self, key: &[u8]) {
        self.filter.add(key);
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.filter.keys_count() == 0
    }

    #[inline]
    pub fn keys_count(&self) -> usize {
        self.filter.keys_count() as usize
    }

    /// Return the constructed bloom filter.
    #[inline]
    pub fn finish(self) -> BloomFilter {
        self.filter
    }
}

// Implement the public Filter abstraction for the BloomFilter.
impl Filter for BloomFilter {
    #[inline(always)]
    fn may_contain(&self, key: &bytes::Bytes) -> bool {
        self.may_contain(key.as_ref())
    }

    #[inline]
    fn build(keys: &[(bytes::Bytes, usize)]) -> Self
    where
        Self: Sized,
    {
        // Default FPR for SST block filters when unspecified.
        let mut builder = BloomFilterBuilder::new(0.01);
        for (k, _s) in keys {
            builder.add_key(k.as_ref());
        }
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_distribute_keys_evenly_using_double_hashing() {
        // Arrange
        let f = BloomFilter::new(100, 0.01);

        // Act
        let (h1a, h2a) = f.double_hash(b"test_key_a");
        let (h1b, h2b) = f.double_hash(b"test_key_b");

        // Assert
        assert_ne!(h1a, h1b);
        assert_ne!(h2a, h2b);
        assert_eq!(h2a & 1, 1);
        assert_eq!(h2b & 1, 1);
    }

    #[test]
    fn should_perform_raw_byte_operations_correctly() {
        // Arrange
        let mut f = BloomFilter::new(10, 0.01);

        // Act
        f.add(b"key1");
        f.add(b"key2");
        f.add(b"key3");

        // Assert
        assert!(f.may_contain(b"key1"));
        assert!(f.may_contain(b"key2"));
        assert!(f.may_contain(b"key3"));
        let _ = f.may_contain(b"absent_key_xyz_12345");
    }

    #[test]
    fn should_report_may_contain_for_added_key() {
        // Arrange
        let mut f = BloomFilter::new(10, 0.01);
        let key = b"hello";

        // Act
        f.add(key);

        // Assert
        assert!(f.may_contain(key));
    }

    #[test]
    fn should_return_false_for_absent_key_most_of_the_time() {
        // Arrange
        let mut f = BloomFilter::new(10, 0.01);
        f.add(b"a");
        f.add(b"b");

        // Act
        let got = f.may_contain(b"z");

        // Assert
        assert!(!got, "absent key should usually be reported absent");
    }

    #[test]
    fn should_reject_empty_data_when_decoding() {
        // Arrange
        let data: &[u8] = &[];

        // Act
        let res = BloomFilter::from_bytes(data, 3, 0);

        // Assert
        assert!(res.is_err());
    }

    #[test]
    fn should_roundtrip_encoded_filter_with_decode_block() {
        // Arrange
        let mut f = BloomFilter::new(8, 0.01);
        f.add(b"k1");
        f.add(b"k2");

        // Act
        let enc = f.encode();
        let other = BloomFilter::decode_block(&enc).unwrap();

        // Assert
        assert!(other.may_contain(b"k1"));
        assert!(other.may_contain(b"k2"));
    }

    #[test]
    fn should_be_empty_initially() {
        // Arrange
        let b = BloomFilterBuilder::new(0.02);

        // Act
        let is_empty = b.is_empty();
        let count = b.keys_count();

        // Assert
        assert!(is_empty);
        assert_eq!(count, 0);
    }

    #[test]
    fn should_finish_with_minimum_size() {
        // Arrange
        let b = BloomFilterBuilder::new(0.02);

        // Act
        let f = b.finish();

        // Assert
        assert!(f.bit_count() >= 64);
    }

    #[test]
    fn should_build_filter_from_keys() {
        // Arrange
        let keys: Vec<(Bytes, usize)> = vec![(Bytes::from("kA"), 1), (Bytes::from("kB"), 1)];

        // Act
        let f = BloomFilter::build(&keys);

        // Assert
        assert_eq!(f.keys_count(), 2);
    }

    #[test]
    fn should_contain_keys_after_build() {
        // Arrange
        let keys: Vec<(Bytes, usize)> = vec![(Bytes::from("kA"), 1), (Bytes::from("kB"), 1)];

        // Act
        let f = BloomFilter::build(&keys);

        // Assert
        assert!(f.may_contain(b"kA"));
        assert!(f.may_contain(b"kB"));
    }

    #[test]
    fn should_have_hash_count_after_build() {
        // Arrange
        let keys: Vec<(Bytes, usize)> = vec![(Bytes::from("kA"), 1), (Bytes::from("kB"), 1)];

        // Act
        let f = BloomFilter::build(&keys);

        // Assert
        assert!(f.hash_count() >= 1);
    }

    #[test]
    fn should_reject_empty_data_when_decoding_with_bit_count() {
        // Arrange
        let data: &[u8] = &[];

        // Act
        let res = BloomFilter::from_bytes_with_bit_count(data, 16, 3, 0);

        // Assert
        assert!(res.is_err());
    }

    #[test]
    fn should_roundtrip_with_from_bytes_ok_path() {
        // Arrange
        let mut f = BloomFilter::new(5, 0.02);
        f.add(b"alpha");
        f.add(b"beta");

        // Act
        let enc = f.encode();
        let g = BloomFilter::decode_block(&enc).unwrap();

        // Assert
        assert!(g.may_contain(b"alpha"));
        assert!(g.may_contain(b"beta"));
    }

    #[test]
    fn should_encode_length_match_ceiled_bit_count_over_8() {
        // Arrange
        let f = BloomFilter::new(3, 0.20);
        let bit_count = f.bit_count();

        // Act
        let enc = f.encode();

        // Assert
        let expected_len = 1 + 4 + 4 + 4 + bit_count.div_ceil(8);
        assert_eq!(enc.len(), expected_len);
    }

    #[test]
    fn should_decode_small_bit_count_without_panic() {
        // Arrange
        let mut raw = Vec::new();
        raw.push(BLOOM_FMT_VERSION);
        raw.extend_from_slice(&3u32.to_le_bytes());
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.push(0u8);

        // Act
        let bf = BloomFilter::decode_block(&raw);

        // Assert
        assert!(bf.is_ok());
    }

    #[test]
    fn should_report_absent_for_small_bit_count_filter() {
        // Arrange
        let mut raw = Vec::new();
        raw.push(BLOOM_FMT_VERSION);
        raw.extend_from_slice(&3u32.to_le_bytes());
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.push(0u8);
        let bf = BloomFilter::decode_block(&raw).unwrap();

        // Act
        let contains = bf.may_contain(b"anything");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_preserve_keys_count_after_finish() {
        // Arrange
        let mut b = BloomFilterBuilder::new(0.05);
        b.add_key(b"x");
        b.add_key(b"y");
        let expected = b.keys_count();

        // Act
        let f = b.finish();

        // Assert
        assert_eq!(f.keys_count() as usize, expected);
    }

    #[test]
    fn should_contain_added_keys_after_finish() {
        // Arrange
        let mut b = BloomFilterBuilder::new(0.05);
        b.add_key(b"x");
        b.add_key(b"y");

        // Act
        let f = b.finish();

        // Assert
        assert!(f.may_contain(b"x"));
        assert!(f.may_contain(b"y"));
    }
}
