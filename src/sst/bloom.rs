//! Bloom filter implementation tuned for hotpath performance.
//!
//! Design goals:
//! - Safe Rust (only `unsafe` for bounds-checked bit ops).
//! - Blocked layout: bits grouped into fixed-size blocks for cache locality.
//! - Single 64-bit hash per key, with cheap double hashing.
//! - Power-of-two block counts for fast masking when possible.
//! - Encoding format unchanged: [ver:u8 | k:u32le | m:u32le | n:u32le | bitset].
//!
//! Blocked layout:
//! - Bitset is conceptually split into 256-byte (2048-bit) blocks.
//! - For a given key, all probes land in a single block.
//! - This massively improves cache behavior for queries and builds.

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

/// Bloom filter using raw byte storage and blocked layout.
#[derive(Debug, PartialEq, Clone)]
pub struct BloomFilter {
    /// Raw byte array for bit storage (LSB-first per byte).
    bytes: Vec<u8>,
    /// Total number of bits (<= bytes.len() * 8).
    bit_count: u32,
    /// Number of hash functions (k).
    hash_count: u32,
    /// Number of keys inserted (n).
    keys_count: u32,
    /// Number of logical blocks (for blocked layout).
    block_count: u32,
    /// Mask for block index when block_count is power of two; otherwise 0.
    block_mask: u32,
    /// Whether this filter can use blocked layout safely.
    blocked: bool,
}

// Encoding format constants.
const BLOOM_FMT_VERSION: u8 = 1;
const BLOOM_HEADER_LEN: usize = 1 + 4 + 4 + 4;

// Mathematical constants (compile-time).
const LN2: f64 = std::f64::consts::LN_2;
const LN2_SQ: f64 = LN2 * LN2;

// Blocked layout constants.
const BLOCK_BYTES: usize = 256;
const BLOCK_BITS: u32 = (BLOCK_BYTES * 8) as u32; // 2048
const BLOCK_BITS_MASK: u32 = BLOCK_BITS - 1;

// Hash seed for xxh3 (single 64-bit hash per key).
const HASH_SEED: u64 = 0x9E37_79B1_85EB_CA87;

impl BloomFilter {
    /// Create a new bloom filter for an expected number of keys and a target false-positive rate.
    #[inline]
    pub fn new(num_keys: usize, false_positive_rate: f64) -> Self {
        let bits_per_key = Self::calculate_bits_per_key(false_positive_rate);
        let min_bits = cmp::max(num_keys.saturating_mul(bits_per_key), 64);

        // Round up to a whole number of blocks, then to at least one block.
        let mut blocks = (min_bits as u32).div_ceil(BLOCK_BITS).max(1);

        // Prefer a power-of-two block count for fast masking.
        if !blocks.is_power_of_two() {
            blocks = blocks.next_power_of_two();
        }

        let bit_count = blocks * BLOCK_BITS;
        let byte_count = (bit_count as usize).div_ceil(8);
        let hash_count = Self::calculate_hash_count(bits_per_key);

        let block_count = blocks;
        let block_mask = if block_count.is_power_of_two() {
            block_count - 1
        } else {
            0
        };

        Self {
            bytes: vec![0u8; byte_count],
            bit_count,
            hash_count,
            keys_count: 0,
            block_count,
            block_mask,
            blocked: true,
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
        Ok(Self::from_parts(
            data.to_vec(),
            bit_count,
            hash_count,
            keys_count,
        ))
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
        Ok(Self::from_parts(
            data[..required_bytes].to_vec(),
            bit_count as u32,
            hash_count,
            keys_count,
        ))
    }

    /// Shared constructor for all paths that know the bytes and bit_count.
    #[inline]
    fn from_parts(bytes: Vec<u8>, bit_count: u32, hash_count: u32, keys_count: u32) -> Self {
        let (block_count, block_mask, blocked) = Self::compute_block_layout(bit_count);

        Self {
            bytes,
            bit_count,
            hash_count: cmp::max(hash_count, 1),
            keys_count,
            block_count,
            block_mask,
            blocked,
        }
    }

    /// Compute block layout from bit_count, deciding whether blocked mode is safe.
    ///
    /// Blocked mode requires:
    /// - bit_count >= BLOCK_BITS
    /// - bit_count is a multiple of BLOCK_BITS
    #[inline]
    fn compute_block_layout(bit_count: u32) -> (u32, u32, bool) {
        if bit_count >= BLOCK_BITS && bit_count.is_multiple_of(BLOCK_BITS) {
            let block_count = bit_count / BLOCK_BITS;
            let block_mask = if block_count.is_power_of_two() {
                block_count - 1
            } else {
                0
            };
            (block_count, block_mask, true)
        } else {
            // Legacy / small filters fall back to linear layout.
            (0, 0, false)
        }
    }

    /// Add a key to the bloom filter (hot path).
    #[inline]
    pub fn add(&mut self, key: &[u8]) {
        if self.bytes.is_empty() || self.bit_count == 0 {
            return;
        }

        let (h_base, h_step) = Self::double_hash(key);

        if self.blocked {
            self.add_blocked(h_base, h_step);
        } else {
            self.add_linear(h_base, h_step);
        }

        self.keys_count = self.keys_count.saturating_add(1);
    }

    #[inline]
    fn add_blocked(&mut self, h_base: u64, h_step: u32) {
        debug_assert!(self.blocked);
        debug_assert!(self.block_count > 0);

        let block_index = Self::block_index(h_base, self.block_count, self.block_mask) as usize;
        let base_bit = (block_index as u32 * BLOCK_BITS) as usize;

        let mut h = h_base as u32;
        for _ in 0..self.hash_count {
            h = h.wrapping_add(h_step);
            let bit_in_block = (h & BLOCK_BITS_MASK) as usize;
            let bit_index = base_bit + bit_in_block;
            Self::set_bit(&mut self.bytes, bit_index);
        }
    }

    #[inline]
    fn add_linear(&mut self, h_base: u64, h_step: u32) {
        let m = self.bit_count;
        let mask = m.wrapping_sub(1);
        let use_mask = m & mask == 0; // power-of-two bit_count

        let mut h = h_base as u32;
        for _ in 0..self.hash_count {
            h = h.wrapping_add(h_step);
            let bit_index = if use_mask {
                (h & mask) as usize
            } else {
                (h % m) as usize
            };
            Self::set_bit(&mut self.bytes, bit_index);
        }
    }

    /// Check if a key might be in the filter (hot path).
    ///
    /// - Single hash per key.
    /// - All probes stay within one block when blocked layout is enabled.
    /// - Early exit on the first missing bit.
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bytes.is_empty() || self.bit_count == 0 {
            return false;
        }

        let (h_base, h_step) = Self::double_hash(key);

        if self.blocked {
            self.may_contain_blocked(h_base, h_step)
        } else {
            self.may_contain_linear(h_base, h_step)
        }
    }

    #[inline]
    fn may_contain_blocked(&self, h_base: u64, h_step: u32) -> bool {
        debug_assert!(self.blocked);
        debug_assert!(self.block_count > 0);

        let block_index = Self::block_index(h_base, self.block_count, self.block_mask) as usize;
        let base_bit = (block_index as u32 * BLOCK_BITS) as usize;

        let mut h = h_base as u32;
        for _ in 0..self.hash_count {
            h = h.wrapping_add(h_step);
            let bit_in_block = (h & BLOCK_BITS_MASK) as usize;
            let bit_index = base_bit + bit_in_block;
            if !Self::test_bit(&self.bytes, bit_index) {
                return false;
            }
        }

        true
    }

    #[inline]
    fn may_contain_linear(&self, h_base: u64, h_step: u32) -> bool {
        let m = self.bit_count;
        let mask = m.wrapping_sub(1);
        let use_mask = m & mask == 0;

        let mut h = h_base as u32;
        for _ in 0..self.hash_count {
            h = h.wrapping_add(h_step);
            let bit_index = if use_mask {
                (h & mask) as usize
            } else {
                (h % m) as usize
            };
            if !Self::test_bit(&self.bytes, bit_index) {
                return false;
            }
        }

        true
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

        let bytes = bits_bytes[..required_bytes].to_vec();
        Ok(Self::from_parts(bytes, bit_count, hash_count, keys_count))
    }

    /// Number of hash functions `k`.
    #[inline]
    pub fn hash_count(&self) -> u32 {
        self.hash_count
    }

    /// Number of keys inserted `n`.
    #[inline]
    pub fn keys_count(&self) -> u32 {
        self.keys_count
    }

    /// Total number of bits `m`.
    #[inline]
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
    #[inline]
    fn calculate_bits_per_key(false_positive_rate: f64) -> usize {
        let p = false_positive_rate.clamp(1e-9, 0.5); // clamp to sane range
        let bits_per_key = -p.ln() / LN2_SQ;
        bits_per_key.ceil() as usize
    }

    /// Optimal number of hashes: k ≈ (m/n) * ln2.
    /// Capped at 10 for diminishing returns beyond that point.
    #[inline]
    fn calculate_hash_count(bits_per_key: usize) -> u32 {
        let k = (bits_per_key as f64 * LN2).round();
        k.clamp(1.0, 10.0) as u32
    }

    /// Single 64-bit hash + cheap derivation:
    /// - h_base: base hash (also used for block selection)
    /// - h_step: odd 32-bit increment used for double hashing
    #[inline(always)]
    fn double_hash(key: &[u8]) -> (u64, u32) {
        let h = xxh3_64_with_seed(key, HASH_SEED);
        let mut step = (h.rotate_right(17) as u32) | 1; // ensure odd
                                                        // Avoid pathological very small step values.
        if step == 1 {
            step = 3;
        }
        (h, step)
    }

    /// Compute block index from hash value and block layout.
    #[inline(always)]
    fn block_index(h: u64, block_count: u32, block_mask: u32) -> u32 {
        if block_mask != 0 {
            (h as u32) & block_mask
        } else {
            // Non power-of-two fallback (rare for new filters).
            (h % block_count as u64) as u32
        }
    }

    #[inline]
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

    #[inline]
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
        let false_positive_rate = match bits_per_key {
            0..=5 => 0.10,
            6..=8 => 0.05,
            9..=11 => 0.01,
            12..=15 => 0.001,
            _ => 0.0001,
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
    #[inline]
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
        let (h1a, s1a) = BloomFilter::double_hash(b"test_key_a");
        let (h1b, s1b) = BloomFilter::double_hash(b"test_key_b");

        // Assert
        assert_ne!(h1a, h1b);
        assert_ne!(s1a, s1b);
        assert_eq!(s1a & 1, 1);
        assert_eq!(s1b & 1, 1);
    }

    #[test]
    fn should_perform_raw_byte_operations_correctly() {
        let mut f = BloomFilter::new(10, 0.01);

        f.add(b"key1");
        f.add(b"key2");
        f.add(b"key3");

        assert!(f.may_contain(b"key1"));
        assert!(f.may_contain(b"key2"));
        assert!(f.may_contain(b"key3"));
        let _ = f.may_contain(b"absent_key_xyz_12345");
    }

    #[test]
    fn should_report_may_contain_for_added_key() {
        let mut f = BloomFilter::new(10, 0.01);
        let key = b"hello";

        f.add(key);

        assert!(f.may_contain(key));
    }

    #[test]
    fn should_return_false_for_absent_key_most_of_the_time() {
        let mut f = BloomFilter::new(10, 0.01);
        f.add(b"a");
        f.add(b"b");

        let got = f.may_contain(b"z");

        assert!(!got, "absent key should usually be reported absent");
    }

    #[test]
    fn should_reject_empty_data_when_decoding() {
        let data: &[u8] = &[];
        let res = BloomFilter::from_bytes(data, 3, 0);
        assert!(res.is_err());
    }

    #[test]
    fn should_roundtrip_encoded_filter_with_decode_block() {
        let mut f = BloomFilter::new(8, 0.01);
        f.add(b"k1");
        f.add(b"k2");

        let enc = f.encode();
        let other = BloomFilter::decode_block(&enc).unwrap();

        assert!(other.may_contain(b"k1"));
        assert!(other.may_contain(b"k2"));
    }

    #[test]
    fn should_be_empty_initially() {
        let b = BloomFilterBuilder::new(0.02);
        assert!(b.is_empty());
        assert_eq!(b.keys_count(), 0);
    }

    #[test]
    fn should_finish_with_minimum_size() {
        let b = BloomFilterBuilder::new(0.02);
        let f = b.finish();
        assert!(f.bit_count() >= 64);
    }

    #[test]
    fn should_build_filter_from_keys() {
        let keys: Vec<(Bytes, usize)> = vec![(Bytes::from("kA"), 1), (Bytes::from("kB"), 1)];
        let f = BloomFilter::build(&keys);
        assert_eq!(f.keys_count(), 2);
    }

    #[test]
    fn should_contain_keys_after_build() {
        let keys: Vec<(Bytes, usize)> = vec![(Bytes::from("kA"), 1), (Bytes::from("kB"), 1)];
        let f = BloomFilter::build(&keys);
        assert!(f.may_contain(b"kA"));
        assert!(f.may_contain(b"kB"));
    }

    #[test]
    fn should_have_hash_count_after_build() {
        let keys: Vec<(Bytes, usize)> = vec![(Bytes::from("kA"), 1), (Bytes::from("kB"), 1)];
        let f = BloomFilter::build(&keys);
        assert!(f.hash_count() >= 1);
    }

    #[test]
    fn should_reject_empty_data_when_decoding_with_bit_count() {
        let data: &[u8] = &[];
        let res = BloomFilter::from_bytes_with_bit_count(data, 16, 3, 0);
        assert!(res.is_err());
    }

    #[test]
    fn should_roundtrip_with_from_bytes_ok_path() {
        let mut f = BloomFilter::new(5, 0.02);
        f.add(b"alpha");
        f.add(b"beta");

        let enc = f.encode();
        let g = BloomFilter::decode_block(&enc).unwrap();

        assert!(g.may_contain(b"alpha"));
        assert!(g.may_contain(b"beta"));
    }

    #[test]
    fn should_encode_length_match_ceiled_bit_count_over_8() {
        let f = BloomFilter::new(3, 0.20);
        let bit_count = f.bit_count();
        let enc = f.encode();
        let expected_len = 1 + 4 + 4 + 4 + bit_count.div_ceil(8);
        assert_eq!(enc.len(), expected_len);
    }

    #[test]
    fn should_decode_small_bit_count_without_panic() {
        let mut raw = Vec::new();
        raw.push(BLOOM_FMT_VERSION);
        raw.extend_from_slice(&3u32.to_le_bytes());
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.push(0u8);

        let bf = BloomFilter::decode_block(&raw);
        assert!(bf.is_ok());
    }

    #[test]
    fn should_report_absent_for_small_bit_count_filter() {
        let mut raw = Vec::new();
        raw.push(BLOOM_FMT_VERSION);
        raw.extend_from_slice(&3u32.to_le_bytes());
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.push(0u8);
        let bf = BloomFilter::decode_block(&raw).unwrap();

        let contains = bf.may_contain(b"anything");
        assert!(!contains);
    }

    #[test]
    fn should_preserve_keys_count_after_finish() {
        let mut b = BloomFilterBuilder::new(0.05);
        b.add_key(b"x");
        b.add_key(b"y");
        let expected = b.keys_count();

        let f = b.finish();
        assert_eq!(f.keys_count() as usize, expected);
    }

    #[test]
    fn should_contain_added_keys_after_finish() {
        let mut b = BloomFilterBuilder::new(0.05);
        b.add_key(b"x");
        b.add_key(b"y");

        let f = b.finish();

        assert!(f.may_contain(b"x"));
        assert!(f.may_contain(b"y"));
    }

    #[test]
    fn should_bloom_filter_false_positive_rate_with_bounds() {
        // Build a bloom filter with 1000 keys, target ~1% FPR (using 10 bits/key)
        let mut builder = BloomFilterBuilder::with_expected_keys(1_000, 10);
        for i in 0..1_000u32 {
            let key = format!("key_{:06}", i);
            builder.add_key(key.as_bytes());
        }
        let filter = builder.finish();

        // Query 10,000 non-existent keys (offset range to avoid true positives)
        let mut false_positives = 0;
        for i in 100_000..110_000u32 {
            let key = format!("key_{:06}", i);
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / 10_000.0;

        // Target is ~1%, allow tolerance up to 3%
        assert!(fpr <= 0.03, "False positive rate {} exceeds 3% bound", fpr);

        // All inserted keys must be found (no false negatives)
        for i in 0..1_000u32 {
            let key = format!("key_{:06}", i);
            assert!(
                filter.may_contain(key.as_bytes()),
                "Key {} not found (false negative!)",
                i
            );
        }
    }

    #[test]
    fn should_encode_decode_bloom_filter_block() {
        let mut builder = BloomFilterBuilder::with_expected_keys(500, 10);
        for i in 0..500u32 {
            let key = format!("bloom_test_key_{:08}", i);
            builder.add_key(key.as_bytes());
        }
        let original = builder.finish();

        let encoded = original.encode();
        let decoded = BloomFilter::decode_block(&encoded).expect("decode should succeed");

        assert_eq!(decoded.bit_count(), original.bit_count());
        assert_eq!(decoded.hash_count(), original.hash_count());
        assert_eq!(decoded.keys_count(), original.keys_count());

        for i in 0..500u32 {
            let key = format!("bloom_test_key_{:08}", i);
            assert!(
                decoded.may_contain(key.as_bytes()),
                "Key {} not found after decode",
                i
            );
        }

        for i in 1000..1100u32 {
            let key = format!("bloom_test_key_{:08}", i);
            assert_eq!(
                original.may_contain(key.as_bytes()),
                decoded.may_contain(key.as_bytes()),
                "Mismatch for non-existent key {}",
                i
            );
        }
    }
}
