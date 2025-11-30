//! Bloom filter implementation tuned for hotpath performance.
//!
//! # Design Goals
//!
//! - **Safe Rust**: Only `unsafe` for bounds-checked bit ops (with `debug_assert`).
//! - **Blocked layout**: Bits grouped into fixed-size blocks for cache locality.
//! - **Single hash**: One 64-bit xxh3 hash per key, with Kirsch-Mitzenmacher double hashing.
//! - **Power-of-two optimization**: Block counts are powers of two for fast masking.
//! - **Bounded allocation**: `MAX_FILTER_BITS` cap prevents runaway memory usage.
//!
//! # Blocked Layout
//!
//! The bitset is divided into 256-byte (2048-bit) blocks. For any given key, all `k`
//! hash probes land within a single block. This provides:
//!
//! - **Cache efficiency**: One cache line fetch (or two adjacent) per query.
//! - **Predictable memory access**: No random jumps across the entire bitset.
//! - **Slightly higher FPR**: Theoretical FPR increases ~15-20% vs unblocked, but
//!   cache benefits far outweigh this for real workloads.
//!
//! # Encoding Format
//!
//! Wire format is unchanged for compatibility:
//! ```text
//! [version:u8 | k:u32le | m:u32le | n:u32le | bitset...]
//! ```
//!
//! # Double Hashing (Kirsch-Mitzenmacher)
//!
//! Given a single 64-bit hash `h`, we derive `k` probe positions as:
//! ```text
//! h_i = (h_lo + i * h_hi) mod m
//! ```
//! where `h_lo = h as u32` and `h_hi = (h >> 32) | 1` (forced odd for full period).

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
    /// Total number of bits (m). Always `<= bytes.len() * 8`.
    bits: u32,
    /// Number of hash functions (k).
    hashes: u32,
    /// Number of keys inserted (n).
    keys: u32,
    /// Number of logical blocks (for blocked layout).
    blocks: u32,
    /// Mask for block index when `blocks` is power of two; otherwise 0.
    block_mask: u32,
    /// Whether this filter uses blocked layout.
    blocked: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Encoding format version.
const BLOOM_FMT_VERSION: u8 = 1;

/// Header size: version(1) + k(4) + m(4) + n(4) = 13 bytes.
const BLOOM_HEADER_LEN: usize = 1 + 4 + 4 + 4;

/// Maximum filter size: 256 MiB (2 Gbit). Prevents runaway allocations.
const MAX_FILTER_BITS: u32 = 256 * 1024 * 1024 * 8;

/// ln(2) for optimal hash count calculation.
const LN2: f64 = std::f64::consts::LN_2;

/// ln(2)^2 for optimal bits-per-key calculation.
const LN2_SQ: f64 = LN2 * LN2;

/// Block size in bytes (256 bytes = 2 cache lines on most architectures).
const BLOCK_BYTES: usize = 256;

/// Block size in bits (2048).
const BLOCK_BITS: u32 = (BLOCK_BYTES * 8) as u32;

/// Mask for intra-block bit index (2047).
const BLOCK_BITS_MASK: u32 = BLOCK_BITS - 1;

/// Hash seed for xxh3 (arbitrary prime-ish constant).
const HASH_SEED: u64 = 0x9E37_79B1_85EB_CA87;

impl BloomFilter {
    // ─────────────────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────────────────

    /// Create a new bloom filter for an expected number of keys and a target false-positive rate.
    ///
    /// The filter size is rounded up to a power-of-two number of 2048-bit blocks
    /// and capped at `MAX_FILTER_BITS` to prevent runaway allocations.
    #[inline]
    pub fn new(num_keys: usize, false_positive_rate: f64) -> Self {
        let bits_per_key = Self::optimal_bits_per_key(false_positive_rate);
        // Cap early to prevent overflow in subsequent calculations.
        let raw_bits = num_keys
            .saturating_mul(bits_per_key)
            .max(64)
            .min(MAX_FILTER_BITS as usize) as u32;

        // Round up to whole blocks, prefer power-of-two for fast masking.
        let mut num_blocks = raw_bits.div_ceil(BLOCK_BITS).max(1);
        if !num_blocks.is_power_of_two() {
            num_blocks = num_blocks.next_power_of_two();
        }

        // Cap total bits to prevent OOM.
        let bits = cmp::min(num_blocks * BLOCK_BITS, MAX_FILTER_BITS);
        let num_blocks = bits / BLOCK_BITS;
        let byte_len = (bits as usize).div_ceil(8);
        let hashes = Self::optimal_hash_count(bits_per_key);

        let block_mask = if num_blocks.is_power_of_two() {
            num_blocks - 1
        } else {
            0
        };

        Self {
            bytes: vec![0u8; byte_len],
            bits,
            hashes,
            keys: 0,
            blocks: num_blocks,
            block_mask,
            blocked: true,
        }
    }

    /// Create bloom filter from existing raw bitset (legacy compatibility).
    #[inline]
    pub fn from_bytes(data: &[u8], hash_count: u32, keys_count: u32) -> MidgeResult<Self> {
        if data.is_empty() {
            return Err(MidgeError::InvalidData(
                "Bloom filter data too small".into(),
            ));
        }
        let bits = (data.len() * 8) as u32;
        Ok(Self::from_parts(
            data.to_vec(),
            bits,
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
        let required = bit_count.div_ceil(8);
        if data.len() < required {
            return Err(MidgeError::InvalidData(
                "Bloom filter data incomplete".into(),
            ));
        }
        Ok(Self::from_parts(
            data[..required].to_vec(),
            bit_count as u32,
            hash_count,
            keys_count,
        ))
    }

    /// Shared constructor for all paths that already have validated bytes.
    #[inline]
    fn from_parts(bytes: Vec<u8>, bits: u32, hashes: u32, keys: u32) -> Self {
        let (blocks, block_mask, blocked) = Self::compute_block_layout(bits);
        Self {
            bytes,
            bits,
            hashes: cmp::max(hashes, 1),
            keys,
            blocks,
            block_mask,
            blocked,
        }
    }

    /// Compute block layout from bit count, deciding whether blocked mode is safe.
    ///
    /// Blocked mode requires:
    /// - `bits >= BLOCK_BITS`
    /// - `bits` is a multiple of `BLOCK_BITS`
    #[inline]
    fn compute_block_layout(bits: u32) -> (u32, u32, bool) {
        if bits >= BLOCK_BITS && bits.is_multiple_of(BLOCK_BITS) {
            let blocks = bits / BLOCK_BITS;
            let mask = if blocks.is_power_of_two() {
                blocks - 1
            } else {
                0
            };
            (blocks, mask, true)
        } else {
            // Legacy / small filters fall back to linear layout.
            (0, 0, false)
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Insertion (hot path)
    // ─────────────────────────────────────────────────────────────────────────

    /// Add a key to the bloom filter.
    #[inline]
    pub fn add(&mut self, key: &[u8]) {
        if self.bytes.is_empty() || self.bits == 0 {
            return;
        }

        let (h, step) = Self::double_hash(key);

        if self.blocked {
            self.add_blocked(h, step);
        } else {
            self.add_linear(h, step);
        }

        self.keys = self.keys.saturating_add(1);
    }

    #[inline]
    fn add_blocked(&mut self, h: u64, step: u32) {
        let block_idx = Self::block_index(h, self.blocks, self.block_mask) as usize;
        let base_bit = block_idx * BLOCK_BITS as usize;

        let mut probe = h as u32;
        for _ in 0..self.hashes {
            probe = probe.wrapping_add(step);
            let bit_in_block = (probe & BLOCK_BITS_MASK) as usize;
            Self::set_bit(&mut self.bytes, base_bit + bit_in_block);
        }
    }

    #[inline]
    fn add_linear(&mut self, h: u64, step: u32) {
        let m = self.bits;
        let mask = m.wrapping_sub(1);
        let use_mask = m & mask == 0; // power-of-two

        let mut probe = h as u32;
        for _ in 0..self.hashes {
            probe = probe.wrapping_add(step);
            let idx = if use_mask {
                (probe & mask) as usize
            } else {
                (probe % m) as usize
            };
            Self::set_bit(&mut self.bytes, idx);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query (hot path)
    // ─────────────────────────────────────────────────────────────────────────

    /// Check if a key might be in the filter.
    ///
    /// Returns `true` if the key is possibly present (may be a false positive),
    /// or `false` if the key is definitely absent.
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bytes.is_empty() || self.bits == 0 {
            return false;
        }

        let (h, step) = Self::double_hash(key);

        if self.blocked {
            self.query_blocked(h, step)
        } else {
            self.query_linear(h, step)
        }
    }

    #[inline]
    fn query_blocked(&self, h: u64, step: u32) -> bool {
        let block_idx = Self::block_index(h, self.blocks, self.block_mask) as usize;
        let base_bit = block_idx * BLOCK_BITS as usize;

        let mut probe = h as u32;
        for _ in 0..self.hashes {
            probe = probe.wrapping_add(step);
            let bit_in_block = (probe & BLOCK_BITS_MASK) as usize;
            if !Self::test_bit(&self.bytes, base_bit + bit_in_block) {
                return false;
            }
        }
        true
    }

    #[inline]
    fn query_linear(&self, h: u64, step: u32) -> bool {
        let m = self.bits;
        let mask = m.wrapping_sub(1);
        let use_mask = m & mask == 0;

        let mut probe = h as u32;
        for _ in 0..self.hashes {
            probe = probe.wrapping_add(step);
            let idx = if use_mask {
                (probe & mask) as usize
            } else {
                (probe % m) as usize
            };
            if !Self::test_bit(&self.bytes, idx) {
                return false;
            }
        }
        true
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Serialization
    // ─────────────────────────────────────────────────────────────────────────

    /// Encode to bytes with metadata header.
    ///
    /// Layout: `[version(1) | k(u32le) | m(u32le) | n(u32le) | bitset...]`
    #[inline]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(BLOOM_HEADER_LEN + self.bytes.len());
        buf.extend_from_slice(&[BLOOM_FMT_VERSION]);
        buf.extend_from_slice(&self.hashes.to_le_bytes());
        buf.extend_from_slice(&self.bits.to_le_bytes());
        buf.extend_from_slice(&self.keys.to_le_bytes());
        buf.extend_from_slice(&self.bytes);
        buf.freeze()
    }

    /// Decode from an SST filter block payload.
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

        let hashes = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        let bits = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);
        let keys = u32::from_le_bytes([data[9], data[10], data[11], data[12]]);

        let required = bits.div_ceil(8) as usize;
        let bitset = &data[BLOOM_HEADER_LEN..];
        if bitset.len() < required {
            return Err(MidgeError::InvalidData("Bloom bitset incomplete".into()));
        }

        Ok(Self::from_parts(
            bitset[..required].to_vec(),
            bits,
            hashes,
            keys,
        ))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    /// Number of hash functions (k).
    #[inline]
    pub fn hash_count(&self) -> u32 {
        self.hashes
    }

    /// Number of keys inserted (n).
    #[inline]
    pub fn keys_count(&self) -> u32 {
        self.keys
    }

    /// Total number of bits (m).
    #[inline]
    pub fn bit_count(&self) -> usize {
        self.bits as usize
    }

    /// Estimate false positive rate based on current fill.
    ///
    /// Uses the standard formula: `(1 - e^(-kn/m))^k`
    #[inline]
    pub fn estimated_fpr(&self) -> f64 {
        if self.bits == 0 || self.keys == 0 {
            return 0.0;
        }
        let k = self.hashes as f64;
        let n = self.keys as f64;
        let m = self.bits as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Estimate serialized size for `num_keys` at `fp_rate`.
    #[inline]
    pub fn estimate_size(num_keys: usize, fp_rate: f64) -> usize {
        let bits = Self::optimal_bits_per_key(fp_rate)
            .saturating_mul(num_keys)
            .max(64);
        BLOOM_HEADER_LEN + bits.div_ceil(8)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Optimal bits/key for target FPR: `m/n ≈ -ln(p) / ln²(2)`
    #[inline]
    fn optimal_bits_per_key(fpr: f64) -> usize {
        let p = fpr.clamp(1e-9, 0.5);
        (-p.ln() / LN2_SQ).ceil() as usize
    }

    /// Optimal hash count: `k ≈ (m/n) * ln(2)`, capped at 10.
    #[inline]
    fn optimal_hash_count(bits_per_key: usize) -> u32 {
        let k = (bits_per_key as f64 * LN2).round();
        k.clamp(1.0, 10.0) as u32
    }

    /// Kirsch-Mitzenmacher double hashing from a single 64-bit hash.
    ///
    /// - `h`: full 64-bit hash (low 32 bits used as base, full value for block selection)
    /// - `step`: `(h >> 32) | 1` (upper 32 bits, forced odd for full period)
    #[inline(always)]
    fn double_hash(key: &[u8]) -> (u64, u32) {
        let h = xxh3_64_with_seed(key, HASH_SEED);
        // Upper 32 bits provide independent randomness; force odd for coprime period.
        let step = ((h >> 32) as u32) | 1;
        (h, step)
    }

    /// Compute block index from hash value.
    #[inline(always)]
    fn block_index(h: u64, blocks: u32, mask: u32) -> u32 {
        if mask != 0 {
            (h as u32) & mask
        } else {
            (h % blocks as u64) as u32
        }
    }

    /// Set a bit in the byte array (LSB-first per byte).
    #[inline]
    fn set_bit(bytes: &mut [u8], idx: usize) {
        let byte_idx = idx >> 3;
        let bit_off = idx & 7;
        debug_assert!(byte_idx < bytes.len());
        // SAFETY: debug_assert guards bounds; unchecked keeps hot path tight.
        unsafe {
            *bytes.get_unchecked_mut(byte_idx) |= 1u8 << bit_off;
        }
    }

    /// Test a bit in the byte array.
    #[inline]
    fn test_bit(bytes: &[u8], idx: usize) -> bool {
        let byte_idx = idx >> 3;
        let bit_off = idx & 7;
        debug_assert!(byte_idx < bytes.len());
        unsafe { (*bytes.get_unchecked(byte_idx) & (1u8 << bit_off)) != 0 }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Builder
// ─────────────────────────────────────────────────────────────────────────────

/// Bloom filter builder for constructing filters during SST creation.
///
/// This builder streams keys directly into the bloom filter without storing them,
/// enabling memory-efficient construction of massive SST files.
///
/// # Example
///
/// ```ignore
/// let mut builder = BloomFilterBuilder::with_expected_keys(10_000, 10);
/// for key in keys {
///     builder.add_key(&key);
/// }
/// let filter = builder.finish();
/// ```
#[derive(Debug, Clone)]
pub struct BloomFilterBuilder {
    filter: BloomFilter,
    /// Target FPR (retained for potential future resize logic).
    #[allow(dead_code)]
    target_fpr: f64,
}

impl BloomFilterBuilder {
    /// Default initial capacity when expected key count is unknown.
    const DEFAULT_CAPACITY: usize = 1024;

    /// Create a builder with a target false-positive rate.
    ///
    /// Uses a default initial capacity of 1024 keys.
    #[inline]
    pub fn new(false_positive_rate: f64) -> Self {
        let fpr = false_positive_rate.clamp(1e-9, 0.5);
        Self {
            filter: BloomFilter::new(Self::DEFAULT_CAPACITY, fpr),
            target_fpr: fpr,
        }
    }

    /// Create a builder with a target bits-per-key ratio.
    ///
    /// Common values:
    /// - 10 bits/key ≈ 1% FPR
    /// - 14 bits/key ≈ 0.1% FPR
    /// - 20 bits/key ≈ 0.01% FPR
    #[inline]
    pub fn with_bits_per_key(bits_per_key: u32) -> Self {
        let fpr = Self::fpr_from_bits_per_key(bits_per_key);
        Self::new(fpr)
    }

    /// Create a builder with known expected capacity (most efficient).
    #[inline]
    pub fn with_expected_keys(expected_keys: usize, bits_per_key: u32) -> Self {
        let fpr = Self::fpr_from_bits_per_key(bits_per_key);
        Self {
            filter: BloomFilter::new(expected_keys, fpr),
            target_fpr: fpr,
        }
    }

    /// Add a key to the filter (streaming mode).
    #[inline]
    pub fn add_key(&mut self, key: &[u8]) {
        self.filter.add(key);
    }

    /// Returns `true` if no keys have been added.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.filter.keys == 0
    }

    /// Number of keys added so far.
    #[inline]
    pub fn keys_count(&self) -> usize {
        self.filter.keys as usize
    }

    /// Bit count of the underlying filter.
    #[inline]
    pub fn bit_count(&self) -> usize {
        self.filter.bits as usize
    }

    /// Estimated FPR based on current fill level.
    #[inline]
    pub fn estimated_fpr(&self) -> f64 {
        self.filter.estimated_fpr()
    }

    /// Consume the builder and return the finished filter.
    #[inline]
    pub fn finish(self) -> BloomFilter {
        self.filter
    }

    /// Convert bits-per-key to approximate FPR.
    #[inline]
    fn fpr_from_bits_per_key(bpk: u32) -> f64 {
        // FPR ≈ (1 - e^(-k/bpk))^k where k ≈ bpk * ln(2)
        // Simplified lookup for common values:
        match bpk {
            0..=5 => 0.10,
            6..=8 => 0.05,
            9..=11 => 0.01,
            12..=15 => 0.001,
            16..=20 => 0.0001,
            _ => 0.00001,
        }
    }
}

impl Default for BloomFilterBuilder {
    /// Default builder with 1% FPR target.
    fn default() -> Self {
        Self::new(0.01)
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

    #[test]
    fn should_estimate_fpr_within_reasonable_bounds() {
        // Arrange: 1000 keys at 10 bits/key ≈ 1% theoretical FPR
        let mut builder = BloomFilterBuilder::with_expected_keys(1_000, 10);
        for i in 0..1_000u32 {
            builder.add_key(format!("k{i}").as_bytes());
        }
        let filter = builder.finish();

        // Act
        let estimated = filter.estimated_fpr();

        // Assert: should be in the ballpark of 1% (blocked layout rounds up,
        // so actual FPR is often better than theoretical; allow wide range)
        assert!(
            (0.0001..=0.05).contains(&estimated),
            "estimated FPR {estimated} out of expected range"
        );
    }

    #[test]
    fn should_report_zero_fpr_when_empty() {
        let filter = BloomFilter::new(100, 0.01);
        assert_eq!(filter.estimated_fpr(), 0.0);
    }

    #[test]
    fn should_use_default_builder_with_one_percent_fpr() {
        let builder = BloomFilterBuilder::default();
        assert!(builder.is_empty());
        // Default capacity is 1024, should have reasonable bit count
        assert!(builder.bit_count() >= 64);
    }

    #[test]
    fn should_cap_filter_size_at_max_bits() {
        // Request a large filter that would exceed MAX_FILTER_BITS
        // MAX_FILTER_BITS = 256 * 1024 * 1024 * 8 = ~2 billion bits
        // At 10 bits/key, that's ~200M keys; request more to trigger cap
        let filter = BloomFilter::new(500_000_000, 0.01);

        // Should be capped at MAX_FILTER_BITS (256 MiB * 8)
        assert!(filter.bit_count() <= MAX_FILTER_BITS as usize);
    }

    #[test]
    fn should_derive_step_from_upper_hash_bits() {
        // The step should be derived from the upper 32 bits, not a rotation
        let (h, step) = BloomFilter::double_hash(b"test");

        // Step should be ((h >> 32) as u32) | 1
        let expected_step = ((h >> 32) as u32) | 1;
        assert_eq!(step, expected_step);

        // Step must always be odd
        assert_eq!(step & 1, 1);
    }

    #[test]
    fn should_expose_builder_accessors() {
        let mut builder = BloomFilterBuilder::with_expected_keys(500, 10);
        assert!(builder.is_empty());
        assert_eq!(builder.keys_count(), 0);

        builder.add_key(b"hello");
        assert!(!builder.is_empty());
        assert_eq!(builder.keys_count(), 1);

        // bit_count should be positive
        assert!(builder.bit_count() > 0);

        // estimated_fpr should be very small with only 1 key in a large filter
        assert!(builder.estimated_fpr() < 0.001);
    }
}
