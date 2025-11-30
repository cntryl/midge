//! Concurrent bloom filter for memtable point lookup optimization.
//!
//! This provides a "hint" filter that can be used to quickly reject point lookups
//! for keys that definitely don't exist in the memtable, avoiding skiplist traversal.
//!
//! # Design Goals
//!
//! - **Lock-free**: Uses atomic bit operations for concurrent add/query.
//! - **No false negatives**: If a key was added, `may_contain` always returns true.
//! - **Low overhead**: Small memory footprint, fast hashing with xxh3.
//! - **Hint only**: This is an optimization hint; the skiplist is the source of truth.
//!
//! # Usage
//!
//! The bloom hint is optional and can be disabled if the overhead isn't worth it
//! for small memtables. It's most beneficial when:
//! - Many point lookups hit keys that don't exist
//! - The memtable is large (many keys)

use std::sync::atomic::{AtomicU64, Ordering};
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// Default number of bits in the bloom filter (64KB = 512Kbit).
/// This provides ~1% FPR for ~36K keys at 14 bits/key.
const DEFAULT_BITS: usize = 64 * 1024 * 8;

/// Number of hash functions (k). 7 is optimal for ~1% FPR at 10 bits/key.
const NUM_HASHES: u32 = 7;

/// Hash seed for xxh3.
const HASH_SEED: u64 = 0x9E37_79B1_85EB_CA87;

/// Concurrent bloom filter using atomic bit operations.
///
/// Uses AtomicU64 array for lock-free concurrent bit setting.
/// The filter size is fixed at construction time.
pub struct BloomHint {
    /// Bit array stored as AtomicU64 words.
    bits: Box<[AtomicU64]>,
    /// Total number of bits (always a multiple of 64).
    num_bits: u32,
    /// Mask for fast modulo when num_bits is power of two.
    bit_mask: u32,
}

impl BloomHint {
    /// Create a new bloom hint filter with default size (64KB).
    #[inline]
    pub fn new() -> Self {
        Self::with_bits(DEFAULT_BITS)
    }

    /// Create a bloom hint filter with specified bit count.
    ///
    /// The bit count is rounded up to the nearest multiple of 64.
    /// For best performance, use a power of two.
    #[inline]
    pub fn with_bits(bits: usize) -> Self {
        // Round up to multiple of 64
        let num_bits = (bits.max(64).div_ceil(64) * 64) as u32;
        let num_words = num_bits as usize / 64;

        // Allocate zeroed atomic array
        let bits: Vec<AtomicU64> = (0..num_words).map(|_| AtomicU64::new(0)).collect();

        let bit_mask = if num_bits.is_power_of_two() {
            num_bits - 1
        } else {
            0
        };

        Self {
            bits: bits.into_boxed_slice(),
            num_bits,
            bit_mask,
        }
    }

    /// Create a bloom hint filter sized for expected key count.
    ///
    /// Uses ~10 bits per key for ~1% false positive rate.
    #[inline]
    pub fn for_keys(expected_keys: usize) -> Self {
        let bits = (expected_keys * 10).max(64).next_power_of_two();
        Self::with_bits(bits)
    }

    /// Add a key to the bloom filter.
    ///
    /// This is lock-free and can be called concurrently from multiple threads.
    /// Uses relaxed ordering since we only need eventual visibility.
    #[inline]
    pub fn add(&self, key: &[u8]) {
        let (h, step) = Self::double_hash(key);
        self.add_hashed(h, step);
    }

    /// Add using pre-computed hash values.
    #[inline]
    fn add_hashed(&self, h: u64, step: u32) {
        let mut probe = h as u32;
        for _ in 0..NUM_HASHES {
            probe = probe.wrapping_add(step);
            let bit_idx = self.bit_index(probe);
            self.set_bit(bit_idx);
        }
    }

    /// Check if a key might be in the filter.
    ///
    /// Returns `true` if the key is possibly present (may be false positive),
    /// or `false` if the key is definitely absent (never a false negative).
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h, step) = Self::double_hash(key);
        self.may_contain_hashed(h, step)
    }

    /// Check using pre-computed hash values.
    #[inline]
    fn may_contain_hashed(&self, h: u64, step: u32) -> bool {
        let mut probe = h as u32;
        for _ in 0..NUM_HASHES {
            probe = probe.wrapping_add(step);
            let bit_idx = self.bit_index(probe);
            if !self.test_bit(bit_idx) {
                return false;
            }
        }
        true
    }

    /// Compute bit index from probe value.
    #[inline(always)]
    fn bit_index(&self, probe: u32) -> usize {
        if self.bit_mask != 0 {
            (probe & self.bit_mask) as usize
        } else {
            (probe % self.num_bits) as usize
        }
    }

    /// Set a bit using atomic OR.
    #[inline]
    fn set_bit(&self, bit_idx: usize) {
        let word_idx = bit_idx / 64;
        let bit_off = bit_idx % 64;
        // Use relaxed ordering - we only need eventual visibility
        self.bits[word_idx].fetch_or(1u64 << bit_off, Ordering::Relaxed);
    }

    /// Test a bit using atomic load.
    #[inline]
    fn test_bit(&self, bit_idx: usize) -> bool {
        let word_idx = bit_idx / 64;
        let bit_off = bit_idx % 64;
        // Use relaxed ordering - we tolerate slightly stale reads
        (self.bits[word_idx].load(Ordering::Relaxed) & (1u64 << bit_off)) != 0
    }

    /// Kirsch-Mitzenmacher double hashing from single 64-bit hash.
    #[inline(always)]
    fn double_hash(key: &[u8]) -> (u64, u32) {
        let h = xxh3_64_with_seed(key, HASH_SEED);
        // Upper 32 bits provide independent randomness; force odd for coprime period.
        let step = ((h >> 32) as u32) | 1;
        (h, step)
    }
}

impl Default for BloomHint {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: BloomHint is Send + Sync because all mutations use atomics.
unsafe impl Send for BloomHint {}
unsafe impl Sync for BloomHint {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn should_report_may_contain_for_added_key() {
        // Arrange
        let bloom = BloomHint::new();

        // Act
        bloom.add(b"hello");

        // Assert
        assert!(bloom.may_contain(b"hello"));
    }

    #[test]
    fn should_return_false_for_absent_key() {
        // Arrange
        let bloom = BloomHint::new();
        bloom.add(b"hello");

        // Act
        let result = bloom.may_contain(b"world_not_added");

        // Assert
        // Should typically be false (very low FPR for single key in large filter)
        assert!(!result);
    }

    #[test]
    fn should_have_no_false_negatives_given_many_keys() {
        // Arrange
        let bloom = BloomHint::for_keys(1000);
        let keys: Vec<String> = (0..1000).map(|i| format!("key_{:06}", i)).collect();

        // Act - add all keys
        for key in &keys {
            bloom.add(key.as_bytes());
        }

        // Assert - all keys must be found (no false negatives)
        for key in &keys {
            assert!(
                bloom.may_contain(key.as_bytes()),
                "Key {} should be found",
                key
            );
        }
    }

    #[test]
    fn should_have_low_false_positive_rate() {
        // Arrange
        let bloom = BloomHint::for_keys(1000);
        for i in 0..1000u32 {
            bloom.add(format!("key_{:06}", i).as_bytes());
        }

        // Act - test 10000 non-existent keys
        let mut false_positives = 0;
        for i in 100_000..110_000u32 {
            if bloom.may_contain(format!("key_{:06}", i).as_bytes()) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / 10_000.0;

        // Assert - FPR should be below 5% (target ~1%)
        assert!(fpr < 0.05, "FPR {} exceeds 5%", fpr);
    }

    #[test]
    fn should_support_concurrent_adds_without_data_races() {
        // Arrange
        let bloom = Arc::new(BloomHint::for_keys(10_000));
        let num_threads = 4;
        let keys_per_thread = 1000;

        // Act - concurrent inserts
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let bloom = Arc::clone(&bloom);
                thread::spawn(move || {
                    for i in 0..keys_per_thread {
                        let key = format!("t{}_key_{:06}", t, i);
                        bloom.add(key.as_bytes());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Assert - all keys should be found
        for t in 0..num_threads {
            for i in 0..keys_per_thread {
                let key = format!("t{}_key_{:06}", t, i);
                assert!(
                    bloom.may_contain(key.as_bytes()),
                    "Key {} should be found after concurrent insert",
                    key
                );
            }
        }
    }

    #[test]
    fn should_support_concurrent_reads_and_writes() {
        // Arrange
        let bloom = Arc::new(BloomHint::for_keys(1000));

        // Pre-add some keys
        for i in 0..500 {
            bloom.add(format!("pre_{:06}", i).as_bytes());
        }

        // Act - concurrent reads and writes
        let bloom_write = Arc::clone(&bloom);
        let bloom_read = Arc::clone(&bloom);

        let writer = thread::spawn(move || {
            for i in 500..1000 {
                bloom_write.add(format!("post_{:06}", i).as_bytes());
            }
        });

        let reader = thread::spawn(move || {
            let mut found = 0;
            for i in 0..500 {
                if bloom_read.may_contain(format!("pre_{:06}", i).as_bytes()) {
                    found += 1;
                }
            }
            found
        });

        writer.join().unwrap();
        let found = reader.join().unwrap();

        // Assert - pre-added keys should always be found
        assert_eq!(found, 500);
    }

    #[test]
    fn should_create_with_custom_bit_count() {
        // Arrange & Act
        let bloom = BloomHint::with_bits(1024);

        // Assert - should be at least 1024 bits (rounded to 64)
        bloom.add(b"test");
        assert!(bloom.may_contain(b"test"));
    }

    #[test]
    fn should_use_power_of_two_optimization() {
        // Arrange
        let bloom = BloomHint::with_bits(1024); // Power of two

        // Act & Assert - should work correctly
        for i in 0..100 {
            bloom.add(format!("key_{}", i).as_bytes());
        }
        for i in 0..100 {
            assert!(bloom.may_contain(format!("key_{}", i).as_bytes()));
        }
    }
}
