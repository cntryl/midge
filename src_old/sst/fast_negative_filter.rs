//! Fast negative filter for streaming optimization (Phase 1.5)
//!
//! This module implements an L1-cache-friendly negative filter that complements
//! per-block bloom filters. It uses a 256-bit summary bitset (1 bit per block,
//! max 256 blocks per SST) to quickly eliminate negative lookups without touching
//! per-block blooms.
//!
//! # Design
//!
//! - Each SST can have up to 256 data blocks
//! - The filter is a 256-bit bitset (32 bytes, fits in L1 cache)
//! - Bit `i` is set if ANY block `i` contains at least one key
//! - A cleared bit `i` guarantees no key in range [0, ∞) is in block `i`
//! - Loaded once at SST open, checked before per-block blooms
//!
//! # Wire Format
//!
//! ```text
//! [bitset_bytes (32 bytes of dense bits)]
//! ```
//!
//! Each byte holds 8 block bits (LSB-first). Bit position = block index.

use crate::error::{MidgeError, MidgeResult};
use bytes::Bytes;

/// Maximum blocks per SST (limiting factor for dense 256-bit bitset).
pub const MAX_BLOCKS_PER_SST: usize = 256;

/// Size of the fast negative filter in bytes (32 bytes = 256 bits = 256 blocks).
pub const FAST_NEGATIVE_FILTER_BYTES: usize = 32;

/// Fast negative filter: a compact bitset for 256 blocks.
///
/// Each bit represents whether a block might contain keys.
/// Used for fast negative lookups before checking per-block blooms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FastNegativeFilter {
    /// 32-byte bitset (LSB-first per byte)
    bitset: [u8; FAST_NEGATIVE_FILTER_BYTES],
}

impl FastNegativeFilter {
    /// Create a new, empty fast negative filter (all bits cleared).
    #[inline]
    pub fn new() -> Self {
        Self {
            bitset: [0u8; FAST_NEGATIVE_FILTER_BYTES],
        }
    }

    /// Create from an existing 32-byte bitset.
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> MidgeResult<Self> {
        if bytes.len() != FAST_NEGATIVE_FILTER_BYTES {
            return Err(MidgeError::InvalidData(format!(
                "FastNegativeFilter: expected {} bytes, got {}",
                FAST_NEGATIVE_FILTER_BYTES,
                bytes.len()
            )));
        }
        let mut bitset = [0u8; FAST_NEGATIVE_FILTER_BYTES];
        bitset.copy_from_slice(bytes);
        Ok(Self { bitset })
    }

    /// Mark block `block_index` as potentially containing keys.
    ///
    /// # Panics
    /// Panics if `block_index >= 256`.
    #[inline]
    pub fn set_block(&mut self, block_index: usize) {
        assert!(
            block_index < MAX_BLOCKS_PER_SST,
            "block_index {} out of range [0, {})",
            block_index,
            MAX_BLOCKS_PER_SST
        );
        let byte_idx = block_index / 8;
        let bit_idx = block_index % 8;
        self.bitset[byte_idx] |= 1u8 << bit_idx;
    }

    /// Check if a block might contain keys (bit is set).
    ///
    /// Returns `false` only if the bit is explicitly cleared,
    /// guaranteeing no key in that block. Returns `true` if bit is set
    /// (key might be present, needs per-block bloom check).
    ///
    /// # Panics
    /// Panics if `block_index >= 256`.
    #[inline]
    pub fn might_contain_block(&self, block_index: usize) -> bool {
        assert!(
            block_index < MAX_BLOCKS_PER_SST,
            "block_index {} out of range [0, {})",
            block_index,
            MAX_BLOCKS_PER_SST
        );
        let byte_idx = block_index / 8;
        let bit_idx = block_index % 8;
        (self.bitset[byte_idx] & (1u8 << bit_idx)) != 0
    }

    /// Encode to 32 bytes for storage.
    #[inline]
    pub fn encode(&self) -> Bytes {
        Bytes::copy_from_slice(&self.bitset)
    }

    /// Decode from bytes.
    #[inline]
    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        Self::from_bytes(data)
    }

    /// Get reference to the underlying bitset.
    #[inline]
    pub fn bitset(&self) -> &[u8; FAST_NEGATIVE_FILTER_BYTES] {
        &self.bitset
    }

    /// Get mutable reference to the underlying bitset (for builder).
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn bitset_mut(&mut self) -> &mut [u8; FAST_NEGATIVE_FILTER_BYTES] {
        &mut self.bitset
    }
}

impl Default for FastNegativeFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_initialize_empty_filter() {
        // Arrange
        // Act
        let filter = FastNegativeFilter::new();

        // Assert
        for i in 0..256 {
            assert!(!filter.might_contain_block(i));
        }
    }

    #[test]
    fn should_check_block_bits_after_setting() {
        // Arrange
        let mut filter = FastNegativeFilter::new();
        filter.set_block(0);
        filter.set_block(5);
        filter.set_block(255);

        // Act
        // Assert
        assert!(filter.might_contain_block(0));
        assert!(!filter.might_contain_block(1));
        assert!(filter.might_contain_block(5));
        assert!(!filter.might_contain_block(4));
        assert!(filter.might_contain_block(255));
        assert!(!filter.might_contain_block(254));
    }

    #[test]
    fn should_roundtrip_encode_decode() {
        // Arrange
        let mut filter = FastNegativeFilter::new();
        filter.set_block(10);
        filter.set_block(100);
        filter.set_block(255);
        let encoded = filter.encode();

        // Act
        let decoded = FastNegativeFilter::decode(&encoded).unwrap();

        // Assert
        assert_eq!(filter, decoded);
        assert!(decoded.might_contain_block(10));
        assert!(decoded.might_contain_block(100));
        assert!(decoded.might_contain_block(255));
        assert!(!decoded.might_contain_block(11));
    }

    #[test]
    fn should_reject_invalid_size() {
        // Arrange
        // Act
        let result1 = FastNegativeFilter::from_bytes(&[0u8; 16]);
        let result2 = FastNegativeFilter::from_bytes(&[0u8; 48]);

        // Assert
        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[test]
    #[should_panic(expected = "block_index")]
    fn should_panic_on_block_index_out_of_range() {
        // Arrange
        let mut filter = FastNegativeFilter::new();

        // Act & Assert: Should panic
        filter.set_block(256);
    }

    #[test]
    fn should_use_lsb_first_layout() {
        // Arrange
        let mut filter = FastNegativeFilter::new();
        for i in 0..8 {
            filter.set_block(i);
        }

        // Act
        // Assert
        assert_eq!(filter.bitset[0], 0xFF);
        for i in 1..FAST_NEGATIVE_FILTER_BYTES {
            assert_eq!(filter.bitset[i], 0);
        }
    }

    #[test]
    fn should_have_32_byte_encoding() {
        // Arrange
        let filter = FastNegativeFilter::new();

        // Act
        let encoded = filter.encode();

        // Assert
        assert_eq!(encoded.len(), FAST_NEGATIVE_FILTER_BYTES);
    }

    #[test]
    fn should_default_to_empty_filter() {
        // Arrange
        // Act
        let filter = FastNegativeFilter::default();

        // Assert
        for i in 0..256 {
            assert!(!filter.might_contain_block(i));
        }
    }
}
