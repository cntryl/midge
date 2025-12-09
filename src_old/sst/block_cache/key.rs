//! Block identity types for the block cache.
//!
//! `BlockKey` uniquely identifies a block within the storage engine by combining
//! file identity, offset, block type, and column family. Hashing is optimized for
//! use as a sharding key and hash table lookup.

use std::hash::{Hash, Hasher};

/// The type of block stored in the cache.
///
/// Different block types may have different caching priorities or accounting rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BlockKind {
    /// User data block from an SST file.
    Data = 0,
    /// Index block for locating data blocks.
    Index = 1,
    /// Bloom filter block for key existence checks.
    Filter = 2,
    /// Metadata block (properties, stats, etc.).
    Meta = 3,
    /// Compression dictionary block.
    CompressionDict = 4,
}

impl BlockKind {
    /// Convert to a single byte for compact representation.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Unique identifier for a block in the cache.
///
/// The key is designed to be:
/// - **Compact**: fits in 24 bytes (no heap allocation).
/// - **Fast to hash**: all fields are integers; no string hashing.
/// - **CF-aware**: enables per-column-family accounting and isolation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockKey {
    /// SST file number (globally unique within the DB).
    pub file_number: u64,
    /// Byte offset of the block within the file.
    pub block_offset: u64,
    /// Type of block (data, index, filter, etc.).
    pub block_kind: BlockKind,
    /// Column family ID for per-CF accounting.
    pub cf_id: u32,
}

impl BlockKey {
    /// Create a new block key.
    #[inline]
    pub const fn new(
        file_number: u64,
        block_offset: u64,
        block_kind: BlockKind,
        cf_id: u32,
    ) -> Self {
        Self {
            file_number,
            block_offset,
            block_kind,
            cf_id,
        }
    }

    /// Compute a 64-bit hash suitable for shard selection.
    ///
    /// This uses a fast mixer rather than the full `Hash` trait to
    /// give a quick shard index without building a `Hasher`.
    ///
    /// Note: Returns a non-zero hash (0 is reserved for empty buckets in the hash table).
    #[inline]
    pub fn shard_hash(&self) -> u64 {
        // FxHash-style mixing: fast and good distribution.
        const K: u64 = 0x517cc1b727220a95;
        let mut h = self.file_number.wrapping_mul(K);
        h ^= self.block_offset.wrapping_mul(K);
        h ^= (self.block_kind.as_u8() as u64).wrapping_mul(K);
        h ^= (self.cf_id as u64).wrapping_mul(K);
        // Ensure non-zero (0 is reserved for empty buckets)
        if h == 0 {
            h = 1;
        }
        h
    }
}

impl Hash for BlockKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_number.hash(state);
        self.block_offset.hash(state);
        self.block_kind.hash(state);
        self.cf_id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_equal_given_same_fields_when_compared() {
        let a = BlockKey::new(1, 4096, BlockKind::Data, 0);
        let b = BlockKey::new(1, 4096, BlockKind::Data, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn should_differ_given_different_offset_when_compared() {
        let a = BlockKey::new(1, 4096, BlockKind::Data, 0);
        let b = BlockKey::new(1, 8192, BlockKind::Data, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn should_differ_given_different_kind_when_compared() {
        let a = BlockKey::new(1, 4096, BlockKind::Data, 0);
        let b = BlockKey::new(1, 4096, BlockKind::Index, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn should_produce_different_shard_hash_given_different_keys_when_hashed() {
        let a = BlockKey::new(1, 0, BlockKind::Data, 0);
        let b = BlockKey::new(2, 0, BlockKind::Data, 0);
        assert_ne!(a.shard_hash(), b.shard_hash());
    }

    #[test]
    fn should_produce_same_shard_hash_given_same_key_when_hashed_twice() {
        let k = BlockKey::new(42, 65536, BlockKind::Filter, 3);
        assert_eq!(k.shard_hash(), k.shard_hash());
    }
}
