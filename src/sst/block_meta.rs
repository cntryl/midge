//! Block metadata and index table structures
//!
//! This module defines the core `BlockMeta` struct that threads through the read path,
//! iterator, and compaction logic. It encapsulates all block-level metadata needed
//! for efficient operations without recomputation.

use bytes::Bytes;
use crate::sst::format::BlockHandle;
use crate::sst::fast_negative_filter::FastNegativeFilter;
use crate::sst::sequential_access_optimizer::SequentialAccessOptimizer;
use crate::error::{MidgeError, MidgeResult};
use xxhash_rust::xxh3::xxh3_64_with_seed;
use std::cell::RefCell;
use std::fmt;

/// Per-block bloom filter (Phase 1)
///
/// TODO:
/// - Upgrade hashing to `xxh3_64` with double-hashing and `k` probes (k=5..7)
/// - Keep the on-disk wire-format stable; only change in-memory semantics.
/// - Add unit/integration tests to validate FPR and backward compatibility.
///
/// A bloom filter associated with a single data block.
/// Provides fast negative lookups: if `maybe_contains` returns false, the key is definitely not in the block.
#[derive(Clone, Debug)]
pub struct BlockBloom {
    /// Raw bloom filter bits
    bits: Vec<u8>,
    /// Capacity in bytes
    capacity_bytes: usize,
    /// Number of probes (k)
    k: u8,
}

impl BlockBloom {
    /// Create a new BlockBloom with the specified capacity in bytes and default k=5
    pub fn new(capacity_bytes: usize) -> Self {
        Self::new_with_k(capacity_bytes, 5)
    }

    /// Create a new BlockBloom with specified probe count `k`.
    pub fn new_with_k(capacity_bytes: usize, k: u8) -> Self {
        Self {
            bits: vec![0u8; capacity_bytes],
            capacity_bytes,
            k,
        }
    }

    /// Add a key to the bloom filter using double hashing (Kirsch-Mitzenmacher)
    pub fn add(&mut self, key: &[u8]) {
        let (h_lo, h_hi) = Self::double_hash(key);
        let m_bits = (self.capacity_bytes * 8) as u32;
        for i in 0..self.k as u32 {
            let probe = h_lo.wrapping_add(i.wrapping_mul(h_hi));
            let idx = (probe % m_bits) as usize;
            Self::set_bit(&mut self.bits, idx);
        }
    }

    /// Check if a key might be in the bloom filter (no false negatives, possible false positives)
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let (h_lo, h_hi) = Self::double_hash(key);
        let m_bits = (self.capacity_bytes * 8) as u32;
        for i in 0..self.k as u32 {
            let probe = h_lo.wrapping_add(i.wrapping_mul(h_hi));
            let idx = (probe % m_bits) as usize;
            if !Self::get_bit(&self.bits, idx) {
                return false;
            }
        }
        true
    }

    /// Return the capacity in bytes
    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    /// Encode bloom to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.bits.len() + 8);
        // Write capacity as varint
        buf.extend_from_slice(&(self.capacity_bytes as u32).to_le_bytes());
        // Write bloom bits
        buf.extend_from_slice(&self.bits);
        Bytes::from(buf)
    }

    /// Decode bloom from bytes
    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        if data.len() < 4 {
            return Err(MidgeError::InvalidData(
                "BlockBloom data too short".to_string(),
            ));
        }

        let capacity_bytes = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let bits = data[4..].to_vec();

        if bits.len() != capacity_bytes {
            return Err(MidgeError::InvalidData(
                "BlockBloom data size mismatch".to_string(),
            ));
        }

        Ok(Self {
            bits,
            capacity_bytes,
            k: 5, // default to 5 probes for backward compatibility
        })
    }

    /// Simple hash function for bloom filter
    #[inline]
    fn double_hash(key: &[u8]) -> (u32, u32) {
        // Use xxh3_64 as a source of randomness and split
        const HASH_SEED: u64 = 0x9E37_79B1_85EB_CA87;
        let h = xxh3_64_with_seed(key, HASH_SEED);
        let h_lo = h as u32;
        let mut h_hi = (h >> 32) as u32;
        // Force odd to avoid cycles
        if h_hi.is_multiple_of(2) {
            h_hi |= 1;
        }
        (h_lo, h_hi)
    }

    #[inline]
    fn set_bit(bits: &mut [u8], idx: usize) {
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        bits[byte_idx] |= 1 << bit_idx;
    }

    #[inline]
    fn get_bit(bits: &[u8], idx: usize) -> bool {
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        (bits[byte_idx] & (1 << bit_idx)) != 0
    }
}

/// Index entry with per-block bloom support
#[derive(Clone, Debug)]
pub struct BlockIndexEntry {
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub block_offset: u64,
    pub block_len: u32,
    pub bloom_offset: Option<u64>,
}

/// Persisted block summary; intended for inclusion in the SST meta index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSummary {
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub key_count: u32,
    pub bloom_offset: Option<u64>,
}

impl BlockSummary {
    pub fn new(min_key: Bytes, max_key: Bytes, key_count: u32, bloom_offset: Option<u64>) -> Self {
        Self {
            min_key,
            max_key,
            key_count,
            bloom_offset,
        }
    }

    /// Simple encode format for block summaries.
    /// Layout: [count: u32LE][entry...]
    /// entry: [min_len:u32LE][min_key][max_len:u32LE][max_key][key_count:u32LE][bloom_offset:u64LE (0=none)]
    pub fn encode_all(summaries: &[BlockSummary]) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(summaries.len() as u32).to_le_bytes());
        for s in summaries {
            buf.extend_from_slice(&(s.min_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.min_key.as_ref());
            buf.extend_from_slice(&(s.max_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.max_key.as_ref());
            buf.extend_from_slice(&s.key_count.to_le_bytes());
            let off = s.bloom_offset.unwrap_or(0);
            buf.extend_from_slice(&off.to_le_bytes());
        }
        Bytes::from(buf)
    }

    pub fn decode_all(data: &[u8]) -> MidgeResult<Vec<BlockSummary>> {
        if data.len() < 4 {
            return Err(MidgeError::InvalidData("BlockSummary data too short".into()));
        }
        let mut offset = 0usize;
        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if offset + 4 > data.len() { return Err(MidgeError::InvalidData("BlockSummary truncated".into())); }
            let min_len = u32::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]) as usize; offset += 4;
            if offset + min_len > data.len() { return Err(MidgeError::InvalidData("BlockSummary min_key truncated".into())); }
            let min_key = Bytes::copy_from_slice(&data[offset..offset+min_len]); offset += min_len;
            if offset + 4 > data.len() { return Err(MidgeError::InvalidData("BlockSummary truncated".into())); }
            let max_len = u32::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]) as usize; offset += 4;
            if offset + max_len > data.len() { return Err(MidgeError::InvalidData("BlockSummary max_key truncated".into())); }
            let max_key = Bytes::copy_from_slice(&data[offset..offset+max_len]); offset += max_len;
            if offset + 4 > data.len() { return Err(MidgeError::InvalidData("BlockSummary truncated".into())); }
            let key_count = u32::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]); offset += 4;
            if offset + 8 > data.len() { return Err(MidgeError::InvalidData("BlockSummary truncated".into())); }
            let bloom_offset = u64::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3], data[offset+4], data[offset+5], data[offset+6], data[offset+7]]); offset += 8;
            let bloom_offset = if bloom_offset == 0 { None } else { Some(bloom_offset) };
            result.push(BlockSummary::new(min_key, max_key, key_count, bloom_offset));
        }
        Ok(result)
    }
}

/// SST Footer with per-block bloom support
#[derive(Clone, Debug)]
pub struct SstFooter {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub has_per_block_blooms: bool,
}

/// Metadata for a single data block in an SST
///
/// This struct carries all information needed to reason about a block's contents
/// without reading it. It's used throughout the read path, iteration, and compaction.
#[derive(Clone, Debug)]
pub struct BlockMeta {
    /// Minimum key in the block (first key)
    pub min_key: Bytes,

    /// Maximum key in the block (last key, also index key)
    pub max_key: Bytes,

    /// Physical location and size of the data block
    pub handle: BlockHandle,

    /// Whether this block contains range tombstones
    pub has_tombstones: bool,

    /// Min key covered by range tombstones in this block (if any)
    pub tombstone_min: Option<Bytes>,

    /// Max key covered by range tombstones in this block (if any)
    pub tombstone_max: Option<Bytes>,

    /// Optional offset of per-block bloom filter (for Phase 1)
    pub bloom_offset: Option<u64>,

    /// Optional cached per-block bloom filter
    bloom: Option<BlockBloom>,
}

impl BlockMeta {
    /// Create a new BlockMeta
    pub fn new(min_key: Bytes, max_key: Bytes, handle: BlockHandle) -> Self {
        Self {
            min_key,
            max_key,
            handle,
            has_tombstones: false,
            tombstone_min: None,
            tombstone_max: None,
            bloom_offset: None,
            bloom: None,
        }
    }

    /// Set tombstone range for this block
    pub fn with_tombstones(
        mut self,
        has_tombstones: bool,
        min: Option<Bytes>,
        max: Option<Bytes>,
    ) -> Self {
        self.has_tombstones = has_tombstones;
        self.tombstone_min = min;
        self.tombstone_max = max;
        self
    }

    /// Set per-block bloom filter offset
    pub fn with_bloom_offset(mut self, offset: u64) -> Self {
        self.bloom_offset = Some(offset);
        self
    }

    /// Set cached per-block bloom filter
    pub fn with_bloom(mut self, bloom: BlockBloom) -> Self {
        self.bloom = Some(bloom);
        self
    }

    /// Query the per-block bloom filter (if present)
    /// Returns None if no bloom is loaded; true/false if bloom is present
    pub fn bloom_maybe_contains(&self, key: &[u8]) -> bool {
        if let Some(ref bloom) = self.bloom {
            bloom.maybe_contains(key)
        } else {
            true // Conservative: assume key might be present if no bloom
        }
    }

    /// Check if this block has a loaded bloom filter
    pub fn has_loaded_bloom(&self) -> bool {
        self.bloom.is_some()
    }

    /// Get reference to the bloom filter (if loaded)
    pub fn bloom(&self) -> Option<&BlockBloom> {
        self.bloom.as_ref()
    }

    /// Check if a key potentially falls within this block
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        key >= self.min_key.as_ref() && key <= self.max_key.as_ref()
    }

    /// Check if a range [start, end) intersects with this block
    #[inline]
    pub fn range_intersects(&self, start: &[u8], end: &[u8]) -> bool {
        // Range [start, end) intersects [min_key, max_key] if:
        // start <= max_key and end > min_key
        start <= self.max_key.as_ref() && end > self.min_key.as_ref()
    }

    /// Check if this block might be fully covered by range tombstones
    /// (fast-path for compaction to skip reading blocks)
    #[inline]
    pub fn might_be_fully_covered(&self) -> bool {
        self.has_tombstones
            && self.tombstone_min.is_some()
            && self.tombstone_max.is_some()
            && self.tombstone_min.as_ref().expect("checked is_some") <= &self.min_key
            // Range tombstones are [start, end), so end must strictly exceed max_key
            && self.tombstone_max.as_ref().expect("checked is_some") > &self.max_key
    }
}

/// Compact in-memory representation of the index table
///
/// Separates search keys (for binary search) from full block metadata,
/// minimizing memory footprint while keeping all necessary information available.
#[derive(Clone, Debug)]
pub struct IndexTable {
    /// Search keys: prefix-compressed min-keys for binary search
    /// Each entry corresponds to the min_key of the corresponding block.
    search_keys: Vec<Bytes>,

    /// Block metadata: offsets, lengths, fence pointers, tombstone info
    metas: Vec<BlockMeta>,

    /// Optional fast negative filter for streaming optimization (Phase 1.5)
    /// Provides L1-cached negative lookup acceleration
    fast_negative_filter: Option<FastNegativeFilter>,

    /// Optional sequential access optimizer for range scan acceleration (Phase 3.5)
    /// Predicts sequential block access patterns and caches repeated lookups
    sequential_optimizer: Option<RefCell<SequentialAccessOptimizer>>,
}

impl IndexTable {
    /// Create a new IndexTable from block metadata
    pub fn new(metas: Vec<BlockMeta>) -> Self {
        let search_keys = metas.iter().map(|m| m.min_key.clone()).collect();
        Self {
            search_keys,
            metas,
            fast_negative_filter: None,
            sequential_optimizer: None,
        }
    }

    /// Create a new IndexTable with a fast negative filter
    pub fn with_fast_negative_filter(
        metas: Vec<BlockMeta>,
        filter: FastNegativeFilter,
    ) -> Self {
        let search_keys = metas.iter().map(|m| m.min_key.clone()).collect();
        Self {
            search_keys,
            metas,
            fast_negative_filter: Some(filter),
            sequential_optimizer: None,
        }
    }

    /// Attach a sequential access optimizer (Phase 3.5)
    #[inline]
    pub fn set_sequential_optimizer(&mut self, optimizer: SequentialAccessOptimizer) {
        self.sequential_optimizer = Some(RefCell::new(optimizer));
    }

    /// Get the sequential access optimizer if present
    #[inline]
    pub fn sequential_optimizer(&self) -> Option<&RefCell<SequentialAccessOptimizer>> {
        self.sequential_optimizer.as_ref()
    }

    /// Set the fast negative filter (for streaming optimization)
    #[inline]
    pub fn set_fast_negative_filter(&mut self, filter: FastNegativeFilter) {
        self.fast_negative_filter = Some(filter);
    }

    /// Get the fast negative filter if present
    #[inline]
    pub fn fast_negative_filter(&self) -> Option<&FastNegativeFilter> {
        self.fast_negative_filter.as_ref()
    }

    /// Check if a block might contain keys using the fast negative filter
    ///
    /// Returns `false` only if we're certain the block is empty.
    /// Returns `true` if the block might contain keys (requires per-block bloom check).
    ///
    /// This is an optimization for the negative lookup path:
    /// - If filter is present and bit is clear, return `false` (no keys possible)
    /// - Otherwise, return `true` (proceed to per-block bloom or block read)
    #[inline]
    pub fn might_contain_block_via_fast_filter(&self, block_index: usize) -> bool {
        if let Some(ref filter) = self.fast_negative_filter {
            filter.might_contain_block(block_index)
        } else {
            true // No filter: conservatively assume block might have keys
        }
    }

    /// Find the block that might contain a given key
    ///
    /// Uses binary search on block max_keys to find candidate blocks,
    /// then verifies key falls within [min_key, max_key] range.
    /// Returns `Some(&BlockMeta)` for the candidate block, or `None` if definitely not present.
    #[inline]
    pub fn find_block(&self, key: &[u8]) -> Option<&BlockMeta> {
        if self.metas.is_empty() {
            return None;
        }

        let key_hash = Self::hash_key(key);

        // Fast path: use sequential predictor if present
        if let Some(opt_cell) = &self.sequential_optimizer {
            if let Ok(mut opt) = opt_cell.try_borrow_mut() {
                if let Some(pred_idx) = opt.predict_next_block() {
                    if let Some(meta) = self.metas.get(pred_idx) {
                        if key >= meta.min_key.as_ref() && key <= meta.max_key.as_ref() {
                            opt.record_lookup(key_hash, pred_idx);
                            return Some(meta);
                        }
                    }
                }
            }
        }

        // Find the first block where max_key >= key
        // This is the first block that could contain the key
        let idx = self
            .metas
            .partition_point(|m| m.max_key.as_ref() < key);

        if idx >= self.metas.len() {
            // Key is beyond all blocks
            return None;
        }

        // Check if the key falls within this block's range
        if key >= self.metas[idx].min_key.as_ref() && key <= self.metas[idx].max_key.as_ref() {
            if let Some(opt_cell) = &self.sequential_optimizer {
                if let Ok(mut opt) = opt_cell.try_borrow_mut() {
                    opt.record_lookup(key_hash, idx);
                }
            }
            return Some(&self.metas[idx]);
        }

        None
    }

    /// Find all blocks that might intersect a range [start, end)
    pub fn find_blocks_in_range(&self, start: &[u8], end: &[u8]) -> Vec<&BlockMeta> {
        if self.metas.is_empty() || start >= end {
            return Vec::new();
        }

        self.metas
            .iter()
            .filter(|m| m.range_intersects(start, end))
            .collect()
    }

    /// Get all block metadata (immutable)
    #[inline]
    pub fn blocks(&self) -> &[BlockMeta] {
        &self.metas
    }

    /// Number of blocks in this table
    #[inline]
    pub fn len(&self) -> usize {
        self.metas.len()
    }

    /// Whether this table is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.metas.is_empty()
    }

    /// Get metadata for a specific block by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&BlockMeta> {
        self.metas.get(index)
    }

    /// Iterator over all blocks
    pub fn iter(&self) -> impl Iterator<Item = &BlockMeta> {
        self.metas.iter()
    }

    #[inline]
    fn hash_key(key: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Memory footprint estimate (for monitoring)
    pub fn memory_usage(&self) -> usize {
        let search_keys_size: usize = self.search_keys.iter().map(|k| k.len()).sum();
        let metas_size = std::mem::size_of::<BlockMeta>() * self.metas.len();
        search_keys_size + metas_size
    }
}

impl fmt::Display for BlockMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BlockMeta {{ min_key: {:?}, max_key: {:?}, offset: {}, size: {}, has_tombstones: {} }}",
            String::from_utf8_lossy(&self.min_key),
            String::from_utf8_lossy(&self.max_key),
            self.handle.offset,
            self.handle.size,
            self.has_tombstones
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_block_meta() {
        // Arrange
        // (no setup needed)

        // Act
        let meta = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("apricot"),
            BlockHandle::new(100, 1024),
        );

        // Assert
        assert_eq!(meta.min_key, Bytes::from("apple"));
        assert_eq!(meta.max_key, Bytes::from("apricot"));
        assert!(!meta.has_tombstones);
    }

    #[test]
    fn should_check_key_containment() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("apple"),
            Bytes::from("banana"),
            BlockHandle::new(100, 1024),
        );

        // Act
        // (assertions check containment)

        // Assert
        assert!(meta.contains_key(b"apple"));
        assert!(meta.contains_key(b"apricot"));
        assert!(meta.contains_key(b"banana"));
        assert!(!meta.contains_key(b"aardvark"));
        assert!(!meta.contains_key(b"cherry"));
    }

    #[test]
    fn should_check_range_intersection() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("b"),
            Bytes::from("d"),
            BlockHandle::new(100, 1024),
        );

        // Act
        // (assertions check intersections)

        // Assert
        assert!(meta.range_intersects(b"a", b"c")); // [a, c) intersects [b, d]
        assert!(meta.range_intersects(b"c", b"e")); // [c, e) intersects [b, d]
        assert!(meta.range_intersects(b"b", b"d")); // [b, d) intersects [b, d]
        assert!(!meta.range_intersects(b"a", b"b")); // [a, b) doesn't intersect [b, d]
        assert!(!meta.range_intersects(b"e", b"f")); // [e, f) doesn't intersect [b, d]
    }

    #[test]
    fn should_build_index_table() {
        // Arrange
        let metas = vec![
            BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(100, 100),
            ),
            BlockMeta::new(
                Bytes::from("g"),
                Bytes::from("z"),
                BlockHandle::new(200, 100),
            ),
        ];

        // Act
        let table = IndexTable::new(metas);

        // Assert
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn should_find_block_by_key() {
        // Arrange
        let metas = vec![
            BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(100, 100),
            ),
            BlockMeta::new(
                Bytes::from("g"),
                Bytes::from("z"),
                BlockHandle::new(200, 100),
            ),
        ];

        let table = IndexTable::new(metas);

        // Act
        let block_b = table.find_block(b"b");
        let block_e = table.find_block(b"e");
        let block_x = table.find_block(b"x");

        // Assert
        assert_eq!(block_b.unwrap().min_key, Bytes::from("a"));
        assert_eq!(block_e.unwrap().min_key, Bytes::from("d"));
        assert_eq!(block_x.unwrap().min_key, Bytes::from("g"));
    }

    #[test]
    fn should_find_blocks_in_range() {
        // Arrange
        let metas = vec![
            BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
            BlockMeta::new(
                Bytes::from("d"),
                Bytes::from("f"),
                BlockHandle::new(100, 100),
            ),
            BlockMeta::new(
                Bytes::from("g"),
                Bytes::from("z"),
                BlockHandle::new(200, 100),
            ),
        ];

        let table = IndexTable::new(metas);

        // Act
        let blocks = table.find_blocks_in_range(b"b", b"h");

        // Assert
        assert_eq!(blocks.len(), 3); // All blocks intersect [b, h)
    }

    // Phase 1: Per-block bloom invariant tests
    #[test]
    fn should_have_no_false_negatives_in_bloom() {
        // Arrange
        let mut bloom = BlockBloom::new(1024);
        let key = b"test_key";

        // Act
        bloom.add(key);

        // Assert
        assert!(bloom.maybe_contains(key), "Bloom filter must not have false negatives");
    }

    #[test]
    fn should_query_bloom_from_block_meta() {
        // Arrange
        let mut bloom = BlockBloom::new(512);
        bloom.add(b"key1");

        // Act
        let meta = BlockMeta::new(
            Bytes::from("key1"),
            Bytes::from("key9"),
            BlockHandle::new(0, 100),
        )
        .with_bloom(bloom);

        // Assert
        assert!(meta.bloom_maybe_contains(b"key1"));
        assert!(meta.has_loaded_bloom());
        assert!(meta.bloom().is_some());
    }

    #[test]
    fn should_conservatively_answer_without_loaded_bloom() {
        // Arrange
        let meta = BlockMeta::new(
            Bytes::from("a"),
            Bytes::from("z"),
            BlockHandle::new(0, 100),
        );

        // Act
        let maybe_contains = meta.bloom_maybe_contains(b"key");
        let has_bloom = meta.has_loaded_bloom();

        // Assert: Without a bloom, should conservatively return true (might be present)
        assert!(maybe_contains);
        assert!(!has_bloom);
    }

    #[test]
    fn should_preserve_bloom_through_encode_decode() {
        // Arrange
        let mut bloom = BlockBloom::new(256);
        bloom.add(b"key1");
        bloom.add(b"key2");

        // Act
        let encoded = bloom.encode();
        let decoded = BlockBloom::decode(&encoded).expect("decode failed");

        // Assert
        assert_eq!(bloom.capacity_bytes(), decoded.capacity_bytes());
        assert!(decoded.maybe_contains(b"key1"));
        assert!(decoded.maybe_contains(b"key2"));
    }

    #[test]
    fn should_track_bloom_offset_in_meta() {
        // Arrange
        let mut meta = BlockMeta::new(
            Bytes::from("a"),
            Bytes::from("z"),
            BlockHandle::new(0, 100),
        );

        // Act
        meta = meta.with_bloom_offset(512);

        // Assert
        assert_eq!(meta.bloom_offset, Some(512));
    }
}
