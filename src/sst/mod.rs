// == COPILOT MASTER RULES FOR SST SUBSYSTEM ==========================================
// These rules define the authoritative architecture for Midge SSTs. All completions
// touching SST encoding, blocks, builders, iterators, index files, filters, or table
// management MUST follow these rules exactly.
//
// =====================================================================================
// 1. SST GOALS
// -------------------------------------------------------------------------------------
// Midge SSTs must be:
//   - Immutable
//   - Ordered by key (lexicographic)
//   - TLV encoded (type-length-value) at block level
//   - Prefix-compressed inside each block
//   - Checksummed (CRC32C) per block
//   - Backed by a sparse index for fast lookup
//   - Efficient for both point lookup and range scan
//
// SSTs must never:
//   - Contain partial or corrupted blocks
//   - Be mutable after finalize()
//   - Allow holes, unsorted keys, or duplicate keys in same CF
//
// =====================================================================================
// 2. FILE STRUCTURE (authoritative format)
// -------------------------------------------------------------------------------------
// SST file layout:
//
//   [ Data Block 0 ]
//   [ Data Block 1 ]
//   ...
//   [ Index Block ]
//   [ Filter Block ]     (optional but recommended; Bloom or Prefix Bloom)
//   [ Footer (fixed 48 bytes, RocksDB-compatible) ]
//
// Footer contains: 
//   - metaindex_handle
//   - index_handle
//   - magic number: 0xdb4775248b80fb57 (RocksDB-compatible)
//
// Blocks follow:
//
//   Block = <block_data_bytes> + <1 byte compression type> + <4 byte crc32c>
//
// Compression types supported: NONE, Snappy (optional), Zstd (optional).
//
// =====================================================================================
// 3. BLOCK RULES
// -------------------------------------------------------------------------------------
// Data block invariants:
//   - Keys are prefix-compressed using SharedPrefixLength + Suffix.
//   - Values stored as raw byte slices.
//   - Restart points every N entries (default: 16).
//   - BlockBuilder MUST guarantee:
//         sorted keys
//         no duplicates
//         restart array at end of block
//
// Index block invariants:
//   - Contains (separator key → block_handle) entries.
//   - Separator key MUST be the minimal separator between last key of current block
//     and first key of next block.
//
// Filter block invariants:
//   - Bloom filter OR prefix bloom filter with k hash functions.
//   - Key membership is advisory (false positive allowed, false negative forbidden).
//
// =====================================================================================
// 4. SSTBuilder RULES
// -------------------------------------------------------------------------------------
// SSTBuilder MUST perform the following in order:
//
//   builder.add(key, value)
//     - keys MUST be strictly increasing
//     - no duplicates allowed
//     - adds to current data block
//
//   When block is full (>= target_size):
//     - finalize block
//     - compute block handle (offset + length)
//     - add index entry with separator key
//     - write block to file
//     - start new block
//
//   builder.finish()
//     - write final data block
//     - write filter block (if enabled)
//     - write index block
//     - write footer
//     - fsync the file
//
// Builder MUST guarantee atomic SST creation: the file is not considered valid until
// footer is written.
//
// =====================================================================================
// 5. READING + ITERATION RULES
// -------------------------------------------------------------------------------------
// SSTable MUST support:
//
//   - point lookup via:
//         1. binary search in sparse index
//         2. read target block
//         3. prefix decode entries
//
//   - range iteration via two-phase iterator:
//         Phase 1: seek(start_key) using index
//         Phase 2: scan current + subsequent blocks
//
// Iterators MUST:
//
//   - Never load the full SST into memory
//   - Use block cache for hot blocks
//   - Validate block checksum before use
//   - Be safe against corrupted SSTs (return error on invalid CRC, bad encoding)
//
// =====================================================================================
// 6. BLOCK CACHE RULES
// -------------------------------------------------------------------------------------
// Block cache MUST be:
//
//   - Sharded (power-of-two shard count)
//   - Keyed by (sst_id, block_offset)
//   - Backed by ClockPro or TinyLFU
//   - Zero-copy: store Bytes or Arc<[u8]> not owned Vec
//
// Cache MUST NOT:
//
//   - Store decompressed blocks if "raw+compressed" mode is enabled
//   - Attempt to cache oversized blocks (above a tunable threshold)
//   - Evict index blocks prematurely (keep index blocks hot)
//
// =====================================================================================
// 7. FILTER RULES
// -------------------------------------------------------------------------------------
// Filter policy MUST:
//
//   - Be optional, but recommended for large SSTs
//   - Implement FilterPolicy trait with:
//         create_filter(keys)
//         key_may_match(key, filter_block)
//   - Support:
//         - Bloom
//         - Prefix Bloom (preferred)
//
// Lookup MUST consult filter BEFORE touching block cache.
//
// =====================================================================================
// 8. SAFETY + DURABILITY INVARIANTS
// -------------------------------------------------------------------------------------
// SSTs MUST:
//
//   - Be immutable once finished
//   - Always have a valid footer OR be considered corrupt
//   - Have strictly increasing key ranges across blocks
//   - Never mix CF IDs inside same SST
//   - Never truncate block boundaries
//
// Compaction MUST:
//
//   - Read full SSTs from source levels
//   - Merge sorted streams without reordering
//   - Drop tombstones based on snapshot rules
//   - Produce new SSTs that follow ALL invariants above
//
// =====================================================================================
// 9. PERFORMANCE RULES
// -------------------------------------------------------------------------------------
// Copilot MUST optimize according to:
//
//   - Minimal allocations inside BlockBuilder
//   - Efficient prefix compression
//   - Memory reuse of temporary buffers
//   - Avoid copying block data when placing into cache
//   - Sequential writes for SSTBuilder
//   - Binary search on index
//   - Minimized I/O seeks
//
// =====================================================================================
// 10. WHAT COPILOT MUST NEVER DO
// -------------------------------------------------------------------------------------
// ❌ Never allow out-of-order keys.
// ❌ Never write an SST without a footer.
// ❌ Never skip CRC32C verification for loaded blocks.
// ❌ Never store full blocks in heap-allocated Vec unnecessarily.
// ❌ Never allow mutable modifications of finalized SSTs.
// ❌ Never embed CF ID into keys (CF already encoded in directory / file naming).
// ❌ Never bypass block cache for non-index blocks.
//
// =====================================================================================
//
// Follow these rules EXACTLY for all SST-related code in the Midge codebase.
// =====================================================================================

//! SST (Sorted String Table) module
//!
//! Provides both in-memory memtables and on-disk SST file implementations.
//!
//! - **memtable**: In-memory skiplist-based key-value store
//! - **encoding**: TLV-based entry encoding for SST files
//! - **types**: SST file format types (blocks, footers, handles)
//! - **traits**: Reader/Writer/Factory contracts for SST implementations
//! - **fs**: Filesystem-backed SST implementation

use crate::common::MidgeResult;
use crate::iterators::SkipList;
use bytes::Bytes;
use std::sync::Arc;

pub mod bloom;
pub mod cache;
pub mod encoding;
pub mod fs;
pub mod sparse_index;
pub mod traits;
pub mod types;

pub use bloom::{BloomFactory, BloomFilterFactory, BloomReader, BloomTestResult, BloomWriter};
pub use cache::{BlockCache, CacheKey, CacheMetrics, CachePolicyType, CacheValue};
pub use fs::FsSstFactory;
pub use sparse_index::{BlockRange, IndexEntry, SparseIndexReader, SparseIndexWriter};
pub use traits::{DynSstWriter, SstFactory, SstReader, SstStateReader, SstWriter};
pub use types::{Block, BlockHandle, BlockType, Footer, KeyState, RangeTombstone, SstEntry};

/// Key-value pair
#[derive(Clone, Debug)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub sequence: u64,
}

/// Memtable trait for lock-free concurrent access
pub trait Memtable: Send + Sync {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()>;
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>>;
    fn delete(&self, key: Vec<u8>) -> MidgeResult<()>;
    fn size_bytes(&self) -> usize;
}

/// SkipList-based Memtable (lock-free, MVCC-aware)
pub struct SkipListMemtable {
    skiplist: Arc<SkipList>,
    seq_generator: std::sync::atomic::AtomicU64,
    size_bytes: std::sync::atomic::AtomicUsize,
}

impl SkipListMemtable {
    pub fn new() -> Self {
        Self {
            skiplist: Arc::new(SkipList::new()),
            seq_generator: std::sync::atomic::AtomicU64::new(1),
            size_bytes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Iterate over all entries in the memtable
    /// Returns (key, value, sequence) tuples in sorted order
    pub fn iter_all(&self, _max_seq: u64) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        self.skiplist
            .drain_with_meta_with_exp()
            .into_iter()
            .map(|(key, value, seq, _, _, _)| (key.to_vec(), value.map(|vb| vb.to_vec()), seq))
            .collect()
    }
}

impl Default for SkipListMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable for SkipListMemtable {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        let size_delta = key.len() + value.len() + 16;
        self.skiplist
            .upsert(Bytes::from(key), Some(Bytes::from(value)), seq);
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
        Ok(self.skiplist.get(key, u64::MAX).map(|b| b.to_vec()))
    }

    fn delete(&self, key: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        let size_delta = key.len() + 16;
        self.skiplist.delete(Bytes::from(key), seq);
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
}
