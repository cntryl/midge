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
// Compression types: See src/sst/compression/mod.rs for authoritative codes:
//   0=None, 1=LZ4, 2=Zstd(3), 3=Zstd(9+), 4=Zlib, 5=Snappy
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
//         optional compression via CompressionPolicy
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
//     - finalize block (with optional compression via compress_block())
//     - compute block handle (offset + length)
//     - add index entry with separator key
//     - write block to file
//     - start new block
//
//   builder.finish()
//     - write final data block (compressed)
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
//! ## Key Design: SST uses `std::fs` directly, NOT `storage/` layer
//!
//! SSTs intentionally use synchronous, direct filesystem I/O via `std::fs` rather than
//! the callback-driven `StorageBackend` trait. This is correct because:
//!
//! - **Immutable after finalize()**: SST files never change once written, only read or deleted
//! - **Blocking I/O required**: SST access patterns (seek + read at offset) need synchronous I/O
//! - **Local files first**: SSTs are written locally, then persisted to cloud via HybridStorage
//! - **Hot path on read side**: Reader needs fast, direct access without callback overhead
//!
//! ### Integration with Storage Layer
//!
//! - **Write path**: Compaction creates SSTs via `FsSstFactoryIo` (using io::Fs abstraction)
//!   → Files stored in local directory
//!   → HybridStorage persists to cloud (via `StorageBackend` callbacks)
//!
//! - **Read path**: Queries use `SstFileIo` to read local SSTs
//!   → Uses io::Fs for flexible filesystem backends (Real, Mock, Chaos)
//!   → Block cache + bloom filters for optimization
//!   → No cloud access on read (reads hit local cache or cloud-synced local file)
//!
//! ## Module Overview
//!
//! - **memtable**: In-memory skiplist-based key-value store
//! - **encoding**: TLV-based entry encoding for SST files
//! - **types**: SST file format types (blocks, footers, handles)
//! - **traits**: Reader/Writer/Factory contracts for SST implementations
//! - **fs**: Filesystem-backed SST implementation (uses io::Fs abstraction)

use crate::common::MidgeResult;
use crate::iterators::skiplist::OpType;
use crate::iterators::SkipList;
use bytes::Bytes;
use std::sync::Arc;

pub mod bloom;
pub mod cache;
pub mod compression;
pub mod encoding;
pub mod fs;
pub mod index;
pub mod read_amp_metrics;
pub mod sparse_index;
pub mod traits;
pub mod trie;
pub mod types;

pub use bloom::{BloomFactory, BloomFilterFactory, BloomReader, BloomTestResult, BloomWriter};
pub use cache::{BlockCache, CacheKey, CacheMetrics, CachePolicyType, CacheValue};
pub use compression::{
    compress_block, decompress_block, CompressionAlgo, CompressionPolicy, BLOCK_TRAILER_SIZE,
    MAX_BLOCK_SIZE, MIN_COMPRESS_SIZE,
};
pub use fs::FsSstFactoryIo;
pub use index::{IndexKind, IndexTuner, KeyStructureProfile, KeyStructureProfiler};
pub use read_amp_metrics::ReadAmpMetrics;
pub use sparse_index::{BlockRange, IndexEntry, SparseIndexReader, SparseIndexWriter};
pub use traits::{DynSstWriter, SstFactory, SstReader, SstStateReader, SstWriter};
pub use trie::{TrieBuilder, TrieReader, TrieWriter};
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
    /// Get all versions for merge resolution
    fn get_versions_for_merge(
        &self,
        key: &[u8],
    ) -> Vec<(
        Option<bytes::Bytes>,
        Option<u64>,
        crate::iterators::skiplist::OpType,
    )>;
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

    /// Get all versions for a key (for merge resolution)
    /// Returns (value, expiration, op_type) tuples in chronological order (oldest first)
    pub fn get_versions_for_merge(
        &self,
        key: &[u8],
    ) -> Vec<(Option<bytes::Bytes>, Option<u64>, OpType)> {
        self.skiplist.get_versions_for_merge(key, u64::MAX)
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

    /// Put with explicit sequence and optional expiration (Unix millis)
    pub fn put_with_seq(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        seq: u64,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let size_delta = key.len() + value.len() + 16;
        self.skiplist.upsert_exp(
            Bytes::from(key),
            Some(Bytes::from(value)),
            seq,
            expiration,
            OpType::Put,
        );
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Put with optional expiration (backwards compatible) - generates seq internally
    pub fn put_with_exp(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let seq = self.next_seq();
        self.put_with_seq(key, value, seq, expiration)
    }

    /// Store a merge operand with explicit sequence
    pub fn merge_with_seq(&self, key: Vec<u8>, operand: Vec<u8>, seq: u64) -> MidgeResult<()> {
        let size_delta = key.len() + operand.len() + 16;
        self.skiplist.upsert_exp(
            Bytes::from(key),
            Some(Bytes::from(operand)),
            seq,
            None, // No expiration for merge operands
            OpType::Merge,
        );
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Store a merge operand (backwards compatible)
    pub fn merge(&self, key: Vec<u8>, operand: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        self.merge_with_seq(key, operand, seq)
    }

    /// Delete with explicit sequence (tombstone)
    pub fn delete_with_seq(&self, key: Vec<u8>, seq: u64) -> MidgeResult<()> {
        let size_delta = key.len() + 16;
        self.skiplist.delete(Bytes::from(key), seq);
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Delete (backwards compatible) - tombstone with generated seq
    #[allow(dead_code)]
    fn delete(&self, key: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        self.delete_with_seq(key, seq)
    }
}

impl Default for SkipListMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable for SkipListMemtable {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()> {
        self.put_with_exp(key, value, None)
    }

    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
        // Respect expiration if present
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let visible = self.skiplist.get_visible_with_exp(key, u64::MAX);

        Ok(match visible {
            Some(Some((bytes, exp))) => {
                if exp.map(|e| e <= now).unwrap_or(false) {
                    None
                } else {
                    Some(bytes.to_vec())
                }
            }
            Some(None) => None,
            None => None,
        })
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

    fn get_versions_for_merge(
        &self,
        key: &[u8],
    ) -> Vec<(
        Option<bytes::Bytes>,
        Option<u64>,
        crate::iterators::skiplist::OpType,
    )> {
        self.skiplist.get_versions_for_merge(key, u64::MAX)
    }
}
