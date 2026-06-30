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
//! - **Immutable after `finalize()`**: SST files never change once written, only read or deleted
//! - **Blocking I/O required**: SST access patterns (seek + read at offset) need synchronous I/O
//! - **Local files first**: SSTs are written locally, then persisted to cloud via `HybridStorage`
//! - **Hot path on read side**: Reader needs fast, direct access without callback overhead
//!
//! ### Integration with Storage Layer
//!
//! - **Write path**: Compaction creates SSTs via `FsSstFactoryIo` (using `io::Fs` abstraction)
//!   → Files stored in local directory
//!   → `HybridStorage` persists to cloud (via `StorageBackend` callbacks)
//!
//! - **Read path**: Queries use `SstFileIo` to read local SSTs
//!   → Uses `io::Fs` for flexible filesystem backends (Real, Mock, Chaos)
//!   → Block cache + bloom filters for optimization
//!   → No cloud access on read (reads hit local cache or cloud-synced local file)
//!
//! ## Module Overview
//!
//! - **memtable**: In-memory skiplist-based key-value store
//! - **encoding**: TLV-based entry encoding for SST files
//! - **types**: SST file format types (blocks, footers, handles)
//! - **traits**: Reader/Writer/Factory contracts for SST implementations
//! - **fs**: Filesystem-backed SST implementation (uses `io::Fs` abstraction)

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

pub use fs::FsSstFactoryIo;

pub use read_amp_metrics::ReadAmpMetrics;

pub use traits::{SstFactory, SstReader, SstStateReader};

/// Pad generated SST sequence names to the full `u64` width so filesystem and
/// object-store listings sort in the same order as creation sequence.
pub const SST_SEQUENCE_WIDTH: usize = 20;

/// Format a canonical SST filename. Storage roots already encode the object
/// type via the `sst/` directory or cloud prefix, so the file name only carries
/// ordering identity.
#[must_use]
pub fn file_name(cf_id: u32, level: u32, sequence: u64) -> String {
    format!("{cf_id:06}_{level:02}_{sequence:0SST_SEQUENCE_WIDTH$}.sst")
}

/// Format the cloud object key for an SST file.
#[must_use]
pub fn object_key(file_name: &str) -> String {
    format!("sst/{file_name}")
}

/// Format the temporary staging path for an SST file inside the local SST root.
#[must_use]
pub fn temp_object_key(file_name: &str) -> String {
    format!("sst/{file_name}.tmp")
}

type MemtableEntryWithMeta = (Vec<u8>, Option<Vec<u8>>, u64, Option<u64>, u8);

/// Key-value pair
#[derive(Clone, Debug)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub sequence: u64,
}

/// Memtable trait for lock-free concurrent access
pub trait Memtable: Send + Sync {
    /// Insert or update a value in the memtable.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be recorded in the underlying memtable.
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()>;

    /// Read the latest visible value for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot service the read.
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>>;

    /// Record a tombstone for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error when the tombstone cannot be recorded.
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
    #[must_use]
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

    fn is_expired(expiration: Option<u64>) -> bool {
        let Some(exp_time) = expiration else {
            return false;
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        exp_time <= now
    }

    /// Iterate over all entries in the memtable.
    ///
    /// Returns every version in sorted key order and newest-first sequence
    /// order per key so flush/compaction paths can preserve metadata exactly.
    pub fn iter_all_with_meta(&self, _max_seq: u64) -> Vec<MemtableEntryWithMeta> {
        self.skiplist
            .drain_with_meta_with_exp()
            .into_iter()
            .map(|(key, value, seq, _, exp, op)| {
                (
                    key.to_vec(),
                    value.map(|vb| vb.to_vec()),
                    seq,
                    exp,
                    op.as_u8(),
                )
            })
            .collect()
    }

    /// Iterate over all entries in the memtable.
    /// Returns (key, value, sequence) tuples in sorted order.
    #[must_use]
    pub fn iter_all(&self, max_seq: u64) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        self.iter_all_with_meta(max_seq)
            .into_iter()
            .map(|(key, value, seq, _, _)| (key, value, seq))
            .collect()
    }

    /// Get visible value at or before `snapshot_seq` (respecting expirations).
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot service the lookup.
    pub fn get_at_seq(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<Option<Vec<u8>>> {
        let visible = self.skiplist.get_visible_with_exp(key, snapshot_seq);

        Ok(match visible {
            Some(Some((bytes, exp))) => {
                if Self::is_expired(exp) {
                    None
                } else {
                    Some(bytes.to_vec())
                }
            }
            Some(None) | None => None,
        })
    }

    /// Get key state at a specific snapshot sequence.
    ///
    /// Expired visible values are surfaced as tombstones so older versions do
    /// not reappear through lower layers during snapshot reads.
    /// Get the full presence state for a key at `snapshot_seq`.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot service the lookup.
    pub fn get_key_state_at(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<crate::sst::types::KeyState> {
        for (entry_key, value, seq, is_tombstone, exp, op) in
            self.skiplist.drain_with_meta_with_exp()
        {
            if entry_key.as_ref() != key {
                continue;
            }
            if snapshot_seq != u64::MAX && seq > snapshot_seq {
                continue;
            }

            return Ok(match (value, is_tombstone) {
                (_, true) | (None, _) => crate::sst::types::KeyState::Tombstone(seq),
                (Some(value), false) => {
                    if Self::is_expired(exp) {
                        crate::sst::types::KeyState::Tombstone(seq)
                    } else {
                        crate::sst::types::KeyState::Value(value, seq, exp, op.as_u8())
                    }
                }
            });
        }

        Ok(crate::sst::types::KeyState::Absent)
    }

    /// Get value as Bytes (zero-copy, for performance-critical paths).
    ///
    /// Returns Bytes instead of `Vec<u8>`, avoiding allocation for callers
    /// that can work with the Arc-based Bytes type.
    /// Get the latest visible value as `Bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot service the lookup.
    pub fn get_bytes(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        let visible = self.skiplist.get_visible_with_exp(key, u64::MAX);

        Ok(match visible {
            Some(Some((bytes, exp))) => {
                if Self::is_expired(exp) {
                    None
                } else {
                    Some(bytes)
                }
            }
            Some(None) | None => None,
        })
    }

    /// Get value at sequence as Bytes (zero-copy, for snapshot reads).
    /// Get the visible value at `snapshot_seq` as `Bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot service the lookup.
    pub fn get_bytes_at_seq(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<Option<Bytes>> {
        let visible = self.skiplist.get_visible_with_exp(key, snapshot_seq);

        Ok(match visible {
            Some(Some((bytes, exp))) => {
                if Self::is_expired(exp) {
                    None
                } else {
                    Some(bytes)
                }
            }
            Some(None) | None => None,
        })
    }

    /// Scan visible key state in `[start, end)` at the provided snapshot sequence.
    ///
    /// Expired visible values are surfaced as tombstones so they suppress older
    /// values during cross-layer merges.
    /// Scan the key-state view across a range at `snapshot_seq`.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot service the scan.
    pub fn range_state_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> Vec<(Vec<u8>, crate::sst::types::KeyState)> {
        use std::collections::BTreeMap;

        let mut by_key = BTreeMap::new();

        for (key, value, seq, is_tombstone, exp, op) in self.skiplist.drain_with_meta_with_exp() {
            if snapshot_seq != u64::MAX && seq > snapshot_seq {
                continue;
            }
            if start.is_some_and(|s| key.as_ref() < s) {
                continue;
            }
            if end.is_some_and(|e| key.as_ref() >= e) {
                continue;
            }
            if by_key.contains_key(key.as_ref()) {
                continue;
            }

            let state = match (value, is_tombstone) {
                (_, true) | (None, _) => crate::sst::types::KeyState::Tombstone(seq),
                (Some(value), false) => {
                    if Self::is_expired(exp) {
                        crate::sst::types::KeyState::Tombstone(seq)
                    } else {
                        crate::sst::types::KeyState::Value(value, seq, exp, op.as_u8())
                    }
                }
            };
            by_key.insert(key.to_vec(), state);
        }

        by_key.into_iter().collect()
    }

    /// Put with explicit sequence and optional expiration (Unix millis)
    /// Insert or update a value using an explicit sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot record the write.
    pub fn put_with_seq(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        seq: u64,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        self.put_bytes_with_seq(Bytes::from(key), Bytes::from(value), seq, expiration)
    }

    /// Put with explicit sequence, accepting pre-allocated Bytes (zero-copy fast path).
    /// Insert or update a `Bytes` value using an explicit sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot record the write.
    pub fn put_bytes_with_seq(
        &self,
        key: Bytes,
        value: Bytes,
        seq: u64,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let size_delta = key.len() + value.len() + 16;
        self.skiplist
            .upsert_exp(key, Some(value), seq, expiration, OpType::Put);
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Put with optional expiration (backwards compatible) - generates seq internally
    /// Insert or update a value with an expiration timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot record the write.
    pub fn put_with_exp(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let seq = self.next_seq();
        self.put_with_seq(key, value, seq, expiration)
    }

    /// Delete with explicit sequence (tombstone)
    /// Record a tombstone using an explicit sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot record the tombstone.
    pub fn delete_with_seq(&self, key: Vec<u8>, seq: u64) -> MidgeResult<()> {
        self.delete_bytes_with_seq(Bytes::from(key), seq)
    }

    /// Delete with explicit sequence, accepting pre-allocated Bytes (zero-copy fast path).
    /// Record a tombstone using an explicit sequence number and `Bytes` key.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot record the tombstone.
    pub fn delete_bytes_with_seq(&self, key: Bytes, seq: u64) -> MidgeResult<()> {
        let size_delta = key.len() + 16;
        self.skiplist.delete(key, seq);
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Delete range with explicit sequence [`start_key`, `end_key`)
    /// Record a range tombstone using an explicit sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot record the tombstone.
    pub fn delete_range_with_seq(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        seq: u64,
    ) -> MidgeResult<()> {
        let count = self
            .skiplist
            .delete_range(Some(start_key), Some(end_key), seq);
        // Estimate size impact: tombstone per deleted key
        let size_delta = count * 32; // rough estimate
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
        let visible = self.skiplist.get_visible_with_exp(key, u64::MAX);

        Ok(match visible {
            Some(Some((bytes, exp))) => {
                if Self::is_expired(exp) {
                    None
                } else {
                    Some(bytes.to_vec())
                }
            }
            Some(None) | None => None,
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_format_sst_names_in_lexicographic_sequence_order() {
        let names = [1, 2, 10, u64::MAX]
            .into_iter()
            .map(|seq| super::file_name(7, 2, seq))
            .collect::<Vec<_>>();
        let mut sorted = names.clone();
        sorted.sort();

        assert_eq!(names, sorted);
        assert_eq!(names[0], "000007_02_00000000000000000001.sst");
        assert_eq!(names[3], "000007_02_18446744073709551615.sst");
    }

    #[test]
    fn should_format_sst_object_keys_without_repeating_sst_prefix_in_file_name() {
        let file_name = super::file_name(0, 0, 1);

        assert_eq!(file_name, "000000_00_00000000000000000001.sst");
        assert_eq!(
            super::object_key(&file_name),
            "sst/000000_00_00000000000000000001.sst"
        );
        assert_eq!(
            super::temp_object_key(&file_name),
            "sst/000000_00_00000000000000000001.sst.tmp"
        );
    }
}
