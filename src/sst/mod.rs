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
use parking_lot::RwLock;
use std::sync::Arc;

pub mod bloom;
pub mod cache;
pub mod compression;
pub mod encoding;
pub mod fs;
pub mod index;
mod name;
pub mod read_amp_metrics;
pub(crate) mod read_path_metrics;
pub mod sparse_index;
pub mod traits;
pub mod trie;
pub mod types;

pub use fs::FsSstFactoryIo;

pub(crate) use name::PersistedSstName;
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
    range_tombstones: RwLock<Vec<crate::sst::types::RangeTombstone>>,
}

impl SkipListMemtable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            skiplist: Arc::new(SkipList::new()),
            seq_generator: std::sync::atomic::AtomicU64::new(1),
            size_bytes: std::sync::atomic::AtomicUsize::new(0),
            range_tombstones: RwLock::new(Vec::new()),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn is_expired(expiration: Option<u64>) -> bool {
        Self::is_expired_at(expiration, Self::current_time_millis())
    }

    fn current_time_millis() -> u64 {
        crate::common::time::unix_time_millis()
    }

    fn is_expired_at(expiration: Option<u64>, now_millis: u64) -> bool {
        crate::common::time::is_expired_at(expiration, now_millis)
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
        self.get_key_state_at_with_time(key, snapshot_seq, Self::current_time_millis())
    }

    /// Get key state using the caller's fixed snapshot clock.
    pub fn get_key_state_at_with_time(
        &self,
        key: &[u8],
        snapshot_seq: u64,
        now_millis: u64,
    ) -> MidgeResult<crate::sst::types::KeyState> {
        Ok(self
            .skiplist
            .get_visible_entry_with_exp(key, snapshot_seq)
            .map_or(crate::sst::types::KeyState::Absent, |entry| {
                match (entry.value, entry.is_tombstone) {
                    (_, true) | (None, _) => crate::sst::types::KeyState::Tombstone(entry.seq),
                    (Some(value), false) => {
                        if Self::is_expired_at(entry.expiration, now_millis) {
                            crate::sst::types::KeyState::Tombstone(entry.seq)
                        } else {
                            crate::sst::types::KeyState::Value(
                                value,
                                entry.seq,
                                entry.expiration,
                                entry.op.as_u8(),
                            )
                        }
                    }
                }
            }))
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
        self.range_state_at_with_time(start, end, snapshot_seq, Self::current_time_millis())
    }

    /// Scan key state using the caller's fixed snapshot clock.
    pub fn range_state_at_with_time(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
        now_millis: u64,
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
                    if Self::is_expired_at(exp, now_millis) {
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
        if start_key >= end_key {
            return Ok(());
        }

        self.range_tombstones
            .write()
            .push(crate::sst::types::RangeTombstone::new(
                start_key.to_vec(),
                end_key.to_vec(),
                seq,
            ));
        // Keep the estimate bounded by the tombstone itself. The range marker,
        // rather than one point tombstone per currently resident key, is the
        // durable representation.
        let size_delta = start_key.len() + end_key.len() + 24;
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Return range tombstones visible at `snapshot_seq` in insertion order.
    #[must_use]
    pub fn range_tombstones_at(&self, snapshot_seq: u64) -> Vec<crate::sst::types::RangeTombstone> {
        self.range_tombstones
            .read()
            .iter()
            .filter(|tombstone| snapshot_seq == u64::MAX || tombstone.seq <= snapshot_seq)
            .cloned()
            .collect()
    }

    /// Return all range tombstones for flush/compaction publication.
    #[must_use]
    pub fn range_tombstones(&self) -> Vec<crate::sst::types::RangeTombstone> {
        self.range_tombstones.read().clone()
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
    use super::SkipListMemtable;
    use crate::sst::types::KeyState;
    use bytes::Bytes;

    #[test]
    fn should_format_sst_names_in_lexicographic_sequence_order() {
        // Arrange
        let names = [1, 2, 10, u64::MAX]
            .into_iter()
            .map(|seq| super::file_name(7, 2, seq))
            .collect::<Vec<_>>();

        // Act
        let mut sorted = names.clone();
        sorted.sort();

        // Assert
        assert_eq!(names, sorted);
        assert_eq!(names[0], "000007_02_00000000000000000001.sst");
        assert_eq!(names[3], "000007_02_18446744073709551615.sst");
    }

    #[test]
    fn should_format_sst_object_keys_without_repeating_sst_prefix_in_file_name() {
        // Arrange
        let file_name = super::file_name(0, 0, 1);

        // Act
        let object_key = super::object_key(&file_name);
        let temp_object_key = super::temp_object_key(&file_name);

        // Assert
        assert_eq!(file_name, "000000_00_00000000000000000001.sst");
        assert_eq!(object_key, "sst/000000_00_00000000000000000001.sst");
        assert_eq!(
            temp_object_key,
            "sst/000000_00_00000000000000000001.sst.tmp"
        );
    }

    #[test]
    fn should_get_memtable_key_state_with_direct_lookup() {
        // Arrange
        let memtable = SkipListMemtable::new();
        memtable
            .put_with_seq(b"key".to_vec(), b"old".to_vec(), 10, None)
            .expect("put old value");
        memtable
            .put_with_seq(b"key".to_vec(), b"new".to_vec(), 20, None)
            .expect("put new value");

        // Act
        let at_15 = memtable.get_key_state_at(b"key", 15).expect("state at 15");
        let at_25 = memtable.get_key_state_at(b"key", 25).expect("state at 25");

        // Assert
        assert!(matches!(
            at_15,
            KeyState::Value(value, 10, None, 0) if value == Bytes::from_static(b"old")
        ));
        assert!(matches!(
            at_25,
            KeyState::Value(value, 20, None, 0) if value == Bytes::from_static(b"new")
        ));
    }

    #[test]
    fn should_get_memtable_tombstone_key_state_with_direct_lookup() {
        // Arrange
        let memtable = SkipListMemtable::new();
        memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 10, None)
            .expect("put value");
        memtable
            .delete_with_seq(b"key".to_vec(), 20)
            .expect("delete value");

        // Act
        let before_delete = memtable
            .get_key_state_at(b"key", 15)
            .expect("state before delete");
        let after_delete = memtable
            .get_key_state_at(b"key", 25)
            .expect("state after delete");

        // Assert
        assert!(matches!(
            before_delete,
            KeyState::Value(value, 10, None, 0) if value == Bytes::from_static(b"value")
        ));
        assert!(matches!(after_delete, KeyState::Tombstone(20)));
    }
}
