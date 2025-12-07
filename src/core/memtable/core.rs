use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::common::timestamp;
use crate::core::data_structures::skiplist::{OpType, SkipList};
use crate::error::MidgeResult;
use crate::wal::WalRecord;

use super::bloom_hint::BloomHint;
use super::range_tombstones::RangeTombstones;
use super::wal_loading;

/// Return current Unix epoch time in milliseconds.
fn current_time_millis() -> u64 {
    timestamp::now_millis()
}

/// Return true if the provided expiration has passed.
/// - None => never expires => returns false
/// - Some(exp_millis) => returns true if now >= exp_millis
fn is_expired(expiration: Option<u64>) -> bool {
    match expiration {
        None => false,
        Some(exp_millis) => current_time_millis() >= exp_millis,
    }
}

/// Simple in-memory memtable using a lock-free SkipList for ordered keys and tombstones.
///
/// Optionally includes a bloom filter hint to accelerate negative point lookups.
#[derive(Clone)]
pub struct MemTable {
    pub(super) inner: Arc<SkipList>,
    pub(super) bytes: Arc<AtomicUsize>,
    pub(super) range_tombstones: RangeTombstones,
    /// Optional bloom filter for fast negative lookups.
    /// When enabled, point queries check bloom first and skip skiplist traversal
    /// if the key is definitely absent.
    bloom_hint: Option<Arc<BloomHint>>,
}

impl MemTable {
    /// Create a new memtable without bloom filter optimization.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SkipList::new()),
            bytes: Arc::new(AtomicUsize::new(0)),
            range_tombstones: RangeTombstones::new(),
            bloom_hint: None,
        }
    }

    /// Create a new memtable with bloom filter optimization enabled.
    ///
    /// The bloom filter helps accelerate point lookups by quickly rejecting
    /// queries for keys that definitely don't exist, avoiding skiplist traversal.
    ///
    /// # Arguments
    /// * `expected_keys` - Estimated number of keys for sizing the bloom filter.
    ///   Uses ~10 bits per key for ~1% false positive rate.
    pub fn with_bloom_hint(expected_keys: usize) -> Self {
        Self {
            inner: Arc::new(SkipList::new()),
            bytes: Arc::new(AtomicUsize::new(0)),
            range_tombstones: RangeTombstones::new(),
            bloom_hint: Some(Arc::new(BloomHint::for_keys(expected_keys))),
        }
    }

    /// Load initial state from WAL records (Vec<WalRecord>), useful at startup.
    pub fn load_from_wal(&self, records: Vec<WalRecord>) -> MidgeResult<()> {
        wal_loading::load_from_wal(self, records)
    }

    /// Get the latest value for a key, respecting TTL expiration.
    /// Returns None if key doesn't exist, is deleted, or has expired.
    ///
    /// When bloom hint is enabled, quickly returns None for keys that
    /// definitely don't exist, avoiding skiplist traversal.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        // Fast path: check bloom filter if enabled
        if let Some(ref bloom) = self.bloom_hint {
            if !bloom.may_contain(key) {
                return None; // Definitely absent
            }
        }

        // Get the latest version with expiration info
        match self.inner.get_visible_with_exp(key, u64::MAX) {
            Some(Some((v, exp))) => {
                // Check if expired
                if is_expired(exp) {
                    None // Expired, treat as absent
                } else {
                    Some(v)
                }
            }
            _ => None, // Tombstone or not present
        }
    }

    /// Get at a snapshot sequence (inclusive), with TTL enforcement.
    /// Returns None if:
    /// - Key does not exist
    /// - Key has a tombstone at or before the snapshot sequence
    /// - Key has expired (and enforces no-resurrection: does not fall back to older versions)
    ///
    /// When bloom hint is enabled, quickly returns None for keys that
    /// definitely don't exist, avoiding skiplist traversal.
    pub fn get_at(&self, key: &[u8], seq: u64) -> Option<Bytes> {
        // Fast path: check bloom filter if enabled
        if let Some(ref bloom) = self.bloom_hint {
            if !bloom.may_contain(key) {
                return None; // Definitely absent
            }
        }

        match self.inner.get_visible_with_exp(key, seq) {
            Some(Some((v, exp))) => {
                if is_expired(exp) {
                    // Expired: return None, do not resurrect older versions
                    None
                } else {
                    Some(v)
                }
            }
            _ => None, // tombstone or not present
        }
    }

    /// Get all versions for a key (for merge resolution).
    /// Returns (value_opt, expiration_opt, op_type) tuples in descending sequence order.
    pub fn get_versions_for_merge(
        &self,
        key: &[u8],
        seq: u64,
    ) -> Vec<(Option<Bytes>, Option<u64>, OpType)> {
        self.inner.get_versions_for_merge(key, seq)
    }

    /// Get all keys currently in the memtable (for merge resolution during flush).
    pub fn get_all_keys(&self) -> Vec<Bytes> {
        self.inner.get_all_keys()
    }

    #[inline]
    pub fn put(&self, key: &[u8], value: &[u8]) {
        // Default to seq=0 if not provided
        self.put_with_seq(key, value, 0)
    }

    #[inline]
    pub fn put_with_seq(&self, key: &[u8], value: &[u8], seq: u64) {
        self.put_with_seq_and_exp(key, value, seq, None)
    }

    /// Put a key/value where the caller already owns Bytes; avoids copying slices.
    /// This is useful for hot paths where callers already have owned Bytes.
    #[inline]
    pub fn put_owned_with_seq(&self, key: Bytes, value: Bytes, seq: u64) {
        self.put_owned_with_seq_and_exp(key, value, seq, None)
    }

    /// Put a key/value with expiration where caller owns Bytes.
    #[inline]
    pub fn put_owned_with_seq_and_exp(
        &self,
        key: Bytes,
        value: Bytes,
        seq: u64,
        expiration: Option<u64>,
    ) {
        self.upsert_owned_with_op_type(key, Some(value), seq, expiration, OpType::Put)
    }

    /// Put a key-value pair with a specific sequence number and optional expiration time.
    /// - expiration: Unix epoch time in milliseconds, or None for never-expiring
    #[inline]
    pub fn put_with_seq_and_exp(
        &self,
        key: &[u8],
        value: &[u8],
        seq: u64,
        expiration: Option<u64>,
    ) {
        self.upsert_with_op_type(key, value, seq, expiration, OpType::Put)
    }

    /// Store a merge operand with a specific sequence number and optional expiration time.
    #[inline]
    pub fn merge_with_seq_and_exp(
        &self,
        key: &[u8],
        value: &[u8],
        seq: u64,
        expiration: Option<u64>,
    ) {
        self.upsert_with_op_type(key, value, seq, expiration, OpType::Merge)
    }

    /// Internal helper to upsert with a specific operation type
    fn upsert_with_op_type(
        &self,
        key: &[u8],
        value: &[u8],
        seq: u64,
        expiration: Option<u64>,
        op_type: OpType,
    ) {
        // Add to bloom filter if enabled (before skiplist for correct may_contain)
        if let Some(ref bloom) = self.bloom_hint {
            bloom.add(key);
        }

        let k = Bytes::copy_from_slice(key);
        let v = Some(Bytes::copy_from_slice(value));
        let total_bytes = key.len() + value.len();

        // Perform the upsert (single skiplist operation)
        self.inner.upsert_exp(k, v, seq, expiration, op_type);

        // Update byte count (simplified accounting - counts all data, some double-counting for updates)
        // This is acceptable as it's reset on drain and provides upper-bound for memory usage
        self.bytes.fetch_add(total_bytes, Ordering::Relaxed);
    }

    /// Internal helper to upsert using owned Bytes (avoids copying when caller already owns Bytes)
    fn upsert_owned_with_op_type(
        &self,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        expiration: Option<u64>,
        op_type: OpType,
    ) {
        // Add to bloom filter if enabled (before skiplist for correct may_contain)
        if let Some(ref bloom) = self.bloom_hint {
            bloom.add(&key);
        }

        let total_bytes = key.len() + value.as_ref().map(|v| v.len()).unwrap_or(0);

        // Perform the upsert directly with owned Bytes
        self.inner.upsert_exp(key, value, seq, expiration, op_type);

        // Update accounting
        self.bytes.fetch_add(total_bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn delete(&self, key: &[u8]) {
        self.delete_with_seq(key, 0)
    }

    #[inline]
    pub fn delete_with_seq(&self, key: &[u8], seq: u64) {
        // Add to bloom filter if enabled (tombstones are still "present")
        if let Some(ref bloom) = self.bloom_hint {
            bloom.add(key);
        }

        let k = Bytes::copy_from_slice(key);
        let key_len = key.len();

        // Tombstones never have expiration - perform the delete
        self.inner.upsert_exp(k, None, seq, None, OpType::Delete);

        // Count tombstone storage (simplified - may overcount on repeated deletes)
        self.bytes.fetch_add(key_len, Ordering::Relaxed);
    }

    pub fn apply_batch(&self, mutations: Vec<crate::api::mutation::Mutation>) {
        // reuse existing put/delete logic to maintain accounting
        for m in mutations {
            match m.op {
                crate::api::mutation::MutationOp::Put
                | crate::api::mutation::MutationOp::Insert => {
                    if let Some(v) = m.value {
                        self.put(&m.key, &v);
                    }
                }
                crate::api::mutation::MutationOp::CompareAndSwap => {
                    // CAS validation should happen before reaching memtable
                    // At this point, just apply as a regular put
                    if let Some(v) = m.value {
                        self.put(&m.key, &v);
                    }
                }
                crate::api::mutation::MutationOp::Merge => {
                    // Store merge operand (will be resolved during read/flush)
                    if let Some(v) = m.value {
                        self.merge_with_seq_and_exp(&m.key, &v, 0, None);
                    }
                }
                crate::api::mutation::MutationOp::Delete => {
                    self.delete(&m.key);
                }
                crate::api::mutation::MutationOp::DeleteRange => {
                    // Best-effort: walk range and set tombstones, adjust bytes by removed value sizes
                    let end_opt = m.range_end.as_ref().map(|b| b.as_ref());
                    // Compute total bytes for values removed
                    let mut removed_bytes = 0usize;
                    let items = self.inner.range(Some(m.key.as_ref()), end_opt);
                    for (_k, v) in &items {
                        removed_bytes += v.len();
                    }
                    let _ = self.inner.delete_range(Some(m.key.as_ref()), end_opt, 0);
                    let _ = self
                        .bytes
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                            Some(cur.saturating_sub(removed_bytes))
                        });
                }
            }
        }
    }

    /// Scan range at a snapshot sequence (inclusive). Currently returns latest state.
    pub fn scan_range_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        seq: u64,
    ) -> Vec<(Bytes, Bytes)> {
        self.inner.range_visible(start, end, seq)
    }

    /// Returns true if the memtable has reached or exceeded the provided byte limit.
    pub fn is_full(&self, limit: usize) -> bool {
        self.bytes.load(Ordering::Relaxed) >= limit
    }

    /// Returns the current memory usage in bytes
    pub fn size_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Returns true if the memtable is currently empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.load(Ordering::Relaxed) == 0
    }

    /// Drain the memtable as user-visible entries only (skips tombstones),
    /// resetting the memtable to an empty state.
    /// Note: With lock-free skiplist, this creates a snapshot rather than true drain.
    pub fn drain(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let out = self.inner.drain_with_meta_with_exp();
        self.bytes.store(0, Ordering::Relaxed);
        // Convert to user-visible entries only (skip tombstones)
        out.into_iter()
            .filter_map(|(k, v_opt, _seq, _tomb, _exp, _op)| {
                v_opt.map(|v| (k.to_vec(), v.to_vec()))
            })
            .collect()
    }

    /// Drain with metadata for flushing: returns (key, value_opt, seq, tombstone, expiration)
    /// and resets the memtable. Useful for writing seq/tombstone/expiration to SST.
    /// Note: With lock-free skiplist, this creates a snapshot rather than true drain.
    pub fn drain_with_meta(&self) -> Vec<crate::core::EntryMeta> {
        let out = self.inner.drain_with_meta_with_exp();
        self.bytes.store(0, Ordering::Relaxed);
        // Convert Bytes to Vec<u8> at API boundary and wrap in EntryMeta
        out.into_iter()
            .map(|(k, v, seq, tomb, exp, op)| {
                crate::core::EntryMeta::new(k.to_vec(), v.map(|b| b.to_vec()), seq, tomb, exp, op)
            })
            .collect()
    }

    /// Drain with metadata but encode the keys as internal keys: userkey || seq (BE) || kind(u8)
    /// This is useful for SST writers that expect internal-key encoded keys on disk.
    /// Note: With lock-free skiplist, this creates a snapshot rather than true drain.
    pub fn drain_with_meta_internal(&self) -> Vec<crate::core::EntryMeta> {
        let raws = self.inner.drain_with_meta_with_exp();
        self.bytes.store(0, Ordering::Relaxed);
        // Transform keys into internal-key encoding
        let mut out: Vec<crate::core::EntryMeta> = Vec::with_capacity(raws.len());
        for (k, v_opt, seq, tomb, exp, op) in raws {
            let ik = crate::common::internal_key::encode_internal_key(&k, seq, tomb);
            out.push(crate::core::EntryMeta::new(
                ik,
                v_opt.map(|b| b.to_vec()),
                seq,
                tomb,
                exp,
                op,
            ));
        }
        out
    }

    /// Record and apply a range deletion [start, end) with sequence `seq`.
    /// This sets tombstones for existing keys in the skiplist for immediate visibility and
    /// appends a range tombstone descriptor for persistence during flush.
    pub fn delete_range_with_seq(&self, start: &[u8], end: &[u8], seq: u64) {
        // Apply to skiplist for immediate visibility
        let mut removed_bytes = 0usize;
        for (_k, v) in self.inner.range(Some(start), Some(end)) {
            removed_bytes += v.len();
        }
        let _ = self.inner.delete_range(Some(start), Some(end), seq);
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(removed_bytes))
            });
        // Record the range tombstone for flush
        self.range_tombstones
            .push(start.to_vec(), end.to_vec(), seq);
    }

    /// Drain and return recorded range tombstones, resetting the list.
    pub fn drain_range_tombstones(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        self.range_tombstones.drain()
    }

    /// Returns true if any active range tombstone covers `key` (current view, unflushed).
    #[inline]
    pub fn is_range_deleted(&self, key: &[u8]) -> bool {
        self.range_tombstones.covers(key)
    }

    /// Returns true if a range tombstone with sequence <= `seq` covers `key` (snapshot view).
    #[inline]
    pub fn is_range_deleted_at(&self, key: &[u8], seq: u64) -> bool {
        self.range_tombstones.covers_at(key, seq)
    }

    /// Return a sorted list of key/value pairs in the range [start, end).
    /// If a bound is None, it's unbounded on that side. Tombstones are skipped.
    pub fn scan_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Bytes, Bytes)> {
        self.inner.range(start, end)
    }

    /// Return tombstoned keys within [start, end) in the current memtable.
    pub fn tombstones_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Bytes> {
        self.inner.tombstones_range(start, end)
    }

    /// Returns true if there are any range tombstones recorded in this memtable.
    pub fn has_range_tombstones(&self) -> bool {
        !self.range_tombstones.is_empty()
    }

    /// Return tombstoned keys visible at snapshot within [start, end).
    pub fn tombstones_range_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        seq: u64,
    ) -> Vec<Bytes> {
        self.inner.tombstones_range_visible(start, end, seq)
    }
}

// Re-export a short alias for callers
pub type Memtable = MemTable;

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_exclude_deleted_keys_given_tombstones_when_scanning() {
        // Arrange
        let mt = MemTable::new();
        mt.put(b"a", b"1");
        mt.put(b"b", b"2");
        mt.delete(b"a");

        // Act
        let rows = mt.scan_range(Some(b"a"), Some(b"z"));

        // Assert
        assert_eq!(
            rows,
            vec![(Bytes::from_static(b"b"), Bytes::from_static(b"2"))]
        );
    }

    #[test]
    fn should_hide_newer_values_given_snapshot_when_get_at() {
        // Arrange
        let mt = MemTable::new();
        mt.put_with_seq(b"k", b"v1", 10);
        // newer write not visible to snapshot 11
        mt.put_with_seq(b"k", b"v2", 11);

        // Act
        let v_at_11 = mt.get_at(b"k", 11);
        let v_at_12 = mt.get_at(b"k", 12);
        let v_latest = mt.get(b"k");

        // Assert
        // Snapshot isolation: snapshot at seq 11 sees writes with seq < 11 (i.e., seq 10)
        // Snapshot at seq 12 sees writes with seq < 12 (i.e., seq 10 and 11)
        assert_eq!(v_at_11, Some(Bytes::from_static(b"v1")));
        assert_eq!(v_at_12, Some(Bytes::from_static(b"v2")));
        assert_eq!(v_latest, Some(Bytes::from_static(b"v2")));
    }

    #[test]
    fn should_filter_entries_by_snapshot_given_range_when_scan_range_at() {
        // Arrange
        let mt = MemTable::new();
        mt.put_with_seq(b"a", b"1", 1);
        mt.put_with_seq(b"b", b"2", 2);
        mt.put_with_seq(b"c", b"3", 3);

        // Act
        let rows_at_3 = mt.scan_range_at(Some(b"a"), Some(b"z"), 3);

        // Assert
        assert_eq!(
            rows_at_3,
            vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
                (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
            ]
        );
    }

    #[test]
    fn should_create_entrymeta_given_put_then_drain_with_meta_when_using_memtable() {
        // Arrange
        let mt = MemTable::new();
        let key = b"k1".to_vec();
        let val = b"v1".to_vec();

        // Act
        mt.put_with_seq(&key, &val, 42);
        let metas = mt.drain_with_meta();

        // Assert
        assert_eq!(metas.len(), 1);
        let m = &metas[0];
        assert_eq!(m.key, key);
        assert_eq!(m.value.clone(), Some(val));
        assert_eq!(m.sequence, 42);
        assert!(!m.is_tombstone, "should not be tombstone");
    }

    #[test]
    fn should_return_tombstone_given_delete_when_drained_with_meta() {
        // Arrange
        let mt = MemTable::new();
        let key = b"k2".to_vec();

        // Act
        mt.delete_with_seq(&key, 7);
        let metas = mt.drain_with_meta();

        // Assert
        assert_eq!(metas.len(), 1);
        let m = &metas[0];
        assert_eq!(m.key, key);
        assert!(m.value.is_none(), "value should be None for tombstone");
        assert_eq!(m.sequence, 7);
        assert!(m.is_tombstone, "should be tombstone");
    }

    #[test]
    fn should_return_true_when_is_empty_on_new_memtable() {
        // Arrange
        let mt = MemTable::new();

        // Act

        // Assert
        assert!(mt.is_empty());
    }

    #[test]
    fn should_return_false_when_is_empty_after_put() {
        // Arrange
        let mt = MemTable::new();

        // Act
        mt.put(b"key", b"value");

        // Assert
        assert!(!mt.is_empty());
    }

    #[test]
    fn should_return_true_when_is_empty_after_drain() {
        // Arrange
        let mt = MemTable::new();
        mt.put(b"key", b"value");

        // Act
        let _ = mt.drain();

        // Assert
        assert!(mt.is_empty());
    }

    #[test]
    fn should_delete_range_with_sequence() {
        // Arrange
        let mt = MemTable::new();
        mt.put_with_seq(b"a", b"1", 1);
        mt.put_with_seq(b"b", b"2", 2);
        mt.put_with_seq(b"c", b"3", 3);
        mt.put_with_seq(b"d", b"4", 4);

        // Act
        mt.delete_range_with_seq(b"b", b"d", 5);

        // Assert
        assert_eq!(mt.get(b"a"), Some(Bytes::from_static(b"1")));
        assert_eq!(mt.get(b"b"), None);
        assert_eq!(mt.get(b"c"), None);
        assert_eq!(mt.get(b"d"), Some(Bytes::from_static(b"4")));
    }

    #[test]
    fn should_drain_with_meta_internal_keys() {
        // Arrange
        let mt = MemTable::new();
        mt.put_with_seq(b"key1", b"value1", 10);
        mt.delete_with_seq(b"key2", 20);

        // Act
        let metas = mt.drain_with_meta_internal();

        // Assert
        assert_eq!(metas.len(), 2);

        // First entry: put with seq 10
        let entry1 = &metas[0];
        assert!(entry1.key.len() > b"key1".len()); // internal key is longer
        assert_eq!(entry1.value.as_ref().unwrap(), b"value1");
        assert_eq!(entry1.sequence, 10);
        assert!(!entry1.is_tombstone);
        assert!(entry1.expiration_millis.is_none());

        // Second entry: delete with seq 20
        let entry2 = &metas[1];
        assert!(entry2.key.len() > b"key2".len());
        assert!(entry2.value.is_none());
        assert_eq!(entry2.sequence, 20);
        assert!(entry2.is_tombstone);
        assert!(entry2.expiration_millis.is_none());
    }

    #[test]
    fn should_scan_range_with_no_bounds() {
        // Arrange
        let mt = MemTable::new();
        mt.put(b"a", b"1");
        mt.put(b"b", b"2");
        mt.put(b"c", b"3");

        // Act
        let rows = mt.scan_range(None, None);

        // Assert
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn should_handle_get_on_empty_memtable() {
        // Arrange
        let mt = MemTable::new();

        // Act
        let result = mt.get(b"nonexistent");

        // Assert
        assert_eq!(result, None);
    }

    #[test]
    fn should_handle_scan_on_empty_memtable() {
        // Arrange
        let mt = MemTable::new();

        // Act
        let rows = mt.scan_range(None, None);

        // Assert
        assert_eq!(rows.len(), 0);
    }

    // =====================================================================
    // P0: Drain ordering invariant tests
    // =====================================================================

    #[test]
    fn should_drain_all_versions_for_same_key() {
        // Arrange: Write multiple versions of the same key
        let mt = MemTable::new();
        mt.put_with_seq(b"key", b"v1", 10);
        mt.put_with_seq(b"key", b"v2", 20);
        mt.put_with_seq(b"key", b"v3", 30);

        // Act
        let metas = mt.drain_with_meta_internal();

        // Assert: All 3 versions should be returned
        assert_eq!(metas.len(), 3);
    }

    #[test]
    fn should_return_versions_in_sequence_descending_order_within_key() {
        // Arrange: Write versions in sequence order (how writes normally happen)
        // Note: The skiplist stores versions in insertion order (most recent insert first),
        // which matches sequence order when sequences are allocated monotonically.
        let mt = MemTable::new();
        mt.put_with_seq(b"key", b"old", 5);
        mt.put_with_seq(b"key", b"mid", 50);
        mt.put_with_seq(b"key", b"new", 100);

        // Act
        let metas = mt.drain_with_meta_internal();

        // Assert: Versions are returned in insertion order (most recent insert first)
        // which corresponds to newest sequence first when sequences are allocated in order
        let seqs: Vec<u64> = metas.iter().map(|m| m.sequence).collect();
        assert_eq!(seqs, vec![100, 50, 5], "Versions should be newest-first");
    }

    #[test]
    fn should_preserve_tombstone_in_drain_with_meta() {
        // Arrange
        let mt = MemTable::new();
        mt.put_with_seq(b"key", b"value", 10);
        mt.delete_with_seq(b"key", 20);

        // Act
        let metas = mt.drain_with_meta_internal();

        // Assert: Should have both the value and the tombstone
        assert_eq!(metas.len(), 2);
        let tombstone = metas.iter().find(|m| m.is_tombstone);
        assert!(tombstone.is_some());
        assert_eq!(tombstone.unwrap().sequence, 20);
    }

    #[test]
    fn should_encode_internal_keys_correctly_in_drain() {
        // Arrange
        let mt = MemTable::new();
        mt.put_with_seq(b"user_key", b"value", 42);

        // Act
        let metas = mt.drain_with_meta_internal();

        // Assert: Internal key should be longer than user key (has seq + type suffix)
        assert_eq!(metas.len(), 1);
        let entry = &metas[0];
        assert!(entry.key.len() > b"user_key".len());
        // Decode and verify
        if let Some((user, seq, tomb)) =
            crate::common::internal_key::decode_internal_key(&entry.key)
        {
            assert_eq!(user.as_slice(), b"user_key");
            assert_eq!(seq, 42);
            assert!(!tomb);
        } else {
            panic!("Failed to decode internal key");
        }
    }

    #[test]
    fn should_handle_interleaved_put_delete_operations_in_drain() {
        // Arrange
        let mt = MemTable::new();
        mt.put_with_seq(b"a", b"v1", 1);
        mt.delete_with_seq(b"a", 2);
        mt.put_with_seq(b"a", b"v3", 3);
        mt.put_with_seq(b"b", b"vb", 4);

        // Act
        let metas = mt.drain_with_meta_internal();

        // Assert: Should have all 4 entries
        assert_eq!(metas.len(), 4);
    }

    #[test]
    fn should_reset_size_after_drain() {
        // Arrange
        let mt = MemTable::new();
        mt.put(b"key1", b"value1");
        mt.put(b"key2", b"value2");
        assert!(mt.size_bytes() > 0);

        // Act
        let _ = mt.drain();

        // Assert
        assert_eq!(mt.size_bytes(), 0);
        assert!(mt.is_empty());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Bloom hint optimization tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn should_find_key_with_bloom_hint_enabled() {
        // Arrange
        let mt = MemTable::with_bloom_hint(100);
        mt.put(b"key1", b"value1");

        // Act
        let result = mt.get(b"key1");

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    }

    #[test]
    fn should_return_none_for_absent_key_with_bloom_hint() {
        // Arrange
        let mt = MemTable::with_bloom_hint(100);
        mt.put(b"key1", b"value1");

        // Act
        let result = mt.get(b"nonexistent_key");

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_find_key_at_snapshot_with_bloom_hint() {
        // Arrange
        let mt = MemTable::with_bloom_hint(100);
        mt.put_with_seq(b"key1", b"v1", 10);
        mt.put_with_seq(b"key1", b"v2", 20);

        // Act
        let v_at_15 = mt.get_at(b"key1", 15);
        let v_at_25 = mt.get_at(b"key1", 25);

        // Assert
        assert_eq!(v_at_15, Some(Bytes::from_static(b"v1")));
        assert_eq!(v_at_25, Some(Bytes::from_static(b"v2")));
    }

    #[test]
    fn should_handle_delete_with_bloom_hint() {
        // Arrange
        let mt = MemTable::with_bloom_hint(100);
        mt.put(b"key1", b"value1");
        mt.delete(b"key1");

        // Act
        let result = mt.get(b"key1");

        // Assert - key is deleted (tombstone)
        assert!(result.is_none());
    }

    #[test]
    fn should_retrieve_all_keys_given_many_inserts_with_bloom_hint_when_reading() {
        // Arrange
        let mt = MemTable::with_bloom_hint(1000);
        for i in 0..1000u32 {
            let key = format!("key_{:06}", i);
            let value = format!("value_{}", i);
            mt.put(key.as_bytes(), value.as_bytes());
        }

        // Act - retrieve all inserted keys
        let mut all_found = true;
        for i in 0..1000u32 {
            let key = format!("key_{:06}", i);
            let expected = format!("value_{}", i);
            let result = mt.get(key.as_bytes());
            if result != Some(Bytes::from(expected)) {
                all_found = false;
                break;
            }
        }

        // Assert
        assert!(all_found, "All inserted keys should be found");

        // Also verify non-existent keys are not found (part of same behavior: bloom hint accuracy)
        for i in 100_000..100_100u32 {
            let key = format!("key_{:06}", i);
            let result = mt.get(key.as_bytes());
            assert!(result.is_none(), "Key {} should not be found", key);
        }
    }
}
