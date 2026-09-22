//! Ordered in-memory state shared by the WAL recovery and SST publication layers.
//!
//! This module owns the concrete skiplist memtable and its conservative encoded
//! bounds. SST layout types remain imported temporarily while their ownership is
//! refactored separately.

pub(crate) mod size_bound;
pub(crate) mod skiplist;

/// Raw skiplist access for this crate's benchmark harnesses only.
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub mod bench {
    pub use super::skiplist::SkipList;
}

use crate::common::{MidgeError, MidgeResult};
use crate::memtable::skiplist::{OpType, SkipList};
use crate::sst::encoding::EntryType;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

/// The SST entry type for a memtable version.
///
/// Spelled out rather than reusing the encoded byte, so the two numbering
/// schemes are never conflated.
pub(crate) fn entry_type_of(op: OpType) -> EntryType {
    match op {
        OpType::Put => EntryType::Put,
        OpType::Delete => EntryType::Delete,
    }
}

type MemtableEntryWithMeta = (Vec<u8>, Option<Vec<u8>>, u64, Option<u64>, EntryType);

/// SkipList-based Memtable (lock-free, MVCC-aware)
pub struct SkipListMemtable {
    skiplist: Arc<SkipList>,
    size_bytes: std::sync::atomic::AtomicUsize,
    encoded_size_bound: std::sync::atomic::AtomicUsize,
    range_tombstone_count: std::sync::atomic::AtomicUsize,
    range_tombstones: RwLock<Vec<crate::sst::types::RangeTombstone>>,
}

impl SkipListMemtable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            skiplist: Arc::new(SkipList::new()),
            size_bytes: std::sync::atomic::AtomicUsize::new(0),
            encoded_size_bound: std::sync::atomic::AtomicUsize::new(size_bound::FIXED_SST_BYTES),
            range_tombstone_count: std::sync::atomic::AtomicUsize::new(0),
            range_tombstones: RwLock::new(Vec::new()),
        }
    }

    #[must_use]
    pub(crate) fn contains_key_sequence(&self, key: &[u8], sequence: u64) -> bool {
        self.skiplist.contains_sequence(key, sequence)
    }

    pub(crate) fn encoded_size_upper_bound(&self) -> usize {
        self.encoded_size_bound
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn add_encoded_size_bound(&self, bytes: usize) {
        let _ = self.encoded_size_bound.fetch_update(
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_add(bytes)),
        );
    }

    #[cfg(test)]
    fn is_expired(expiration: Option<u64>) -> bool {
        Self::is_expired_at(expiration, Self::current_time_millis())
    }

    #[cfg(test)]
    fn current_time_millis() -> u64 {
        crate::common::time::unix_time_millis()
    }

    fn is_expired_at(expiration: Option<u64>, now_millis: u64) -> bool {
        crate::common::time::is_expired_at(expiration, now_millis)
    }

    pub(crate) fn visit_frozen_versions(
        &self,
        budget: &crate::common::resource_budget::ResourceBudget,
        visit: impl FnMut(&[u8], Option<&[u8]>, u64, Option<u64>, OpType) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        self.skiplist.visit_versions(budget, visit)
    }

    pub(crate) fn visit_frozen_ranges(
        &self,
        mut visit: impl FnMut(&crate::sst::types::RangeTombstone) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        for range in self.range_tombstones.read().iter() {
            visit(range)?;
        }
        Ok(())
    }

    /// Iterate over all entries in the memtable.
    ///
    /// Returns every version in sorted key order and newest-first sequence
    /// order per key so flush/compaction paths can preserve metadata exactly.
    pub fn iter_all_with_meta(&self) -> Vec<MemtableEntryWithMeta> {
        self.skiplist
            .drain_with_meta_with_exp()
            .into_iter()
            .map(|(key, value, seq, _, exp, op)| {
                (
                    key.to_vec(),
                    value.map(|vb| vb.to_vec()),
                    seq,
                    exp,
                    entry_type_of(op),
                )
            })
            .collect()
    }

    /// Iterate over all entries in the memtable.
    /// Returns (key, value, sequence) tuples in sorted order.
    #[must_use]
    pub fn iter_all(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        self.iter_all_with_meta()
            .into_iter()
            .map(|(key, value, seq, _, _)| (key, value, seq))
            .collect()
    }

    /// Get visible value at or before `snapshot_seq` (respecting expirations).
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying memtable cannot service the lookup.
    #[cfg(test)]
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
    #[cfg(test)]
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
                                entry_type_of(entry.op),
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
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
        // Seek into the range instead of copying every version of the whole
        // memtable and filtering afterwards: a scan's cost follows its range.
        self.skiplist
            .range_visible_with_meta(start, end, snapshot_seq)
            .into_iter()
            .map(|(key, value, seq, is_tombstone, exp, op)| {
                let state = match (value, is_tombstone) {
                    (_, true) | (None, _) => crate::sst::types::KeyState::Tombstone(seq),
                    (Some(value), false) => {
                        if Self::is_expired_at(exp, now_millis) {
                            crate::sst::types::KeyState::Tombstone(seq)
                        } else {
                            crate::sst::types::KeyState::Value(value, seq, exp, entry_type_of(op))
                        }
                    }
                };
                (key.to_vec(), state)
            })
            .collect()
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
        let encoded_delta = size_bound::point_bytes(key.len(), value.len());
        if !self
            .skiplist
            .upsert_exp(key, Some(value), seq, expiration, OpType::Put)
        {
            return Err(MidgeError::Corruption(format!(
                "duplicate memtable key/sequence pair at sequence {seq}"
            )));
        }
        self.add_encoded_size_bound(encoded_delta);
        Ok(())
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
        let encoded_delta = size_bound::point_bytes(key.len(), 0);
        if !self.skiplist.delete(key, seq) {
            return Err(MidgeError::Corruption(format!(
                "duplicate memtable key/sequence pair at sequence {seq}"
            )));
        }
        self.add_encoded_size_bound(encoded_delta);
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

        let mut range_tombstones = self.range_tombstones.write();
        let old_capacity = range_tombstones.capacity();
        range_tombstones.push(crate::sst::types::RangeTombstone::new(
            start_key.to_vec(),
            end_key.to_vec(),
            seq,
        ));
        let capacity_bytes = range_tombstones
            .capacity()
            .saturating_sub(old_capacity)
            .saturating_mul(std::mem::size_of::<crate::sst::types::RangeTombstone>());
        let size_delta = capacity_bytes
            .saturating_add(start_key.len())
            .saturating_add(end_key.len());
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        self.range_tombstone_count
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        drop(range_tombstones);
        self.add_encoded_size_bound(size_bound::range_bytes(start_key.len(), end_key.len()));
        // Keep the estimate bounded by the tombstone itself. The range marker,
        // rather than one point tombstone per currently resident key, is the
        // durable representation.
        Ok(())
    }

    /// Return range tombstones visible at `snapshot_seq` in insertion order.
    #[must_use]
    pub fn range_tombstones_at(&self, snapshot_seq: u64) -> Vec<crate::sst::types::RangeTombstone> {
        if self
            .range_tombstone_count
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return Vec::new();
        }
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
        if self
            .range_tombstone_count
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return Vec::new();
        }
        self.range_tombstones.read().clone()
    }

    /// Return the conservatively accounted resident bytes retained by this
    /// memtable.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.skiplist
            .retained_bytes()
            .saturating_add(self.size_bytes.load(std::sync::atomic::Ordering::Relaxed))
    }
}

impl Default for SkipListMemtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl SkipListMemtable {
    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
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
}

#[cfg(test)]
mod tests {
    use super::SkipListMemtable;
    use crate::sst::types::KeyState;
    use crate::MidgeError;
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn should_return_newest_visible_state_per_key_only_within_range(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let memtable = SkipListMemtable::new();
        memtable.put_with_seq(b"a".to_vec(), b"before-range".to_vec(), 1, None)?;
        memtable.put_with_seq(b"b".to_vec(), b"old".to_vec(), 2, None)?;
        memtable.put_with_seq(b"b".to_vec(), b"new".to_vec(), 5, None)?;
        memtable.put_with_seq(b"c".to_vec(), b"live".to_vec(), 3, None)?;
        memtable.delete_bytes_with_seq(Bytes::from_static(b"c"), 6)?;
        memtable.put_with_seq(b"d".to_vec(), b"expired".to_vec(), 4, Some(10))?;
        memtable.put_with_seq(b"z".to_vec(), b"after-range".to_vec(), 7, None)?;

        // Act
        let latest = memtable.range_state_at_with_time(Some(b"b"), Some(b"z"), u64::MAX, 100);
        let at_seq_4 = memtable.range_state_at_with_time(Some(b"b"), Some(b"z"), 4, 100);

        // Assert
        assert_eq!(
            latest,
            vec![
                (
                    b"b".to_vec(),
                    KeyState::Value(
                        Bytes::from_static(b"new"),
                        5,
                        None,
                        crate::sst::encoding::EntryType::Put
                    )
                ),
                (b"c".to_vec(), KeyState::Tombstone(6)),
                (b"d".to_vec(), KeyState::Tombstone(4)),
            ]
        );
        assert_eq!(
            at_seq_4,
            vec![
                (
                    b"b".to_vec(),
                    KeyState::Value(
                        Bytes::from_static(b"old"),
                        2,
                        None,
                        crate::sst::encoding::EntryType::Put
                    )
                ),
                (
                    b"c".to_vec(),
                    KeyState::Value(
                        Bytes::from_static(b"live"),
                        3,
                        None,
                        crate::sst::encoding::EntryType::Put
                    )
                ),
                (b"d".to_vec(), KeyState::Tombstone(4)),
            ]
        );
        Ok(())
    }

    #[test]
    fn should_match_flush_writer_bound_when_memtable_contains_versions_and_range_tombstones() {
        // Arrange
        let memtable = SkipListMemtable::new();
        memtable
            .put_with_seq(b"key".to_vec(), vec![b'a'; 4096], 10, None)
            .expect("first version");
        memtable
            .put_with_seq(b"key".to_vec(), vec![b'b'; 8192], 11, Some(200))
            .expect("second version");
        memtable
            .delete_with_seq(b"removed".to_vec(), 12)
            .expect("point tombstone");
        memtable
            .delete_range_with_seq(b"start", b"stop", 13)
            .expect("range tombstone");
        let before_duplicate = memtable.encoded_size_upper_bound();
        let duplicate = memtable.put_with_seq(b"key".to_vec(), vec![b'c'; 16], 10, None);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = crate::sst::SstFactory::create(&factory).expect("writer");

        // Act
        for (key, value, sequence, expiration, op) in memtable.iter_all_with_meta() {
            writer
                .add_with_meta(&key, value.as_deref(), sequence, op, expiration)
                .expect("writer point");
        }
        for range in memtable.range_tombstones() {
            writer
                .add_range_tombstone(&range.start, &range.end, range.seq)
                .expect("writer range");
        }

        // Assert
        assert!(duplicate.is_err());
        assert_eq!(memtable.encoded_size_upper_bound(), before_duplicate);
        assert_eq!(
            Some(memtable.encoded_size_upper_bound()),
            writer.encoded_size_upper_bound()
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
            KeyState::Value(value, 10, None, crate::sst::encoding::EntryType::Put) if value == Bytes::from_static(b"old")
        ));
        assert!(matches!(
            at_25,
            KeyState::Value(value, 20, None, crate::sst::encoding::EntryType::Put) if value == Bytes::from_static(b"new")
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
            KeyState::Value(value, 10, None, crate::sst::encoding::EntryType::Put) if value == Bytes::from_static(b"value")
        ));
        assert!(matches!(after_delete, KeyState::Tombstone(20)));
    }

    #[test]
    fn should_preserve_exact_size_when_duplicate_sequence_rejected() {
        // Arrange
        let memtable = SkipListMemtable::new();

        // Act
        memtable
            .put_bytes_with_seq(
                Bytes::from_static(b"key"),
                Bytes::from_static(b"value"),
                1,
                None,
            )
            .expect("first put");
        let after_put = memtable.size_bytes();
        let duplicate = memtable.put_bytes_with_seq(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"other"),
            1,
            None,
        );
        memtable
            .delete_bytes_with_seq(Bytes::from_static(b"dead"), 2)
            .expect("delete");
        memtable
            .delete_range_with_seq(b"a", b"z", 3)
            .expect("range delete");

        // Assert
        assert!(after_put > 3 + 5);
        assert!(matches!(duplicate, Err(MidgeError::Corruption(_))));
        assert!(memtable.size_bytes() > after_put + 4 + 1 + 1);
    }

    #[test]
    fn should_grow_accounted_bytes_given_repeated_versions_of_same_key() {
        // Arrange
        let memtable = SkipListMemtable::new();
        let mut previous = 0;

        // Act
        for sequence in 1..=3 {
            memtable
                .put_bytes_with_seq(
                    Bytes::from_static(b"key"),
                    Bytes::from_static(b"v"),
                    sequence,
                    None,
                )
                .expect("version put");
            let current = memtable.size_bytes();
            assert!(current > previous);
            previous = current;
        }

        // Assert
        assert!(memtable.size_bytes() > 3 * (3 + 1));
    }

    #[test]
    fn should_never_expose_write_before_its_size_reservation() {
        // Arrange
        let memtable = Arc::new(SkipListMemtable::new());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writer_memtable = Arc::clone(&memtable);
        let writer_barrier = Arc::clone(&barrier);
        let writer = std::thread::spawn(move || {
            writer_barrier.wait();
            writer_memtable
                .put_bytes_with_seq(
                    Bytes::from_static(b"visible-key"),
                    Bytes::from_static(b"visible-value"),
                    1,
                    None,
                )
                .expect("put visible value");
        });

        // Act
        barrier.wait();
        while memtable
            .get_bytes_at_seq(b"visible-key", u64::MAX)
            .expect("read memtable")
            .is_none()
        {
            std::hint::spin_loop();
        }

        // Assert
        assert!(memtable.size_bytes() >= b"visible-key".len() + b"visible-value".len() + 16);
        writer.join().expect("join writer");
    }
}
