//! Compaction execution: version collection, merging, and output
//!
//! This module implements a K-way compaction pipeline:
//!   1. Collect one logical-version buffer per input SST.
//!   2. Merge their heads into a sorted stream (key ascending, seq descending).
//!   3. Deduplicate one key at a time (newest version first).
//!   4. Normalize expired values to masking tombstones on-the-fly.
//!   5. Feed the result directly to the `SstFactory` writer.
//!
//! This avoids an additional all-input vector, key map, and deduplicated output
//! vector. The reader trait currently materializes one buffer per SST.

use crate::common::MidgeResult;
use crate::sst::traits::SstFactory;
use crate::sst::types::{KeyState, RangeTombstone};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::path::Path;

/// A single logical version of a key observed during compaction.
///
/// Compaction consumers treat this as the "flattened" key history:
///   - `seq` is strictly monotonic per write.
///   - Higher `seq` means "newer".
///   - Tombstones represent deletions.
///   - TTL is expressed as an absolute expiry timestamp (milliseconds since epoch).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionVersion {
    /// User key
    pub key: Vec<u8>,
    /// Sequence number (higher = newer)
    pub seq: u64,
    /// Whether this is a tombstone (deletion marker)
    pub is_tombstone: bool,
    /// Value bytes (None if tombstone)
    pub value: Option<Vec<u8>>,
    /// Expiration time in milliseconds since epoch (optional)
    pub expiration: Option<u64>,
}

/// Stream-based deduplicator: yields only the first (highest-seq) version per key.
///
/// This adapter sits between the version merge and the write path. It consumes
/// the merged stream (which is already key-ascending, seq-descending) and emits
/// exactly one entry per unique key—the first one it sees for that key.
///
/// Memory usage: O(deduplicated key count) only for tracking the most recent key.
#[cfg(test)]
pub struct StreamDeduplicate<I: Iterator<Item = CompactionVersion>> {
    inner: I,
    last_key: Option<Vec<u8>>,
    now_millis: u64,
}

#[cfg(test)]
impl<I: Iterator<Item = CompactionVersion>> StreamDeduplicate<I> {
    pub fn new(inner: I) -> Self {
        let now_millis = crate::common::time::unix_time_millis();

        Self {
            inner,
            last_key: None,
            now_millis,
        }
    }
}

#[cfg(test)]
impl<I: Iterator<Item = CompactionVersion>> Iterator for StreamDeduplicate<I> {
    type Item = CompactionVersion;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let version = self.inner.next()?;

            let version = if is_expired(&version, self.now_millis) && !version.is_tombstone {
                CompactionVersion {
                    key: version.key.clone(),
                    seq: version.seq,
                    is_tombstone: true,
                    value: None,
                    expiration: None,
                }
            } else {
                version
            };

            // Skip duplicate keys (we already emitted the highest-seq version of this key)
            if let Some(ref last_key) = self.last_key {
                if last_key == &version.key {
                    continue;
                }
            }

            // New key; remember it and emit this version
            self.last_key = Some(version.key.clone());
            return Some(version);
        }
    }
}

/// Return `true` if this version is expired with respect to `now_millis`.
fn is_expired(version: &CompactionVersion, now_millis: u64) -> bool {
    crate::common::time::is_expired_at(version.expiration, now_millis)
}

fn tombstone_is_obsolete(sequence: u64, snapshot_horizon: Option<u64>) -> bool {
    snapshot_horizon.is_none_or(|horizon| sequence <= horizon)
}

fn ensure_compaction_not_aborted(abort_check: Option<&dyn Fn() -> bool>) -> MidgeResult<()> {
    if abort_check.is_some_and(|check| check()) {
        return Err(crate::common::MidgeError::Aborted(
            "compaction aborted due to ingest epoch change".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SstCompactionInput {
    pub versions: Vec<CompactionVersion>,
    pub range_tombstones: Vec<RangeTombstone>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TombstoneGcPolicy {
    pub(crate) snapshot_horizon: Option<u64>,
    pub(crate) point_eligible: bool,
    pub(crate) range_eligible: bool,
}

/// One sorted input and its current merge head.
struct VersionMergeInput {
    iter: std::vec::IntoIter<CompactionVersion>,
    current: Option<CompactionVersion>,
}

/// Heap item that orders compaction versions by key ascending and sequence
/// descending. `BinaryHeap` is a max heap, so the key ordering is inverted.
#[derive(Debug, Clone)]
struct VersionHeapItem {
    key: Vec<u8>,
    seq: u64,
    input_idx: usize,
}

impl PartialEq for VersionHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq && self.input_idx == other.input_idx
    }
}

impl Eq for VersionHeapItem {}

impl PartialOrd for VersionHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => match self.seq.cmp(&other.seq) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => self.input_idx.cmp(&other.input_idx).reverse(),
            },
        }
    }
}

/// K-way merge over the per-SST version vectors. It emits one input head at a
/// time, so deduplication and SST writing never need an additional global
/// version vector or key map.
struct VersionMergeIterator {
    inputs: Vec<VersionMergeInput>,
    heap: BinaryHeap<VersionHeapItem>,
}

impl VersionMergeIterator {
    fn new(mut streams: Vec<Vec<CompactionVersion>>) -> Self {
        let mut inputs = Vec::with_capacity(streams.len());
        let mut heap = BinaryHeap::new();

        for (input_idx, stream) in streams.iter_mut().enumerate() {
            stream.sort_by(|left, right| {
                left.key
                    .cmp(&right.key)
                    .then_with(|| right.seq.cmp(&left.seq))
            });
            let mut iter = std::mem::take(stream).into_iter();
            let current = iter.next();
            if let Some(entry) = &current {
                heap.push(VersionHeapItem {
                    key: entry.key.clone(),
                    seq: entry.seq,
                    input_idx,
                });
            }
            inputs.push(VersionMergeInput { iter, current });
        }

        Self { inputs, heap }
    }
}

impl Iterator for VersionMergeIterator {
    type Item = CompactionVersion;

    fn next(&mut self) -> Option<Self::Item> {
        let head = self.heap.pop()?;
        let input = self.inputs.get_mut(head.input_idx)?;
        let current = input.current.take()?;

        if let Some(next) = input.iter.next() {
            self.heap.push(VersionHeapItem {
                key: next.key.clone(),
                seq: next.seq,
                input_idx: head.input_idx,
            });
            input.current = Some(next);
        }

        Some(current)
    }
}

fn normalize_range_tombstones(mut tombstones: Vec<RangeTombstone>) -> Vec<RangeTombstone> {
    tombstones.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
            .then_with(|| right.seq.cmp(&left.seq))
    });
    tombstones.dedup();
    tombstones
}

fn collect_reader_input(
    reader: &dyn crate::sst::traits::SstReaderExt,
) -> MidgeResult<SstCompactionInput> {
    let versions = reader
        .scan_range_state(None, None)?
        .into_iter()
        .filter_map(|(key, state)| match state {
            KeyState::Absent => None,
            KeyState::Tombstone(seq) => Some(CompactionVersion {
                key: key.to_vec(),
                seq,
                is_tombstone: true,
                value: None,
                expiration: None,
            }),
            KeyState::Value(value, seq, expiration, _op_type) => Some(CompactionVersion {
                key: key.to_vec(),
                seq,
                is_tombstone: false,
                value: Some(value.to_vec()),
                expiration,
            }),
        })
        .collect();

    Ok(SstCompactionInput {
        versions,
        range_tombstones: reader.range_tombstones(),
    })
}

/// Materialize each input SST independently for a K-way merge. Reader APIs
/// currently return vectors, but retaining those per-input buffers avoids a
/// second all-input vector and lets downstream deduplication write directly
/// to the output stream.
pub(crate) fn collect_compaction_stream_inputs(
    sst_factory: &dyn SstFactory,
    input_files: &[String],
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<(Vec<Vec<CompactionVersion>>, Vec<RangeTombstone>)> {
    let mut streams = Vec::with_capacity(input_files.len());
    let mut range_tombstones = Vec::new();

    for filename in input_files {
        // Periodically check whether we should abort (cooperative cancellation)
        if let Some(check) = abort_check {
            if check() {
                tracing::info!(file = %filename, "compaction aborting due to ingest epoch change");
                return Err(crate::common::MidgeError::Aborted(
                    "compaction aborted due to ingest epoch change".to_string(),
                ));
            }
        }

        let path = Path::new(filename);

        let reader = sst_factory.open(path)?;
        let input = collect_reader_input(reader.as_ref())?;
        if !input.range_tombstones.is_empty() {
            tracing::debug!(
                file = %filename,
                count = input.range_tombstones.len(),
                "compaction observed SST range tombstones"
            );
        }
        streams.push(input.versions);
        range_tombstones.extend(input.range_tombstones);
    }

    Ok((streams, normalize_range_tombstones(range_tombstones)))
}

/// Deduplicate versions, keeping only the newest **non-expired** entry per key.
///
/// Rules:
///   - Versions with TTL that has passed at compaction time are discarded.
///   - Among remaining versions, we keep the one with the highest `seq` per key.
///   - Output is sorted by key in ascending order.
///
/// This is a pure, side-effect-free helper and is intentionally independent of
/// any particular SST layout.
#[cfg(test)]
pub fn deduplicate_versions(versions: &[CompactionVersion]) -> Vec<CompactionVersion> {
    let now_millis = crate::common::time::unix_time_millis();

    // Map: key -> newest visible version (by sequence).
    let mut newest_by_key: BTreeMap<Vec<u8>, CompactionVersion> = BTreeMap::new();

    for version in versions {
        let normalized = if is_expired(version, now_millis) && !version.is_tombstone {
            CompactionVersion {
                key: version.key.clone(),
                seq: version.seq,
                is_tombstone: true,
                value: None,
                expiration: None,
            }
        } else {
            version.clone()
        };

        let key = &normalized.key;

        match newest_by_key.get(key) {
            None => {
                // First observation of this key.
                newest_by_key.insert(key.clone(), normalized);
            }
            Some(existing) => {
                // Keep the one with the higher sequence number.
                if normalized.seq > existing.seq {
                    newest_by_key.insert(key.clone(), normalized);
                }
            }
        }
    }

    // BTreeMap keeps keys sorted; just collect in order.
    newest_by_key.into_values().collect()
}

/// Filter out tombstones from a deduplicated version set.
///
/// NOTE:
///   - This default function removes all tombstones unconditionally (legacy behavior).
///   - Prefer `filter_tombstones_with_horizon()` which is snapshot-aware and only
///     drops tombstones older than the provided snapshot horizon.
#[cfg(test)]
pub fn filter_tombstones(versions: &[CompactionVersion]) -> Vec<CompactionVersion> {
    filter_tombstones_with_horizon(versions, None)
}

/// Filter tombstones but preserve those newer than `snapshot_horizon` (if provided).
///
/// Semantics:
///   - If `snapshot_horizon` is `None`, all tombstones are dropped (legacy behavior).
///   - If `Some(h)`, tombstones with `seq > h` are preserved to prevent resurrection
///     for snapshots reading at sequence `h` or earlier.
#[cfg(test)]
pub fn filter_tombstones_with_horizon(
    versions: &[CompactionVersion],
    snapshot_horizon: Option<u64>,
) -> Vec<CompactionVersion> {
    versions
        .iter()
        .filter(|version| {
            !(version.is_tombstone && tombstone_is_obsolete(version.seq, snapshot_horizon))
        })
        .cloned()
        .collect()
}

/// Merge, normalize, deduplicate, and write compaction versions without
/// materializing a second deduplicated result vector.
pub(crate) fn write_merged_compaction_output_to_sst(
    sst_factory: &dyn SstFactory,
    output_filename: &str,
    streams: Vec<Vec<CompactionVersion>>,
    range_tombstones: &[RangeTombstone],
    tombstone_gc: TombstoneGcPolicy,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<bool> {
    let mut writer = sst_factory.create()?;
    let now_millis = crate::common::time::unix_time_millis();
    let mut last_key: Option<Vec<u8>> = None;
    let mut written = 0usize;
    let (obsolete_range_tombstones, retained_range_tombstones): (Vec<_>, Vec<_>) =
        range_tombstones.iter().partition(|tombstone| {
            tombstone_gc.range_eligible
                && tombstone_is_obsolete(tombstone.seq, tombstone_gc.snapshot_horizon)
        });

    let mut merged = VersionMergeIterator::new(streams).peekable();
    let mut seen = 0usize;
    while let Some(mut version) = merged.next() {
        if seen.is_multiple_of(1024) {
            ensure_compaction_not_aborted(abort_check)?;
        }
        seen = seen.saturating_add(1);

        while merged
            .peek()
            .is_some_and(|candidate| candidate.key == version.key && candidate.seq == version.seq)
        {
            let duplicate = merged.next().expect("peeked compaction version exists");
            seen = seen.saturating_add(1);
            if seen.is_multiple_of(1024) {
                ensure_compaction_not_aborted(abort_check)?;
            }
            if duplicate != version {
                return Err(crate::common::MidgeError::Corruption(format!(
                    "conflicting compaction versions for key {:?} at sequence {}",
                    String::from_utf8_lossy(&version.key),
                    version.seq
                )));
            }
        }

        if is_expired(&version, now_millis) && !version.is_tombstone {
            version.is_tombstone = true;
            version.value = None;
            version.expiration = None;
        }

        if last_key.as_deref() == Some(version.key.as_slice()) {
            continue;
        }
        last_key = Some(version.key.clone());

        if obsolete_range_tombstones
            .iter()
            .any(|tombstone| tombstone.covers(&version.key) && tombstone.seq >= version.seq)
        {
            continue;
        }
        if tombstone_gc.point_eligible
            && version.is_tombstone
            && tombstone_is_obsolete(version.seq, tombstone_gc.snapshot_horizon)
        {
            continue;
        }

        writer.add_sorted_with_meta(
            &version.key,
            version.value.as_deref(),
            version.seq,
            u8::from(version.is_tombstone) * 2,
            version.expiration,
        )?;
        written = written.saturating_add(1);
    }

    for (index, tombstone) in retained_range_tombstones.iter().enumerate() {
        if index % 1024 == 0 {
            ensure_compaction_not_aborted(abort_check)?;
        }
        writer.add_range_tombstone(&tombstone.start, &tombstone.end, tombstone.seq)?;
    }

    ensure_compaction_not_aborted(abort_check)?;

    if written == 0 && retained_range_tombstones.is_empty() {
        return Ok(false);
    }

    let output_path = Path::new(output_filename);
    crate::sst::fs::finish_writer_to_path(writer, output_path)?;

    // Cancellation can race with finalization. Once an output exists, it is
    // still only staged: remove it before reporting the abort so a later
    // compaction cannot mistake it for a publishable replacement.
    if abort_check.is_some_and(|check| check()) {
        match std::fs::remove_file(output_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(crate::common::MidgeError::Io(error)),
        }
        return Err(crate::common::MidgeError::Aborted(
            "compaction aborted due to ingest epoch change".to_string(),
        ));
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_version<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        key: K,
        seq: u64,
        is_tombstone: bool,
        value: Option<V>,
        expiration: Option<u64>,
    ) -> CompactionVersion {
        CompactionVersion {
            key: key.as_ref().to_vec(),
            seq,
            is_tombstone,
            value: value.map(|v| v.as_ref().to_vec()),
            expiration,
        }
    }

    #[test]
    fn should_keep_highest_sequence_when_deduplicating_versions() {
        // Arrange
        let versions = vec![
            mk_version("key1", 1, false, Some("value1"), None),
            mk_version("key1", 2, false, Some("value1_updated"), None),
        ];

        // Act
        let deduped = deduplicate_versions(&versions);

        // Assert
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].seq, 2);
        assert_eq!(
            deduped[0].value.as_deref(),
            Some(b"value1_updated".as_ref())
        );
    }

    #[test]
    fn should_remove_tombstones_when_filtering_versions() {
        // Arrange
        let versions = vec![
            mk_version("key1", 1, false, Some("value1"), None),
            mk_version("key2", 2, true, None::<&[u8]>, None),
        ];

        // Act
        let filtered = filter_tombstones(&versions);

        // Assert
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].key, b"key1".to_vec());
    }

    #[test]
    fn should_skip_expired_entries_when_deduplicating_with_ttl() {
        // Arrange
        let now = crate::common::time::unix_time_millis();

        let versions = vec![
            mk_version(
                "key1",
                1,
                false,
                Some("expired"),
                Some(now.saturating_sub(1)), // Expired
            ),
            mk_version("key1", 2, false, Some("valid"), None),
        ];

        // Act
        let deduped = deduplicate_versions(&versions);

        // Assert
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].value.as_deref(), Some(b"valid".as_ref()));
    }

    #[test]
    fn should_deduplicate_multiple_keys_independently() {
        // Arrange
        let versions = vec![
            mk_version("a", 1, false, Some("a1"), None),
            mk_version("a", 3, false, Some("a3"), None),
            mk_version("b", 2, false, Some("b2"), None),
            mk_version("b", 1, false, Some("b1"), None),
        ];

        // Act
        let deduped = deduplicate_versions(&versions);

        // Assert

        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].key, b"a".to_vec());
        assert_eq!(deduped[0].seq, 3);
        assert_eq!(deduped[1].key, b"b".to_vec());
        assert_eq!(deduped[1].seq, 2);
    }

    #[test]
    fn should_merge_version_streams_by_key_then_descending_sequence() {
        // Arrange
        let left = vec![
            mk_version("a", 3, false, Some("a3"), None),
            mk_version("c", 1, false, Some("c1"), None),
        ];
        let right = vec![
            mk_version("a", 4, false, Some("a4"), None),
            mk_version("b", 2, false, Some("b2"), None),
        ];

        // Act
        let merged: Vec<_> = VersionMergeIterator::new(vec![left, right]).collect();

        // Assert
        assert_eq!(
            merged
                .iter()
                .map(|version| (version.key.clone(), version.seq))
                .collect::<Vec<_>>(),
            vec![
                (b"a".to_vec(), 4),
                (b"a".to_vec(), 3),
                (b"b".to_vec(), 2),
                (b"c".to_vec(), 1),
            ]
        );
    }

    #[test]
    fn should_use_input_index_given_equal_key_sequence_when_merging() {
        // Arrange
        let first = vec![mk_version("same", 7, false, Some("first"), None)];
        let second = vec![mk_version("same", 7, false, Some("second"), None)];

        // Act
        let merged: Vec<_> = VersionMergeIterator::new(vec![first, second]).collect();

        // Assert
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].value.as_deref(), Some(b"first".as_ref()));
        assert_eq!(merged[1].value.as_deref(), Some(b"second".as_ref()));
    }

    #[test]
    fn should_drop_expired_value_given_compaction_time_after_expiration() {
        let now = 1_000_000u64;
        let v = mk_version("k", 1, false, Some("v"), Some(now - 1));
        assert!(is_expired(&v, now));
    }

    #[test]
    fn should_not_drop_unexpired_value_given_compaction_time_before_expiration() {
        // Arrange
        let now = 1_000_000u64;
        let v_future = mk_version("k", 1, false, Some("v"), Some(now + 10));
        let v_none = mk_version("k", 1, false, Some("v"), None);

        // Act
        let future_expired = is_expired(&v_future, now);
        let none_expired = is_expired(&v_none, now);

        // Assert
        assert!(!future_expired);
        assert!(!none_expired);
    }

    #[test]
    fn should_preserve_tombstone_visibility_given_overlapping_ssts_when_scanning() {
        use crate::sst::traits::{SstReader, SstStateReader};

        // Arrange
        struct FakeReader;

        impl SstReader for FakeReader {
            fn get(&self, _key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
                Ok(None)
            }

            fn scan_range(
                &self,
                _start: Option<&[u8]>,
                _end: Option<&[u8]>,
            ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
                Ok(Vec::new())
            }
        }

        impl SstStateReader for FakeReader {
            fn get_state(&self, _key: &[u8]) -> MidgeResult<KeyState> {
                Ok(KeyState::Absent)
            }

            fn scan_range_state(
                &self,
                _start: Option<&[u8]>,
                _end: Option<&[u8]>,
            ) -> MidgeResult<Vec<(bytes::Bytes, KeyState)>> {
                Ok(vec![
                    (
                        bytes::Bytes::from_static(b"alpha"),
                        KeyState::Value(bytes::Bytes::from_static(b"v1"), 42, Some(900), 0),
                    ),
                    (bytes::Bytes::from_static(b"beta"), KeyState::Tombstone(41)),
                ])
            }

            fn range_tombstones(&self) -> Vec<RangeTombstone> {
                vec![RangeTombstone::new(b"c".to_vec(), b"f".to_vec(), 40)]
            }
        }

        // Act
        let input = collect_reader_input(&FakeReader).expect("collect stateful input");

        // Assert
        assert_eq!(input.versions.len(), 2);
        assert_eq!(input.versions[0].key, b"alpha".to_vec());
        assert_eq!(input.versions[0].seq, 42);
        assert_eq!(input.versions[0].expiration, Some(900));
        assert!(input.versions[1].is_tombstone);
        assert_eq!(input.range_tombstones.len(), 1);
        assert_eq!(input.range_tombstones[0].start, b"c".to_vec());
    }

    #[test]
    fn should_normalize_duplicate_range_tombstones_when_collecting_input() {
        // Arrange
        let tombstones = vec![
            RangeTombstone::new(b"m".to_vec(), b"z".to_vec(), 7),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 9),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 9),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 5),
        ];

        // Act
        let normalized = normalize_range_tombstones(tombstones);

        // Assert
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized[0].start, b"a".to_vec());
        assert_eq!(normalized[0].seq, 9);
        assert_eq!(normalized[1].seq, 5);
        assert_eq!(normalized[2].start, b"m".to_vec());
    }

    #[test]
    fn should_stream_deduplicate_multiple_versions_when_using_iterator() {
        // Arrange: Create two input iterators (simulating SST readers)
        let stream1 = vec![
            mk_version("a", 3, false, Some("a3"), None),
            mk_version("a", 1, false, Some("a1"), None),
            mk_version("c", 2, false, Some("c2"), None),
        ];

        let stream2 = vec![
            mk_version("a", 2, false, Some("a2"), None),
            mk_version("b", 5, false, Some("b5"), None),
        ];

        // Act: Create merge iterator from both streams
        let version_iter = VersionMergeIterator::new(vec![stream1, stream2]);

        // Stream deduplicate (keeps only first/highest-seq per key)
        let dedup_iter = StreamDeduplicate::new(version_iter);

        // Collect results
        let deduped: Vec<_> = dedup_iter.collect();

        // Assert
        // Should have 3 unique keys: a (seq 3), b (seq 5), c (seq 2)
        assert_eq!(deduped.len(), 3);

        // Check order: keys should be in ascending order
        assert_eq!(deduped[0].key, b"a".to_vec());
        assert_eq!(deduped[0].seq, 3); // Highest seq for 'a'

        assert_eq!(deduped[1].key, b"b".to_vec());
        assert_eq!(deduped[1].seq, 5);

        assert_eq!(deduped[2].key, b"c".to_vec());
        assert_eq!(deduped[2].seq, 2);
    }

    #[test]
    fn should_retain_recent_tombstone_given_snapshot_horizon_when_compacting() {
        // Arrange: create versions where one key has a recent tombstone
        let recent_tombstone = mk_version("k", 200, true, None::<&[u8]>, None);
        let older_put = mk_version("k", 100, false, Some("v"), None);
        let versions = vec![older_put, recent_tombstone.clone()];

        // Act: filter tombstones with a snapshot horizon of 150
        // Tombstones newer than the horizon (seq > 150) must be preserved.
        let filtered = filter_tombstones_with_horizon(&versions, Some(150));

        // Assert: recent tombstone (seq 200) should be preserved
        assert!(
            filtered.iter().any(|v| v.is_tombstone && v.seq == 200),
            "expected recent tombstone to be preserved by compaction filter (snapshot-aware)"
        );
    }

    #[test]
    fn should_drop_tombstone_when_sequence_equals_snapshot_horizon() {
        // Arrange
        let versions = vec![
            mk_version("k_old", 5, false, Some("old"), None),
            mk_version("k_eq", 150, true, None::<&[u8]>, None),
            mk_version("k_new", 151, true, None::<&[u8]>, None),
        ];

        // Act
        let filtered = filter_tombstones_with_horizon(&versions, Some(150));

        // Assert
        assert!(
            !filtered
                .iter()
                .any(|v| v.key == b"k_eq".to_vec() && v.is_tombstone),
            "tombstone at the exact horizon must be dropped"
        );
        assert!(
            filtered
                .iter()
                .any(|v| v.key == b"k_new".to_vec() && v.is_tombstone),
            "tombstone above horizon must be preserved"
        );
        assert!(
            filtered
                .iter()
                .any(|v| v.key == b"k_old".to_vec() && !v.is_tombstone),
            "non-tombstone entries should remain"
        );
    }

    #[test]
    fn should_drop_tombstone_when_sequence_is_below_snapshot_horizon() {
        // Arrange
        let versions = vec![
            mk_version("k_low", 149, true, None::<&[u8]>, None),
            mk_version("k_high", 200, true, None::<&[u8]>, None),
        ];

        // Act
        let filtered = filter_tombstones_with_horizon(&versions, Some(150));

        // Assert
        assert!(
            !filtered
                .iter()
                .any(|v| v.key == b"k_low".to_vec() && v.is_tombstone),
            "tombstone below horizon must be dropped"
        );
        assert!(
            filtered
                .iter()
                .any(|v| v.key == b"k_high".to_vec() && v.is_tombstone),
            "tombstone above horizon must be preserved"
        );
    }

    #[test]
    fn should_merge_nonempty_stream_when_another_input_stream_is_empty() {
        // Arrange
        let stream1 = vec![];
        let stream2 = vec![mk_version("k", 10, false, Some("v"), None)];

        // Act
        let version_iter = VersionMergeIterator::new(vec![stream1, stream2]);

        let dedup_iter = StreamDeduplicate::new(version_iter);
        let result: Vec<_> = dedup_iter.collect();

        // Assert

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"k".to_vec());
        assert_eq!(result[0].seq, 10);
    }

    #[test]
    fn should_deduplicate_correctly_across_streams_with_overlapping_keys() {
        // Arrange
        // Stream 1: a(5), a(3), b(4)
        let stream1 = vec![
            mk_version("a", 5, false, Some("a5"), None),
            mk_version("a", 3, false, Some("a3"), None),
            mk_version("b", 4, false, Some("b4"), None),
        ];

        // Stream 2: a(4), b(6), c(2)
        let stream2 = vec![
            mk_version("a", 4, false, Some("a4"), None),
            mk_version("b", 6, false, Some("b6"), None),
            mk_version("c", 2, false, Some("c2"), None),
        ];

        let version_iter = VersionMergeIterator::new(vec![stream1, stream2]);

        // Act
        let dedup_iter = StreamDeduplicate::new(version_iter);
        let result: Vec<_> = dedup_iter.collect();

        // Assert: Should have 3 keys: a(5), b(6), c(2)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].seq, 5); // a: highest is 5
        assert_eq!(result[1].seq, 6); // b: highest is 6
        assert_eq!(result[2].seq, 2); // c: only has 2
    }
}
