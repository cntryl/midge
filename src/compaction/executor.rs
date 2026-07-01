//! Compaction execution: version collection, merging, and output
//!
//! This module implements a **streaming** compaction pipeline:
//!   1. Collect per-SST iterators of logical "versions" from input files.
//!   2. Merge them into a single sorted stream (key ascending, seq descending).
//!   3. Stream through deduplication (one entry per key, newest first).
//!   4. Drop expired entries on-the-fly (TTL-based filtering).
//!   5. Optionally filter tombstones for final output.
//!   6. Stream to output SST using the `SstFactory` writer.
//!
//! The streaming design ensures constant memory usage regardless of input size.
//! The API remains backward compatible with the original batch helpers.

use crate::common::MidgeResult;
use crate::sst::traits::SstFactory;
use crate::sst::types::{KeyState, RangeTombstone};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// A single logical version of a key observed during compaction.
///
/// Compaction consumers treat this as the "flattened" key history:
///   - `seq` is strictly monotonic per write.
///   - Higher `seq` means "newer".
///   - Tombstones represent deletions.
///   - TTL is expressed as an absolute expiry timestamp (seconds since epoch).
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
    /// Expiration time in seconds since epoch (optional)
    pub expiration: Option<u64>,
}

/// Stream-based deduplicator: yields only the first (highest-seq) version per key.
///
/// This adapter sits between `MergeIterator` and the write path. It consumes
/// the merged stream (which is already key-ascending, seq-descending) and emits
/// exactly one entry per unique key—the first one it sees for that key.
///
/// Memory usage: O(deduplicated key count) only for tracking the most recent key.
pub struct StreamDeduplicate<I: Iterator<Item = CompactionVersion>> {
    inner: I,
    last_key: Option<Vec<u8>>,
    now_secs: u64,
}

impl<I: Iterator<Item = CompactionVersion>> StreamDeduplicate<I> {
    pub fn new(inner: I) -> Self {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());

        Self {
            inner,
            last_key: None,
            now_secs,
        }
    }
}

impl<I: Iterator<Item = CompactionVersion>> Iterator for StreamDeduplicate<I> {
    type Item = CompactionVersion;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let version = self.inner.next()?;

            // Skip expired entries
            if is_expired(&version, self.now_secs) {
                continue;
            }

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

/// Return `true` if this version is expired with respect to `now_secs`.
fn is_expired(version: &CompactionVersion, now_secs: u64) -> bool {
    matches!(version.expiration, Some(exp) if exp <= now_secs)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SstCompactionInput {
    pub versions: Vec<CompactionVersion>,
    pub range_tombstones: Vec<RangeTombstone>,
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

/// Collect compaction input from the given input SST files.
pub fn collect_compaction_input(
    sst_factory: &dyn SstFactory,
    input_files: &[String],
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<SstCompactionInput> {
    let mut versions = Vec::new();
    let mut range_tombstones = Vec::new();

    for filename in input_files {
        // Periodically check whether we should abort (cooperative cancellation)
        if let Some(check) = abort_check {
            if check() {
                tracing::info!(file = %filename, "compaction aborting due to ingest epoch change");
                return Ok(SstCompactionInput::default());
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
        versions.extend(input.versions);
        range_tombstones.extend(input.range_tombstones);
    }

    versions.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| right.seq.cmp(&left.seq))
    });

    Ok(SstCompactionInput {
        versions,
        range_tombstones: normalize_range_tombstones(range_tombstones),
    })
}

/// Collect all point-key versions from the given input SST files.
pub fn collect_versions(
    sst_factory: &dyn SstFactory,
    input_files: &[String],
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<Vec<CompactionVersion>> {
    Ok(collect_compaction_input(sst_factory, input_files, abort_check)?.versions)
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
pub fn deduplicate_versions(versions: &[CompactionVersion]) -> Vec<CompactionVersion> {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    // Map: key -> newest visible version (by sequence).
    let mut newest_by_key: BTreeMap<Vec<u8>, CompactionVersion> = BTreeMap::new();

    for version in versions {
        // Skip expired entries
        if is_expired(version, now_secs) {
            continue;
        }

        let key = &version.key;

        match newest_by_key.get(key) {
            None => {
                // First observation of this key.
                newest_by_key.insert(key.clone(), version.clone());
            }
            Some(existing) => {
                // Keep the one with the higher sequence number.
                if version.seq > existing.seq {
                    newest_by_key.insert(key.clone(), version.clone());
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
pub fn filter_tombstones(versions: &[CompactionVersion]) -> Vec<CompactionVersion> {
    filter_tombstones_with_horizon(versions, None)
}

/// Filter tombstones but preserve those newer than `snapshot_horizon` (if provided).
///
/// Semantics:
///   - If `snapshot_horizon` is `None`, all tombstones are dropped (legacy behavior).
///   - If `Some(h)`, tombstones with `seq > h` are preserved to prevent resurrection
///     for snapshots reading at sequence `h` or earlier.
pub fn filter_tombstones_with_horizon(
    versions: &[CompactionVersion],
    snapshot_horizon: Option<u64>,
) -> Vec<CompactionVersion> {
    match snapshot_horizon {
        Some(h) => versions
            .iter()
            .filter(|v| !(v.is_tombstone && v.seq <= h)) // drop tombstones older-or-equal to horizon
            .cloned()
            .collect(),
        None => versions
            .iter()
            .filter(|v| !v.is_tombstone)
            .cloned()
            .collect(),
    }
}

/// Write versions to a new SST using the provided `SstFactory`.
///
/// Semantics:
///   - Non-tombstone entries become "Put" records.
///   - Tombstone entries become "Delete" records.
///   - TTL is preserved in the metadata.
///   - Sequence numbers are written as provided (no rewriting here).
///
/// The writer implementation is responsible for:
///   - Block construction (e.g. TLV encoding).
///   - Compression.
///   - Checksums.
///   - Index / fence pointer emission.
pub fn write_versions_to_sst(
    sst_factory: &dyn SstFactory,
    output_filename: &str,
    versions: &[CompactionVersion],
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<()> {
    write_compaction_output_to_sst(sst_factory, output_filename, versions, &[], abort_check)
}

pub fn write_compaction_output_to_sst(
    sst_factory: &dyn SstFactory,
    output_filename: &str,
    versions: &[CompactionVersion],
    range_tombstones: &[RangeTombstone],
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<()> {
    let mut writer = sst_factory.create()?;

    let mut added: usize = 0;
    let add_start = std::time::Instant::now();
    for (i, version) in versions.iter().enumerate() {
        // Periodically check if we should abort (every 1024 entries)
        if i % 1024 == 0 {
            if let Some(check) = abort_check {
                if check() {
                    tracing::info!(output = %output_filename, "compaction aborting during write due to ingest epoch change at {} entries", i);
                    return Err(crate::common::MidgeError::Internal(
                        "compaction aborted due to ingest epoch change".to_string(),
                    ));
                }
            }
        }

        let op_type = if version.is_tombstone { 2u8 } else { 0u8 };

        writer.add_with_meta(
            &version.key,
            // `add_with_meta` expects `Option<&[u8]>` for value; we pass through.
            version.value.as_deref(),
            version.seq,
            op_type,
            version.expiration,
        )?;
        added += 1;
    }

    for (i, tombstone) in range_tombstones.iter().enumerate() {
        if i % 1024 == 0 {
            if let Some(check) = abort_check {
                if check() {
                    tracing::info!(output = %output_filename, "compaction aborting during range tombstone write due to ingest epoch change at {} tombstones", i);
                    return Err(crate::common::MidgeError::Internal(
                        "compaction aborted due to ingest epoch change".to_string(),
                    ));
                }
            }
        }

        writer.add_range_tombstone(&tombstone.start, &tombstone.end, tombstone.seq)?;
    }
    let add_ns = add_start.elapsed().as_nanos();

    let path = Path::new(output_filename);
    let finish_start = std::time::Instant::now();
    crate::sst::fs::finish_writer_to_path(writer, path)?;
    let finish_ns = finish_start.elapsed().as_nanos();

    tracing::info!(
        output = %output_filename,
        versions = added,
        add_ns = add_ns,
        finish_ns = finish_ns,
        "compaction write breakdown"
    );

    Ok(())
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

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
    fn should_detect_expired_version_when_past_expiration() {
        let now = 1_000_000u64;
        let v = mk_version("k", 1, false, Some("v"), Some(now - 1));
        assert!(is_expired(&v, now));
    }

    #[test]
    fn should_not_expire_version_when_future_or_none() {
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
    fn should_collect_versions_when_stateful_reader_exposes_range_tombstones() {
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
        use crate::compaction::merge::{MergeEntry, MergeIterator};
        use bytes::Bytes;

        // Arrange: Create two input iterators (simulating SST readers)
        let stream1 = vec![
            MergeEntry {
                key: Bytes::from(b"a".to_vec()),
                value: Bytes::from(b"a3".to_vec()),
                seq: 3,
            },
            MergeEntry {
                key: Bytes::from(b"a".to_vec()),
                value: Bytes::from(b"a1".to_vec()),
                seq: 1,
            },
            MergeEntry {
                key: Bytes::from(b"c".to_vec()),
                value: Bytes::from(b"c2".to_vec()),
                seq: 2,
            },
        ];

        let stream2 = vec![
            MergeEntry {
                key: Bytes::from(b"a".to_vec()),
                value: Bytes::from(b"a2".to_vec()),
                seq: 2,
            },
            MergeEntry {
                key: Bytes::from(b"b".to_vec()),
                value: Bytes::from(b"b5".to_vec()),
                seq: 5,
            },
        ];

        // Act: Create merge iterator from both streams
        let merge_iter =
            MergeIterator::from_iterators(vec![stream1.into_iter(), stream2.into_iter()]);

        // Convert MergeEntry to CompactionVersion
        let version_iter = merge_iter.map(|entry| CompactionVersion {
            key: entry.key.to_vec(),
            seq: entry.seq,
            is_tombstone: false,
            value: Some(entry.value.to_vec()),
            expiration: None,
        });

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
    fn should_not_drop_recent_tombstones_when_snapshot_horizon_exists() {
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
    fn should_handle_empty_streams_in_merge() {
        use crate::compaction::merge::{MergeEntry, MergeIterator};
        use bytes::Bytes;

        // Arrange
        let stream1: Vec<MergeEntry> = vec![];
        let stream2 = vec![MergeEntry {
            key: Bytes::from(b"k".to_vec()),
            value: Bytes::from(b"v".to_vec()),
            seq: 10,
        }];

        // Act
        let merge_iter =
            MergeIterator::from_iterators(vec![stream1.into_iter(), stream2.into_iter()]);

        let version_iter = merge_iter.map(|entry| CompactionVersion {
            key: entry.key.to_vec(),
            seq: entry.seq,
            is_tombstone: false,
            value: Some(entry.value.to_vec()),
            expiration: None,
        });

        let dedup_iter = StreamDeduplicate::new(version_iter);
        let result: Vec<_> = dedup_iter.collect();

        // Assert

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"k".to_vec());
        assert_eq!(result[0].seq, 10);
    }

    #[test]
    fn should_deduplicate_correctly_across_streams_with_overlapping_keys() {
        use crate::compaction::merge::{MergeEntry, MergeIterator};
        use bytes::Bytes;

        // Arrange
        // Stream 1: a(5), a(3), b(4)
        let stream1 = vec![
            MergeEntry {
                key: Bytes::from(b"a".to_vec()),
                value: Bytes::from(b"a5".to_vec()),
                seq: 5,
            },
            MergeEntry {
                key: Bytes::from(b"a".to_vec()),
                value: Bytes::from(b"a3".to_vec()),
                seq: 3,
            },
            MergeEntry {
                key: Bytes::from(b"b".to_vec()),
                value: Bytes::from(b"b4".to_vec()),
                seq: 4,
            },
        ];

        // Stream 2: a(4), b(6), c(2)
        let stream2 = vec![
            MergeEntry {
                key: Bytes::from(b"a".to_vec()),
                value: Bytes::from(b"a4".to_vec()),
                seq: 4,
            },
            MergeEntry {
                key: Bytes::from(b"b".to_vec()),
                value: Bytes::from(b"b6".to_vec()),
                seq: 6,
            },
            MergeEntry {
                key: Bytes::from(b"c".to_vec()),
                value: Bytes::from(b"c2".to_vec()),
                seq: 2,
            },
        ];

        let merge_iter =
            MergeIterator::from_iterators(vec![stream1.into_iter(), stream2.into_iter()]);

        let version_iter = merge_iter.map(|entry| CompactionVersion {
            key: entry.key.to_vec(),
            seq: entry.seq,
            is_tombstone: false,
            value: Some(entry.value.to_vec()),
            expiration: None,
        });

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
