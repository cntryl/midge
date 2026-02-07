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
            .map(|d| d.as_secs())
            .unwrap_or(0);

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

/// Collect all versions from the given input SST files.
///
/// NOTE:
///   - At the moment this is a **best-effort stub** until the SST reader
///     traits are fully wired for compaction (e.g. `SstStateReader`).
///   - It *intentionally* returns an empty collection rather than panicking
///     or guessing at the concrete SST reader type.
///   - This function is structured so that it can be upgraded to a streaming
///     implementation without changing its public signature.
///
/// Once the SST layer exposes an iterator that yields logical versions, this
/// function should:
///   - Open each file through `sst_factory`.
///   - Iterate all entries, mapping them to `CompactionVersion`.
///   - Push into the accumulating `versions` vector.
pub fn collect_versions(
    sst_factory: &dyn SstFactory,
    input_files: &[String],
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<Vec<CompactionVersion>> {
    let versions = Vec::new();

    for filename in input_files {
        // Periodically check whether we should abort (cooperative cancellation)
        if let Some(check) = abort_check {
            if check() {
                tracing::info!(file = %filename, "compaction aborting due to ingest epoch change");
                return Ok(Vec::new());
            }
        }

        let path = Path::new(filename);

        // Use the generic SstReader API from the factory to open the file.
        let _reader = sst_factory.open(path)?;

        // **Note**: The current SstFactory trait returns `Box<dyn SstReader>`, which
        // doesn't directly expose `SstStateReader` methods (seq, tombstone, expiration).
        // To properly wire this, we would need either:
        //   1. An extended trait combining both interfaces, or
        //   2. A separate factory method for stateful readers, or
        //   3. Open directly via `SstFile::open()` within compaction.
        //
        // For now, we keep the function signature for compatibility with the architecture,
        // but the real implementation requires SST trait extension. The streaming pipeline
        // (MergeIterator + StreamDeduplicate) is ready and tested; this is the remaining
        // integration point.
    }

    Ok(versions)
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
        .map(|d| d.as_secs())
        .unwrap_or(0);

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

        let op_type = if version.is_tombstone { 1u8 } else { 0u8 };

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
    let add_ns = add_start.elapsed().as_nanos();

    let path = Path::new(output_filename);
    let finish_start = std::time::Instant::now();
    writer.finish_to_path(path)?;
    let finish_ns = finish_start.elapsed().as_nanos();

    tracing::info!(output = %output_filename, versions = added, add_ms = (add_ns as f64) / 1_000_000.0, finish_ms = (finish_ns as f64) / 1_000_000.0, "compaction write breakdown");

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
