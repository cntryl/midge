//! Compaction execution implementation.
//!
//! This module contains the low-level machinery for executing compaction operations:
//! - Collecting versions from multiple SST files
//! - Filtering tombstones based on snapshot visibility
//! - Applying compaction filters
//! - Writing compacted SST files
//!
//! The high-level compaction strategy (when to compact, which files to select)
//! is handled by the parent `compaction` module.

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::MidgeResult;
use crate::sst::traits::RangeTombstone as SstRangeTombstone;

// ============================================================================
// Types
// ============================================================================

/// A single version of a key collected during compaction.
///
/// Multiple versions may exist for the same user key at different sequence numbers.
/// During compaction, these are merged according to the LSM merge semantics
/// (newer versions shadow older ones).
#[derive(Debug, Clone)]
pub struct CompactionVersion {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub tombstone: bool,
    pub value: Option<Bytes>,
    pub expiration: Option<u64>, // TTL: Unix milliseconds when key expires
}

/// A range tombstone participating in a compaction run.
#[derive(Debug, Clone)]
pub struct CompactionRangeTombstone {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: u64,
}

// ============================================================================
// Version Collection
// ============================================================================

/// Collect all versions of all keys from a set of SST files.
///
/// This scans each SST file and extracts (user_key, seq, value/tombstone) tuples.
/// The same key may appear multiple times with different sequence numbers.
///
/// # Arguments
/// * `reader_factory` - Factory for opening SST readers
/// * `sst_dir` - Directory containing SST files
/// * `sst_names` - List of SST filenames to compact
///
/// # Returns
/// Vector of all key versions found, in no particular order. Caller should sort
/// before writing to maintain key ordering invariants.
pub(crate) fn collect_compaction_versions(
    reader_factory: &Arc<dyn crate::sst::SstReaderFactory>,
    sst_dir: &std::path::Path,
    sst_names: &[String],
) -> Vec<CompactionVersion> {
    let mut versions = Vec::new();
    let mut seen: HashSet<(Vec<u8>, u64, bool)> = HashSet::new();

    // Process files in the order given by the compaction strategy
    // For L0, this is newest first (higher sequence numbers)
    for name in sst_names.iter() {
        let path = sst_dir.join(name);
        if !path.exists() {
            tracing::warn!(file = %name, "SST file not found during collection");
            continue;
        }
        let Ok(sst) = reader_factory.open(&path) else {
            tracing::warn!(file = %name, "Failed to open SST during collection");
            continue;
        };
        let Ok(rows) = sst.scan_range_state(None, None) else {
            tracing::warn!(file = %name, "Failed to scan SST during collection");
            continue;
        };

        let mut file_versions = Vec::new();
        for (raw_key, state) in rows {
            // The key returned by scan_range_state is now a user key (after SST encoding fix)
            // The sequence number is in the KeyState, not in the key itself
            let user_key = raw_key.to_vec();
            let (seq, tombstone, value, expiration) = match state {
                crate::sst::KeyState::Value(v, seq, exp) => (seq, false, Some(v), exp),
                crate::sst::KeyState::Tombstone(seq) => (seq, true, None, None),
                crate::sst::KeyState::Absent => continue,
            };
            if seen.insert((user_key.clone(), seq, tombstone)) {
                file_versions.push((user_key.clone(), seq));
                versions.push(CompactionVersion {
                    user_key,
                    seq,
                    tombstone,
                    value,
                    expiration,
                });
            }
        }
    }

    versions
}

/// Collect all range tombstones from a set of SST files for compaction.
pub(crate) fn collect_compaction_range_tombstones(
    reader_factory: &Arc<dyn crate::sst::SstReaderFactory>,
    sst_dir: &std::path::Path,
    sst_names: &[String],
) -> Vec<CompactionRangeTombstone> {
    let mut out = Vec::new();
    let mut seen: HashSet<(Vec<u8>, Vec<u8>, u64)> = HashSet::new();

    for name in sst_names.iter() {
        let path = sst_dir.join(name);
        if !path.exists() {
            tracing::warn!(file = %name, "SST file not found during range tombstone collection");
            continue;
        }

        let Ok(sst) = reader_factory.open(&path) else {
            tracing::warn!(file = %name, "Failed to open SST during range tombstone collection");
            continue;
        };

        let rts = sst.range_tombstones();
        for rt in rts {
            let key = (rt.start.clone(), rt.end.clone(), rt.seq);
            if seen.insert(key.clone()) {
                out.push(CompactionRangeTombstone {
                    start: key.0,
                    end: key.1,
                    seq: key.2,
                });
            }
        }
    }

    out
}

/// Sort versions by user_key (ascending), then sequence (descending), then tombstone flag.
///
/// This establishes the canonical ordering for compacted output:
/// - Keys are in ascending order
/// - For the same key, newer versions (higher seq) come first
/// - For the same key+seq, values come before tombstones
#[inline]
pub(crate) fn sort_versions_for_output(versions: &mut [CompactionVersion]) {
    versions.sort_by(|a, b| match a.user_key.cmp(&b.user_key) {
        Ordering::Equal => match b.seq.cmp(&a.seq) {
            Ordering::Equal => a.tombstone.cmp(&b.tombstone), // values (false) before tombstones (true)
            other => other,
        },
        other => other,
    });
}

/// Convert values to point tombstones when they are covered by a range tombstone
/// at or before their sequence number.
pub(crate) fn filter_versions_with_range_tombstones(
    versions: &[CompactionVersion],
    range_tombs: &[CompactionRangeTombstone],
) -> Vec<CompactionVersion> {
    if range_tombs.is_empty() {
        return versions.to_vec();
    }

    let mut rt_vec = Vec::with_capacity(range_tombs.len());
    for rt in range_tombs {
        rt_vec.push(SstRangeTombstone {
            start: rt.start.clone(),
            end: rt.end.clone(),
            seq: rt.seq,
        });
    }

    versions
        .iter()
        .cloned()
        .map(|mut v| {
            let mut max_rt_seq: Option<u64> = None;
            for rt in &rt_vec {
                if v.user_key >= rt.start && v.user_key < rt.end && v.seq <= rt.seq {
                    max_rt_seq = Some(max_rt_seq.map_or(rt.seq, |m| m.max(rt.seq)));
                }
            }
            if let Some(max_seq) = max_rt_seq {
                v.tombstone = true;
                v.value = None;
                v.seq = max_seq;
            }
            v
        })
        .collect()
}

// ============================================================================
// Version Merging & Deduplication
// ============================================================================

/// Deduplicate versions while preserving older versions visible to active snapshots.
///
/// This function keeps multiple versions of the same key when necessary to maintain
/// snapshot visibility. A version is kept if:
/// 1. It's the newest version of the key, OR
/// 2. It has a sequence number < min_snapshot_seq (visible to at least one snapshot)
///
/// Note: Snapshots see all writes with sequence < snapshot_seq (strictly less than).
/// So if min_snapshot_seq is the smallest active snapshot, any version with seq < min_snapshot_seq
/// is visible to at least that snapshot and must be preserved.
///
/// # Arguments
/// * `versions` - Versions sorted by user_key (ascending), then seq (descending)
/// * `min_snapshot_seq` - Minimum sequence number of any active snapshot, or None
///
/// # Returns
/// Deduplicated list preserving snapshot visibility
pub(crate) fn deduplicate_versions(
    versions: &[CompactionVersion],
    min_snapshot_seq: Option<u64>,
) -> Vec<CompactionVersion> {
    let mut result = Vec::new();
    let mut last_user_key: Option<&[u8]> = None;

    for v in versions {
        let current_key = v.user_key.as_slice();

        // Keep this version if:
        // 1. It's a new key (first version we see for this key), OR
        // 2. It's visible to an active snapshot (seq < min_snapshot_seq)
        let is_new_key = last_user_key != Some(current_key);
        let visible_to_snapshot = min_snapshot_seq.is_some_and(|min_seq| v.seq < min_seq);

        if is_new_key || visible_to_snapshot {
            result.push(v.clone());
            if is_new_key {
                last_user_key = Some(current_key);
            }
        }
    }
    result
}

/// Filter tombstones that are safe to garbage collect.
///
/// A tombstone can be dropped if:
/// 1. There's a newer version of the same key (tombstone is shadowed)
/// 2. The tombstone's sequence is less than the minimum active snapshot sequence
///    (no snapshot can see it)
///
/// # Arguments
/// * `versions` - All versions collected from input SSTs
/// * `min_snapshot_seq` - Minimum sequence number of any active snapshot, or None
///
/// # Returns
/// Tuple of (filtered versions, count of removed tombstones)
pub(crate) fn filter_safe_tombstones(
    versions: &[CompactionVersion],
    min_snapshot_seq: Option<u64>,
) -> (Vec<CompactionVersion>, usize) {
    // Group versions by user_key to track the newest sequence per key
    let mut newest_seq_per_key: HashMap<&[u8], u64> = HashMap::new();
    for v in versions {
        newest_seq_per_key
            .entry(v.user_key.as_slice())
            .and_modify(|seq| *seq = (*seq).max(v.seq))
            .or_insert(v.seq);
    }

    let mut result = Vec::new();
    let mut removed_count = 0;
    for v in versions {
        if v.tombstone {
            // Keep tombstone if:
            // - It's the newest version of the key, OR
            // - There's an active snapshot that might see it
            let is_newest = newest_seq_per_key.get(v.user_key.as_slice()) == Some(&v.seq);
            let visible_to_snapshot = min_snapshot_seq.is_some_and(|min_seq| v.seq >= min_seq);

            if is_newest || visible_to_snapshot {
                result.push(v.clone());
            } else {
                removed_count += 1;
            }
        } else {
            result.push(v.clone());
        }
    }
    (result, removed_count)
}

// ============================================================================
// Compaction Filtering
// ============================================================================

/// Apply compaction filter to versions, removing entries based on filter decisions.
///
/// Compaction filters allow users to implement custom logic for dropping or
/// transforming entries during compaction (e.g., TTL expiration, prefix filtering).
///
/// # Arguments
/// * `versions` - Input versions to filter
/// * `filter` - The compaction filter to apply
/// * `target_level` - Level where the compacted output will be written
///
/// # Returns
/// Filtered list of versions with filter decisions applied
pub(crate) fn apply_compaction_filter(
    versions: &[CompactionVersion],
    filter: &dyn super::filter::CompactionFilter,
    target_level: u32,
) -> Vec<CompactionVersion> {
    use super::filter::FilterDecision;

    let mut result = Vec::new();
    for v in versions {
        let decision = filter.filter(target_level, v);

        match decision {
            FilterDecision::Keep => {
                result.push(v.clone());
            }
            FilterDecision::Remove => {
                // Drop this entry entirely
            }
            FilterDecision::RemoveAndTombstone => {
                // Convert to tombstone
                result.push(CompactionVersion {
                    user_key: v.user_key.clone(),
                    seq: v.seq,
                    tombstone: true,
                    value: None,
                    expiration: v.expiration,
                });
            }
        }
    }
    result
}

// ============================================================================
// SST Writing
// ============================================================================

/// Shared dependencies and configuration for SST writing operations.
/// These are typically set once and reused across multiple writes.
pub(crate) struct SstWriterContext<'a> {
    pub sst_factory: &'a Arc<dyn crate::sst::SstFactory>,
    pub compression: crate::common::codec::CompressionType,
    pub block_size: usize,
    pub sst_dir: &'a std::path::Path,
    pub cloud_sst_manager: Option<&'a Arc<crate::sst::cloud::CloudSstManager>>,
}

/// Write a compacted SST file from the given versions.
///
/// This creates a new SST file on disk with all the provided versions in sorted order.
/// The SST metadata (smallest/largest key, sequence range) is computed from the input.
///
/// # Arguments
/// * `ctx` - Shared SST writer context (factories, directories, settings)
/// * `versions` - Sorted list of key versions to write
/// * `cf_id` - Column family ID for this SST
///
/// # Returns
/// * `Ok(Some((path, metadata)))` - Path to the created SST and its metadata
/// * `Ok(None)` - No SST created (empty input)
/// * `Err(_)` - I/O or encoding error
pub(crate) fn write_compacted_sst(
    ctx: &SstWriterContext,
    versions: &[CompactionVersion],
    cf_id: u32,
) -> MidgeResult<Option<(PathBuf, crate::manifest::FileMeta)>> {
    let SstWriterContext {
        sst_factory,
        compression,
        block_size,
        sst_dir,
        cloud_sst_manager,
    } = ctx;
    if versions.is_empty() {
        return Ok(None);
    }

    // Note: versions may contain multiple entries for the same user_key when snapshots are active.
    // This is correct behavior - we preserve different versions for snapshot visibility.
    // The SST format with internal keys (user_key || seq || kind) handles this correctly.

    let mut writer = sst_factory.create(*compression, *block_size, true);
    let mut smallest_key: Option<Vec<u8>> = None;
    let mut largest_key: Option<Vec<u8>> = None;
    let mut smallest_seq: Option<u64> = None;
    let mut largest_seq: Option<u64> = None;

    for entry in versions {
        if smallest_key.is_none() {
            smallest_key = Some(entry.user_key.clone());
        }
        largest_key = Some(entry.user_key.clone());
        smallest_seq = Some(match smallest_seq {
            Some(s) => s.min(entry.seq),
            None => entry.seq,
        });
        largest_seq = Some(match largest_seq {
            Some(s) => s.max(entry.seq),
            None => entry.seq,
        });

        // When use_internal=true, the writer expects keys to be pre-encoded as internal keys
        let internal_key = crate::common::internal_key::encode_internal_key(
            &entry.user_key,
            entry.seq,
            entry.tombstone,
        );

        if entry.tombstone {
            writer.add_with_meta(&internal_key, None, entry.seq, true, entry.expiration)?;
        } else if let Some(value) = &entry.value {
            writer.add_with_meta(
                &internal_key,
                Some(value.as_ref()),
                entry.seq,
                false,
                entry.expiration,
            )?;
        }
    }

    // Persist SST using writer.finish_to_path so that filesystem-backed
    // writers (FsDynWriter) can perform atomic rename + fsync operations.
    // This ensures the SST is fully durable before manifest updates.
    let id = uuid::Uuid::new_v4().to_string();
    let file_path = sst_dir.join(format!("{}.sst", id));
    writer.finish_to_path(&file_path)?;

    // Calculate tombstone counts
    let point_tombstone_count = versions.iter().filter(|v| v.tombstone).count() as u64;
    let range_tombstone_count = 0; // Range tombstones currently not written by compaction
    let total_entries = versions.len() as u64;

    let size_bytes = std::fs::metadata(&file_path)
        .map(|md| md.len())
        .unwrap_or(0);
    let name = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    let meta = crate::manifest::FileMeta {
        name: name.clone(),
        level: 0,
        size_bytes,
        cf_id,
        smallest_key,
        largest_key,
        smallest_seq,
        largest_seq,
        sublevel: 0, // Will be assigned when added to manifest
        cloud_location: None,
        cloud_checksum: None,
        cloud_uploaded_at: None,
        cloud_state: None,
        point_tombstone_count,
        range_tombstone_count,
        total_entries,
    };

    // Upload SST to cloud if cloud manager is configured
    if let Some(cloud_manager) = cloud_sst_manager {
        let sst_id = id.clone();
        let sequence_range = (smallest_seq.unwrap_or(0), largest_seq.unwrap_or(0));
        let key_range = (
            meta.smallest_key.clone().map(|k| k.to_vec()),
            meta.largest_key.clone().map(|k| k.to_vec()),
        );

        // Spawn async upload (non-blocking)
        let cloud_manager_clone = Arc::clone(cloud_manager);
        let file_path_clone = file_path.clone();
        let sst_id_clone = sst_id.clone();

        std::thread::spawn(move || {
            if let Err(e) = cloud_manager_clone.upload_sst_async(
                sst_id_clone,
                file_path_clone,
                sequence_range,
                key_range,
                None,
            ) {
                tracing::error!("Failed to upload compacted SST to cloud: {}", e);
            }
        });
    }

    Ok(Some((file_path, meta)))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Test helpers
    fn make_version(key: &[u8], seq: u64, tombstone: bool) -> CompactionVersion {
        CompactionVersion {
            user_key: key.to_vec(),
            seq,
            tombstone,
            value: if tombstone {
                None
            } else {
                Some(Bytes::from(format!("value_{}", seq)))
            },
            expiration: None,
        }
    }

    fn make_tombstone(key: &[u8], seq: u64) -> CompactionVersion {
        CompactionVersion {
            user_key: key.to_vec(),
            seq,
            tombstone: true,
            value: None,
            expiration: None,
        }
    }

    // ========================================================================
    // Sorting Tests
    // ========================================================================

    #[test]
    fn should_sort_by_user_key_ascending_when_sorting() {
        // Arrange
        let mut versions = vec![
            make_version(b"key3", 100, false),
            make_version(b"key1", 100, false),
            make_version(b"key2", 100, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].user_key, b"key1");
        assert_eq!(versions[1].user_key, b"key2");
        assert_eq!(versions[2].user_key, b"key3");
    }

    #[test]
    fn should_sort_by_seq_descending_given_same_key_when_sorting() {
        // Arrange
        let mut versions = vec![
            make_version(b"key1", 50, false),
            make_version(b"key1", 200, false),
            make_version(b"key1", 100, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].seq, 200);
        assert_eq!(versions[1].seq, 100);
        assert_eq!(versions[2].seq, 50);
    }

    #[test]
    fn should_sort_values_before_tombstones_given_same_key_and_seq() {
        // Arrange
        let mut versions = vec![
            make_tombstone(b"key1", 100),
            make_version(b"key1", 100, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert!(!versions[0].tombstone); // Value first
        assert!(versions[1].tombstone); // Tombstone second
    }

    // ========================================================================
    // Deduplication with Snapshot Tests
    // ========================================================================

    #[test]
    fn should_preserve_old_value_visible_to_snapshot_when_deduplicating() {
        // Arrange
        let mut versions = vec![
            make_tombstone(b"key1", 200), // Newest: tombstone (not visible to snapshot)
            make_version(b"key1", 100, false), // Older: value (visible to snapshot at seq=150)
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions, Some(150));

        // Assert
        assert_eq!(
            result.len(),
            2,
            "Should keep both tombstone and old value visible to snapshot"
        );
        assert_eq!(result[0].seq, 200);
        assert!(result[0].tombstone);
        assert_eq!(result[1].seq, 100);
        assert!(!result[1].tombstone);
    }

    #[test]
    fn should_discard_old_value_not_visible_to_snapshot_when_deduplicating() {
        // Arrange
        let mut versions = vec![
            make_tombstone(b"key1", 200),     // Newest: tombstone
            make_version(b"key1", 50, false), // Older: value (NOT visible to snapshot at seq=25)
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions, Some(25));

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].seq, 200);
        assert!(result[0].tombstone);
    }

    // ========================================================================
    // Tombstone Filtering Tests
    // ========================================================================

    #[test]
    fn should_keep_all_versions_given_no_tombstones_when_filtering() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, None);

        // Assert
        assert_eq!(result.len(), 2);
        assert_eq!(removed, 0);
    }

    #[test]
    fn should_remove_old_tombstone_given_newer_value_exists() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 200, false), // Newest: value
            make_tombstone(b"key1", 100),      // Older: tombstone (can be dropped)
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, None);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(removed, 1);
        assert!(!result[0].tombstone); // Only the value remains
    }

    #[test]
    fn should_keep_tombstone_visible_to_snapshot() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 200, false), // Newest: value
            make_tombstone(b"key1", 150),      // Older: tombstone but visible to snapshot
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, Some(150));

        // Assert
        assert_eq!(result.len(), 2);
        assert_eq!(removed, 0); // Tombstone kept because snapshot can see it
    }

    #[test]
    fn should_remove_tombstone_not_visible_to_snapshot() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 200, false), // Newest: value
            make_tombstone(b"key1", 100),      // Older: tombstone not visible to snapshot
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, Some(150));

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(removed, 1); // Tombstone dropped because seq < min_snapshot_seq
    }

    #[test]
    fn should_remove_multiple_old_tombstones_for_same_key() {
        // Arrange
        let versions = vec![
            make_tombstone(b"key1", 300), // Newest tombstone
            make_tombstone(b"key1", 200), // Old tombstone 1
            make_tombstone(b"key1", 100), // Old tombstone 2
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, None);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(removed, 2);
        assert_eq!(result[0].seq, 300);
    }

    #[test]
    fn should_handle_snapshot_seq_equals_tombstone_seq_edge_case() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 200, false),
            make_tombstone(b"key1", 100), // Exactly at snapshot boundary
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, Some(100));

        // Assert
        assert_eq!(result.len(), 2);
        assert_eq!(removed, 0);
    }

    // ========================================================================
    // Compaction Filter Tests
    // ========================================================================

    struct KeepAllFilter;
    impl crate::core::compaction::filter::CompactionFilter for KeepAllFilter {
        fn filter(
            &self,
            _level: u32,
            _version: &CompactionVersion,
        ) -> crate::core::compaction::filter::FilterDecision {
            crate::core::compaction::filter::FilterDecision::Keep
        }
    }

    struct RemoveAllFilter;
    impl crate::core::compaction::filter::CompactionFilter for RemoveAllFilter {
        fn filter(
            &self,
            _level: u32,
            _version: &CompactionVersion,
        ) -> crate::core::compaction::filter::FilterDecision {
            crate::core::compaction::filter::FilterDecision::Remove
        }
    }

    struct TombstoneAllFilter;
    impl crate::core::compaction::filter::CompactionFilter for TombstoneAllFilter {
        fn filter(
            &self,
            _level: u32,
            _version: &CompactionVersion,
        ) -> crate::core::compaction::filter::FilterDecision {
            crate::core::compaction::filter::FilterDecision::RemoveAndTombstone
        }
    }

    #[test]
    fn should_keep_all_entries_given_keep_filter() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];
        let filter = KeepAllFilter;

        // Act
        let result = apply_compaction_filter(&versions, &filter, 1);

        // Assert
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].user_key, b"key1");
        assert_eq!(result[1].user_key, b"key2");
    }

    #[test]
    fn should_remove_all_entries_given_remove_filter() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];
        let filter = RemoveAllFilter;

        // Act
        let result = apply_compaction_filter(&versions, &filter, 1);

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_convert_to_tombstones_given_tombstone_filter() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];
        let filter = TombstoneAllFilter;

        // Act
        let result = apply_compaction_filter(&versions, &filter, 1);

        // Assert
        assert_eq!(result.len(), 2);
        assert!(result[0].tombstone);
        assert!(result[1].tombstone);
        assert_eq!(result[0].user_key, b"key1");
        assert_eq!(result[1].user_key, b"key2");
    }

    // ========================================================================
    // SST Writing Tests
    // ========================================================================

    fn create_test_context<'a>(
        sst_factory: &'a Arc<dyn crate::sst::SstFactory>,
        sst_dir: &'a std::path::Path,
    ) -> SstWriterContext<'a> {
        SstWriterContext {
            sst_factory,
            compression: crate::common::codec::CompressionType::None,
            block_size: 4096,
            sst_dir,
            cloud_sst_manager: None,
        }
    }

    #[test]
    fn should_return_none_given_empty_versions_when_writing_sst() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> = Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_test_context(&sst_factory, temp_dir.path());
        let versions: Vec<CompactionVersion> = vec![];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn should_write_single_version_correctly() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> = Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_test_context(&sst_factory, temp_dir.path());
        let versions = vec![make_version(b"test_key", 100, false)];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (path, meta) = result.unwrap().unwrap();
        assert!(path.exists());
        assert_eq!(meta.smallest_key, Some(b"test_key".to_vec()));
        assert_eq!(meta.largest_key, Some(b"test_key".to_vec()));
        assert_eq!(meta.smallest_seq, Some(100));
        assert_eq!(meta.largest_seq, Some(100));
        assert_eq!(meta.total_entries, 1);
    }

    #[test]
    fn should_count_tombstones_correctly_in_metadata() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> = Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_test_context(&sst_factory, temp_dir.path());
        let versions = vec![
            make_version(b"key1", 100, false),
            make_tombstone(b"key2", 200),
            make_tombstone(b"key3", 150),
        ];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (_path, meta) = result.unwrap().unwrap();
        assert_eq!(meta.point_tombstone_count, 2);
        assert_eq!(meta.total_entries, 3);
    }

    // NOTE: Removed test should_fail_given_duplicate_keys_when_writing_sst
    // Duplicate user keys are intentionally allowed for snapshot-aware compaction.
    // The SST format with internal keys (user_key||seq||kind) ensures uniqueness.

    // ============================================================================
    // Unit Tests for Version Merging Logic
    // ============================================================================

    #[test]
    fn should_sort_versions_with_newest_first() {
        // Arrange
        let mut versions = vec![
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 1,
                tombstone: false,
                value: Some(Bytes::from_static(b"v1")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 10,
                tombstone: false,
                value: Some(Bytes::from_static(b"v10")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 5,
                tombstone: false,
                value: Some(Bytes::from_static(b"v5")),
                expiration: None,
            },
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].seq, 10);
        assert_eq!(versions[1].seq, 5);
        assert_eq!(versions[2].seq, 1);
    }

    #[test]
    fn should_deduplicate_keeping_only_newest_version_when_no_snapshots() {
        // Arrange
        let versions = vec![
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 10,
                tombstone: false,
                value: Some(Bytes::from_static(b"v10")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 5,
                tombstone: false,
                value: Some(Bytes::from_static(b"v5")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 1,
                tombstone: false,
                value: Some(Bytes::from_static(b"v1")),
                expiration: None,
            },
        ];

        // Act
        let result = deduplicate_versions(&versions, None);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].seq, 10);
        assert_eq!(result[0].value, Some(Bytes::from_static(b"v10")));
    }

    #[test]
    fn should_preserve_versions_visible_to_snapshots() {
        // Arrange
        let versions = vec![
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 10,
                tombstone: false,
                value: Some(Bytes::from_static(b"v10")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 5,
                tombstone: false,
                value: Some(Bytes::from_static(b"v5")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key1".to_vec(),
                seq: 1,
                tombstone: false,
                value: Some(Bytes::from_static(b"v1")),
                expiration: None,
            },
        ];

        // Act
        let result = deduplicate_versions(&versions, Some(7));

        // Assert
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].seq, 10);
        assert_eq!(result[1].seq, 5);
        assert_eq!(result[2].seq, 1);
    }

    #[test]
    fn should_handle_multiple_keys_with_different_sequences() {
        // Arrange
        let mut versions = vec![
            CompactionVersion {
                user_key: b"key_a".to_vec(),
                seq: 3,
                tombstone: false,
                value: Some(Bytes::from_static(b"a3")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key_b".to_vec(),
                seq: 8,
                tombstone: false,
                value: Some(Bytes::from_static(b"b8")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key_a".to_vec(),
                seq: 1,
                tombstone: false,
                value: Some(Bytes::from_static(b"a1")),
                expiration: None,
            },
            CompactionVersion {
                user_key: b"key_b".to_vec(),
                seq: 2,
                tombstone: false,
                value: Some(Bytes::from_static(b"b2")),
                expiration: None,
            },
        ];

        // Act
        sort_versions_for_output(&mut versions);
        let result = deduplicate_versions(&versions, None);

        // Assert
        assert_eq!(result.len(), 2);
        // key_a comes first (alphabetically)
        assert_eq!(result[0].user_key, b"key_a");
        assert_eq!(result[0].seq, 3);
        // key_b comes second
        assert_eq!(result[1].user_key, b"key_b");
        assert_eq!(result[1].seq, 8);
    }
}
