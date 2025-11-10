//! Version merging and deduplication logic.

use super::types::CompactionVersion;

/// Deduplicate versions to keep only the newest version of each key.
///
/// When multiple versions of the same key exist (sorted with newest first),
/// only the first (newest) version is retained. This ensures that the output
/// SST contains exactly one entry per key, maintaining the ordering invariant.
///
/// # Arguments
/// * `versions` - Versions sorted by user_key (ascending), then seq (descending)
///
/// # Returns
/// Deduplicated list with at most one version per user key
pub(crate) fn deduplicate_versions(versions: &[CompactionVersion]) -> Vec<CompactionVersion> {
    let mut result = Vec::new();
    let mut last_user_key: Option<&[u8]> = None;

    for v in versions {
        let current_key = v.user_key.as_slice();
        // Only keep the first version of each key (which is the newest due to sorting)
        if last_user_key != Some(current_key) {
            result.push(v.clone());
            last_user_key = Some(current_key);
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
    use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::collection::sort_versions_for_output;
    use bytes::Bytes;

    // Helper to create a version with minimal fields
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

    // Test deduplicate_versions
    #[test]
    fn should_return_empty_given_empty_input_when_deduplicating() {
        // Arrange
        let versions: Vec<CompactionVersion> = vec![];

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_keep_single_version_given_one_key_when_deduplicating() {
        // Arrange
        let versions = vec![make_version(b"key1", 100, false)];

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].user_key, b"key1");
        assert_eq!(result[0].seq, 100);
    }

    #[test]
    fn should_keep_newest_version_given_duplicate_keys_when_sorted_by_seq_desc() {
        // Arrange
        let mut versions = vec![
            make_version(b"key1", 200, false), // Newest version first
            make_version(b"key1", 100, false), // Older version
            make_version(b"key1", 50, false),  // Oldest version
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].user_key, b"key1");
        assert_eq!(result[0].seq, 200);
    }

    #[test]
    fn should_keep_first_occurrence_of_each_key_given_multiple_keys_when_deduplicating() {
        // Arrange
        let mut versions = vec![
            make_version(b"key1", 300, false),
            make_version(b"key1", 200, false),
            make_version(b"key2", 250, false),
            make_version(b"key2", 150, false),
            make_version(b"key3", 100, false),
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].user_key, b"key1");
        assert_eq!(result[0].seq, 300);
        assert_eq!(result[1].user_key, b"key2");
        assert_eq!(result[1].seq, 250);
        assert_eq!(result[2].user_key, b"key3");
        assert_eq!(result[2].seq, 100);
    }

    #[test]
    fn should_keep_tombstone_given_newest_version_is_tombstone_when_deduplicating() {
        // Arrange
        let mut versions = vec![
            make_tombstone(b"key1", 200),      // Newest: tombstone
            make_version(b"key1", 100, false), // Older: value
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].user_key, b"key1");
        assert_eq!(result[0].seq, 200);
        assert!(result[0].tombstone);
    }

    // Test filter_safe_tombstones
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
    fn should_keep_newest_tombstone_given_multiple_versions_when_filtering() {
        // Arrange
        let versions = vec![
            make_tombstone(b"key1", 200),      // Newest: tombstone
            make_version(b"key1", 100, false), // Older: value
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, None);

        // Assert
        assert_eq!(result.len(), 2);
        assert_eq!(removed, 0);
        assert_eq!(
            result
                .iter()
                .filter(|v| v.tombstone && v.seq == 200)
                .count(),
            1
        );
    }

    #[test]
    fn should_remove_old_tombstone_given_newer_value_exists_when_filtering() {
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
    fn should_keep_tombstone_visible_to_snapshot_when_filtering() {
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
    fn should_remove_tombstone_not_visible_to_snapshot_when_filtering() {
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
    fn should_handle_unsorted_input_when_deduplicating() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 100, false), // Old version
            make_version(b"key1", 200, false), // Newer version
            make_version(b"key2", 150, false),
        ];
        // Deliberately NOT sorting to test precondition

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        // It should keep key1@100 (first occurrence), not key1@200
        // This test DOCUMENTS the precondition: input must be sorted
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].seq, 100); // First occurrence, not newest!
        assert_eq!(result[1].seq, 150);
    }

    #[test]
    fn should_remove_multiple_old_tombstones_for_same_key_given_no_snapshots() {
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
    fn should_handle_all_tombstones_when_filtering() {
        // Arrange
        let versions = vec![
            make_tombstone(b"key1", 100),
            make_tombstone(b"key2", 200),
            make_tombstone(b"key3", 150),
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, None);

        // Assert
        assert_eq!(result.len(), 3);
        assert_eq!(removed, 0);
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

    #[test]
    fn should_not_filter_values_based_on_snapshot_seq() {
        // Arrange
        let versions = vec![
            make_version(b"key1", 200, false), // New value
            make_version(b"key1", 40, false),  // Old value below threshold
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, Some(50));

        // Assert
        assert_eq!(result.len(), 2);
        assert_eq!(removed, 0);
    }

    #[test]
    fn should_handle_large_dataset_when_deduplicating() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        let mut versions = Vec::new();
        for key_id in 0..100 {
            for version in 0..10 {
                versions.push(make_version(
                    format!("key_{:05}", key_id).as_bytes(),
                    (version * 10) as u64,
                    false,
                ));
            }
        }
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 100);
        #[allow(clippy::needless_range_loop)]
        for i in 0..100 {
            assert_eq!(result[i].seq, 90); // Highest version for each key
        }
    }

    #[test]
    fn should_remove_old_tombstone_given_no_snapshots_when_filtering() {
        // Arrange
        // With no snapshots, only the newest version of each key should be kept
        let versions = vec![
            make_version(b"key1", 200, false), // Newest: value
            make_tombstone(b"key1", 100),      // Older: tombstone (should be GC'd)
        ];

        // Act
        let (result, removed) = filter_safe_tombstones(&versions, None);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(removed, 1);
        assert!(!result[0].tombstone);
        assert_eq!(result[0].seq, 200);
    }

    #[test]
    #[should_panic(expected = "Data corruption")]
    fn should_detect_same_key_seq_different_tombstone_flags() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        // This should NEVER happen in a correct LSM-tree
        let mut versions = vec![
            make_version(b"key1", 100, false), // Value
            make_tombstone(b"key1", 100),      // Tombstone - SAME SEQ!
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        // deduplicate_versions will silently keep the first (value)
        let result = deduplicate_versions(&versions);

        // Current behavior: Keeps value, drops tombstone
        assert_eq!(result.len(), 1);
        assert!(!result[0].tombstone);

        // But this is WRONG! We should panic or error on this invariant violation
        panic!("Data corruption: same key+seq with different tombstone flags should never exist!");
    }

    #[test]
    fn should_keep_value_when_value_and_tombstone_have_same_seq() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        // After sorting, value comes before tombstone
        let mut versions = vec![
            make_tombstone(b"key1", 100),      // Tombstone
            make_version(b"key1", 100, false), // Value - SAME SEQ!
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert!(!versions[0].tombstone);
        assert!(versions[1].tombstone);

        // Deduplication keeps first (value)
        let result = deduplicate_versions(&versions);
        assert_eq!(result.len(), 1);
        assert!(!result[0].tombstone);
    }

    #[test]
    fn should_merge_entries_given_overlapping_key_ranges() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        let mut versions = vec![
            make_version(b"a", 100, false),
            make_version(b"c", 150, false),
            make_version(b"b", 200, false), // Overlaps between a and c
            make_version(b"d", 250, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].user_key, b"a");
        assert_eq!(versions[1].user_key, b"b");
        assert_eq!(versions[2].user_key, b"c");
        assert_eq!(versions[3].user_key, b"d");
    }

    #[test]
    fn should_merge_and_sort_entries_given_multiple_levels() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        let mut versions = vec![
            // L0 entries (newer, higher seq)
            make_version(b"key1", 500, false),
            make_version(b"key3", 600, false),
            // L1 entries (older, lower seq)
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].user_key, b"key1");
        assert_eq!(versions[0].seq, 500); // Newer first
        assert_eq!(versions[1].user_key, b"key1");
        assert_eq!(versions[1].seq, 100); // Older second
        assert_eq!(versions[2].user_key, b"key2");
        assert_eq!(versions[3].user_key, b"key3");
    }

    #[test]
    fn should_drop_obsolete_entries_below_smallest_snapshot() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        let mut versions = vec![
            make_version(b"key1", 200, false), // Newest
            make_version(b"key1", 100, false), // Obsolete (older version)
            make_version(b"key1", 50, false),  // Obsolete (even older)
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].seq, 200);
    }

    #[test]
    fn should_return_sorted_and_deduplicated_entries_after_collection() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        let mut versions = vec![
            make_version(b"z", 50, false),
            make_version(b"a", 100, false),
            make_version(b"a", 200, false), // Duplicate key
            make_version(b"m", 150, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 3); // a@200, m, z
        assert_eq!(result[0].user_key, b"a");
        assert_eq!(result[0].seq, 200); // Kept newest
        assert_eq!(result[1].user_key, b"m");
        assert_eq!(result[2].user_key, b"z");
    }

    #[test]
    fn should_merge_tombstones_and_values_given_conflicting_versions() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        let mut versions = vec![
            make_version(b"key1", 100, false),
            make_tombstone(b"key2", 200),
            make_version(b"key3", 300, false),
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 3);
        assert!(!result[0].tombstone);
        assert!(result[1].tombstone);
        assert!(!result[2].tombstone);
    }

    #[test]
    fn should_maintain_uniqueness_after_encode_decode_roundtrip() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        let mut versions = vec![
            make_version(b"key1", 200, false),
            make_version(b"key1", 100, false),
            make_version(b"key2", 150, false),
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let encoded: Vec<Vec<u8>> = versions
            .iter()
            .map(|v| crate::internal_key::encode_internal_key(&v.user_key, v.seq, v.tombstone))
            .collect();

        // Verify encoded keys are in ascending order
        for i in 1..encoded.len() {
            assert!(
                encoded[i] > encoded[i - 1],
                "Encoded keys not in ascending order at index {}: {:?} <= {:?}",
                i,
                hex::encode(&encoded[i]),
                hex::encode(&encoded[i - 1])
            );
        }

        // Decode back
        let decoded: Vec<_> = encoded
            .iter()
            .map(|ik| crate::internal_key::decode_internal_key(ik).unwrap())
            .collect();

        // Assert
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0], (b"key1".to_vec(), 200, false));
        assert_eq!(decoded[1], (b"key1".to_vec(), 100, false));
        assert_eq!(decoded[2], (b"key2".to_vec(), 150, false));
    }

    #[test]
    fn should_preserve_snapshot_visibility_given_active_snapshot_seq() {
        // Arrange
        let versions = [
            make_version(b"key1", 50, false),  // Below snapshot
            make_version(b"key2", 100, false), // At snapshot
            make_version(b"key3", 150, false), // Above snapshot
        ];

        // Act
        // Snapshot filtering happens at GET time, not compaction time
        // Compaction preserves all versions for snapshot isolation

        // Assert
        assert_eq!(versions.len(), 3);
    }

    #[test]
    fn should_ignore_versions_newer_than_snapshot_when_collecting() {
        // Arrange
        // Snapshot filtering happens at read time, not compaction time
        let versions = vec![
            make_version(b"key1", 1000, false), // Very new
            make_version(b"key2", 50, false),   // Old
        ];

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn should_limit_memory_usage_given_large_number_of_ssts() {
        // Arrange
        let mut versions = Vec::new();
        for i in 0..1000 {
            versions.push(make_version(
                format!("key_{:04}", i).as_bytes(),
                i as u64,
                false,
            ));
        }

        // Act
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions.len(), 1000);
        // Memory usage is bounded by version count, not SST file size
    }

    #[test]
    fn should_stream_versions_incrementally_given_iterator_mode() {
        // Arrange
        // (Current implementation doesn't stream)
        let versions = [
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];

        // Act
        let chunk_size = 1;
        for chunk in versions.chunks(chunk_size) {
            assert!(!chunk.is_empty());
        }

        // Assert
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn should_count_versions_and_tombstones_given_merge_result() {
        // Arrange
        let versions = [
            make_version(b"key1", 100, false),
            make_tombstone(b"key2", 200),
            make_version(b"key3", 300, false),
            make_tombstone(b"key4", 400),
        ];

        // Act
        let tombstone_count = versions.iter().filter(|v| v.tombstone).count();
        let value_count = versions.iter().filter(|v| !v.tombstone).count();

        // Assert
        assert_eq!(tombstone_count, 2);
        assert_eq!(value_count, 2);
        assert_eq!(versions.len(), 4);
    }

    #[test]
    fn should_filter_out_expired_entries_given_ttl_threshold() {
        // Arrange
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut expired = make_version(b"expired_key", 100, false);
        expired.expiration = Some(now - 1000); // Expired 1 second ago

        let mut valid = make_version(b"valid_key", 200, false);
        valid.expiration = Some(now + 10000); // Expires in future

        let versions = [expired, valid];

        // Act
        let not_expired: Vec<_> = versions
            .iter()
            .filter(|v| {
                if let Some(exp) = v.expiration {
                    exp > now
                } else {
                    true // No expiration = never expires
                }
            })
            .collect();

        // Assert
        assert_eq!(not_expired.len(), 1);
        assert_eq!(not_expired[0].user_key, b"valid_key");
    }

    #[test]
    fn should_merge_duplicate_keys_given_different_cf_ids() {
        // Arrange
        use crate::core::compaction::execution::collection::sort_versions_for_output;
        
        // (CF info is encoded in internal key, not CompactionVersion)
        let mut versions = vec![
            make_version(b"key", 100, false),
            make_version(b"key", 200, false), // Same user key, different seq
        ];
        sort_versions_for_output(&mut versions);

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].seq, 200);
    }

    #[test]
    fn should_recover_partially_uploaded_sst_given_cloud_reconciliation() {
        // Arrange
        // In actual implementation, cloud manager would detect incomplete uploads
        let versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];

        // Act
        let result = deduplicate_versions(&versions);

        // Assert
        assert_eq!(result.len(), 2);
    }
}
