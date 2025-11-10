///! Version merging and deduplication logic.

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
}
