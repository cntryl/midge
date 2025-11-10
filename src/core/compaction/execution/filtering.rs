use super::types::CompactionVersion;

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
    filter: &dyn super::super::filter::CompactionFilter,
    target_level: u32,
) -> Vec<CompactionVersion> {
    use super::super::filter::FilterDecision;

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

#[cfg(test)]
mod tests {
    use super::super::super::filter::{CompactionFilter, FilterDecision};
    use super::*;
    use bytes::Bytes;

    // Helper to create a version
    fn make_version_with_value(key: &[u8], seq: u64, value: &[u8]) -> CompactionVersion {
        CompactionVersion {
            user_key: key.to_vec(),
            seq,
            tombstone: false,
            value: Some(Bytes::from(value.to_vec())),
            expiration: None,
        }
    }

    // Mock filter that keeps everything
    struct KeepAllFilter;
    impl CompactionFilter for KeepAllFilter {
        fn filter(&self, _level: u32, _version: &CompactionVersion) -> FilterDecision {
            FilterDecision::Keep
        }
    }

    // Mock filter that removes everything
    struct RemoveAllFilter;
    impl CompactionFilter for RemoveAllFilter {
        fn filter(&self, _level: u32, _version: &CompactionVersion) -> FilterDecision {
            FilterDecision::Remove
        }
    }

    // Mock filter that converts to tombstones
    struct TombstoneAllFilter;
    impl CompactionFilter for TombstoneAllFilter {
        fn filter(&self, _level: u32, _version: &CompactionVersion) -> FilterDecision {
            FilterDecision::RemoveAndTombstone
        }
    }

    #[test]
    fn should_keep_all_entries_given_keep_filter_when_applying_filter() {
        // Arrange
        let versions = vec![
            make_version_with_value(b"key1", 100, b"value1"),
            make_version_with_value(b"key2", 200, b"value2"),
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
    fn should_remove_all_entries_given_remove_filter_when_applying_filter() {
        // Arrange
        let versions = vec![
            make_version_with_value(b"key1", 100, b"value1"),
            make_version_with_value(b"key2", 200, b"value2"),
        ];
        let filter = RemoveAllFilter;

        // Act
        let result = apply_compaction_filter(&versions, &filter, 1);

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_convert_to_tombstones_given_tombstone_filter_when_applying_filter() {
        // Arrange
        let versions = vec![
            make_version_with_value(b"key1", 100, b"value1"),
            make_version_with_value(b"key2", 200, b"value2"),
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

    #[test]
    fn should_preserve_sequence_given_tombstone_conversion_when_applying_filter() {
        // Arrange
        let versions = vec![make_version_with_value(b"key1", 12345, b"value")];
        let filter = TombstoneAllFilter;

        // Act
        let result = apply_compaction_filter(&versions, &filter, 1);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].seq, 12345);
        assert!(result[0].tombstone);
    }

    #[test]
    fn should_apply_filter_to_tombstone_entries() {
        // Arrange
        struct RemoveTombstonesFilter;
        impl CompactionFilter for RemoveTombstonesFilter {
            fn filter(&self, _level: u32, version: &CompactionVersion) -> FilterDecision {
                if version.tombstone {
                    FilterDecision::Remove
                } else {
                    FilterDecision::Keep
                }
            }
        }

        fn make_version(key: &[u8], seq: u64, _tombstone: bool) -> CompactionVersion {
            CompactionVersion {
                user_key: key.to_vec(),
                seq,
                tombstone: false,
                value: Some(Bytes::from(format!("value_{}", seq))),
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

        let versions = vec![
            make_version(b"key1", 100, false),
            make_tombstone(b"key2", 200),
            make_version(b"key3", 150, false),
        ];
        let filter = RemoveTombstonesFilter;

        // Act
        let result = apply_compaction_filter(&versions, &filter, 1);

        // Assert
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|v| !v.tombstone));
    }
}
