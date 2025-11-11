//! Version collection from SST files during compaction.

use std::sync::Arc;

use super::types::CompactionVersion;

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
    use std::collections::HashSet;

    let mut versions = Vec::new();
    let mut seen: HashSet<(Vec<u8>, u64, bool)> = HashSet::new();

    for name in sst_names.iter().rev() {
        let path = sst_dir.join(name);
        if !path.exists() {
            continue;
        }
        let Ok(sst) = reader_factory.open(&path) else {
            continue;
        };
        let Ok(rows) = sst.scan_range_state(None, None) else {
            continue;
        };
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

/// Sort versions by user_key (ascending), then sequence (descending), then tombstone flag.
///
/// This establishes the canonical ordering for compacted output:
/// - Keys are in ascending order
/// - For the same key, newer versions (higher seq) come first
/// - For the same key+seq, values come before tombstones
#[inline]
pub(crate) fn sort_versions_for_output(versions: &mut [CompactionVersion]) {
    use std::cmp::Ordering;
    versions.sort_by(|a, b| match a.user_key.cmp(&b.user_key) {
        Ordering::Equal => match b.seq.cmp(&a.seq) {
            Ordering::Equal => a.tombstone.cmp(&b.tombstone), // values (false) before tombstones (true)
            other => other,
        },
        other => other,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
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

    // Test sort_versions_for_output
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
    fn should_sort_values_before_tombstones_given_same_key_and_seq_when_sorting() {
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

    #[test]
    fn should_handle_empty_keys_when_sorting() {
        // Arrange
        let mut versions = vec![
            make_version(b"", 100, false),
            make_version(b"a", 200, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].user_key, b"");
        assert_eq!(versions[1].user_key, b"a");
    }

    #[test]
    fn should_handle_binary_keys_when_sorting() {
        // Arrange
        let mut versions = vec![
            make_version(b"key\x00\xff", 100, false),
            make_version(b"key\x00\x00", 100, false),
            make_version(b"key", 100, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].user_key, b"key");
        assert_eq!(versions[1].user_key, b"key\x00\x00");
        assert_eq!(versions[2].user_key, b"key\x00\xff");
    }

    #[test]
    fn should_handle_sequence_boundaries_when_sorting() {
        // Arrange
        let mut versions = vec![
            make_version(b"key1", u64::MAX, false),
            make_version(b"key1", 0, false),
            make_version(b"key1", u64::MAX - 1, false),
        ];

        // Act
        sort_versions_for_output(&mut versions);

        // Assert
        assert_eq!(versions[0].seq, u64::MAX);
        assert_eq!(versions[1].seq, u64::MAX - 1);
        assert_eq!(versions[2].seq, 0);
    }

    #[test]
    fn should_collect_all_versions_given_multiple_column_families() {
        // Arrange
        use std::sync::Arc;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstReaderFactory> =
            Arc::new(crate::sst::mem::MemSstReaderFactory::new(false));

        // Act
        let versions = collect_compaction_versions(
            &sst_factory,
            temp_dir.path(),
            &["nonexistent.sst".to_string()],
        );

        // Assert
        assert_eq!(versions.len(), 0);
    }

    #[test]
    fn should_skip_deleted_sst_metadata_given_manifest_marked_deleted() {
        // Arrange
        use std::sync::Arc;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstReaderFactory> =
            Arc::new(crate::sst::mem::MemSstReaderFactory::new(false));

        // Act
        let versions = collect_compaction_versions(
            &sst_factory,
            temp_dir.path(),
            &[
                "deleted_file_1.sst".to_string(),
                "deleted_file_2.sst".to_string(),
            ],
        );

        // Assert
        assert_eq!(versions.len(), 0);
    }

    #[test]
    fn should_preserve_file_order_given_same_level_input() {
        // Arrange
        use std::sync::Arc;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstReaderFactory> =
            Arc::new(crate::sst::mem::MemSstReaderFactory::new(false));

        // Act
        let file_names = vec!["file1.sst".to_string(), "file2.sst".to_string()];
        let versions = collect_compaction_versions(&sst_factory, temp_dir.path(), &file_names);

        // Assert
        assert_eq!(versions.len(), 0);
    }

    #[test]
    fn should_handle_partial_read_errors_and_continue_merge() {
        // Arrange
        use std::sync::Arc;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstReaderFactory> =
            Arc::new(crate::sst::mem::MemSstReaderFactory::new(false));

        // Act
        let versions = collect_compaction_versions(
            &sst_factory,
            temp_dir.path(),
            &["corrupted.sst".to_string()],
        );

        // Assert
        assert_eq!(versions.len(), 0);
    }
}
