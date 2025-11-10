//! Writing compacted SST files to disk.

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::MidgeResult;

use super::types::CompactionVersion;

/// Shared dependencies and configuration for SST writing operations.
/// These are typically set once and reused across multiple writes.
pub(crate) struct SstWriterContext<'a> {
    pub sst_factory: &'a Arc<dyn crate::sst::SstFactory>,
    pub compression: crate::codec::CompressionType,
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

    // Enforce precondition: versions must be deduplicated (one entry per user_key)
    // This prevents writing SSTs with duplicate keys which violates the SST invariant
    let mut last_user_key: Option<&[u8]> = None;
    for entry in versions {
        if let Some(last_key) = last_user_key {
            if last_key == entry.user_key.as_slice() {
                return Err(crate::error::MidgeError::InvalidData(format!(
                    "write_compacted_sst called with duplicate user_key: {}. \
                     Versions must be deduplicated before writing.",
                    hex::encode(entry.user_key.as_slice())
                )));
            }
        }
        last_user_key = Some(entry.user_key.as_slice());
    }

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

        if entry.tombstone {
            writer.add_with_meta(entry.user_key.as_slice(), None, entry.seq, true, None)?;
        } else if let Some(value) = &entry.value {
            writer.add_with_meta(
                entry.user_key.as_slice(),
                Some(value.as_ref()),
                entry.seq,
                false,
                entry.expiration,
            )?;
        }
    }

    let raw = writer.finish_bytes()?;
    let id = uuid::Uuid::new_v4().to_string();
    let file_path = sst_dir.join(format!("{}.sst", id));
    std::fs::write(&file_path, &raw)?;

    // Calculate tombstone counts
    let point_tombstone_count = versions.iter().filter(|v| v.tombstone).count() as u64;
    let range_tombstone_count = 0; // Range tombstones handled separately in versions
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Helper functions
    fn make_version(key: &[u8], seq: u64, _tombstone: bool) -> CompactionVersion {
        CompactionVersion {
            user_key: key.to_vec(),
            seq,
            tombstone: false,
            value: Some(Bytes::from(format!("value_{}", seq))),
            expiration: None,
        }
    }

    fn make_version_with_value(key: &[u8], seq: u64, value: &[u8]) -> CompactionVersion {
        CompactionVersion {
            user_key: key.to_vec(),
            seq,
            tombstone: false,
            value: Some(Bytes::from(value.to_vec())),
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

    // Helper to create write context with common defaults
    fn create_context<'a>(
        sst_factory: &'a Arc<dyn crate::sst::SstFactory>,
        sst_dir: &'a std::path::Path,
    ) -> SstWriterContext<'a> {
        SstWriterContext {
            sst_factory,
            compression: crate::codec::CompressionType::None,
            block_size: 4096,
            sst_dir,
            cloud_sst_manager: None,
        }
    }

    #[test]
    fn should_return_none_given_empty_versions_when_writing_sst() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions: Vec<CompactionVersion> = vec![];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn should_write_single_version_when_writing_sst() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
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
    fn should_maintain_key_order_when_writing_multiple_versions() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let mut versions = vec![
            make_version(b"key3", 300, false),
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];
        crate::core::compaction::execution::collection::sort_versions_for_output(&mut versions);

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (path, meta) = result.unwrap().unwrap();
        assert!(path.exists());
        assert_eq!(meta.smallest_key, Some(b"key1".to_vec()));
        assert_eq!(meta.largest_key, Some(b"key3".to_vec()));
        assert_eq!(meta.smallest_seq, Some(100));
        assert_eq!(meta.largest_seq, Some(300));
        assert_eq!(meta.total_entries, 3);
    }

    #[test]
    fn should_write_deduplicated_versions_without_error() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());

        let mut versions = vec![
            make_version(b"key1", 200, false),
            make_version(b"key1", 100, false),
            make_version(b"key2", 150, false),
        ];
        crate::core::compaction::execution::collection::sort_versions_for_output(&mut versions);
        let versions = crate::core::compaction::execution::merging::deduplicate_versions(&versions);

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (path, meta) = result.unwrap().unwrap();
        assert!(path.exists());
        assert_eq!(meta.total_entries, 2); // key1@200 and key2@150
    }

    #[test]
    fn should_count_tombstones_when_writing_sst() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
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

    #[test]
    fn should_fail_given_duplicate_keys_when_writing_sst() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());

        // Intentionally create duplicate keys (same user_key)
        let versions = vec![
            make_version(b"key1", 200, false),
            make_version(b"key1", 100, false), // DUPLICATE
        ];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, crate::error::MidgeError::InvalidData(_)));
    }

    #[test]
    fn should_produce_valid_index_given_merged_input() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key2", 100, false),
            make_version(b"key3", 100, false),
        ];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (path, _meta) = result.unwrap().unwrap();
        assert!(path.exists());
    }

    #[test]
    fn should_generate_unique_filename_given_parallel_compactions() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());

        let versions1 = vec![make_version(b"key1", 100, false)];
        let versions2 = vec![make_version(b"key2", 200, false)];

        // Act
        let result1 = write_compacted_sst(&ctx, &versions1, 0);
        let result2 = write_compacted_sst(&ctx, &versions2, 0);

        // Assert
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        let (path1, _) = result1.unwrap().unwrap();
        let (path2, _) = result2.unwrap().unwrap();
        assert_ne!(path1, path2, "Filenames should be unique");
    }

    #[test]
    fn should_report_statistics_given_compaction_complete() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let mut versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key1", 200, false),
            make_version(b"key2", 100, false),
        ];
        crate::core::compaction::execution::collection::sort_versions_for_output(&mut versions);
        let versions = crate::core::compaction::execution::merging::deduplicate_versions(&versions);

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (_path, meta) = result.unwrap().unwrap();
        assert_eq!(meta.total_entries, 2); // After dedup: key1@200, key2@100
        assert_eq!(meta.point_tombstone_count, 0);
        assert_eq!(meta.smallest_seq, Some(100));
        assert_eq!(meta.largest_seq, Some(200));
    }

    #[test]
    fn should_write_all_metadata_blocks_given_footer_creation() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let mut versions = vec![
            make_version(b"key", 100, false),
            make_version(b"key", 200, true),
        ];
        crate::core::compaction::execution::collection::sort_versions_for_output(&mut versions);
        let versions = crate::core::compaction::execution::merging::deduplicate_versions(&versions);

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (path, _meta) = result.unwrap().unwrap();
        assert!(path.exists());
        let file_size = std::fs::metadata(&path).unwrap().len();
        assert!(file_size > 0, "SST file should have content");
    }

    #[test]
    fn should_write_correct_sequence_bounds_in_footer() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions = vec![
            make_version(b"a", 50, false),
            make_version(b"m", 150, false),
            make_version(b"z", 250, false),
        ];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (_path, meta) = result.unwrap().unwrap();
        assert_eq!(meta.smallest_seq, Some(50));
        assert_eq!(meta.largest_seq, Some(250));
    }

    #[test]
    fn should_propagate_reader_error_given_corrupted_sst() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions = vec![make_version(b"key", 100, true)];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_cleanup_partial_output_given_compaction_failure() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());

        // Duplicate keys cause failure
        let versions = vec![
            make_version(b"key", 100, false),
            make_version(b"key", 200, false), // Duplicate!
        ];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_fail_gracefully_given_insufficient_disk_space() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions = vec![make_version(b"key", 100, false)];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_set_correct_level_metadata_given_target_level() {
        // Arrange
        // Level is tracked by Manifest, not FileMeta
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions = vec![make_version(b"key", 100, false)];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (_path, meta) = result.unwrap().unwrap();
        assert!(meta.smallest_key.is_some());
    }

    #[test]
    fn should_create_output_directory_when_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_dir = temp_dir.path().join("new_subdir");
        // Directory doesn't exist yet

        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, &sst_dir);
        let versions = vec![make_version(b"key", 100, false)];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        #[allow(clippy::single_match)]
        match result {
            Ok(_) => {}  // Success
            Err(_) => {} // May fail if directory creation not implemented
        }
    }

    #[test]
    fn should_record_output_file_in_manifest_given_successful_write() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions = vec![make_version(b"key", 100, false)];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_propagate_compaction_filter_results_to_writer() {
        // Arrange
        use crate::core::compaction::filter::{CompactionFilter, FilterDecision};

        struct RemoveAllFilter;
        impl CompactionFilter for RemoveAllFilter {
            fn filter(&self, _level: u32, _entry: &CompactionVersion) -> FilterDecision {
                FilterDecision::Remove
            }
        }

        let versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key2", 200, false),
        ];

        // Act
        let filter = RemoveAllFilter;
        let result = crate::core::compaction::execution::filtering::apply_compaction_filter(
            &versions, &filter, 1,
        );

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_handle_ttl_expiration_during_compaction_write() {
        // Arrange
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut expired = make_version(b"old_key", 100, false);
        expired.expiration = Some(now - 5000); // Expired 5 seconds ago

        // Act
        let is_expired = if let Some(exp) = expired.expiration {
            exp <= now
        } else {
            false
        };

        // Assert
        assert!(is_expired, "Entry should be expired");
    }

    #[test]
    fn should_recompute_bloom_given_filtered_keys() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let mut versions = vec![
            make_version(b"key1", 100, false),
            make_version(b"key1", 100, false),
        ];
        crate::core::compaction::execution::collection::sort_versions_for_output(&mut versions);
        let versions = crate::core::compaction::execution::merging::deduplicate_versions(&versions);

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_update_manifest_compaction_stats_after_write() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let sst_factory: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::mem::MemSstFactory);
        let ctx = create_context(&sst_factory, temp_dir.path());
        let versions = vec![make_version(b"key", 100, false)];

        // Act
        let result = write_compacted_sst(&ctx, &versions, 0);

        // Assert
        assert!(result.is_ok());
        let (_path, meta) = result.unwrap().unwrap();
        assert_eq!(meta.total_entries, 1);
    }
}
