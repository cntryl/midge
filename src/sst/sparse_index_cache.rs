//! Sparse index cache for SST files
//!
//! Provides fast sparse index lookups to avoid unnecessary SST file reads.
//! Caches parsed SparseIndex objects to eliminate deserialization overhead.

use crate::core::manifest::Manifest;
use crate::sst::metadata_cache::{collect_sst_names, SstMetadataCache};
use crate::sst::reader_common::SstMetadata;
use crate::sst::sparse_index::SparseIndex;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use std::path::Path;

/// Cache of parsed sparse indexes for SST files.
///
/// This cache stores deserialized SparseIndex objects to avoid:
/// - Repeated SST file reads
/// - Sparse index deserialization overhead on every get() operation
///
/// The cache is lock-free (using DashMap) to support concurrent reads.
pub struct SparseIndexCache {
    cache: SstMetadataCache<SparseIndex>,
}

impl SparseIndexCache {
    /// Create a new sparse index cache for the given SST directory
    pub fn new(sst_dir: PathBuf) -> Self {
        Self {
            cache: SstMetadataCache::new(sst_dir),
        }
    }

    /// Populate the cache from all SST files listed in the manifest
    ///
    /// Reads each SST file, extracts the sparse index, and caches it.
    /// Silently skips SST files that cannot be read or parsed.
    /// Supports both legacy `ssts` field and newer `files` field.
    pub fn populate_from_manifest(&self, manifest: &Manifest) {
        for sst_name in collect_sst_names(manifest) {
            let sst_path = self.cache.sst_dir().join(&sst_name);

            // Read SST file bytes
            if let Ok(bytes) = std::fs::read(&sst_path) {
                // Parse SST metadata (sparse index is included)
                if let Ok(metadata) = SstMetadata::from_bytes(&bytes) {
                    self.cache.insert(sst_name, metadata.sparse_index);
                }
            }
        }
    }

    /// Get the sparse index for a given SST file
    ///
    /// Returns:
    /// - `Some(Arc<SparseIndex>)` if the index is cached
    /// - `None` if the index is not cached
    pub fn get(&self, sst_name: &str) -> Option<Arc<SparseIndex>> {
        self.cache.get(sst_name)
    }

    /// Insert a sparse index for an SST file
    pub fn insert(&self, sst_name: String, sparse_index: SparseIndex) {
        self.cache.insert(sst_name, sparse_index);
    }

    /// Remove a sparse index for an SST file (e.g., after compaction)
    pub fn remove(&self, sst_name: &str) -> Option<Arc<SparseIndex>> {
        self.cache.remove(sst_name)
    }

    /// Clear all cached sparse indexes
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached sparse indexes
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Get a reference to the underlying cache (for testing/metrics)
    #[cfg(test)]
    pub(crate) fn cache(&self) -> &SstMetadataCache<SparseIndex> {
        &self.cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::codec::CompressionType;
    use crate::core::manifest::{FileMeta, Manifest};
    use crate::sst::mem::SstMemWriter;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_sst_with_keys(
        dir: &Path,
        filename: &str,
        keys: &[&[u8]],
    ) -> std::io::Result<()> {
        let mut writer = SstMemWriter::new(CompressionType::None, 4096);

        for (i, key) in keys.iter().enumerate() {
            writer.add(key, format!("value{}", i).as_bytes()).unwrap();
        }

        let bytes = writer.finish_bytes().unwrap();
        let path = dir.join(filename);
        let mut file = std::fs::File::create(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    fn create_manifest_with_files(filenames: Vec<String>) -> Manifest {
        let mut manifest = Manifest::default();
        for (i, name) in filenames.into_iter().enumerate() {
            manifest.files.push(FileMeta {
                name,
                level: 0,
                size_bytes: 1024,
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"z".to_vec()),
                smallest_seq: Some(i as u64),
                largest_seq: Some(i as u64),
                sublevel: 0,
                cloud_location: None,
                cloud_checksum: None,
                cloud_uploaded_at: None,
                cloud_state: None,
                point_tombstone_count: 0,
                range_tombstone_count: 0,
                total_entries: 10,
            });
        }
        manifest
    }

    #[test]
    fn should_populate_cache_from_manifest_when_ssts_exist() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", &[b"key1", b"key2"]).unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_002.blob", &[b"key3", b"key4"]).unwrap();

        let manifest = create_manifest_with_files(vec![
            "sst_001.blob".to_string(),
            "sst_002.blob".to_string(),
        ]);

        let cache = SparseIndexCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.populate_from_manifest(&manifest);

        // Assert
        assert_eq!(cache.len(), 2);
        assert!(cache.cache().cache().contains_key("sst_001.blob"));
        assert!(cache.cache().cache().contains_key("sst_002.blob"));
    }

    #[test]
    fn should_return_sparse_index_when_cached() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", &[b"apple", b"banana"]).unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = SparseIndexCache::new(temp_dir.path().to_path_buf());
        cache.populate_from_manifest(&manifest);

        // Act
        let result = cache.get("sst_001.blob");

        // Assert
        assert!(result.is_some());
        let sparse_index = result.unwrap();
        assert!(!sparse_index.entries().is_empty());
    }

    #[test]
    fn should_skip_missing_sst_files_when_populating() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", &[b"key1"]).unwrap();

        let manifest = create_manifest_with_files(vec![
            "sst_001.blob".to_string(),
            "sst_002.blob".to_string(), // This file doesn't exist
        ]);

        let cache = SparseIndexCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.populate_from_manifest(&manifest);

        // Assert
        assert_eq!(cache.len(), 1, "Should only cache the existing SST");
        assert!(cache.cache().cache().contains_key("sst_001.blob"));
        assert!(!cache.cache().cache().contains_key("sst_002.blob"));
    }

    #[test]
    fn should_find_block_handle_using_cached_index() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(
            temp_dir.path(),
            "sst_001.blob",
            &[b"apple", b"banana", b"cherry"],
        )
        .unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = SparseIndexCache::new(temp_dir.path().to_path_buf());
        cache.populate_from_manifest(&manifest);

        // Act
        let sparse_index = cache.get("sst_001.blob").unwrap();
        let block_handle = sparse_index.find_block(b"banana");

        // Assert
        assert!(block_handle.is_some());
    }
}
