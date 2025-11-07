//! Generic SST metadata cache infrastructure
//!
//! Provides a generic cache for SST file metadata (bloom filters, sparse indexes, etc.)
//! to eliminate code duplication and optimize SST file reads.

use crate::manifest::Manifest;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Generic cache for SST file metadata
///
/// This provides lock-free concurrent access to cached metadata extracted from SST files.
/// The cache is parameterized by the metadata type `T`.
pub struct SstMetadataCache<T> {
    /// Maps SST filename -> cached metadata
    cache: Arc<DashMap<String, Arc<T>>>,
    /// Directory containing SST files
    sst_dir: PathBuf,
}

impl<T> SstMetadataCache<T> {
    /// Create a new metadata cache for the given SST directory
    pub fn new(sst_dir: PathBuf) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            sst_dir,
        }
    }

    /// Get the SST directory path
    pub fn sst_dir(&self) -> &PathBuf {
        &self.sst_dir
    }

    /// Get cached metadata for a given SST file
    ///
    /// Returns:
    /// - `Some(Arc<T>)` if the metadata is cached
    /// - `None` if the metadata is not cached
    pub fn get(&self, sst_name: &str) -> Option<Arc<T>> {
        self.cache.get(sst_name).map(|entry| Arc::clone(&entry))
    }

    /// Insert metadata for an SST file
    pub fn insert(&self, sst_name: String, metadata: T) {
        self.cache.insert(sst_name, Arc::new(metadata));
    }

    /// Remove metadata for an SST file (e.g., after compaction)
    pub fn remove(&self, sst_name: &str) -> Option<Arc<T>> {
        self.cache.remove(sst_name).map(|(_, metadata)| metadata)
    }

    /// Clear all cached metadata
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Get a reference to the underlying cache (for testing/metrics)
    #[cfg(test)]
    pub(crate) fn cache(&self) -> &Arc<DashMap<String, Arc<T>>> {
        &self.cache
    }
}

/// Extract all SST filenames from a manifest
///
/// Supports both legacy `ssts` field and newer `files` field.
pub fn collect_sst_names(manifest: &Manifest) -> Vec<String> {
    let mut sst_names = std::collections::HashSet::new();

    // Add from legacy ssts field
    for name in &manifest.ssts {
        sst_names.insert(name.clone());
    }

    // Add from newer files field
    for file_meta in &manifest.files {
        sst_names.insert(file_meta.name.clone());
    }

    sst_names.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_create_empty_cache_when_new() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();

        // Act
        let cache: SstMetadataCache<String> = SstMetadataCache::new(temp_dir.path().to_path_buf());

        // Assert
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn should_insert_and_retrieve_metadata() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let cache = SstMetadataCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.insert("sst_001.blob".to_string(), "test_metadata".to_string());
        let result = cache.get("sst_001.blob");

        // Assert
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), "test_metadata");
    }

    #[test]
    fn should_return_none_when_metadata_not_cached() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let cache: SstMetadataCache<String> = SstMetadataCache::new(temp_dir.path().to_path_buf());

        // Act
        let result = cache.get("nonexistent.blob");

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_remove_metadata_when_remove_called() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let cache = SstMetadataCache::new(temp_dir.path().to_path_buf());
        cache.insert("sst_001.blob".to_string(), "test".to_string());

        // Act
        let removed = cache.remove("sst_001.blob");

        // Assert
        assert!(removed.is_some());
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn should_return_none_when_removing_nonexistent_metadata() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let cache: SstMetadataCache<String> = SstMetadataCache::new(temp_dir.path().to_path_buf());

        // Act
        let removed = cache.remove("nonexistent.blob");

        // Assert
        assert!(removed.is_none());
    }

    #[test]
    fn should_clear_all_metadata_when_clear_called() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let cache = SstMetadataCache::new(temp_dir.path().to_path_buf());
        cache.insert("sst_001.blob".to_string(), "data1".to_string());
        cache.insert("sst_002.blob".to_string(), "data2".to_string());

        // Act
        cache.clear();

        // Assert
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn should_update_metadata_when_inserting_duplicate_key() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let cache = SstMetadataCache::new(temp_dir.path().to_path_buf());
        cache.insert("sst_001.blob".to_string(), "old".to_string());

        // Act
        cache.insert("sst_001.blob".to_string(), "new".to_string());

        // Assert
        assert_eq!(cache.len(), 1);
        assert_eq!(*cache.get("sst_001.blob").unwrap(), "new");
    }

    #[test]
    fn should_collect_sst_names_from_legacy_field() {
        // Arrange
        let manifest = Manifest {
            ssts: vec!["sst_001.blob".to_string(), "sst_002.blob".to_string()],
            ..Default::default()
        };

        // Act
        let names = collect_sst_names(&manifest);

        // Assert
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"sst_001.blob".to_string()));
        assert!(names.contains(&"sst_002.blob".to_string()));
    }

    #[test]
    fn should_collect_sst_names_from_files_field() {
        // Arrange
        let manifest = Manifest {
            files: vec![crate::manifest::FileMeta {
                name: "sst_001.blob".to_string(),
                level: 0,
                size_bytes: 1024,
                cf_id: 0,
                smallest_key: None,
                largest_key: None,
                smallest_seq: None,
                largest_seq: None,
                sublevel: 0,
                cloud_location: None,
                cloud_checksum: None,
                cloud_uploaded_at: None,
                cloud_state: None,
                point_tombstone_count: 0,
                range_tombstone_count: 0,
                total_entries: 0,
            }],
            ..Default::default()
        };

        // Act
        let names = collect_sst_names(&manifest);

        // Assert
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"sst_001.blob".to_string()));
    }

    #[test]
    fn should_deduplicate_sst_names_from_both_fields() {
        // Arrange
        let manifest = Manifest {
            ssts: vec!["sst_001.blob".to_string()],
            files: vec![crate::manifest::FileMeta {
                name: "sst_001.blob".to_string(),
                level: 0,
                size_bytes: 1024,
                cf_id: 0,
                smallest_key: None,
                largest_key: None,
                smallest_seq: None,
                largest_seq: None,
                sublevel: 0,
                cloud_location: None,
                cloud_checksum: None,
                cloud_uploaded_at: None,
                cloud_state: None,
                point_tombstone_count: 0,
                range_tombstone_count: 0,
                total_entries: 0,
            }],
            ..Default::default()
        };

        // Act
        let names = collect_sst_names(&manifest);

        // Assert
        assert_eq!(names.len(), 1);
    }
}
