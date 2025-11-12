//! Bloom filter cache for SST files
//!
//! Provides fast bloom filter lookups to avoid unnecessary SST file opens.
//! Caches parsed BloomFilter objects to eliminate deserialization overhead.

use crate::core::manifest::Manifest;
use crate::sst::bloom::BloomFilter;
use crate::sst::mem::SstMemReader;
use crate::sst::metadata_cache::{collect_sst_names, SstMetadataCache};
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use std::path::Path;

/// Cache of parsed bloom filters for SST files.
///
/// This cache stores deserialized BloomFilter objects to avoid:
/// - Repeated SST file reads
/// - Bloom filter deserialization overhead on every get() operation
///
/// The cache is lock-free (using DashMap) to support concurrent reads.
pub struct BloomCache {
    cache: SstMetadataCache<BloomFilter>,
}

impl BloomCache {
    /// Create a new bloom cache for the given SST directory
    pub fn new(sst_dir: PathBuf) -> Self {
        Self {
            cache: SstMetadataCache::new(sst_dir),
        }
    }

    /// Populate the cache from all SST files listed in the manifest
    ///
    /// Reads each SST file, extracts the bloom filter, and caches it.
    /// Silently skips SST files that cannot be read or parsed.
    /// Supports both legacy `ssts` field and newer `files` field.
    pub fn populate_from_manifest(&self, manifest: &Manifest) {
        for sst_name in collect_sst_names(manifest) {
            let sst_path = self.cache.sst_dir().join(&sst_name);

            // Read SST file bytes
            if let Ok(bytes) = std::fs::read(&sst_path) {
                // Parse SST structure (from_bytes takes ownership)
                if let Ok(sst_reader) = SstMemReader::from_bytes(bytes) {
                    // Extract bloom filter bytes
                    if let Some(bloom_bytes) = sst_reader.get_bloom_filter_bytes() {
                        // Decode into BloomFilter object
                        if let Ok(bloom) = BloomFilter::decode_block(&bloom_bytes) {
                            self.cache.insert(sst_name, bloom);
                        }
                    }
                }
            }
        }
    }

    /// Check if a key may exist in the given SST file
    ///
    /// Returns:
    /// - `true` if the bloom filter is not cached (conservative - assume key may exist)
    /// - `true` if the bloom filter says the key may exist
    /// - `false` if the bloom filter says the key definitely does not exist
    pub fn may_contain(&self, sst_name: &str, key: &[u8]) -> bool {
        match self.cache.get(sst_name) {
            Some(bloom) => bloom.may_contain(key),
            None => true, // Conservative: if no bloom filter, assume key may exist
        }
    }

    /// Insert a bloom filter for an SST file
    pub fn insert(&self, sst_name: String, bloom: BloomFilter) {
        self.cache.insert(sst_name, bloom);
    }

    /// Remove a bloom filter for an SST file (e.g., after compaction)
    pub fn remove(&self, sst_name: &str) -> Option<Arc<BloomFilter>> {
        self.cache.remove(sst_name)
    }

    /// Clear all cached bloom filters
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached bloom filters
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Get a reference to the underlying cache (for testing/metrics)
    #[cfg(test)]
    pub(crate) fn cache(&self) -> &SstMetadataCache<BloomFilter> {
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

        let cache = BloomCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.populate_from_manifest(&manifest);

        // Assert
        assert_eq!(cache.len(), 2);
        assert!(cache.cache().cache().contains_key("sst_001.blob"));
        assert!(cache.cache().cache().contains_key("sst_002.blob"));
    }

    #[test]
    fn should_return_true_when_key_may_exist_in_cached_bloom() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let keys: &[&[u8]] = &[b"apple", b"banana", b"cherry"];
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", keys).unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = BloomCache::new(temp_dir.path().to_path_buf());
        cache.populate_from_manifest(&manifest);

        // Act
        let result = cache.may_contain("sst_001.blob", b"apple");

        // Assert
        assert!(result, "Should return true for key that exists");
    }

    #[test]
    fn should_return_false_when_key_definitely_absent_in_bloom() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let keys: &[&[u8]] = &[b"apple", b"banana", b"cherry"];
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", keys).unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = BloomCache::new(temp_dir.path().to_path_buf());
        cache.populate_from_manifest(&manifest);

        // Act
        let result = cache.may_contain("sst_001.blob", b"definitely_not_there_xyz123");

        // Assert
        assert!(!result, "Should return false for key bloom says is absent");
    }

    #[test]
    fn should_return_true_when_bloom_not_cached_for_sst() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let cache = BloomCache::new(temp_dir.path().to_path_buf());

        // Act
        let result = cache.may_contain("nonexistent.blob", b"any_key");

        // Assert
        assert!(
            result,
            "Should conservatively return true when bloom not cached"
        );
    }

    #[test]
    fn should_skip_missing_sst_files_when_populating() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", &[b"key1"]).unwrap();
        // sst_002.blob intentionally not created

        let manifest = create_manifest_with_files(vec![
            "sst_001.blob".to_string(),
            "sst_002.blob".to_string(), // This file doesn't exist
        ]);

        let cache = BloomCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.populate_from_manifest(&manifest);

        // Assert
        assert_eq!(cache.len(), 1, "Should only cache the existing SST");
        assert!(cache.cache().cache().contains_key("sst_001.blob"));
        assert!(!cache.cache().cache().contains_key("sst_002.blob"));
    }
}
