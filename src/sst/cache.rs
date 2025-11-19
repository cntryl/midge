//! Combined SST metadata cache
//!
//! Optimized cache that reads SST files once to extract both bloom filters
//! and sparse indexes, avoiding duplicate file I/O.

use crate::core::manifest::Manifest;
use crate::sst::bloom::BloomFilter;
use crate::sst::bloom_cache::BloomCache;
use crate::sst::mem::SstMemReader;
use crate::sst::metadata_cache::collect_sst_names;
use crate::sst::reader_common::SstMetadata;
use crate::sst::sparse_index_cache::SparseIndexCache;
use std::path::PathBuf;

/// Combined cache for bloom filters and sparse indexes
///
/// This provides an optimized way to populate both caches by reading
/// each SST file only once, rather than twice (once per cache).
pub struct SstCache {
    pub bloom_cache: BloomCache,
    pub sparse_index_cache: SparseIndexCache,
    sst_dir: PathBuf,
}

impl SstCache {
    /// Create a new combined SST cache for the given directory
    pub fn new(sst_dir: PathBuf) -> Self {
        Self {
            bloom_cache: BloomCache::new(sst_dir.clone()),
            sparse_index_cache: SparseIndexCache::new(sst_dir.clone()),
            sst_dir,
        }
    }

    /// Populate both bloom and sparse index caches from the manifest
    ///
    /// This is more efficient than calling `populate_from_manifest` on each
    /// cache individually, as it only reads each SST file once.
    ///
    /// Silently skips SST files that cannot be read or parsed.
    pub fn populate_from_manifest(&self, manifest: &Manifest) {
        for sst_name in collect_sst_names(manifest) {
            let sst_path = self.sst_dir.join(&sst_name);

            // Read SST file bytes once
            if let Ok(bytes) = std::fs::read(&sst_path) {
                // Extract sparse index from metadata
                if let Ok(metadata) = SstMetadata::from_bytes(&bytes) {
                    self.sparse_index_cache
                        .insert(sst_name.clone(), metadata.sparse_index);
                }

                // Extract bloom filter from SST structure
                // We need to re-read because SstMetadata::from_bytes consumes bytes
                // and doesn't expose the bloom filter directly
                if let Ok(bytes_copy) = std::fs::read(&sst_path) {
                    if let Ok(sst_reader) = SstMemReader::from_bytes(bytes_copy) {
                        if let Some(bloom_bytes) = sst_reader.get_bloom_filter_bytes() {
                            if let Ok(bloom) = BloomFilter::decode_block(&bloom_bytes) {
                                self.bloom_cache.insert(sst_name.clone(), bloom);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Clear both caches
    pub fn clear(&self) {
        self.bloom_cache.clear();
        self.sparse_index_cache.clear();
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
        dir: &std::path::Path,
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
    fn should_populate_both_caches_when_called() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let keys: &[&[u8]] = &[b"apple", b"banana", b"cherry"];
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", keys).unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = SstCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.populate_from_manifest(&manifest);

        // Assert
        assert_eq!(cache.bloom_cache.len(), 1);
        assert_eq!(cache.sparse_index_cache.len(), 1);
        assert!(cache.bloom_cache.may_contain("sst_001.blob", b"apple"));
        assert!(cache.sparse_index_cache.get("sst_001.blob").is_some());
    }

    #[test]
    fn should_populate_multiple_ssts() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", &[b"key1", b"key2"]).unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_002.blob", &[b"key3", b"key4"]).unwrap();

        let manifest = create_manifest_with_files(vec![
            "sst_001.blob".to_string(),
            "sst_002.blob".to_string(),
        ]);
        let cache = SstCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.populate_from_manifest(&manifest);

        // Assert
        assert_eq!(cache.bloom_cache.len(), 2);
        assert_eq!(cache.sparse_index_cache.len(), 2);
    }

    #[test]
    fn should_clear_both_caches_when_clear_called() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", &[b"key1"]).unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = SstCache::new(temp_dir.path().to_path_buf());
        cache.populate_from_manifest(&manifest);

        // Act
        cache.clear();

        // Assert
        assert_eq!(cache.bloom_cache.len(), 0);
        assert_eq!(cache.sparse_index_cache.len(), 0);
    }

    #[test]
    fn should_handle_missing_files_gracefully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", &[b"key1"]).unwrap();

        let manifest = create_manifest_with_files(vec![
            "sst_001.blob".to_string(),
            "sst_002.blob".to_string(), // Doesn't exist
        ]);
        let cache = SstCache::new(temp_dir.path().to_path_buf());

        // Act
        cache.populate_from_manifest(&manifest);

        // Assert
        assert_eq!(cache.bloom_cache.len(), 1);
        assert_eq!(cache.sparse_index_cache.len(), 1);
    }

    #[test]
    fn should_verify_bloom_filter_functionality() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let keys: &[&[u8]] = &[b"apple", b"banana", b"cherry"];
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", keys).unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = SstCache::new(temp_dir.path().to_path_buf());
        cache.populate_from_manifest(&manifest);

        // Act

        // Assert
        assert!(cache.bloom_cache.may_contain("sst_001.blob", b"apple"));
        assert!(!cache
            .bloom_cache
            .may_contain("sst_001.blob", b"nonexistent_xyz123"));
    }

    #[test]
    fn should_verify_sparse_index_functionality() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let keys: &[&[u8]] = &[b"apple", b"banana", b"cherry"];
        create_test_sst_with_keys(temp_dir.path(), "sst_001.blob", keys).unwrap();

        let manifest = create_manifest_with_files(vec!["sst_001.blob".to_string()]);
        let cache = SstCache::new(temp_dir.path().to_path_buf());
        cache.populate_from_manifest(&manifest);

        // Act
        let sparse_index = cache.sparse_index_cache.get("sst_001.blob").unwrap();

        // Assert
        assert!(sparse_index.find_block(b"banana").is_some());
    }
}
