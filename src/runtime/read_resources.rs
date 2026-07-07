//! Shared immutable read resources for runtime snapshots.

use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::metadata::FileMeta;
use crate::sst::cache::{BlockCache, CachePolicyType};
use crate::sst::fs::SstFileIo;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub(crate) struct ReadResources {
    sst_fs: Arc<dyn Fs>,
    sst_path_prefix: PathBuf,
    block_cache: Arc<BlockCache>,
    readers: Mutex<HashMap<ReaderCacheKey, Arc<SstFileIo>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReaderCacheKey {
    name: String,
    sst_id: u64,
}

impl ReadResources {
    pub(crate) fn new(
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: PathBuf,
        block_cache_size: usize,
        block_cache_policy: CachePolicyType,
    ) -> Self {
        Self {
            sst_fs,
            sst_path_prefix,
            block_cache: Arc::new(BlockCache::new(
                u64::try_from(block_cache_size).unwrap_or(u64::MAX),
                16,
                block_cache_policy,
            )),
            readers: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn reader_for(&self, file_meta: &FileMeta) -> MidgeResult<Arc<SstFileIo>> {
        let name = file_meta.name.clone();
        let sst_id = Self::sst_id_for_file(file_meta);
        let cache_key = ReaderCacheKey {
            name: name.clone(),
            sst_id,
        };
        if let Some(reader) = self
            .readers
            .lock()
            .map_err(|_| MidgeError::Internal("SST reader cache lock poisoned".into()))?
            .get(&cache_key)
            .cloned()
        {
            crate::sst::read_path_metrics::global_sst_read_metrics().record_reader_cache_hit();
            return Ok(reader);
        }
        crate::sst::read_path_metrics::global_sst_read_metrics().record_reader_cache_miss();

        let sst_path = self.sst_path_prefix.join(&name);
        let path_str = sst_path.to_string_lossy().to_string();
        let reader = Arc::new(
            SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))?
                .with_sst_id(sst_id)
                .with_block_cache(Arc::clone(&self.block_cache)),
        );

        let mut readers = self
            .readers
            .lock()
            .map_err(|_| MidgeError::Internal("SST reader cache lock poisoned".into()))?;
        if let Some(existing) = readers.get(&cache_key).cloned() {
            return Ok(existing);
        }
        readers.insert(cache_key, Arc::clone(&reader));
        Ok(reader)
    }

    pub(crate) fn capture_readers(
        &self,
        sst_files: &[FileMeta],
    ) -> HashMap<String, Arc<SstFileIo>> {
        sst_files
            .iter()
            .filter_map(|file_meta| {
                self.reader_for(file_meta)
                    .ok()
                    .map(|reader| (file_meta.name.clone(), reader))
            })
            .collect()
    }

    pub(crate) fn prune_to_live_ssts(&self, live_names: &HashSet<String>) {
        if let Ok(mut readers) = self.readers.lock() {
            let stale_sst_ids: Vec<u64> = readers
                .iter()
                .filter_map(|(key, reader)| {
                    (!live_names.contains(&key.name)).then_some(reader.sst_id())
                })
                .collect();
            readers.retain(|key, _reader| live_names.contains(&key.name));
            drop(readers);

            for sst_id in stale_sst_ids {
                self.block_cache.remove_sst(sst_id);
            }
        }
    }

    fn sst_id_for_file(file_meta: &FileMeta) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        file_meta.cf_id.hash(&mut hasher);
        file_meta.sst_seq.hash(&mut hasher);
        file_meta.size_bytes.hash(&mut hasher);
        file_meta.content_crc32c.hash(&mut hasher);
        file_meta.name.hash(&mut hasher);
        hasher.finish()
    }

    #[cfg(test)]
    pub(crate) fn cached_reader_count(&self) -> usize {
        self.readers.lock().map_or(0, |readers| readers.len())
    }

    #[cfg(test)]
    pub(crate) fn block_cache(&self) -> Arc<BlockCache> {
        Arc::clone(&self.block_cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::{SstFactory, SstStateReader};

    fn write_test_sst(temp_dir: &tempfile::TempDir, name: &str) -> MidgeResult<FileMeta> {
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 7, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))?;

        Ok(FileMeta {
            name: name.to_string(),
            level: 0,
            size_bytes: std::fs::metadata(temp_dir.path().join(name))?.len(),
            cf_id: 0,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(7),
            largest_seq: Some(7),
            ..Default::default()
        })
    }

    #[test]
    fn should_keep_active_reader_arc_when_pruned_reader_reused() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let file_meta = write_test_sst(&temp_dir, "reader-cache.sst")?;
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );

        // Act
        let first = resources.reader_for(&file_meta)?;
        let second = resources.reader_for(&file_meta)?;
        resources.prune_to_live_ssts(&HashSet::new());

        // Assert
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(resources.cached_reader_count(), 0);
        assert!(Arc::strong_count(&first) >= 2);
        Ok(())
    }

    #[test]
    fn should_prune_block_cache_entries_when_sst_reader_is_pruned() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let file_meta = write_test_sst(&temp_dir, "pruned-cache.sst")?;
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );
        let reader = resources.reader_for(&file_meta)?;
        let block_cache = resources.block_cache();
        assert!(matches!(
            reader.get_state_at(b"key", u64::MAX)?,
            crate::sst::types::KeyState::Value(_, 7, None, 0)
        ));
        assert!(
            !block_cache.is_empty(),
            "point read should populate shared block cache before pruning"
        );

        // Act
        resources.prune_to_live_ssts(&HashSet::new());

        // Assert
        assert_eq!(resources.cached_reader_count(), 0);
        assert!(
            block_cache.is_empty(),
            "pruning a reader should evict its dead SST blocks"
        );
        Ok(())
    }

    #[test]
    fn should_derive_distinct_cache_ids_for_same_name_when_manifest_identity_differs() {
        // Arrange
        let first = FileMeta {
            name: "same.sst".to_string(),
            cf_id: 1,
            sst_seq: 10,
            size_bytes: 128,
            content_crc32c: Some(1),
            ..Default::default()
        };
        let second = FileMeta {
            name: "same.sst".to_string(),
            cf_id: 2,
            sst_seq: 10,
            size_bytes: 128,
            content_crc32c: Some(1),
            ..Default::default()
        };

        // Act
        let first_id = ReadResources::sst_id_for_file(&first);
        let second_id = ReadResources::sst_id_for_file(&second);

        // Assert
        assert_ne!(first_id, second_id);
    }

    #[test]
    fn should_not_reuse_reader_when_same_name_has_different_manifest_identity() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let first_meta = write_test_sst(&temp_dir, "same-name.sst")?;
        let second_meta = FileMeta {
            cf_id: first_meta.cf_id + 1,
            ..first_meta.clone()
        };
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );

        // Act
        let first_reader = resources.reader_for(&first_meta)?;
        let second_reader = resources.reader_for(&second_meta)?;

        // Assert
        assert!(!Arc::ptr_eq(&first_reader, &second_reader));
        assert_eq!(resources.cached_reader_count(), 2);
        Ok(())
    }
}
