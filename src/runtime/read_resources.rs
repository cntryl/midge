//! Shared immutable read resources for runtime snapshots.

use crate::common::resource_budget::ResourceBudget;
use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::metadata::FileMeta;
use crate::sst::cache::{BlockCache, CachePolicyType};
use crate::sst::fs::SstFileIo;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

mod admission;
use admission::{OpenAdmission, OpenAttempt};

pub(crate) struct ReadResources {
    sst_fs: Arc<dyn Fs>,
    sst_path_prefix: PathBuf,
    block_cache: Arc<BlockCache>,
    readers: Mutex<HashMap<ReaderCacheKey, CachedReader>>,
    metadata_budget: ResourceBudget,
    reader_admission: OpenAdmission,
    access_clock: std::sync::atomic::AtomicU64,
    diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
}

struct CachedReader {
    reader: Arc<SstFileIo>,
    last_used: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReaderCacheKey {
    name: String,
    sst_id: u64,
}

impl ReadResources {
    #[cfg(test)]
    pub(crate) fn new(
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: PathBuf,
        block_cache_size: usize,
        block_cache_policy: CachePolicyType,
    ) -> Self {
        Self::new_with_diagnostics(
            sst_fs,
            sst_path_prefix,
            block_cache_size,
            block_cache_policy,
            crate::diagnostics::legacy_runtime_diagnostics(),
        )
    }

    pub(crate) fn new_with_diagnostics(
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: PathBuf,
        block_cache_size: usize,
        block_cache_policy: CachePolicyType,
        diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
    ) -> Self {
        let sst_fs = sst_fs
            .with_read_observer(diagnostics.clone())
            .unwrap_or(sst_fs);
        Self {
            sst_fs,
            sst_path_prefix,
            block_cache: Arc::new(BlockCache::new(
                u64::try_from(block_cache_size.saturating_sub(block_cache_size / 4))
                    .unwrap_or(u64::MAX),
                16,
                block_cache_policy,
            )),
            readers: Mutex::new(HashMap::new()),
            metadata_budget: ResourceBudget::new(block_cache_size / 4),
            reader_admission: OpenAdmission::new(block_cache_size / 4),
            access_clock: std::sync::atomic::AtomicU64::new(0),
            diagnostics,
        }
    }

    pub(crate) fn diagnostics(&self) -> Arc<crate::diagnostics::RuntimeDiagnostics> {
        Arc::clone(&self.diagnostics)
    }

    pub(crate) fn reader_for(&self, file_meta: &FileMeta) -> MidgeResult<Arc<SstFileIo>> {
        self.reader_for_with_metrics(file_meta, true)
    }

    fn reader_for_with_metrics(
        &self,
        file_meta: &FileMeta,
        record_metrics: bool,
    ) -> MidgeResult<Arc<SstFileIo>> {
        let name = file_meta.name.clone();
        let sst_id = Self::sst_id_for_file(file_meta);
        let cache_key = ReaderCacheKey {
            name: name.clone(),
            sst_id,
        };
        if let Some(reader) = self.cached_reader(&cache_key)? {
            if record_metrics {
                self.diagnostics.sst_metrics().record_reader_cache_hit();
            }
            return Ok(reader);
        }
        let attempt = loop {
            match self
                .reader_admission
                .begin(&cache_key, &self.metadata_budget)
            {
                Err(error @ MidgeError::ResourceLimit(_)) => {
                    if !self.evict_idle_reader()? {
                        return Err(error);
                    }
                }
                result => break result?,
            }
        };
        let owner = match attempt {
            OpenAttempt::Owner(owner) => owner,
            OpenAttempt::Shared(pending) => return pending.wait(),
        };
        // A completed owner may have populated the cache between the initial
        // lookup and our admission. Keep that exact reader and avoid extra I/O.
        let result = self.cached_reader(&cache_key).and_then(|cached| {
            if let Some(reader) = cached {
                Ok(reader)
            } else {
                if record_metrics {
                    self.diagnostics.sst_metrics().record_reader_cache_miss();
                }
                self.open_reader(file_meta, cache_key)
            }
        });
        owner.complete(&result);
        result
    }

    fn open_reader(
        &self,
        file_meta: &FileMeta,
        cache_key: ReaderCacheKey,
    ) -> MidgeResult<Arc<SstFileIo>> {
        let sst_path = self.sst_path_prefix.join(&file_meta.name);
        let path_str = sst_path.to_string_lossy().to_string();
        let opened = loop {
            match SstFileIo::open_for_compaction(
                &path_str,
                Arc::clone(&self.sst_fs),
                self.metadata_budget.clone(),
            ) {
                Ok(reader) => break reader,
                Err(error @ MidgeError::ResourceLimit(_)) => {
                    if !self.evict_idle_reader()? {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        };
        if file_meta.size_bytes != 0 && opened.file_size() != file_meta.size_bytes {
            return Err(MidgeError::Corruption(format!(
                "SST '{}' size differs from its manifest",
                file_meta.name
            )));
        }
        let reader = Arc::new(
            opened
                .with_block_cache(Arc::clone(&self.block_cache), cache_key.sst_id)
                .with_read_path_diagnostics(Arc::clone(&self.diagnostics)),
        );
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| MidgeError::Internal("SST reader cache lock poisoned".into()))?;
        readers.insert(
            cache_key,
            CachedReader {
                reader: Arc::clone(&reader),
                last_used: self
                    .access_clock
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            },
        );
        Ok(reader)
    }

    fn evict_idle_reader(&self) -> MidgeResult<bool> {
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| MidgeError::Internal("SST reader cache lock poisoned".into()))?;
        let oldest = readers
            .iter()
            .filter(|(_, cached)| Arc::strong_count(&cached.reader) == 1)
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(key, _)| key.clone());
        Ok(oldest.is_some_and(|oldest| readers.remove(&oldest).is_some()))
    }

    fn cached_reader(&self, key: &ReaderCacheKey) -> MidgeResult<Option<Arc<SstFileIo>>> {
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| MidgeError::Internal("SST reader cache lock poisoned".into()))?;
        Ok(readers.get_mut(key).map(|cached| {
            cached.last_used = self
                .access_clock
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Arc::clone(&cached.reader)
        }))
    }

    pub(crate) fn sst_fs(&self) -> Arc<dyn Fs> {
        Arc::clone(&self.sst_fs)
    }

    pub(crate) fn prune_to_live_ssts(&self, live_names: &HashSet<String>) {
        if let Ok(mut readers) = self.readers.lock() {
            let stale_sst_ids: Vec<u64> = readers
                .iter()
                .filter_map(|(key, reader)| {
                    (!live_names.contains(&key.name)).then_some(reader.reader.sst_id())
                })
                .collect();
            readers.retain(|key, _reader| live_names.contains(&key.name));
            drop(readers);

            for sst_id in stale_sst_ids {
                let _ = self.block_cache.remove_sst(sst_id);
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
mod concurrency_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::{SstFactory, SstStateReader};

    pub(super) fn write_test_sst(
        temp_dir: &tempfile::TempDir,
        name: &str,
    ) -> MidgeResult<FileMeta> {
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
    fn should_evict_reader_metadata_when_live_inventory_exceeds_cache_budget() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
            PathBuf::new(),
            64 * 1024,
            CachePolicyType::Lru,
        );

        // Act
        for id in 0..100 {
            let meta = write_test_sst(&temp_dir, &format!("bounded-{id}.sst"))?;
            let reader = resources.reader_for(&meta)?;
            assert!(matches!(
                reader.get_state_at(b"key", u64::MAX)?,
                crate::sst::types::KeyState::Value(_, 7, None, 0)
            ));
        }

        // Assert
        assert!(resources.cached_reader_count() < 100);
        Ok(())
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
