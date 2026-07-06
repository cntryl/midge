//! Shared immutable read resources for runtime snapshots.

use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::metadata::FileMeta;
use crate::sst::cache::BlockCache;
use crate::sst::fs::SstFileIo;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub(crate) struct ReadResources {
    sst_fs: Arc<dyn Fs>,
    sst_path_prefix: PathBuf,
    block_cache: Arc<BlockCache>,
    readers: Mutex<HashMap<String, Arc<SstFileIo>>>,
}

impl ReadResources {
    pub(crate) fn new(
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: PathBuf,
        block_cache_size: usize,
    ) -> Self {
        Self {
            sst_fs,
            sst_path_prefix,
            block_cache: Arc::new(BlockCache::new_default(
                u64::try_from(block_cache_size).unwrap_or(u64::MAX),
            )),
            readers: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn reader_for(&self, file_meta: &FileMeta) -> MidgeResult<Arc<SstFileIo>> {
        let name = file_meta.name.clone();
        if let Some(reader) = self
            .readers
            .lock()
            .map_err(|_| MidgeError::Internal("SST reader cache lock poisoned".into()))?
            .get(&name)
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
                .with_sst_id(Self::sst_id_for_name(&name))
                .with_block_cache(Arc::clone(&self.block_cache)),
        );

        let mut readers = self
            .readers
            .lock()
            .map_err(|_| MidgeError::Internal("SST reader cache lock poisoned".into()))?;
        if let Some(existing) = readers.get(&name).cloned() {
            return Ok(existing);
        }
        readers.insert(name, Arc::clone(&reader));
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
            readers.retain(|name, _reader| live_names.contains(name));
        }
    }

    fn sst_id_for_name(name: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        name.hash(&mut hasher);
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
    use crate::sst::traits::SstFactory;

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
}
