//! Shared immutable read resources for runtime snapshots.

use crate::common::resource_budget::ResourceBudget;
use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::metadata::FileMeta;
use crate::sst::cache::{BlockCache, CachePolicyType};
use crate::sst::fs::SstFileIo;
#[cfg(test)]
use crate::types::EntryType;
use dashmap::DashMap;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

mod admission;
use crate::sst::read_path_metrics::SstReadObserver as _;
use admission::{OpenAdmission, OpenAttempt};

pub(crate) struct ReadResources {
    sst_fs: Arc<dyn Fs>,
    sst_path_prefix: PathBuf,
    block_cache: Arc<BlockCache>,
    /// Sharded so concurrent point reads do not serialize on one lock; a hit
    /// is looked up by the manifest entry's borrowed fields without copying
    /// its name (#919).
    readers: DashMap<ReaderCacheKey, CachedReader>,
    /// The block-cache id of every live manifest entry. It outlives reader
    /// eviction, so a reopened reader finds its warm blocks and pruning a dead
    /// SST purges its blocks even when its reader was already evicted under
    /// metadata pressure.
    block_cache_ids: Mutex<HashMap<ReaderCacheKey, u64>>,
    metadata_budget: ResourceBudget,
    reader_admission: OpenAdmission,
    access_clock: std::sync::atomic::AtomicU64,
    /// Source of block-cache identities: an entry takes the next value the
    /// first time it opens and keeps it until pruned, so no two live entries
    /// ever share one and a pruned entry's id is never reissued (#917).
    next_sst_id: std::sync::atomic::AtomicU64,
    diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
    #[cfg(test)]
    prune_calls: std::sync::atomic::AtomicUsize,
}

struct CachedReader {
    reader: Arc<SstFileIo>,
    last_used: std::sync::atomic::AtomicU64,
}

/// The manifest identity a cached reader was opened for. A same-named entry
/// with a different family, size, or checksum is a different file and gets its
/// own reader; the fields are compared structurally, never through a digest.
#[derive(Debug, Clone)]
struct ReaderCacheKey {
    name: String,
    cf_id: u32,
    size_bytes: u64,
    content_crc32c: Option<u32>,
}

impl ReaderCacheKey {
    fn for_file(file_meta: &FileMeta) -> Self {
        Self {
            name: file_meta.name.clone(),
            cf_id: file_meta.cf_id,
            size_bytes: file_meta.size_bytes,
            content_crc32c: file_meta.content_crc32c,
        }
    }
}

/// The identity fields a reader-cache lookup compares. A `FileMeta` is one
/// directly, so a hit never allocates a key; the owned `ReaderCacheKey` is built
/// only when a reader is opened.
trait ReaderKey {
    fn name(&self) -> &str;
    fn cf_id(&self) -> u32;
    fn size_bytes(&self) -> u64;
    fn content_crc32c(&self) -> Option<u32>;
}

impl ReaderKey for ReaderCacheKey {
    fn name(&self) -> &str {
        &self.name
    }
    fn cf_id(&self) -> u32 {
        self.cf_id
    }
    fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    fn content_crc32c(&self) -> Option<u32> {
        self.content_crc32c
    }
}

impl ReaderKey for FileMeta {
    fn name(&self) -> &str {
        &self.name
    }
    fn cf_id(&self) -> u32 {
        self.cf_id
    }
    fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    fn content_crc32c(&self) -> Option<u32> {
        self.content_crc32c
    }
}

// `Hash`/`Eq` live on the trait object so an owned key and a borrowed
// `FileMeta` hash and compare identically, which `Borrow` requires.
impl Hash for dyn ReaderKey + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.cf_id().hash(state);
        self.size_bytes().hash(state);
        self.content_crc32c().hash(state);
    }
}

impl PartialEq for dyn ReaderKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.cf_id() == other.cf_id()
            && self.size_bytes() == other.size_bytes()
            && self.content_crc32c() == other.content_crc32c()
    }
}

impl Eq for dyn ReaderKey + '_ {}

impl Hash for ReaderCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as &dyn ReaderKey).hash(state);
    }
}

impl PartialEq for ReaderCacheKey {
    fn eq(&self, other: &Self) -> bool {
        (self as &dyn ReaderKey) == (other as &dyn ReaderKey)
    }
}

impl Eq for ReaderCacheKey {}

impl<'a> Borrow<dyn ReaderKey + 'a> for ReaderCacheKey {
    fn borrow(&self) -> &(dyn ReaderKey + 'a) {
        self
    }
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
            Arc::new(crate::diagnostics::RuntimeDiagnostics::default()),
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
            readers: DashMap::new(),
            block_cache_ids: Mutex::new(HashMap::new()),
            metadata_budget: ResourceBudget::new(block_cache_size / 4),
            reader_admission: OpenAdmission::new(block_cache_size / 4),
            access_clock: std::sync::atomic::AtomicU64::new(0),
            next_sst_id: std::sync::atomic::AtomicU64::new(1),
            diagnostics,
            #[cfg(test)]
            prune_calls: std::sync::atomic::AtomicUsize::new(0),
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
        if let Some(reader) = self.cached_reader(file_meta) {
            if record_metrics {
                self.diagnostics.sst_metrics().record_reader_cache_hit();
            }
            return Ok(reader);
        }
        let cache_key = ReaderCacheKey::for_file(file_meta);
        let attempt = loop {
            match self
                .reader_admission
                .begin(&cache_key, &self.metadata_budget)
            {
                Err(error @ MidgeError::ResourceLimit(_)) => {
                    if !self.evict_idle_reader() {
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
        let result = if let Some(reader) = self.cached_reader(&cache_key) {
            Ok(reader)
        } else {
            if record_metrics {
                self.diagnostics.sst_metrics().record_reader_cache_miss();
            }
            self.open_reader(file_meta, cache_key)
        };
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
                    if !self.evict_idle_reader() {
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
        // Ids start at 1; 0 marks a reader opened without a block cache.
        let sst_id = *self
            .block_cache_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(cache_key.clone())
            .or_insert_with(|| {
                self.next_sst_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            });
        let reader = Arc::new(
            opened
                .with_block_cache(Arc::clone(&self.block_cache), sst_id)
                .with_read_path_diagnostics(self.diagnostics.clone()),
        );
        self.readers.insert(
            cache_key,
            CachedReader {
                reader: Arc::clone(&reader),
                last_used: std::sync::atomic::AtomicU64::new(
                    self.access_clock
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                ),
            },
        );
        Ok(reader)
    }

    /// Drop the least recently used reader nobody holds. Readers are shared
    /// through `Arc`s, so an evicted reader stays usable by in-flight reads and
    /// its file handle closes when the last one finishes.
    fn evict_idle_reader(&self) -> bool {
        let oldest = self
            .readers
            .iter()
            .filter(|entry| Arc::strong_count(&entry.value().reader) == 1)
            .min_by_key(|entry| {
                entry
                    .value()
                    .last_used
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .map(|entry| entry.key().clone());
        oldest.is_some_and(|oldest| {
            self.readers
                .remove_if(&oldest, |_, cached| Arc::strong_count(&cached.reader) == 1)
                .is_some()
        })
    }

    /// A cache hit takes one shard's read lock and never copies the key.
    fn cached_reader(&self, key: &dyn ReaderKey) -> Option<Arc<SstFileIo>> {
        self.readers.get(key).map(|cached| {
            cached.last_used.store(
                self.access_clock
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            Arc::clone(&cached.reader)
        })
    }

    #[cfg(test)]
    pub(crate) fn prune_call_count(&self) -> usize {
        self.prune_calls.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn prune_to_live_ssts(&self, live_names: &HashSet<String>) {
        #[cfg(test)]
        self.prune_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // A poisoned lock only means another thread panicked mid-update; the
        // maps stay structurally valid, and skipping the prune would leak
        // dead readers and blocks.
        self.readers
            .retain(|key, _reader| live_names.contains(&key.name));
        let mut stale_sst_ids = Vec::new();
        self.block_cache_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key, sst_id| {
                let live = live_names.contains(&key.name);
                if !live {
                    stale_sst_ids.push(*sst_id);
                }
                live
            });
        for sst_id in stale_sst_ids {
            let _ = self.block_cache.remove_sst(sst_id);
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_reader_count(&self) -> usize {
        self.readers.len()
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
    use crate::io::FsError;
    use crate::sst::traits::{SstFactory, SstStateReader};

    pub(super) fn write_test_sst(
        temp_dir: &tempfile::TempDir,
        name: &str,
    ) -> MidgeResult<FileMeta> {
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 7, EntryType::Put, None)?;
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
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
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
                crate::types::KeyState::Value(_, 7, None, crate::types::EntryType::Put)
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
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
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
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );
        let reader = resources.reader_for(&file_meta)?;
        let block_cache = resources.block_cache();
        assert!(matches!(
            reader.get_state_at(b"key", u64::MAX)?,
            crate::types::KeyState::Value(_, 7, None, crate::types::EntryType::Put)
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
    fn should_purge_block_cache_for_dead_sst_when_reader_was_evicted_before_prune(
    ) -> MidgeResult<()> {
        // Arrange: metadata pressure evicted the reader while its blocks stay
        // cached, then compaction deletes the SST.
        let temp_dir = tempfile::tempdir()?;
        let file_meta = write_test_sst(&temp_dir, "evicted-then-deleted.sst")?;
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );
        let reader = resources.reader_for(&file_meta)?;
        assert!(matches!(
            reader.get_state_at(b"key", u64::MAX)?,
            crate::types::KeyState::Value(_, 7, None, crate::types::EntryType::Put)
        ));
        drop(reader);
        let block_cache = resources.block_cache();
        assert!(!block_cache.is_empty());
        assert!(resources.evict_idle_reader());
        assert_eq!(resources.cached_reader_count(), 0);

        // Act
        resources.prune_to_live_ssts(&HashSet::new());

        // Assert
        assert!(
            block_cache.is_empty(),
            "a dead SST's blocks must be purged even after its reader was evicted"
        );
        Ok(())
    }

    #[test]
    fn should_match_owned_key_given_borrowed_manifest_entry_when_hashed_and_compared() {
        // Arrange: the owned key and the entry it was built from, plus an
        // entry that differs only in its checksum (#919).
        let entry = FileMeta {
            name: "lookup.sst".to_string(),
            cf_id: 3,
            size_bytes: 4096,
            content_crc32c: Some(0xdead_beef),
            ..Default::default()
        };
        let owned = ReaderCacheKey::for_file(&entry);
        let other = FileMeta {
            content_crc32c: Some(1),
            ..entry.clone()
        };
        let hash_of = |key: &dyn ReaderKey| -> u64 {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        };

        // Act
        let owned_as_borrowed: &dyn ReaderKey = Borrow::<dyn ReaderKey>::borrow(&owned);

        // Assert: a hit through the borrowed entry finds the owned key; a
        // different identity does not.
        assert_eq!(hash_of(owned_as_borrowed), hash_of(&entry));
        assert!(owned_as_borrowed == &entry as &dyn ReaderKey);
        assert!(owned_as_borrowed != &other as &dyn ReaderKey);
    }

    #[test]
    fn should_keep_block_cache_id_given_evicted_reader_when_reopened_before_prune(
    ) -> MidgeResult<()> {
        // Arrange: the reader is evicted under metadata pressure while its
        // manifest entry stays live, so its warm blocks must stay reachable.
        let temp_dir = tempfile::tempdir()?;
        let meta = write_test_sst(&temp_dir, "evicted.sst")?;
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );
        let first_id = resources.reader_for(&meta)?.block_cache_sst_id();
        assert!(resources.evict_idle_reader());
        assert_eq!(resources.cached_reader_count(), 0);

        // Act
        let second_id = resources.reader_for(&meta)?.block_cache_sst_id();

        // Assert
        assert_eq!(first_id, second_id);
        Ok(())
    }

    #[test]
    fn should_assign_fresh_block_cache_id_given_pruned_file_when_reopened() -> MidgeResult<()> {
        // Arrange: a reader is opened, pruned away, and the same manifest
        // entry is opened again (#917).
        let temp_dir = tempfile::tempdir()?;
        let meta = write_test_sst(&temp_dir, "reopen.sst")?;
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );
        let first_id = resources.reader_for(&meta)?.block_cache_sst_id();
        resources.prune_to_live_ssts(&HashSet::new());

        // Act
        let second_id = resources.reader_for(&meta)?.block_cache_sst_id();

        // Assert: a block-cache identity is never handed out twice, so a
        // stale block can never alias a reopened file, whatever its metadata.
        assert_ne!(first_id, second_id);
        Ok(())
    }

    #[test]
    fn should_assign_distinct_block_cache_ids_given_same_name_when_manifest_identity_differs(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let first = write_test_sst(&temp_dir, "same.sst")?;
        let second = FileMeta {
            cf_id: first.cf_id + 1,
            ..first.clone()
        };
        let resources = ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );

        // Act
        let first_id = resources.reader_for(&first)?.block_cache_sst_id();
        let second_id = resources.reader_for(&second)?.block_cache_sst_id();

        // Assert
        assert_ne!(first_id, second_id);
        Ok(())
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
            Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
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
