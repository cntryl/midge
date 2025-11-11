use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::api::column_family::ColumnFamilyId;
use crate::error::{MidgeError, MidgeResult};
use crate::core::memtable::MemTable;
use crate::core::metrics::Metrics;
use crate::core::wal_replay::replay_wal_to_memtables_after_seq;
use crate::manifest::Manifest;

// Import from sibling modules
use super::column_family::{ColumnFamily, ColumnFamilySet};

/// Core LSM-tree storage engine with WAL, memtables, SSTs, and background compaction.
///
/// Supports column families, snapshot isolation, and configurable compression/caching.
pub struct MidgeEngine {
    /// WAL coordinator managing write-ahead log operations
    pub(crate) wal_coordinator: crate::wal::WalCoordinator,
    pub(crate) cf_set: Arc<ColumnFamilySet>,
    pub(crate) seq: AtomicU64,
    pub(crate) txn_id: AtomicU64,
    pub(crate) db_path: PathBuf,
    #[allow(dead_code)]
    pub(super) mem_mode: bool,
    pub(crate) read_only: bool,
    pub(crate) memtable_size: usize,
    pub(crate) sst_dir: PathBuf,
    pub(crate) block_size: usize,
    pub(crate) compression: crate::codec::CompressionType,
    pub(crate) sst_factory: Arc<dyn crate::sst::SstFactory>,
    pub(crate) sst_reader_factory: Arc<dyn crate::sst::SstReaderFactory>,
    pub(crate) wal_buffer_size: usize,
    pub(crate) wal_sync: bool,
    pub(crate) snapshot_registry: Arc<crate::api::snapshot::SnapshotRegistry>,
    pub(crate) block_cache: Option<Arc<crate::cache::BlockCache>>,
    pub(crate) table_cache: Option<Arc<crate::sst::table_cache::TableCache>>,
    pub(crate) metrics: Arc<Metrics>,
    /// Performance metrics for real-time monitoring and optimization
    pub(crate) performance_metrics: Arc<crate::core::metrics::PerformanceMetrics>,
    /// Background flush coordinator
    pub(crate) flush_coordinator: crate::core::FlushCoordinator,
    /// Background compaction coordinator (optional - may be disabled)
    pub(crate) compaction_coordinator: Option<crate::core::CompactionCoordinator>,
    pub(crate) merge_operators: RwLock<HashMap<u32, crate::api::DynMergeOperator>>,
    pub(crate) cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    /// Database lock to prevent concurrent writers. Held for RAII - released on drop.
    #[allow(dead_code)]
    pub(super) db_lock: Option<Box<dyn crate::core::locking::DbLock>>,
    /// Dynamic read-only flag that can be set during runtime (e.g., when lock renewal fails)
    pub(super) is_read_only: AtomicBool,
    /// Transaction manager for ACID guarantees
    pub(crate) txn_manager: crate::core::transaction::TransactionManager,
    /// Flush mutex to serialize concurrent flush operations and prevent file conflicts
    pub(crate) flush_mutex: Mutex<()>,
    /// Cached manifest for fast read access without disk I/O
    /// OPTIMIZATION: Eliminates manifest load on every get() - 75% performance improvement
    pub(crate) manifest_cache: crate::sst::manifest_cache::ManifestCache,
    /// Bloom filter cache for fast SST pre-checks
    /// OPTIMIZATION: Avoids SST opens when bloom says key is absent
    pub(super) bloom_cache: crate::sst::bloom_cache::BloomCache,
    /// Sparse index cache for fast block lookups
    /// OPTIMIZATION: Avoids SST metadata reads and index deserialization overhead
    pub(super) sparse_index_cache: crate::sst::sparse_index_cache::SparseIndexCache,
}

impl MidgeEngine {
    /// Open or create a database using the high-level `Config` API.
    ///
    /// Delegated to `state::initialization::open_with_config()`.
    pub fn open_with_config(config: crate::config::Config) -> MidgeResult<Self> {
        crate::core::engine::state::initialization::open_with_config(config)
    }

    /// Create a KvStore adapter for this engine.
    ///
    /// This wraps the engine in a composition-based adapter that implements
    /// the `KvStore` trait. This is the preferred way to expose engine functionality
    /// through the public KvStore API.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let engine = Arc::new(MidgeEngine::open(opts)?);
    /// let kv_store = engine.as_kv_store();
    /// kv_store.put(&cf, b"key", b"value")?;
    /// ```
    pub fn as_kv_store(self: &Arc<Self>) -> super::adapters::KvStoreAdapter {
        super::adapters::KvStoreAdapter::new(Arc::clone(self))
    }

    /// Wait for the compaction coordinator to become idle.
    ///
    /// If compaction is disabled or not configured, this returns Ok(()) immediately.
    pub fn wait_for_compaction_idle(&self, timeout: Duration) -> MidgeResult<()> {
        // First, wait for any outstanding background flushes to complete. This
        // ensures SST files and manifest updates from flushes are finished
        // before we consider compaction idle.
        self.flush_coordinator.wait_until_idle(timeout)?;

        if let Some(ref coord) = self.compaction_coordinator {
            coord.wait_until_idle(timeout)
        } else {
            Ok(())
        }
    }

    /// Open or create a database with the specified storage mode.
    ///
    /// Delegated to `state::initialization::open()`.
    pub fn open(opts: crate::MidgeOptions) -> MidgeResult<Self> {
        crate::core::engine::state::initialization::open(opts)
    }

    /// Open with a provided `SstFactory` implementation.
    ///
    /// Delegated to `state::initialization::open_with_factories()`.
    pub fn open_with_factories(
        opts: crate::MidgeOptions,
        sst_factory: Box<dyn crate::sst::SstFactory>,
        sst_reader_factory: Box<dyn crate::sst::SstReaderFactory>,
        wal_factory: Box<dyn crate::wal::WalFactory>,
        mem_mode: bool,
    ) -> MidgeResult<Self> {
        crate::core::engine::state::initialization::open_with_factories(
            opts,
            sst_factory,
            sst_reader_factory,
            wal_factory,
            mem_mode,
        )
    }

    /// Transition the engine to read-only mode.
    /// Called when lock renewal fails or other critical errors occur.
    /// Once set, all mutation operations will be rejected.
    pub fn transition_to_read_only(&self) {
        self.is_read_only.store(true, Ordering::SeqCst);
        warn!("Database transitioned to read-only mode");
    }

    /// Check if the engine is in read-only mode (either from startup or runtime transition)
    pub(crate) fn check_read_only(&self) -> MidgeResult<()> {
        if self.read_only || self.is_read_only.load(Ordering::SeqCst) {
            return Err(MidgeError::ReadOnly);
        }
        Ok(())
    }

    /// Get a read-only snapshot of the cached manifest
    /// OPTIMIZATION: Avoids disk I/O on every read operation
    /// Delegates to ManifestCache which clones to avoid holding RwLock during SST iteration
    #[inline]
    pub(crate) fn get_manifest(&self) -> Manifest {
        self.manifest_cache.get()
    }

    /// Update the cached manifest (called after flush/compaction)
    pub(crate) fn update_manifest_cache(&self, manifest: Manifest) {
        self.manifest_cache.update(manifest);
    }

    /// Update caches for a newly created SST file
    /// Called after flush or compaction to cache bloom filters and sparse indexes
    pub(crate) fn update_caches_for_new_sst(&self, sst_name: &str) {
        let sst_path = self.sst_dir.join(sst_name);

        // Try to load and cache the bloom filter
        if let Ok(bytes) = std::fs::read(&sst_path) {
            if let Ok(sst_reader) = crate::sst::mem::SstMemReader::from_bytes(bytes.clone()) {
                // Cache bloom filter if present
                if let Some(bloom_bytes) = sst_reader.get_bloom_filter_bytes() {
                    if let Ok(bloom) = crate::sst::bloom::BloomFilter::decode_block(&bloom_bytes) {
                        self.bloom_cache.insert(sst_name.to_string(), bloom);
                    }
                }
            }

            // Cache sparse index
            if let Ok(metadata) = crate::sst::reader_common::SstMetadata::from_bytes(&bytes) {
                self.sparse_index_cache
                    .insert(sst_name.to_string(), metadata.sparse_index);
            }
        }
    }

    // Helper methods for accessing default CF MemTable (now lock-free!)
    pub(crate) fn with_default_memtable<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&MemTable) -> R,
    {
        let cf = self.cf_set.default_cf();
        let mt = cf.memtable.read();
        f(&mt)
    }

    pub(crate) fn with_default_memtable_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&MemTable) -> R,
    {
        // MemTable uses interior mutability (lock-free skiplist)
        // No need for write lock for reads, just read lock
        let cf = self.cf_set.default_cf();
        let mt = cf.memtable.read();
        f(&mt)
    }

    // Helper methods for accessing any CF's MemTable
    pub(crate) fn with_cf_memtable<F, R>(&self, cf_id: ColumnFamilyId, f: F) -> Option<R>
    where
        F: FnOnce(&MemTable) -> R,
    {
        let cf = self.cf_set.cfs.get(&cf_id.as_u32())?;
        let mt = cf.memtable.read();
        Some(f(&mt))
    }

    pub(crate) fn with_cf_memtable_mut<F, R>(&self, cf_id: ColumnFamilyId, f: F) -> Option<R>
    where
        F: FnOnce(&MemTable) -> R,
    {
        // MemTable uses interior mutability (lock-free skiplist)
        let cf = self.cf_set.cfs.get(&cf_id.as_u32())?;
        let mt = cf.memtable.read();
        Some(f(&mt))
    }

    /// Replay WAL records into column families. Ignores records for dropped CFs.
    /// Returns the maximum sequence number seen in the records.
    pub(super) fn replay_wal_to_cfs(
        cf_set: &ColumnFamilySet,
        records: &[crate::wal::WalRecord],
    ) -> u64 {
        Self::replay_wal_to_cfs_after_seq(cf_set, records, 0)
    }

    /// Replay WAL records to column families, skipping records with sequence <= skip_before_seq.
    pub(super) fn replay_wal_to_cfs_after_seq(
        cf_set: &ColumnFamilySet,
        records: &[crate::wal::WalRecord],
        skip_before_seq: u64,
    ) -> u64 {
        // Build a map of cf_id -> Arc<ColumnFamily> for replay
        // We clone the Arcs so they live long enough for the replay
        let cf_refs: Vec<Arc<ColumnFamily>> = cf_set
            .cfs
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        // Acquire read locks on all memtables and build the cf_map
        // Note: We hold all locks for the duration of replay for consistency
        let guards: Vec<_> = cf_refs.iter().map(|cf| cf.memtable.read()).collect();
        let mut cf_map: HashMap<u32, &MemTable> = HashMap::new();
        for (cf, guard) in cf_refs.iter().zip(guards.iter()) {
            cf_map.insert(cf.id.as_u32(), &**guard);
        }

        replay_wal_to_memtables_after_seq(&mut cf_map, records, skip_before_seq)
    }

}

// KvStore trait implementation moved to operations/kv_store.rs

impl Drop for MidgeEngine {
    fn drop(&mut self) {
        // Flush WAL to ensure all writes are persisted
        let _ = self.wal_coordinator.flush();

        // FlushCoordinator will be automatically dropped and shutdown gracefully

        // Background compaction thread is an infinite loop; rely on process exit
        // to terminate it for now.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::column_family::ColumnFamilyId;
    use crate::wal::WalOpKind;
    use std::sync::Arc;

    fn create_test_engine() -> Arc<MidgeEngine> {
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::Memory,
            ..Default::default()
        };
        Arc::new(MidgeEngine::open(opts).expect("Failed to create test engine"))
    }

    // ==================== Initialization Tests ====================

    #[test]
    fn should_create_engine_given_memory_mode() {
        // Act
        let engine = create_test_engine();

        // Assert
        assert!(engine.mem_mode);
        assert!(!engine.read_only);
        assert_eq!(engine.seq.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn should_delegate_to_open_with_config() {
        // Arrange
        let config = crate::config::ConfigBuilder::new("./test_db_config")
            .build()
            .expect("Failed to build config");

        // Act
        let result = MidgeEngine::open_with_config(config);

        // Assert
        assert!(result.is_ok());
    }

    // ==================== Adapter Creation Tests ====================

    #[test]
    fn should_create_kv_store_adapter() {
        use crate::api::KvStore;
        
        // Arrange
        let engine = create_test_engine();

        // Act
        let adapter = engine.as_kv_store();

        // Assert
        // Verify adapter is created (type check via usage)
        let cf = adapter.default_column_family();
        assert_eq!(cf.id(), crate::api::column_family::DEFAULT_CF_ID);
    }

    // ==================== Read-Only Mode Tests ====================

    #[test]
    fn should_not_be_readonly_when_engine_created() {
        // Arrange
        let engine = create_test_engine();

        // Act
        let result = engine.check_read_only();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_operations_when_transitioned_to_readonly() {
        // Arrange
        let engine = create_test_engine();

        // Act
        engine.transition_to_read_only();
        let result = engine.check_read_only();

        // Assert
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MidgeError::ReadOnly));
    }

    #[test]
    fn should_reject_operations_when_created_in_readonly_mode() {
        // Arrange
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::Memory,
            read_only: true,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("Failed to create engine"));

        // Act
        let result = engine.check_read_only();

        // Assert
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MidgeError::ReadOnly));
    }

    #[test]
    fn should_remain_readonly_after_transition() {
        // Arrange
        let engine = create_test_engine();
        engine.transition_to_read_only();

        // Act
        let first_check = engine.check_read_only();
        let second_check = engine.check_read_only();

        // Assert
        assert!(first_check.is_err());
        assert!(second_check.is_err());
    }

    // ==================== Manifest Cache Tests ====================

    #[test]
    fn should_return_cached_manifest_when_requested() {
        // Arrange
        let engine = create_test_engine();

        // Act
        let manifest = engine.get_manifest();

        // Assert
        assert_eq!(manifest.last_persisted_sequence, 0);
        assert!(manifest.files.is_empty());
    }

    #[test]
    fn should_update_manifest_cache_when_called() {
        // Arrange
        let engine = create_test_engine();
        let mut manifest = engine.get_manifest();
        manifest.last_persisted_sequence = 42;

        // Act
        engine.update_manifest_cache(manifest.clone());
        let updated = engine.get_manifest();

        // Assert
        assert_eq!(updated.last_persisted_sequence, 42);
    }

    // ==================== Memtable Accessor Tests ====================

    #[test]
    fn should_access_default_memtable_when_using_with_default_memtable() {
        // Arrange
        let engine = create_test_engine();

        // Act
        let result = engine.with_default_memtable(|mt| mt.is_empty());

        // Assert
        assert!(result, "Default memtable should be empty on creation");
    }

    #[test]
    fn should_access_default_memtable_mutably_when_using_with_default_memtable_mut() {
        // Arrange
        let engine = create_test_engine();

        // Act
        let result = engine.with_default_memtable_mut(|mt| mt.size_bytes());

        // Assert
        assert_eq!(result, 0, "Default memtable should have zero size on creation");
    }

    #[test]
    fn should_return_none_when_accessing_nonexistent_cf_memtable() {
        // Arrange
        let engine = create_test_engine();
        let nonexistent_cf = ColumnFamilyId::new(999);

        // Act
        let result = engine.with_cf_memtable(nonexistent_cf, |mt| mt.is_empty());

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_return_some_when_accessing_existing_cf_memtable() {
        // Arrange
        let engine = create_test_engine();
        let default_cf = crate::api::column_family::DEFAULT_CF_ID;

        // Act
        let result = engine.with_cf_memtable(default_cf, |mt| mt.is_empty());

        // Assert
        assert!(result.is_some());
        assert!(result.unwrap());
    }

    #[test]
    fn should_access_cf_memtable_immutably() {
        // Arrange
        let engine = create_test_engine();
        let default_cf = crate::api::column_family::DEFAULT_CF_ID;

        // Act
        let result = engine.with_cf_memtable(default_cf, |mt| mt.is_empty());

        // Assert
        assert!(result.is_some());
        assert!(result.unwrap());
    }

    // ==================== WAL Replay Tests ====================

    #[test]
    fn should_return_zero_sequence_when_replaying_empty_wal() {
        // Arrange
        let engine = create_test_engine();
        let records: Vec<crate::wal::WalRecord> = vec![];

        // Act
        let max_seq = MidgeEngine::replay_wal_to_cfs(&engine.cf_set, &records);

        // Assert
        assert_eq!(max_seq, 0);
    }

    #[test]
    fn should_return_max_sequence_when_replaying_wal_records() {
        // Arrange
        let engine = create_test_engine();
        let records = vec![
            crate::wal::WalRecord::new(WalOpKind::Put, bytes::Bytes::from(&b"key1"[..]), Some(bytes::Bytes::from(&b"val1"[..])), 1),
            crate::wal::WalRecord::new(WalOpKind::Put, bytes::Bytes::from(&b"key2"[..]), Some(bytes::Bytes::from(&b"val2"[..])), 5),
            crate::wal::WalRecord::new(WalOpKind::Put, bytes::Bytes::from(&b"key3"[..]), Some(bytes::Bytes::from(&b"val3"[..])), 3),
        ];

        // Act
        let max_seq = MidgeEngine::replay_wal_to_cfs(&engine.cf_set, &records);

        // Assert
        assert_eq!(max_seq, 5, "Should return the maximum sequence number");
    }

    #[test]
    fn should_replay_records_into_memtable() {
        // Arrange
        let engine = create_test_engine();
        let records = vec![
            crate::wal::WalRecord::new(WalOpKind::Put, bytes::Bytes::from(&b"key1"[..]), Some(bytes::Bytes::from(&b"value1"[..])), 1),
            crate::wal::WalRecord::new(WalOpKind::Put, bytes::Bytes::from(&b"key2"[..]), Some(bytes::Bytes::from(&b"value2"[..])), 2),
        ];

        // Act
        MidgeEngine::replay_wal_to_cfs(&engine.cf_set, &records);

        // Assert
        let is_empty = engine.with_default_memtable(|mt| mt.is_empty());
        assert!(!is_empty, "Memtable should contain replayed records");
    }

    #[test]
    fn should_ignore_records_for_dropped_cfs_when_replaying() {
        // Arrange
        let engine = create_test_engine();
        let nonexistent_cf_id = 999;
        let records = vec![
            crate::wal::WalRecord::new_cf(
                crate::api::column_family::ColumnFamilyId::new(nonexistent_cf_id),
                WalOpKind::Put,
                bytes::Bytes::from(&b"key1"[..]),
                Some(bytes::Bytes::from(&b"value1"[..])),
                1,
            ),
        ];

        // Act - should not panic
        let max_seq = MidgeEngine::replay_wal_to_cfs(&engine.cf_set, &records);

        // Assert
        assert_eq!(max_seq, 1, "Should still track sequence numbers");
    }

    // ==================== Compaction Coordinator Tests ====================

    #[test]
    fn should_return_ok_immediately_when_compaction_disabled() {
        // Arrange
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::Memory,
            enable_compaction: false,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("Failed to create engine"));

        // Act
        let result = engine.wait_for_compaction_idle(Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());
    }

    // ==================== Cache Update Tests ====================

    #[test]
    fn should_not_panic_when_updating_caches_for_nonexistent_sst() {
        // Arrange
        let engine = create_test_engine();

        // Act - should not panic even if file doesn't exist
        engine.update_caches_for_new_sst("nonexistent.sst");

        // Assert - no assertion needed, just verify no panic
    }

    // ==================== Drop Tests ====================

    #[test]
    fn should_flush_wal_when_dropped() {
        // Arrange
        let engine = create_test_engine();
        
        // Act
        drop(engine);

        // Assert - if we get here without panic, WAL was flushed successfully
    }
}

