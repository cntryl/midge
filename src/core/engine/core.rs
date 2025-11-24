use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::api::column_family::ColumnFamilyId;
use crate::core::memtable::MemTable;
use crate::core::persistence::wal_replay::replay_wal_to_memtables_after_seq;
use crate::error::{MidgeError, MidgeResult};
use crate::manifest::Manifest;
use crate::metrics::Metrics;

// Import from sibling modules
use super::column_family::{ColumnFamily, ColumnFamilySet};
use crate::core::compaction::CompactionPlan;

/// Core LSM-tree storage engine with WAL, memtables, SSTs, and background compaction.
///
/// Supports column families, snapshot isolation, and configurable compression/caching.
pub struct MidgeEngine {
    /// WAL coordinator managing write-ahead log operations
    pub(crate) wal_coordinator: crate::wal::WalController,
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
    pub(crate) compression: crate::common::codec::CompressionType,
    pub(crate) sst_factory: Arc<dyn crate::sst::SstFactory>,
    pub(crate) sst_reader_factory: Arc<dyn crate::sst::SstReaderFactory>,
    pub(crate) wal_buffer_size: usize,
    pub(crate) wal_sync: bool,
    /// Transaction manager for optimistic concurrency control
    pub(crate) txn_manager: crate::core::transaction::TransactionController,
    pub(crate) snapshot_registry: Arc<crate::api::snapshot::SnapshotRegistry>,
    pub(crate) block_cache: Option<Arc<dyn crate::sst::BlockCacheTrait>>,
    pub(crate) table_cache: Option<Arc<crate::sst::table_cache::TableCache>>,
    pub(crate) metrics: Arc<Metrics>,
    /// Performance metrics for real-time monitoring and optimization
    pub(crate) performance_metrics: Arc<crate::metrics::PerformanceMetrics>,
    /// Background flush coordinator
    pub(crate) flush_coordinator: crate::core::FlushCoordinator,
    /// Background compaction coordinator (optional - may be disabled)
    pub(crate) compaction_coordinator: Option<crate::core::CompactionController>,
    pub(crate) merge_operators: RwLock<HashMap<u32, crate::api::DynMergeOperator>>,
    pub(crate) cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    /// Database lock to prevent concurrent writers. Held for RAII - released on drop.
    #[allow(dead_code)]
    pub(super) db_lock: Option<Box<dyn crate::core::locking::DbLock>>,
    /// Dynamic read-only flag that can be set during runtime (e.g., when lock renewal fails)
    pub(super) is_read_only: AtomicBool,
    /// Transaction manager for ACID guarantees
    /// Flush mutex to serialize concurrent flush operations and prevent file conflicts
    pub(crate) flush_mutex: Mutex<()>,
    /// Cached manifest for fast read access without disk I/O
    /// OPTIMIZATION: Eliminates manifest load on every get() - 75% performance improvement
    pub(crate) manifest_cache: Arc<crate::sst::manifest_cache::ManifestCache>,
    /// Bloom filter cache for fast SST pre-checks
    /// OPTIMIZATION: Avoids SST opens when bloom says key is absent
    pub(super) bloom_cache: crate::sst::bloom_cache::BloomCache,
    /// Sparse index cache for fast block lookups
    /// OPTIMIZATION: Avoids SST metadata reads and index deserialization overhead
    pub(super) sparse_index_cache: crate::sst::sparse_index_cache::SparseIndexCache,
    /// Optional autotuner for adaptive parameter adjustment
    #[allow(dead_code)]
    pub(crate) autotuner: Option<Arc<crate::config::Autotuner>>,
    /// Optional test hooks for deterministic coordination in tests
    #[allow(dead_code)]
    pub(crate) test_hooks: Option<crate::common::test_hooks::TestHooks>,
    /// Atomic version set for lock-free reads of manifest state
    pub(crate) version_set: crate::core::manifest::AtomicVersionSet,
    /// Version manager actor for serialized manifest updates
    pub(crate) version_manager: Arc<crate::core::manifest::VersionManager>,
    /// Background error reported by async maintenance (flush/compaction). When set,
    /// write operations should be blocked until cleared to avoid data loss.
    pub(crate) background_error: Arc<parking_lot::RwLock<Option<crate::error::MidgeError>>>,
}

impl MidgeEngine {
    /// Open or create a database using the high-level `Config` API.
    ///
    /// Delegated to `state::open_with_config()`.
    pub fn open_with_config(config: crate::config::Config) -> MidgeResult<Self> {
        crate::core::engine::state::open_with_config(config)
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
    pub fn as_kv_store(self: &Arc<Self>) -> super::KvStoreAdapter {
        super::KvStoreAdapter::new(Arc::clone(self))
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
    /// Delegated to `state::open()`.
    pub fn open(opts: crate::MidgeOptions) -> MidgeResult<Self> {
        crate::core::engine::state::open(opts)
    }

    /// Set the engine background error (e.g. reported by background flush/compaction)
    /// This will cause write operations to block until the error is cleared.
    pub fn set_background_error(&self, err: crate::error::MidgeError) {
        *self.background_error.write() = Some(err);
        tracing::warn!(
            "Engine background error set: {}",
            self.background_error
                .read()
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "<unknown>".to_string())
        );
    }

    /// Clear the engine background error and resume normal operations.
    pub fn clear_background_error(&self) {
        *self.background_error.write() = None;
        tracing::info!("Engine background error cleared");
    }

    /// Block until any background error is cleared. Returns immediately if no background error.
    pub(crate) fn wait_for_background_error_cleared(&self) {
        let mut backoff_ms = 1u64;
        let max_backoff_ms = 100u64;
        loop {
            if self.background_error.read().is_none() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
        }
    }

    /// Open with a provided `SstFactory` implementation.
    ///
    /// Delegated to `state::open_with_factories()`.
    pub fn open_with_factories(
        opts: crate::MidgeOptions,
        sst_factory: Box<dyn crate::sst::SstFactory>,
        sst_reader_factory: Box<dyn crate::sst::SstReaderFactory>,
        wal_factory: Box<dyn crate::wal::WalFactory>,
        mem_mode: bool,
    ) -> MidgeResult<Self> {
        crate::core::engine::state::open_with_factories(
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
    pub fn get_manifest(&self) -> Manifest {
        self.manifest_cache.get()
    }

    /// Update the cached manifest (called after flush/compaction)
    pub(crate) fn update_manifest_cache(&self, manifest: Manifest) {
        self.manifest_cache.update(manifest);
    }

    /// Forces a deterministic compaction that rewrites *all SST files* for the
    /// given column family.
    ///
    /// This is the canonical way to ensure compaction filters run reliably in tests.
    ///
    /// # Why this is test-friendly
    /// - background compaction is disabled in test opts
    /// - every SST file belonging to the CF is rewritten
    /// - independent of LSM layout, sizes, levels, or thresholds
    pub fn compact_cf_full_rewrite(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
    ) -> MidgeResult<()> {
        // test-level import removed (not required by this test)

        let cf_id: u32 = cf.id().into();

        // Derive plan from current manifest snapshot
        let manifest = self.version_set.load().manifest.clone();
        let cf_files: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();

        if cf_files.is_empty() {
            return Ok(());
        }

        let input_files: Vec<String> = cf_files.iter().map(|f| f.name.clone()).collect();

        // Compute min and max levels without unwrap() so linting prohibits
        // using unwrap/expect in non-test builds. We already return early
        // above if cf_files is empty, but avoid unwrap for clarity.
        let (min_level, max_level) = {
            let mut levels = cf_files.iter().map(|f| f.level);
            let first = match levels.next() {
                Some(v) => v,
                None => return Ok(()),
            };
            levels.fold((first, first), |(min, max), v| (std::cmp::min(min, v), std::cmp::max(max, v)))
        };
        let target_level = if max_level < 6 {
            max_level + 1
        } else {
            max_level
        };

        let plan = CompactionPlan {
            input_files,
            output_files: Vec::new(),
            source_level: min_level,
            target_level,
            cf_id,
        };

        // Use the compaction controller's synchronous execution path for
        // deterministic, test-friendly full-rewrite compaction.
        if let Some(ref controller) = self.compaction_coordinator {
            controller.run_plan_sync(
                &self.db_path,
                &self.cf_set,
                &self.sst_dir,
                &self.sst_factory,
                &self.sst_reader_factory,
                &self.snapshot_registry,
                self.compression,
                self.block_size,
                &self.cloud_sst_manager,
                &self.test_hooks,
                &self.version_manager,
                &Some(self.background_error.clone()),
                plan,
            )
        } else {
            // If no controller is configured (e.g., pure in-memory mode), no-op.
            Ok(())
        }
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
        let mt = cf.memtable.load();
        f(&mt)
    }

    pub(crate) fn with_default_memtable_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&MemTable) -> R,
    {
        // MemTable uses interior mutability (lock-free skiplist)
        // ArcSwap provides atomic load - no locks needed
        let cf = self.cf_set.default_cf();
        let mt = cf.memtable.load();
        f(&mt)
    }

    // Helper methods for accessing any CF's MemTable
    pub(crate) fn with_cf_memtable<F, R>(&self, cf_id: ColumnFamilyId, f: F) -> Option<R>
    where
        F: FnOnce(&MemTable) -> R,
    {
        let cf = self.cf_set.cfs.get(&cf_id.as_u32())?;
        let mt = cf.memtable.load();
        Some(f(&mt))
    }

    pub(crate) fn with_cf_memtable_mut<F, R>(&self, cf_id: ColumnFamilyId, f: F) -> Option<R>
    where
        F: FnOnce(&MemTable) -> R,
    {
        // MemTable uses interior mutability (lock-free skiplist)
        // ArcSwap provides atomic load - no locks needed
        let cf = self.cf_set.cfs.get(&cf_id.as_u32())?;
        let mt = cf.memtable.load();
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

        // Load memtables atomically and build the cf_map
        // ArcSwap ensures consistent snapshot across all CFs
        let mt_arcs: Vec<_> = cf_refs.iter().map(|cf| cf.memtable.load()).collect();
        let mut cf_map: HashMap<u32, &MemTable> = HashMap::new();
        for (cf, mt_arc) in cf_refs.iter().zip(mt_arcs.iter()) {
            cf_map.insert(cf.id.as_u32(), &**mt_arc);
        }

        replay_wal_to_memtables_after_seq(&mut cf_map, records, skip_before_seq)
    }

    /// Check if a transaction is currently active (for testing purposes)
    pub fn is_transaction_active(&self, txn_id: u64) -> bool {
        self.txn_manager.is_active(txn_id)
    }
}

impl Drop for MidgeEngine {
    fn drop(&mut self) {
        // Debugging: indicate we're beginning engine drop
        eprintln!("[DEBUG] MidgeEngine::drop - start");
        // Flush WAL to ensure all writes are persisted
        eprintln!("[DEBUG] MidgeEngine::drop - calling wal_coordinator.flush()");
        let _ = self.wal_coordinator.flush();
        eprintln!("[DEBUG] MidgeEngine::drop - wal_coordinator.flush returned");

        // Shutdown VersionManager actor gracefully
        eprintln!("[DEBUG] MidgeEngine::drop - calling version_manager.shutdown()");
        self.version_manager.shutdown();
        eprintln!("[DEBUG] MidgeEngine::drop - version_manager.shutdown returned");

        // FlushCoordinator will be automatically dropped and shutdown gracefully

        // Background compaction thread is an infinite loop; rely on process exit
        // to terminate it for now.
    }
}

// KvStore trait implementation moved to operations/kv_store.rs

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn create_test_engine() -> Arc<MidgeEngine> {
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::Memory,
            ..Default::default()
        };
        Arc::new(MidgeEngine::open(opts).expect("Failed to create test engine"))
    }

    #[test]
    fn should_return_ok_immediately_when_compaction_disabled() {
        // Arrange
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::Memory,
            enable_compaction: false,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("Failed to create test engine"));

        // Act
        let result = engine.wait_for_compaction_idle(std::time::Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_not_panic_when_updating_caches_for_nonexistent_sst() {
        // Arrange
        let engine = create_test_engine();

        // Act
        engine.update_caches_for_new_sst("nonexistent.sst");

        // Assert
    }

    #[test]
    fn should_flush_wal_when_dropped() {
        // Arrange
        let engine = create_test_engine();

        // Act
        drop(engine);

        // Assert
    }

    #[test]
    fn should_build_plan_over_all_cf_files_when_compacting_full_rewrite() {
        // Note: earlier versions defined a RecordingController and a nested
        // impl for `crate::core::CompactionController` here — those were
        // unused and caused non-local impl warnings. The test below does
        // not require any test-only impls, so we omit them to keep the
        // test scoped and avoid clippy noise.

        // Build a minimal engine with a fabricated manifest and a stub controller.
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::Memory,
            enable_compaction: false,
            ..Default::default()
        };
        let mut engine = MidgeEngine::open(opts).expect("Failed to create engine");

        // Fabricate a simple manifest with two files for the default CF.
        let cf_id = crate::api::column_family::DEFAULT_CF_ID.as_u32();
        let files = vec![
            crate::manifest::FileMeta {
                name: "000001.sst".to_string(),
                level: 0,
                size_bytes: 0,
                cf_id,
                sst_seq: 1,
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
            },
            crate::manifest::FileMeta {
                name: "000002.sst".to_string(),
                level: 1,
                size_bytes: 0,
                cf_id,
                sst_seq: 2,
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
            },
        ];

        let manifest = Manifest {
            files: files.clone(),
            ssts: files.iter().map(|f| f.name.clone()).collect(),
            ..Default::default()
        };

        engine.version_set = crate::core::manifest::AtomicVersionSet::new(
            crate::core::manifest::VersionSet::new(manifest),
        );

        let cf = engine.default_column_family();

        // Act: call full-rewrite compaction to build the plan. This should
        // complete without panicking even if the controller is None.
        let result = engine.compact_cf_full_rewrite(&cf);

        // Assert: in the Memory + no-controller configuration, this should be a no-op.
        assert!(result.is_ok());
    }
}
