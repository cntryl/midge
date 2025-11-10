//! Column family management for the storage engine.

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::api::column_family::{
    ColumnFamilyConfig, ColumnFamilyHandle, ColumnFamilyId, DEFAULT_CF_ID, DEFAULT_CF_NAME,
};
use crate::core::memtable::MemTable;
use crate::error::MidgeResult;

/// Per-column-family state.
///
/// Each column family maintains its own active MemTable, immutable memtables queue,
/// and SSTable hierarchy. Column families share the WAL and global sequence counter.
///
/// # Memtable Lifecycle
/// - **Active memtable**: Current writable memtable (wrapped in RwLock for atomic replacement)
/// - **Immutable memtables**: Bounded queue of frozen memtables waiting to be flushed
/// - **Max immutables**: Configured limit (`ColumnFamilyConfig.max_immutable_memtables`)
///
/// When active memtable exceeds size limit:
/// 1. Freeze active memtable (move to immutable queue)
/// 2. Create new empty active memtable
/// 3. Enqueue flush job for oldest immutable
/// 4. If immutable queue is full, stall writes until flush completes
pub(crate) struct ColumnFamily {
    pub(crate) id: ColumnFamilyId,
    pub(crate) name: String,
    pub(crate) config: ColumnFamilyConfig,

    /// Active writable memtable (wrapped for atomic replacement during freeze)
    pub(crate) memtable: Arc<RwLock<MemTable>>,

    /// Queue of frozen memtables waiting to be flushed (oldest first)
    pub(crate) immutable_memtables: Arc<Mutex<VecDeque<MemTable>>>,

    /// Current number of immutable memtables (cached for fast check without locking)
    pub(crate) immutable_count: AtomicUsize,
}

impl ColumnFamily {
    pub(crate) fn new(id: ColumnFamilyId, name: String, config: ColumnFamilyConfig) -> Self {
        Self {
            id,
            name,
            config,
            memtable: Arc::new(RwLock::new(MemTable::new())),
            immutable_memtables: Arc::new(Mutex::new(VecDeque::new())),
            immutable_count: AtomicUsize::new(0),
        }
    }

    pub(crate) fn handle(&self) -> ColumnFamilyHandle {
        ColumnFamilyHandle::new(self.id, self.name.clone())
    }

    /// Check if the active memtable has exceeded its size limit.
    pub(crate) fn is_full(&self) -> bool {
        let mt = self.memtable.read();
        mt.is_full(self.config.memtable_max_bytes)
    }

    /// Try to freeze the active memtable and create a new empty one.
    ///
    /// Returns `true` if successful, `false` if the immutable queue is full.
    /// When false is returned, writes should be stalled until a flush completes.
    pub(crate) fn try_freeze_memtable(&self) -> bool {
        // Check if immutable queue is already at capacity (fast path without locking)
        let current_count = self.immutable_count.load(Ordering::Acquire);
        if current_count >= self.config.max_immutable_memtables {
            return false; // Queue full, cannot freeze
        }

        // Lock immutable queue for the freeze operation
        let mut immutables = self.immutable_memtables.lock();

        // Double-check after acquiring lock (another thread may have added)
        if immutables.len() >= self.config.max_immutable_memtables {
            return false;
        }

        // Lock active memtable for replacement
        let mut mt_write = self.memtable.write();

        // Clone the old memtable (cheap Arc clone of inner skip-list)
        let old_memtable = (*mt_write).clone();

        // Replace with new empty memtable
        *mt_write = MemTable::new();

        // Release write lock before pushing to immutable queue
        drop(mt_write);

        // Add frozen memtable to immutable queue (oldest first, newest last)
        immutables.push_back(old_memtable);
        self.immutable_count.fetch_add(1, Ordering::Release);

        true
    }

    /// Pop the oldest immutable memtable for flushing.
    ///
    /// Returns `None` if the immutable queue is empty.
    ///
    /// TODO: This will be used in Phase 5 when implementing proper per-CF
    /// background flush coordination. Currently, flush directly accesses
    /// the immutable queue.
    #[allow(dead_code)]
    pub(crate) fn pop_immutable(&self) -> Option<MemTable> {
        let mut immutables = self.immutable_memtables.lock();
        if let Some(mt) = immutables.pop_front() {
            self.immutable_count.fetch_sub(1, Ordering::Release);
            Some(mt)
        } else {
            None
        }
    }

    /// Get the current number of immutable memtables waiting to be flushed.
    pub(crate) fn immutable_count(&self) -> usize {
        self.immutable_count.load(Ordering::Acquire)
    }

    /// Check if writes should be stalled due to too many immutable memtables.
    pub(crate) fn should_stall_writes(&self) -> bool {
        self.immutable_count() >= self.config.max_immutable_memtables
    }
}

/// Manages all column families in the database.
///
/// Uses DashMap for concurrent lookup access, with a creation lock to prevent
/// races during CF registration (checking name/id availability and inserting into two maps).
///
/// Provides lookup by ID or name, and tracks the next available CF ID.
pub(crate) struct ColumnFamilySet {
    pub(crate) cfs: Arc<DashMap<u32, Arc<ColumnFamily>>>,
    pub(crate) name_to_id: Arc<DashMap<String, u32>>,
    pub(crate) next_cf_id: AtomicU32,

    /// Serializes column family creation to prevent race conditions.
    /// Protects the check-then-insert pattern across two maps (cfs and name_to_id).
    pub(crate) create_lock: Arc<Mutex<()>>,
}

impl ColumnFamilySet {
    pub(crate) fn new() -> Self {
        let set = Self {
            cfs: Arc::new(DashMap::new()),
            name_to_id: Arc::new(DashMap::new()),
            next_cf_id: AtomicU32::new(1),
            create_lock: Arc::new(Mutex::new(())),
        };

        // Always create default CF (ID=0) to make default_cf() infallible
        let default_cf = Arc::new(ColumnFamily::new(
            DEFAULT_CF_ID,
            DEFAULT_CF_NAME.to_string(),
            ColumnFamilyConfig::default(),
        ));
        set.cfs.insert(0, default_cf);
        set.name_to_id.insert(DEFAULT_CF_NAME.to_string(), 0);

        set
    }

    pub(crate) fn create_cf(
        &self,
        id: ColumnFamilyId,
        name: String,
        config: ColumnFamilyConfig,
    ) -> MidgeResult<ColumnFamilyHandle> {
        // Serialize CF creation to prevent race conditions
        // Protects the check-then-insert pattern across two maps
        let _guard = self.create_lock.lock();

        let id_u32 = id.as_u32();

        // Check for duplicate ID
        if self.cfs.contains_key(&id_u32) {
            return Err(crate::error::MidgeError::invalid_config(format!(
                "Column family with ID {} already exists",
                id_u32
            )));
        }

        // Check for duplicate name
        if self.name_to_id.contains_key(&name) {
            return Err(crate::error::MidgeError::invalid_config(format!(
                "Column family with name '{}' already exists",
                name
            )));
        }

        // Create new CF
        let cf = Arc::new(ColumnFamily::new(id, name.clone(), config));
        let handle = cf.handle();

        // Atomically insert into both maps (protected by lock above)
        self.name_to_id.insert(name, id_u32);
        self.cfs.insert(id_u32, cf);

        Ok(handle)
    }

    #[inline]
    pub(crate) fn get_cf(&self, id: ColumnFamilyId) -> Option<Arc<ColumnFamily>> {
        self.cfs.get(&id.as_u32()).map(|r| r.value().clone())
    }

    #[inline]
    pub(crate) fn get_cf_by_name(&self, name: &str) -> Option<Arc<ColumnFamily>> {
        self.name_to_id
            .get(name)
            .and_then(|id| self.cfs.get(id.value()).map(|r| r.value().clone()))
    }

    #[inline]
    pub(crate) fn default_cf(&self) -> Arc<ColumnFamily> {
        // SAFETY: Default CF is always created in ColumnFamilySet::new()
        // This is now truly infallible by construction
        self.cfs
            .get(&0)
            .map(|r| r.value().clone())
            .expect("default CF must exist")
    }

    /// Get the configuration for a column family.
    #[inline]
    pub(crate) fn get_cf_config(&self, id: ColumnFamilyId) -> Option<ColumnFamilyConfig> {
        self.get_cf(id).map(|cf| cf.config.clone())
    }
}
