//! Column family management for the storage engine.

use dashmap::DashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::api::column_family::{
    ColumnFamilyConfig, ColumnFamilyHandle, ColumnFamilyId, DEFAULT_CF_ID, DEFAULT_CF_NAME,
};
use crate::core::memtable::MemTable;
use crate::error::MidgeResult;

/// Per-column-family state.
///
/// Each column family maintains its own MemTable, immutable memtables queue,
/// and SSTable hierarchy. Column families share the WAL and global sequence counter.
pub(super) struct ColumnFamily {
    pub(super) id: ColumnFamilyId,
    pub(super) name: String,
    pub(super) config: ColumnFamilyConfig,
    pub(super) memtable: MemTable,
}

impl ColumnFamily {
    pub(super) fn new(id: ColumnFamilyId, name: String, config: ColumnFamilyConfig) -> Self {
        Self {
            id,
            name,
            config,
            memtable: MemTable::new(),
        }
    }

    pub(super) fn handle(&self) -> ColumnFamilyHandle {
        ColumnFamilyHandle::new(self.id, self.name.clone())
    }
}

/// Manages all column families in the database.
///
/// Lock-free implementation using DashMap for concurrent access without RwLock contention.
/// Provides lookup by ID or name, and tracks the next available CF ID.
pub(super) struct ColumnFamilySet {
    pub(super) cfs: Arc<DashMap<u32, Arc<ColumnFamily>>>,
    pub(super) name_to_id: Arc<DashMap<String, u32>>,
    pub(super) next_cf_id: AtomicU32,
}

impl ColumnFamilySet {
    pub(super) fn new() -> Self {
        let set = Self {
            cfs: Arc::new(DashMap::new()),
            name_to_id: Arc::new(DashMap::new()),
            next_cf_id: AtomicU32::new(1),
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

    pub(super) fn create_cf(
        &self,
        id: ColumnFamilyId,
        name: String,
        config: ColumnFamilyConfig,
    ) -> MidgeResult<ColumnFamilyHandle> {
        let id_u32 = id.as_u32();

        if self.cfs.contains_key(&id_u32) {
            return Err(crate::error::MidgeError::invalid_config(format!(
                "Column family with ID {} already exists",
                id_u32
            )));
        }

        if self.name_to_id.contains_key(&name) {
            return Err(crate::error::MidgeError::invalid_config(format!(
                "Column family with name '{}' already exists",
                name
            )));
        }

        let cf = Arc::new(ColumnFamily::new(id, name.clone(), config));
        let handle = cf.handle();

        self.name_to_id.insert(name, id_u32);
        self.cfs.insert(id_u32, cf);

        Ok(handle)
    }

    #[inline]
    pub(super) fn get_cf(&self, id: ColumnFamilyId) -> Option<Arc<ColumnFamily>> {
        self.cfs.get(&id.as_u32()).map(|r| r.value().clone())
    }

    #[inline]
    pub(super) fn get_cf_by_name(&self, name: &str) -> Option<Arc<ColumnFamily>> {
        self.name_to_id
            .get(name)
            .and_then(|id| self.cfs.get(id.value()).map(|r| r.value().clone()))
    }

    #[inline]
    pub(super) fn default_cf(&self) -> Arc<ColumnFamily> {
        // SAFETY: Default CF is always created in ColumnFamilySet::new()
        // This is now truly infallible by construction
        self.cfs
            .get(&0)
            .map(|r| r.value().clone())
            .expect("default CF must exist")
    }

    /// Get the configuration for a column family.
    #[inline]
    pub(super) fn get_cf_config(&self, id: ColumnFamilyId) -> Option<ColumnFamilyConfig> {
        self.get_cf(id).map(|cf| cf.config.clone())
    }
}
