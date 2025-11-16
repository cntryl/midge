//! Column Family management for MidgeEngine.
//!
//! This module encapsulates all column family operations including:
//! - Creation and deletion of column families
//! - Column family querying and listing
//! - Merge operator registration per CF
//! - Merge resolution logic

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::api::column_family::{
    ColumnFamilyConfig, ColumnFamilyHandle, ColumnFamilyId, DEFAULT_CF_ID,
};
use crate::error::{MidgeError, MidgeResult};
use crate::manifest::Manifest;

use super::MidgeEngine;

impl MidgeEngine {
    /// Create a new column family with the specified name and configuration.
    ///
    /// # Arguments
    /// * `name` - Name for the column family (must be unique)
    /// * `config` - Configuration options for the column family
    ///
    /// # Returns
    /// A handle to the newly created column family
    ///
    /// # Errors
    /// Returns an error if:
    /// - The database is in read-only mode
    /// - A column family with the same name already exists
    /// - The manifest cannot be persisted
    ///
    /// # Example
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # use cntryl_midge::api::column_family::ColumnFamilyConfig;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let config = ColumnFamilyConfig::default();
    /// let cf = engine.create_column_family("my_cf", config)?;
    /// # Ok::<(), cntryl_midge::MidgeError>(())
    /// ```
    pub fn create_column_family(
        &self,
        name: &str,
        config: ColumnFamilyConfig,
    ) -> MidgeResult<ColumnFamilyHandle> {
        if self.read_only {
            return Err(MidgeError::invalid_config(
                "Cannot create column family in read-only mode",
            ));
        }

        let cf_id = ColumnFamilyId::new(self.cf_set.next_cf_id.fetch_add(1, Ordering::SeqCst));
        let handle = self
            .cf_set
            .create_cf(cf_id, name.to_string(), config.clone())?;

        let mut manifest = Manifest::load(&self.db_path).unwrap_or_default();
        manifest.add_cf(cf_id, name.to_string(), Some(config));

        // Persist manifest. If persistence fails, roll back the in-memory CF registration
        if let Err(e) = manifest.save_atomic(&self.db_path) {
            // Best-effort rollback of in-memory state inserted by create_cf
            let id_u32 = cf_id.as_u32();
            let _ = self.cf_set.cfs.remove(&id_u32);
            let _ = self.cf_set.name_to_id.remove(handle.name());
            return Err(e);
        }

        // Update cached manifest after successful save
        self.update_manifest_cache(manifest);

        Ok(handle)
    }

    /// Drop a column family and delete all its data.
    ///
    /// # Warning
    /// This operation is irreversible. All data in the column family will be permanently deleted.
    ///
    /// # Arguments
    /// * `handle` - Handle to the column family to drop
    ///
    /// # Errors
    /// Returns an error if:
    /// - The column family does not exist
    /// - Attempting to drop the default column family
    /// - The database is in read-only mode
    /// - The column family has unflushed data
    ///
    /// # Example
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # use cntryl_midge::api::column_family::ColumnFamilyConfig;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// # let cf = engine.create_column_family("temp_cf", ColumnFamilyConfig::default())?;
    /// engine.flush_cf(&cf)?; // Flush before dropping
    /// engine.drop_column_family(&cf)?;
    /// # Ok::<(), cntryl_midge::MidgeError>(())
    /// ```
    pub fn drop_column_family(&self, handle: &ColumnFamilyHandle) -> MidgeResult<()> {
        if self.read_only {
            return Err(MidgeError::invalid_config(
                "Cannot drop column family in read-only mode",
            ));
        }

        let cf_id = handle.id();

        if cf_id == DEFAULT_CF_ID {
            return Err(MidgeError::invalid_config(
                "Cannot drop the default column family",
            ));
        }

        // Check for unflushed data - refuse to drop if memtable or immutables are non-empty
        // Acquire flush_mutex to synchronize with any in-flight flush operations.
        // This avoids races where a flush is in progress and might return an error
        // or modify the manifest concurrently while the CF drop proceeds.
        let _flush_guard = self.flush_mutex.lock();
        let cf_id_u32 = cf_id.as_u32();
        if let Some(cf) = self.cf_set.cfs.get(&cf_id_u32) {
            // Check if active memtable has any data
            let memtable = cf.memtable.read();
            let is_empty = memtable.is_empty();
            drop(memtable);

            if !is_empty {
                return Err(MidgeError::invalid_config(format!(
                    "Cannot drop column family '{}' with unflushed data in active memtable. \
                     Please flush the column family first.",
                    handle.name()
                )));
            }

            // Check if there are any immutable memtables
            let immutable_count = cf.immutable_count();
            if immutable_count > 0 {
                return Err(MidgeError::invalid_config(format!(
                    "Cannot drop column family '{}' with {} unflushed immutable memtable(s). \
                     Please flush the column family first.",
                    handle.name(),
                    immutable_count
                )));
            }
        }

        // Remove from manifest. Collect SST file names first so we can delete them
        // after the manifest is updated.
        let mut manifest = Manifest::load(&self.db_path).unwrap_or_default();

        let cf_id_u32 = cf_id.as_u32();
        let files_to_delete: Vec<String> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id_u32)
            .map(|f| f.name.clone())
            .collect();

        manifest.remove_cf(cf_id);
        manifest.save_atomic(&self.db_path)?;

        // Update cached manifest after successful save
        self.update_manifest_cache(manifest.clone());

        // Delete SST files for this CF (best-effort)
        for name in files_to_delete {
            let path = self.sst_dir.join(&name);
            let _ = std::fs::remove_file(path);
        }

        // Remove in-memory CF metadata
        self.cf_set.cfs.remove(&cf_id_u32);
        self.cf_set.name_to_id.remove(handle.name());

        Ok(())
    }

    /// List all column families in the database.
    ///
    /// # Returns
    /// A vector of handles to all column families, including the default CF.
    ///
    /// # Example
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// for cf in engine.list_column_families() {
    ///     println!("Column family: {}", cf.name());
    /// }
    /// # Ok::<(), cntryl_midge::MidgeError>(())
    /// ```
    pub fn list_column_families(&self) -> Vec<ColumnFamilyHandle> {
        self.cf_set
            .cfs
            .iter()
            .map(|entry| entry.value().handle())
            .collect()
    }

    /// Get the default column family handle.
    ///
    /// The default column family always exists and has ID 0.
    ///
    /// # Example
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let default_cf = engine.default_column_family();
    /// engine.put(&default_cf, b"key", b"value")?;
    /// # Ok::<(), cntryl_midge::MidgeError>(())
    /// ```
    pub fn default_column_family(&self) -> ColumnFamilyHandle {
        self.cf_set.default_cf().handle()
    }

    /// Get a column family handle by name.
    ///
    /// # Arguments
    /// * `name` - Name of the column family to retrieve
    ///
    /// # Returns
    /// A handle to the column family
    ///
    /// # Errors
    /// Returns an error if no column family with the specified name exists.
    ///
    /// # Example
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # use cntryl_midge::api::column_family::ColumnFamilyConfig;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// # engine.create_column_family("my_cf", ColumnFamilyConfig::default())?;
    /// let cf = engine.get_column_family("my_cf")?;
    /// engine.put(&cf, b"key", b"value")?;
    /// # Ok::<(), cntryl_midge::MidgeError>(())
    /// ```
    pub fn get_column_family(&self, name: &str) -> MidgeResult<ColumnFamilyHandle> {
        self.cf_set
            .get_cf_by_name(name)
            .map(|cf| cf.handle())
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("Column family '{}' does not exist", name))
            })
    }

    /// Register a merge operator for a specific column family.
    ///
    /// Merge operators allow custom application-defined merge logic for combining
    /// multiple merge operands into a single value.
    ///
    /// # Arguments
    /// * `cf` - Column family handle
    /// * `operator` - Merge operator implementation
    ///
    /// # Example
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions, IntegerAddOperator};
    /// # use std::sync::Arc;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let cf = engine.default_column_family();
    /// let operator = IntegerAddOperator;
    /// engine.register_merge_operator(&cf, Arc::new(operator));
    /// # Ok::<(), cntryl_midge::MidgeError>(())
    /// ```
    pub fn register_merge_operator(
        &self,
        cf: &ColumnFamilyHandle,
        operator: crate::api::DynMergeOperator,
    ) {
        let cf_id = cf.id().as_u32();
        let mut ops = self.merge_operators.write();
        ops.insert(cf_id, operator);
    }

    /// Resolve merge operations given a list of versions from newest to oldest.
    ///
    /// This method applies the registered merge operator for the column family
    /// to combine multiple merge operands into a single resolved value.
    ///
    /// # Arguments
    /// * `key` - The key being merged
    /// * `versions` - List of (value, expiration, op_type) tuples from newest to oldest
    ///
    /// # Returns
    /// The resolved value, or None if the merge cannot be resolved
    ///
    /// # Note
    /// This is an internal method used during memtable flush and compaction.
    pub(crate) fn resolve_merges(
        &self,
        cf_id: ColumnFamilyId,
        key: &[u8],
        versions: Vec<(Option<Bytes>, Option<u64>, crate::core::skiplist::OpType)>,
    ) -> MidgeResult<Option<Bytes>> {
        // Use the provided CF ID to lookup the correct merge operator
        let cf_id = cf_id.as_u32();

        let ops = self.merge_operators.read();
        let Some(merge_op) = ops.get(&cf_id) else {
            // No merge operator registered - can't resolve
            return Ok(None);
        };

        // Collect merge operands and base value
        let mut operands: Vec<Bytes> = Vec::new();
        let mut base_value: Option<Bytes> = None;

        for (value_opt, _exp, op_type) in versions.iter().rev() {
            // Iterate oldest -> newest
            match op_type {
                crate::core::skiplist::OpType::Put => {
                    // Base value found - reset and start collecting from here
                    base_value = value_opt.clone();
                    operands.clear(); // Clear any operands before this Put
                }
                crate::core::skiplist::OpType::Merge => {
                    if let Some(val) = value_opt {
                        operands.push(val.clone());
                    }
                }
                crate::core::skiplist::OpType::Delete => {
                    // Tombstone resets the chain - clear everything and continue
                    base_value = None;
                    operands.clear();
                }
            }
        }

        if operands.is_empty() {
            return Ok(base_value);
        }

        // Apply merge operator: merge_many handles both base+operands and operands-only cases
        let operand_refs: Vec<&[u8]> = operands.iter().map(|b| b.as_ref()).collect();
        let result = merge_op.merge_many(key, base_value.as_deref(), &operand_refs);

        match result {
            Ok(val) => Ok(Some(Bytes::from(val))),
            Err(_) => Ok(None), // Merge failed - return None
        }
    }
}
