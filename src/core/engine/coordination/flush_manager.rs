//! Flush coordination for MidgeEngine.
//!
//! This module handles memtable flushing coordination including:
//! - Memtable rollover and flush queueing
//! - Flush-to-SST conversion
//! - Merge resolution before flush

use std::sync::atomic::Ordering;

use crate::api::column_family::{ColumnFamilyId, DEFAULT_CF_ID};
use crate::error::MidgeResult;

use super::super::MidgeEngine;

impl MidgeEngine {
    /// Roll over memtable and queue flush job for the specified column family.
    ///
    /// This freezes the current active memtable and queues it for background flushing.
    /// WAL is rotated to a new file for future writes.
    ///
    /// # Arguments
    ///
    /// * `cf_id` - Column family ID to flush (use DEFAULT_CF_ID for default CF)
    ///
    /// # Returns
    ///
    /// Sequence number at rollover time for tracking flush progress
    pub(crate) fn rollover_and_queue_flush(&self, cf_id: ColumnFamilyId) -> MidgeResult<u64> {
        crate::core::flush::rollover_and_queue_flush(
            cf_id,
            &self.seq,
            self.wal_coordinator.writer_lock(),
            self.wal_coordinator.factory(),
            &self.db_path.join("wal"),
            || {
                if cf_id == DEFAULT_CF_ID {
                    let entries = self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                    let range_tombstones =
                        self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                    (entries, range_tombstones)
                } else {
                    // For non-default CFs, use with_cf_memtable_mut
                    let entries = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_with_meta_internal()).unwrap_or_default();
                    let range_tombstones = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_range_tombstones()).unwrap_or_default();
                    (entries, range_tombstones)
                }
            },
            &self.flush_coordinator,
        )
    }

    /// Flush memtable to SST for the specified column family.
    ///
    /// Drains memtable contents and writes them to a new SST file on disk.
    /// Resolves any pending merge operations before flushing.
    ///
    /// # Arguments
    ///
    /// * `cf_id` - Column family ID to flush
    ///
    /// # Returns
    ///
    /// Tuple of (SST path, file metadata) for manifest updates
    pub(crate) fn flush_memtable_to_sst(&self, cf_id: ColumnFamilyId) -> MidgeResult<(std::path::PathBuf, crate::manifest::FileMeta)> {
        // Resolve any pending merge operations before flushing
        self.resolve_memtable_merges(cf_id)?;

        // Get CF config
        let cf_config = self.cf_set.get_cf_config(cf_id).unwrap_or_default();

        crate::core::flush::flush_memtable_to_sst(
            cf_id,
            || {
                if cf_id == DEFAULT_CF_ID {
                    let entries = self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                    let range_tombstones =
                        self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                    (entries, range_tombstones)
                } else {
                    let entries = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_with_meta_internal()).unwrap_or_default();
                    let range_tombstones = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_range_tombstones()).unwrap_or_default();
                    (entries, range_tombstones)
                }
            },
            crate::core::flush::FlushConfig {
                sst_factory: &self.sst_factory,
                compression: cf_config.compression.into(),
                block_size: self.block_size,
                bloom_bits_per_key: cf_config.bloom_bits_per_key,
                sst_dir: &self.sst_dir,
                metrics: &self.metrics,
                cloud_sst_mgr: self.cloud_sst_manager.as_ref().map(|m| m.as_ref()),
            },
        )
    }

    /// Resolve all pending merge operations in the memtable before flushing.
    ///
    /// This combines all merge operands for each key into a single resolved value
    /// using the registered merge operator. Only processes keys with actual merge operations.
    ///
    /// # Arguments
    ///
    /// * `cf_id` - Column family ID to resolve merges for
    fn resolve_memtable_merges(&self, cf_id: ColumnFamilyId) -> MidgeResult<()> {
        // Get all keys from memtable
        let all_keys = if cf_id == DEFAULT_CF_ID {
            self.with_default_memtable(|mt| mt.get_all_keys())
        } else {
            self.with_cf_memtable(cf_id, |mt| mt.get_all_keys()).unwrap_or_default()
        };

        // For each key, check if it has merge operands and resolve them
        for key in all_keys.iter() {
            let versions = if cf_id == DEFAULT_CF_ID {
                self.with_default_memtable(|mt| mt.get_versions_for_merge(key, u64::MAX))
            } else {
                self.with_cf_memtable(cf_id, |mt| mt.get_versions_for_merge(key, u64::MAX)).unwrap_or_default()
            };

            if versions.is_empty() {
                continue;
            }

            // Check if the latest operation is a Delete or Put - if so, don't resolve
            // (only resolve if we have Merge operations)
            if let Some((_value, _exp, op_type)) = versions.first() {
                if *op_type == crate::core::skiplist::OpType::Delete
                    || *op_type == crate::core::skiplist::OpType::Put
                {
                    continue; // Skip non-merge operations
                }
            }

            // Check if there are any merge operations
            let has_merges = versions
                .iter()
                .any(|(_, _, op)| *op == crate::core::skiplist::OpType::Merge);
            if !has_merges {
                continue; // Skip keys without merges
            }

            // Resolve the merges using cf_manager
            if let Some(resolved_value) = self.resolve_merges(key, versions)? {
                // Replace all versions with a single Put containing the resolved value
                let seq = self.seq.load(Ordering::SeqCst);
                if cf_id == DEFAULT_CF_ID {
                    self.with_default_memtable_mut(|mt| {
                        mt.put_with_seq_and_exp(key, &resolved_value, seq, None);
                    });
                } else {
                    self.with_cf_memtable_mut(cf_id, |mt| {
                        mt.put_with_seq_and_exp(key, &resolved_value, seq, None);
                    });
                }
            }
        }

        Ok(())
    }
}
