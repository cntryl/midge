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
        // Get CF config
        let cf_config = self.cf_set.get_cf_config(cf_id).unwrap_or_default();

        // Resolve merge operations BEFORE drain (while OpType is still available in skiplist)
        self.resolve_memtable_merges(cf_id)?;

        // Drain memtable
        let (entries, range_tombstones) = if cf_id == DEFAULT_CF_ID {
            let entries = self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
            let range_tombstones =
                self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
            (entries, range_tombstones)
        } else {
            let entries = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_with_meta_internal()).unwrap_or_default();
            let range_tombstones = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_range_tombstones()).unwrap_or_default();
            (entries, range_tombstones)
        };

        crate::core::flush::flush_memtable_to_sst(
            cf_id,
            || (entries, range_tombstones),
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
    /// This method collects all versions for keys with merge operations, resolves them
    /// using the registered merge operator, and writes back the resolved value with a new
    /// sequence number. The drain operation will then pick up the newest (resolved) version.
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

            // Check if there are any merge operations - if so, resolve them
            let has_merges = versions
                .iter()
                .any(|(_, _, op)| *op == crate::core::skiplist::OpType::Merge);
            if !has_merges {
                continue; // Skip keys without merges
            }

            // Resolve the merges using cf_manager with the correct CF ID
            if let Some(resolved_value) = self.resolve_merges(cf_id, key, versions)? {
                // Replace all versions with a single Put containing the resolved value
                // Use fetch_add to get a new sequence number that's higher than all existing ones
                let seq = self.seq.fetch_add(1, Ordering::SeqCst);
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
