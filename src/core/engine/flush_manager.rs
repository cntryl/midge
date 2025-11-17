//! Flush coordination for MidgeEngine.
//!
//! This module handles memtable flushing coordination including:
//! - Memtable rollover and flush queueing

use crate::api::column_family::{ColumnFamilyId, DEFAULT_CF_ID};
use crate::error::MidgeResult;

use super::MidgeEngine;

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
        crate::core::persistence::flush::rollover_and_queue_flush(
            cf_id,
            &self.seq,
            self.wal_coordinator.writer_lock(),
            self.wal_coordinator.factory(),
            &self.db_path.join("wal"),
            || {
                if cf_id == DEFAULT_CF_ID {
                    let entries =
                        self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                    let range_tombstones =
                        self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                    (entries, range_tombstones)
                } else {
                    // For non-default CFs, use with_cf_memtable_mut
                    let entries = self
                        .with_cf_memtable_mut(cf_id, |mt| mt.drain_with_meta_internal())
                        .unwrap_or_default();
                    let range_tombstones = self
                        .with_cf_memtable_mut(cf_id, |mt| mt.drain_range_tombstones())
                        .unwrap_or_default();
                    (entries, range_tombstones)
                }
            },
            &self.flush_coordinator,
        )
    }
}
