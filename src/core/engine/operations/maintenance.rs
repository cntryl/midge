//! Maintenance Operations Module
//!
//! This module contains all maintenance operations for MidgeEngine, including:
//! - Flush operations (flush memtable to SST)
//! - Compaction operations (compact_level, compact_range, compact_all)
//! - Checkpoint operations (create_checkpoint)
//! - Close operations (clean shutdown)
//!
//! All operations handle:
//! - Manifest updates
//! - Cache updates
//! - Background worker coordination
//! - Consistency guarantees

use crate::api::column_family::ColumnFamilyHandle;
use crate::core::engine::core::MidgeEngine;
use crate::core::manifest::Manifest;
use crate::error::{MidgeError, MidgeResult};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, warn};

impl MidgeEngine {
    /// Flush MemTable to SST and update manifest. No-op if MemTable is empty or read-only.
    ///
    /// Currently flushes the default column family.
    /// Use `flush_cf()` to flush a specific column family.
    pub fn flush(&self) -> MidgeResult<()> {
        self.flush_cf(&self.default_column_family())
    }

    /// Flush a specific column family's memtable to SST.
    pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
        // Serialize flush operations to prevent concurrent file conflicts
        let _flush_guard = self.flush_mutex.lock();

        if self.read_only {
            return Ok(());
        }

        let cf_id = cf.id();
        
        // Check if memtable is empty
        let is_empty = if cf_id == crate::api::column_family::DEFAULT_CF_ID {
            self.with_default_memtable(|mt| mt.is_empty())
        } else {
            self.with_cf_memtable(cf_id, |mt| mt.is_empty()).unwrap_or(true)
        };

        if is_empty {
            return Ok(());
        }

        let (file_path, file_meta) = self.flush_memtable_to_sst(cf_id)?;
        let mut m =
            Manifest::load_with_retry(&self.db_path, 10, std::time::Duration::from_millis(10))?;
        let name = file_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        if !name.is_empty() {
            let size_bytes = std::fs::metadata(&file_path)
                .map(|md| md.len())
                .unwrap_or(0);

            let sublevel =
                if let (Some(sk), Some(lk)) = (&file_meta.smallest_key, &file_meta.largest_key) {
                    m.assign_l0_sublevel(sk, lk)
                } else {
                    0
                };

            // preserve metadata computed by flush_memtable_to_sst
            m.files.push(crate::manifest::FileMeta {
                name: file_meta.name.clone(),
                level: file_meta.level,
                size_bytes,
                cf_id: cf_id.as_u32(), // Use the actual CF ID
                smallest_key: file_meta.smallest_key,
                largest_key: file_meta.largest_key,
                smallest_seq: file_meta.smallest_seq,
                largest_seq: file_meta.largest_seq,
                sublevel,
                cloud_location: file_meta.cloud_location,
                cloud_checksum: file_meta.cloud_checksum,
                cloud_uploaded_at: file_meta.cloud_uploaded_at,
                cloud_state: file_meta.cloud_state,
                point_tombstone_count: file_meta.point_tombstone_count,
                range_tombstone_count: file_meta.range_tombstone_count,
                total_entries: file_meta.total_entries,
            });
            m.ssts.push(name.clone());
        }
        m.last_persisted_sequence = self.seq.load(Ordering::SeqCst);
        m.save_atomic(&self.db_path)?;

        // Update cached manifest after successful save
        self.update_manifest_cache(m);

        // Update bloom and sparse index caches for the new SST
        if !name.is_empty() {
            self.update_caches_for_new_sst(&name);
        }

        // TODO: After manifest is persisted, we should replace memtable with new empty one
        // to avoid re-flushing data (since drain is non-destructive). However, this breaks
        // snapshot reads in some edge cases. Needs further investigation.
        // self.replace_memtable_after_flush(cf_id);

        Ok(())
    }

    /// Trigger manual compaction for a specific level in a column family.
    ///
    /// This compacts all files at the specified level to the next level.
    /// The compaction runs asynchronously in the background compaction thread.
    ///
    /// # Arguments
    /// * `cf` - Column family handle
    /// * `level` - Level to compact (0-based)
    ///
    /// # Errors
    /// Returns an error if compaction is disabled or the channel is disconnected.
    pub fn compact_level(&self, cf: &ColumnFamilyHandle, level: u32) -> MidgeResult<()> {
        if let Some(ref coordinator) = self.compaction_coordinator {
            coordinator.compact_level(cf.id.as_u32(), level)
        } else {
            Err(MidgeError::invalid_config(
                "Manual compaction requested but compaction is disabled",
            ))
        }
    }

    /// Trigger manual compaction for a key range in a column family.
    ///
    /// This compacts all files overlapping the specified key range across all levels.
    /// The compaction runs asynchronously in the background compaction thread.
    ///
    /// # Arguments
    /// * `cf` - Column family handle
    /// * `start_key` - Start of key range (inclusive), None means from beginning
    /// * `end_key` - End of key range (exclusive), None means to end
    ///
    /// # Errors
    /// Returns an error if compaction is disabled or the channel is disconnected.
    pub fn compact_range(
        &self,
        cf: &ColumnFamilyHandle,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> MidgeResult<()> {
        if let Some(ref coordinator) = self.compaction_coordinator {
            coordinator.compact_range(
                cf.id.as_u32(),
                start_key.map(|k| k.to_vec()),
                end_key.map(|k| k.to_vec()),
            )
        } else {
            Err(MidgeError::invalid_config(
                "Manual compaction requested but compaction is disabled",
            ))
        }
    }

    /// Wait for all pending flush operations to complete.
    ///
    /// This blocks until the background flush worker has processed all queued flush jobs.
    /// Useful for tests that need to ensure flushes are complete before asserting state.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for flush completion
    ///
    /// # Errors
    /// Returns an error if the timeout expires or the flush worker is disconnected.
    pub fn wait_for_flush(&self, timeout: std::time::Duration) -> MidgeResult<()> {
        self.flush_coordinator.wait_until_idle(timeout)
    }

    /// Wait for all pending compaction operations to complete.
    ///
    /// This blocks until the background compaction worker has processed all queued jobs.
    /// Useful for tests that need to ensure compactions are complete before asserting state.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for compaction completion
    ///
    /// # Errors
    /// Returns an error if compaction is disabled, the timeout expires, or the worker is disconnected.
    pub fn wait_for_compaction(&self, timeout: std::time::Duration) -> MidgeResult<()> {
        if let Some(ref coordinator) = self.compaction_coordinator {
            coordinator.wait_until_idle(timeout)
        } else {
            Err(MidgeError::invalid_config(
                "Cannot wait for compaction when compaction is disabled",
            ))
        }
    }

    /// Close the engine: flush MemTable and stop background workers.
    pub fn close(self) -> MidgeResult<()> {
        let _ = self.flush();
        Ok(())
    }

    /// Create a filesystem checkpoint at `dst_dir` containing a consistent snapshot of the DB.
    /// This writes a manifest copy and links/copies all referenced SST files. CURRENT is written
    /// in the checkpoint directory to point at the manifest.
    pub fn create_checkpoint(&self, dst_dir: &std::path::Path) -> MidgeResult<()> {
        // Ensure current MemTable contents are persisted
        if !self.read_only {
            let _ = self.flush();
        }
        // Load manifest snapshot
        let m = Manifest::load(&self.db_path).unwrap_or_default();
        // Prepare checkpoint directories
        std::fs::create_dir_all(dst_dir)?;
        let dst_sst = dst_dir.join("sst");
        std::fs::create_dir_all(&dst_sst)?;
        // Link or copy each SST into checkpoint/sst
        for name in &m.ssts {
            let src = self.sst_dir.join(name);
            let dst = dst_sst.join(name);
            if !src.exists() {
                continue;
            }
            // Try hard link, fallback to copy
            match std::fs::hard_link(&src, &dst) {
                Ok(_) => {}
                Err(_) => {
                    std::fs::copy(&src, &dst)?;
                }
            }
        }
        // Debug: list checkpoint SST files
        if let Ok(entries) = std::fs::read_dir(&dst_sst) {
            debug!("listing checkpoint sst entries");
            for e in entries.flatten() {
                let p = e.path();
                if let Ok(md) = std::fs::metadata(&p) {
                    debug!(path = %p.display(), size_bytes = md.len(), "checkpoint sst file");
                } else {
                    debug!(path = %p.display(), "checkpoint sst file (no metadata)");
                }
            }
        }
        // Try opening SSTs in the checkpoint to validate format
        if let Ok(entries) = std::fs::read_dir(&dst_sst) {
            for e in entries.flatten() {
                let p = e.path();
                match crate::sst::fs::SstFile::open(&p) {
                    Ok(sst) => match crate::sst::SstStateReader::scan_range_state(&sst, None, None)
                    {
                        Ok(rows) => {
                            debug!(path = %p.display(), rows = ?rows, "checkpoint sst scan succeeded")
                        }
                        Err(err) => {
                            warn!(path = %p.display(), error = ?err, "checkpoint sst scan failed")
                        }
                    },
                    Err(err) => warn!(
                        path = %p.display(),
                        error = ?err,
                        "failed to open checkpoint sst"
                    ),
                }
            }
        }
        // Write manifest.json verbatim into checkpoint
        let manifest_path = dst_dir.join("manifest.json");
        let data = serde_json::to_vec_pretty(&m)?;
        std::fs::write(&manifest_path, &data)?;
        // Write CURRENT pointer
        std::fs::write(dst_dir.join("CURRENT"), b"manifest.json")?;
        Ok(())
    }

    /// Minimal compaction: merge all existing SSTs into a single SST.
    /// Preserves per-entry sequence metadata so snapshot visibility remains correct.
    pub fn compact_all(&self) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::ReadOnly);
        }
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        if manifest.ssts.is_empty() {
            return Ok(());
        }
        let mut versions = crate::core::compaction::collect_compaction_versions(
            &self.sst_reader_factory,
            &self.sst_dir,
            &manifest.ssts,
        );
        if versions.is_empty() {
            return Ok(());
        }
        crate::core::compaction::sort_versions_for_output(&mut versions);

        // Apply tombstone GC safety: only drop tombstones that are shadowed
        // and not visible to any active snapshot
        let min_snapshot_seq = self.snapshot_registry.min_active_seq();
        let (versions, removed_tombstones) =
            crate::core::compaction::filter_safe_tombstones(&versions, min_snapshot_seq);

        // Track tombstone removal metrics
        if removed_tombstones > 0 {
            self.metrics
                .record_tombstones_removed(removed_tombstones as u64);
        }

        // Apply compaction filter from default CF
        let cf_id = crate::api::column_family::DEFAULT_CF_ID;
        let filter_arc = self.cf_set
            .cfs
            .get(&cf_id.as_u32())
            .and_then(|cf| {
                let guard = cf.compaction_filter.read();
                if let Some(ref arc) = *guard {
                    Some(Arc::clone(arc))
                } else {
                    None
                }
            });
        
        let versions = if let Some(filter) = filter_arc {
            crate::core::compaction::apply_compaction_filter(&versions, filter.as_ref(), 0)
        } else {
            let noop = crate::compaction_filter::NoOpFilter;
            crate::core::compaction::apply_compaction_filter(&versions, &noop, 0)
        };

        // Deduplicate to ensure only one version per key in output SST
        let versions = crate::core::compaction::deduplicate_versions(&versions);

        // Debug: log versions that will be written during compact_all
        debug!(
            version_count = versions.len(),
            "compact_all: writing versions to output SST"
        );

        // Build the output SST file name
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let sst_name = format!("L0_{:016}.sst", seq);
        let sst_path = self.sst_dir.join(&sst_name);

        // Write the output SST
        let mut writer = self
            .sst_factory
            .create(self.compression, self.block_size, true);

        for v in &versions {
            if v.tombstone {
                writer.add_with_meta(v.user_key.as_slice(), None, v.seq, true, v.expiration)?;
            } else if let Some(value) = &v.value {
                writer.add_with_meta(
                    v.user_key.as_slice(),
                    Some(value.as_ref()),
                    v.seq,
                    false,
                    v.expiration,
                )?;
            }
        }

        let raw = writer.finish_bytes()?;
        std::fs::write(&sst_path, &raw)?;
        let bytes_written = raw.len();
        debug!(
            path = %sst_path.display(),
            bytes_written,
            "compact_all: output SST written"
        );

        // Update manifest: replace old SSTs with new one
        let mut m = Manifest::load(&self.db_path).unwrap_or_default();
        m.ssts.retain(|n| !manifest.ssts.contains(n));
        m.files.retain(|f| !manifest.ssts.contains(&f.name));
        m.ssts.push(sst_name.clone());

        // Compute metadata for the new SST
        let size_bytes = std::fs::metadata(&sst_path)
            .map(|md| md.len())
            .unwrap_or(0);
        let smallest_key = versions
            .first()
            .map(|v| v.user_key.clone());
        let largest_key = versions
            .last()
            .map(|v| v.user_key.clone());
        let smallest_seq = versions.iter().map(|v| v.seq).min();
        let largest_seq = versions.iter().map(|v| v.seq).max();
        let point_tombstone_count = versions.iter().filter(|v| v.tombstone).count() as u64;

        // compact_all operates on the default CF
        let cf_id = crate::api::column_family::DEFAULT_CF_ID.as_u32();
        
        m.files.push(crate::manifest::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes,
            cf_id,
            smallest_key,
            largest_key,
            smallest_seq,
            largest_seq,
            sublevel: 0,
            cloud_location: None,
            cloud_checksum: None,
            cloud_uploaded_at: None,
            cloud_state: None,
            point_tombstone_count,
            range_tombstone_count: 0,
            total_entries: versions.len() as u64,
        });

        m.last_persisted_sequence = self.seq.load(Ordering::SeqCst);
        m.save_atomic(&self.db_path)?;

        // Update cached manifest after successful save
        self.update_manifest_cache(m);

        // Invalidate table cache entries for old SSTs before deleting files
        if let Some(ref cache) = self.table_cache {
            for old_sst in &manifest.ssts {
                cache.remove(old_sst);
            }
        }

        // Delete old SST files
        for old_sst in &manifest.ssts {
            let old_path = self.sst_dir.join(old_sst);
            if old_path.exists() {
                if let Err(e) = std::fs::remove_file(&old_path) {
                    warn!(path = %old_path.display(), error = %e, "failed to remove old SST");
                } else {
                    debug!(path = %old_path.display(), "removed old SST");
                }
            }
        }

        // Update bloom and sparse index caches for the new SST
        self.update_caches_for_new_sst(&sst_name);

        Ok(())
    }

    /// Set a custom compaction filter for a column family.
    ///
    /// The filter will be applied during compaction to drop or modify keys.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # use cntryl_midge::compaction::filter::{CompactionFilter, FilterDecision, CompactionVersion};
    /// # use std::sync::Arc;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// struct MyFilter;
    /// impl CompactionFilter for MyFilter {
    ///     fn filter(&self, _level: u32, version: &CompactionVersion) -> FilterDecision {
    ///         // Custom logic here
    ///         FilterDecision::Keep
    ///     }
    /// }
    ///
    /// let cf = engine.default_column_family();
    /// engine.set_compaction_filter(&cf, Arc::new(MyFilter));
    /// ```
    pub fn set_compaction_filter(
        &self,
        cf: &ColumnFamilyHandle,
        filter: Arc<dyn crate::core::compaction::filter::CompactionFilter>,
    ) -> MidgeResult<()> {
        let cf_id = cf.id();
        
        if let Some(cf_entry) = self.cf_set.cfs.get(&cf_id.as_u32()) {
            let mut filter_lock = cf_entry.compaction_filter.write();
            *filter_lock = Some(filter);
            Ok(())
        } else {
            Err(MidgeError::InvalidConfig {
                message: format!("Column family {} not found", cf.name()),
            })
        }
    }

    /// Clear the compaction filter for a column family.
    pub fn clear_compaction_filter(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
        let cf_id = cf.id();
        
        if let Some(cf_entry) = self.cf_set.cfs.get(&cf_id.as_u32()) {
            let mut filter_lock = cf_entry.compaction_filter.write();
            *filter_lock = None;
            Ok(())
        } else {
            Err(MidgeError::InvalidConfig {
                message: format!("Column family {} not found", cf.name()),
            })
        }
    }
}
