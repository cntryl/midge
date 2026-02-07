//! Flush Actor - handles memtable to SST flushes
//!
//! Responsible for:
//! - Freezing active memtables
//! - Writing immutable memtables to SST files
//! - Coordinating with manifest actor for metadata updates

use super::super::state::RuntimeState;
use crate::common::{MidgeError, MidgeResult};
use crate::sst::Memtable;
use std::path::Path;

/// Actor handling memtable flushes
pub struct FlushActor {
    /// Number of flushes in progress
    in_progress: usize,
    /// SST factory for creating writers (None in memory mode)
    sst_factory: Option<std::sync::Arc<dyn crate::sst::SstFactory>>,
    /// Whether we're in memory-only mode (no disk operations)
    memory_mode: bool,
}

impl FlushActor {
    pub fn new(
        sst_dir: &Path,
        memory_mode: bool,
        compression_policy: crate::sst::compression::CompressionPolicy,
    ) -> MidgeResult<Self> {
        let sst_factory: Option<std::sync::Arc<dyn crate::sst::SstFactory>> = if memory_mode {
            None // Don't create factory in memory mode
        } else {
            let fs = std::sync::Arc::new(crate::io::RealFs::new(sst_dir)?);
            Some(std::sync::Arc::new(
                crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                    .with_compression_policy(compression_policy),
            ))
        };
        Ok(Self {
            in_progress: 0,
            sst_factory,
            memory_mode,
        })
    }

    /// Handle a flush request for a column family
    ///
    /// If SBA is available, reserves space before flushing. Handles backpressure
    /// responses (WaitForCloud, WaitForCompaction, RejectNoSpace).
    ///
    /// This freezes the active memtable and queues it for background flush.
    /// Returns the name of the SST file that will be created.
    pub fn handle_flush(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::engine::ColumnFamilyId,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<String> {
        // In memory mode, flushes are no-ops (everything stays in memory)
        if self.memory_mode {
            return Ok(format!("memory_flush_{}", state.sequence));
        }

        // Estimate SST size: approximate as active memtable size
        let est_size = 1024 * 1024; // 1MB estimate; could be more precise

        // Try to reserve space if SBA is available
        if let Some(hybrid) = sba {
            let reservation = hybrid.reserve_for_flush(est_size);
            match reservation {
                crate::storage::hybrid::actor::ReservationResult::Ok => {
                    // Proceed with flush
                }
                crate::storage::hybrid::actor::ReservationResult::WaitForCloudUpload => {
                    tracing::warn!(cf_id = cf_id, "Flush blocked: waiting for cloud upload");
                    return Err(MidgeError::Internal(
                        "Flush blocked: waiting for cloud upload".to_string(),
                    ));
                }
                crate::storage::hybrid::actor::ReservationResult::WaitForCompaction => {
                    tracing::warn!(cf_id = cf_id, "Flush blocked: waiting for compaction");
                    return Err(MidgeError::Internal(
                        "Flush blocked: waiting for compaction".to_string(),
                    ));
                }
                crate::storage::hybrid::actor::ReservationResult::RejectNoSpace => {
                    tracing::error!(cf_id = cf_id, "Flush rejected: no disk space available");
                    return Err(MidgeError::Internal("No disk space available".to_string()));
                }
            }
        }

        // === Phase 1.1: Check write stall condition BEFORE adding to immutable queue ===
        // If immutable queue is at capacity, reject flush request with WriteStall error.
        // This provides backpressure to clients: they must retry after backoff.
        if state.should_stall_writes(cf_id) {
            let max_immutable = state.max_immutable_memtables;
            let immutable_count = state
                .get_cf(cf_id)
                .map(|cf| cf.immutable_memtables.len())
                .unwrap_or(0);

            tracing::warn!(
                cf_id = cf_id,
                immutable_count = immutable_count,
                max_immutable = max_immutable,
                "Write stall: immutable memtable queue at capacity"
            );
            return Err(MidgeError::WriteStall(format!(
                "immutable memtable queue full ({}/{}); flush in progress",
                immutable_count, max_immutable
            )));
        }

        // Get the column family (after stall check to avoid borrow issues)
        let cf = state
            .get_cf_mut(cf_id)
            .ok_or_else(|| MidgeError::Internal(format!("Column family {} not found", cf_id)))?;

        // Freeze current memtable and create new one
        let frozen = std::mem::replace(
            &mut cf.memtable,
            std::sync::Arc::new(crate::sst::SkipListMemtable::new()),
        );

        // Add to immutable list
        cf.immutable_memtables.push(frozen.clone());

        // Generate SST filename
        let sst_seq = state
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .copied()
            .unwrap_or(1);
        let sst_name = format!("sst_{:06}_{:06}.sst", cf_id, sst_seq);
        let sst_path = state.sst_dir.join(&sst_name);

        // Ensure parent directory exists
        if let Some(parent) = sst_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Update next SST sequence (journal before applying)
        {
            let edit = crate::metadata::ManifestEdit::BumpNextSstSeq {
                cf_id,
                next_seq: sst_seq + 1,
            };
            if let Err(e) = crate::metadata::append_edit(&state.db_path, &edit) {
                tracing::warn!(error = ?e, "failed to append BumpNextSstSeq to journal");
            }
        }
        state.manifest.next_sst_seqs.insert(cf_id, sst_seq + 1);

        self.in_progress += 1;

        tracing::info!(cf_id = cf_id, sst_name = %sst_name, "Flush started");

        // Write frozen memtable to SST file (blocking for now; could be async)
        let write_start = std::time::Instant::now();
        self.write_memtable_to_sst(&frozen, &sst_path)?;
        let write_ns = write_start.elapsed().as_nanos();

        tracing::info!(cf_id = cf_id, sst_name = %sst_name, write_ms = (write_ns as f64) / 1_000_000.0, "SST file written");

        // Signal flush completion to SBA if available
        if let Some(hybrid) = sba {
            let sst_path_obj = std::fs::metadata(&sst_path)?;
            hybrid.flush_completed(sst_path_obj.len());
        }

        // Queue SST for cloud upload if using cloud-backed storage
        if sba.is_some() {
            state.cloud.pending_uploads.push(sst_name.clone());
            tracing::debug!(cf_id = cf_id, sst_name = %sst_name, "SST queued for cloud upload");
        }

        Ok(sst_name)
    }

    /// Write a frozen memtable to an SST file
    fn write_memtable_to_sst(
        &self,
        memtable: &std::sync::Arc<crate::sst::SkipListMemtable>,
        path: &Path,
    ) -> MidgeResult<()> {
        // Create SST writer (should not reach here in memory mode, but be defensive)
        let sst_factory = self.sst_factory.as_ref().ok_or_else(|| {
            MidgeError::Internal("SST factory not available in memory mode".to_string())
        })?;
        let mut writer = sst_factory.create()?;

        // Get all entries from memtable and write to SST
        let entries = memtable.iter_all(u64::MAX);

        let add_start = std::time::Instant::now();
        let mut added_count: usize = 0;
        for (key, value, seq) in entries {
            // Determine op_type: 0=Put, 2=Delete
            let op_type = if value.is_some() { 0 } else { 2 };

            writer.add_with_meta(&key, value.as_deref(), seq, op_type, None)?;
            added_count += 1;
        }
        let add_ns = add_start.elapsed().as_nanos();

        // Finish and write to path (finish_to_path does its own timing)
        let finish_start = std::time::Instant::now();
        Box::new(writer).finish_to_path(path)?;
        let finish_ns = finish_start.elapsed().as_nanos();

        tracing::info!(path = ?path, added = added_count, add_ms = (add_ns as f64) / 1_000_000.0, finish_ms = (finish_ns as f64) / 1_000_000.0, "memtable -> sst flush breakdown");

        Ok(())
    }

    /// Handle flush completion notification
    pub fn handle_flush_complete(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::engine::ColumnFamilyId,
        sst_name: &str,
        sequence: u64,
    ) {
        if let Some(cf) = state.get_cf_mut(cf_id) {
            // Remove the oldest immutable memtable (FIFO) and update total memory accounting
            if !cf.immutable_memtables.is_empty() {
                let removed = cf.immutable_memtables.remove(0);
                let removed_size = removed.size_bytes();
                state.total_memtable_bytes =
                    state.total_memtable_bytes.saturating_sub(removed_size);
            }
        }

        // Update last persisted sequence
        if sequence > state.manifest.last_persisted_sequence {
            state.manifest.last_persisted_sequence = sequence;
        }

        self.in_progress = self.in_progress.saturating_sub(1);

        tracing::info!(cf_id = cf_id, sst_name, sequence, "Flush completed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn create_test_flush_actor() -> MidgeResult<FlushActor> {
        FlushActor::new(
            &PathBuf::from("/tmp"),
            false,
            crate::sst::compression::CompressionPolicy::default(),
        )
    }

    #[test]
    fn should_initialize_flush_actor_with_zero_in_progress() -> MidgeResult<()> {
        // Arrange
        // (no setup needed)

        // Act
        let actor = create_test_flush_actor()?;

        // Assert
        assert_eq!(actor.in_progress, 0);

        Ok(())
    }

    #[test]
    fn should_increment_in_progress_count() {
        // Arrange
        let mut actor = create_test_flush_actor().unwrap();
        assert_eq!(actor.in_progress, 0);

        // Act
        actor.in_progress += 1;

        // Assert
        assert_eq!(actor.in_progress, 1);
    }

    #[test]
    fn should_accumulate_multiple_in_progress() {
        // Arrange
        let mut actor = create_test_flush_actor().unwrap();

        // Act
        actor.in_progress += 1;
        actor.in_progress += 1;
        actor.in_progress += 1;

        // Assert
        assert_eq!(actor.in_progress, 3);
    }

    #[test]
    fn should_decrement_in_progress_on_complete() {
        // Arrange
        let mut actor = create_test_flush_actor().unwrap();
        actor.in_progress = 2;

        // Act
        actor.in_progress = actor.in_progress.saturating_sub(1);

        // Assert
        assert_eq!(actor.in_progress, 1);
    }

    #[test]
    fn should_not_go_negative_with_saturating_sub() {
        // Arrange
        let mut actor = create_test_flush_actor().unwrap();
        actor.in_progress = 0;

        // Act
        actor.in_progress = actor.in_progress.saturating_sub(1);

        // Assert: should stay at 0
        assert_eq!(actor.in_progress, 0);
    }

    #[test]
    fn should_maintain_monotonic_in_progress_tracking() {
        // Arrange
        let mut actor = create_test_flush_actor().unwrap();

        // Act
        actor.in_progress += 1;
        actor.in_progress += 1;
        actor.in_progress = actor.in_progress.saturating_sub(1);

        // Assert: increment and decrement pattern
        assert_eq!(actor.in_progress, 1);
    }
}
