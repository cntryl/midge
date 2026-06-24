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

#[derive(Debug, Clone)]
pub struct FlushOutput {
    pub sst_name: String,
    pub file_meta: Option<crate::runtime::FileMeta>,
}

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
    ) -> MidgeResult<FlushOutput> {
        // Invariant: a flush may create output files early, but those files are
        // not authoritative until manifest publication completes. Any error
        // before publication must leave WAL-backed state recoverable.
        // In memory mode, flushes are no-ops (everything stays in memory)
        if self.memory_mode {
            return Ok(FlushOutput {
                sst_name: format!("memory_flush_{}", state.sequence),
                file_meta: None,
            });
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
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_write_stall_cloud();
                    }
                    tracing::warn!(cf_id = cf_id, "Flush blocked: waiting for cloud upload");
                    return Err(MidgeError::WriteStall(
                        "flush blocked until cloud upload frees durable capacity".to_string(),
                    ));
                }
                crate::storage::hybrid::actor::ReservationResult::WaitForCompaction => {
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_write_stall_compaction();
                    }
                    tracing::warn!(cf_id = cf_id, "Flush blocked: waiting for compaction");
                    return Err(MidgeError::WriteStall(
                        "flush blocked until compaction frees durable capacity".to_string(),
                    ));
                }
                crate::storage::hybrid::actor::ReservationResult::RejectNoSpace => {
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_no_space_event();
                        t.metrics().record_write_stall_no_space();
                    }
                    tracing::error!(cf_id = cf_id, "Flush rejected: no disk space available");
                    return Err(MidgeError::NoSpace(
                        "no disk space available for flush".to_string(),
                    ));
                }
            }
        }

        // === Phase 1.1: Check immutable queue capacity BEFORE adding to it ===
        // If immutable queue is at capacity, reject flush request with WriteStall error.
        // This provides backpressure to clients: they must retry after backoff.
        if state.is_immutable_memtable_queue_full(cf_id) {
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
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_write_stall_memory();
            }
            return Err(MidgeError::WriteStall(format!(
                "immutable memtable queue full ({}/{}); flush in progress",
                immutable_count, max_immutable
            )));
        }

        let current_segment_id = state.wal.current_segment_id;

        // Get the column family (after stall check to avoid borrow issues)
        let cf = state
            .get_cf_mut(cf_id)
            .ok_or_else(|| MidgeError::Internal(format!("Column family {} not found", cf_id)))?;

        if cf.memtable.size_bytes() == 0 {
            return Ok(FlushOutput {
                sst_name: format!("noop_flush_{}", state.sequence),
                file_meta: None,
            });
        }

        // Freeze current memtable and create new one
        let frozen = std::mem::replace(
            &mut cf.memtable,
            std::sync::Arc::new(crate::sst::SkipListMemtable::new()),
        );
        cf.active_memtable_started_in_segment = current_segment_id;

        // Add to immutable list
        cf.immutable_memtables.push(frozen.clone());

        // Generate SST filename
        let sst_seq = state
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .copied()
            .unwrap_or(1);
        let sst_name = crate::sst::file_name(cf_id, 0, sst_seq);
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
            crate::metadata::append_edit(&state.db_path, &edit)?;
        }
        state.manifest.next_sst_seqs.insert(cf_id, sst_seq + 1);

        self.in_progress += 1;

        tracing::info!(cf_id = cf_id, sst_name = %sst_name, "Flush started");

        // Write frozen memtable to SST file (blocking for now; could be async)
        let write_start = std::time::Instant::now();
        let mut file_meta = self.write_memtable_to_sst(&frozen, &sst_path, cf_id, &sst_name)?;
        let write_ns = write_start.elapsed().as_nanos();
        file_meta.level = 0;

        tracing::info!(cf_id = cf_id, sst_name = %sst_name, write_ms = (write_ns as f64) / 1_000_000.0, "SST file written");

        state.append_intent(crate::runtime::IntentLogEntry::FlushPublish {
            phase: crate::runtime::PublicationPhase::OutputDurable,
            cf_id,
            sequence: state.sequence,
            file_meta: file_meta.clone(),
        })?;

        fail::fail_point!("midge::flush::after_sst_write_before_publish");

        // Signal flush completion to SBA if available
        if let Some(hybrid) = sba {
            let sst_path_obj = std::fs::metadata(&sst_path)?;
            hybrid.flush_completed(sst_path_obj.len());

            let remote_bytes = std::fs::read(&sst_path)?;
            hybrid.write_sst_object(&sst_name, remote_bytes)?;
            tracing::debug!(cf_id = cf_id, sst_name = %sst_name, "SST mirrored to authoritative cloud storage");
        }

        Ok(FlushOutput {
            sst_name,
            file_meta: Some(file_meta),
        })
    }

    /// Write a frozen memtable to an SST file
    fn write_memtable_to_sst(
        &self,
        memtable: &std::sync::Arc<crate::sst::SkipListMemtable>,
        path: &Path,
        cf_id: crate::engine::ColumnFamilyId,
        sst_name: &str,
    ) -> MidgeResult<crate::runtime::FileMeta> {
        // Invariant: this function may write a complete SST file, but it does
        // not publish that file. Callers must treat the result as staged output
        // until manifest state makes it authoritative.
        // Create SST writer (should not reach here in memory mode, but be defensive)
        let sst_factory = self.sst_factory.as_ref().ok_or_else(|| {
            MidgeError::Internal("SST factory not available in memory mode".to_string())
        })?;
        let mut writer = sst_factory.create()?;

        // Get all entries from memtable and write to SST
        let entries = memtable.iter_all_with_meta(u64::MAX);
        if entries.is_empty() {
            return Err(MidgeError::Corruption(
                "attempted to flush an empty memtable".to_string(),
            ));
        }

        let smallest_key = entries
            .first()
            .map(|(key, _, _, _, _)| key.clone())
            .ok_or_else(|| MidgeError::Corruption("memtable flush missing smallest key".into()))?;
        let largest_key = entries
            .last()
            .map(|(key, _, _, _, _)| key.clone())
            .ok_or_else(|| MidgeError::Corruption("memtable flush missing largest key".into()))?;
        let mut smallest_seq = u64::MAX;
        let mut largest_seq = 0_u64;

        let add_start = std::time::Instant::now();
        let mut added_count: usize = 0;
        for (key, value, seq, expiration, op_type) in entries {
            smallest_seq = smallest_seq.min(seq);
            largest_seq = largest_seq.max(seq);

            writer.add_with_meta(&key, value.as_deref(), seq, op_type, expiration)?;
            added_count += 1;
        }
        let add_ns = add_start.elapsed().as_nanos();

        // Finish and write to path (finish_to_path does its own timing)
        let finish_start = std::time::Instant::now();
        Box::new(writer).finish_to_path(path)?;
        let finish_ns = finish_start.elapsed().as_nanos();

        tracing::info!(path = ?path, added = added_count, add_ms = (add_ns as f64) / 1_000_000.0, finish_ms = (finish_ns as f64) / 1_000_000.0, "memtable -> sst flush breakdown");

        let size_bytes = std::fs::metadata(path)?.len();
        Ok(crate::runtime::FileMeta {
            name: sst_name.to_string(),
            level: 0,
            size_bytes,
            cf_id,
            smallest_key: Some(smallest_key),
            largest_key: Some(largest_key),
            smallest_seq: Some(smallest_seq),
            largest_seq: Some(largest_seq),
        })
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

    #[test]
    fn should_name_flushed_sst_with_canonical_lex_sortable_format() -> MidgeResult<()> {
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let db_path = temp.path().to_path_buf();
        let mut state = RuntimeState::new(db_path.clone(), false);
        let mut actor = FlushActor::new(
            &db_path.join("sst"),
            false,
            crate::sst::compression::CompressionPolicy::default(),
        )?;
        state.get_cf(0).expect("default cf").memtable.put_with_seq(
            b"k".to_vec(),
            b"v".to_vec(),
            1,
            None,
        )?;

        let output = actor.handle_flush(&mut state, 0, None)?;

        assert_eq!(output.sst_name, "000000_00_00000000000000000001.sst");
        assert!(state.sst_dir.join(&output.sst_name).exists());

        Ok(())
    }
}
