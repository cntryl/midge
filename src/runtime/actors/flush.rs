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

#[derive(Clone)]
pub struct FlushOutput {
    pub sst_name: String,
    /// Highest sequence represented by this exact frozen memtable.
    pub sequence: u64,
    pub file_meta: Option<crate::runtime::FileMeta>,
    /// Identity of the frozen memtable represented by this output.
    pub frozen_memtable: Option<std::sync::Arc<crate::sst::SkipListMemtable>>,
    /// Accounting reservation for this exact flush publication.
    pub reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
}

struct FlushPublication<'a> {
    cf_id: crate::types::ColumnFamilyId,
    sst_name: &'a str,
    sequence: u64,
    frozen: &'a std::sync::Arc<crate::sst::SkipListMemtable>,
    sst_path: &'a Path,
    reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
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
    /// responses (`WaitForCloud`, `WaitForCompaction`, `RejectNoSpace`).
    ///
    /// This freezes the active memtable and queues it for background flush.
    /// Returns the name of the SST file that will be created.
    pub fn handle_flush(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<FlushOutput> {
        // Invariant: a flush may create output files early, but those files are
        // not authoritative until manifest publication completes. Any error
        // before publication must leave WAL-backed state recoverable.
        // In memory mode, flushes are no-ops (everything stays in memory)
        if self.memory_mode {
            return Ok(FlushOutput {
                sst_name: format!("memory_flush_{}", state.sequence),
                sequence: state.sequence,
                file_meta: None,
                frozen_memtable: None,
                reservation: None,
            });
        }

        // A failed immutable always owns the next flush attempt for its CF.
        // Explicit flushes may retry before the maintenance deadline; automatic
        // callers select this CF only when the backoff has elapsed.
        if let Some(attempt) = state.begin_pending_immutable_flush(cf_id, false) {
            self.in_progress += 1;
            let est_size = u64::try_from(attempt.memtable.size_bytes()).unwrap_or(u64::MAX);
            let reservation = match Self::reserve_flush(sba, cf_id, est_size.max(1)) {
                Ok(reservation) => reservation,
                Err(error) => {
                    self.fail_tracked_flush(state, cf_id, &attempt.memtable, sba, None);
                    return Err(error);
                }
            };
            return self.execute_tracked_flush(state, cf_id, &attempt, sba, reservation);
        }

        let active_size = state
            .get_cf(cf_id)
            .ok_or_else(|| MidgeError::Internal(format!("Column family {cf_id} not found")))?
            .memtable
            .size_bytes();
        if active_size == 0 {
            return Ok(FlushOutput {
                sst_name: format!("noop_flush_{}", state.sequence),
                sequence: state.sequence,
                file_meta: None,
                frozen_memtable: None,
                reservation: None,
            });
        }

        let est_size = u64::try_from(active_size).unwrap_or(u64::MAX).max(1);
        let flush_reservation = Self::reserve_flush(sba, cf_id, est_size)?;
        Self::ensure_immutable_capacity(state, cf_id, sba, flush_reservation)?;

        let current_segment_id = state.wal.current_segment_id;
        let sequence = state.sequence;
        let sst_seq = state
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .copied()
            .unwrap_or(1);
        let sst_name = crate::sst::file_name(cf_id, 0, sst_seq);

        // Get the column family (after stall check to avoid borrow issues)
        let Some(cf) = state.get_cf_mut(cf_id) else {
            if let Some(token) = flush_reservation {
                Self::release_flush_reservation(sba, token);
            }
            return Err(MidgeError::Internal(format!(
                "Column family {cf_id} not found"
            )));
        };

        // Freeze current memtable and create new one
        let frozen = std::mem::replace(
            &mut cf.memtable,
            std::sync::Arc::new(crate::sst::SkipListMemtable::new()),
        );
        cf.active_memtable_started_in_segment = current_segment_id;

        let Some(attempt) = state.track_new_immutable_flush(
            cf_id,
            std::sync::Arc::clone(&frozen),
            sst_name,
            sst_seq,
            sequence,
        ) else {
            if let Some(cf) = state.get_cf_mut(cf_id) {
                cf.memtable = frozen;
            }
            if let Some(token) = flush_reservation {
                Self::release_flush_reservation(sba, token);
            }
            return Err(MidgeError::Internal(format!(
                "Column family {cf_id} disappeared while freezing its memtable"
            )));
        };
        self.in_progress += 1;
        self.execute_tracked_flush(state, cf_id, &attempt, sba, flush_reservation)
    }

    fn execute_tracked_flush(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        attempt: &crate::runtime::state::ImmutableFlush,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) -> MidgeResult<FlushOutput> {
        let sst_path = state.sst_dir.join(&attempt.sst_name);
        let result = (|| {
            if let Some(parent) = sst_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Self::ensure_sst_sequence_reserved(state, cf_id, attempt.sst_seq)?;

            tracing::info!(
                cf_id,
                sst_name = %attempt.sst_name,
                failures = attempt.failures,
                "Flush started"
            );
            let publication = FlushPublication {
                cf_id,
                sst_name: &attempt.sst_name,
                sequence: attempt.sequence,
                frozen: &attempt.memtable,
                sst_path: &sst_path,
                reservation,
            };
            self.publish_flush_output(state, &publication, sba)
        })();

        if result.is_err() {
            self.fail_tracked_flush(state, cf_id, &attempt.memtable, sba, reservation);
        }
        result
    }

    fn ensure_sst_sequence_reserved(
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
    ) -> MidgeResult<()> {
        let next_seq = sst_seq.saturating_add(1);
        if state
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .is_some_and(|current| *current >= next_seq)
        {
            return Ok(());
        }

        crate::metadata::append_edit(
            &state.db_path,
            &crate::metadata::ManifestEdit::BumpNextSstSeq { cf_id, next_seq },
        )?;
        state.manifest.next_sst_seqs.insert(cf_id, next_seq);
        Ok(())
    }

    fn fail_tracked_flush(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        frozen: &std::sync::Arc<crate::sst::SkipListMemtable>,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) {
        self.in_progress = self.in_progress.saturating_sub(1);
        if let Some(token) = reservation {
            Self::release_flush_reservation(sba, token);
        }
        if let Some(retry_after) = state.mark_immutable_flush_failed(cf_id, frozen) {
            tracing::warn!(
                cf_id,
                retry_after_ms = retry_after.as_millis(),
                "Flush failed; frozen memtable retained for retry"
            );
        }
    }

    fn reserve_flush(
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        cf_id: crate::types::ColumnFamilyId,
        est_size: u64,
    ) -> MidgeResult<Option<crate::storage::hybrid::actor::StorageReservationToken>> {
        let Some(hybrid) = sba else {
            return Ok(None);
        };

        match hybrid.reserve_for_flush_with_token(est_size) {
            Ok(token) => Ok(Some(token)),
            Err(crate::storage::hybrid::actor::ReservationResult::WaitForCloudUpload) => {
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_write_stall_cloud();
                }
                tracing::warn!(cf_id, "Flush blocked: waiting for cloud upload");
                Err(MidgeError::WriteStall(
                    "flush blocked until cloud upload frees durable capacity".to_string(),
                ))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::WaitForCompaction) => {
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_write_stall_compaction();
                }
                tracing::warn!(cf_id, "Flush blocked: waiting for compaction");
                Err(MidgeError::WriteStall(
                    "flush blocked until compaction frees durable capacity".to_string(),
                ))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::RejectNoSpace) => {
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_no_space_event();
                    t.metrics().record_write_stall_no_space();
                }
                tracing::error!(cf_id, "Flush rejected: no disk space available");
                Err(MidgeError::NoSpace(
                    "no disk space available for flush".to_string(),
                ))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::Ok) => Err(MidgeError::Internal(
                "storage returned a successful reservation without a token".to_string(),
            )),
        }
    }

    fn ensure_immutable_capacity(
        state: &RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) -> MidgeResult<()> {
        if !state.is_immutable_memtable_queue_full(cf_id) {
            return Ok(());
        }

        let max_immutable = state.max_immutable_memtables;
        let immutable_count = state
            .get_cf(cf_id)
            .map_or(0, |cf| cf.immutable_memtables.len());
        tracing::warn!(
            cf_id,
            immutable_count,
            max_immutable,
            "Write stall: immutable memtable queue at capacity"
        );
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_write_stall_memory();
        }
        if let Some(token) = reservation {
            Self::release_flush_reservation(sba, token);
        }
        Err(MidgeError::WriteStall(format!(
            "immutable memtable queue full ({immutable_count}/{max_immutable}); flush in progress"
        )))
    }

    fn release_flush_reservation(
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        token: crate::storage::hybrid::actor::StorageReservationToken,
    ) {
        if let Some(hybrid) = sba {
            hybrid.flush_failed_with_token(token);
        }
    }

    fn publish_flush_output(
        &self,
        state: &mut RuntimeState,
        publication: &FlushPublication<'_>,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<FlushOutput> {
        let start = std::time::Instant::now();
        let mut file_meta = self.write_memtable_to_sst(
            publication.frozen,
            publication.sst_path,
            publication.cf_id,
            publication.sst_name,
        )?;
        file_meta.level = 0;
        let write_ms = start.elapsed().as_secs_f64() * 1000.0;

        state.record_flush_publication_intent(
            publication.cf_id,
            publication.sequence,
            file_meta.clone(),
        )?;

        crate::failpoints::fail_point!("midge::flush::after_sst_write_before_publish");

        if let Some(hybrid) = sba {
            let remote_bytes = std::fs::read(publication.sst_path)?;
            hybrid.write_sst_object(publication.sst_name, remote_bytes)?;
            tracing::debug!(
                cf_id = publication.cf_id,
                sst_name = %publication.sst_name,
                "SST mirrored to authoritative cloud storage"
            );
        }

        tracing::info!(cf_id = publication.cf_id, sst_name = %publication.sst_name, write_ms, "SST file written");

        Ok(FlushOutput {
            sst_name: publication.sst_name.to_string(),
            sequence: publication.sequence,
            file_meta: Some(file_meta),
            frozen_memtable: Some(std::sync::Arc::clone(publication.frozen)),
            reservation: publication.reservation,
        })
    }

    /// Write a frozen memtable to an SST file
    fn write_memtable_to_sst(
        &self,
        memtable: &std::sync::Arc<crate::sst::SkipListMemtable>,
        path: &Path,
        cf_id: crate::types::ColumnFamilyId,
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
        let range_tombstones = memtable.range_tombstones();
        if entries.is_empty() && range_tombstones.is_empty() {
            return Err(MidgeError::Corruption(
                "attempted to flush an empty memtable".to_string(),
            ));
        }

        // Manifest bounds are a conservative routing proof, so they must
        // cover both point entries and every range-tombstone endpoint. If a
        // tombstone falls outside the point-key bounds, narrowing metadata to
        // the points lets reads skip the SST and resurrect older values.
        let smallest_key = entries
            .iter()
            .map(|(key, _, _, _, _)| key.clone())
            .chain(
                range_tombstones
                    .iter()
                    .flat_map(|tombstone| [tombstone.start.clone(), tombstone.end.clone()]),
            )
            .min()
            .ok_or_else(|| MidgeError::Corruption("memtable flush missing smallest key".into()))?;
        let largest_key = entries
            .iter()
            .map(|(key, _, _, _, _)| key.clone())
            .chain(
                range_tombstones
                    .iter()
                    .flat_map(|tombstone| [tombstone.start.clone(), tombstone.end.clone()]),
            )
            .max()
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
        for tombstone in &range_tombstones {
            smallest_seq = smallest_seq.min(tombstone.seq);
            largest_seq = largest_seq.max(tombstone.seq);
            writer.add_range_tombstone(&tombstone.start, &tombstone.end, tombstone.seq)?;
        }
        let add_ns = add_start.elapsed().as_nanos();

        // Finish and write to path (helper records its own timing)
        let finish_start = std::time::Instant::now();
        crate::sst::fs::finish_writer_to_path(writer, path)?;
        let finish_ns = finish_start.elapsed().as_nanos();

        let add_duration_ms =
            std::time::Duration::from_nanos(u64::try_from(add_ns).unwrap_or(u64::MAX))
                .as_secs_f64()
                * 1000.0;
        let finish_duration_ms =
            std::time::Duration::from_nanos(u64::try_from(finish_ns).unwrap_or(u64::MAX))
                .as_secs_f64()
                * 1000.0;
        tracing::info!(path = ?path, added = added_count, add_duration_ms, finish_duration_ms, "memtable -> sst flush breakdown");

        let sst_bytes = std::fs::read(path)?;
        let size_bytes = sst_bytes.len() as u64;
        Ok(crate::runtime::FileMeta {
            name: sst_name.to_string(),
            level: 0,
            size_bytes,
            content_crc32c: Some(crc32c::crc32c(&sst_bytes)),
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
        cf_id: crate::types::ColumnFamilyId,
        sst_name: &str,
        sequence: u64,
        frozen_memtable: Option<&std::sync::Arc<crate::sst::SkipListMemtable>>,
    ) {
        if let Some(frozen_memtable) = frozen_memtable {
            if let Some(removed_size) = state.complete_immutable_flush(cf_id, frozen_memtable) {
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

    /// Abort a flush after the SST was written but before manifest
    /// publication. The frozen immutable remains queued for retry.
    pub fn handle_flush_failure(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        frozen_memtable: Option<&std::sync::Arc<crate::sst::SkipListMemtable>>,
    ) {
        self.in_progress = self.in_progress.saturating_sub(1);
        if let Some(frozen_memtable) = frozen_memtable {
            let _ = state.mark_immutable_flush_failed(cf_id, frozen_memtable);
        }
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
        // Arrange
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

        // Act
        // Assert
        assert_eq!(output.sst_name, "000000_00_00000000000000000001.sst");
        assert!(state.sst_dir.join(&output.sst_name).exists());

        Ok(())
    }

    #[test]
    fn should_retry_same_immutable_with_stable_identity_when_pending() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let db_path = temp.path().to_path_buf();
        let mut state = RuntimeState::new(db_path.clone(), false);
        let mut actor = FlushActor::new(
            &db_path.join("sst"),
            false,
            crate::sst::compression::CompressionPolicy::default(),
        )?;
        state.sequence = 7;
        state.get_cf(0).expect("default cf").memtable.put_with_seq(
            b"frozen".to_vec(),
            b"value".to_vec(),
            7,
            None,
        )?;

        let frozen = {
            let cf = state.get_cf_mut(0).expect("default cf");
            std::mem::replace(
                &mut cf.memtable,
                std::sync::Arc::new(crate::sst::SkipListMemtable::new()),
            )
        };
        let failed = state
            .track_new_immutable_flush(0, frozen, crate::sst::file_name(0, 0, 1), 1, 7)
            .expect("track failed immutable");
        state
            .mark_immutable_flush_failed(0, &failed.memtable)
            .expect("mark immutable pending");
        state.sequence = 8;
        state.get_cf(0).expect("default cf").memtable.put_with_seq(
            b"later".to_vec(),
            b"value".to_vec(),
            8,
            None,
        )?;

        // Act
        let retried = actor.handle_flush(&mut state, 0, None)?;

        // Assert
        assert_eq!(retried.sst_name, failed.sst_name);
        assert_eq!(retried.sequence, 7);
        assert!(std::sync::Arc::ptr_eq(
            retried
                .frozen_memtable
                .as_ref()
                .expect("retry frozen memtable"),
            &failed.memtable
        ));
        assert_eq!(
            state
                .get_cf(0)
                .expect("default cf")
                .immutable_memtables
                .len(),
            1,
            "retry must not freeze the newer active memtable"
        );

        Ok(())
    }
}
