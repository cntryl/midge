//! Cloud-covered WAL pruning coordination.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::{CloudWalPruneGuard, HybridPersistence};

// Amortize catalog, metadata, and SST proof round trips across strict-write
// workloads that publish many small WAL segments. Keep the batch bounded so a
// maintenance worker cannot monopolize the publication gate indefinitely.
const CLOUD_WAL_PRUNE_BATCH_SIZE: usize = 32;

fn run_cloud_wal_prune_preflight(
    storage: &crate::storage::HybridStorage,
    candidates: &[(u64, u64)],
    candidate_ids: Vec<u64>,
    metadata_snapshot: Option<crate::runtime::hybrid_persistence::CloudMetadataPruneSnapshot>,
    local_guard: CloudWalPruneGuard,
    writer_epoch: u64,
    attempt_budget: std::time::Duration,
) {
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = crate::common::OperationDeadline::from_budget(attempt_budget);
        match metadata_snapshot {
            Some(snapshot) => snapshot.verify_exact_then(&deadline, |manifest, metadata_guard| {
                storage.prune_cloud_wal_segments_within(
                    candidates,
                    CloudWalPruneGuard::new(manifest, Some(metadata_guard))
                        .with_memory_limit(local_guard.memory_limit())
                        .with_progress(local_guard.progress()),
                    writer_epoch,
                    &deadline,
                )
            }),
            None => storage.prune_cloud_wal_segments_within(
                candidates,
                local_guard,
                writer_epoch,
                &deadline,
            ),
        }
    }))
    .unwrap_or_else(|_| {
        Err(crate::common::MidgeError::Internal(
            "cloud WAL prune preflight panicked".to_string(),
        ))
    });

    match attempt {
        Ok(results) => {
            for (segment_id, result) in results {
                match result {
                    Ok(()) => storage.queue_cloud_wal_prune_complete(
                        segment_id,
                        crate::storage::StorageOutcome::Ok(()),
                    ),
                    Err(error) => {
                        storage.queue_cloud_wal_prune_attempt_failed(segment_id, error.to_string());
                    }
                }
            }
        }
        Err(error) => {
            for segment_id in candidate_ids {
                storage.queue_cloud_wal_prune_attempt_failed(segment_id, error.to_string());
            }
        }
    }
}

impl EventLoop {
    pub(crate) fn prune_cloud_wal_segments_covered_by_manifest(&mut self) {
        self.reap_cloud_wal_prune_worker();
        if self.cloud_maintenance_enabled() && !self.cloud_maintenance.dispatching {
            self.schedule_cloud_maintenance();
            return;
        }
        // Reaping may restore a control request that was deferred behind the
        // publication gate. Give the run loop a chance to dispatch it before
        // another maintenance prune reacquires the gate; otherwise a steady
        // stream of eligible WAL segments can defer the same request forever.
        if self.pending_msg.is_some() || !self.publication_gate.deferred_messages.is_empty() {
            return;
        }
        if !self.wal_actor.is_cloud_async() || self.state.is_memory_mode() {
            return;
        }

        // A flush publication mutates the local control files on its worker.
        // Start only between publication phases, then own the gate until the
        // worker has verified metadata and retired catalog authority.
        let layout_publication_active = self
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
            || !self.state.compaction.compacting_ssts.is_empty()
            || self.flush_actor.is_inflight();
        if self.cloud_wal_prune_worker.is_some()
            || self.publication_gate.active
            || layout_publication_active
        {
            return;
        }

        let Some(storage) = self.hybrid_storage.clone() else {
            return;
        };
        let Some(recovery_floor_segment) = self.state.cloud_wal_recovery_floor_segment() else {
            return;
        };
        let persisted_sequence = self.state.manifest.last_persisted_sequence;
        let candidates =
            self.next_cloud_wal_prune_candidates(recovery_floor_segment, persisted_sequence);
        if candidates.is_empty() {
            return;
        }
        let metadata_snapshot = self.cloud_metadata_prune_snapshot_for_wal_cleanup();
        // Filesystem-backed cloud simulation has no separate control store;
        // its event-loop manifest is the authority snapshot guarded below.
        let local_guard = CloudWalPruneGuard::new(self.state.manifest.clone(), None)
            .with_memory_limit(self.compaction_actor.compaction_memory_limit())
            .with_progress(self.cloud_wal_prune_progress.clone());
        let writer_epoch = self.state.writer_epoch;
        // This callerless attempt has retry ownership, but shutdown must still
        // be able to join it within the cloud drain window. Starting the budget
        // here also bounds the whole multi-proof sequence rather than granting
        // each provider callback a fresh timeout.
        let attempt_budget = self
            .runtime_response_timeout
            .min(self.shutdown_cloud_drain_timeout);
        for (segment_id, _) in &candidates {
            self.cloud_wal.prune_inflight.insert(*segment_id);
        }
        self.publication_gate.active = true;

        let candidate_ids = candidates
            .iter()
            .map(|(segment_id, _)| *segment_id)
            .collect::<Vec<_>>();
        let worker_name = format!(
            "midge-wal-prune-preflight-{}-{}",
            candidate_ids.first().copied().unwrap_or_default(),
            candidate_ids.last().copied().unwrap_or_default()
        );
        let worker_candidate_ids = candidate_ids.clone();
        let worker = std::thread::Builder::new()
            .name(worker_name)
            .spawn(move || {
                run_cloud_wal_prune_preflight(
                    storage.as_ref(),
                    &candidates,
                    worker_candidate_ids,
                    metadata_snapshot,
                    local_guard,
                    writer_epoch,
                    attempt_budget,
                );
            });

        match worker {
            Ok(worker) => self.cloud_wal_prune_worker = Some(worker),
            Err(error) => {
                for segment_id in candidate_ids {
                    self.cloud_wal.prune_inflight.remove(&segment_id);
                }
                self.publication_gate.active = false;
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    %error,
                    "Failed to start cloud WAL prune preflight worker"
                );
            }
        }
    }

    fn next_cloud_wal_prune_candidates(
        &self,
        recovery_floor_segment: u64,
        persisted_sequence: u64,
    ) -> Vec<(u64, u64)> {
        // Retire catalog authority only as an oldest-first prefix. A newer
        // record may mask an older authoritative WAL record for the same key;
        // removing the newer segment across a gap can resurrect that older
        // state after a later tombstone/TTL compaction.
        self.cloud_wal
            .acked_segments
            .iter()
            .take_while(|(segment_id, max_sequence)| {
                **segment_id < recovery_floor_segment
                    && **max_sequence <= self.state.wal.cloud_durable_seq
                    && **max_sequence <= persisted_sequence
                    && !self.cloud_wal.prune_inflight.contains(segment_id)
            })
            .take(CLOUD_WAL_PRUNE_BATCH_SIZE)
            .map(|(segment_id, max_sequence)| (*segment_id, *max_sequence))
            .collect()
    }

    pub(in crate::runtime::event_loop) fn reap_cloud_wal_prune_worker(&mut self) {
        if self
            .cloud_wal_prune_worker
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
        {
            self.join_cloud_wal_prune_worker();
            self.restore_publication_deferred_message();
            if !self.cloud_maintenance_enabled() {
                self.schedule_next_flush_worker();
            }
        }
    }

    pub(in crate::runtime::event_loop) fn join_cloud_wal_prune_worker(&mut self) {
        if let Some(worker) = self.cloud_wal_prune_worker.take() {
            if worker.join().is_err() {
                self.state.mark_persistence_anomaly();
                tracing::warn!("cloud WAL prune preflight worker panicked during join");
            }
            self.publication_gate.active = false;
        }
    }
}

impl Drop for EventLoop {
    fn drop(&mut self) {
        self.join_cloud_wal_prune_worker();
    }
}

impl EventLoop {
    pub(in crate::runtime::event_loop) fn remove_cloud_durable_local_wal_segment(
        &mut self,
        segment_id: u64,
    ) {
        if self.state.is_memory_mode() {
            return;
        }

        let local_path = self
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        let local_bytes = std::fs::metadata(&local_path)
            .ok()
            .map(|metadata| metadata.len());
        match std::fs::remove_file(&local_path) {
            Ok(()) => {
                if let (Some(storage), Some(bytes)) = (&self.hybrid_storage, local_bytes) {
                    storage.release_local_wal_bytes(bytes);
                }
                tracing::debug!(
                segment_id,
                path = %local_path.display(),
                "Removed cloud-durable local WAL segment"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    segment_id,
                    path = %local_path.display(),
                    error = %error,
                    "Failed to remove cloud-durable local WAL segment; recovery remains safe but storage may leak"
                );
            }
        }
    }
}
