//! Cloud-covered WAL pruning coordination.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::{CloudWalPruneGuard, HybridPersistence};

impl EventLoop {
    pub(crate) fn prune_cloud_wal_segments_covered_by_manifest(&mut self) {
        self.reap_cloud_wal_prune_worker();
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
        let candidates: Vec<_> = self
            .cloud_wal
            .acked_segments
            .iter()
            .filter(|(segment_id, max_sequence)| {
                **segment_id < recovery_floor_segment
                    && **max_sequence <= self.state.wal.cloud_durable_seq
                    && **max_sequence <= persisted_sequence
                    && !self.cloud_wal.prune_inflight.contains(segment_id)
            })
            .map(|(segment_id, max_sequence)| (*segment_id, *max_sequence))
            .collect();
        let candidate = candidates
            .iter()
            .copied()
            .find(|(segment_id, _)| {
                self.cloud_wal
                    .prune_cursor
                    .is_none_or(|cursor| *segment_id > cursor)
            })
            .or_else(|| candidates.first().copied());
        let Some((segment_id, max_sequence)) = candidate else {
            return;
        };
        // Advance before starting the attempt. A permanently unverifiable low
        // segment therefore cannot starve later independently eligible WALs.
        self.cloud_wal.prune_cursor = Some(segment_id);

        let metadata_snapshot = match self.cloud_metadata_prune_snapshot_for_wal_cleanup() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    segment_id,
                    error = %error,
                    "Skipping remote WAL prune because committed metadata could not be captured"
                );
                return;
            }
        };
        // Filesystem-backed cloud simulation has no separate control store;
        // its event-loop manifest is the authority snapshot guarded below.
        let local_manifest = self.state.manifest.clone();
        let writer_epoch = self.state.writer_epoch;
        self.cloud_wal.prune_inflight.insert(segment_id);
        self.publication_gate.active = true;

        let worker_storage = storage.clone();
        let worker = std::thread::Builder::new()
            .name(format!("midge-wal-prune-preflight-{segment_id}"))
            .spawn(move || {
                let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let deadline = crate::common::OperationDeadline::unbounded();
                    match metadata_snapshot {
                        Some(snapshot) => snapshot.verify_exact_then(|manifest, metadata_guard| {
                            worker_storage.prune_cloud_wal_segment_within(
                                segment_id,
                                max_sequence,
                                CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
                                writer_epoch,
                                &deadline,
                            )
                        }),
                        None => worker_storage.prune_cloud_wal_segment_within(
                            segment_id,
                            max_sequence,
                            CloudWalPruneGuard::new(local_manifest, None),
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

                if let Err(error) = attempt {
                    worker_storage
                        .queue_cloud_wal_prune_attempt_failed(segment_id, error.to_string());
                }
            });

        match worker {
            Ok(worker) => self.cloud_wal_prune_worker = Some(worker),
            Err(error) => {
                self.cloud_wal.prune_inflight.remove(&segment_id);
                self.publication_gate.active = false;
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    segment_id,
                    %error,
                    "Failed to start cloud WAL prune preflight worker"
                );
            }
        }
    }

    pub(super) fn reap_cloud_wal_prune_worker(&mut self) {
        if self
            .cloud_wal_prune_worker
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
        {
            self.join_cloud_wal_prune_worker();
            self.restore_publication_deferred_message();
            self.schedule_next_flush_worker();
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
        match std::fs::remove_file(&local_path) {
            Ok(()) => tracing::debug!(
                segment_id,
                path = %local_path.display(),
                "Removed cloud-durable local WAL segment"
            ),
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
