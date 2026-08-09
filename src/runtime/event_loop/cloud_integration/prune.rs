//! Cloud-covered WAL pruning coordination.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::{CloudWalPruneGuard, HybridPersistence};

const MAX_WAL_PRUNE_VALIDATIONS_PER_PASS: usize = 16;
const MAX_WAL_PRUNE_REQUESTS_PER_PASS: usize = 4;

impl EventLoop {
    pub(crate) fn prune_cloud_wal_segments_covered_by_manifest(&mut self) {
        if !self.wal_actor.is_cloud_async() || self.state.is_memory_mode() {
            return;
        }

        let Some(storage) = self.hybrid_storage.clone() else {
            return;
        };
        let Some(recovery_floor_segment) = self.state.cloud_wal_recovery_floor_segment() else {
            return;
        };
        let persisted_sequence = self.state.manifest.last_persisted_sequence;
        let now = std::time::Instant::now();

        let eligible_segments: Vec<(u64, u64)> = self
            .cloud_wal
            .acked_segments
            .iter()
            .filter(|(segment_id, max_sequence)| {
                **segment_id < recovery_floor_segment
                    && **max_sequence <= self.state.wal.cloud_durable_seq
                    && **max_sequence <= persisted_sequence
                    && !self.cloud_wal.prune_inflight.contains(segment_id)
                    && self
                        .cloud_wal
                        .prune_retries
                        .get(segment_id)
                        .is_none_or(|(_, retry_at)| *retry_at <= now)
            })
            .take(MAX_WAL_PRUNE_VALIDATIONS_PER_PASS)
            .filter_map(|(segment_id, max_sequence)| {
                let Ok(covered_by_manifest) = storage.remote_wal_segment_covered_by_manifest(
                    *segment_id,
                    *max_sequence,
                    &self.state.manifest,
                ) else {
                    return None;
                };

                covered_by_manifest.then_some((*segment_id, *max_sequence))
            })
            .take(MAX_WAL_PRUNE_REQUESTS_PER_PASS)
            .collect();

        if eligible_segments.is_empty() {
            return;
        }

        if let Err(error) = storage.verify_manifest_cloud_objects(&self.state.manifest) {
            self.state.mark_persistence_anomaly();
            tracing::warn!(
                error = %error,
                "Skipping remote WAL prune because manifest-referenced cloud objects are not fully readable"
            );
            return;
        }

        let metadata_guard =
            match self.cloud_metadata_prune_guard_for_wal_cleanup_with_convergence() {
                Ok(guard) => guard,
                Err(error) => {
                    self.state.mark_persistence_anomaly();
                    tracing::warn!(
                        error = %error,
                        "Skipping remote WAL prune because cloud metadata is not fully readable"
                    );
                    return;
                }
            };

        let prune_guard = CloudWalPruneGuard::new(self.state.manifest.clone(), metadata_guard);

        for (segment_id, max_sequence) in eligible_segments {
            self.cloud_wal.prune_inflight.insert(segment_id);
            if let Err(error) = storage.prune_cloud_wal_segment(
                segment_id,
                max_sequence,
                prune_guard.clone(),
                self.state.writer_epoch,
            ) {
                self.cloud_wal.prune_inflight.remove(&segment_id);
                if error.contains("at capacity") {
                    tracing::debug!(
                        segment_id,
                        error = %error,
                        "Cloud WAL prune admission is full; retaining segment for retry"
                    );
                } else {
                    self.state.mark_persistence_anomaly();
                    tracing::warn!(
                        segment_id,
                        error = %error,
                        "Failed to schedule cloud-covered remote WAL prune"
                    );
                }
            }
        }
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
