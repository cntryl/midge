//! Cloud WAL segment sealing and flush scheduling.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::HybridPersistence;
use std::time::Instant;

impl EventLoop {
    pub(crate) fn seal_current_cloud_segment(
        &mut self,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        self.seal_current_cloud_segment_inner(false, &crate::common::OperationDeadline::unbounded())
    }

    pub(in crate::runtime::event_loop) fn seal_current_cloud_segment_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        self.seal_current_cloud_segment_inner(false, deadline)
    }

    pub(in crate::runtime::event_loop) fn seal_recovered_cloud_active_segment(
        &mut self,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        self.seal_current_cloud_segment_inner(true, &crate::common::OperationDeadline::unbounded())
    }

    fn seal_current_cloud_segment_inner(
        &mut self,
        recovered_active: bool,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        let result = self.try_seal_current_cloud_segment(recovered_active, deadline);
        if result.is_err() && self.durability.cloud_seal_retry_needed() {
            self.durability.defer_cloud_seal_retry();
        }
        result
    }

    fn try_seal_current_cloud_segment(
        &mut self,
        recovered_active: bool,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        if !self.wal_actor.is_cloud_async() {
            return Ok(None);
        }
        let Some(storage) = self.hybrid_storage.clone() else {
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync requires HybridStorage".to_string(),
            ));
        };
        if self.state.is_memory_mode() || self.state.wal.pending_writes == 0 {
            return Ok(None);
        }
        if !recovered_active && !self.cloud_wal.upload_backlog.is_empty() {
            return Err(crate::common::MidgeError::WriteStall(
                "older CloudAsync WAL segments are still awaiting upload admission".to_string(),
            ));
        }
        self.durability.mark_cloud_seal_retry_needed();
        self.validate_runtime_writer_lease_within(deadline)?;

        let segment_id = self.state.wal.current_segment_id;
        let bytes_buffered = self.wal_actor.bytes_since_sync() as u64;
        let local_path = self
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        let existing_bytes = std::fs::metadata(&local_path).map_or(0, |metadata| metadata.len());
        if !recovered_active {
            storage.ensure_wal_upload_capacity(existing_bytes.max(bytes_buffered))?;
        }
        let seal_start = Instant::now();
        let max_sequence = self
            .wal_actor
            .flush_for_cloud_upload_within(&mut self.state, deadline)?;
        crate::failpoints::fail_point!(
            "midge::cloud::inject_fail_after_wal_flush_before_rotate",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: cloud seal failed after WAL flush before rotate".to_string(),
            ))
        );
        // Revalidate after the potentially slow flush but before the active
        // file is irreversibly rotated. A failure here leaves the same active
        // segment intact for a later retry.
        self.validate_runtime_writer_lease_within(deadline)?;
        if let Err(error) = self.wal_actor.rotate(&mut self.state) {
            tracing::error!(error = %error, "CloudAsync: WAL rotate failed");
            return Err(error);
        }

        self.durability
            .rotate_from_to(segment_id, self.state.wal.current_segment_id)?;
        self.durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        self.cloud_wal
            .upload_backlog
            .insert(segment_id, max_sequence);
        self.wal_actor.complete_cloud_upload_seal(&mut self.state);
        self.durability.record_cloud_flush();
        self.durability.clear_cloud_seal_retry_needed();

        // From this point on the sealed file is a tracked obligation. Even a
        // lease or queue failure cannot make a later segment skip it.
        crate::failpoints::fail_point!(
            "midge::cloud::inject_fail_after_wal_rotate_before_enqueue",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: cloud seal failed after WAL rotate before enqueue".to_string(),
            ))
        );
        self.try_drain_cloud_wal_upload_backlog_within(deadline)?;

        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_async_wal_segment_sealed(
                bytes_buffered,
                Self::elapsed_micros_to_u64(seal_start.elapsed()),
            );
        }

        Ok(Some((segment_id, max_sequence)))
    }

    fn try_drain_cloud_wal_upload_backlog_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let Some(storage) = self.hybrid_storage.clone() else {
            if self.cloud_wal.upload_backlog.is_empty() {
                return Ok(());
            }
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync WAL upload backlog requires HybridStorage".to_string(),
            ));
        };

        if self.cloud_wal.upload_backlog.is_empty() {
            return Ok(());
        }
        if !self.cloud_wal.uploads_ready() {
            return Ok(());
        }
        self.cloud_wal.begin_upload_attempt();
        // Validate once per drain pass, not once per segment. Under a provider
        // leader store each validation is a cloud GET, and the durable guarantee
        // is not this poll: `WalPublicationCatalog::require_epoch` plus the
        // catalog compare-exchange reject a fenced writer's publish at the object
        // store regardless. This check is a fast-fail, so paying for it per
        // segment buys nothing and multiplies the stall on a deep backlog.
        self.validate_runtime_writer_lease_within(deadline)?;

        loop {
            let Some((&segment_id, &max_sequence)) =
                self.cloud_wal.upload_backlog.first_key_value()
            else {
                return Ok(());
            };
            let local_path = self
                .state
                .wal_dir
                .join(crate::wal::segment_file_name(segment_id));
            let resource = match storage.enqueue_wal_segment(segment_id, &local_path, max_sequence)
            {
                Ok(resource) => resource,
                Err(crate::common::MidgeError::WriteStall(_)) => {
                    // Capacity remains owned by already admitted uploads. Keep
                    // this segment in the runtime backlog and avoid repeatedly
                    // paying lease validation and WAL readback while the queue
                    // is still full.
                    self.cloud_wal.defer_upload_retry();
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            self.cloud_wal.upload_backlog.remove(&segment_id);
            if !self.state.cloud.pending_uploads.contains(&resource) {
                self.state.cloud.pending_uploads.push(resource);
            }
        }
    }

    pub(in crate::runtime::event_loop) fn drain_cloud_wal_upload_backlog(&mut self) {
        let deadline = crate::common::OperationDeadline::from_budget(self.runtime_response_timeout);
        self.drain_cloud_wal_upload_backlog_within(&deadline);
    }

    pub(in crate::runtime::event_loop) fn drain_cloud_wal_upload_backlog_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) {
        if let Err(error) = self.try_drain_cloud_wal_upload_backlog_within(deadline) {
            self.cloud_wal.defer_upload_retry();
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "could not admit recovered CloudAsync WAL upload");
        }
    }
}

impl EventLoop {
    pub(crate) fn maybe_flush_cloud_async_wal(&mut self) {
        if !self.wal_actor.is_cloud_async() {
            return;
        }
        if self.hybrid_storage.is_none() {
            return;
        }

        if self.state.is_memory_mode() {
            return;
        }

        // No pending local records to ship.
        if self.state.wal.pending_writes == 0 {
            return;
        }

        let pending_writes = self.state.wal.pending_writes;
        let bytes_buffered = self.wal_actor.bytes_since_sync();

        if !self
            .durability
            .should_flush_cloud_async(pending_writes, bytes_buffered)
        {
            return;
        }

        let seal_result = self.seal_current_cloud_segment();
        let Ok(Some((segment_id, max_sequence))) = seal_result else {
            if let Err(error) = seal_result {
                tracing::error!(error = %error, "CloudAsync: forced WAL seal failed");
            }
            return;
        };

        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some() {
            // Throttle: log every 1000 segments to avoid noise.
            if segment_id.is_multiple_of(1000) {
                eprintln!(
                    "[midge] CloudAsync flush: segment_id={segment_id} max_sequence={max_sequence} pending_cloud={} ",
                    self.state.wal.pending_writes > 0
                );
            }
        }

        self.drain_auto_flush_memtables();
    }
}
