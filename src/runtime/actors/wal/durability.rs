// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::WalActor;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::wal::DurabilityPolicy;
use std::time::{Duration, Instant};

impl WalActor {
    pub(crate) fn restore_recovered_cloud_active_wal(
        &mut self,
        state: &mut RuntimeState,
        recovered: crate::runtime::RecoveredCloudActiveWal,
    ) -> MidgeResult<()> {
        if self.durability_policy != DurabilityPolicy::CloudAsync {
            return Err(MidgeError::Internal(
                "recovered cloud active WAL installed outside CloudAsync mode".to_string(),
            ));
        }
        if recovered.record_count == 0 || recovered.valid_bytes == 0 {
            return Err(MidgeError::RecoveryFailed(
                "recovered cloud active WAL metadata is empty".to_string(),
            ));
        }

        self.segment_max_sequence = recovered.max_sequence;
        self.pending_sync_count = recovered.record_count;
        self.bytes_since_sync = recovered.valid_bytes;
        state.wal.pending_writes = recovered.record_count;
        Ok(())
    }

    pub(super) fn apply_transaction_durability(
        &mut self,
        state: &mut RuntimeState,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
        begin_seq: u64,
    ) -> MidgeResult<()> {
        match effective_durability {
            DurabilityPolicy::Strict | DurabilityPolicy::CloudMirrored => {
                self.apply_strict_transaction_group_durability(state, last_sequence)?;
            }
            DurabilityPolicy::Batched => {
                state.pending_txn_min_seq = Some(
                    state
                        .pending_txn_min_seq
                        .map_or(begin_seq, |pending| pending.min(begin_seq)),
                );
                if state.pending_txn_start_time.is_none() {
                    state.pending_txn_start_time = Some(Instant::now());
                }
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_pending_txn_started();
                }
            }
            DurabilityPolicy::CloudAsync | DurabilityPolicy::BestEffort => {}
        }
        Ok(())
    }

    pub(super) fn apply_strict_transaction_group_durability(
        &mut self,
        state: &mut RuntimeState,
        last_sequence: u64,
    ) -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::wal::txn_after_commit_append_before_sync");
        self.sync_internal(state)?;
        state.wal.local_durable_seq = last_sequence;
        crate::failpoints::fail_point!("midge::wal::txn_after_sync_before_ack");
        Ok(())
    }

    /// Internal sync helper - fsyncs the writer
    pub(super) fn sync_internal(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        // Epoch fencing check: verify our epoch is still current before making
        // data durable.  If a newer writer has taken over, we must stop.
        if let Some(store) = &self.leader_store {
            store.validate_epoch(&self.leader_holder_id, self.current_epoch).map_err(|e| {
                tracing::error!(epoch = self.current_epoch, err = %e, "fenced at sync boundary");
                e
            })?;
        }

        if let Some(writer) = &mut self.writer {
            crate::failpoints::fail_point!("midge::wal::inject_no_space_on_sync", |_| Err(
                MidgeError::NoSpace(
                    "wal writer fsync failed: failpoint: no space on WAL sync".to_string()
                )
            ));

            // Bound the acknowledgement wait so a degraded storage device cannot
            // starve the event loop indefinitely.
            let start = Instant::now();
            let fsync_timeout = self.storage_io_timeout;

            let sync_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.sync_with_timeout(fsync_timeout)
            }));

            let elapsed = start.elapsed();

            // Handle panic or error from sync
            match sync_result {
                Ok(Ok(())) => {
                    // Success
                }
                Ok(Err(e)) => {
                    if matches!(e, MidgeError::NoSpace(_)) {
                        Self::record_no_space_event();
                    }
                    return Err(e);
                }
                Err(panic_info) => {
                    tracing::error!(
                        panic_info = ?panic_info,
                        "WAL fsync panic; returning error to unblock event loop"
                    );
                    return Err(crate::common::MidgeError::Internal(
                        "WAL fsync panicked".to_string(),
                    ));
                }
            }

            self.sync_calls += 1;
            self.sync_total += elapsed;
            self.last_sync_instant = Instant::now();

            // Record the logical WAL sync here. The writer runner owns the
            // physical fsync count and latency metrics at the filesystem call
            // boundary so one barrier is never counted at multiple layers.
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_sync();
            }

            if std::env::var_os("MIDGE_TRACE_WAL_SYNC").is_some()
                && self.sync_calls.is_multiple_of(1000)
            {
                let avg_ms =
                    (self.sync_total.as_secs_f64() * 1000.0) / Self::u64_to_f64(self.sync_calls);
                eprintln!(
                    "[midge] wal.sync: calls={} total_ms={:.2} avg_ms={:.3}",
                    self.sync_calls,
                    self.sync_total.as_secs_f64() * 1000.0,
                    avg_ms
                );
            }

            crate::failpoints::fail_point!("midge::wal::after_fsync_before_durable_frontier");
        }

        state.wal.last_synced_seq = state.sequence;
        state.wal.local_durable_seq = state.sequence;
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;

        Ok(())
    }

    /// Sync WAL to disk (public interface).
    ///
    /// Returns the sealed flush generation. In group commit modes (Batched/CloudAsync),
    /// all pending writes at sync time are grouped under this generation.
    pub fn sync(&mut self, state: &mut RuntimeState) -> MidgeResult<u64> {
        let pending = state.wal.pending_writes;
        let sealed_generation = self.flush_generation;

        self.sync_internal(state)?;

        // Advance to next generation for next batch
        self.flush_generation += 1;

        tracing::debug!(
            pending_writes = pending,
            synced_seq = state.wal.last_synced_seq,
            local_durable = state.wal.local_durable_seq,
            sealed_generation,
            "WAL sync"
        );

        Ok(sealed_generation)
    }

    /// Flush WAL buffers without fsync.
    ///
    /// `CloudAsync` durability uses local WAL as a staging file for upload.
    /// We avoid fsync on every write, but do a flush+fsync only when sealing
    /// a segment right before upload so the uploader reads a complete file.
    #[cfg(test)]
    pub fn flush_for_cloud_upload(&mut self, state: &mut RuntimeState) -> MidgeResult<u64> {
        self.flush_for_cloud_upload_within(state, &crate::common::OperationDeadline::unbounded())
    }

    /// Flush a cloud-staging WAL only when its complete configured I/O wait can
    /// still fit inside the shared operation deadline.
    ///
    /// The filesystem writer treats an in-progress flush timeout as a sticky
    /// write failure because completion is ambiguous. Refusing before it starts
    /// keeps a short caller budget from poisoning an otherwise healthy writer;
    /// the unchanged active segment can be retried by callerless maintenance.
    pub fn flush_for_cloud_upload_within(
        &mut self,
        state: &mut RuntimeState,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<u64> {
        let pending = state.wal.pending_writes;
        let segment_max_sequence = self.segment_max_sequence;

        if let Some(writer) = &mut self.writer {
            if deadline.is_bounded() && deadline.remaining() < self.storage_io_timeout {
                return Err(crate::common::MidgeError::Timeout(format!(
                    "insufficient operation budget for WAL flush: remaining={:?}, required={:?}",
                    deadline.remaining(),
                    self.storage_io_timeout
                )));
            }
            writer.flush()?;
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_flush();
            }
        }

        // Treat everything appended so far as ready-to-ship.
        state.wal.last_synced_seq = segment_max_sequence;
        state.wal.local_durable_seq = segment_max_sequence;

        tracing::debug!(
            pending_writes = pending,
            flushed_seq = state.wal.last_synced_seq,
            "WAL flush (CloudAsync upload)"
        );

        Ok(segment_max_sequence)
    }

    /// Clear buffered WAL accounting after a `CloudAsync` segment is successfully
    /// sealed, rotated, and enqueued for upload.
    pub fn complete_cloud_upload_seal(&mut self, state: &mut RuntimeState) {
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;
        self.segment_max_sequence = 0;
        self.last_sync_instant = Instant::now();
    }

    /// Check if batched sync should trigger
    pub fn should_sync_batch(&self) -> bool {
        if self.is_cloud_async() {
            return false;
        }
        // Time-based check (max_delay_ms) OR byte-count threshold
        let by_bytes = self.bytes_since_sync >= self.batch_config.max_bytes;
        let by_time = self.last_sync_instant.elapsed().as_millis()
            >= u128::from(self.batch_config.max_delay_ms);
        by_bytes || by_time
    }

    /// Return how long the event loop can sleep before the batched sync deadline.
    #[must_use]
    pub fn sync_deadline_timeout(&self) -> Option<Duration> {
        if self.is_cloud_async() || !self.has_pending_data() {
            return None;
        }

        let max_delay = Duration::from_millis(self.batch_config.max_delay_ms);
        Some(max_delay.saturating_sub(self.last_sync_instant.elapsed()))
    }

    /// Returns true if there is any buffered data awaiting sync.
    pub fn has_pending_data(&self) -> bool {
        self.bytes_since_sync > 0 || self.pending_sync_count > 0
    }

    /// Reset the sync timer without performing a sync.
    ///
    /// Used when the time-based threshold fires but there is nothing to sync,
    /// so the timer doesn't immediately re-trigger on the next tick.
    pub fn reset_sync_timer(&mut self) {
        self.last_sync_instant = Instant::now();
    }
}
