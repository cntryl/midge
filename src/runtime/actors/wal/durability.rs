// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::WalActor;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::wal::DurabilityPolicy;
use std::time::{Duration, Instant};

impl WalActor {
    pub(super) fn apply_transaction_durability(
        &mut self,
        state: &mut RuntimeState,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
        begin_seq: u64,
    ) -> MidgeResult<()> {
        match effective_durability {
            DurabilityPolicy::Strict | DurabilityPolicy::CloudMirrored => {
                crate::failpoints::fail_point!("midge::wal::txn_after_commit_append_before_sync");
                self.sync_internal(state)?;
                state.wal.local_durable_seq = last_sequence;
                crate::failpoints::fail_point!("midge::wal::txn_after_sync_before_ack");
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

    /// Internal sync helper - fsyncs the writer
    pub(super) fn sync_internal(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        // Epoch fencing check: verify our epoch is still current before making
        // data durable.  If a newer writer has taken over, we must stop.
        if let Some(store) = &self.leader_store {
            store.validate_epoch(self.current_epoch).map_err(|e| {
                tracing::error!(epoch = self.current_epoch, err = %e, "fenced at sync boundary");
                e
            })?;
        }

        if let Some(writer) = &mut self.writer {
            crate::failpoints::fail_point!("midge::wal::inject_no_space_on_sync", |_| Err(
                MidgeError::NoSpace("failpoint: no space on WAL sync".to_string())
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
    pub fn flush_for_cloud_upload(&mut self, state: &mut RuntimeState) -> MidgeResult<u64> {
        let pending = state.wal.pending_writes;
        let segment_max_sequence = self.segment_max_sequence;

        if let Some(writer) = &mut self.writer {
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
        // Time-based check (max_delay_ms) OR byte-count threshold
        let by_bytes = self.bytes_since_sync >= self.batch_config.max_bytes;
        let by_time = self.last_sync_instant.elapsed().as_millis()
            >= u128::from(self.batch_config.max_delay_ms);
        by_bytes || by_time
    }

    /// Return how long the event loop can sleep before the batched sync deadline.
    #[must_use]
    pub fn sync_deadline_timeout(&self) -> Option<Duration> {
        if !self.has_pending_data() {
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
