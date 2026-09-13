// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::{WalActor, WalSyncReceipt, WalTransitionOperation};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_transition::{WalSealTicket, WalSyncTicket};
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
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
                state.begin_pending_transaction(begin_seq);
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
        if let Err(error) = self.sync_internal(state) {
            // The strict records are already on disk but not yet in memory.
            // Continuing as `Open` would let the runtime view and the recovery
            // view diverge, so even a failure before the fsync call fences.
            if !self.is_fenced() {
                self.fence_transition(
                    state,
                    format!("strict transaction sync failed after WAL append: {error}"),
                );
            }
            return Err(error);
        }
        state.wal.local_durable_seq = last_sequence;
        crate::failpoints::fail_point!("midge::wal::txn_after_sync_before_ack");
        Ok(())
    }

    /// Actor-internal durability barrier for strict transaction groups.
    ///
    /// This never touches the durability coordinator generation, so it needs
    /// no protocol ticket: the records are made durable and the frontier moves,
    /// but waiters keyed to the current generation stay pending until the
    /// paired event-loop sync closes that generation.
    pub(super) fn sync_internal(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let receipt = self.begin_sync_io(state)?;
        self.commit_sync_io(state, receipt)
    }

    /// First half of the paired sync transition: fsync the writer.
    ///
    /// Requires a [`WalSyncTicket`], which only the transition protocol can
    /// mint, so a frontier-advancing fsync cannot run outside the paired
    /// event-loop operation. On return the actor is either `Open` (nothing
    /// irreversible happened, retryable) or `Fenced`.
    pub(crate) fn begin_sync_transition(
        &mut self,
        state: &mut RuntimeState,
        _ticket: &WalSyncTicket,
    ) -> MidgeResult<WalSyncReceipt> {
        let result = self.begin_sync_io(state);
        debug_assert!(
            result.is_ok() || self.is_open() || self.is_fenced(),
            "a failed WAL sync must settle in Open or Fenced"
        );
        result
    }

    /// Second half of the paired sync transition: publish the receipt.
    pub(crate) fn commit_sync_transition(
        &mut self,
        state: &mut RuntimeState,
        receipt: WalSyncReceipt,
        _ticket: &WalSyncTicket,
    ) -> MidgeResult<()> {
        self.commit_sync_io(state, receipt)
    }

    fn begin_sync_io(&mut self, state: &mut RuntimeState) -> MidgeResult<WalSyncReceipt> {
        self.ensure_filesystem_wal_available(state)?;

        // Epoch fencing check: verify our epoch is still current before making
        // data durable.  If a newer writer has taken over, we must stop.
        if let Some(store) = &self.leader_store {
            if let Err(error) = store.validate_epoch(&self.leader_holder_id, self.current_epoch) {
                let error = MidgeError::from(error);
                tracing::error!(epoch = self.current_epoch, err = %error, "fenced at sync boundary");
                // A failed authority proof is not a retryable pre-fsync I/O
                // error: this writer may already be stale. Keep it from
                // accepting more WAL-backed work while the event loop pairs
                // this actor fence with the transition protocol and waiters.
                self.fence_transition(
                    state,
                    format!("WAL writer authority validation failed: {error}"),
                );
                return Err(error);
            }
        }

        self.begin_io_transition(WalTransitionOperation::Sync)?;

        if let Err(error) = WalTransitionBoundary::BeforeFsync.check() {
            self.finish_io_transition()?;
            return Err(error);
        }

        if let Err(error) = Self::sync_failure_boundary() {
            // An idle durability barrier has no uncommitted WAL-backed state,
            // so a synchronous failure before the filesystem call is safe to
            // retry. Once records are pending, the same failure is ambiguous:
            // strict transactions have been appended but are not yet visible
            // in memory, and continuing could make runtime and recovery views
            // diverge.
            if self.has_pending_data() {
                self.fence_transition(state, error.to_string());
            } else {
                self.finish_io_transition()?;
            }
            return Err(error);
        }

        if self.writer().is_some() {
            // Bound the acknowledgement wait so a degraded storage device cannot
            // starve the event loop indefinitely.
            let start = Instant::now();
            let fsync_timeout = self.storage_io_timeout;

            let sync_result = {
                let writer = self.writer_mut().ok_or_else(|| {
                    MidgeError::Fenced("WAL writer disappeared during fsync".to_string())
                })?;
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    writer.sync_with_timeout(fsync_timeout)
                }))
            };

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
                    self.fence_transition(state, format!("WAL fsync failed: {e}"));
                    return Err(e);
                }
                Err(panic_info) => {
                    tracing::error!(
                        panic_info = ?panic_info,
                        "WAL fsync panic; returning error to unblock event loop"
                    );
                    let error =
                        crate::common::MidgeError::Internal("WAL fsync panicked".to_string());
                    self.fence_transition(state, error.to_string());
                    return Err(error);
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

            if let Err(error) = Self::after_fsync_boundary() {
                self.fence_transition(state, error.to_string());
                return Err(error);
            }
            if let Err(error) = WalTransitionBoundary::AfterFsync.check() {
                self.fence_transition(state, error.to_string());
                return Err(error);
            }
        }

        Ok(WalSyncReceipt {
            durable_sequence: state.sequence,
            pending_writes: state.wal.pending_writes,
        })
    }

    fn commit_sync_io(
        &mut self,
        state: &mut RuntimeState,
        receipt: WalSyncReceipt,
    ) -> MidgeResult<()> {
        if state.sequence != receipt.durable_sequence
            || state.wal.pending_writes != receipt.pending_writes
        {
            let error = MidgeError::Fenced(
                "WAL state changed while a sync receipt awaited commit".to_string(),
            );
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        self.finish_io_transition().inspect_err(|error| {
            state.mark_persistence_anomaly();
            tracing::error!(%error, "WAL sync could not return to open state");
        })?;
        state.wal.last_synced_seq = receipt.durable_sequence;
        state.wal.local_durable_seq = receipt.durable_sequence;
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;

        Ok(())
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn sync_failure_boundary() -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::wal::inject_no_space_on_sync", |_| Err(
            MidgeError::NoSpace(
                "wal writer fsync failed: failpoint: no space on WAL sync".to_string()
            )
        ));
        Ok(())
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn after_fsync_boundary() -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::wal::after_fsync_before_durable_frontier", |_| Err(
            MidgeError::Internal(
                "failpoint: WAL fsync completed before logical commit".to_string()
            )
        ));
        Ok(())
    }

    /// Flush WAL buffers without fsync.
    ///
    /// `CloudAsync` durability uses local WAL as a staging file for upload.
    /// We avoid fsync on every write, but do a flush+fsync only when sealing
    /// a segment right before upload so the uploader reads a complete file.
    #[cfg(test)]
    pub fn flush_for_cloud_upload(
        &mut self,
        state: &mut RuntimeState,
        ticket: &WalSealTicket,
    ) -> MidgeResult<u64> {
        self.flush_for_cloud_upload_within(
            state,
            &crate::common::OperationDeadline::unbounded(),
            ticket,
        )
    }

    /// Flush a cloud-staging WAL only when its complete configured I/O wait can
    /// still fit inside the shared operation deadline.
    ///
    /// The filesystem writer treats an in-progress flush timeout as a sticky
    /// write failure because completion is ambiguous. Refusing before it starts
    /// keeps a short caller budget from poisoning an otherwise healthy writer;
    /// the unchanged active segment can be retried by callerless maintenance.
    ///
    /// Requires a [`WalSealTicket`]: the flush leaves the actor in the
    /// `CloudFlush` transition until the paired rotate or cancel, so it may
    /// only run inside a protocol-owned seal.
    pub fn flush_for_cloud_upload_within(
        &mut self,
        state: &mut RuntimeState,
        deadline: &crate::common::OperationDeadline,
        _ticket: &WalSealTicket,
    ) -> MidgeResult<u64> {
        self.ensure_filesystem_wal_available(state)?;
        let pending = state.wal.pending_writes;
        let segment_max_sequence = self.segment_max_sequence;

        self.begin_io_transition(WalTransitionOperation::CloudFlush)?;

        if self.writer().is_some() {
            if deadline.is_bounded() && deadline.remaining() < self.storage_io_timeout {
                self.finish_io_transition()?;
                return Err(crate::common::MidgeError::Timeout(format!(
                    "insufficient operation budget for WAL flush: remaining={:?}, required={:?}",
                    deadline.remaining(),
                    self.storage_io_timeout
                )));
            }
            let flush_result = self
                .writer_mut()
                .ok_or_else(|| MidgeError::Fenced("WAL writer disappeared during flush".into()))?
                .flush();
            if let Err(error) = flush_result {
                self.fence_transition(state, format!("WAL flush failed: {error}"));
                return Err(error);
            }
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_flush();
            }
        }

        tracing::debug!(
            pending_writes = pending,
            flushed_seq = segment_max_sequence,
            "WAL flush (CloudAsync upload)"
        );

        Ok(segment_max_sequence)
    }

    /// Clear buffered WAL accounting after a `CloudAsync` segment is sealed and
    /// owned by the upload backlog or inflight frontier.
    ///
    /// The rotation receipt is the proof of ownership transfer: it exists only
    /// after `rotate` sealed the file, so accounting cannot be reset for a
    /// segment that is still the active WAL.
    pub(crate) fn complete_cloud_upload_seal(
        &mut self,
        state: &mut RuntimeState,
        receipt: super::WalRotationReceipt,
    ) {
        let max_sequence = receipt.max_sequence();
        state.wal.last_synced_seq = max_sequence;
        state.wal.local_durable_seq = max_sequence;
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
