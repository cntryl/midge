// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::{WalActor, WalSyncReceipt, WalTransitionOperation};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_transition::{WalSealTicket, WalSyncTicket};
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crate::wal::DurabilityPolicy;
use std::time::{Duration, Instant};

impl WalActor {
    pub(super) fn apply_transaction_durability(
        &mut self,
        state: &mut RuntimeState,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
    ) -> MidgeResult<()> {
        match effective_durability {
            DurabilityPolicy::Strict => {
                self.apply_strict_transaction_group_durability(state, last_sequence)?;
            }
            DurabilityPolicy::Batched => {
                state.begin_pending_transaction();
                self.counters.record(|m| {
                    m.record_pending_txn_started();
                });
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
        self.apply_strict_group_durability_with_group(state, last_sequence, None)
    }

    pub(super) fn apply_strict_group_durability_with_group(
        &mut self,
        state: &mut RuntimeState,
        last_sequence: u64,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::wal::txn_after_commit_append_before_sync");
        if let Err(error) = self.sync_internal_with_group(state, group) {
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
        state.wal.frontiers.advance_local_to(last_sequence);
        crate::failpoints::fail_point!("midge::wal::txn_after_sync_before_ack");
        Ok(())
    }

    /// Actor-internal durability barrier for strict transaction groups.
    ///
    /// This never touches the durability coordinator generation, so it needs
    /// no protocol ticket: the records are made durable and the frontier moves,
    /// but waiters keyed to the current generation stay pending until the
    /// paired event-loop sync closes that generation.
    fn sync_internal_with_group(
        &mut self,
        state: &mut RuntimeState,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<()> {
        let receipt = self.begin_sync_io(state, group)?;
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
        ticket: &WalSyncTicket,
    ) -> MidgeResult<WalSyncReceipt> {
        let result = self.begin_sync_io(state, None).map(|mut receipt| {
            receipt.generation = Some(ticket.generation());
            receipt
        });
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
        ticket: &WalSyncTicket,
    ) -> MidgeResult<()> {
        if receipt.generation != Some(ticket.generation()) {
            let error = MidgeError::Internal(
                "WAL sync receipt belongs to a different barrier generation".into(),
            );
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        self.commit_sync_io(state, receipt)
    }

    pub(super) fn validate_sync_authority(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let admission_prevalidated =
            std::mem::replace(&mut self.sync_authority_prevalidated, false);
        self.ensure_lease_validity()?;
        self.ensure_filesystem_wal_available(state)?;

        // A strict transaction group carries its provider proof from admission
        // directly to this sync. Other syncs refresh authority once here; the
        // physical sync remains guarded by `with_writer_epoch` in either case.
        if !admission_prevalidated {
            if let Some(store) = &self.leader_store {
                if let Err(error) = store.validate_epoch_before_sync(
                    &self.leader_holder_id,
                    self.current_epoch,
                    self.storage_io_timeout,
                ) {
                    let error = error.into_writer_effect_error("WAL sync authority validation");
                    tracing::error!(epoch = self.current_epoch, err = %error, "fenced at sync boundary");
                    // A failed authority proof is not a retryable pre-fsync I/O
                    // error: this writer may already be stale. Keep it from
                    // accepting more WAL-backed work while the event loop pairs
                    // this actor fence with the transition protocol and waiters.
                    self.fence_with_cause(
                        state,
                        format!("WAL writer authority validation failed: {error}"),
                        true,
                    );
                    return Err(error);
                }
            }
        }

        self.ensure_lease_validity()
    }

    fn begin_sync_io(
        &mut self,
        state: &mut RuntimeState,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<WalSyncReceipt> {
        self.validate_sync_authority(state)?;
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

        // Capture the exact admitted prefix before I/O. Future queued work
        // must never be included by reading mutable writer/state fields after
        // the physical barrier returns.
        let receipt = self.capture_sync_receipt(state);

        if self.writer().is_some() {
            // Bound the acknowledgement wait so a degraded storage device cannot
            // starve the event loop indefinitely.
            let start = Instant::now();
            let fsync_timeout = self.storage_io_timeout;

            let sync_result = {
                let writer = self.writer_mut().ok_or_else(|| {
                    MidgeError::Internal("WAL writer disappeared during fsync".to_string())
                })?;
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match group {
                    Some(group) => writer
                        .writer_group_capability()
                        .ok_or_else(|| {
                            MidgeError::Internal("strict WAL group lost its sync capability".into())
                        })?
                        .sync_in_group(fsync_timeout, group),
                    None => writer.sync_with_timeout(fsync_timeout),
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
                        Self::record_no_space_event(&self.counters);
                    }
                    self.fence_with_cause(
                        state,
                        format!("WAL fsync failed: {e}"),
                        Self::is_authority_failure(&e),
                    );
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
            self.counters.record(|m| {
                m.record_wal_sync();
                m.record_wal_fsync_latency(elapsed);
            });

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

        self.persist_sync_boundary(state, group, receipt.end_offset)?;
        Ok(receipt)
    }

    pub(super) fn capture_sync_receipt(&self, state: &RuntimeState) -> WalSyncReceipt {
        WalSyncReceipt {
            durable_sequence: self
                .writer()
                .and_then(|writer| writer.tail().last_seq)
                .unwrap_or(state.sequence),
            pending_writes: state.wal.pending_writes,
            pending_sync_count: self.pending_sync_count,
            bytes_since_sync: self.bytes_since_sync,
            end_offset: self.writer().map(crate::wal::WalWriter::current_pos),
            writer_epoch: self.current_epoch,
            segment_id: state.wal.open_segment(),
            generation: None,
        }
    }

    pub(super) fn persist_sync_boundary(
        &mut self,
        state: &mut RuntimeState,
        group: Option<&crate::lease::WriterEffectGroup>,
        end_offset: Option<u64>,
    ) -> MidgeResult<()> {
        if let Some(bytes) = end_offset {
            let recorded = match group {
                Some(group) => state.wal.history.record_sync_in_group(bytes, group),
                None => state.wal.history.record_sync(bytes),
            };
            if let Err(error) = recorded {
                self.fence_transition(
                    state,
                    format!("failed to persist WAL sync boundary: {error}"),
                );
                return Err(Self::appended_unit_error(error));
            }
        }
        Ok(())
    }

    fn commit_sync_io(
        &mut self,
        state: &mut RuntimeState,
        receipt: WalSyncReceipt,
    ) -> MidgeResult<()> {
        if state.sequence < receipt.durable_sequence
            || state.wal.pending_writes < receipt.pending_writes
            || self.pending_sync_count < receipt.pending_sync_count
            || self.bytes_since_sync < receipt.bytes_since_sync
            || self.current_epoch != receipt.writer_epoch
            || state.wal.open_segment() != receipt.segment_id
        {
            let error = MidgeError::Internal(
                "WAL state changed while a sync receipt awaited commit".to_string(),
            );
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        self.finish_io_transition().inspect_err(|error| {
            state.mark_persistence_anomaly();
            tracing::error!(%error, "WAL sync could not return to open state");
        })?;
        state
            .wal
            .frontiers
            .advance_synced_to(receipt.durable_sequence);
        state.wal.pending_writes -= receipt.pending_writes;
        self.pending_sync_count -= receipt.pending_sync_count;
        self.bytes_since_sync -= receipt.bytes_since_sync;

        Ok(())
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn sync_failure_boundary() -> MidgeResult<()> {
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
    pub(super) fn after_fsync_boundary() -> MidgeResult<()> {
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

    /// Flush and fsync a cloud-staging WAL within the shared operation deadline.
    ///
    /// The per-I/O timeout caps the shared remaining budget; a shorter caller
    /// deadline still permits a fast sync. An accepted sync timeout is sticky:
    /// the actor fences and retains the writer until admitted effects drain.
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
        let open_max_sequence = self.open_segment_max_sequence();

        self.begin_io_transition(WalTransitionOperation::CloudFlush)?;

        if self.writer().is_some() {
            let Some(io_timeout) = deadline.clamp_nonzero(self.storage_io_timeout) else {
                self.finish_io_transition()?;
                return Err(crate::common::MidgeError::Timeout(format!(
                    "operation deadline exhausted before WAL flush: remaining={:?}",
                    deadline.remaining()
                )));
            };
            // Sealing advances local_durable_seq, and cloud recovery treats a
            // local sealed segment as complete. Fsync here (one barrier per
            // sealed segment, not per write) so power loss cannot leave a
            // torn sealed file.
            let sync_result = Self::sync_failure_boundary().and_then(|()| {
                self.writer_mut()
                    .ok_or_else(|| {
                        MidgeError::Internal("WAL writer disappeared during flush".into())
                    })?
                    .sync_with_timeout(io_timeout)
            });
            if let Err(error) = sync_result {
                self.fence_transition(state, format!("WAL seal fsync failed: {error}"));
                return Err(error);
            }
            self.counters.record(|m| {
                m.record_wal_flush();
            });
        }

        tracing::debug!(
            pending_writes = pending,
            flushed_seq = open_max_sequence,
            "WAL flush (CloudAsync upload)"
        );

        Ok(open_max_sequence)
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
        state.wal.frontiers.advance_synced_to(max_sequence);
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;
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
