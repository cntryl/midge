//! Flush and compaction storage-budget reservations.

use super::{actor, HybridStorage, HybridStorageBudgetSnapshot, StorageEvent};
use crate::storage::hybrid::pressure::StorageAdmissionKind;

impl HybridStorage {
    fn observe_admission<T>(
        &self,
        kind: StorageAdmissionKind,
        bytes: u64,
        admit: impl FnOnce(&mut actor::StorageBudgetActor) -> Result<T, actor::ReservationResult>,
    ) -> Result<T, actor::ReservationResult> {
        let mut actor = self.budget_actor.lock();
        let result = admit(&mut actor);
        let free = actor.disk_state().free_bytes(actor.max_local_bytes());
        actor.admission_pressure.observe(
            kind,
            bytes,
            free,
            result
                .as_ref()
                .map_or_else(|error| *error, |_| actor::ReservationResult::Ok),
            std::time::Instant::now(),
        );
        result
    }
}

impl HybridStorage {
    pub(crate) fn set_flush_headroom(&self, bytes: u64) -> crate::common::MidgeResult<()> {
        self.observe_admission(StorageAdmissionKind::FlushHeadroom, bytes, |actor| {
            actor.set_flush_headroom(bytes)
        })
        .map_err(|_| {
            crate::common::MidgeError::NoSpace(
                "ephemeral local disk cannot preserve flush staging for accepted cloud writes"
                    .into(),
            )
        })
    }
    pub(crate) fn reconcile_startup_scratch_residue(
        &self,
        bytes: u64,
    ) -> crate::common::MidgeResult<()> {
        self.observe_admission(StorageAdmissionKind::StartupResidue, bytes, |actor| {
            actor.reconcile_startup_scratch_residue(bytes)
        })
        .map_err(|_| {
            crate::common::MidgeError::NoSpace(
                "retained startup scratch exceeds ephemeral local disk budget".into(),
            )
        })
    }

    pub(crate) fn admit_local_scratch_bytes(&self, bytes: u64) -> crate::common::MidgeResult<()> {
        self.observe_admission(StorageAdmissionKind::TransactionSpill, bytes, |actor| {
            actor.admit_local_scratch_bytes(bytes)
        })
        .map_err(|_| {
            crate::common::MidgeError::NoSpace(
                "ephemeral local disk budget cannot admit transaction spill".into(),
            )
        })
    }

    pub(crate) fn release_local_scratch_bytes(&self, bytes: u64) {
        self.budget_actor.lock().release_local_scratch_bytes(bytes);
        self.emit_reservation_result(actor::ReservationResult::Ok);
    }

    pub(crate) fn admit_local_wal_bytes(&self, bytes: u64) -> crate::common::MidgeResult<()> {
        let result = self.observe_admission(StorageAdmissionKind::Wal, bytes, |actor| {
            actor.admit_local_wal_bytes(bytes)
        });
        if let Err(result) = result {
            self.emit_reservation_result(result);
            return Err(crate::common::MidgeError::NoSpace(
                "ephemeral local disk budget cannot admit WAL append".into(),
            ));
        }
        Ok(())
    }

    /// Called only after removing an acknowledged local WAL file.
    pub(crate) fn release_local_wal_bytes(&self, bytes: u64) {
        self.budget_actor.lock().release_local_wal_bytes(bytes);
        self.emit_reservation_result(actor::ReservationResult::Ok);
    }

    /// Settle a successful append to its measured physical growth. This is
    /// ordinary accounting and does not emit a backpressure event per write.
    pub(crate) fn settle_local_wal_admission(&self, admitted: u64, actual: u64) {
        self.budget_actor
            .lock()
            .release_local_wal_bytes(admitted.saturating_sub(actual));
    }

    /// Configure an engine whose reads can fetch missing SST blocks from
    /// cloud storage. Call before starting runtime workers.
    pub(crate) fn enable_ephemeral_sst_cache(&self, max_local_bytes: u64) {
        self.budget_actor
            .lock()
            .enable_ephemeral_sst_cache(max_local_bytes);
        self.ephemeral_sst_cache
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Reconcile physical files at startup or after a failed cleanup. Existing
    /// operation reservations remain charged in addition to these bytes.
    pub(crate) fn reconcile_local_disk_usage(&self, sst_bytes: u64, wal_bytes: u64) {
        self.budget_actor
            .lock()
            .reconcile_local_disk_usage(sst_bytes, wal_bytes);
    }

    /// Release resident SST bytes only after the runtime confirms deletion.
    pub(crate) fn release_local_sst_bytes(&self, bytes: u64) {
        self.budget_actor.lock().release_local_sst_bytes(bytes);
        self.emit_reservation_result(actor::ReservationResult::Ok);
    }

    pub fn reserve_for_flush_with_token(
        &self,
        est_size: u64,
    ) -> Result<actor::StorageReservationToken, actor::ReservationResult> {
        let result = self.observe_admission(StorageAdmissionKind::Flush, est_size, |actor| {
            actor.reserve_for_flush_with_token(est_size)
        });

        let reservation_result = match result {
            Ok(_) => actor::ReservationResult::Ok,
            Err(result) => result,
        };
        self.emit_reservation_result(reservation_result);

        result
    }

    fn emit_reservation_result(&self, result: actor::ReservationResult) {
        let event = match result {
            actor::ReservationResult::Ok => StorageEvent::BackpressureOff,
            actor::ReservationResult::WaitForCloudUpload
            | actor::ReservationResult::WaitForCompaction
            | actor::ReservationResult::RejectNoSpace => StorageEvent::BackpressureOn,
        };
        Self::queue_storage_event(&self.event_queue, self.external_event_tx.as_ref(), event);
    }
}

impl HybridStorage {
    /// Settle the exact flush reservation that published an SST.
    pub fn flush_completed_with_token(
        &self,
        token: actor::StorageReservationToken,
        actual_size: u64,
    ) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.complete_flush_for(token, actual_size);
    }

    /// Release the exact flush reservation whose output did not publish.
    pub fn flush_failed_with_token(&self, token: actor::StorageReservationToken) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.abort_flush_for(token);
    }

    /// Reserve compaction output space and return the token for its terminal
    /// completion or cancellation.
    pub fn compaction_planned_with_token(
        &self,
        input_sizes: &[u64],
    ) -> actor::StorageReservationToken {
        let mut actor = self.budget_actor.lock();
        actor.plan_compaction_with_token(input_sizes)
    }

    pub(crate) fn reserve_compaction_staging_with_token(
        &self,
        bytes: u64,
    ) -> Result<actor::StorageReservationToken, actor::ReservationResult> {
        self.observe_admission(StorageAdmissionKind::Compaction, bytes, |actor| {
            actor.reserve_compaction_staging_with_token(bytes)
        })
    }

    /// Settle the exact compaction reservation after manifest publication.
    pub fn compaction_completed_with_token(
        &self,
        token: actor::StorageReservationToken,
        output_sizes: &[u64],
    ) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.complete_compaction_for(token, output_sizes);
    }

    /// Settle output accounting while retaining authoritative input bytes
    /// after a post-manifest publication failure.
    pub fn compaction_inputs_retained_with_token(
        &self,
        token: actor::StorageReservationToken,
        output_sizes: &[u64],
    ) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.retain_compaction_inputs_for(token, output_sizes);
    }

    /// Release the exact compaction reservation without deleting its inputs.
    pub fn compaction_aborted_with_token(&self, token: actor::StorageReservationToken) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.abort_compaction_for(token);
    }

    pub fn budget_snapshot(&self) -> HybridStorageBudgetSnapshot {
        let actor = self.budget_actor.lock();
        let disk_state = actor.disk_state();
        let max_local_bytes = actor.max_local_bytes();
        HybridStorageBudgetSnapshot {
            max_local_bytes,
            total_committed_bytes: disk_state.total_committed(),
            free_bytes: disk_state.free_bytes(max_local_bytes),
            usage_percent: disk_state.usage_percent(max_local_bytes),
            pending_evictions: actor.pending_eviction_count(),
            usage: actor.usage_snapshot(),
            blocked_admission: actor.admission_pressure.snapshot(std::time::Instant::now()),
            admission_rejections_total: actor.admission_pressure.rejections_total,
        }
    }

    /// Get count of pending uploads (for monitoring)
    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue.lock().entries.len()
    }

    #[cfg(test)]
    pub(super) fn pending_upload_bytes(&self) -> u64 {
        self.upload_queue.lock().pending_bytes
    }
}
