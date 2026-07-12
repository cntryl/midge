//! Flush and compaction storage-budget reservations.

use super::{actor, HybridStorage, HybridStorageBudgetSnapshot, StorageEvent};

impl HybridStorage {
    pub fn reserve_for_flush_with_token(
        &self,
        est_size: u64,
    ) -> Result<actor::StorageReservationToken, actor::ReservationResult> {
        let mut actor = self.budget_actor.lock();
        let result = actor.reserve_for_flush_with_token(est_size);
        drop(actor);

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

    /// Settle the exact compaction reservation after manifest publication.
    pub fn compaction_completed_with_token(
        &self,
        token: actor::StorageReservationToken,
        output_sizes: &[u64],
    ) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.complete_compaction_for(token, output_sizes);
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
            pending_evictions: actor.pending_evictions().len(),
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
