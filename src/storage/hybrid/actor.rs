//! Storage Budget Actor - manages disk space and backpressure
//!
//! Responsibilities:
//! - Track disk usage across WAL, SSTs, compaction, and pending uploads
//! - Enforce watermarks (high, critical, emergency)
//! - Reserve space before flush/compaction
//! - Coordinate cloud uploads and local eviction
//! - Signal backpressure to the engine

use super::policy::StorageBudgetPolicy;
use super::state::DiskState;
use std::collections::{HashMap, VecDeque};

/// Opaque identity for one flush or compaction space reservation.
///
/// A terminal operation must consume this exact token. This prevents a
/// delayed completion from releasing an unrelated reservation that happened
/// to be queued first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub struct StorageReservationToken(u64);

#[derive(Debug, Clone, Copy)]
struct CompactionReservation {
    input_bytes: u64,
    estimated_output_bytes: u64,
}

/// Result of a space reservation request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationResult {
    /// Space reserved successfully
    Ok,
    /// Wait for cloud uploads to complete
    WaitForCloudUpload,
    /// Wait for compaction to free space
    WaitForCompaction,
    /// No space available; reject write or flush
    RejectNoSpace,
}

/// Storage Budget Actor
pub struct StorageBudgetActor {
    policy: StorageBudgetPolicy,
    disk_state: DiskState,
    /// Queue of SST IDs waiting for upload (FIFO for LRU/FIFO strategies)
    pending_evictions: VecDeque<(u64, u64)>, // (sst_id, size)
    /// Flush reservations indexed by their operation identity.
    flush_reservations: HashMap<StorageReservationToken, u64>,
    /// Compaction reservations indexed by their operation identity.
    compaction_reservations: HashMap<StorageReservationToken, CompactionReservation>,
    /// Next opaque reservation identity.
    next_reservation_id: u64,
    /// Last reported watermark state
    last_watermark_state: WatermarkState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatermarkState {
    Normal,
    High,
    Critical,
    Emergency,
}

impl StorageBudgetActor {
    pub fn new(policy: StorageBudgetPolicy) -> Self {
        Self {
            policy,
            disk_state: DiskState::new(),
            pending_evictions: VecDeque::new(),
            flush_reservations: HashMap::new(),
            compaction_reservations: HashMap::new(),
            next_reservation_id: 1,
            last_watermark_state: WatermarkState::Normal,
        }
    }

    fn next_token(&mut self) -> StorageReservationToken {
        let token = StorageReservationToken(self.next_reservation_id);
        self.next_reservation_id = self.next_reservation_id.wrapping_add(1).max(1);
        token
    }

    /// Reserve output space for one flush and return its operation token.
    pub fn reserve_for_flush_with_token(
        &mut self,
        est_size: u64,
    ) -> Result<StorageReservationToken, ReservationResult> {
        let usage_percent = self.disk_state.usage_percent(self.policy.max_local_bytes);

        // Emergency: reject all new writes
        if self.policy.is_emergency_watermark(usage_percent) {
            tracing::warn!("Emergency watermark hit; rejecting flush");
            return Err(ReservationResult::RejectNoSpace);
        }

        // Critical: wait for cloud uploads
        if self.policy.is_critical_watermark(usage_percent) {
            tracing::warn!("Critical watermark hit; requesting cloud uploads");
            return Err(ReservationResult::WaitForCloudUpload);
        }

        // High: may need to wait for compaction
        if self.policy.is_high_watermark(usage_percent) {
            let free = self.disk_state.free_bytes(self.policy.max_local_bytes);
            if free < est_size {
                tracing::info!("High watermark; waiting for compaction to free space");
                return Err(ReservationResult::WaitForCompaction);
            }
        }

        // Reserve space for the new SST
        let token = self.next_token();
        self.disk_state.new_sst_reserve = self.disk_state.new_sst_reserve.saturating_add(est_size);
        self.flush_reservations.insert(token, est_size);
        self.check_watermarks();

        Ok(token)
    }

    /// Complete exactly one flush reservation. Returns false for a duplicate
    /// or unknown token and intentionally leaves accounting unchanged.
    pub fn complete_flush_for(&mut self, token: StorageReservationToken, actual_size: u64) -> bool {
        let Some(reserved_size) = self.flush_reservations.remove(&token) else {
            tracing::warn!(?token, "ignoring unmatched flush completion reservation");
            return false;
        };
        self.disk_state.new_sst_reserve = self
            .disk_state
            .new_sst_reserve
            .saturating_sub(reserved_size);
        self.disk_state.sst_bytes = self.disk_state.sst_bytes.saturating_add(actual_size);
        self.check_watermarks();
        tracing::info!(
            ?token,
            actual_size,
            "Flush completed; SST added to local storage"
        );
        true
    }

    /// Release exactly one unpublished flush reservation.
    pub fn abort_flush_for(&mut self, token: StorageReservationToken) -> bool {
        let Some(reserved_size) = self.flush_reservations.remove(&token) else {
            tracing::warn!(?token, "ignoring unmatched flush reservation release");
            return false;
        };
        self.disk_state.new_sst_reserve = self
            .disk_state
            .new_sst_reserve
            .saturating_sub(reserved_size);
        self.check_watermarks();
        true
    }

    /// Plan a compaction and return the reservation that must settle it.
    pub fn plan_compaction_with_token(&mut self, input_sizes: &[u64]) -> StorageReservationToken {
        let total_input: u64 = input_sizes.iter().sum();
        // Estimate output as ~90% of input (conservative)
        let estimated_output = total_input.saturating_mul(9) / 10;
        let token = self.next_token();
        self.disk_state.compaction_reserve = self
            .disk_state
            .compaction_reserve
            .saturating_add(estimated_output);
        self.compaction_reservations.insert(
            token,
            CompactionReservation {
                input_bytes: total_input,
                estimated_output_bytes: estimated_output,
            },
        );
        self.check_watermarks();
        tracing::info!(
            ?token,
            input_bytes = total_input,
            reserve_bytes = estimated_output,
            "Compaction planned"
        );
        token
    }

    /// Complete exactly one compaction reservation.
    pub fn complete_compaction_for(
        &mut self,
        token: StorageReservationToken,
        output_sizes: &[u64],
    ) -> bool {
        let Some(reservation) = self.compaction_reservations.remove(&token) else {
            tracing::warn!(
                ?token,
                "ignoring unmatched compaction completion reservation"
            );
            return false;
        };
        let total_output: u64 = output_sizes.iter().sum();
        self.disk_state.compaction_reserve = self
            .disk_state
            .compaction_reserve
            .saturating_sub(reservation.estimated_output_bytes);
        self.disk_state.sst_bytes = self
            .disk_state
            .sst_bytes
            .saturating_sub(reservation.input_bytes)
            .saturating_add(total_output);
        self.check_watermarks();
        tracing::info!(?token, output_bytes = total_output, "Compaction completed");
        true
    }

    /// Release exactly one uncompleted compaction reservation.
    pub fn abort_compaction_for(&mut self, token: StorageReservationToken) -> bool {
        let Some(reservation) = self.compaction_reservations.remove(&token) else {
            tracing::warn!(?token, "ignoring unmatched compaction reservation release");
            return false;
        };
        self.disk_state.compaction_reserve = self
            .disk_state
            .compaction_reserve
            .saturating_sub(reservation.estimated_output_bytes);
        self.check_watermarks();
        true
    }

    /// Complete a cloud upload
    #[cfg(test)]
    fn complete_cloud_upload(&mut self, sst_id: u64, actual_size: u64) {
        // When cloud upload completes, the SST is stable in cloud.
        // We can now consider it for local eviction.
        self.pending_evictions.push_back((sst_id, actual_size));
        tracing::info!("Cloud upload completed; SST marked for potential eviction");
    }

    /// Check watermarks and emit warnings if state changed
    fn check_watermarks(&mut self) {
        let usage_percent = self.disk_state.usage_percent(self.policy.max_local_bytes);
        let new_state = if self.policy.is_emergency_watermark(usage_percent) {
            WatermarkState::Emergency
        } else if self.policy.is_critical_watermark(usage_percent) {
            WatermarkState::Critical
        } else if self.policy.is_high_watermark(usage_percent) {
            WatermarkState::High
        } else {
            WatermarkState::Normal
        };

        if new_state != self.last_watermark_state {
            tracing::warn!(
                new_state = ?new_state,
                usage_percent = usage_percent,
                "Watermark transition"
            );
            self.last_watermark_state = new_state;
        }
    }

    /// Get current disk state (snapshot)
    pub fn disk_state(&self) -> DiskState {
        self.disk_state.clone()
    }

    pub fn max_local_bytes(&self) -> u64 {
        self.policy.max_local_bytes
    }

    /// Get pending eviction queue
    pub fn pending_evictions(&self) -> Vec<(u64, u64)> {
        self.pending_evictions.iter().copied().collect()
    }

    /// Pop the next SST for eviction
    #[cfg(test)]
    pub fn next_eviction(&mut self) -> Option<(u64, u64)> {
        self.pending_evictions.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reserve_space_when_below_high_watermark() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act
        let result = actor.reserve_for_flush_with_token(100_000);

        // Assert
        assert!(result.is_ok());
        assert_eq!(actor.disk_state().new_sst_reserve, 100_000);
    }

    #[test]
    fn should_queue_evictions_on_cloud_upload() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act
        actor.complete_cloud_upload(42, 50_000);

        // Assert
        let evictions = actor.pending_evictions();
        assert_eq!(evictions.len(), 1);
        assert_eq!(evictions[0], (42, 50_000));
    }

    // ===== E2E Disk Pressure Stress Tests (Section 9) =====

    #[test]
    fn should_handle_gradual_disk_pressure_buildup() {
        // Arrange: Simulate gradual writes filling disk
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Gradually add SSTs (no watermark transitions yet)
        for _ in 0..3 {
            let token = actor
                .reserve_for_flush_with_token(250_000)
                .expect("flush reservation should succeed");
            assert!(actor.complete_flush_for(token, 240_000));
        }

        // Assert: Should have ~720KB of SST data, all fits comfortably (720KB < 1MB)
        let state = actor.disk_state();
        assert!(state.sst_bytes >= 600_000 && state.sst_bytes <= 800_000);
        assert!(state.total_committed() < 1024 * 1024); // Still below max
    }

    #[test]
    fn should_wait_for_cloud_upload_at_critical_watermark() {
        // Arrange: 1MB disk, critical watermark at 95%
        // ((total as f64 / 1048576 as f64) * 100.0) as u32 >= 95
        // Need: total >= 1048576 * 0.95 = 995549.2 bytes
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Fill to 996K bytes (995549 / 1048576 * 100 = 95%, but truncates to 94%)
        // So actually need 999K to get 95.2%
        actor.disk_state.sst_bytes = 999_000;

        let result = actor.reserve_for_flush_with_token(10_000);

        // Assert: Should ask to wait for cloud uploads
        assert_eq!(result, Err(ReservationResult::WaitForCloudUpload));
    }

    #[test]
    fn should_wait_for_compaction_at_high_watermark() {
        // Arrange: 1MB disk, high watermark at 90%
        // Need: total >= 1048576 * 0.90 = 943718.4 bytes
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Fill to 944K (944000 / 1048576 * 100 = 90.02%)
        actor.disk_state.sst_bytes = 944_000;

        // Request a flush that won't fit in remaining space
        let result = actor.reserve_for_flush_with_token(110_000); // Remaining ~104KB, won't fit 110KB

        // Assert: Should request compaction
        assert_eq!(result, Err(ReservationResult::WaitForCompaction));
    }

    #[test]
    fn should_reject_writes_at_emergency_watermark() {
        // Arrange: 1MB disk, emergency at 98%
        // Need: total >= 1048576 * 0.98 = 1027643.48 bytes
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Fill to 1028K (1028000 / 1048576 * 100 = 98.0%)
        actor.disk_state.sst_bytes = 1_028_000;

        let result = actor.reserve_for_flush_with_token(10_000);

        // Assert: Should reject writes
        assert_eq!(result, Err(ReservationResult::RejectNoSpace));
    }

    #[test]
    fn should_track_flush_completion() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Reserve, complete, verify accounting
        let token = actor
            .reserve_for_flush_with_token(50_000)
            .expect("flush reservation should succeed");

        let before_complete = actor.disk_state();
        assert_eq!(before_complete.new_sst_reserve, 50_000);
        assert_eq!(before_complete.sst_bytes, 0);

        assert!(actor.complete_flush_for(token, 45_000));

        // Assert: Reserve → SST conversion
        let after_complete = actor.disk_state();
        assert_eq!(after_complete.new_sst_reserve, 0);
        assert_eq!(after_complete.sst_bytes, 45_000);
    }

    #[test]
    fn should_release_full_flush_reservation_when_actual_output_exceeds_estimate() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor
            .reserve_for_flush_with_token(10)
            .expect("flush reservation should succeed");

        // Act
        assert!(actor.complete_flush_for(token, 20));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.new_sst_reserve, 0);
        assert_eq!(state.sst_bytes, 20);
    }

    #[test]
    fn should_release_flush_reservation_when_flush_fails() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor
            .reserve_for_flush_with_token(100)
            .expect("flush reservation should succeed");

        // Act
        assert!(actor.abort_flush_for(token));

        // Assert
        assert_eq!(actor.disk_state().new_sst_reserve, 0);
        assert_eq!(actor.disk_state().sst_bytes, 0);
    }

    #[test]
    fn should_finalize_compaction_publication_when_output_replaces_inputs() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        actor.disk_state.sst_bytes = 300;

        // Act
        let token = actor.plan_compaction_with_token(&[100, 100]);
        assert!(actor.complete_compaction_for(token, &[50]));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.compaction_reserve, 0);
        assert_eq!(state.sst_bytes, 150);
    }

    #[test]
    fn should_release_compaction_reservation_when_compaction_is_aborted() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor.plan_compaction_with_token(&[100, 100]);

        // Act
        assert!(actor.abort_compaction_for(token));

        // Assert
        assert_eq!(actor.disk_state().compaction_reserve, 0);
        assert_eq!(actor.disk_state().sst_bytes, 0);
    }

    #[test]
    fn should_settle_only_the_matching_flush_reservation_when_completions_reorder() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let first = actor
            .reserve_for_flush_with_token(100)
            .expect("first flush reservation should succeed");
        let second = actor
            .reserve_for_flush_with_token(200)
            .expect("second flush reservation should succeed");

        // Act
        assert!(actor.complete_flush_for(second, 150));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.new_sst_reserve, 100);
        assert_eq!(state.sst_bytes, 150);
        assert!(actor.abort_flush_for(first));
        assert_eq!(actor.disk_state().new_sst_reserve, 0);
    }

    #[test]
    fn should_release_only_the_matching_compaction_reservation_when_terminal_paths_reorder() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        actor.disk_state.sst_bytes = 1_000;
        let first = actor.plan_compaction_with_token(&[100]);
        let second = actor.plan_compaction_with_token(&[200]);

        // Act
        assert!(actor.complete_compaction_for(second, &[150]));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.compaction_reserve, 90);
        assert_eq!(state.sst_bytes, 950);
        assert!(actor.abort_compaction_for(first));
        assert_eq!(actor.disk_state().compaction_reserve, 0);
    }

    #[test]
    fn should_ignore_duplicate_token_terminal_events_without_changing_accounting() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor
            .reserve_for_flush_with_token(100)
            .expect("flush reservation should succeed");

        // Act
        assert!(actor.complete_flush_for(token, 90));
        let settled = actor.disk_state();

        // Assert
        assert!(!actor.complete_flush_for(token, 90));
        assert!(!actor.abort_flush_for(token));
        assert_eq!(actor.disk_state().new_sst_reserve, settled.new_sst_reserve);
        assert_eq!(actor.disk_state().sst_bytes, settled.sst_bytes);
    }

    #[test]
    fn should_recover_from_emergency_after_cloud_upload() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Reach emergency watermark (1028K = 98.0%)
        actor.disk_state.sst_bytes = 1_028_000;
        let result1 = actor.reserve_for_flush_with_token(10_000);
        assert_eq!(result1, Err(ReservationResult::RejectNoSpace));

        // Simulate cloud upload freeing 300KB
        actor.complete_cloud_upload(1, 300_000);
        actor.disk_state.sst_bytes = 728_000; // 69.4% usage

        // Try reservation again
        let result2 = actor.reserve_for_flush_with_token(50_000);

        // Assert: Should recover to Normal watermark (<90%)
        assert!(result2.is_ok());
    }

    #[test]
    fn should_handle_concurrent_operations_under_pressure() {
        // Arrange: Test complex scenario with simultaneous flushes and compaction
        let policy = StorageBudgetPolicy::new(5 * 1024 * 1024); // 5 MB for more flexibility
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Build up with mixed operations
        for i in 0..10 {
            // Attempt flush (each 150KB reserve)
            let flush_result = actor.reserve_for_flush_with_token(150_000);

            // Complete flush if successful
            if let Ok(token) = flush_result {
                assert!(actor.complete_flush_for(token, 140_000));
            }

            // Every 3 flushes, simulate a compaction
            if i % 3 == 2 && i > 0 {
                let estimated_input =
                    300_000 + (u64::try_from(i).expect("loop index fits in u64") * 50_000);
                let token =
                    actor.plan_compaction_with_token(&[estimated_input / 2, estimated_input / 2]);
                assert!(actor.complete_compaction_for(token, &[estimated_input / 2]));
            }
        }

        // Assert: Should have processed multiple operations
        let state = actor.disk_state();
        assert!(state.sst_bytes > 0, "SSTs should have been created");
        assert!(state.total_committed() > 0, "Total committed should be > 0");
    }

    #[test]
    fn should_track_pending_evictions_during_pressure() {
        // Arrange: Simulate scenario where cloud uploads queue evictions
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Create several SSTs and upload them
        for sst_id in 1_u64..=5 {
            actor.complete_cloud_upload(sst_id, 100_000);
        }

        // Assert: All uploads should be queued for eviction
        let evictions = actor.pending_evictions();
        assert_eq!(evictions.len(), 5);

        // Verify FIFO order
        for (idx, (sst_id, size)) in evictions.iter().enumerate() {
            assert_eq!(*sst_id, (idx + 1) as u64);
            assert_eq!(*size, 100_000);
        }

        // Verify we can pop them in order
        for expected_sst_id in 1_u64..=5 {
            let (sst_id, size) = actor.next_eviction().unwrap();
            assert_eq!(sst_id, expected_sst_id);
            assert_eq!(size, 100_000);
        }

        // Verify queue is empty
        assert!(actor.next_eviction().is_none());
    }

    #[test]
    fn should_correctly_calculate_metrics_under_sustained_load() {
        // Arrange: Sustained high-load scenario
        let policy = StorageBudgetPolicy::new(10 * 1024 * 1024); // 10 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Simulate sustained write load
        let mut total_flushed = 0u64;
        for i in 0..50 {
            let flush_size = 100_000 + (i % 50_000);

            if let Ok(token) = actor.reserve_for_flush_with_token(flush_size) {
                let actual_size = flush_size - 10_000; // 10KB overhead
                assert!(actor.complete_flush_for(token, actual_size));
                total_flushed += actual_size;
            }
        }

        // Assert: Metrics should accurately reflect all operations
        let state = actor.disk_state();
        assert_eq!(
            state.sst_bytes, total_flushed,
            "SST bytes should match total flushed"
        );
        assert!(
            state.total_committed() >= total_flushed,
            "Total committed should be >= SST bytes"
        );
    }
}
