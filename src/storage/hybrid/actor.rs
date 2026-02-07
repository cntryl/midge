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
use std::collections::VecDeque;

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

/// Events for the Storage Budget Actor
#[derive(Debug, Clone)]
pub enum StorageBudgetEvent {
    /// Try to reserve space for a flush; include estimated size
    ReserveForFlush { est_size: u64 },
    /// A flush completed with actual size
    FlushCompleted { actual_size: u64 },
    /// A cloud upload completed; free up reserve
    CloudUploadCompleted { sst_id: u64, actual_size: u64 },
    /// Compaction is about to start
    CompactionPlanned { input_sizes: Vec<u64> },
    /// Compaction finished with output sizes
    CompactionCompleted { output_sizes: Vec<u64> },
}

/// Storage Budget Actor
pub struct StorageBudgetActor {
    policy: StorageBudgetPolicy,
    disk_state: DiskState,
    /// Queue of SST IDs waiting for upload (FIFO for LRU/FIFO strategies)
    pending_evictions: VecDeque<(u64, u64)>, // (sst_id, size)
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
            last_watermark_state: WatermarkState::Normal,
        }
    }

    /// Handle an incoming event
    pub fn handle_event(&mut self, event: StorageBudgetEvent) -> Option<ReservationResult> {
        match event {
            StorageBudgetEvent::ReserveForFlush { est_size } => {
                self.try_reserve_for_flush(est_size)
            }
            StorageBudgetEvent::FlushCompleted { actual_size } => {
                self.complete_flush(actual_size);
                None
            }
            StorageBudgetEvent::CloudUploadCompleted {
                sst_id,
                actual_size,
            } => {
                self.complete_cloud_upload(sst_id, actual_size);
                None
            }
            StorageBudgetEvent::CompactionPlanned { input_sizes } => {
                self.plan_compaction(&input_sizes);
                None
            }
            StorageBudgetEvent::CompactionCompleted { output_sizes } => {
                self.complete_compaction(&output_sizes);
                None
            }
        }
    }

    /// Try to reserve space for a flush
    fn try_reserve_for_flush(&mut self, est_size: u64) -> Option<ReservationResult> {
        let usage_percent = self.disk_state.usage_percent(self.policy.max_local_bytes);

        // Emergency: reject all new writes
        if self.policy.is_emergency_watermark(usage_percent) {
            tracing::warn!("Emergency watermark hit; rejecting flush");
            return Some(ReservationResult::RejectNoSpace);
        }

        // Critical: wait for cloud uploads
        if self.policy.is_critical_watermark(usage_percent) {
            tracing::warn!("Critical watermark hit; requesting cloud uploads");
            return Some(ReservationResult::WaitForCloudUpload);
        }

        // High: may need to wait for compaction
        if self.policy.is_high_watermark(usage_percent) {
            let free = self.disk_state.free_bytes(self.policy.max_local_bytes);
            if free < est_size {
                tracing::info!("High watermark; waiting for compaction to free space");
                return Some(ReservationResult::WaitForCompaction);
            }
        }

        // Reserve space for the new SST
        self.disk_state.new_sst_reserve = self.disk_state.new_sst_reserve.saturating_add(est_size);
        self.check_watermarks();

        Some(ReservationResult::Ok)
    }

    /// Complete a flush, converting reserve to actual SST bytes
    fn complete_flush(&mut self, actual_size: u64) {
        self.disk_state.new_sst_reserve =
            self.disk_state.new_sst_reserve.saturating_sub(actual_size);
        self.disk_state.sst_bytes = self.disk_state.sst_bytes.saturating_add(actual_size);
        tracing::info!(actual_size, "Flush completed; SST added to local storage");
    }

    /// Complete a cloud upload
    fn complete_cloud_upload(&mut self, _sst_id: u64, _actual_size: u64) {
        // When cloud upload completes, the SST is stable in cloud.
        // We can now consider it for local eviction.
        self.pending_evictions.push_back((_sst_id, _actual_size));
        tracing::info!("Cloud upload completed; SST marked for potential eviction");
    }

    /// Plan a compaction by reserving output space
    fn plan_compaction(&mut self, input_sizes: &[u64]) {
        let total_input: u64 = input_sizes.iter().sum();
        // Estimate output as ~90% of input (conservative)
        let estimated_output = (total_input as f64 * 0.9) as u64;
        self.disk_state.compaction_reserve = estimated_output;
        tracing::info!(
            input_bytes = total_input,
            reserve_bytes = estimated_output,
            "Compaction planned"
        );
    }

    /// Complete a compaction, converting reserve to SST bytes
    fn complete_compaction(&mut self, output_sizes: &[u64]) {
        let total_output: u64 = output_sizes.iter().sum();
        self.disk_state.compaction_reserve = 0;
        self.disk_state.sst_bytes = self.disk_state.sst_bytes.saturating_add(total_output);
        tracing::info!(output_bytes = total_output, "Compaction completed");
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

    /// Get pending eviction queue
    pub fn pending_evictions(&self) -> Vec<(u64, u64)> {
        self.pending_evictions.iter().copied().collect()
    }

    /// Pop the next SST for eviction
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
        let result = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 100_000 });

        // Assert
        assert_eq!(result, Some(ReservationResult::Ok));
        assert_eq!(actor.disk_state().new_sst_reserve, 100_000);
    }

    #[test]
    fn should_queue_evictions_on_cloud_upload() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act
        actor.handle_event(StorageBudgetEvent::CloudUploadCompleted {
            sst_id: 42,
            actual_size: 50_000,
        });

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
            let result =
                actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 250_000 });
            assert_eq!(result, Some(ReservationResult::Ok));

            actor.handle_event(StorageBudgetEvent::FlushCompleted {
                actual_size: 240_000,
            });
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

        let result = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 10_000 });

        // Assert: Should ask to wait for cloud uploads
        assert_eq!(result, Some(ReservationResult::WaitForCloudUpload));
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
        let result = actor.handle_event(StorageBudgetEvent::ReserveForFlush {
            est_size: 110_000, // Remaining ~104KB, won't fit 110KB
        });

        // Assert: Should request compaction
        assert_eq!(result, Some(ReservationResult::WaitForCompaction));
    }

    #[test]
    fn should_reject_writes_at_emergency_watermark() {
        // Arrange: 1MB disk, emergency at 98%
        // Need: total >= 1048576 * 0.98 = 1027643.48 bytes
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Fill to 1028K (1028000 / 1048576 * 100 = 98.0%)
        actor.disk_state.sst_bytes = 1_028_000;

        let result = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 10_000 });

        // Assert: Should reject writes
        assert_eq!(result, Some(ReservationResult::RejectNoSpace));
    }

    #[test]
    fn should_track_flush_completion() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Reserve, complete, verify accounting
        let _ = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 50_000 });

        let before_complete = actor.disk_state();
        assert_eq!(before_complete.new_sst_reserve, 50_000);
        assert_eq!(before_complete.sst_bytes, 0);

        actor.handle_event(StorageBudgetEvent::FlushCompleted {
            actual_size: 45_000,
        });

        // Assert: Reserve → SST conversion
        let after_complete = actor.disk_state();
        assert_eq!(after_complete.new_sst_reserve, 5_000); // 50K - 45K
        assert_eq!(after_complete.sst_bytes, 45_000);
    }

    #[test]
    fn should_recover_from_emergency_after_cloud_upload() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Reach emergency watermark (1028K = 98.0%)
        actor.disk_state.sst_bytes = 1_028_000;
        let result1 = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 10_000 });
        assert_eq!(result1, Some(ReservationResult::RejectNoSpace));

        // Simulate cloud upload freeing 300KB
        actor.handle_event(StorageBudgetEvent::CloudUploadCompleted {
            sst_id: 1,
            actual_size: 300_000,
        });
        actor.disk_state.sst_bytes = 728_000; // 69.4% usage

        // Try reservation again
        let result2 = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 50_000 });

        // Assert: Should recover to Normal watermark (<90%)
        assert_eq!(result2, Some(ReservationResult::Ok));
    }

    #[test]
    fn should_handle_concurrent_operations_under_pressure() {
        // Arrange: Test complex scenario with simultaneous flushes and compaction
        let policy = StorageBudgetPolicy::new(5 * 1024 * 1024); // 5 MB for more flexibility
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Build up with mixed operations
        for i in 0..10 {
            // Attempt flush (each 150KB reserve)
            let flush_result =
                actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 150_000 });

            // Complete flush if successful
            if flush_result == Some(ReservationResult::Ok) {
                actor.handle_event(StorageBudgetEvent::FlushCompleted {
                    actual_size: 140_000,
                });
            }

            // Every 3 flushes, simulate a compaction
            if i % 3 == 2 && i > 0 {
                let estimated_input = 300_000 + (i as u64 * 50_000);
                actor.handle_event(StorageBudgetEvent::CompactionPlanned {
                    input_sizes: vec![estimated_input / 2, estimated_input / 2],
                });

                actor.handle_event(StorageBudgetEvent::CompactionCompleted {
                    output_sizes: vec![estimated_input / 2],
                });
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
        for sst_id in 1..=5 {
            actor.handle_event(StorageBudgetEvent::CloudUploadCompleted {
                sst_id: sst_id as u64,
                actual_size: 100_000,
            });
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
        for expected_sst_id in 1..=5 {
            let (sst_id, size) = actor.next_eviction().unwrap();
            assert_eq!(sst_id, expected_sst_id as u64);
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

            if let Some(ReservationResult::Ok) =
                actor.handle_event(StorageBudgetEvent::ReserveForFlush {
                    est_size: flush_size,
                })
            {
                let actual_size = flush_size - 10_000; // 10KB overhead
                actor.handle_event(StorageBudgetEvent::FlushCompleted { actual_size });
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
