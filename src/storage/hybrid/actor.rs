//! Storage Budget Actor - manages disk space and backpressure
//!
//! Responsibilities:
//! - Track disk usage across WAL, SSTs, compaction, and pending uploads
//! - Enforce watermarks (high, critical, emergency)
//! - Reserve space before flush/compaction
//! - Coordinate cloud uploads and local eviction
//! - Signal backpressure to the engine

use super::policy::{EvictionStrategy, StorageBudgetPolicy};
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
    /// Query current disk state
    QuerySpace { respond_to: String },
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
    /// External signal: WAL grew by this amount
    WalGrew { bytes: u64 },
    /// External signal: SST was deleted locally
    LocalSSTPurged { bytes: u64 },
}

/// Storage Budget Actor
pub struct StorageBudgetActor {
    policy: StorageBudgetPolicy,
    disk_state: DiskState,
    eviction_strategy: EvictionStrategy,
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
            eviction_strategy: EvictionStrategy::LRU,
            pending_evictions: VecDeque::new(),
            last_watermark_state: WatermarkState::Normal,
        }
    }

    pub fn with_eviction_strategy(mut self, strategy: EvictionStrategy) -> Self {
        self.eviction_strategy = strategy;
        self
    }

    /// Handle an incoming event
    pub fn handle_event(&mut self, event: StorageBudgetEvent) -> Option<ReservationResult> {
        match event {
            StorageBudgetEvent::QuerySpace { .. } => {
                tracing::info!(
                    usage_percent = self.disk_state.usage_percent(self.policy.max_local_bytes),
                    used_bytes = self.disk_state.total_committed(),
                    max_bytes = self.policy.max_local_bytes,
                    "Space query"
                );
                None
            }
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
            StorageBudgetEvent::WalGrew { bytes } => {
                self.disk_state.wal_bytes = self.disk_state.wal_bytes.saturating_add(bytes);
                self.check_watermarks();
                None
            }
            StorageBudgetEvent::LocalSSTPurged { bytes } => {
                self.disk_state.sst_bytes = self.disk_state.sst_bytes.saturating_sub(bytes);
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

    #[cfg(test)]
    fn set_sst_bytes(&mut self, bytes: u64) {
        self.disk_state.sst_bytes = bytes;
    }

    #[cfg(test)]
    fn set_wal_bytes(&mut self, bytes: u64) {
        self.disk_state.wal_bytes = bytes;
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
    fn should_return_wait_for_compaction_at_high_watermark() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);
        actor.set_sst_bytes(900_000);

        // Act
        let result = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 150_000 });

        // Assert
        assert_eq!(result, Some(ReservationResult::WaitForCompaction));
    }

    #[test]
    fn should_return_wait_for_cloud_upload_at_critical_watermark() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);
        actor.set_sst_bytes(950_000);

        // Act
        let result = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 10_000 });

        // Assert
        assert_eq!(result, Some(ReservationResult::WaitForCloudUpload));
    }

    #[test]
    fn should_reject_writes_at_emergency_watermark() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);
        actor.set_sst_bytes(980_000);

        // Act
        let result = actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 10_000 });

        // Assert
        assert_eq!(result, Some(ReservationResult::RejectNoSpace));
    }

    #[test]
    fn should_track_flush_completion() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act
        actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 100_000 });
        actor.handle_event(StorageBudgetEvent::FlushCompleted {
            actual_size: 95_000,
        });

        // Assert
        assert_eq!(actor.disk_state().new_sst_reserve, 0);
        assert_eq!(actor.disk_state().sst_bytes, 95_000);
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
}
