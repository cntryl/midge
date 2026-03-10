//! Durability coordination logic — manages waiter groups, CloudAsync inflight state, and frontier checks.
//!
//! Extracted from EventLoop to reduce cognitive load and improve testability.
//! Owns the policy-independent parts of durability enforcement.

use crate::common::KeyedGroupCommit;
use crate::engine::api::Durability;
use std::collections::HashMap;
use std::time::Instant;

/// Single CloudAsync segment being uploaded
#[derive(Debug, Clone)]
pub struct CloudAsyncInflightSegment {
    pub enqueued_at: Instant,
    pub max_sequence: u64,
}

/// Waiter types for group commit
#[derive(Debug, Clone)]
pub enum DurabilityWaiter {
    WalAppend {
        request_id: u64,
        sequence: u64,
    },
    /// Internal waiter used when caller already acknowledged but needs cleanup.
    ConfirmWalAppend {
        request_id: u64,
    },
    TransactionApply {
        request_id: u64,
        last_sequence: u64,
        op_count: usize,
    },
    /// Internal waiter used when caller already acknowledged but needs cleanup.
    ConfirmTransactionApply {
        request_id: u64,
    },
    CloudDurability {
        request_id: u64,
    },
    Read {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64,
        #[allow(dead_code)]
        requested_durability: Durability,
    },
    RangeScan {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        start: Vec<u8>,
        end: Vec<u8>,
        sequence: u64,
        #[allow(dead_code)]
        requested_durability: Durability,
    },
}

/// Coordinates all durability-related state and decisions.
///
/// Owns:
/// - Group commit waiter queues (KeyedGroupCommit)
/// - CloudAsync inflight segment tracking
/// - Durability frontier checks
///
/// Does NOT own:
/// - WAL actor (read-only access to WalState for frontier checks)
/// - Storage or network concerns (those are caller's responsibility)
pub struct DurabilityCoordinator {
    /// Group commit: waiters keyed by WAL segment or generation
    waiters: Option<KeyedGroupCommit<u64, DurabilityWaiter>>,

    /// CloudAsync: track enqueue->ack per WAL segment
    inflight: HashMap<u64, CloudAsyncInflightSegment>,

    /// CloudAsync: timestamp of last flush/rotate
    last_cloud_flush: Instant,

    /// Is CloudAsync enabled? (read from wal_actor.is_cloud_async())
    is_cloud_async: bool,
}

impl DurabilityCoordinator {
    /// Create a new coordinator with initial durability key.
    pub fn new(initial_durability_key: u64, is_cloud_async: bool) -> Self {
        Self {
            waiters: Some(KeyedGroupCommit::new(initial_durability_key)),
            inflight: HashMap::new(),
            last_cloud_flush: Instant::now(),
            is_cloud_async,
        }
    }

    /// Check if a sequence number is durable at the requested level.
    ///
    /// Special case: u64::MAX (latest available) always returns true and bypasses durability checks.
    #[inline]
    pub fn is_durable(
        &self,
        sequence: u64,
        requested_durability: Durability,
        local_durable_seq: u64,
        cloud_durable_seq: u64,
    ) -> bool {
        if sequence == u64::MAX {
            // "Latest available" reads proceed immediately; no durability guarantee needed
            return true;
        }

        match requested_durability {
            Durability::Strict | Durability::Steady => sequence <= local_durable_seq,
            Durability::CloudPersisted => sequence <= cloud_durable_seq,
        }
    }

    /// Queue a waiter for later completion.
    pub fn queue_waiter(&self, waiter: DurabilityWaiter) {
        if let Some(waiters) = &self.waiters {
            waiters.join(waiter);
        }
    }

    pub fn queue_waiter_for_key(&self, key: u64, waiter: DurabilityWaiter) {
        if let Some(waiters) = &self.waiters {
            waiters.join_for_key(key, waiter);
        }
    }

    /// Get all waiters ready for completion at the given key.
    pub fn complete_waiters_at(&self, key: u64) -> Vec<DurabilityWaiter> {
        self.waiters
            .as_ref()
            .map(|w| w.complete(&key))
            .unwrap_or_default()
    }

    /// Drain all pending waiters (used on shutdown or error).
    pub fn drain_all_waiters(&self) -> Vec<DurabilityWaiter> {
        self.waiters
            .as_ref()
            .map(|w| w.drain_all())
            .unwrap_or_default()
    }

    /// Check if there are pending waiters.
    pub fn has_pending_waiters(&self) -> bool {
        self.waiters
            .as_ref()
            .map(|w| w.pending_len() > 0)
            .unwrap_or(false)
    }

    /// Rotate group commit to new key (advance generation/segment).
    pub fn rotate_to(&self, new_key: u64) {
        if let Some(waiters) = &self.waiters {
            let _ = waiters.rotate_to(new_key);
        }
    }

    /// Record a CloudAsync segment enqueued for upload.
    pub fn record_cloud_segment_inflight(&mut self, segment_id: u64, max_sequence: u64) {
        self.inflight.insert(
            segment_id,
            CloudAsyncInflightSegment {
                enqueued_at: Instant::now(),
                max_sequence,
            },
        );
    }

    /// Get all CloudAsync segments ready for completion (whose max_sequence is now durable).
    pub fn get_ready_cloud_segments(&mut self, durable_seq: u64) -> Vec<u64> {
        let mut ready: Vec<u64> = self
            .inflight
            .iter()
            .filter_map(|(seg, info)| {
                if info.max_sequence <= durable_seq {
                    Some(*seg)
                } else {
                    None
                }
            })
            .collect();
        ready.sort_unstable();

        // Remove them from inflight
        for seg_id in &ready {
            self.inflight.remove(seg_id);
        }

        ready
    }

    /// Get timing info for a CloudAsync segment (for telemetry).
    pub fn take_cloud_segment_timing(&mut self, segment_id: u64) -> Option<Instant> {
        self.inflight
            .remove(&segment_id)
            .map(|info| info.enqueued_at)
    }

    /// Remove and return the max_sequence for a specific inflight segment.
    /// Useful when a segment fails and we need to invalidate idempotency allocations
    /// that were part of that segment.
    pub fn take_cloud_segment_max_sequence(&mut self, segment_id: u64) -> Option<u64> {
        self.inflight
            .remove(&segment_id)
            .map(|info| info.max_sequence)
    }

    pub fn inflight_segment_for_sequence(&self, sequence: u64) -> Option<u64> {
        self.inflight
            .iter()
            .filter(|(_, info)| info.max_sequence >= sequence)
            .min_by_key(|(_, info)| info.max_sequence)
            .map(|(segment_id, _)| *segment_id)
    }
    /// Clear all inflight segments (on error or shutdown).
    pub fn clear_inflight(&mut self) {
        self.inflight.clear();
    }

    /// Check if CloudAsync should flush based on thresholds.
    ///
    /// **CRITICAL**: This function MUST NOT flush based on pending writer count alone.
    /// Implicit flush-on-writer causes sequential benchmarks to measure cloud upload
    /// overhead instead of engine throughput. CloudAsync uploads run asynchronously;
    /// commits never block on upload completion unless explicit CloudStrict policy is used.
    ///
    /// Flush triggers (ALL must be satisfied):
    /// - `cloud_pending > 0` (some data exists to flush)
    /// - At least ONE of:
    ///   * `bytes_buffered >= CLOUD_ASYNC_MIN_SEGMENT_BYTES`
    ///   * `cloud_pending >= CLOUD_ASYNC_MAX_PENDING_WRITES`
    ///   * `elapsed >= CLOUD_ASYNC_MAX_FLUSH_DELAY_BACKLOG`
    pub fn should_flush_cloud_async(&self, cloud_pending: usize, bytes_buffered: usize) -> bool {
        if !self.is_cloud_async {
            return false;
        }

        // CloudAsync rotate/upload policy thresholds
        const CLOUD_ASYNC_MIN_SEGMENT_BYTES: usize = 16 * 1024 * 1024; // 16MB (was 8MB)
        const CLOUD_ASYNC_MAX_FLUSH_DELAY_BACKLOG: std::time::Duration =
            std::time::Duration::from_millis(500); // 500ms (was 25ms)
        const CLOUD_ASYNC_MAX_PENDING_WRITES: usize = 10_000; // 10k (was 2048)

        // FORBIDDEN: Never flush based on pending writer count alone.
        // Pending writers may be used for metrics/backpressure, but MUST NOT trigger flushes.
        // Only threshold-based conditions below are allowed to trigger uploads.

        cloud_pending > 0
            && (bytes_buffered >= CLOUD_ASYNC_MIN_SEGMENT_BYTES
                || cloud_pending >= CLOUD_ASYNC_MAX_PENDING_WRITES
                || self.last_cloud_flush.elapsed() >= CLOUD_ASYNC_MAX_FLUSH_DELAY_BACKLOG)
    }

    /// Update last flush timestamp (call after CloudAsync segment is enqueued).
    pub fn record_cloud_flush(&mut self) {
        self.last_cloud_flush = Instant::now();
    }

    /// Get reference to waiters for inspection (internal use only).
    #[allow(dead_code)]
    pub(super) fn waiters(&self) -> &Option<KeyedGroupCommit<u64, DurabilityWaiter>> {
        &self.waiters
    }
}
