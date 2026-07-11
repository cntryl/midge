//! Durability coordination logic — manages waiter groups, `CloudAsync` inflight state, and frontier checks.
//!
//! Extracted from `EventLoop` to reduce cognitive load and improve testability.
//! Owns the policy-independent parts of durability enforcement.

use crate::common::KeyedGroupCommit;
#[cfg(test)]
use crate::types::ReadDurability;
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

/// Single `CloudAsync` segment being uploaded
#[derive(Debug, Clone)]
pub struct CloudAsyncInflightSegment {
    pub enqueued_at: Instant,
    pub max_sequence: u64,
}

/// Waiter types for group commit
#[derive(Debug, Clone)]
pub enum DurabilityWaiter {
    #[cfg(test)]
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
        touched_cfs: Vec<crate::types::ColumnFamilyId>,
    },
    /// Internal waiter used when caller already acknowledged but needs cleanup.
    ConfirmTransactionApply {
        request_id: u64,
    },
    CloudDurability {
        request_id: u64,
    },
    #[cfg(test)]
    Read {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64,
    },
    #[cfg(test)]
    RangeScan {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start: Vec<u8>,
        end: Vec<u8>,
        sequence: u64,
    },
}

/// Coordinates all durability-related state and decisions.
///
/// Owns:
/// - Group commit waiter queues (`KeyedGroupCommit`)
/// - `CloudAsync` inflight segment tracking
/// - Durability frontier checks
///
/// Does NOT own:
/// - WAL actor (read-only access to `WalState` for frontier checks)
/// - Storage or network concerns (those are caller's responsibility)
pub struct DurabilityCoordinator {
    /// Group commit: waiters keyed by WAL segment or generation
    waiters: Option<KeyedGroupCommit<u64, DurabilityWaiter>>,

    /// `CloudAsync`: track enqueue->ack per WAL segment
    inflight: HashMap<u64, CloudAsyncInflightSegment>,

    /// `CloudAsync`: timestamp of last flush/rotate
    last_cloud_flush: Instant,

    /// `CloudAsync`: a seal failed after local WAL flush and must be retried.
    cloud_seal_retry_needed: bool,

    /// Is `CloudAsync` enabled? (read from `wal_actor.is_cloud_async()`)
    is_cloud_async: bool,

    cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
}

impl DurabilityCoordinator {
    /// Create a new coordinator with initial durability key.
    pub fn new(
        initial_durability_key: u64,
        is_cloud_async: bool,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
    ) -> Self {
        Self {
            waiters: Some(KeyedGroupCommit::new(initial_durability_key)),
            inflight: HashMap::new(),
            last_cloud_flush: Instant::now(),
            cloud_seal_retry_needed: false,
            is_cloud_async,
            cloud_runtime_policy,
        }
    }

    /// Check if a sequence number is durable at the requested level.
    ///
    /// Special case: `u64::MAX` (latest available) always returns true and bypasses durability checks.
    #[inline]
    #[cfg(test)]
    pub fn is_durable(
        sequence: u64,
        requested_durability: ReadDurability,
        local_durable_seq: u64,
        cloud_durable_seq: u64,
    ) -> bool {
        if sequence == u64::MAX {
            // "Latest available" reads proceed immediately; no durability guarantee needed
            return true;
        }

        match requested_durability {
            ReadDurability::Strict | ReadDurability::Steady => sequence <= local_durable_seq,
            ReadDurability::CloudPersisted => sequence <= cloud_durable_seq,
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
            .map(super::super::common::singleflight::KeyedGroupCommit::drain_all)
            .unwrap_or_default()
    }

    /// Check if there are pending waiters.
    pub fn has_pending_waiters(&self) -> bool {
        self.waiters.as_ref().is_some_and(|w| w.pending_len() > 0)
    }

    /// Rotate group commit to new key (advance generation/segment).
    pub fn rotate_to(&self, new_key: u64) {
        if let Some(waiters) = &self.waiters {
            let _ = waiters.rotate_to(new_key);
        }
    }

    /// Record a `CloudAsync` segment enqueued for upload.
    pub fn record_cloud_segment_inflight(&mut self, segment_id: u64, max_sequence: u64) {
        self.inflight.insert(
            segment_id,
            CloudAsyncInflightSegment {
                enqueued_at: Instant::now(),
                max_sequence,
            },
        );
    }

    /// Get the contiguous acked `CloudAsync` segments starting at the oldest
    /// inflight segment. A later segment ack never makes earlier gaps durable.
    pub fn take_contiguous_acked_cloud_segments(
        &mut self,
        acked_segments: &BTreeMap<u64, u64>,
    ) -> Result<Vec<(u64, u64)>, String> {
        let mut inflight: Vec<(u64, u64)> = self
            .inflight
            .iter()
            .map(|(segment_id, info)| (*segment_id, info.max_sequence))
            .collect();
        inflight.sort_unstable_by_key(|(segment_id, _)| *segment_id);

        let mut ready = Vec::new();
        for (segment_id, expected_max_sequence) in inflight {
            let Some(acked_max_sequence) = acked_segments.get(&segment_id) else {
                break;
            };
            if *acked_max_sequence != expected_max_sequence {
                return Err(format!(
                    "cloud WAL segment {segment_id} ack max sequence {acked_max_sequence} does not match inflight max sequence {expected_max_sequence}"
                ));
            }
            ready.push((segment_id, expected_max_sequence));
        }

        for (segment_id, _) in &ready {
            self.inflight.remove(segment_id);
        }

        Ok(ready)
    }

    /// Get timing info for a `CloudAsync` segment (for telemetry).
    pub fn take_cloud_segment_timing(&mut self, segment_id: u64) -> Option<Instant> {
        self.inflight
            .remove(&segment_id)
            .map(|info| info.enqueued_at)
    }

    /// Remove and return the `max_sequence` for a specific inflight segment.
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

    /// Check if `CloudAsync` should flush based on thresholds.
    ///
    /// **CRITICAL**: This function MUST NOT flush based on pending writer count alone.
    /// Implicit flush-on-writer causes sequential benchmarks to measure cloud upload
    /// overhead instead of engine throughput. `CloudAsync` uploads run asynchronously;
    /// commits never block on upload completion unless explicit `CloudStrict` policy is used.
    ///
    /// Flush triggers (ALL must be satisfied):
    /// - `pending_writes > 0` (some local WAL data exists to seal)
    /// - At least ONE of:
    ///   * `bytes_buffered >= CLOUD_ASYNC_MIN_SEGMENT_BYTES`
    ///   * `pending_writes >= CLOUD_ASYNC_MAX_PENDING_WRITES`
    ///   * `elapsed >= CLOUD_ASYNC_MAX_FLUSH_DELAY_BACKLOG`
    pub fn should_flush_cloud_async(&self, pending_writes: usize, bytes_buffered: usize) -> bool {
        if !self.is_cloud_async {
            return false;
        }

        // FORBIDDEN: Never flush based on pending writer count alone.
        // Pending writers may be used for metrics/backpressure, but MUST NOT trigger flushes.
        // Only threshold-based conditions below are allowed to trigger uploads.

        pending_writes > 0
            && (bytes_buffered >= self.cloud_runtime_policy.wal_seal.min_segment_bytes
                || pending_writes >= self.cloud_runtime_policy.wal_seal.max_pending_writes
                || self.last_cloud_flush.elapsed()
                    >= self.cloud_runtime_policy.wal_seal.max_flush_delay)
    }

    pub fn mark_cloud_seal_retry_needed(&mut self) {
        self.cloud_seal_retry_needed = true;
    }

    pub fn clear_cloud_seal_retry_needed(&mut self) {
        self.cloud_seal_retry_needed = false;
    }

    #[must_use]
    pub fn cloud_seal_retry_needed(&self) -> bool {
        self.cloud_seal_retry_needed
    }

    /// Update last flush timestamp (call after `CloudAsync` segment is enqueued).
    pub fn record_cloud_flush(&mut self) {
        self.last_cloud_flush = Instant::now();
    }
}
