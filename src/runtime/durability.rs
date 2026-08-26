//! Durability coordination logic — manages waiter groups, `CloudAsync` inflight state, and frontier checks.
//!
//! Extracted from `EventLoop` to reduce cognitive load and improve testability.
//! Owns the policy-independent parts of durability enforcement.

use crate::common::KeyedGroupCommit;
#[cfg(test)]
use crate::types::ReadDurability;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(not(test))]
const CLOUD_SEAL_RETRY_DELAY: Duration = Duration::from_secs(1);
#[cfg(test)]
const CLOUD_SEAL_RETRY_DELAY: Duration = Duration::from_millis(10);

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

    /// `CloudAsync`: a seal failed and must be retried.
    cloud_seal_retry_needed: bool,

    /// Earliest time at which a failed cloud seal may be attempted again.
    cloud_seal_retry_at: Option<Instant>,

    /// Is `CloudAsync` enabled? (read from `wal_actor.is_cloud_async()`)
    is_cloud_async: bool,

    cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
    waiters_fanned_out: AtomicU64,
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
            cloud_seal_retry_at: None,
            is_cloud_async,
            cloud_runtime_policy,
            waiters_fanned_out: AtomicU64::new(0),
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

    /// Request ids of the cloud-durability waiters queued at `key`.
    ///
    /// Read before the work runs so the event loop can derive one shared
    /// deadline from the callers it is about to serve.
    pub(crate) fn cloud_durability_request_ids_at(&self, key: u64) -> Vec<u64> {
        self.waiters
            .as_ref()
            .map(|waiters| {
                waiters
                    .inspect_key(&key, |waiter| match waiter {
                        DurabilityWaiter::CloudDurability { request_id } => Some(*request_id),
                        _ => None,
                    })
                    .into_iter()
                    .flatten()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Request ids whose cloud-durability proof depends on `key` closing.
    ///
    /// A waiter attached to a later inflight segment also depends on every
    /// earlier frontier gap. Its remaining budget therefore has to be
    /// considered when validating an acknowledgement for that gap.
    pub(crate) fn cloud_durability_request_ids_at_or_after(&self, key: u64) -> Vec<u64> {
        let mut dependent_keys = self
            .inflight
            .keys()
            .copied()
            .filter(|candidate| *candidate >= key)
            .collect::<Vec<_>>();
        if !dependent_keys.contains(&key) {
            dependent_keys.push(key);
        }
        dependent_keys.sort_unstable();
        dependent_keys.dedup();
        dependent_keys
            .into_iter()
            .flat_map(|dependent_key| self.cloud_durability_request_ids_at(dependent_key))
            .collect()
    }

    /// Get all waiters ready for completion at the given key.
    pub fn complete_waiters_at(&self, key: u64) -> Vec<DurabilityWaiter> {
        let completed = self
            .waiters
            .as_ref()
            .map(|w| w.complete(&key))
            .unwrap_or_default();
        self.waiters_fanned_out.fetch_add(
            u64::try_from(completed.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        completed
    }

    #[must_use]
    pub(crate) fn waiters_fanned_out(&self) -> u64 {
        self.waiters_fanned_out.load(Ordering::Relaxed)
    }

    /// Drain all pending waiters (used on shutdown or error).
    pub fn drain_all_waiters(&self) -> Vec<DurabilityWaiter> {
        self.waiters
            .as_ref()
            .map(super::super::common::singleflight::KeyedGroupCommit::drain_all)
            .unwrap_or_default()
    }

    /// Drain waiters whose cloud durability depends on a failed segment.
    /// Earlier sealed generations remain independently completable.
    pub fn drain_waiters_at_or_after(&self, segment_id: u64) -> Vec<DurabilityWaiter> {
        self.waiters
            .as_ref()
            .map(|waiters| waiters.drain_where(|key| *key >= segment_id))
            .unwrap_or_default()
    }

    /// Drain every waiter and realign the coordinator after an external WAL
    /// generation advanced without the paired checked transition.
    pub fn drain_all_waiters_and_reset(&self, new_key: u64) -> Vec<DurabilityWaiter> {
        self.waiters
            .as_ref()
            .map(|waiters| waiters.drain_all_and_reset(new_key))
            .unwrap_or_default()
    }

    /// Check if there are pending waiters.
    pub fn has_pending_waiters(&self) -> bool {
        self.waiters.as_ref().is_some_and(|w| w.pending_len() > 0)
    }

    /// Rotate group commit to new key (advance generation/segment).
    pub fn rotate_from_to(
        &self,
        expected_current: u64,
        new_key: u64,
    ) -> crate::common::MidgeResult<()> {
        if let Some(waiters) = &self.waiters {
            waiters
                .rotate_from_to(&expected_current, new_key)
                .map_err(|_| {
                    crate::common::MidgeError::Internal(format!(
                        "durability generation drift: expected current key {expected_current}"
                    ))
                })?;
        }
        Ok(())
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

    /// Return the expected maximum sequence for an inflight segment without
    /// removing it from the contiguous durability frontier.
    ///
    /// A failed upload may be retried by the storage layer. Keeping its
    /// frontier entry in place prevents later segment acknowledgements from
    /// skipping the failed segment while that retry is outstanding.
    pub fn cloud_segment_max_sequence(&self, segment_id: u64) -> Option<u64> {
        self.inflight.get(&segment_id).map(|info| info.max_sequence)
    }

    pub fn inflight_segment_for_sequence(&self, sequence: u64) -> Option<u64> {
        self.inflight
            .iter()
            .filter(|(_, info)| info.max_sequence >= sequence)
            .min_by_key(|(_, info)| info.max_sequence)
            .map(|(segment_id, _)| *segment_id)
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

        if pending_writes == 0 {
            return false;
        }
        if self.cloud_seal_retry_needed {
            return self.cloud_seal_retry_due();
        }

        bytes_buffered >= self.cloud_runtime_policy.wal_seal.min_segment_bytes
            || pending_writes >= self.cloud_runtime_policy.wal_seal.max_pending_writes
            || self.last_cloud_flush.elapsed() >= self.cloud_runtime_policy.wal_seal.max_flush_delay
    }

    /// Return the remaining time before a non-empty `CloudAsync` WAL must be
    /// sealed. The event loop uses this deadline while idle so a sub-threshold
    /// segment neither spins nor waits for unrelated background maintenance.
    #[must_use]
    pub fn cloud_seal_deadline_timeout(&self, pending_writes: usize) -> Option<Duration> {
        if !self.is_cloud_async || pending_writes == 0 {
            return None;
        }
        if self.cloud_seal_retry_needed {
            return Some(self.cloud_seal_retry_at.map_or(Duration::ZERO, |retry_at| {
                retry_at.saturating_duration_since(Instant::now())
            }));
        }

        Some(
            self.cloud_runtime_policy
                .wal_seal
                .max_flush_delay
                .saturating_sub(self.last_cloud_flush.elapsed()),
        )
    }

    pub fn mark_cloud_seal_retry_needed(&mut self) {
        self.cloud_seal_retry_needed = true;
    }

    pub fn defer_cloud_seal_retry(&mut self) {
        if self.cloud_seal_retry_needed {
            self.cloud_seal_retry_at = Some(Instant::now() + CLOUD_SEAL_RETRY_DELAY);
        }
    }

    pub fn clear_cloud_seal_retry_needed(&mut self) {
        self.cloud_seal_retry_needed = false;
        self.cloud_seal_retry_at = None;
    }

    #[must_use]
    pub fn cloud_seal_retry_needed(&self) -> bool {
        self.cloud_seal_retry_needed
    }

    #[must_use]
    pub fn cloud_seal_retry_due(&self) -> bool {
        self.cloud_seal_retry_needed
            && self
                .cloud_seal_retry_at
                .is_none_or(|retry_at| Instant::now() >= retry_at)
    }

    /// Update last flush timestamp (call after `CloudAsync` segment is enqueued).
    pub fn record_cloud_flush(&mut self) {
        self.last_cloud_flush = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cloud_coordinator() -> DurabilityCoordinator {
        DurabilityCoordinator::new(0, true, crate::runtime::CloudRuntimePolicy::default())
    }

    #[test]
    fn should_buffer_ack_given_earlier_segment_still_unacked_when_ack_arrives_out_of_order() {
        // Arrange
        let mut coordinator = cloud_coordinator();
        coordinator.record_cloud_segment_inflight(1, 10);
        coordinator.record_cloud_segment_inflight(2, 20);
        let mut acknowledgements = BTreeMap::from([(2, 20)]);

        // Act
        let blocked = coordinator
            .take_contiguous_acked_cloud_segments(&acknowledgements)
            .expect("buffer later acknowledgement");
        acknowledgements.insert(1, 10);
        let ready = coordinator
            .take_contiguous_acked_cloud_segments(&acknowledgements)
            .expect("drain contiguous acknowledgements");

        // Assert
        assert!(blocked.is_empty());
        assert_eq!(ready, vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn should_reject_ack_given_mismatched_max_sequence_when_segment_is_inflight() {
        // Arrange
        let mut coordinator = cloud_coordinator();
        coordinator.record_cloud_segment_inflight(7, 70);
        let acknowledgements = BTreeMap::from([(7, 71)]);

        // Act
        let result = coordinator.take_contiguous_acked_cloud_segments(&acknowledgements);

        // Assert
        assert!(matches!(result, Err(message) if message.contains("does not match")));
        assert_eq!(coordinator.cloud_segment_max_sequence(7), Some(70));
    }

    #[test]
    fn should_expose_cloud_seal_deadline_only_given_pending_cloud_async_wal() {
        // Arrange
        let cloud = cloud_coordinator();
        let local =
            DurabilityCoordinator::new(0, false, crate::runtime::CloudRuntimePolicy::default());

        // Act
        let pending_deadline = cloud.cloud_seal_deadline_timeout(1);
        let empty_deadline = cloud.cloud_seal_deadline_timeout(0);
        let local_deadline = local.cloud_seal_deadline_timeout(1);

        // Assert
        assert!(pending_deadline.is_some());
        assert_eq!(empty_deadline, None);
        assert_eq!(local_deadline, None);
    }

    #[test]
    fn should_retain_waiter_when_completing_different_generation() {
        // Arrange
        let coordinator = cloud_coordinator();
        coordinator.queue_waiter(DurabilityWaiter::CloudDurability { request_id: 1 });
        coordinator.rotate_from_to(0, 1).expect("rotate generation");

        // Act
        let wrong = coordinator.complete_waiters_at(1);
        let correct = coordinator.complete_waiters_at(0);

        // Assert
        assert!(wrong.is_empty());
        assert!(matches!(
            correct.as_slice(),
            [DurabilityWaiter::CloudDurability { request_id: 1 }]
        ));
        assert_eq!(coordinator.waiters_fanned_out(), 1);
    }

    #[test]
    fn should_complete_all_waiters_once_when_generation_seals() {
        // Arrange
        let coordinator = cloud_coordinator();
        coordinator.queue_waiter_for_key(0, DurabilityWaiter::CloudDurability { request_id: 10 });
        coordinator.queue_waiter(DurabilityWaiter::CloudDurability { request_id: 11 });
        assert_eq!(coordinator.complete_waiters_at(0).len(), 1);
        coordinator.queue_waiter_for_key(0, DurabilityWaiter::CloudDurability { request_id: 10 });

        // Act
        coordinator.rotate_from_to(0, 1).expect("seal generation");
        let completed = coordinator.complete_waiters_at(0);

        // Assert
        assert_eq!(completed.len(), 2);
        assert!(coordinator.complete_waiters_at(0).is_empty());
        assert_eq!(coordinator.waiters_fanned_out(), 3);
    }
}
