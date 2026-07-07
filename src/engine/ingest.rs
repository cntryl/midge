//! Internal ingest batching for write throughput optimization
//!
//! This module implements per-CF write batching INTERNALLY to increase throughput
//! for concurrent streaming and write-heavy workloads. It does NOT change any
//! public APIs or semantics.
//!
//! Design:
//! - Concurrent transactions submit through per-CF write grouping.
//! - A temporary leader drains follower submissions and commits a merged runtime transaction.
//! - Backpressure checks stay tied to runtime stall state.
//! - Correctness: writes are atomic, ordered per CF, errors propagate to caller

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{next_request_id, RuntimeHandle, RuntimeResponse, TransactionOp};
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Maximum operations per batch before forcing a commit
const MAX_BATCH_OPS: usize = 1024;

/// Maximum transactions to group together before forcing commit
const MAX_GROUPED_BATCHES: usize = 64;

/// Initial/default timeout for adaptive write grouping
const WRITE_GROUP_TIMEOUT_INITIAL: u64 = 100; // microseconds

/// Minimum adaptive timeout (favor low latency at low concurrency)
const WRITE_GROUP_TIMEOUT_MIN: u64 = 10;

/// Maximum adaptive timeout (cap at diminishing returns point)
const WRITE_GROUP_TIMEOUT_MAX: u64 = 500;

/// Batch size threshold for increasing timeout (high traffic signal)
const HIGH_BATCHING_THRESHOLD: usize = 16;

/// Batch size threshold for decreasing timeout (low traffic signal)
const LOW_BATCHING_THRESHOLD: usize = 2;

/// Maximum time a leader waits for the runtime to apply a grouped transaction
const WRITE_GROUP_APPLY_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum time a waiter will block waiting for a leader response
const WRITE_GROUP_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval at which a follower checks whether it must rescue orphaned queued work.
const WRITE_GROUP_RESCUE_INTERVAL: Duration = Duration::from_micros(50);

#[derive(Clone, Copy)]
struct ApplyTransactionOptions {
    start_sequence: Option<u64>,
    conflict_policy: crate::runtime::ConflictPolicy,
    collect_submit_timing: bool,
}

fn submit_timing_phase_start(enabled: bool) -> Option<Instant> {
    enabled.then(Instant::now)
}

fn record_submit_leader_collect(started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        crate::diagnostics::record_transaction_submit_leader_collect(started_at.elapsed());
    }
}

fn record_submit_runtime_apply(started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        crate::diagnostics::record_transaction_submit_runtime_apply(started_at.elapsed());
    }
}

fn record_submit_follower_wait(started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        crate::diagnostics::record_transaction_submit_follower_wait(started_at.elapsed());
    }
}

/// Pending batch request waiting for leader to group it with others
pub(crate) struct PendingBatchRequest {
    /// The batch of operations to commit
    pub ops: Vec<TransactionOp>,
    /// Durability policy for this batch (None = use default)
    pub durability_policy: Option<crate::wal::DurabilityPolicy>,
    /// Response channel to send result back to caller
    pub result_tx: crossbeam::channel::Sender<MidgeResult<u64>>,
}

/// Coordinator for write grouping / leader-based batching
///
/// This mechanism reduces the rate of `ApplyTransaction` messages sent to the runtime
/// by merging multiple pending batch submissions from concurrent threads into a
/// single transaction. The "leader" thread drains pending requests and commits them
/// as a merged batch, reducing single-threaded event loop contention.
///
/// Adaptive timeout mechanism:
/// - High concurrency (batching many): increases timeout to collect more requests
/// - Low concurrency (batching few): decreases timeout to reduce latency
/// - Self-tunes to workload pattern
///
/// This is inspired by the write grouping pattern used in `RocksDB`, `PebbleDB`, etc.
pub(crate) struct WriteGroupCoordinator {
    /// Atomic flag: true if a thread is actively serving as leader
    leader_active: AtomicBool,
    /// Bounded queue of pending batch requests waiting to be grouped
    pending_queue: (Sender<PendingBatchRequest>, Receiver<PendingBatchRequest>),
    /// Adaptive timeout in microseconds (self-tunes based on batching effectiveness)
    adaptive_timeout_us: std::sync::atomic::AtomicU64,
    /// Metrics
    leader_runs: Arc<std::sync::atomic::AtomicU64>,
    batches_grouped: Arc<std::sync::atomic::AtomicU64>,
}

struct LeaderGuard {
    coord: Arc<WriteGroupCoordinator>,
    active: bool,
}

impl LeaderGuard {
    fn new(coord: Arc<WriteGroupCoordinator>) -> Self {
        Self {
            coord,
            active: true,
        }
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if self.active {
            self.coord.release_leader();
        }
    }
}

impl WriteGroupCoordinator {
    pub fn new() -> Self {
        let (tx, rx) = bounded(1024);
        Self {
            leader_active: AtomicBool::new(false),
            pending_queue: (tx, rx),
            adaptive_timeout_us: std::sync::atomic::AtomicU64::new(WRITE_GROUP_TIMEOUT_INITIAL),
            leader_runs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            batches_grouped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Get current adaptive timeout as Duration
    fn get_timeout(&self) -> Duration {
        let us = self.adaptive_timeout_us.load(Ordering::Relaxed);
        Duration::from_micros(us)
    }

    /// Adapt timeout based on batching effectiveness
    ///
    /// - High batching (>= 16 requests): increase timeout by 20% (more traffic likely)
    /// - Low batching (<= 2 requests): decrease timeout by 20% (favor low latency)
    /// - Medium batching: keep stable
    fn adjust_timeout(&self, batch_size: usize) {
        let current = self.adaptive_timeout_us.load(Ordering::Relaxed);

        let new_timeout = if batch_size >= HIGH_BATCHING_THRESHOLD {
            // High traffic: increase timeout to collect more requests
            // +20% per adjustment, capped at max
            (current * 12 / 10).min(WRITE_GROUP_TIMEOUT_MAX)
        } else if batch_size <= LOW_BATCHING_THRESHOLD {
            // Low traffic: decrease timeout for lower latency
            // -20% per adjustment, floored at min
            (current * 8 / 10).max(WRITE_GROUP_TIMEOUT_MIN)
        } else {
            // Medium traffic: stable (no adjustment)
            current
        };

        if new_timeout != current {
            self.adaptive_timeout_us
                .store(new_timeout, Ordering::Relaxed);
        }
    }

    /// Try to become the leader. Returns true if CAS succeeded.
    fn try_acquire_leader(&self) -> bool {
        self.leader_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Release the leader lock
    fn release_leader(&self) {
        self.leader_active.store(false, Ordering::Release);
    }

    /// Get metrics for monitoring
    pub fn metrics(&self) -> (u64, u64, u64) {
        (
            self.leader_runs.load(Ordering::Relaxed),
            self.batches_grouped.load(Ordering::Relaxed),
            self.adaptive_timeout_us.load(Ordering::Relaxed),
        )
    }
}

/// Per-CF ingest coordinator
pub(crate) struct IngestCoordinator {
    cf_id: crate::engine::ColumnFamilyId,
    /// Cached write stall status.
    stall_flag: Arc<AtomicBool>,
    /// Write grouping coordinator for batch submissions
    write_group_coord: Arc<WriteGroupCoordinator>,
}

impl IngestCoordinator {
    /// Create an ingest coordinator for a column family.
    pub fn new(cf_id: crate::engine::ColumnFamilyId) -> Self {
        let stall_flag = Arc::new(AtomicBool::new(false));
        Self {
            cf_id,
            stall_flag,
            write_group_coord: Arc::new(WriteGroupCoordinator::new()),
        }
    }

    /// Submit a batch with write grouping / leader-based batching.
    ///
    /// This implementation reduces message rate to the runtime event loop by merging
    /// multiple concurrent batch submissions into a single `ApplyTransaction`.
    ///
    /// The key idea (from `RocksDB` write grouping):
    /// - First caller becomes "leader" (via atomic CAS)
    /// - Leader drains all pending requests from the queue
    /// - Leader merges all ops into a single transaction
    /// - Leader sends ONE `ApplyTransaction` to runtime
    /// - Leader fans-out the response to all waiters
    /// - Other callers wait for the leader's response
    ///
    /// The `durability_policy` parameter allows per-request durability control.
    /// If None, the runtime will use the engine's default durability policy.
    pub fn submit_ops(
        &self,
        runtime: &RuntimeHandle,
        ops: Vec<TransactionOp>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
        start_sequence: Option<u64>,
        conflict_policy: crate::runtime::ConflictPolicy,
        collect_submit_timing: bool,
    ) -> MidgeResult<u64> {
        if ops.is_empty() {
            return Ok(0);
        }

        if self.stall_flag.load(Ordering::Acquire) {
            if let Ok(true) = runtime.check_write_stall(self.cf_id) {
                return Err(MidgeError::WriteStall(format!(
                    "Memory budget exceeded for CF {}",
                    self.cf_id
                )));
            }
            self.stall_flag.store(false, Ordering::Release);
        }

        // Explicit transaction commits carry a start sequence and must not be
        // merged by caller-side write grouping. The runtime may still coalesce
        // safe WAL appends while preserving each transaction's response.
        if start_sequence.is_some() {
            return self.submit_direct(
                runtime,
                ops,
                durability_policy,
                start_sequence,
                conflict_policy,
                collect_submit_timing,
            );
        }

        if self.write_group_coord.try_acquire_leader() {
            self.drain_as_leader(runtime, Some(ops), durability_policy, collect_submit_timing)
                .unwrap_or_else(|| {
                    Err(MidgeError::Internal(
                        "Write group leader completed with no result".to_string(),
                    ))
                })
        } else {
            self.submit_as_follower(runtime, ops, durability_policy, collect_submit_timing)
        }
    }

    fn drain_as_leader(
        &self,
        runtime: &RuntimeHandle,
        initial_ops: Option<Vec<TransactionOp>>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
        collect_submit_timing: bool,
    ) -> Option<MidgeResult<u64>> {
        let _leader_guard = LeaderGuard::new(Arc::clone(&self.write_group_coord));

        self.write_group_coord
            .leader_runs
            .fetch_add(1, Ordering::Relaxed);

        let mut initial_ops = initial_ops;
        let mut initial_result: Option<MidgeResult<u64>> = None;

        loop {
            let mut pending_requests = Vec::new();

            let (mut all_ops, batch_durability, is_initial_batch) = match initial_ops.take() {
                Some(initial) => (initial, durability_policy, true),
                None => match self.write_group_coord.pending_queue.1.try_recv() {
                    Ok(pending) => {
                        pending_requests.push((pending.result_tx, pending.durability_policy));
                        (pending.ops, pending.durability_policy, false)
                    }
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                },
            };

            if all_ops.is_empty() {
                break;
            }

            let collect_started_at = submit_timing_phase_start(collect_submit_timing);
            all_ops = self.drain_pending_queue(all_ops, &mut pending_requests);
            record_submit_leader_collect(collect_started_at);

            if all_ops.is_empty() {
                break;
            }

            let batch_size = pending_requests.len() + usize::from(is_initial_batch);
            self.write_group_coord
                .batches_grouped
                .fetch_add(batch_size as u64, Ordering::Relaxed);

            let result =
                self.apply_transaction(runtime, all_ops, batch_durability, collect_submit_timing);

            self.notify_waiters(&pending_requests, &result);

            if is_initial_batch {
                initial_result = Some(result);
            }

            self.write_group_coord.adjust_timeout(batch_size);
        }

        initial_result
    }

    fn drain_pending_queue(
        &self,
        mut all_ops: Vec<TransactionOp>,
        pending_requests: &mut Vec<(
            crossbeam::channel::Sender<MidgeResult<u64>>,
            Option<crate::wal::DurabilityPolicy>,
        )>,
    ) -> Vec<TransactionOp> {
        let drain_start = Instant::now();
        let adaptive_timeout = self.write_group_coord.get_timeout();

        loop {
            match self.write_group_coord.pending_queue.1.try_recv() {
                Ok(pending) => {
                    all_ops.extend(pending.ops);
                    pending_requests.push((pending.result_tx, pending.durability_policy));
                }
                Err(TryRecvError::Empty) => {
                    if drain_start.elapsed() > adaptive_timeout
                        || pending_requests.len() >= MAX_GROUPED_BATCHES
                        || all_ops.len() > MAX_BATCH_OPS
                    {
                        break;
                    }
                    std::thread::yield_now();
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }

        all_ops
    }

    fn apply_transaction(
        &self,
        runtime: &RuntimeHandle,
        ops: Vec<TransactionOp>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
        collect_submit_timing: bool,
    ) -> MidgeResult<u64> {
        Self::send_apply_transaction(
            runtime,
            ops,
            durability_policy,
            ApplyTransactionOptions {
                start_sequence: None,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
                collect_submit_timing,
            },
            &self.stall_flag,
            Some((WRITE_GROUP_APPLY_TIMEOUT, "Write group commit timed out")),
            None,
        )
    }

    fn notify_waiters(
        &self,
        pending_requests: &Vec<(
            crossbeam::channel::Sender<MidgeResult<u64>>,
            Option<crate::wal::DurabilityPolicy>,
        )>,
        result: &MidgeResult<u64>,
    ) {
        let error_msg = match result {
            Ok(_) => None,
            Err(e) => Some(format!("Write group commit failed: {e:?}")),
        };

        for (waiter_tx, _) in pending_requests {
            if let Ok(seq) = result {
                if waiter_tx.send(Ok(*seq)).is_err() {
                    tracing::warn!(
                        cf_id = self.cf_id,
                        seq = *seq,
                        "failed to send write result to waiter (receiver dropped)"
                    );
                }
            } else {
                let err_msg = error_msg.clone().unwrap_or_default();
                if waiter_tx.send(Err(MidgeError::Internal(err_msg))).is_err() {
                    tracing::warn!(
                        cf_id = self.cf_id,
                        "failed to send error to waiter (receiver dropped)"
                    );
                }
            }
        }
    }

    fn submit_as_follower(
        &self,
        runtime: &RuntimeHandle,
        ops: Vec<TransactionOp>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
        collect_submit_timing: bool,
    ) -> MidgeResult<u64> {
        let (result_tx, result_rx) = crossbeam::channel::bounded(1);

        let pending = PendingBatchRequest {
            ops,
            durability_policy,
            result_tx,
        };

        match self.write_group_coord.pending_queue.0.try_send(pending) {
            Ok(()) => self.wait_for_grouped_result(runtime, &result_rx, collect_submit_timing),
            Err(crossbeam::channel::TrySendError::Full(pending)) => self.submit_direct(
                runtime,
                pending.ops,
                pending.durability_policy,
                None,
                crate::runtime::ConflictPolicy::LastWriteWins,
                collect_submit_timing,
            ),
            Err(crossbeam::channel::TrySendError::Disconnected(_)) => Err(MidgeError::Internal(
                "Write grouping coordinator disconnected".to_string(),
            )),
        }
    }

    fn wait_for_grouped_result(
        &self,
        runtime: &RuntimeHandle,
        result_rx: &crossbeam::channel::Receiver<MidgeResult<u64>>,
        collect_submit_timing: bool,
    ) -> MidgeResult<u64> {
        let started_at = Instant::now();
        let follower_wait_started_at = submit_timing_phase_start(collect_submit_timing);

        loop {
            let elapsed = started_at.elapsed();
            if elapsed >= WRITE_GROUP_WAIT_TIMEOUT {
                record_submit_follower_wait(follower_wait_started_at);
                return Err(MidgeError::Internal(
                    "Write grouping leader timed out".to_string(),
                ));
            }

            let remaining = WRITE_GROUP_WAIT_TIMEOUT.saturating_sub(elapsed);
            let wait_for = remaining.min(WRITE_GROUP_RESCUE_INTERVAL);
            match result_rx.recv_timeout(wait_for) {
                Ok(result) => {
                    record_submit_follower_wait(follower_wait_started_at);
                    return result;
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    if self.write_group_coord.try_acquire_leader() {
                        let _ = self.drain_as_leader(runtime, None, None, collect_submit_timing);
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    record_submit_follower_wait(follower_wait_started_at);
                    return Err(MidgeError::Internal(
                        "Write grouping leader disconnected".to_string(),
                    ));
                }
            }
        }
    }

    fn submit_direct(
        &self,
        runtime: &RuntimeHandle,
        ops: Vec<TransactionOp>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
        start_sequence: Option<u64>,
        conflict_policy: crate::runtime::ConflictPolicy,
        collect_submit_timing: bool,
    ) -> MidgeResult<u64> {
        Self::send_apply_transaction(
            runtime,
            ops,
            durability_policy,
            ApplyTransactionOptions {
                start_sequence,
                conflict_policy,
                collect_submit_timing,
            },
            &self.stall_flag,
            None,
            None,
        )
    }

    fn send_apply_transaction(
        runtime: &RuntimeHandle,
        ops: Vec<TransactionOp>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
        options: ApplyTransactionOptions,
        stall_flag: &AtomicBool,
        timeout: Option<(Duration, &'static str)>,
        expected_op_count: Option<usize>,
    ) -> MidgeResult<u64> {
        let request_id = next_request_id()?;

        let runtime_apply_started_at = submit_timing_phase_start(options.collect_submit_timing);
        let response_result = if let Some((timeout, timeout_msg)) = timeout {
            runtime
                .send_apply_transaction_and_wait_timeout(
                    request_id,
                    ops,
                    durability_policy,
                    options.start_sequence,
                    options.conflict_policy,
                    timeout,
                )
                .and_then(|response| {
                    response.ok_or_else(|| MidgeError::Internal(timeout_msg.to_string()))
                })
        } else {
            runtime.send_apply_transaction_and_wait(
                request_id,
                ops,
                durability_policy,
                options.start_sequence,
                options.conflict_policy,
            )
        };
        record_submit_runtime_apply(runtime_apply_started_at);

        let response = response_result?;
        Self::decode_apply_transaction_response(response, expected_op_count, stall_flag)
    }

    fn decode_apply_transaction_response(
        response: RuntimeResponse,
        expected_op_count: Option<usize>,
        stall_flag: &AtomicBool,
    ) -> MidgeResult<u64> {
        match response {
            RuntimeResponse::TransactionApplied {
                last_sequence,
                op_count,
                write_stall_hint,
                ..
            } => {
                stall_flag.store(write_stall_hint, Ordering::Release);

                if let Some(expected) = expected_op_count {
                    if op_count != expected {
                        return Err(MidgeError::Internal(format!(
                            "Batch op count mismatch: expected {expected}, got {op_count}"
                        )));
                    }
                }

                Ok(last_sequence)
            }
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to ApplyTransaction".to_string(),
            )),
        }
    }
}

impl Drop for IngestCoordinator {
    fn drop(&mut self) {
        let (leader_runs, batches_grouped, final_timeout_us) = self.write_group_coord.metrics();
        if leader_runs > 0 {
            let avg_group_size = f64::from(u32::try_from(batches_grouped).unwrap_or(u32::MAX))
                / f64::from(u32::try_from(leader_runs).unwrap_or(u32::MAX));
            tracing::debug!(
                cf_id = self.cf_id,
                leader_runs,
                batches_grouped,
                avg_group_size,
                final_timeout_us,
                "Write-group coordinator summary"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::state::RuntimeState;
    use crate::runtime::{Runtime, RuntimeConfig};
    use crate::wal::DurabilityPolicy;
    use bytes::Bytes;

    #[test]
    fn should_drain_queued_follower_when_rescue_leader_has_no_initial_ops(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let (runtime, _handle) = Runtime::new();
        let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
        let (runtime, runtime_handle) = runtime.start_with_config(
            state,
            RuntimeConfig {
                wal_durability_policy: DurabilityPolicy::Batched,
                ..RuntimeConfig::default()
            },
        )?;
        let coordinator = IngestCoordinator::new(0);
        let (result_tx, result_rx) = crossbeam::channel::bounded(1);
        let pending = PendingBatchRequest {
            ops: vec![TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"rescue-key"),
                value: Bytes::from_static(b"rescue-value"),
                ttl_seconds: None,
                insert_only: false,
            }],
            durability_policy: Some(DurabilityPolicy::Batched),
            result_tx,
        };
        assert!(coordinator.write_group_coord.try_acquire_leader());
        coordinator
            .write_group_coord
            .pending_queue
            .0
            .try_send(pending)
            .expect("queue pending request");

        // Act
        let initial_result = coordinator.drain_as_leader(&runtime_handle, None, None, false);

        // Assert
        assert!(initial_result.is_none());
        let result = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("rescue leader should notify queued follower");
        assert!(result.is_ok());

        runtime.shutdown();
        Ok(())
    }
}
