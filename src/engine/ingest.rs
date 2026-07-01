//! Internal ingest batching for write throughput optimization
//!
//! This module implements per-CF write batching INTERNALLY to increase throughput
//! for concurrent streaming and write-heavy workloads. It does NOT change any
//! public APIs or semantics.
//!
//! Design:
//! - Each column family has one ingest loop/task
//! - Concurrent writers enqueue write intents instead of committing immediately
//! - The ingest loop builds a `WriteBatch` and commits as a SINGLE transaction
//! - Batching policy: flush when max ops/bytes/deadline reached
//! - Backpressure: bounded queue enforces `WriteStall` semantics
//! - Correctness: writes are atomic, ordered per CF, errors propagate to caller

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{next_request_id, RuntimeHandle, RuntimeResponse, TransactionOp};
use bytes::Bytes;
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Maximum operations per batch before forcing a commit
const MAX_BATCH_OPS: usize = 1024;

/// Maximum bytes per batch before forcing a commit
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024; // 4MB

struct BatchMetrics<'a> {
    cf_id: crate::engine::ColumnFamilyId,
    batch_count: &'a mut u64,
    total_batch_size: &'a mut u64,
    max_batch_size: &'a mut usize,
    loop_start: &'a Instant,
}

/// Maximum time to wait before forcing a batch commit
const MAX_BATCH_DELAY: Duration = Duration::from_micros(500);

/// Bounded queue depth per CF (backpressure limit)
const INGEST_QUEUE_DEPTH: usize = 4096;

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
    isolation_policy: crate::runtime::TransactionIsolationPolicy,
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

/// Response from write grouping leader after committing merged batch
pub(crate) struct BatchResponse {
    pub last_sequence: u64,
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

    fn dismiss(&mut self) {
        self.active = false;
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

/// Write submitted to the ingest coordinator's point-write queue.
pub(crate) struct IngestWrite {
    pub cf_id: crate::engine::ColumnFamilyId,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub ttl_seconds: Option<u64>,
    pub insert_only: bool,
    /// Oneshot channel to send result back to caller
    pub result_tx: crossbeam::channel::Sender<MidgeResult<u64>>,
}

impl IngestWrite {
    fn estimated_size(&self) -> usize {
        self.key.len() + self.value.as_ref().map_or(0, bytes::Bytes::len) + 64
    }

    fn to_transaction_op(&self) -> TransactionOp {
        if let Some(value) = &self.value {
            TransactionOp::Put {
                cf_id: self.cf_id,
                key: self.key.clone(),
                value: value.clone(),
                ttl_seconds: self.ttl_seconds,
                insert_only: self.insert_only,
            }
        } else {
            TransactionOp::Delete {
                cf_id: self.cf_id,
                key: self.key.clone(),
            }
        }
    }
}

/// Accumulated batch of writes
struct WriteBatch {
    intents: Vec<IngestWrite>,
    total_bytes: usize,
    first_enqueued: Instant,
}

impl WriteBatch {
    fn new() -> Self {
        Self {
            intents: Vec::new(),
            total_bytes: 0,
            first_enqueued: Instant::now(),
        }
    }

    fn add(&mut self, intent: IngestWrite) {
        self.total_bytes += intent.estimated_size();
        self.intents.push(intent);
    }

    fn is_empty(&self) -> bool {
        self.intents.is_empty()
    }

    fn len(&self) -> usize {
        self.intents.len()
    }

    fn clear(&mut self) {
        self.intents.clear();
        self.total_bytes = 0;
        self.first_enqueued = Instant::now();
    }
}

/// Per-CF ingest coordinator
pub(crate) struct IngestCoordinator {
    cf_id: crate::engine::ColumnFamilyId,
    write_tx: Sender<IngestWrite>,
    stop_tx: Sender<()>,
    thread_handle: Option<thread::JoinHandle<()>>,
    /// Cached write stall status (updated by runtime, read by ingest loop)
    /// This avoids a round-trip message to runtime on every batch commit.
    stall_flag: Arc<AtomicBool>,
    /// Write grouping coordinator for batch submissions
    write_group_coord: Arc<WriteGroupCoordinator>,
}

impl IngestCoordinator {
    /// Create and start an ingest coordinator for a column family
    pub fn new(cf_id: crate::engine::ColumnFamilyId, runtime: RuntimeHandle) -> MidgeResult<Self> {
        let (write_tx, write_rx) = bounded(INGEST_QUEUE_DEPTH);
        let (stop_tx, stop_rx) = bounded(1);
        let stall_flag = Arc::new(AtomicBool::new(false));
        let stall_flag_clone = Arc::clone(&stall_flag);

        let thread_handle = thread::Builder::new()
            .name(format!("midge-ingest-cf{cf_id}"))
            .spawn(move || {
                Self::ingest_loop(&cf_id, &runtime, &write_rx, &stop_rx, &stall_flag_clone);
            })
            .map_err(|e| {
                crate::common::MidgeError::Internal(format!(
                    "Failed to spawn ingest thread for CF {cf_id}: {e}"
                ))
            })?;

        Ok(Self {
            cf_id,
            write_tx,
            stop_tx,
            thread_handle: Some(thread_handle),
            stall_flag,
            write_group_coord: Arc::new(WriteGroupCoordinator::new()),
        })
    }

    /// Update the cached stall status (called by engine when runtime notifies)
    pub fn set_stall_status(&self, stalled: bool) {
        self.stall_flag.store(stalled, Ordering::Release);
    }

    /// Submit a point write to the ingest queue.
    ///
    /// Returns `WriteStall` if queue is full (backpressure), or the sequence number on success.
    pub fn submit_write(
        &self,
        cf_id: crate::engine::ColumnFamilyId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        ttl_seconds: Option<u64>,
        insert_only: bool,
    ) -> MidgeResult<u64> {
        let (result_tx, result_rx) = crossbeam::channel::bounded(1);
        let intent = IngestWrite {
            cf_id,
            key: Bytes::from(key),
            value: value.map(Bytes::from),
            ttl_seconds,
            insert_only,
            result_tx,
        };

        self.write_tx.try_send(intent).map_err(|e| match e {
            crossbeam::channel::TrySendError::Full(_) => MidgeError::WriteStall(format!(
                "Ingest queue full for CF {}: backpressure active",
                self.cf_id
            )),
            crossbeam::channel::TrySendError::Disconnected(_) => {
                MidgeError::Internal("Ingest coordinator stopped".to_string())
            }
        })?;

        // Wait for result from ingest loop
        result_rx
            .recv()
            .map_err(|_| MidgeError::Internal("Ingest loop died".to_string()))?
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
        isolation_policy: crate::runtime::TransactionIsolationPolicy,
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

        // Transaction commits must preserve per-transaction conflict semantics.
        // Avoid cross-request write grouping when conflict checks are requested.
        if start_sequence.is_some()
            && matches!(
                isolation_policy,
                crate::runtime::TransactionIsolationPolicy::AbortOnWriteConflict
            )
        {
            return self.submit_direct(
                runtime,
                ops,
                durability_policy,
                start_sequence,
                isolation_policy,
            );
        }

        if self.write_group_coord.try_acquire_leader() {
            self.drain_as_leader(runtime, Some(ops), durability_policy)
                .unwrap_or_else(|| {
                    Err(MidgeError::Internal(
                        "Write group leader completed with no result".to_string(),
                    ))
                })
        } else {
            self.submit_as_follower(runtime, ops, durability_policy)
        }
    }

    fn drain_as_leader(
        &self,
        runtime: &RuntimeHandle,
        initial_ops: Option<Vec<TransactionOp>>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
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

            all_ops = self.drain_pending_queue(all_ops, &mut pending_requests);

            if all_ops.is_empty() {
                break;
            }

            let batch_size = pending_requests.len() + usize::from(is_initial_batch);
            self.write_group_coord
                .batches_grouped
                .fetch_add(batch_size as u64, Ordering::Relaxed);

            let result = self.apply_transaction(runtime, all_ops, batch_durability);

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
    ) -> MidgeResult<u64> {
        Self::send_apply_transaction(
            runtime,
            ops,
            durability_policy,
            ApplyTransactionOptions {
                start_sequence: None,
                isolation_policy: crate::runtime::TransactionIsolationPolicy::LastWriteWins,
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
    ) -> MidgeResult<u64> {
        let (result_tx, result_rx) = crossbeam::channel::bounded(1);

        let pending = PendingBatchRequest {
            ops,
            durability_policy,
            result_tx,
        };

        match self.write_group_coord.pending_queue.0.try_send(pending) {
            Ok(()) => self.wait_for_grouped_result(runtime, &result_rx),
            Err(crossbeam::channel::TrySendError::Full(pending)) => self.submit_direct(
                runtime,
                pending.ops,
                pending.durability_policy,
                None,
                crate::runtime::TransactionIsolationPolicy::LastWriteWins,
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
    ) -> MidgeResult<u64> {
        let started_at = Instant::now();

        loop {
            let elapsed = started_at.elapsed();
            if elapsed >= WRITE_GROUP_WAIT_TIMEOUT {
                return Err(MidgeError::Internal(
                    "Write grouping leader timed out".to_string(),
                ));
            }

            let remaining = WRITE_GROUP_WAIT_TIMEOUT.saturating_sub(elapsed);
            let wait_for = remaining.min(WRITE_GROUP_RESCUE_INTERVAL);
            match result_rx.recv_timeout(wait_for) {
                Ok(result) => return result,
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    if self.write_group_coord.try_acquire_leader() {
                        let _ = self.drain_as_leader(runtime, None, None);
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
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
        isolation_policy: crate::runtime::TransactionIsolationPolicy,
    ) -> MidgeResult<u64> {
        Self::send_apply_transaction(
            runtime,
            ops,
            durability_policy,
            ApplyTransactionOptions {
                start_sequence,
                isolation_policy,
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

        let response = if let Some((timeout, timeout_msg)) = timeout {
            runtime
                .send_apply_transaction_and_wait_timeout(
                    request_id,
                    ops,
                    durability_policy,
                    options.start_sequence,
                    options.isolation_policy,
                    timeout,
                )?
                .ok_or_else(|| MidgeError::Internal(timeout_msg.to_string()))?
        } else {
            runtime.send_apply_transaction_and_wait(
                request_id,
                ops,
                durability_policy,
                options.start_sequence,
                options.isolation_policy,
            )?
        };

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

    /// Ingest loop: batches writes and commits them
    fn ingest_loop(
        cf_id: &crate::engine::ColumnFamilyId,
        runtime: &RuntimeHandle,
        write_rx: &Receiver<IngestWrite>,
        stop_rx: &Receiver<()>,
        stall_flag: &Arc<AtomicBool>,
    ) {
        let mut batch = WriteBatch::new();
        let mut batch_count = 0u64;
        let mut total_batch_size = 0u64;
        let mut max_batch_size = 0usize;
        let loop_start = Instant::now();

        loop {
            // When the batch is empty, block until a write arrives or shutdown is
            // signalled. This avoids the previous 100µs busy-spin that caused
            // ~10,000 wakeups/sec per CF when idle.
            let got_write = if batch.is_empty() {
                crossbeam::channel::select! {
                    recv(write_rx) -> msg => match msg {
                        Ok(intent) => {
                            batch.add(intent);
                            true
                        }
                        Err(_) => {
                            // write channel disconnected — exit
                            break;
                        }
                    },
                    recv(stop_rx) -> _ => {
                        // Shutdown: drain remaining writes
                        while let Ok(intent) = write_rx.try_recv() {
                            batch.add(intent);
                        }
                        if !batch.is_empty() {
                            Self::commit_batch(runtime, *cf_id, &mut batch, stall_flag);
                        }
                        break;
                    },
                }
            } else {
                // Batch has items — use a deadline-bounded select so we flush
                // within MAX_BATCH_DELAY even if no more writes arrive.
                let remaining = MAX_BATCH_DELAY.saturating_sub(batch.first_enqueued.elapsed());
                crossbeam::channel::select! {
                    recv(write_rx) -> msg => if let Ok(intent) = msg {
                        batch.add(intent);
                        true
                    } else {
                        // write channel disconnected — flush & exit
                        Self::commit_batch(runtime, *cf_id, &mut batch, stall_flag);
                        break;
                    },
                    recv(stop_rx) -> _ => {
                        while let Ok(intent) = write_rx.try_recv() {
                            batch.add(intent);
                        }
                        if !batch.is_empty() {
                            Self::commit_batch(runtime, *cf_id, &mut batch, stall_flag);
                        }
                        break;
                    },
                    default(remaining) => {
                        // Batch deadline expired — commit what we have
                        Self::commit_batch(runtime, *cf_id, &mut batch, stall_flag);
                        false
                    },
                }
            };

            if got_write {
                // Drain additional available writes opportunistically
                while batch.len() < MAX_BATCH_OPS && batch.total_bytes < MAX_BATCH_BYTES {
                    match write_rx.try_recv() {
                        Ok(intent) => batch.add(intent),
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }

                // Commit batch immediately after receiving write(s)
                // This ensures low latency for all commits
                if !batch.is_empty() {
                    let batch_len = batch.len();
                    let batch_bytes = batch.total_bytes;
                    let commit_start = Instant::now();
                    Self::commit_batch(runtime, *cf_id, &mut batch, stall_flag);
                    let commit_time_us =
                        u64::try_from(commit_start.elapsed().as_micros()).unwrap_or(u64::MAX);

                    let mut metrics = BatchMetrics {
                        cf_id: *cf_id,
                        batch_count: &mut batch_count,
                        total_batch_size: &mut total_batch_size,
                        max_batch_size: &mut max_batch_size,
                        loop_start: &loop_start,
                    };
                    Self::record_batch_metrics(
                        &mut metrics,
                        batch_len,
                        batch_bytes,
                        commit_time_us,
                    );
                }
            }
        }

        Self::log_summary(
            *cf_id,
            batch_count,
            total_batch_size,
            max_batch_size,
            loop_start,
        );
    }

    fn record_batch_metrics(
        metrics: &mut BatchMetrics<'_>,
        batch_len: usize,
        batch_bytes: usize,
        commit_time_us: u64,
    ) {
        *metrics.batch_count += 1;
        *metrics.total_batch_size += batch_len as u64;
        *metrics.max_batch_size = (*metrics.max_batch_size).max(batch_len);

        tracing::debug!(
            cf_id = metrics.cf_id,
            batch_len,
            batch_bytes,
            commit_time_us,
            "Committed ingest batch"
        );

        if (*metrics.batch_count).is_multiple_of(100)
            || metrics.loop_start.elapsed().as_secs().is_multiple_of(5)
        {
            let avg_size = (*metrics.total_batch_size)
                .checked_div(*metrics.batch_count)
                .unwrap_or(0);
            tracing::info!(
                cf_id = metrics.cf_id,
                batch_count = *metrics.batch_count,
                avg_batch_size = avg_size,
                max_batch_size = *metrics.max_batch_size,
                last_commit_time_us = commit_time_us,
                "Ingest batching stats"
            );
        }
    }

    fn log_summary(
        cf_id: crate::engine::ColumnFamilyId,
        batch_count: u64,
        total_batch_size: u64,
        max_batch_size: usize,
        loop_start: Instant,
    ) {
        let total_elapsed = loop_start.elapsed();
        let avg_batch_size = total_batch_size.checked_div(batch_count).unwrap_or(0);
        let batches_per_sec =
            f64::from(u32::try_from(batch_count).unwrap_or(u32::MAX)) / total_elapsed.as_secs_f64();
        tracing::info!(
            cf_id = cf_id,
            batch_count,
            total_ops = total_batch_size,
            avg_batch_size,
            max_batch_size,
            batches_per_sec,
            elapsed_secs = total_elapsed.as_secs_f64(),
            "Ingest coordinator summary"
        );

        tracing::info!(cf_id = cf_id, "Ingest coordinator stopped");
    }

    /// Commit a batch as a single transaction
    fn commit_batch(
        runtime: &RuntimeHandle,
        cf_id: crate::engine::ColumnFamilyId,
        batch: &mut WriteBatch,
        stall_flag: &AtomicBool,
    ) {
        tracing::debug!(
            cf_id = cf_id,
            batch_len = batch.intents.len(),
            batch_bytes = batch.total_bytes,
            "commit_batch started"
        );

        let ops: Vec<TransactionOp> = batch
            .intents
            .iter()
            .map(IngestWrite::to_transaction_op)
            .collect();

        // Fast path: check cached stall flag (avoids round-trip in common case)
        // The flag is updated by runtime when memtable pressure changes.
        // If stalled, do a synchronous check to confirm (flag may be stale).
        if stall_flag.load(Ordering::Acquire) {
            // Verify stall is still active via runtime
            if let Ok(true) = runtime.check_write_stall(cf_id) {
                let err_msg = format!(
                    "Memory budget exceeded for CF {cf_id}: immutable queue full or memory threshold exceeded"
                );
                for intent in batch.intents.drain(..) {
                    let _ = intent
                        .result_tx
                        .send(Err(MidgeError::WriteStall(err_msg.clone())));
                }
                batch.clear();
                return;
            }
            // Stall cleared - update flag and proceed
            stall_flag.store(false, Ordering::Release);
        }

        // Send batch as ApplyTransaction
        let result = Self::send_apply_transaction(
            runtime,
            ops,
            None,
            ApplyTransactionOptions {
                start_sequence: None,
                isolation_policy: crate::runtime::TransactionIsolationPolicy::LastWriteWins,
            },
            stall_flag,
            None,
            Some(batch.intents.len()),
        );

        // Propagate result to all waiters
        match result {
            Ok(last_seq) => {
                // Success: notify all callers with final sequence
                for intent in batch.intents.drain(..) {
                    if intent.result_tx.send(Ok(last_seq)).is_err() {
                        tracing::debug!("failed to send result to waiter (receiver dropped)");
                    }
                }
            }
            Err(e) => {
                // Failure: propagate error to all callers
                let err_msg = format!("Batch commit failed: {e:?}");
                for intent in batch.intents.drain(..) {
                    if intent
                        .result_tx
                        .send(Err(MidgeError::Internal(err_msg.clone())))
                        .is_err()
                    {
                        tracing::debug!("failed to send error to waiter (receiver dropped)");
                    }
                }
            }
        }

        batch.clear();
    }

    /// Shutdown the ingest coordinator gracefully
    pub fn shutdown(&self) {
        let _ = self.stop_tx.send(());
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

        if self.thread_handle.is_some() {
            tracing::warn!(
                cf_id = self.cf_id,
                "IngestCoordinator dropped without explicit shutdown"
            );
            let _ = self.stop_tx.send(());
        }
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
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
        let coordinator = IngestCoordinator::new(0, runtime_handle.clone())?;
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
        let initial_result = coordinator.drain_as_leader(&runtime_handle, None, None);

        // Assert
        assert!(initial_result.is_none());
        let result = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("rescue leader should notify queued follower");
        assert!(result.is_ok());

        coordinator.shutdown();
        runtime.shutdown();
        Ok(())
    }
}
