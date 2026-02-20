//! Internal ingest batching for write throughput optimization
//!
//! This module implements per-CF write batching INTERNALLY to increase throughput
//! for concurrent streaming and write-heavy workloads. It does NOT change any
//! public APIs or semantics.
//!
//! Design:
//! - Each column family has one ingest loop/task
//! - Concurrent writers enqueue write intents instead of committing immediately
//! - The ingest loop builds a WriteBatch and commits as a SINGLE transaction
//! - Batching policy: flush when max ops/bytes/deadline reached
//! - Backpressure: bounded queue enforces WriteStall semantics
//! - Correctness: writes are atomic, ordered per CF, errors propagate to caller

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{next_request_id, RuntimeHandle, RuntimeMsg, RuntimeResponse, TransactionOp};
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

/// Pending batch request waiting for leader to group it with others
pub(crate) struct PendingBatchRequest {
    /// The batch of operations to commit
    pub intents: Vec<BatchWriteOp>,
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
/// This mechanism reduces the rate of ApplyTransaction messages sent to the runtime
/// by merging multiple pending batch submissions from concurrent threads into a
/// single transaction. The "leader" thread drains pending requests and commits them
/// as a merged batch, reducing single-threaded event loop contention.
///
/// Adaptive timeout mechanism:
/// - High concurrency (batching many): increases timeout to collect more requests
/// - Low concurrency (batching few): decreases timeout to reduce latency
/// - Self-tunes to workload pattern
///
/// This is inspired by the write grouping pattern used in RocksDB, PebbleDB, etc.
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

/// Write intent submitted to ingest coordinator
pub(crate) struct WriteIntent {
    pub cf_id: crate::engine::ColumnFamilyId,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub ttl_seconds: Option<u64>,
    pub insert_only: bool,
    /// Oneshot channel to send result back to caller
    pub result_tx: crossbeam::channel::Sender<MidgeResult<u64>>,
}

impl WriteIntent {
    fn estimated_size(&self) -> usize {
        self.key.len() + self.value.as_ref().map(|v| v.len()).unwrap_or(0) + 64
    }

    fn to_transaction_op(&self) -> TransactionOp {
        if self.value.is_some() {
            TransactionOp::Put {
                cf_id: self.cf_id,
                key: self.key.clone(),
                value: self.value.clone().expect("value is_some checked above"),
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
    intents: Vec<WriteIntent>,
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

    fn add(&mut self, intent: WriteIntent) {
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

/// A single write op for batch submission to the ingest coordinator.
pub(crate) struct BatchWriteOp {
    pub cf_id: crate::engine::ColumnFamilyId,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub ttl_seconds: Option<u64>,
    pub insert_only: bool,
}

/// Per-CF ingest coordinator
pub(crate) struct IngestCoordinator {
    cf_id: crate::engine::ColumnFamilyId,
    write_tx: Sender<WriteIntent>,
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
            .name(format!("midge-ingest-cf{}", cf_id))
            .spawn(move || {
                Self::ingest_loop(cf_id, runtime, write_rx, stop_rx, stall_flag_clone);
            })
            .map_err(|e| {
                crate::common::MidgeError::Internal(format!(
                    "Failed to spawn ingest thread for CF {}: {}",
                    cf_id, e
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

    /// Submit a write intent to the ingest queue
    ///
    /// Returns WriteStall if queue is full (backpressure), or the sequence number on success.
    pub fn submit_write(
        &self,
        cf_id: crate::engine::ColumnFamilyId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        ttl_seconds: Option<u64>,
        insert_only: bool,
    ) -> MidgeResult<u64> {
        let (result_tx, result_rx) = crossbeam::channel::bounded(1);
        let intent = WriteIntent {
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
    /// multiple concurrent batch submissions into a single ApplyTransaction.
    ///
    /// The key idea (from RocksDB write grouping):
    /// - First caller becomes "leader" (via atomic CAS)
    /// - Leader drains all pending requests from the queue
    /// - Leader merges all ops into a single transaction
    /// - Leader sends ONE ApplyTransaction to runtime
    /// - Leader fans-out the response to all waiters
    /// - Other callers wait for the leader's response
    ///
    /// The `durability_policy` parameter allows per-request durability control.
    /// If None, the runtime will use the engine's default durability policy.
    pub fn submit_batch(
        &self,
        runtime: &RuntimeHandle,
        intents: Vec<BatchWriteOp>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
    ) -> MidgeResult<u64> {
        if intents.is_empty() {
            return Ok(0);
        }

        // Fast path: check cached stall flag
        if self.stall_flag.load(Ordering::Acquire) {
            if let Ok(true) = runtime.check_write_stall(self.cf_id) {
                return Err(MidgeError::WriteStall(format!(
                    "Memory budget exceeded for CF {}",
                    self.cf_id
                )));
            }
            self.stall_flag.store(false, Ordering::Release);
        }

        // Try to acquire leader status
        if self.write_group_coord.try_acquire_leader() {
            let mut leader_guard = LeaderGuard::new(Arc::clone(&self.write_group_coord));

            // We are the leader: merge this batch with all pending requests
            self.write_group_coord
                .leader_runs
                .fetch_add(1, Ordering::Relaxed);

            let mut initial_intents = Some(intents);
            let mut initial_result: Option<MidgeResult<u64>> = None;

            loop {
                let mut pending_requests = Vec::new();

                let (mut all_intents, batch_durability, is_initial_batch) = if let Some(initial) =
                    initial_intents.take()
                {
                    (initial, durability_policy, true)
                } else {
                    match self.write_group_coord.pending_queue.1.try_recv() {
                        Ok(pending) => {
                            pending_requests.push((pending.result_tx, pending.durability_policy));
                            (pending.intents, pending.durability_policy, false)
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break,
                    }
                };

                if all_intents.is_empty() {
                    break;
                }

                // Drain additional pending requests from the queue
                let drain_start = Instant::now();
                let adaptive_timeout = self.write_group_coord.get_timeout();
                loop {
                    match self.write_group_coord.pending_queue.1.try_recv() {
                        Ok(pending) => {
                            all_intents.extend(pending.intents);
                            pending_requests.push((pending.result_tx, pending.durability_policy));
                        }
                        Err(TryRecvError::Empty) => {
                            if drain_start.elapsed() > adaptive_timeout
                                || pending_requests.len() >= MAX_GROUPED_BATCHES
                                || all_intents.len() > MAX_BATCH_OPS
                            {
                                break;
                            }
                            std::thread::yield_now();
                        }
                        Err(TryRecvError::Disconnected) => break,
                    }
                }

                self.write_group_coord
                    .batches_grouped
                    .fetch_add(pending_requests.len() as u64 + 1, Ordering::Relaxed);

                // Track batch size for adaptive timeout adjustment
                // batch_size = initial batch (1) + pending_requests drained
                let batch_size = pending_requests.len() + 1;

                let ops: Vec<TransactionOp> = all_intents
                    .into_iter()
                    .map(|op| {
                        if let Some(value) = op.value {
                            TransactionOp::Put {
                                cf_id: op.cf_id,
                                key: Bytes::from(op.key),
                                value: Bytes::from(value),
                                ttl_seconds: op.ttl_seconds,
                                insert_only: op.insert_only,
                            }
                        } else {
                            TransactionOp::Delete {
                                cf_id: op.cf_id,
                                key: Bytes::from(op.key),
                            }
                        }
                    })
                    .collect();

                let result = match next_request_id() {
                    Ok(request_id) => runtime
                        .send_and_wait_timeout(
                            RuntimeMsg::ApplyTransaction {
                                request_id,
                                ops,
                                durability_policy: batch_durability,
                            },
                            WRITE_GROUP_APPLY_TIMEOUT,
                        )
                        .and_then(|resp| match resp {
                            Some(RuntimeResponse::TransactionApplied {
                                last_sequence,
                                write_stall_hint,
                                ..
                            }) => {
                                self.stall_flag.store(write_stall_hint, Ordering::Release);
                                Ok(last_sequence)
                            }
                            Some(RuntimeResponse::Error { error, .. }) => Err(error),
                            Some(_) => Err(MidgeError::Internal(
                                "Unexpected response to ApplyTransaction".to_string(),
                            )),
                            None => Err(MidgeError::Internal(
                                "Write group commit timed out".to_string(),
                            )),
                        }),
                    Err(err) => Err(err),
                };

                let error_msg = match &result {
                    Ok(_) => None,
                    Err(e) => Some(format!("Write group commit failed: {:?}", e)),
                };

                for (waiter_tx, _) in pending_requests {
                    match &result {
                        Ok(seq) => {
                            let _ = waiter_tx.send(Ok(*seq));
                        }
                        Err(_) => {
                            let _ = waiter_tx.send(Err(MidgeError::Internal(
                                error_msg.clone().unwrap_or_default(),
                            )));
                        }
                    }
                }

                if is_initial_batch {
                    initial_result = Some(result);
                }

                // Adapt timeout based on batching effectiveness
                // Higher batch_size → increase timeout (high concurrency)
                // Lower batch_size → decrease timeout (low concurrency)
                self.write_group_coord.adjust_timeout(batch_size);
            }

            leader_guard.dismiss();
            self.write_group_coord.release_leader();

            initial_result.unwrap_or_else(|| {
                Err(MidgeError::Internal(
                    "Write group leader completed with no result".to_string(),
                ))
            })
        } else {
            // We are not the leader: try to enqueue request and wait for leader's response
            // If queue is full, submit directly to runtime

            let (result_tx, result_rx) = crossbeam::channel::bounded(1);

            let pending = PendingBatchRequest {
                intents,
                durability_policy,
                result_tx,
            };

            // Try to enqueue request to leader
            match self.write_group_coord.pending_queue.0.try_send(pending) {
                Ok(()) => {
                    // Wait for leader to process and return response
                    result_rx
                        .recv_timeout(WRITE_GROUP_WAIT_TIMEOUT)
                        .map_err(|_| {
                            MidgeError::Internal("Write grouping leader timed out".to_string())
                        })
                        .and_then(|result| result)
                }
                Err(crossbeam::channel::TrySendError::Full(pending)) => {
                    // Queue full - just submit directly (bypass write grouping for this batch)
                    let intents = pending.intents;
                    let ops: Vec<TransactionOp> = intents
                        .into_iter()
                        .map(|op| {
                            if let Some(value) = op.value {
                                TransactionOp::Put {
                                    cf_id: op.cf_id,
                                    key: Bytes::from(op.key),
                                    value: Bytes::from(value),
                                    ttl_seconds: op.ttl_seconds,
                                    insert_only: op.insert_only,
                                }
                            } else {
                                TransactionOp::Delete {
                                    cf_id: op.cf_id,
                                    key: Bytes::from(op.key),
                                }
                            }
                        })
                        .collect();

                    let request_id = next_request_id()?;
                    runtime
                        .send_and_wait(RuntimeMsg::ApplyTransaction {
                            request_id,
                            ops,
                            durability_policy,
                        })
                        .and_then(|resp| match resp {
                            RuntimeResponse::TransactionApplied {
                                last_sequence,
                                write_stall_hint,
                                ..
                            } => {
                                self.stall_flag.store(write_stall_hint, Ordering::Release);
                                Ok(last_sequence)
                            }
                            RuntimeResponse::Error { error, .. } => Err(error),
                            _ => Err(MidgeError::Internal(
                                "Unexpected response to ApplyTransaction".to_string(),
                            )),
                        })
                }
                Err(crossbeam::channel::TrySendError::Disconnected(_)) => Err(
                    MidgeError::Internal("Write grouping coordinator disconnected".to_string()),
                ),
            }
        }
    }

    /// Ingest loop: batches writes and commits them
    fn ingest_loop(
        cf_id: crate::engine::ColumnFamilyId,
        runtime: RuntimeHandle,
        write_rx: Receiver<WriteIntent>,
        stop_rx: Receiver<()>,
        stall_flag: Arc<AtomicBool>,
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
                            Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                        }
                        break;
                    },
                }
            } else {
                // Batch has items — use a deadline-bounded select so we flush
                // within MAX_BATCH_DELAY even if no more writes arrive.
                let remaining = MAX_BATCH_DELAY.saturating_sub(batch.first_enqueued.elapsed());
                crossbeam::channel::select! {
                    recv(write_rx) -> msg => match msg {
                        Ok(intent) => {
                            batch.add(intent);
                            true
                        }
                        Err(_) => {
                            // write channel disconnected — flush & exit
                            Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                            break;
                        }
                    },
                    recv(stop_rx) -> _ => {
                        while let Ok(intent) = write_rx.try_recv() {
                            batch.add(intent);
                        }
                        if !batch.is_empty() {
                            Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                        }
                        break;
                    },
                    default(remaining) => {
                        // Batch deadline expired — commit what we have
                        Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                        false
                    },
                }
            };

            if got_write {
                // Drain additional available writes opportunistically
                while batch.len() < MAX_BATCH_OPS && batch.total_bytes < MAX_BATCH_BYTES {
                    match write_rx.try_recv() {
                        Ok(intent) => batch.add(intent),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break,
                    }
                }

                tracing::debug!(cf_id = cf_id, batch_size = batch.len(), "Committing batch");

                // Commit batch immediately after receiving write(s)
                // This ensures low latency for all commits
                if !batch.is_empty() {
                    let commit_start = Instant::now();
                    Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                    let commit_time_us = commit_start.elapsed().as_micros() as u64;

                    // Track batch metrics
                    batch_count += 1;
                    let _batch_size = batch.intents.len() + batch.intents.capacity(); // rough estimate
                    total_batch_size += batch.len() as u64;
                    max_batch_size = max_batch_size.max(batch.len());

                    // Log periodic metrics every 100 batches or 5 seconds
                    if batch_count.is_multiple_of(100)
                        || loop_start.elapsed().as_secs().is_multiple_of(5)
                    {
                        let avg_size = if batch_count > 0 {
                            total_batch_size / batch_count
                        } else {
                            0
                        };
                        eprintln!(
                            "[ingest-cf{}] batches={} avg_size={} max_size={} commit_time={}µs",
                            cf_id, batch_count, avg_size, max_batch_size, commit_time_us
                        );
                    }
                }
            }
        }

        let total_elapsed = loop_start.elapsed();
        let avg_batch_size = if batch_count > 0 {
            total_batch_size / batch_count
        } else {
            0
        };
        let batches_per_sec = batch_count as f64 / total_elapsed.as_secs_f64();
        eprintln!("[ingest-cf{}] FINAL: batches={} total_ops={} avg_batch_size={} max_batch_size={} batches/sec={:.1} elapsed={:.2}s",
            cf_id, batch_count, total_batch_size, avg_batch_size, max_batch_size, batches_per_sec, total_elapsed.as_secs_f64());

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
            .map(|i| i.to_transaction_op())
            .collect();

        let request_id = next_request_id().expect("request ID in commit_batch");

        // Fast path: check cached stall flag (avoids round-trip in common case)
        // The flag is updated by runtime when memtable pressure changes.
        // If stalled, do a synchronous check to confirm (flag may be stale).
        if stall_flag.load(Ordering::Acquire) {
            // Verify stall is still active via runtime
            if let Ok(true) = runtime.check_write_stall(cf_id) {
                let err_msg = format!(
                    "Memory budget exceeded for CF {}: immutable queue full or memory threshold exceeded",
                    cf_id
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
        let result = runtime
            .send_and_wait(RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy: None, // Use engine's default durability policy
            })
            .and_then(|resp| match resp {
                RuntimeResponse::TransactionApplied {
                    last_sequence,
                    op_count,
                    write_stall_hint,
                    ..
                } => {
                    // Update stall flag from response (piggyback pattern)
                    stall_flag.store(write_stall_hint, Ordering::Release);

                    if op_count != batch.intents.len() {
                        Err(MidgeError::Internal(format!(
                            "Batch op count mismatch: expected {}, got {}",
                            batch.intents.len(),
                            op_count
                        )))
                    } else {
                        Ok(last_sequence)
                    }
                }
                RuntimeResponse::Error { error, .. } => Err(error),
                _ => Err(MidgeError::Internal(
                    "Unexpected response to ApplyTransaction".to_string(),
                )),
            });

        // Propagate result to all waiters
        match result {
            Ok(last_seq) => {
                // Success: notify all callers with final sequence
                for intent in batch.intents.drain(..) {
                    let _ = intent.result_tx.send(Ok(last_seq));
                }
            }
            Err(e) => {
                // Failure: propagate error to all callers
                let err_msg = format!("Batch commit failed: {:?}", e);
                for intent in batch.intents.drain(..) {
                    let _ = intent
                        .result_tx
                        .send(Err(MidgeError::Internal(err_msg.clone())));
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
        // Print write grouping stats
        let (leader_runs, batches_grouped, final_timeout_us) = self.write_group_coord.metrics();
        if leader_runs > 0 {
            let avg_group_size = if leader_runs > 0 {
                batches_grouped as f64 / leader_runs as f64
            } else {
                0.0
            };
            eprintln!(
                "[write-group-cf{}] leader_runs={} batches_grouped={} avg_group_size={:.2} final_timeout={}µs",
                self.cf_id, leader_runs, batches_grouped, avg_group_size, final_timeout_us
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
