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

/// Write intent submitted to ingest coordinator
pub(crate) struct WriteIntent {
    pub cf_id: crate::engine::ColumnFamilyId,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
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

    fn should_flush(&self) -> bool {
        self.intents.len() >= MAX_BATCH_OPS
            || self.total_bytes >= MAX_BATCH_BYTES
            || self.first_enqueued.elapsed() >= MAX_BATCH_DELAY
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
    write_tx: Sender<WriteIntent>,
    stop_tx: Sender<()>,
    thread_handle: Option<thread::JoinHandle<()>>,
    /// Cached write stall status (updated by runtime, read by ingest loop)
    /// This avoids a round-trip message to runtime on every batch commit.
    stall_flag: Arc<AtomicBool>,
}

impl IngestCoordinator {
    /// Create and start an ingest coordinator for a column family
    pub fn new(cf_id: crate::engine::ColumnFamilyId, runtime: RuntimeHandle) -> Self {
        let (write_tx, write_rx) = bounded(INGEST_QUEUE_DEPTH);
        let (stop_tx, stop_rx) = bounded(1);
        let stall_flag = Arc::new(AtomicBool::new(false));
        let stall_flag_clone = Arc::clone(&stall_flag);

        let thread_handle = thread::Builder::new()
            .name(format!("midge-ingest-cf{}", cf_id))
            .spawn(move || {
                Self::ingest_loop(cf_id, runtime, write_rx, stop_rx, stall_flag_clone);
            })
            .expect("Failed to spawn ingest thread");

        Self {
            cf_id,
            write_tx,
            stop_tx,
            thread_handle: Some(thread_handle),
            stall_flag,
        }
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
            key,
            value,
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

    /// Ingest loop: batches writes and commits them
    fn ingest_loop(
        cf_id: crate::engine::ColumnFamilyId,
        runtime: RuntimeHandle,
        write_rx: Receiver<WriteIntent>,
        stop_rx: Receiver<()>,
        stall_flag: Arc<AtomicBool>,
    ) {
        let mut batch = WriteBatch::new();
        let recv_timeout = Duration::from_micros(100);

        loop {
            // Check for shutdown signal
            if stop_rx.try_recv().is_ok() {
                // Drain remaining writes
                while let Ok(intent) = write_rx.try_recv() {
                    batch.add(intent);
                }
                if !batch.is_empty() {
                    Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                }
                break;
            }

            // Receive writes with timeout
            match write_rx.recv_timeout(recv_timeout) {
                Ok(intent) => {
                    batch.add(intent);

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
                        Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    // Timeout: commit pending batch if any
                    if !batch.is_empty() && batch.first_enqueued.elapsed() >= MAX_BATCH_DELAY {
                        Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    // Channel closed: commit final batch and exit
                    if !batch.is_empty() {
                        Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
                    }
                    break;
                }
            }
        }

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

        let request_id = next_request_id();

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
            .send_and_wait(RuntimeMsg::ApplyTransaction { request_id, ops })
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
