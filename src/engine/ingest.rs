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

    /// Submit a batch of write intents as a single atomic transaction.
    ///
    /// Bypasses the per-intent ingest queue and sends directly to the runtime
    /// as an `ApplyTransaction` message. This avoids queue overflow for large
    /// batches (e.g., bulk load) and eliminates per-op channel allocation.
    pub fn submit_batch(
        &self,
        runtime: &RuntimeHandle,
        intents: Vec<BatchWriteOp>,
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

        // Convert to TransactionOps with a single Bytes conversion per key/value
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

        let request_id = next_request_id();
        let result = runtime
            .send_and_wait(RuntimeMsg::ApplyTransaction { request_id, ops })
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
            })?;

        Ok(result)
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
                    Self::commit_batch(&runtime, cf_id, &mut batch, &stall_flag);
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
