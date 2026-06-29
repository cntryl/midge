use crate::io::{Durability, Fs, FsPath, FsResult};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct SyncState {
    pub pending_flushes: u64,
    pub pending_fsyncs: u64,
    pub completed_flushes: u64,
    pub completed_fsyncs: u64,
    /// Set to true when persistent write failures occur
    pub write_failed: bool,
    /// Set to true when persistent fsync failures occur
    pub sync_failed: bool,
    pub last_write_error: Option<String>,
    pub last_sync_error: Option<String>,
}

/// Queue entry with retry tracking to prevent unbounded re-queuing
pub(crate) struct QueuedWrite {
    pub(crate) data: Vec<u8>,
    pub(crate) attempts: u8,
    pub(crate) ack: std::sync::mpsc::SyncSender<crate::common::MidgeResult<u64>>,
}

impl QueuedWrite {
    pub(crate) fn new(
        data: Vec<u8>,
        ack: std::sync::mpsc::SyncSender<crate::common::MidgeResult<u64>>,
    ) -> Self {
        Self {
            data,
            attempts: 0,
            ack,
        }
    }
}

/// Maximum number of retry attempts before dropping a write
pub(crate) const MAX_WRITE_ATTEMPTS: u8 = 3;
/// Maximum queue depth to prevent unbounded memory growth
/// Increased from 1000 to 5000 to handle high-concurrency workloads:
/// - Writer drains ~500k-1M items/sec, so 5000 items ≈ 5-10ms of backlog
/// - Memory overhead: 5000 × 512B average ≈ 2.5MB (acceptable)
/// - Reduces frequency of backpressure triggers while providing smooth flow control
pub(crate) const MAX_QUEUE_DEPTH: usize = 5000;
/// Maximum buffer pool size to prevent unbounded memory growth
/// When pool reaches this size, excess buffers are dropped instead of pooled
/// Sized to accommodate sustained high-concurrency writes:
/// - 64 buffers × ~1KB average ≈ 64KB pool overhead (minimal)
/// - Provides ~1.3% of `MAX_QUEUE_DEPTH` as pooled buffers
/// - Balances memory usage with allocation avoidance for most workloads
pub(crate) const MAX_BUFFER_POOL_SIZE: usize = 64;

/// Configuration struct to reduce constructor arguments
pub struct WriterConfig {
    pub fs: Arc<dyn Fs>,
    pub path: FsPath,
    pub queue: Arc<Mutex<Vec<QueuedWrite>>>,
    pub queue_cond: Arc<Condvar>,
    pub buf_pool: Arc<Mutex<Vec<Vec<u8>>>>,
    pub sync_state: Arc<Mutex<SyncState>>,
    pub sync_cond: Arc<Condvar>,
    pub current_pos: Arc<std::sync::atomic::AtomicU64>,
    pub shutdown: Arc<std::sync::atomic::AtomicBool>,
}

pub struct WriterRunner {
    config: WriterConfig,
}

impl WriterRunner {
    pub fn new(config: WriterConfig) -> Self {
        Self { config }
    }

    fn open_file_handle(&self) -> FsResult<Box<dyn crate::io::File + '_>> {
        // Prefer persistent handle if available
        match self.config.fs.open_persistent_handle(
            &self.config.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        ) {
            Ok(f) => Ok(f),
            Err(_) => self.config.fs.open(
                &self.config.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            ),
        }
    }

    pub fn run(self) {
        // Attempt to open persistent handle once and reuse it where possible
        let mut file_opt = self.open_file_handle().ok();

        loop {
            // Wait for work (queue data, sync request, or shutdown).
            // Lock order: always queue then sync_state (briefly); never the reverse.
            let batch: Vec<QueuedWrite>;
            {
                let mut q = self.config.queue.lock();
                // Wait until: queue has data OR a sync/flush is requested OR shutdown requested.
                //
                // IMPORTANT: `WalWriter::sync()` must work even when the queue is empty. The writer
                // thread performs I/O asynchronously, so an fsync request may arrive with no queued
                // buffers. If we only wake on queued data, callers can deadlock forever.
                while q.is_empty()
                    && !self
                        .config
                        .shutdown
                        .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let has_pending_sync = {
                        let s = self.config.sync_state.lock();
                        (s.pending_fsyncs > s.completed_fsyncs)
                            || (s.pending_flushes > s.completed_flushes)
                    };

                    if has_pending_sync {
                        break;
                    }

                    // Safety-net periodic wake to re-check shutdown/sync flags.
                    // All enqueue and sync-request paths notify the condvar, so this
                    // timeout only guards against missed notifications (unlikely).
                    // 500ms keeps idle CPU near zero while bounding worst-case latency.
                    self.config
                        .queue_cond
                        .wait_for(&mut q, std::time::Duration::from_millis(500));
                }

                // Check for shutdown (but still allow pending fsync/flush requests to complete)
                if self
                    .config
                    .shutdown
                    .load(std::sync::atomic::Ordering::SeqCst)
                    && q.is_empty()
                {
                    let has_pending_sync = {
                        let s = self.config.sync_state.lock();
                        (s.pending_fsyncs > s.completed_fsyncs)
                            || (s.pending_flushes > s.completed_flushes)
                    };
                    if !has_pending_sync {
                        break;
                    }
                }

                // Drain queue
                batch = std::mem::take(&mut *q);
            }

            // If any entry has exceeded max retry attempts, fail the writer and notify waiters
            // instead of silently dropping data (which would cause recovery to miss it).
            if batch
                .iter()
                .any(|entry| entry.attempts >= MAX_WRITE_ATTEMPTS)
            {
                let msg =
                    "WAL write batch exceeded max retries; failing writer to preserve ordering"
                        .to_string();
                tracing::error!("{msg}");
                self.fail_writes(&batch, msg);
                return;
            }

            // Process any queued data
            if !batch.is_empty() {
                // Coalesce
                let total: usize = batch.iter().map(|entry| entry.data.len()).sum();
                let mut big = Vec::with_capacity(total);
                for entry in &batch {
                    big.extend_from_slice(&entry.data);
                }

                // Write
                let big_bytes = bytes::Bytes::from(big);
                let write_start = Instant::now();
                let write_result: Option<u64>;

                // Ensure a handle exists if possible
                if file_opt.is_none() {
                    file_opt = self.open_file_handle().ok();
                }

                // Attempt append; if it fails, reopen and retry once.
                if let Some(ref mut file) = file_opt {
                    match file.append(big_bytes.clone()) {
                        Ok(pos) => write_result = Some(pos),
                        Err(e1) => {
                            tracing::warn!(error = ?e1, "wal writer append failed; reopening and retrying");
                            file_opt = self.open_file_handle().ok();
                            if let Some(ref mut file2) = file_opt {
                                match file2.append(big_bytes.clone()) {
                                    Ok(pos) => write_result = Some(pos),
                                    Err(e2) => {
                                        tracing::error!(error = ?e2, "wal writer append failed after retry");
                                        self.fail_writes(
                                            &batch,
                                            format!("wal writer append failed after retry: {e2}"),
                                        );
                                        return;
                                    }
                                }
                            } else {
                                let msg =
                                    "wal writer could not reopen file handle after append failure"
                                        .to_string();
                                tracing::error!("{msg}");
                                self.fail_writes(&batch, msg);
                                return;
                            }
                        }
                    }
                } else {
                    let msg = "wal writer has no file handle".to_string();
                    tracing::error!("{msg}");
                    self.fail_writes(&batch, msg);
                    return;
                }

                if let Some(start_pos) = write_result {
                    let write_elapsed = write_start.elapsed();
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics()
                            .record_wal_write_syscall(write_elapsed.as_nanos() as u64);
                    }
                    // `append()` returns the starting offset; expose end offset as "current_pos".
                    let end_pos = start_pos.saturating_add(big_bytes.len() as u64);
                    self.config
                        .current_pos
                        .store(end_pos, std::sync::atomic::Ordering::SeqCst);
                    self.ack_batch_success(&batch, start_pos);

                    // Mark completed flushes (barrier for "writes before flush() have been written")
                    {
                        let mut s = self.config.sync_state.lock();
                        s.completed_flushes = s.pending_flushes;
                        self.config.sync_cond.notify_all();
                    }
                }

                self.recycle_batch(batch);
            }

            // If a flush was requested but there was no data batch, still complete it.
            // This makes `WalWriter::flush()` a reliable barrier even when the queue was
            // empty at the time of the call.
            let need_flush = {
                let s = self.config.sync_state.lock();
                s.pending_flushes > s.completed_flushes
            };
            if need_flush {
                let mut s = self.config.sync_state.lock();
                s.completed_flushes = s.pending_flushes;
                self.config.sync_cond.notify_all();
            }

            // Handle pending fsyncs
            let need_sync = {
                let s = self.config.sync_state.lock();
                s.pending_fsyncs > s.completed_fsyncs
            };

            if need_sync {
                if file_opt.is_none() {
                    file_opt = self.open_file_handle().ok();
                }
                if let Some(ref mut file) = file_opt {
                    let sync_start = Instant::now();
                    let sync_result = file.sync(Durability::Durable);
                    if let Err(e) = sync_result {
                        tracing::error!(error = ?e, "WAL fsync failed - marking sync as failed");
                        let mut s = self.config.sync_state.lock();
                        s.sync_failed = true;
                        s.last_sync_error = Some(e.to_string());
                        // Do NOT increment completed_fsyncs - leave waiters to check failure
                        self.config.sync_cond.notify_all();
                        self.config.queue_cond.notify_all();
                        return; // Exit run loop - writer thread terminates on fsync failure
                    }
                    let sync_elapsed = sync_start.elapsed();
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics()
                            .record_wal_fsync_ns(sync_elapsed.as_nanos() as u64);
                        t.metrics().record_wal_fsync_count();
                    }
                }

                let mut s = self.config.sync_state.lock();
                s.completed_fsyncs = s.pending_fsyncs;
                self.config.sync_cond.notify_all();
            }
        }
    }

    fn ack_batch_success(&self, batch: &[QueuedWrite], batch_start_pos: u64) {
        let mut next_pos = batch_start_pos;
        for entry in batch {
            let entry_pos = next_pos;
            next_pos = next_pos.saturating_add(entry.data.len() as u64);
            let _ = entry.ack.send(Ok(entry_pos));
        }
    }

    fn recycle_batch(&self, batch: Vec<QueuedWrite>) {
        let mut pool = self.config.buf_pool.lock();
        let mut dropped = 0usize;
        for entry in batch {
            if pool.len() < MAX_BUFFER_POOL_SIZE {
                pool.push(entry.data);
            } else {
                dropped += 1;
            }
        }
        drop(pool);

        if dropped > 0 {
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_buffer_pool_overflow(dropped as u64);
            }
        }

        self.config.queue_cond.notify_all();
    }

    fn fail_writes(&self, batch: &[QueuedWrite], message: String) {
        for entry in batch {
            let _ = entry.ack.send(Err(Self::error_from_message(&message)));
        }

        let mut s = self.config.sync_state.lock();
        s.write_failed = true;
        s.last_write_error = Some(message);
        self.config.sync_cond.notify_all();
        self.config.queue_cond.notify_all();
    }

    fn error_from_message(message: &str) -> crate::common::MidgeError {
        let lowered = message.to_ascii_lowercase();
        if lowered.contains("no space") || lowered.contains("disk full") {
            crate::common::MidgeError::NoSpace(message.to_string())
        } else {
            crate::common::MidgeError::Internal(message.to_string())
        }
    }
}
