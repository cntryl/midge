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
            let Some(batch) = self.wait_for_batch_or_pending_sync() else {
                break;
            };

            if Self::has_exhausted_write_attempts(&batch) {
                let msg =
                    "WAL write batch exceeded max retries; failing writer to preserve ordering"
                        .to_string();
                tracing::error!("{msg}");
                self.fail_writes(&batch, msg);
                return;
            }

            if !batch.is_empty() {
                let batch_len = batch.iter().map(|entry| entry.data.len()).sum::<usize>() as u64;
                let start_pos = match self.write_batch(&mut file_opt, &batch) {
                    Ok(start_pos) => start_pos,
                    Err(message) => {
                        self.fail_writes(&batch, message);
                        return;
                    }
                };
                self.complete_flushes_after_write(start_pos, batch_len, &batch);
                self.recycle_batch(batch);
            }

            self.complete_pending_flushes();
            if self.complete_pending_fsyncs(&mut file_opt).is_err() {
                return;
            }
        }
    }

    fn wait_for_batch_or_pending_sync(&self) -> Option<Vec<QueuedWrite>> {
        let mut q = self.config.queue.lock();
        while q.is_empty()
            && !self
                .config
                .shutdown
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            if self.has_pending_sync_or_flush() {
                break;
            }

            self.config
                .queue_cond
                .wait_for(&mut q, std::time::Duration::from_millis(500));
        }

        if self.should_exit_on_shutdown(&q) {
            return None;
        }

        Some(std::mem::take(&mut *q))
    }

    fn has_pending_sync_or_flush(&self) -> bool {
        let s = self.config.sync_state.lock();
        (s.pending_fsyncs > s.completed_fsyncs) || (s.pending_flushes > s.completed_flushes)
    }

    fn should_exit_on_shutdown(
        &self,
        queue: &parking_lot::MutexGuard<'_, Vec<QueuedWrite>>,
    ) -> bool {
        self.config
            .shutdown
            .load(std::sync::atomic::Ordering::SeqCst)
            && queue.is_empty()
            && !self.has_pending_sync_or_flush()
    }

    fn has_exhausted_write_attempts(batch: &[QueuedWrite]) -> bool {
        batch
            .iter()
            .any(|entry| entry.attempts >= MAX_WRITE_ATTEMPTS)
    }

    fn write_batch<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
        batch: &[QueuedWrite],
    ) -> Result<u64, String> {
        let big_bytes = bytes::Bytes::from(Self::coalesce_batch(batch));
        let write_start = Instant::now();
        let start_pos = self.append_with_retry(file_opt, big_bytes.clone())?;
        let write_elapsed = write_start.elapsed();

        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_wal_write_syscall(
                u64::try_from(write_elapsed.as_nanos()).unwrap_or(u64::MAX),
            );
        }

        self.config.current_pos.store(
            start_pos.saturating_add(big_bytes.len() as u64),
            std::sync::atomic::Ordering::SeqCst,
        );
        Ok(start_pos)
    }

    fn coalesce_batch(batch: &[QueuedWrite]) -> Vec<u8> {
        let total: usize = batch.iter().map(|entry| entry.data.len()).sum();
        let mut big = Vec::with_capacity(total);
        for entry in batch {
            big.extend_from_slice(&entry.data);
        }
        big
    }

    fn append_with_retry<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
        big_bytes: bytes::Bytes,
    ) -> Result<u64, String> {
        self.ensure_file_handle(file_opt)?;

        if let Some(file) = file_opt.as_mut() {
            match file.append(big_bytes.clone()) {
                Ok(pos) => return Ok(pos),
                Err(e1) => {
                    tracing::warn!(error = ?e1, "wal writer append failed; reopening and retrying");
                }
            }
        }

        *file_opt = self.open_file_handle().ok();
        if let Some(file) = file_opt.as_mut() {
            return file.append(big_bytes).map_err(|e| {
                tracing::error!(error = ?e, "wal writer append failed after retry");
                format!("wal writer append failed after retry: {e}")
            });
        }

        Err("wal writer could not reopen file handle after append failure".to_string())
    }

    fn ensure_file_handle<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
    ) -> Result<(), String> {
        if file_opt.is_none() {
            *file_opt = self.open_file_handle().ok();
        }
        if file_opt.is_some() {
            Ok(())
        } else {
            Err("wal writer has no file handle".to_string())
        }
    }

    fn complete_flushes_after_write(&self, start_pos: u64, batch_len: u64, batch: &[QueuedWrite]) {
        Self::ack_batch_success(batch, start_pos);
        self.config.current_pos.store(
            start_pos.saturating_add(batch_len),
            std::sync::atomic::Ordering::SeqCst,
        );

        let mut s = self.config.sync_state.lock();
        s.completed_flushes = s.pending_flushes;
        self.config.sync_cond.notify_all();
    }

    fn complete_pending_flushes(&self) {
        if !self.has_pending_flushes() {
            return;
        }

        let mut s = self.config.sync_state.lock();
        s.completed_flushes = s.pending_flushes;
        self.config.sync_cond.notify_all();
    }

    fn has_pending_flushes(&self) -> bool {
        let s = self.config.sync_state.lock();
        s.pending_flushes > s.completed_flushes
    }

    fn complete_pending_fsyncs<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
    ) -> Result<(), ()> {
        if !self.has_pending_fsyncs() {
            return Ok(());
        }

        if file_opt.is_none() {
            *file_opt = self.open_file_handle().ok();
        }
        if let Some(file) = file_opt.as_mut() {
            let sync_start = Instant::now();
            if let Err(e) = file.sync(Durability::Durable) {
                tracing::error!(error = ?e, "WAL fsync failed - marking sync as failed");
                self.mark_sync_failure(e.to_string());
                return Err(());
            }
            let sync_elapsed = sync_start.elapsed();
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_fsync_ns(
                    u64::try_from(sync_elapsed.as_nanos()).unwrap_or(u64::MAX),
                );
                t.metrics().record_wal_fsync_count();
            }
        }

        let mut s = self.config.sync_state.lock();
        s.completed_fsyncs = s.pending_fsyncs;
        self.config.sync_cond.notify_all();
        Ok(())
    }

    fn has_pending_fsyncs(&self) -> bool {
        let s = self.config.sync_state.lock();
        s.pending_fsyncs > s.completed_fsyncs
    }

    fn mark_sync_failure(&self, message: String) {
        let mut s = self.config.sync_state.lock();
        s.sync_failed = true;
        s.last_sync_error = Some(message);
        self.config.sync_cond.notify_all();
        self.config.queue_cond.notify_all();
    }

    fn ack_batch_success(batch: &[QueuedWrite], batch_start_pos: u64) {
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
