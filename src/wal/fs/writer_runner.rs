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

pub(crate) struct QueuedWrite {
    pub(crate) data: Vec<u8>,
    pub(crate) ack: std::sync::mpsc::SyncSender<crate::common::MidgeResult<u64>>,
}

impl QueuedWrite {
    pub(crate) fn new(
        data: Vec<u8>,
        ack: std::sync::mpsc::SyncSender<crate::common::MidgeResult<u64>>,
    ) -> Self {
        Self { data, ack }
    }
}

/// Maximum queue depth to prevent unbounded memory growth
/// Increased from 1000 to 5000 to handle high-concurrency workloads:
/// - Writer drains ~500k-1M items/sec, so 5000 items ≈ 5-10ms of backlog
/// - Memory overhead: 5000 × 512B average ≈ 2.5MB (acceptable)
/// - Reduces frequency of backpressure triggers while providing smooth flow control
pub(crate) const MAX_QUEUE_DEPTH: usize = 5000;
/// Safety poll for a writer that missed or received a delayed queue wake-up.
///
/// Producers notify the condition variable after every enqueue, so this is not
/// a batching window. Keep the fallback short: a stale 500 ms poll made an
/// otherwise millisecond-scale append occasionally inherit half a second of
/// latency and destabilized engine-level measurements.
const IDLE_WAKE_FALLBACK: std::time::Duration = std::time::Duration::from_millis(10);
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
            let Some(mut batch) = self.wait_for_batch_or_pending_sync() else {
                break;
            };

            if !batch.is_empty() {
                let batch_len = batch.iter().map(|entry| entry.data.len()).sum::<usize>() as u64;
                let start_pos = match self.write_batch(&mut file_opt, &mut batch) {
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

            self.config.queue_cond.wait_for(&mut q, IDLE_WAKE_FALLBACK);
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

    fn write_batch<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
        batch: &mut [QueuedWrite],
    ) -> Result<u64, String> {
        let big_bytes = bytes::Bytes::from(Self::coalesce_batch(batch));
        let write_start = Instant::now();
        let start_pos = self.append_once(file_opt, big_bytes.clone())?;
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

    fn coalesce_batch(batch: &mut [QueuedWrite]) -> Vec<u8> {
        if let [entry] = batch {
            return std::mem::take(&mut entry.data);
        }

        let total: usize = batch.iter().map(|entry| entry.data.len()).sum();
        let mut big = Vec::with_capacity(total);
        for entry in batch {
            big.extend_from_slice(&entry.data);
        }
        big
    }

    fn append_once<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
        big_bytes: bytes::Bytes,
    ) -> Result<u64, String> {
        let start_pos = self
            .config
            .current_pos
            .load(std::sync::atomic::Ordering::SeqCst);
        self.ensure_file_handle(file_opt)?;

        if let Some(file) = file_opt.as_mut() {
            let write_result =
                if crate::failpoints::is_active("midge::wal::partial_write_then_no_space") {
                    let partial_len = (big_bytes.len() / 2).max(1);
                    file.write_at(start_pos, big_bytes.slice(..partial_len))
                        .and_then(|()| {
                            Err(crate::io::FsError::Io(
                                "no space after partial positional WAL write".to_string(),
                            ))
                        })
                } else {
                    file.write_at(start_pos, big_bytes)
                };
            return match write_result {
                Ok(()) => Ok(start_pos),
                Err(write_error) => {
                    tracing::error!(error = ?write_error, start_pos, "wal writer write_at failed");
                    match file
                        .truncate(start_pos)
                        .and_then(|()| file.sync(Durability::Durable))
                    {
                        Ok(()) => Err(format!(
                            "wal writer write_at failed: {write_error}; partial WAL append rolled back to {start_pos}"
                        )),
                        Err(rollback_error) => Err(format!(
                            "wal writer write_at failed: {write_error}; partial WAL rollback to {start_pos} failed: {rollback_error}"
                        )),
                    }
                }
            };
        }

        Err("wal writer has no file handle".to_string())
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
            match self.open_file_handle() {
                Ok(file) => *file_opt = Some(file),
                Err(error) => {
                    let message =
                        format!("wal writer could not reopen file handle for fsync: {error}");
                    tracing::error!(error = ?error, "WAL fsync failed before sync - marking sync as failed");
                    self.mark_sync_failure(message);
                    return Err(());
                }
            }
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
            if entry.data.is_empty() {
                continue;
            }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::traits::{DirEntry, FsError, Metadata};
    use crate::io::{Fs, FsPath, FsResult, OpenOptions as FsOpenOptions};
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::Duration;

    struct FailingOpenFs;

    impl Fs for FailingOpenFs {
        fn open(
            &self,
            path: &FsPath,
            _opts: FsOpenOptions,
        ) -> FsResult<Box<dyn crate::io::File + '_>> {
            Err(FsError::Unavailable(format!("open failed for {}", path.0)))
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(format!(
                "remove failed for {}",
                path.0
            )))
        }

        fn exists(&self, _path: &FsPath) -> FsResult<bool> {
            Ok(false)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            Err(FsError::Unavailable(format!(
                "metadata failed for {}",
                path.0
            )))
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(format!(
                "create dir failed for {}",
                path.0
            )))
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            Err(FsError::Unavailable(format!(
                "list dir failed for {}",
                path.0
            )))
        }

        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(format!(
                "remove dir failed for {}",
                path.0
            )))
        }

        fn sync_dir(&self, path: &FsPath, _dur: Durability) -> FsResult<()> {
            Err(FsError::Unavailable(format!(
                "sync dir failed for {}",
                path.0
            )))
        }

        fn rename_atomic(&self, from: &FsPath, _to: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(format!(
                "rename failed for {}",
                from.0
            )))
        }
    }

    fn runner_with_sync_state(sync_state: Arc<Mutex<SyncState>>) -> WriterRunner {
        let fs: Arc<dyn Fs> = Arc::new(FailingOpenFs);
        WriterRunner::new(WriterConfig {
            fs,
            path: FsPath::new("wal.log"),
            queue: Arc::new(Mutex::new(Vec::new())),
            queue_cond: Arc::new(Condvar::new()),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            sync_state,
            sync_cond: Arc::new(Condvar::new()),
            current_pos: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    #[test]
    fn should_fail_pending_fsync_when_file_handle_cannot_reopen() {
        // Arrange
        let sync_state = Arc::new(Mutex::new(SyncState {
            pending_fsyncs: 1,
            ..SyncState::default()
        }));
        let runner = runner_with_sync_state(Arc::clone(&sync_state));
        let mut file_opt = None;

        // Act
        let result = runner.complete_pending_fsyncs(&mut file_opt);

        // Assert
        assert!(result.is_err());
        let state = sync_state.lock();
        assert!(state.sync_failed);
        assert_eq!(state.completed_fsyncs, 0);
        assert_eq!(state.pending_fsyncs, 1);
        assert!(state
            .last_sync_error
            .as_deref()
            .is_some_and(|error| error.contains("could not reopen file handle")));
    }

    #[test]
    fn should_poll_promptly_when_queue_notification_is_missed() {
        // Arrange
        let sync_state = Arc::new(Mutex::new(SyncState::default()));
        let runner = runner_with_sync_state(sync_state);
        let queue = Arc::clone(&runner.config.queue);
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        let waiter = std::thread::spawn(move || {
            result_tx
                .send(runner.wait_for_batch_or_pending_sync())
                .expect("send WAL writer wait result");
        });
        std::thread::sleep(Duration::from_millis(25));
        let (ack_tx, _ack_rx) = std::sync::mpsc::sync_channel(1);

        // Act: deliberately enqueue without notifying the condition variable.
        queue.lock().push(QueuedWrite::new(vec![1], ack_tx));
        let batch = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("missed notification fallback should observe queued work");
        waiter.join().expect("join WAL writer wait");

        // Assert
        assert_eq!(batch.expect("writer should observe queued work").len(), 1);
    }
}
