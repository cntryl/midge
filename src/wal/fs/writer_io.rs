//! Filesystem WAL writer using `io::Fs` abstraction
//!
//! This writer uses the base `io::Fs` trait instead of storage abstractions directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.
//!
//! Architectural rules (Maintainer: read carefully and DO NOT modify):
//! ---------------------------------------------------------------
//! • `FsWalWriterIo` ONLY appends bytes to the active WAL file `wal.log`.
//! • It NEVER assigns sequence numbers.
//! • It NEVER rotates WAL segments.
//! • It NEVER writes metadata beyond the encoded WAL record format.
//! • It MUST write records as: `<u32 length prefix><u32 crc32c><encoded record bytes>`.
//! • It MUST update the write position monotonically.
//! • It MUST flush/sync exactly and only when asked.
//! • All concurrency protection is via `Mutex` — do NOT add async constructs.

use crate::common::MidgeResult;
use crate::io::{Fs, FsPath};
use crate::wal::encoding;
use crate::wal::traits::WalWriter;
use crate::wal::types::{WalPos, WalRecord};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::Duration;

use super::writer_runner::{SyncState, WriterConfig, WriterRunner};

const MAX_BACKOFF_MS: u64 = 100;
const MAX_WAIT_ATTEMPTS: u32 = 50;
const JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const WAL_IO_TIMEOUT: Duration = Duration::from_secs(30);

/// Filesystem-backed WAL writer using `io::Fs`.
///
/// This struct is responsible ONLY for writing bytes to `wal.log`.
/// It does not manage segment rotation, sequence assignment, recovery,
/// or any other higher-level concerns. Those belong to the WAL actor.
pub struct FsWalWriterIo {
    /// Reserved for segment rotation.
    fs: Arc<dyn Fs>,

    // Queue of pending encoded record payloads with retry tracking
    queue: Arc<Mutex<Vec<super::writer_runner::QueuedWrite>>>,
    queue_cond: Arc<Condvar>,

    // Pool of reusable small buffers to avoid per-put allocations
    buf_pool: Arc<Mutex<Vec<Vec<u8>>>>,

    // Sync/flush request state and condvar for synchronous sync() and flush() calls
    sync_state: Arc<Mutex<SyncState>>,
    sync_cond: Arc<Condvar>,

    // Writer thread handle
    writer_thread: Mutex<Option<std::thread::JoinHandle<()>>>,

    // Shutdown flag
    shutdown: Arc<std::sync::atomic::AtomicBool>,

    // Current position shared with runner
    current_pos: Arc<std::sync::atomic::AtomicU64>,
}
impl FsWalWriterIo {
    /// Create a new WAL writer targeting `wal.log` using the provided filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be created or opened.
    pub fn new(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let path = FsPath::new(path_str);

        // Verify file exists or can be created by checking metadata
        {
            let _ = fs.open(
                &path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: false,
                },
            )?;
        }

        // Get current file size
        let metadata = fs.metadata(&path)?;
        let current_pos = metadata.len;

        let writer = Self {
            fs,
            current_pos: Arc::new(std::sync::atomic::AtomicU64::new(current_pos)),
            queue: Arc::new(Mutex::new(Vec::new())),
            queue_cond: Arc::new(Condvar::new()),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            sync_state: Arc::new(Mutex::new(SyncState::default())),
            sync_cond: Arc::new(Condvar::new()),
            writer_thread: Mutex::new(None),
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        // Spawn background writer thread
        let config = WriterConfig {
            fs: Arc::clone(&writer.fs),
            path,
            queue: writer.queue.clone(),
            queue_cond: writer.queue_cond.clone(),
            buf_pool: writer.buf_pool.clone(),
            sync_state: writer.sync_state.clone(),
            sync_cond: writer.sync_cond.clone(),
            current_pos: writer.current_pos.clone(),
            shutdown: writer.shutdown.clone(),
        };
        let runner = WriterRunner::new(config);
        let handle = std::thread::Builder::new()
            .name("midge-wal-writer".to_string())
            .spawn(move || runner.run())?;

        *writer.writer_thread.lock() = Some(handle);

        Ok(writer)
    }

    fn encode_record_frame(record: &WalRecord, buf: &mut Vec<u8>) -> MidgeResult<()> {
        let e_start = std::time::Instant::now();
        crate::wal::frame::append_frame_encoded(buf, |payload| {
            encoding::encode_into(record, payload)
        })?;
        let e_elapsed = e_start.elapsed();
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics()
                .record_wal_encode(u64::try_from(e_elapsed.as_nanos()).unwrap_or(u64::MAX));
        }
        Ok(())
    }

    fn take_buffer(&self) -> Vec<u8> {
        let mut pool = self.buf_pool.lock();
        pool.pop().unwrap_or_else(|| Vec::with_capacity(1024))
    }

    fn writer_failure(&self) -> Option<crate::common::MidgeError> {
        let s = self.sync_state.lock();
        if s.write_failed {
            let msg = s
                .last_write_error
                .clone()
                .unwrap_or_else(|| "WAL write failed persistently".to_string());
            return Some(Self::error_from_message(msg));
        }
        if s.sync_failed {
            let msg = s
                .last_sync_error
                .clone()
                .unwrap_or_else(|| "WAL sync failed persistently".to_string());
            return Some(Self::error_from_message(msg));
        }
        None
    }

    fn enqueue_encoded(&self, buf: Vec<u8>) -> MidgeResult<WalPos> {
        self.enqueue_encoded_with_timeout(buf, WAL_IO_TIMEOUT)
    }

    fn enqueue_encoded_with_timeout(
        &self,
        buf: Vec<u8>,
        acknowledgement_timeout: Duration,
    ) -> MidgeResult<WalPos> {
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);

        // Enqueue with backpressure: if queue is full, wait for it to drain.
        let mut backoff_ms = 1u64;
        for attempt in 0..MAX_WAIT_ATTEMPTS {
            if let Some(err) = self.writer_failure() {
                return Err(err);
            }

            let mut q = self.queue.lock();
            if q.len() < super::writer_runner::MAX_QUEUE_DEPTH {
                q.push(super::writer_runner::QueuedWrite::new(buf, ack_tx));
                self.queue_cond.notify_one();

                drop(q);

                if attempt > 0 {
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_wal_backpressure_wait(u64::from(attempt));
                    }
                }

                return match ack_rx.recv_timeout(acknowledgement_timeout) {
                    Ok(result) => result,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let message = format!(
                            "WAL append acknowledgement timed out after {acknowledgement_timeout:?}"
                        );
                        let mut state = self.sync_state.lock();
                        state.write_failed = true;
                        state.last_write_error = Some(message.clone());
                        drop(state);
                        self.sync_cond.notify_all();
                        self.queue_cond.notify_all();
                        Err(crate::common::MidgeError::Timeout(message))
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        Err(self.writer_failure().unwrap_or_else(|| {
                            crate::common::MidgeError::Internal(
                                "WAL writer thread exited before append acknowledgement"
                                    .to_string(),
                            )
                        }))
                    }
                };
            }

            let wait_duration = std::time::Duration::from_millis(backoff_ms);
            self.queue_cond.wait_for(&mut q, wait_duration);
            drop(q);

            backoff_ms = std::cmp::min(backoff_ms * 2, MAX_BACKOFF_MS);
        }

        if let Some(err) = self.writer_failure() {
            return Err(err);
        }

        let q = self.queue.lock();
        Err(crate::common::MidgeError::WriteStall(format!(
            "WAL queue full after {} attempts ({}/{} items, backoff exhausted)",
            MAX_WAIT_ATTEMPTS,
            q.len(),
            super::writer_runner::MAX_QUEUE_DEPTH
        )))
    }

    fn error_from_message(message: String) -> crate::common::MidgeError {
        let lowered = message.to_ascii_lowercase();
        if lowered.contains("no space") || lowered.contains("disk full") {
            crate::common::MidgeError::NoSpace(message)
        } else if lowered.contains("timed out") || lowered.contains("deadline exceeded") {
            crate::common::MidgeError::Fenced(message)
        } else {
            crate::common::MidgeError::Internal(message)
        }
    }

    fn flush_with_timeout(&self, timeout: Duration) -> MidgeResult<()> {
        if let Some(error) = self.writer_failure() {
            return Err(error);
        }
        // A queue-empty check is insufficient because the writer drains into a
        // local batch before the file append completes. Track a generation and
        // wait for that generation's completion instead.
        let my_flush_id = {
            let mut state = self.sync_state.lock();
            state.pending_flushes = state.pending_flushes.saturating_add(1);
            state.pending_flushes
        };
        self.queue_cond.notify_one();

        let started = std::time::Instant::now();
        let mut state = self.sync_state.lock();
        while state.completed_flushes < my_flush_id {
            if state.write_failed {
                let message = state
                    .last_write_error
                    .clone()
                    .unwrap_or_else(|| "WAL write failed persistently".to_string());
                return Err(Self::error_from_message(message));
            }
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                let message = format!("WAL flush timed out after {timeout:?}");
                state.write_failed = true;
                state.last_write_error = Some(message.clone());
                return Err(crate::common::MidgeError::Timeout(message));
            };
            if self.sync_cond.wait_for(&mut state, remaining).timed_out()
                && state.completed_flushes < my_flush_id
            {
                let message = format!("WAL flush timed out after {timeout:?}");
                state.write_failed = true;
                state.last_write_error = Some(message.clone());
                return Err(crate::common::MidgeError::Timeout(message));
            }
        }

        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_wal_flush();
        }
        Ok(())
    }
}

impl WalWriter for FsWalWriterIo {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Encode and enqueue payload into queue. The method only returns after the
        // writer thread has appended the bytes to the local WAL file.
        let mut buf = self.take_buffer();
        buf.clear();
        Self::encode_record_frame(record, &mut buf)?;
        self.enqueue_encoded(buf)
    }

    fn append_batch(&self, records: &[WalRecord]) -> MidgeResult<WalPos> {
        if records.is_empty() {
            return Ok(self.current_pos());
        }

        let mut buf = self.take_buffer();
        buf.clear();

        let mut last_record_offset = 0u64;
        for (index, record) in records.iter().enumerate() {
            if index + 1 == records.len() {
                last_record_offset = buf.len() as u64;
            }
            Self::encode_record_frame(record, &mut buf)?;
        }

        let batch_start = self.enqueue_encoded(buf)?;
        Ok(batch_start.saturating_add(last_record_offset))
    }

    fn flush(&self) -> MidgeResult<()> {
        self.flush_with_timeout(WAL_IO_TIMEOUT)
    }

    fn sync(&self) -> MidgeResult<()> {
        self.sync_with_timeout(Duration::from_secs(30))
    }

    fn sync_with_timeout(&self, timeout: Duration) -> MidgeResult<()> {
        if let Some(error) = self.writer_failure() {
            return Err(error);
        }
        // Request a durable fsync and wait for writer to perform it.
        // Note: we MUST request fsync even if queue is empty, because data may have
        // been written to the file but not yet fsynced (the writer thread writes
        // asynchronously).
        let sync_start = std::time::Instant::now();
        let my_sync_id = {
            let mut s = self.sync_state.lock();
            s.pending_fsyncs = s.pending_fsyncs.saturating_add(1);
            s.pending_fsyncs
        };
        // Wake writer so it can perform fsync
        self.queue_cond.notify_one();

        let mut s = self.sync_state.lock();
        while s.completed_fsyncs < my_sync_id {
            if s.sync_failed {
                let msg = s
                    .last_sync_error
                    .clone()
                    .unwrap_or_else(|| "WAL sync failed persistently".to_string());
                return Err(Self::error_from_message(msg));
            }
            let Some(remaining) = timeout.checked_sub(sync_start.elapsed()) else {
                let message = format!("WAL sync timed out after {timeout:?}");
                s.sync_failed = true;
                s.last_sync_error = Some(message.clone());
                return Err(crate::common::MidgeError::Timeout(message));
            };
            if self.sync_cond.wait_for(&mut s, remaining).timed_out()
                && s.completed_fsyncs < my_sync_id
            {
                let message = format!("WAL sync timed out after {timeout:?}");
                s.sync_failed = true;
                s.last_sync_error = Some(message.clone());
                return Err(crate::common::MidgeError::Timeout(message));
            }
        }
        Ok(())
    }

    fn current_pos(&self) -> WalPos {
        self.current_pos.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn close(&self) -> MidgeResult<()> {
        self.close_with_timeout(JOIN_TIMEOUT)
    }
}

impl FsWalWriterIo {
    fn close_with_timeout(&self, timeout: Duration) -> MidgeResult<()> {
        // Signal shutdown to the writer thread
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Wake the writer thread so it can exit
        self.queue_cond.notify_all();

        // Keep the handle on timeout. Engine cleanup moves the owning runtime
        // to a reaper, which later joins this worker before releasing fencing.
        let start = std::time::Instant::now();
        let mut writer_thread = self.writer_thread.lock();
        while writer_thread
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            if start.elapsed() >= timeout {
                return Err(crate::common::MidgeError::Timeout(format!(
                    "WAL writer did not terminate within {timeout:?}"
                )));
            }
            drop(writer_thread);
            std::thread::sleep(std::time::Duration::from_millis(1));
            writer_thread = self.writer_thread.lock();
        }
        if let Some(handle) = writer_thread.take() {
            let _ = handle.join();
        }
        Ok(())
    }
}

impl Drop for FsWalWriterIo {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.queue_cond.notify_all();

        // Engine::drop moves the runtime, heartbeat, and lease into a reaper.
        // Waiting here is therefore off the caller thread and keeps fencing
        // alive until this writer has actually stopped.
        if let Some(handle) = self.writer_thread.lock().take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::traits::{DirEntry, FileCaps, Metadata};
    use crate::io::{Durability, File, FsResult, OpenOptions};
    use crate::wal::types::{WalOpKind, WalRecord};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn writer_without_background_worker() -> FsWalWriterIo {
        FsWalWriterIo {
            fs: Arc::new(crate::io::MockFs::new()),
            queue: Arc::new(Mutex::new(Vec::new())),
            queue_cond: Arc::new(Condvar::new()),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            sync_state: Arc::new(Mutex::new(SyncState::default())),
            sync_cond: Arc::new(Condvar::new()),
            writer_thread: Mutex::new(None),
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            current_pos: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    struct SyncCountingFile {
        inner: Box<dyn File>,
        durable_sync_count: Arc<AtomicUsize>,
    }

    impl File for SyncCountingFile {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            self.inner.read_at(offset, len)
        }

        fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
            self.inner.write_at(offset, data)
        }

        fn append(&mut self, data: Bytes) -> FsResult<u64> {
            self.inner.append(data)
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }

        fn sync(&mut self, durability: Durability) -> FsResult<()> {
            self.inner.sync(durability)?;
            if durability == Durability::Durable {
                self.durable_sync_count.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }

        fn close(self: Box<Self>) -> FsResult<()> {
            self.inner.close()
        }

        fn caps(&self) -> FileCaps {
            self.inner.caps()
        }
    }

    #[derive(Default)]
    struct SyncCountingFs {
        inner: crate::io::MockFs,
        durable_sync_count: Arc<AtomicUsize>,
    }

    impl Fs for SyncCountingFs {
        fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.inner.open(path, options)
        }

        fn open_persistent_handle(
            &self,
            path: &FsPath,
            options: OpenOptions,
        ) -> FsResult<Box<dyn File>> {
            Ok(Box::new(SyncCountingFile {
                inner: self.inner.open_persistent_handle(path, options)?,
                durable_sync_count: Arc::clone(&self.durable_sync_count),
            }))
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }

        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_dir_all(path)
        }

        fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
            self.inner.sync_dir(path, durability)
        }

        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.inner.rename_atomic(from, to)
        }
    }

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.name, previous);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }

    #[test]
    fn should_create_wal_writer_io() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Assert
        assert_eq!(writer.current_pos(), 0);
        Ok(())
    }

    #[test]
    fn should_timeout_append_acknowledgement_and_fence_writer() {
        // Arrange
        let writer = writer_without_background_worker();

        // Act
        let result =
            writer.enqueue_encoded_with_timeout(vec![1, 2, 3], std::time::Duration::from_millis(5));
        let subsequent_result =
            writer.enqueue_encoded_with_timeout(vec![4], std::time::Duration::from_millis(5));

        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
        assert!(matches!(
            subsequent_result,
            Err(crate::common::MidgeError::Fenced(_))
        ));
    }

    #[test]
    fn should_retain_stuck_writer_handle_when_close_times_out() -> MidgeResult<()> {
        // Arrange
        let writer = writer_without_background_worker();
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release_for_worker = Arc::clone(&release);
        let handle = std::thread::spawn(move || {
            while !release_for_worker.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::yield_now();
            }
        });
        *writer.writer_thread.lock() = Some(handle);

        // Act
        let result = writer.close_with_timeout(std::time::Duration::from_millis(5));

        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
        assert!(writer.writer_thread.lock().is_some());

        release.store(true, std::sync::atomic::Ordering::Release);
        writer.close_with_timeout(std::time::Duration::from_secs(1))?;
        Ok(())
    }

    #[test]
    fn should_support_flush() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Act
        let result = writer.flush();

        // Assert
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn should_sync_durably_when_legacy_skip_variable_is_set() -> MidgeResult<()> {
        // Arrange
        let _environment_guard = EnvVarGuard::set("MIDGE_SKIP_WAL_SYNC", "1");
        let fs = Arc::new(SyncCountingFs::default());
        let sync_count = Arc::clone(&fs.durable_sync_count);
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Act
        writer.sync()?;

        // Assert
        assert_eq!(sync_count.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn should_support_close() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Act
        let result = writer.close();

        // Assert
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn should_advance_current_pos_before_append_record_returns() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = FsWalWriterIo::new("wal.log", fs)?;
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            1,
            7,
        );

        // Act
        let pos = writer.append_record(&record)?;

        // Assert
        assert_eq!(pos, 0);
        assert!(writer.current_pos() > 0);
        Ok(())
    }

    #[test]
    fn should_encode_record_frame_identically_to_payload_then_frame() -> MidgeResult<()> {
        // Arrange
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
            7,
        );
        record.txn_id = Some(9);
        let payload = crate::wal::encoding::encode(&record)?;
        let mut expected = Vec::new();
        crate::wal::frame::append_frame(&mut expected, &payload)?;
        let mut actual = Vec::new();

        // Act
        FsWalWriterIo::encode_record_frame(&record, &mut actual)?;

        // Assert
        assert_eq!(actual, expected);
        Ok(())
    }
}
