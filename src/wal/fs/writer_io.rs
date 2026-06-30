//! Filesystem WAL writer using `io::Fs` abstraction
//!
//! This writer uses the base `io::Fs` trait instead of storage abstractions directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.
//!
//! Architectural rules (Copilot: read carefully and DO NOT modify):
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
use crate::wal::types::{WalOpKind, WalPos, WalRecord};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

use super::writer_runner::{SyncState, WriterConfig, WriterRunner};

/// Filesystem-backed WAL writer using `io::Fs`.
///
/// This struct is responsible ONLY for writing bytes to `wal.log`.
/// It does not manage segment rotation, sequence assignment, recovery,
/// or any other higher-level concerns. Those belong to the WAL actor.
pub struct FsWalWriterIo {
    /// Reserved for segment rotation / reopen.
    #[allow(dead_code)]
    path: FsPath,
    /// Reserved for segment rotation.
    #[allow(dead_code)]
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
            path: path.clone(),
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

    fn encode_record_frame(&self, record: &WalRecord, buf: &mut Vec<u8>) -> MidgeResult<()> {
        let e_start = std::time::Instant::now();
        let encoded = encoding::encode(record)?;
        let e_elapsed = e_start.elapsed();
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_wal_encode(e_elapsed.as_nanos() as u64);
        }
        crate::wal::frame::append_frame(buf, &encoded)
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
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);

        // Enqueue with backpressure: if queue is full, wait for it to drain.
        let mut backoff_ms = 1u64;
        const MAX_BACKOFF_MS: u64 = 100;
        const MAX_WAIT_ATTEMPTS: u32 = 50;

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

                return match ack_rx.recv() {
                    Ok(result) => result,
                    Err(_) => Err(self.writer_failure().unwrap_or_else(|| {
                        crate::common::MidgeError::Internal(
                            "WAL writer thread exited before append acknowledgement".to_string(),
                        )
                    })),
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
        } else {
            crate::common::MidgeError::Internal(message)
        }
    }
}

impl WalWriter for FsWalWriterIo {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Encode and enqueue payload into queue. The method only returns after the
        // writer thread has appended the bytes to the local WAL file.
        let mut buf = self.take_buffer();
        buf.clear();
        self.encode_record_frame(record, &mut buf)?;
        self.enqueue_encoded(buf)
    }

    fn append_op(
        &self,
        _kind: WalOpKind,
        _key: &[u8],
        _value: Option<&[u8]>,
    ) -> MidgeResult<WalPos> {
        // Default implementation: error, as we need a sequence number
        Err(crate::common::MidgeError::NotSupported(
            "append_op without sequence number not supported".into(),
        ))
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
            self.encode_record_frame(record, &mut buf)?;
        }

        let batch_start = self.enqueue_encoded(buf)?;
        Ok(batch_start.saturating_add(last_record_offset))
    }

    fn flush(&self) -> MidgeResult<()> {
        // Mark that a flush is requested and wake the writer; then wait for it to complete.
        //
        // IMPORTANT: We cannot fast-path based on `queue.is_empty()`. The writer thread drains
        // the queue into a local batch, making the queue empty while the actual file append is
        // still in-flight. Callers use flush() as a barrier for "all enqueued records are written".
        let my_flush_id = {
            let mut s = self.sync_state.lock();
            s.pending_flushes = s.pending_flushes.saturating_add(1);
            s.pending_flushes
        };
        // Wake writer so it can process queued data
        self.queue_cond.notify_one();

        // Wait until writer marks the flush as completed
        let mut s = self.sync_state.lock();
        while s.completed_flushes < my_flush_id {
            if s.write_failed {
                let msg = s
                    .last_write_error
                    .clone()
                    .unwrap_or_else(|| "WAL write failed persistently".to_string());
                return Err(Self::error_from_message(msg));
            }
            self.sync_cond.wait(&mut s);
        }

        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_wal_flush();
        }
        Ok(())
    }

    fn sync(&self) -> MidgeResult<()> {
        // Developer convenience: allow skipping WAL fsync during benches/dev runs
        if std::env::var_os("MIDGE_SKIP_WAL_SYNC").is_some() {
            return Ok(());
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
            self.sync_cond.wait(&mut s);
        }
        let elapsed = sync_start.elapsed();
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_wal_fsync_ns(elapsed.as_nanos() as u64);
            t.metrics().record_wal_fsync_count();
        }
        Ok(())
    }

    fn current_pos(&self) -> WalPos {
        self.current_pos.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn close(&self) -> MidgeResult<()> {
        // Signal shutdown to the writer thread
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Wake the writer thread so it can exit
        self.queue_cond.notify_all();

        // === Phase 1.3: Join with timeout to prevent indefinite hangs ===
        // If writer thread is stuck in fsync (NFS hang, disk failure), we timeout
        // after 30s and detach the thread rather than blocking forever.
        const JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

        if let Some(handle) = self.writer_thread.lock().take() {
            let start = std::time::Instant::now();
            loop {
                if handle.is_finished() {
                    let _ = handle.join();
                    break;
                }
                if start.elapsed() > JOIN_TIMEOUT {
                    tracing::error!(
                        timeout_secs = JOIN_TIMEOUT.as_secs(),
                        "WAL writer thread join timeout; thread may be stuck in fsync. \
                         Detaching thread to allow shutdown to proceed. \
                         Data loss may occur if fsync never completes."
                    );
                    // Thread is orphaned but process can exit
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
        Ok(())
    }
}

impl Drop for FsWalWriterIo {
    fn drop(&mut self) {
        // === Phase 1.3: Ensure writer thread is stopped with timeout ===
        // Same logic as close() to prevent drop from blocking forever
        const JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.queue_cond.notify_all();

        if let Some(handle) = self.writer_thread.lock().take() {
            let start = std::time::Instant::now();
            loop {
                if handle.is_finished() {
                    let _ = handle.join();
                    break;
                }
                if start.elapsed() > JOIN_TIMEOUT {
                    tracing::error!("WAL writer thread join timeout on drop; detaching thread");
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::types::WalRecord;
    use bytes::Bytes;

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
}
