//! Filesystem WAL writer using io::Fs abstraction
//!
//! This writer uses the base io::Fs trait instead of storage abstractions directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.
//!
//! Architectural rules (Copilot: read carefully and DO NOT modify):
//! ---------------------------------------------------------------
//! • FsWalWriterIo ONLY appends bytes to the active WAL file `wal.log`.
//! • It NEVER assigns sequence numbers.
//! • It NEVER rotates WAL segments.
//! • It NEVER writes metadata beyond the encoded WAL record format.
//! • It MUST write records as: <u32 length prefix><encoded record bytes>.
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

/// Filesystem-backed WAL writer using io::Fs.
///
/// This struct is responsible ONLY for writing bytes to `wal.log`.
/// It does not manage segment rotation, sequence assignment, recovery,
/// or any other higher-level concerns. Those belong to the WAL actor.
pub struct FsWalWriterIo {
    #[allow(dead_code)]
    path: FsPath,
    #[allow(dead_code)]
    fs: Arc<dyn Fs>,

    // Queue of pending encoded record payloads (Vec<u8>)
    queue: Arc<Mutex<Vec<Vec<u8>>>>,
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
            fs: Arc::clone(&fs),
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
            fs: Arc::clone(&fs),
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
}

impl WalWriter for FsWalWriterIo {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Encode and enqueue payload into queue; do not perform IO here.
        let e_start = std::time::Instant::now();
        let encoded = encoding::encode(record)?;
        let e_elapsed = e_start.elapsed();
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_wal_encode(e_elapsed.as_nanos() as u64);
        }

        let len_prefix = (encoded.len() as u32).to_le_bytes();

        // Get a buffer from pool (or allocate)
        let mut buf = {
            let mut pool = self.buf_pool.lock();
            pool.pop().unwrap_or_else(|| Vec::with_capacity(1024))
        };
        buf.clear();
        buf.extend_from_slice(&len_prefix);
        buf.extend_from_slice(&encoded);

        // Enqueue
        {
            let mut q = self.queue.lock();
            q.push(buf);
            // notify background writer
            self.queue_cond.notify_one();
        }

        // Return a best-effort start position; real position updated by writer thread.
        let pos = self.current_pos.load(std::sync::atomic::Ordering::SeqCst);
        Ok(pos)
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
}
