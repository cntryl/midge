//! Filesystem WAL writer using `io::Fs` abstraction
//!
//! This writer uses the base `io::Fs` trait instead of storage abstractions directly,
//! allowing for swappable real and mock implementations in tests.
//!
//! Architectural rules (Maintainer: read carefully and DO NOT modify):
//! ---------------------------------------------------------------
//! • `FsWalWriterIo` ONLY appends bytes to the WAL ledger's open segment,
//!   which holds only its header when the writer opens it.
//! • It NEVER assigns sequence numbers; it refuses a record whose sequence
//!   would not follow the segment's last one.
//! • It NEVER rotates WAL segments. It writes one frame beyond the records:
//!   the footer that seals the segment, after which it appends nothing.
//! • It MUST write records as: `<u32 length prefix><u32 crc32c><encoded record bytes>`.
//! • It MUST update the write position monotonically.
//! • It MUST flush/sync exactly and only when asked.
//! • All concurrency protection is via `Mutex` — do NOT add async constructs.

use crate::common::{MidgeError, MidgeResult};
use crate::io::FsError;
use crate::io::{Fs, FsPath};
use crate::wal::encoding;
use crate::wal::format::{
    Footer, SealedFacts, SegmentDigest, SegmentHeader, SegmentTail, HEADER_LEN,
};
use crate::wal::traits::{WalAppendError, WalWriter};
use crate::wal::types::{WalPos, WalRecord};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::Duration;

use super::writer_runner::{SyncState, WriterConfig, WriterRunner};

const MAX_BACKOFF_MS: u64 = 100;
const MAX_WAIT_ATTEMPTS: u32 = 50;

/// Filesystem-backed WAL writer using `io::Fs`.
///
/// This struct is responsible ONLY for writing bytes to the open segment.
/// It does not manage segment rotation, sequence assignment, recovery,
/// or any other higher-level concerns. Those belong to the WAL actor.
pub struct FsWalWriterIo {
    /// Reserved for segment rotation.
    fs: Arc<dyn Fs>,
    write_authority: Arc<parking_lot::RwLock<Option<crate::wal::traits::WriteAuthority>>>,
    /// Operational counters of the owning engine.
    counters: crate::telemetry::CounterSink,

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

    // Maximum wait for append, flush, sync, and close acknowledgements.
    io_timeout: Duration,

    /// The header of the segment this writer appends to.
    header: SegmentHeader,
    /// The records acknowledged into the segment.
    tail: Mutex<SegmentTail>,
    /// The digest of every byte written, shared with the runner.
    digest: Arc<Mutex<SegmentDigest>>,
    /// The footer was written: nothing more may be appended.
    sealed: std::sync::atomic::AtomicBool,
}
impl FsWalWriterIo {
    /// Create a new WAL writer appending to the file at `path_str` in the
    /// provided filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be created or opened.
    #[cfg(test)]
    pub fn new(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        Self::new_with_counters(
            path_str,
            fs,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
            crate::telemetry::CounterSink::default(),
        )
    }

    /// Create a writer with an explicit storage acknowledgement timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be created or opened.
    #[cfg(test)]
    pub fn new_with_timeout(
        path_str: &str,
        fs: Arc<dyn Fs>,
        io_timeout: Duration,
    ) -> MidgeResult<Self> {
        Self::new_with_counters(
            path_str,
            fs,
            io_timeout,
            crate::telemetry::CounterSink::default(),
        )
    }

    /// Create a writer that records into an engine's counters.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be created or opened.
    #[cfg(test)]
    pub(crate) fn new_with_counters(
        path_str: &str,
        fs: Arc<dyn Fs>,
        io_timeout: Duration,
        counters: crate::telemetry::CounterSink,
    ) -> MidgeResult<Self> {
        if !fs
            .exists(&FsPath::new(path_str))
            .map_err(FsError::into_midge)?
        {
            create_test_segment(fs.as_ref(), path_str)?;
        }
        Self::new_with_authority(path_str, fs, io_timeout, counters, None)
    }

    pub(crate) fn new_with_authority(
        path_str: &str,
        fs: Arc<dyn Fs>,
        io_timeout: Duration,
        counters: crate::telemetry::CounterSink,
        authority: Option<crate::wal::traits::WriteAuthority>,
    ) -> MidgeResult<Self> {
        if let Some(check) = &authority {
            check()?;
        }
        let path = FsPath::new(path_str);
        // The segment holds exactly its header, which the WAL history wrote
        // and synced, directory entry included, before naming it.
        let file = fs
            .open(
                &path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;
        let current_pos = file.len().map_err(FsError::into_midge)?;
        if current_pos != HEADER_LEN as u64 {
            return Err(MidgeError::Corruption(format!(
                "WAL segment {path} holds {current_pos} bytes; a writer opens only a segment holding just its header"
            )));
        }
        let header_bytes = file
            .read_at(0, HEADER_LEN as u64)
            .map_err(FsError::into_midge)?;
        let header = SegmentHeader::decode(&header_bytes)?;
        drop(file);
        if let Some(check) = &authority {
            check()?;
        }

        let writer = Self {
            fs,
            write_authority: Arc::new(parking_lot::RwLock::new(authority)),
            counters,
            current_pos: Arc::new(std::sync::atomic::AtomicU64::new(current_pos)),
            queue: Arc::new(Mutex::new(Vec::new())),
            queue_cond: Arc::new(Condvar::new()),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            sync_state: Arc::new(Mutex::new(SyncState::default())),
            sync_cond: Arc::new(Condvar::new()),
            writer_thread: Mutex::new(None),
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            io_timeout,
            header,
            tail: Mutex::new(SegmentTail::default()),
            digest: Arc::new(Mutex::new(SegmentDigest::of(&header_bytes))),
            sealed: std::sync::atomic::AtomicBool::new(false),
        };

        // Spawn background writer thread
        let config = WriterConfig {
            write_authority: Arc::clone(&writer.write_authority),
            fs: Arc::clone(&writer.fs),
            path,
            queue: writer.queue.clone(),
            queue_cond: writer.queue_cond.clone(),
            buf_pool: writer.buf_pool.clone(),
            sync_state: writer.sync_state.clone(),
            sync_cond: writer.sync_cond.clone(),
            current_pos: writer.current_pos.clone(),
            shutdown: writer.shutdown.clone(),
            counters: writer.counters.clone(),
            digest: Arc::clone(&writer.digest),
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
        crate::wal::frame::append_frame_encoded(buf, |payload| {
            encoding::encode_into(record, payload)
        })?;
        let e_elapsed = e_start.elapsed();
        self.counters.record(|m| {
            m.record_wal_encode(u64::try_from(e_elapsed.as_nanos()).unwrap_or(u64::MAX));
        });
        Ok(())
    }

    /// Refuse an append once the footer is written, or one whose records
    /// would not follow the segment's last sequence: a segment's sequences
    /// strictly increase from its header's first one.
    fn admit_records<'a>(
        &self,
        records: impl IntoIterator<Item = &'a WalRecord>,
    ) -> Result<(), WalAppendError> {
        if self.sealed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(WalAppendError::unchanged(MidgeError::Internal(format!(
                "WAL segment {} is sealed; nothing more may be appended to it",
                self.header.id
            ))));
        }
        self.tail
            .lock()
            .admit_all(&self.header, records.into_iter().map(|record| record.seq))
            .map_err(|reason| {
                WalAppendError::unchanged(MidgeError::Internal(format!(
                    "WAL segment {} refuses the append: {reason}",
                    self.header.id
                )))
            })
    }

    /// Count records the runner acknowledged into the segment.
    fn count_records<'a>(&self, records: impl IntoIterator<Item = &'a WalRecord>) {
        let mut tail = self.tail.lock();
        for record in records {
            tail.count(record.seq, record.cf_id);
        }
    }

    /// Wait, at most `timeout`, until every queued write is written: the
    /// digest then covers the whole segment.
    fn drained_digest(
        &self,
        timeout: Duration,
    ) -> MidgeResult<parking_lot::MutexGuard<'_, SegmentDigest>> {
        let started = std::time::Instant::now();
        loop {
            if let Some(error) = self.writer_failure() {
                return Err(error);
            }
            let digest = self.digest.lock();
            if self.queue.lock().is_empty() && digest.data_len() == self.current_pos() {
                return Ok(digest);
            }
            drop(digest);
            if started.elapsed() >= timeout {
                return Err(MidgeError::Timeout(format!(
                    "WAL writes did not drain within {timeout:?}"
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The footer sealing every record written, once the writes drained.
    fn sealing(&self, timeout: Duration) -> MidgeResult<Footer> {
        if self.sealed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(MidgeError::Internal(format!(
                "WAL segment {} is already sealed",
                self.header.id
            )));
        }
        let digest = self.drained_digest(timeout)?;
        Ok(Footer::sealing(&self.header, &self.tail.lock(), &digest))
    }

    fn take_buffer(&self) -> Vec<u8> {
        let mut pool = self.buf_pool.lock();
        pool.pop().unwrap_or_else(|| Vec::with_capacity(1024))
    }

    fn writer_failure(&self) -> Option<crate::common::MidgeError> {
        self.sync_state.lock().failure()
    }

    #[cfg(test)]
    fn enqueue_encoded_with_timeout(
        &self,
        buf: Vec<u8>,
        acknowledgement_timeout: Duration,
    ) -> MidgeResult<WalPos> {
        self.enqueue_encoded_accounted(buf, acknowledgement_timeout)
            .map_err(|failure| failure.error)
    }

    fn enqueue_encoded_accounted(
        &self,
        buf: Vec<u8>,
        acknowledgement_timeout: Duration,
    ) -> Result<WalPos, WalAppendError> {
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);

        // Enqueue with backpressure: if queue is full, wait for it to drain.
        let mut backoff_ms = 1u64;
        for attempt in 0..MAX_WAIT_ATTEMPTS {
            if let Some(err) = self.writer_failure() {
                return Err(WalAppendError::unchanged(err));
            }

            let mut q = self.queue.lock();
            if q.len() < super::writer_runner::MAX_QUEUE_DEPTH {
                q.push(super::writer_runner::QueuedWrite::new(buf, ack_tx));
                self.queue_cond.notify_one();

                drop(q);

                if attempt > 0 {
                    self.counters.record(|m| {
                        m.record_wal_backpressure_wait(u64::from(attempt));
                    });
                }

                return match ack_rx.recv_timeout(acknowledgement_timeout) {
                    Ok(result) => result,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let message = format!(
                            "WAL append acknowledgement timed out after {acknowledgement_timeout:?}"
                        );
                        let mut state = self.sync_state.lock();
                        state.write_failed = true;
                        state.last_write_error =
                            Some(crate::common::MidgeError::Timeout(message.clone()));
                        drop(state);
                        self.sync_cond.notify_all();
                        self.queue_cond.notify_all();
                        Err(WalAppendError::unknown(crate::common::MidgeError::Timeout(
                            message,
                        )))
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(
                        WalAppendError::unknown(self.writer_failure().unwrap_or_else(|| {
                            crate::common::MidgeError::Internal(
                                "WAL writer thread exited before append acknowledgement"
                                    .to_string(),
                            )
                        })),
                    ),
                };
            }

            let wait_duration = std::time::Duration::from_millis(backoff_ms);
            self.queue_cond.wait_for(&mut q, wait_duration);
            drop(q);

            backoff_ms = std::cmp::min(backoff_ms * 2, MAX_BACKOFF_MS);
        }

        if let Some(err) = self.writer_failure() {
            return Err(WalAppendError::unchanged(err));
        }

        let q = self.queue.lock();
        Err(WalAppendError::unchanged(
            crate::common::MidgeError::WriteStall(format!(
                "WAL queue full after {} attempts ({}/{} items, backoff exhausted)",
                MAX_WAIT_ATTEMPTS,
                q.len(),
                super::writer_runner::MAX_QUEUE_DEPTH
            )),
        ))
    }
}

impl WalWriter for FsWalWriterIo {
    fn set_write_authority(&self, authority: Option<crate::wal::traits::WriteAuthority>) {
        *self.write_authority.write() = authority;
    }

    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        self.append_record_accounted(record)
            .map_err(|failure| failure.error)
    }

    fn append_record_accounted(&self, record: &WalRecord) -> Result<WalPos, WalAppendError> {
        self.admit_records([record])?;
        let mut buf = self.take_buffer();
        buf.clear();
        self.encode_record_frame(record, &mut buf)
            .map_err(WalAppendError::unchanged)?;
        let position = self.enqueue_encoded_accounted(buf, self.io_timeout)?;
        self.count_records([record]);
        Ok(position)
    }

    fn append_batch(&self, records: &[WalRecord]) -> MidgeResult<WalPos> {
        self.append_batch_accounted(records)
            .map_err(|failure| failure.error)
    }

    fn append_batch_accounted(&self, records: &[WalRecord]) -> Result<WalPos, WalAppendError> {
        if records.is_empty() {
            return Ok(self.current_pos());
        }
        self.admit_records(records)?;
        let mut buf = self.take_buffer();
        buf.clear();
        let mut last_record_offset = 0u64;
        for (index, record) in records.iter().enumerate() {
            if index + 1 == records.len() {
                last_record_offset = buf.len() as u64;
            }
            self.encode_record_frame(record, &mut buf)
                .map_err(WalAppendError::unchanged)?;
        }
        let batch_start = self.enqueue_encoded_accounted(buf, self.io_timeout)?;
        self.count_records(records);
        Ok(batch_start.saturating_add(last_record_offset))
    }

    fn sync(&self) -> MidgeResult<()> {
        self.sync_with_timeout(self.io_timeout)
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
            // A failed batch write stops the writer thread, so this fsync
            // will never complete; report either failure now.
            if s.sync_failed {
                return Err(s.last_sync_error.as_ref().map_or_else(
                    || crate::common::MidgeError::Internal("WAL sync failed persistently".into()),
                    crate::common::MidgeError::replay,
                ));
            }
            if let Some(error) = s.failure() {
                return Err(error);
            }
            let Some(remaining) = timeout.checked_sub(sync_start.elapsed()) else {
                let message = format!("WAL sync timed out after {timeout:?}");
                s.sync_failed = true;
                s.last_sync_error = Some(crate::common::MidgeError::Timeout(message.clone()));
                return Err(crate::common::MidgeError::Timeout(message));
            };
            if self.sync_cond.wait_for(&mut s, remaining).timed_out()
                && s.completed_fsyncs < my_sync_id
            {
                let message = format!("WAL sync timed out after {timeout:?}");
                s.sync_failed = true;
                s.last_sync_error = Some(crate::common::MidgeError::Timeout(message.clone()));
                return Err(crate::common::MidgeError::Timeout(message));
            }
        }
        Ok(())
    }

    fn current_pos(&self) -> WalPos {
        self.current_pos.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn tail(&self) -> SegmentTail {
        self.tail.lock().clone()
    }

    fn seal_facts(&self, timeout: Duration) -> MidgeResult<SealedFacts> {
        self.sealing(timeout).map(|footer| footer.sealed_facts())
    }

    fn write_footer(&self, facts: &SealedFacts, timeout: Duration) -> MidgeResult<()> {
        let footer = self.sealing(timeout)?;
        if footer.sealed_facts() != *facts {
            return Err(MidgeError::Internal(format!(
                "WAL segment {} changed after its seal facts were taken",
                self.header.id
            )));
        }
        let frame = footer.encode_frame()?;
        self.sealed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.enqueue_encoded_accounted(frame, timeout)
            .map_err(|failure| failure.error)?;
        self.sync_with_timeout(timeout)
    }
}

/// Create `path` holding the header of segment 1 of a history of its own,
/// synced with its directory entry, for tests that write a segment
/// directly. A segment file name gives the header its id.
///
/// # Errors
///
/// The I/O error.
#[cfg(test)]
pub(crate) fn create_test_segment(fs: &dyn Fs, path: &str) -> MidgeResult<()> {
    let path = FsPath::new(path);
    let name = path.0.rsplit('/').next().unwrap_or_default();
    let header = SegmentHeader {
        id: crate::wal::parse_segment_file_name(name).unwrap_or(1),
        epoch: 0,
        holder: [0; 16],
        first_seq: 1,
        prev: crate::wal::format::ChainLink::GENESIS,
    };
    let mut file = fs
        .open(
            &path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )
        .map_err(FsError::into_midge)?;
    file.write_at(0, bytes::Bytes::copy_from_slice(&header.encode()))
        .map_err(FsError::into_midge)?;
    file.sync(crate::io::Durability::Durable)
        .map_err(FsError::into_midge)?;
    drop(file);
    let parent = std::path::Path::new(&path.0)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(
            || FsPath::new("."),
            |parent| FsPath::new(parent.to_string_lossy()),
        );
    fs.sync_dir(&parent, crate::io::Durability::Durable)
        .map_err(FsError::into_midge)
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
    use crate::io::traits::{DirEntry, Metadata};
    use crate::io::{Durability, File, FsResult, OpenOptions};
    use crate::wal::types::{WalOpKind, WalRecord};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn writer_without_background_worker() -> FsWalWriterIo {
        FsWalWriterIo {
            write_authority: Arc::new(parking_lot::RwLock::new(None)),
            fs: Arc::new(crate::io::MockFs::new()),
            counters: crate::telemetry::CounterSink::default(),
            queue: Arc::new(Mutex::new(Vec::new())),
            queue_cond: Arc::new(Condvar::new()),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            sync_state: Arc::new(Mutex::new(SyncState::default())),
            sync_cond: Arc::new(Condvar::new()),
            writer_thread: Mutex::new(None),
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            current_pos: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            io_timeout: Duration::from_millis(10),
            header: SegmentHeader {
                id: 1,
                epoch: 0,
                holder: [0; 16],
                first_seq: 1,
                prev: crate::wal::format::ChainLink::GENESIS,
            },
            tail: Mutex::new(SegmentTail::default()),
            digest: Arc::new(Mutex::new(SegmentDigest::of(&[]))),
            sealed: std::sync::atomic::AtomicBool::new(false),
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
    }

    type OpenGate = (
        std::sync::mpsc::Sender<()>,
        std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    );

    #[derive(Default)]
    struct SyncCountingFs {
        open_gate: Option<Arc<OpenGate>>,
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
            if let Some(gate) = &self.open_gate {
                gate.0.send(()).unwrap();
                gate.1.lock().unwrap().recv().unwrap();
            }
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

    /// Fs whose WAL file blocks each write until released, then fails it
    /// with a no-space error, so a test can queue a sync behind the failure.
    struct GatedNoSpaceFs {
        inner: crate::io::MockFs,
        entered: std::sync::Mutex<std::sync::mpsc::Sender<()>>,
        release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    }

    struct GatedNoSpaceFile {
        inner: Box<dyn File>,
        entered: std::sync::mpsc::Sender<()>,
        release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    }

    impl GatedNoSpaceFile {
        fn fail_after_release(&self) -> crate::io::FsError {
            let _ = self.entered.send(());
            let _ = self
                .release
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recv();
            crate::io::FsError::NoSpace("No space left on device".to_string())
        }
    }

    impl File for GatedNoSpaceFile {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            self.inner.read_at(offset, len)
        }

        fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
            Err(self.fail_after_release())
        }

        fn append(&mut self, _data: Bytes) -> FsResult<u64> {
            Err(self.fail_after_release())
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }

        fn sync(&mut self, durability: Durability) -> FsResult<()> {
            self.inner.sync(durability)
        }
    }

    impl Fs for GatedNoSpaceFs {
        fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.inner.open(path, options)
        }

        fn open_persistent_handle(
            &self,
            path: &FsPath,
            options: OpenOptions,
        ) -> FsResult<Box<dyn File>> {
            Ok(Box::new(GatedNoSpaceFile {
                inner: self.inner.open_persistent_handle(path, options)?,
                entered: self
                    .entered
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
                release: Arc::clone(&self.release),
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
        assert!(fs.get_file("wal.log").is_none());

        // Act
        let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs) as Arc<dyn crate::io::Fs>)?;

        // Assert: construction must actually create the backing segment
        // with its header (not just report success), and appends follow it.
        assert_eq!(writer.current_pos(), HEADER_LEN as u64);
        let created = fs.get_file("wal.log").expect("the WAL file exists");
        assert_eq!(created.len(), HEADER_LEN);
        SegmentHeader::decode(&created)?;

        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
        );
        writer.append_record(&record)?;
        drop(writer);
        let written_len = fs.get_file("wal.log").expect("wal file should exist").len() as u64;
        assert!(written_len > HEADER_LEN as u64);
        Ok(())
    }

    #[test]
    fn should_fence_writer_when_append_acknowledgement_times_out() {
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
            Err(crate::common::MidgeError::Timeout(_))
        ));
    }

    #[test]
    fn should_prove_subsequent_append_was_not_enqueued_after_ambiguous_timeout() {
        // Arrange
        let writer = writer_without_background_worker();

        // Act
        let timed_out = writer
            .enqueue_encoded_accounted(vec![1, 2, 3], Duration::ZERO)
            .expect_err("worker cannot acknowledge queued append");
        let rejected = writer
            .enqueue_encoded_accounted(vec![4], Duration::ZERO)
            .expect_err("writer is fenced before enqueue");

        // Assert
        assert!(matches!(
            timed_out.error,
            crate::common::MidgeError::Timeout(_)
        ));
        assert!(!timed_out.unchanged, "timed-out bytes may still be written");
        // A slow local disk is not lost lease authority.
        assert!(
            matches!(rejected.error, crate::common::MidgeError::Timeout(_)),
            "{:?}",
            rejected.error
        );
        assert!(rejected.unchanged, "only the new admission can be released");
        assert_eq!(writer.queue.lock().len(), 1);
    }

    #[test]
    fn should_apply_configured_timeout_when_append_waits_for_acknowledgement() {
        // Arrange
        let writer = writer_without_background_worker();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
        );

        // Act
        let started = std::time::Instant::now();
        let result = writer.append_record(&record);

        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "append ignored configured storage I/O timeout"
        );
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
    fn should_drain_pending_records_when_writer_drops() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs) as Arc<dyn crate::io::Fs>)?;
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"close-key"),
            Some(Bytes::from_static(b"close-value")),
            1,
        );
        writer.append_record(&record)?;
        let appended_pos = writer.current_pos();

        // Act
        drop(writer);

        // Assert: dropping the writer must drain pending data before the
        // writer thread exits, not discard the queue.
        let on_disk = fs.get_file("wal.log").expect("wal file should exist");
        assert!(
            !on_disk.is_empty(),
            "drop returned before pending data reached the backing filesystem"
        );
        assert_eq!(on_disk.len() as u64, appended_pos);
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
        );

        // Act
        let pos = writer.append_record(&record)?;

        // Assert: the first record follows the segment header.
        assert_eq!(pos, HEADER_LEN as u64);
        assert!(writer.current_pos() > HEADER_LEN as u64);
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
        );
        record.txn_id = Some(9);
        let payload = crate::wal::encoding::encode(&record)?;
        let mut expected = Vec::new();
        crate::wal::frame::append_frame(&mut expected, &payload)?;
        let mut actual = Vec::new();

        // Act
        writer_without_background_worker().encode_record_frame(&record, &mut actual)?;

        // Assert
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn should_reject_physical_fsync_when_validity_expires_without_watchdog() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(SyncCountingFs::default());
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        validity
            .activate(1, std::time::Instant::now() + Duration::from_mins(1))
            .unwrap();
        let check = Arc::clone(&validity);
        create_test_segment(fs.as_ref(), "wal.log")?;
        let writer = FsWalWriterIo::new_with_authority(
            "wal.log",
            fs.clone(),
            Duration::from_secs(5),
            crate::telemetry::CounterSink::default(),
            Some(Arc::new(move || {
                check
                    .remaining(1)
                    .map(|_| ())
                    .map_err(|error| error.into_validation_error("physical WAL fsync authority"))
            })),
        )?;
        validity.expire_for_test();
        // Act
        let result = writer.sync();
        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Fenced(_))));
        assert_eq!(fs.durable_sync_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn should_reject_queued_append_when_validity_expires_during_handle_open() -> MidgeResult<()> {
        // Arrange
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let fs = Arc::new(SyncCountingFs {
            open_gate: Some(Arc::new((entered_tx, std::sync::Mutex::new(release_rx)))),
            ..SyncCountingFs::default()
        });
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        validity
            .activate(1, std::time::Instant::now() + Duration::from_mins(1))
            .unwrap();
        let check = Arc::clone(&validity);
        create_test_segment(fs.as_ref(), "wal.log")?;
        let writer = Arc::new(FsWalWriterIo::new_with_authority(
            "wal.log",
            fs.clone(),
            Duration::from_secs(10),
            crate::telemetry::CounterSink::default(),
            Some(Arc::new(move || {
                check
                    .remaining(1)
                    .map(|_| ())
                    .map_err(|error| error.into_validation_error("queued WAL authority"))
            })),
        )?);
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let append_writer = Arc::clone(&writer);
        let appender = std::thread::spawn(move || {
            append_writer.append_record_accounted(&WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                1,
            ))
        });
        let watchdog = std::time::Instant::now() + Duration::from_secs(5);
        while writer.queue.lock().is_empty() {
            assert!(std::time::Instant::now() < watchdog, "append did not queue");
            std::thread::yield_now();
        }
        // Act
        validity.expire_for_test();
        release_tx.send(()).unwrap();
        let result = appender.join().unwrap();
        // Assert
        assert!(matches!(
            result,
            Err(WalAppendError {
                error: crate::common::MidgeError::Fenced(_),
                unchanged: true
            })
        ));
        assert_eq!(
            fs.metadata(&FsPath::new("wal.log")).unwrap().len,
            HEADER_LEN as u64
        );
        assert_eq!(writer.current_pos(), HEADER_LEN as u64);
        Ok(())
    }

    #[test]
    fn should_fail_pending_sync_promptly_with_write_error_when_runner_write_fails(
    ) -> MidgeResult<()> {
        // Arrange: a sync is already waiting when the writer's batch write
        // fails. The runner exits without completing it, so the waiter must
        // see the write failure instead of sleeping out its whole timeout.
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let fs = Arc::new(GatedNoSpaceFs {
            inner: crate::io::MockFs::new(),
            entered: std::sync::Mutex::new(entered_tx),
            release: Arc::new(std::sync::Mutex::new(release_rx)),
        });
        let writer = Arc::new(FsWalWriterIo::new_with_timeout(
            "wal.log",
            fs as Arc<dyn Fs>,
            Duration::from_secs(10),
        )?);
        let appender = {
            let writer = Arc::clone(&writer);
            std::thread::spawn(move || {
                writer.append_record(&WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from_static(b"key"),
                    Some(Bytes::from_static(b"value")),
                    1,
                ))
            })
        };
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("writer reached the failing write");
        let syncer = {
            let writer = Arc::clone(&writer);
            std::thread::spawn(move || {
                let started = std::time::Instant::now();
                let result = writer.sync_with_timeout(Duration::from_secs(10));
                (result, started.elapsed())
            })
        };
        while writer.sync_state.lock().pending_fsyncs == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }

        // Act
        release_tx.send(()).expect("release the failing write");
        let (result, elapsed) = syncer.join().expect("join syncer");
        let _ = appender.join().expect("join appender");

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::NoSpace(_))),
            "{result:?}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "sync waited {elapsed:?} for a writer that had already failed"
        );
        Ok(())
    }
}
