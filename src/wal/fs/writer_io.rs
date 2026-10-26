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
use crate::wal::traits::{GroupedWalWriter, WalAppendError, WalWriter};
use crate::wal::types::{WalPos, WalRecord};
use crate::wal::writer_authority::PendingWriterEffect;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::writer_runner::{SyncState, WriterConfig, WriterRunner};

mod sequenced;

const MAX_BACKOFF_MS: u64 = 100;
const MAX_WAIT_ATTEMPTS: u32 = 50;
const MAX_PENDING_SYNC_REQUESTS: usize = 128;

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
    tail: Arc<Mutex<SegmentTail>>,
    raw_submission_pending: Arc<std::sync::atomic::AtomicBool>,
    raw_submission_memory: crate::common::resource_budget::ResourceBudget,
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
            let mut no_op = || {};
            check.run(&mut no_op)?;
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
            let mut no_op = || {};
            check.run(&mut no_op)?;
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
            tail: Arc::new(Mutex::new(SegmentTail::default())),
            raw_submission_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            raw_submission_memory: crate::common::resource_budget::ResourceBudget::new(
                64 * 1024 * 1024,
            ),
            digest: Arc::new(Mutex::new(SegmentDigest::of(&header_bytes))),
            sealed: std::sync::atomic::AtomicBool::new(false),
        };

        // Spawn background writer thread
        let config = WriterConfig {
            #[cfg(test)]
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
        let failpoint_gate = crate::failpoints::worker_gate();
        let handle = std::thread::Builder::new()
            .name("midge-wal-writer".to_string())
            .spawn(move || failpoint_gate.run(|| runner.run()))?;

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
        if self
            .raw_submission_pending
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(WalAppendError::unchanged(MidgeError::WriteStall(
                "a sequenced WAL append is still awaiting its completion".into(),
            )));
        }
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

    fn submit_footer_operation(
        &self,
        operation_id: u64,
        facts: &SealedFacts,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<crate::wal::footer_completion::WalPendingFooter> {
        let started = Instant::now();
        if let Some(error) = self.writer_failure() {
            return Err(error);
        }
        let footer = self.sealing(timeout)?;
        if footer.sealed_facts() != *facts {
            return Err(MidgeError::Internal(format!(
                "WAL segment {} changed after its seal facts were taken",
                self.header.id
            )));
        }
        let data = footer.encode_frame()?;
        let start_offset = self.current_pos();
        let end_offset = start_offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| MidgeError::ResourceLimit("WAL footer offset overflow".into()))?;
        let expected = crate::wal::footer_completion::WalFooterPrefix {
            operation_id,
            writer_epoch: self.header.epoch,
            segment_id: self.header.id,
            start_offset,
            end_offset,
        };
        if data.is_empty() {
            return Err(MidgeError::Internal("encoded WAL footer is empty".into()));
        }
        crate::failpoints::fail_point!("midge::wal::timeout_before_enqueue", |_| Err(
            MidgeError::Timeout("injected timeout before WAL footer enqueue".into())
        ));
        let (effect, root) =
            PendingWriterEffect::admit(self.write_authority.read().clone(), group, timeout)?;
        let reservation = self
            .raw_submission_memory
            .reserve(data.len(), "asynchronous WAL footer")?;
        let (sender, response) = std::sync::mpsc::sync_channel(1);
        {
            let mut queue = self.queue.lock();
            let mut state = self.sync_state.lock();
            if let Some(error) = state.failure() {
                return Err(error);
            }
            if self.sealed.load(Ordering::Acquire) {
                return Err(MidgeError::Internal(format!(
                    "WAL segment {} is already sealed",
                    self.header.id
                )));
            }
            if queue.len() >= super::writer_runner::MAX_QUEUE_DEPTH
                || state.submitted_writes != state.completed_writes
                || started.elapsed() >= timeout
            {
                return Err(MidgeError::WriteStall(
                    "WAL footer admission is not at a drained segment boundary".into(),
                ));
            }
            let id = state.submitted_writes.checked_add(1).ok_or_else(|| {
                MidgeError::ResourceLimit("WAL writer request identity exhausted".into())
            })?;
            self.sealed.store(true, Ordering::Release);
            state.submitted_writes = id;
            queue.push(super::writer_runner::QueuedWrite::footer(
                data,
                expected,
                sender,
                effect,
                id,
                reservation,
            ));
        }
        self.queue_cond.notify_one();
        let failure_state = Arc::clone(&self.sync_state);
        Ok(crate::wal::footer_completion::WalPendingFooter::new(
            response,
            expected,
            Arc::new(move || failure_state.lock().failure()),
            root,
        ))
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
        self.enqueue_in_group(buf, acknowledgement_timeout, None)
    }

    fn enqueue_in_group(
        &self,
        buf: Vec<u8>,
        acknowledgement_timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> Result<WalPos, WalAppendError> {
        if let Some(error) = self.writer_failure() {
            return Err(WalAppendError::unchanged(error));
        }
        crate::failpoints::fail_point!("midge::wal::timeout_before_enqueue", |_| Err(
            WalAppendError::unchanged(MidgeError::Timeout(
                "injected timeout before WAL enqueue".into()
            ))
        ));
        let admission_start = std::time::Instant::now();
        let authority = self.write_authority.read().clone();
        let (effect, root) = PendingWriterEffect::admit(authority, group, acknowledgement_timeout)
            .map_err(WalAppendError::unchanged)?;
        let native = group.is_some() || root.is_some();
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);

        // Enqueue with backpressure: if queue is full, wait for it to drain.
        let mut backoff_ms = 1u64;
        for attempt in 0..MAX_WAIT_ATTEMPTS {
            if let Some(err) = self.writer_failure() {
                return Err(WalAppendError::unchanged(err));
            }

            let mut q = self.queue.lock();
            // Terminal cleanup takes this same lock before publishing failure
            // and draining tickets. Nothing can queue after the worker exits.
            if let Some(err) = self.writer_failure() {
                return Err(WalAppendError::unchanged(err));
            }
            Self::check_append_budget(native, admission_start, acknowledgement_timeout)?;
            if q.len() < super::writer_runner::MAX_QUEUE_DEPTH {
                let id = {
                    let mut state = self.sync_state.lock();
                    Self::check_append_budget(native, admission_start, acknowledgement_timeout)?;
                    state.submitted_writes += 1;
                    state.submitted_writes
                };
                q.push(super::writer_runner::QueuedWrite::admitted(
                    buf, ack_tx, effect, id,
                ));
                self.queue_cond.notify_one();

                drop(q);

                if attempt > 0 {
                    self.counters.record(|m| {
                        m.record_wal_backpressure_wait(u64::from(attempt));
                    });
                }

                let wait_timeout = if native {
                    acknowledgement_timeout.saturating_sub(admission_start.elapsed())
                } else {
                    acknowledgement_timeout
                };
                return match ack_rx.recv_timeout(wait_timeout) {
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
            let wait_duration = if native {
                wait_duration.min(acknowledgement_timeout.saturating_sub(admission_start.elapsed()))
            } else {
                wait_duration
            };
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

    fn check_append_budget(
        native: bool,
        started: std::time::Instant,
        timeout: Duration,
    ) -> Result<(), WalAppendError> {
        if native && started.elapsed() >= timeout {
            return Err(WalAppendError::unchanged(MidgeError::Timeout(format!(
                "WAL append deadline expired before queue submission after {timeout:?}"
            ))));
        }
        Ok(())
    }
}

impl WalWriter for FsWalWriterIo {
    fn supports_sequenced_submission(&self) -> bool {
        true
    }

    fn submit_sequenced_batch(
        &self,
        prefix: crate::wal::append_completion::WalAppendPrefix,
        units: Vec<crate::wal::sequenced::SequencedWalUnit>,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> Option<Result<crate::wal::append_completion::WalPendingAppend, WalAppendError>> {
        Some(self.submit_owned_units(prefix, units, timeout, group))
    }

    fn supports_sync_submission(&self) -> bool {
        true
    }

    fn submit_sync_prefix(
        &self,
        prefix: crate::wal::traits::WalSyncPrefix,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> Option<MidgeResult<crate::wal::traits::WalPendingSync>> {
        Some(self.enqueue_sync_prefix(prefix, timeout, group))
    }

    fn submit_footer(
        &self,
        operation_id: u64,
        facts: &SealedFacts,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> Option<MidgeResult<crate::wal::footer_completion::WalPendingFooter>> {
        Some(self.submit_footer_operation(operation_id, facts, timeout, group))
    }

    fn set_write_authority(&self, authority: Option<crate::wal::traits::WriteAuthority>) {
        *self.write_authority.write() = authority;
    }

    fn writer_group_capability(&self) -> Option<&dyn GroupedWalWriter> {
        self.write_authority
            .read()
            .as_ref()
            .and_then(|authority| authority.groups())
            .map(|_| self as &dyn GroupedWalWriter)
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

    fn append_batch_accounted(&self, records: &[WalRecord]) -> Result<WalPos, WalAppendError> {
        self.append_batch_with_group(records, None)
    }

    fn sync_with_timeout(&self, timeout: Duration) -> MidgeResult<()> {
        self.sync_with_group(timeout, None)
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

impl GroupedWalWriter for FsWalWriterIo {
    fn begin_group(&self, timeout: Duration) -> MidgeResult<crate::lease::WriterEffectGroup> {
        let authority = self.write_authority.read().clone();
        authority
            .as_ref()
            .and_then(|authority| authority.groups())
            .ok_or_else(|| MidgeError::Internal("WAL writer has no native group authority".into()))?
            .begin_group(timeout)
    }
    fn append_batch_in_group(
        &self,
        records: &[WalRecord],
        group: &crate::lease::WriterEffectGroup,
    ) -> Result<WalPos, WalAppendError> {
        self.append_batch_with_group(records, Some(group))
    }
    fn sync_in_group(
        &self,
        timeout: Duration,
        group: &crate::lease::WriterEffectGroup,
    ) -> MidgeResult<()> {
        self.sync_with_group(timeout, Some(group))
    }
}

impl FsWalWriterIo {
    fn enqueue_sync_prefix(
        &self,
        prefix: crate::wal::traits::WalSyncPrefix,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<crate::wal::traits::WalPendingSync> {
        let started = std::time::Instant::now();
        if let Some(error) = self.writer_failure() {
            return Err(error);
        }
        if prefix.writer_epoch != self.header.epoch
            || prefix.segment_id != self.header.id
            || prefix.first_sequence > prefix.last_sequence
            || prefix.end_offset < HEADER_LEN as u64
            || prefix.end_offset != self.current_pos()
            || prefix.last_sequence
                != self
                    .tail
                    .lock()
                    .last_seq
                    .unwrap_or(self.header.prev.last_seq)
        {
            return Err(MidgeError::InvalidArgument(
                "WAL sync prefix does not match the acknowledged segment tail".into(),
            ));
        }
        let authority = self.write_authority.read().clone();
        let (effect, root) = PendingWriterEffect::admit(authority, group, timeout)?;
        let (sender, response) = std::sync::mpsc::sync_channel(1);
        {
            let _queue = self.queue.lock();
            let mut state = self.sync_state.lock();
            if let Some(error) = state.failure() {
                return Err(error);
            }
            if started.elapsed() >= timeout {
                return Err(MidgeError::Timeout(
                    "WAL sync deadline expired before submission".into(),
                ));
            }
            if state.requests.len() >= MAX_PENDING_SYNC_REQUESTS {
                return Err(MidgeError::WriteStall(
                    "WAL sync request queue is full".into(),
                ));
            }
            if state.submitted_writes != state.completed_writes {
                return Err(MidgeError::WriteStall(
                    "WAL sync prefix awaits an append acknowledgement".into(),
                ));
            }
            let id = state.pending_fsyncs.checked_add(1).ok_or_else(|| {
                MidgeError::ResourceLimit("WAL sync barrier identity space exhausted".into())
            })?;
            state.pending_fsyncs = id;
            let writes_through = state.submitted_writes;
            state.requests.push_back(super::writer_runner::PendingSync {
                id,
                writes_through,
                effect,
                completion: Some(super::writer_runner::PendingSyncCompletion { prefix, sender }),
            });
        }
        self.queue_cond.notify_one();
        let failure_state = Arc::clone(&self.sync_state);
        Ok(crate::wal::traits::WalPendingSync::new(
            response,
            Arc::new(move || failure_state.lock().failure()),
            root,
        ))
    }

    fn append_batch_with_group(
        &self,
        records: &[WalRecord],
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> Result<WalPos, WalAppendError> {
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
        let batch_start = self.enqueue_in_group(buf, self.io_timeout, group)?;
        self.count_records(records);
        Ok(batch_start.saturating_add(last_record_offset))
    }

    fn sync_with_group(
        &self,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<()> {
        if let Some(result) = self.sync_acknowledged_prefix(timeout, group) {
            return result;
        }
        if let Some(error) = self.writer_failure() {
            return Err(error);
        }
        let sync_start = std::time::Instant::now();
        let authority = self.write_authority.read().clone();
        let (effect, root) = PendingWriterEffect::admit(authority, group, timeout)?;
        let native = group.is_some() || root.is_some();
        // Request a durable fsync and wait for writer to perform it.
        // Note: we MUST request fsync even if queue is empty, because data may have
        // been written to the file but not yet fsynced (the writer thread writes
        // asynchronously).
        let (my_sync_id, pending) =
            self.enqueue_legacy_sync(effect, sync_start, timeout, native)?;

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
        drop(s);
        pending
            .wait_with_timeout(timeout.saturating_sub(sync_start.elapsed()))
            .map(|_| ())
    }

    fn enqueue_legacy_sync(
        &self,
        effect: PendingWriterEffect,
        started: std::time::Instant,
        timeout: Duration,
        native: bool,
    ) -> MidgeResult<(u64, crate::wal::traits::WalPendingSync)> {
        let (first_sequence, last_sequence) = {
            let tail = self.tail.lock();
            (
                tail.first_seq.unwrap_or(0),
                tail.last_seq.unwrap_or(self.header.prev.last_seq),
            )
        };
        let (sender, response) = std::sync::mpsc::sync_channel(1);
        let my_sync_id = {
            let _queue = self.queue.lock();
            let mut s = self.sync_state.lock();
            if let Some(error) = s.failure() {
                return Err(error);
            }
            if native && started.elapsed() >= timeout {
                return Err(MidgeError::Timeout(format!(
                    "WAL sync deadline expired before queue submission after {timeout:?}"
                )));
            }
            s.pending_fsyncs = s.pending_fsyncs.saturating_add(1);
            let request = super::writer_runner::PendingSync {
                id: s.pending_fsyncs,
                writes_through: s.submitted_writes,
                effect,
                completion: Some(super::writer_runner::PendingSyncCompletion {
                    prefix: crate::wal::traits::WalSyncPrefix {
                        barrier_id: s.pending_fsyncs,
                        writer_epoch: self.header.epoch,
                        segment_id: self.header.id,
                        first_sequence,
                        last_sequence,
                        end_offset: self.current_pos(),
                    },
                    sender,
                }),
            };
            s.requests.push_back(request);
            s.pending_fsyncs
        };
        // Wake writer so it can perform fsync
        self.queue_cond.notify_one();

        let failure_state = Arc::clone(&self.sync_state);
        let pending = crate::wal::traits::WalPendingSync::new(
            response,
            Arc::new(move || failure_state.lock().failure()),
            None,
        );

        Ok((my_sync_id, pending))
    }

    /// Existing callers wait on the same typed operation that the event loop
    /// can park later. Concurrent raw appends retain the queued legacy path.
    fn sync_acknowledged_prefix(
        &self,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> Option<MidgeResult<()>> {
        let started = std::time::Instant::now();
        {
            let state = self.sync_state.lock();
            if state.submitted_writes != state.completed_writes {
                return None;
            }
        }
        let (first_sequence, last_sequence) = {
            let tail = self.tail.lock();
            (
                tail.first_seq.unwrap_or(0),
                tail.last_seq.unwrap_or(self.header.prev.last_seq),
            )
        };
        let prefix = crate::wal::traits::WalSyncPrefix {
            barrier_id: self.sync_state.lock().pending_fsyncs.saturating_add(1),
            writer_epoch: self.header.epoch,
            segment_id: self.header.id,
            first_sequence,
            last_sequence,
            end_offset: self.current_pos(),
        };
        let pending = match self.submit_sync_prefix(prefix, timeout, group)? {
            Ok(pending) => pending,
            Err(MidgeError::WriteStall(_) | MidgeError::InvalidArgument(_)) => return None,
            Err(error) => return Some(Err(error)),
        };
        let result = pending.wait_with_timeout(timeout.saturating_sub(started.elapsed()));
        if let Err(error @ MidgeError::Timeout(_)) = &result {
            let mut state = self.sync_state.lock();
            state.sync_failed = true;
            state.last_sync_error = Some(error.replay());
            self.sync_cond.notify_all();
            self.queue_cond.notify_all();
        }
        Some(result.map(|_| ()))
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
    use crate::io::{Durability, File, FsResult, OpenOptions, ReadFs};
    use crate::lease::LeaderStore;
    use crate::wal::types::{WalOpKind, WalRecord};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;

    mod sequenced_submissions;
    mod writer_groups;

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
            tail: Arc::new(Mutex::new(SegmentTail::default())),
            raw_submission_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            raw_submission_memory: crate::common::resource_budget::ResourceBudget::new(
                64 * 1024 * 1024,
            ),
            digest: Arc::new(Mutex::new(SegmentDigest::of(&[]))),
            sealed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    struct SyncCountingFile {
        inner: Box<dyn File>,
        durable_sync_count: Arc<AtomicUsize>,
    }

    impl crate::io::ReadFile for SyncCountingFile {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            self.inner.read_at(offset, len)
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }
    }

    impl File for SyncCountingFile {
        fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
            self.inner.write_at(offset, data)
        }

        fn append(&mut self, data: Bytes) -> FsResult<u64> {
            self.inner.append(data)
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

    impl ReadFs for SyncCountingFs {
        fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
            self.open(path, OpenOptions::READ_ONLY)
                .map(|file| file as Box<dyn crate::io::ReadFile>)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }
    }

    impl crate::io::PersistentHandleFs for SyncCountingFs {
        fn open_persistent(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File>> {
            if let Some(gate) = &self.open_gate {
                gate.0.send(()).unwrap();
                gate.1.lock().unwrap().recv().unwrap();
            }
            Ok(Box::new(SyncCountingFile {
                inner: crate::io::open_persistent_handle(&self.inner, path, options)?,
                durable_sync_count: Arc::clone(&self.durable_sync_count),
            }))
        }
    }

    impl Fs for SyncCountingFs {
        fn try_lock_exclusive(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Box<dyn crate::io::FsLock>> {
            self.inner.try_lock_exclusive(path)
        }

        fn persistent_handle_capability(&self) -> Option<&dyn crate::io::PersistentHandleFs> {
            self.inner
                .persistent_handle_capability()
                .map(|_| self as &dyn crate::io::PersistentHandleFs)
        }

        fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.inner.open(path, options)
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
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

    impl crate::io::ReadFile for GatedNoSpaceFile {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            self.inner.read_at(offset, len)
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }
    }

    impl File for GatedNoSpaceFile {
        fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
            Err(self.fail_after_release())
        }

        fn append(&mut self, _data: Bytes) -> FsResult<u64> {
            Err(self.fail_after_release())
        }

        fn sync(&mut self, durability: Durability) -> FsResult<()> {
            self.inner.sync(durability)
        }
    }

    impl ReadFs for GatedNoSpaceFs {
        fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
            self.open(path, OpenOptions::READ_ONLY)
                .map(|file| file as Box<dyn crate::io::ReadFile>)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }
    }

    impl crate::io::PersistentHandleFs for GatedNoSpaceFs {
        fn open_persistent(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File>> {
            Ok(Box::new(GatedNoSpaceFile {
                inner: crate::io::open_persistent_handle(&self.inner, path, options)?,
                entered: self
                    .entered
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
                release: Arc::clone(&self.release),
            }))
        }
    }

    impl Fs for GatedNoSpaceFs {
        fn try_lock_exclusive(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Box<dyn crate::io::FsLock>> {
            self.inner.try_lock_exclusive(path)
        }

        fn persistent_handle_capability(&self) -> Option<&dyn crate::io::PersistentHandleFs> {
            self.inner
                .persistent_handle_capability()
                .map(|_| self as &dyn crate::io::PersistentHandleFs)
        }

        fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.inner.open(path, options)
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
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
        writer
            .append_record_accounted(&record)
            .map_err(|failure| failure.error)?;
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
        let failure = writer.append_record_accounted(&record).unwrap_err();

        // Assert
        assert!(matches!(
            failure.error,
            crate::common::MidgeError::Timeout(_)
        ));
        assert!(
            !failure.unchanged,
            "the queued append can still take effect"
        );
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "append ignored configured storage I/O timeout"
        );
    }

    #[test]
    fn should_report_timeout_when_submitted_sync_cannot_complete_within_budget() {
        // Arrange: no worker can acknowledge the admitted fsync request.
        let writer = writer_without_background_worker();

        // Act
        let result = writer.sync_with_timeout(Duration::from_millis(5));

        // Assert: expiry follows submission, and cannot claim durable success.
        assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
        assert_eq!(writer.sync_state.lock().pending_fsyncs, 1);
        assert!(matches!(
            writer.sync_with_timeout(Duration::from_secs(1)),
            Err(crate::common::MidgeError::Timeout(_))
        ));
    }

    #[test]
    fn should_sync_durably_when_legacy_skip_variable_is_set() -> MidgeResult<()> {
        // Arrange
        let _environment_guard = EnvVarGuard::set("MIDGE_SKIP_WAL_SYNC", "1");
        let fs = Arc::new(SyncCountingFs::default());
        let sync_count = Arc::clone(&fs.durable_sync_count);
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Act
        writer.sync_with_timeout(writer.io_timeout)?;

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
        writer
            .append_record_accounted(&record)
            .map_err(|failure| failure.error)?;
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
        let pos = writer
            .append_record_accounted(&record)
            .map_err(|failure| failure.error)?;

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
    fn should_match_sealed_identity_to_independent_written_segment_bytes() -> MidgeResult<()> {
        // Arrange: use a raw 4 KiB value so the production writer exercises
        // the borrowed WAL value path before final framing.
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
        let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
        let value: Vec<_> = (0_usize..4096)
            .map(|index| {
                let value = index.wrapping_mul(73).wrapping_add(index / 3);
                u8::try_from(value % 251).expect("fixture byte fits in u8")
            })
            .collect();
        assert_eq!(crate::codec::compress_wal_value(&value).1, None);
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"raw-value"),
            Some(Bytes::from(value)),
            7,
        );

        // Act: append and obtain the real writer's seal facts, then compute
        // the segment digest independently from the persisted prefix bytes.
        writer
            .append_record_accounted(&record)
            .map_err(|failure| failure.error)?;
        writer.sync_with_timeout(Duration::from_secs(5))?;
        let facts = writer.seal_facts(Duration::from_secs(5))?;
        let bytes = crate::io::read_all(fs.as_ref(), &FsPath::new("wal.log"))
            .map_err(crate::io::FsError::into_midge)?;
        let data_len =
            usize::try_from(facts.data_len).expect("segment prefix length fits in address space");
        let expected_digest = SegmentDigest::of(&bytes[..data_len]);

        // Assert: segment identity and length bind the exact independently
        // read header and frame bytes written through the production path.
        assert_eq!(data_len, bytes.len());
        assert_eq!(facts.link.id, writer.header.id);
        assert_eq!(facts.link.last_seq, record.seq);
        assert_eq!(facts.link.digest, expected_digest.value());
        assert_eq!(expected_digest.data_len(), facts.data_len);
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
            Some(Arc::new(move |operation: &mut dyn FnMut()| {
                check
                    .remaining(1)
                    .map(|_| ())
                    .map_err(|error| error.into_validation_error("physical WAL fsync authority"))?;
                operation();
                Ok(())
            })),
        )?;
        validity.expire_for_test();
        // Act
        let result = writer.sync_with_timeout(writer.io_timeout);
        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Fenced(_))));
        assert_eq!(fs.durable_sync_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn should_hold_filesystem_writer_fence_around_background_append() -> MidgeResult<()> {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
        let store = Arc::new(crate::lease::fs_leader_store::FsLeaderStore::new(
            Arc::clone(&fs),
            crate::lease::LeaseClock::default(),
        ));
        let predecessor = store.acquire_leadership("predecessor").unwrap();
        let predecessor_epoch = predecessor.epoch;
        create_test_segment(fs.as_ref(), "wal.log")?;
        let entered = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));
        let authority_store = Arc::clone(&store);
        let authority_entered = Arc::clone(&entered);
        let authority_resume = Arc::clone(&resume);
        let authority_calls = Arc::clone(&calls);
        let writer = Arc::new(FsWalWriterIo::new_with_authority(
            "wal.log",
            Arc::clone(&fs),
            Duration::from_secs(5),
            crate::telemetry::CounterSink::default(),
            Some(Arc::new(move |operation: &mut dyn FnMut()| {
                let call = authority_calls.fetch_add(1, Ordering::SeqCst);
                let mut run = || {
                    if call >= 2 {
                        authority_entered.wait();
                        authority_resume.wait();
                    }
                    operation();
                };
                authority_store
                    .with_writer_epoch(
                        "predecessor",
                        predecessor_epoch,
                        Duration::from_secs(1),
                        &mut run,
                    )
                    .map_err(|error| error.into_validation_error("background WAL append"))
            })),
        )?);
        let append_writer = Arc::clone(&writer);

        // Act: pause inside the actual background write while its epoch guard
        // is held, then try takeover from a second store handle.
        let append = std::thread::spawn(move || {
            append_writer.append_record_accounted(&WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"fenced-key"),
                Some(Bytes::from_static(b"value")),
                1,
            ))
        });
        entered.wait();
        let successor_attempt = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || store.acquire_leadership("successor"))
        };
        resume.wait();
        append
            .join()
            .expect("append thread completed")
            .map_err(|failure| failure.error)?;
        let successor = successor_attempt
            .join()
            .expect("successor attempt completed")
            .unwrap();

        // Assert
        assert!(successor.epoch > predecessor.epoch);
        drop(writer);
        Ok(())
    }

    #[test]
    fn should_complete_same_epoch_reservation_while_background_append_holds_native_fence(
    ) -> MidgeResult<()> {
        // Arrange: the engine's WAL writer and event loop share one filesystem
        // lease and can request effects concurrently in the same epoch.
        let directory = tempfile::tempdir().expect("temporary database root");
        let fs: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
        let store = Arc::new(crate::lease::fs_leader_store::FsLeaderStore::new(
            Arc::clone(&fs),
            crate::lease::LeaseClock::default(),
        ));
        let lease = store.acquire_leadership("holder").unwrap();
        let epoch = lease.epoch;
        let wal = crate::io::subdirectory(Arc::clone(&fs), crate::wal::WAL_DIR);
        let fence = crate::runtime::wal_history::WalHistoryFence::new(
            Arc::clone(&store) as Arc<dyn LeaderStore>,
            "holder".into(),
            epoch,
            Duration::from_secs(2),
        );
        let mut history = crate::runtime::wal_history::WalHistory::open_with_fence(
            Arc::clone(&wal),
            1,
            epoch,
            &fence,
        )?;
        let writer = FsWalWriterIo::new_with_counters(
            &crate::wal::segment_file_name(1),
            Arc::clone(&wal),
            Duration::from_secs(2),
            crate::telemetry::CounterSink::default(),
        )?;
        let entered = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let first_effect = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let authority_store = Arc::clone(&store);
        let authority_entered = Arc::clone(&entered);
        let authority_resume = Arc::clone(&resume);
        let authority_first_effect = Arc::clone(&first_effect);
        writer.set_write_authority(Some(Arc::new(move |operation: &mut dyn FnMut()| {
            let mut run = || {
                if authority_first_effect.swap(false, Ordering::AcqRel) {
                    authority_entered.wait();
                    authority_resume.wait();
                }
                operation();
            };
            authority_store
                .with_writer_epoch("holder", epoch, Duration::from_secs(2), &mut run)
                .map_err(|error| error.into_validation_error("background WAL append"))
        })));
        let writer = Arc::new(writer);
        let append_writer = Arc::clone(&writer);
        let append = std::thread::spawn(move || {
            append_writer.append_record_accounted(&WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"concurrent-key"),
                Some(Bytes::from_static(b"value")),
                1,
            ))
        });
        entered.wait();

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let reserve = std::thread::spawn(move || {
            started_tx.send(()).expect("signal reservation start");
            result_tx
                .send(history.reserve(10))
                .expect("send reservation result");
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reservation thread started");

        // Act: same-epoch effects share the continuously held native grant.
        // Reservation finishes before the background append is resumed.
        let early_result = result_rx.recv_timeout(Duration::from_secs(1));
        resume.wait();
        append
            .join()
            .expect("background append thread completed")
            .map_err(|failure| failure.error)?;
        reserve.join().expect("reservation thread completed");

        // Assert
        early_result.expect("reservation completed while append was paused")?;
        drop(writer);
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
            Some(Arc::new(move |operation: &mut dyn FnMut()| {
                check
                    .remaining(1)
                    .map(|_| ())
                    .map_err(|error| error.into_validation_error("queued WAL authority"))?;
                operation();
                Ok(())
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
                writer.append_record_accounted(&WalRecord::new(
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
