use crate::io::{Durability, Fs, FsPath, FsResult};
use crate::wal::format::SegmentDigest;
use crate::wal::traits::WalAppendError;
use crate::wal::writer_authority::{ActiveRequestEffect, PendingWriterEffect};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct SyncState {
    pub(crate) submitted_writes: u64,
    pub(crate) completed_writes: u64,
    pub(crate) requests: std::collections::VecDeque<PendingSync>,
    pub pending_flushes: u64,
    pub pending_fsyncs: u64,
    pub completed_flushes: u64,
    pub completed_fsyncs: u64,
    /// Set to true when persistent write failures occur
    pub write_failed: bool,
    /// Set to true when persistent fsync failures occur
    pub sync_failed: bool,
    /// The failure that set `write_failed`, replayed to every later caller.
    pub last_write_error: Option<crate::common::MidgeError>,
    /// The failure that set `sync_failed`, replayed to every later caller.
    pub last_sync_error: Option<crate::common::MidgeError>,
}

pub(crate) struct PendingSync {
    pub(crate) id: u64,
    pub(crate) writes_through: u64,
    pub(crate) effect: PendingWriterEffect,
    pub(crate) completion: Option<PendingSyncCompletion>,
}

pub(crate) struct PendingSyncCompletion {
    pub(crate) prefix: crate::wal::traits::WalSyncPrefix,
    pub(crate) sender: std::sync::mpsc::SyncSender<
        crate::common::MidgeResult<crate::wal::traits::WalSyncCompletion>,
    >,
}

impl SyncState {
    /// The sticky failure, if any, as an error with its original class.
    pub(crate) fn failure(&self) -> Option<crate::common::MidgeError> {
        let replay = |error: &Option<crate::common::MidgeError>, fallback: &str| {
            error.as_ref().map_or_else(
                || crate::common::MidgeError::Internal(fallback.to_string()),
                crate::common::MidgeError::replay,
            )
        };
        if self.write_failed {
            return Some(replay(
                &self.last_write_error,
                "WAL write failed persistently",
            ));
        }
        if self.sync_failed {
            return Some(replay(
                &self.last_sync_error,
                "WAL sync failed persistently",
            ));
        }
        None
    }
}

/// A WAL writer failure with context, keeping the class of the filesystem
/// error that caused it. The writer never reports `Fenced`: lost authority is
/// the lease layer's call, not the disk's.
pub(crate) fn writer_error(
    context: impl std::fmt::Display,
    source: &crate::io::FsError,
) -> crate::common::MidgeError {
    use crate::common::MidgeError;
    use crate::io::FsError;
    let message = format!("{context}: {source}");
    match source {
        FsError::NoSpace(_) => MidgeError::NoSpace(message),
        FsError::Timeout(_) => MidgeError::Timeout(message),
        _ => MidgeError::Internal(message),
    }
}

pub(crate) struct QueuedWrite {
    pub(crate) data: Vec<u8>,
    ack: WriteAcknowledgement,
    raw: Option<Vec<crate::wal::sequenced::SequencedWalUnit>>,
    encoded_len: usize,
    last_record_offset: u64,
    service_elapsed: Duration,
    _reservation: Option<crate::common::resource_budget::ResourceReservation>,
    effect: Option<PendingWriterEffect>,
    id: u64,
}

enum WriteAcknowledgement {
    Legacy(std::sync::mpsc::SyncSender<Result<u64, WalAppendError>>),
    Sequenced {
        prefix: crate::wal::append_completion::WalAppendPrefix,
        sender: std::sync::mpsc::SyncSender<crate::wal::append_completion::AppendResult>,
    },
    Footer {
        prefix: crate::wal::footer_completion::WalFooterPrefix,
        sender: std::sync::mpsc::SyncSender<
            crate::common::MidgeResult<crate::wal::footer_completion::WalFooterCompletion>,
        >,
    },
}

impl QueuedWrite {
    #[cfg(test)]
    pub(crate) fn new(
        data: Vec<u8>,
        ack: std::sync::mpsc::SyncSender<Result<u64, WalAppendError>>,
    ) -> Self {
        let (effect, _) =
            PendingWriterEffect::admit(None, None, std::time::Duration::from_secs(1)).unwrap();
        Self::admitted(data, ack, effect, 0)
    }

    pub(crate) fn admitted(
        data: Vec<u8>,
        ack: std::sync::mpsc::SyncSender<Result<u64, WalAppendError>>,
        effect: PendingWriterEffect,
        id: u64,
    ) -> Self {
        Self {
            encoded_len: data.len(),
            data,
            ack: WriteAcknowledgement::Legacy(ack),
            raw: None,
            last_record_offset: 0,
            service_elapsed: Duration::ZERO,
            _reservation: None,
            effect: Some(effect),
            id,
        }
    }

    pub(crate) fn sequenced(
        units: Vec<crate::wal::sequenced::SequencedWalUnit>,
        completion: (
            crate::wal::append_completion::WalAppendPrefix,
            std::sync::mpsc::SyncSender<crate::wal::append_completion::AppendResult>,
        ),
        effect: PendingWriterEffect,
        id: u64,
        reservation: crate::common::resource_budget::ResourceReservation,
    ) -> Self {
        Self {
            data: Vec::new(),
            ack: WriteAcknowledgement::Sequenced {
                prefix: completion.0,
                sender: completion.1,
            },
            raw: Some(units),
            encoded_len: 0,
            last_record_offset: 0,
            service_elapsed: Duration::ZERO,
            _reservation: Some(reservation),
            effect: Some(effect),
            id,
        }
    }

    pub(crate) fn footer(
        data: Vec<u8>,
        prefix: crate::wal::footer_completion::WalFooterPrefix,
        sender: std::sync::mpsc::SyncSender<
            crate::common::MidgeResult<crate::wal::footer_completion::WalFooterCompletion>,
        >,
        effect: PendingWriterEffect,
        id: u64,
        reservation: crate::common::resource_budget::ResourceReservation,
    ) -> Self {
        Self {
            encoded_len: data.len(),
            data,
            ack: WriteAcknowledgement::Footer { prefix, sender },
            raw: None,
            last_record_offset: 0,
            service_elapsed: Duration::ZERO,
            _reservation: Some(reservation),
            effect: Some(effect),
            id,
        }
    }

    fn fail(&self, error: WalAppendError) {
        match &self.ack {
            WriteAcknowledgement::Legacy(sender) => {
                let _ = sender.send(Err(error));
            }
            WriteAcknowledgement::Sequenced { sender, .. } => {
                let _ = sender.send(Err(error));
            }
            WriteAcknowledgement::Footer { sender, .. } => {
                let _ = sender.send(Err(error.error));
            }
        }
    }
}

#[derive(Debug)]
struct WriteFailure {
    error: crate::common::MidgeError,
    unchanged: bool,
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
    #[cfg(test)]
    pub(crate) write_authority:
        Arc<parking_lot::RwLock<Option<crate::wal::traits::WriteAuthority>>>,
    pub fs: Arc<dyn Fs>,
    pub path: FsPath,
    pub queue: Arc<Mutex<Vec<QueuedWrite>>>,
    pub queue_cond: Arc<Condvar>,
    pub buf_pool: Arc<Mutex<Vec<Vec<u8>>>>,
    pub sync_state: Arc<Mutex<SyncState>>,
    pub sync_cond: Arc<Condvar>,
    pub current_pos: Arc<std::sync::atomic::AtomicU64>,
    pub shutdown: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) counters: crate::telemetry::CounterSink,
    pub(crate) digest: Arc<Mutex<SegmentDigest>>,
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
        match crate::io::open_persistent_handle(
            self.config.fs.as_ref(),
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
                let start_pos = match self.write_batch(&mut file_opt, &mut batch) {
                    Ok(start_pos) => start_pos,
                    Err(message) => {
                        self.fail_writes(&batch, &message);
                        return;
                    }
                };
                let batch_len = batch.iter().map(|entry| entry.encoded_len).sum::<usize>() as u64;
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
        if self.config.sync_state.lock().failure().is_some() {
            return None;
        }
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

        let count = q.first().map_or(0, |first| {
            q.iter()
                .take_while(|entry| {
                    first
                        .effect
                        .as_ref()
                        .expect("queued write retains its unactivated authority admission")
                        .same_authority(
                            entry
                                .effect
                                .as_ref()
                                .expect("queued write retains its unactivated authority admission"),
                        )
                })
                .count()
        });
        Some(q.drain(..count).collect())
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
    ) -> Result<u64, WriteFailure> {
        let service_start = Instant::now();
        let effects = batch
            .iter_mut()
            .map(|entry| {
                entry
                    .effect
                    .take()
                    .expect("drained write activates its authority admission exactly once")
                    .activate()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| WriteFailure {
                error,
                unchanged: true,
            })?;
        self.encode_sequenced_batch(batch)
            .map_err(|error| WriteFailure {
                error,
                unchanged: true,
            })?;
        let big_bytes = bytes::Bytes::from(Self::coalesce_batch(batch));
        let write_start = Instant::now();
        let start_pos = self.append_with_effects(file_opt, &big_bytes, &effects)?;
        let write_elapsed = write_start.elapsed();

        self.config.counters.record(|m| {
            m.record_wal_write_syscall(u64::try_from(write_elapsed.as_nanos()).unwrap_or(u64::MAX));
        });

        self.config.current_pos.store(
            start_pos.saturating_add(big_bytes.len() as u64),
            std::sync::atomic::Ordering::SeqCst,
        );
        let elapsed = service_start.elapsed();
        for entry in batch {
            entry.service_elapsed = elapsed;
        }
        Ok(start_pos)
    }

    fn encode_sequenced_batch(&self, batch: &mut [QueuedWrite]) -> crate::common::MidgeResult<()> {
        for entry in batch {
            let Some(units) = entry.raw.take() else {
                continue;
            };
            crate::failpoints::fail_point!("midge::wal::before_writer_encode");
            let started = Instant::now();
            entry.data = self.config.buf_pool.lock().pop().unwrap_or_default();
            entry.data.clear();
            for unit in units {
                entry.last_record_offset = entry.data.len() as u64;
                let record = unit.encode_record()?;
                crate::wal::frame::append_frame_encoded(&mut entry.data, |payload| {
                    crate::wal::encoding::encode_into(&record, payload)
                })?;
            }
            entry.encoded_len = entry.data.len();
            self.config.counters.record(|metrics| {
                metrics.record_wal_encode(
                    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                );
            });
        }
        Ok(())
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

    #[cfg(test)]
    fn append_once<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
        big_bytes: &bytes::Bytes,
    ) -> Result<u64, WriteFailure> {
        let (effect, _root) = PendingWriterEffect::admit(
            self.config.write_authority.read().clone(),
            None,
            std::time::Duration::from_secs(1),
        )
        .map_err(|error| WriteFailure {
            error,
            unchanged: true,
        })?;
        let effect = effect.activate().map_err(|error| WriteFailure {
            error,
            unchanged: true,
        })?;
        self.append_with_effects(file_opt, big_bytes, &[effect])
    }

    fn append_with_effects<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
        big_bytes: &bytes::Bytes,
        effects: &[ActiveRequestEffect],
    ) -> Result<u64, WriteFailure> {
        let start_pos = self
            .config
            .current_pos
            .load(std::sync::atomic::Ordering::SeqCst);
        self.ensure_file_handle(file_opt)
            .map_err(|message| WriteFailure {
                error: crate::common::MidgeError::Internal(message),
                unchanged: true,
            })?;

        if let Some(file) = file_opt.as_mut() {
            let write_result = Self::run_with_effects(effects, || {
                if crate::failpoints::is_active("midge::wal::partial_write_then_no_space") {
                    let partial_len = (big_bytes.len() / 2).max(1);
                    file.write_at(start_pos, big_bytes.slice(..partial_len))
                        .and_then(|()| {
                            Err(crate::io::FsError::NoSpace(
                                "after partial positional WAL write".to_string(),
                            ))
                        })
                } else {
                    file.write_at(start_pos, big_bytes.clone())
                }
            })
            .map_err(|error| WriteFailure {
                error: error.error,
                unchanged: !error.started,
            })?;
            return match write_result {
                Ok(()) => {
                    // The digest holds every byte written, in order: a
                    // write rolled back or never made is not in it.
                    let mut digest = self.config.digest.lock();
                    if digest.data_len() == start_pos {
                        digest.update(big_bytes);
                    }
                    Ok(start_pos)
                }
                Err(write_error) => {
                    tracing::error!(error = ?write_error, start_pos, "wal writer write_at failed");
                    Self::rollback_partial_append(file.as_mut(), start_pos, &write_error, effects)?;
                    Err(WriteFailure {
                        error: writer_error(
                            format!("wal writer write_at failed (partial WAL append rolled back to {start_pos})"),
                            &write_error,
                        ),
                        unchanged: true,
                    })
                }
            };
        }

        Err(WriteFailure {
            error: crate::common::MidgeError::Internal("wal writer has no file handle".to_string()),
            unchanged: true,
        })
    }

    fn rollback_partial_append(
        file: &mut dyn crate::io::File,
        start_pos: u64,
        write_error: &crate::io::FsError,
        effects: &[ActiveRequestEffect],
    ) -> Result<(), WriteFailure> {
        let truncate_result = Self::run_with_effects(effects, || file.truncate(start_pos))
            .map_err(|error| WriteFailure {
                error: error.error,
                unchanged: false,
            })?;
        if let Err(rollback_error) = truncate_result {
            return Err(WriteFailure {
                error: writer_error(
                    format!("wal writer write_at failed (partial WAL rollback to {start_pos} failed: {rollback_error})"),
                    write_error,
                ),
                unchanged: false,
            });
        }

        let sync_result = Self::run_with_effects(effects, || file.sync(Durability::Durable))
            .map_err(|error| WriteFailure {
                error: error.error,
                unchanged: false,
            })?;
        sync_result.map_err(|rollback_error| WriteFailure {
            error: writer_error(
                format!("wal writer write_at failed (partial WAL rollback to {start_pos} failed: {rollback_error})"),
                write_error,
            ),
            unchanged: false,
        })
    }

    fn run_with_effects<T>(
        effects: &[ActiveRequestEffect],
        operation: impl FnOnce() -> T,
    ) -> Result<T, crate::wal::writer_effect::WriterEffectError> {
        for effect in effects.iter().skip(1) {
            effect.validate()?;
        }
        effects
            .first()
            .expect("every physical request owns an authority admission")
            .run(operation)
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
        self.config.current_pos.store(
            start_pos.saturating_add(batch_len),
            std::sync::atomic::Ordering::SeqCst,
        );

        let queue = self.config.queue.lock();
        let mut s = self.config.sync_state.lock();
        s.completed_writes = batch.last().map_or(s.completed_writes, |entry| entry.id);
        if queue.is_empty() {
            s.completed_flushes = s.pending_flushes;
        }
        self.config.sync_cond.notify_all();
        drop(s);
        drop(queue);
        // The next loop turn may submit an exact-prefix sync as soon as it
        // observes this acknowledgement. Publish its completed-write proof
        // first, without requiring the caller to win a second worker race.
        Self::ack_batch_success(batch, start_pos);
    }

    fn complete_pending_flushes(&self) {
        if !self.has_pending_flushes() {
            return;
        }

        let queue = self.config.queue.lock();
        if !queue.is_empty() {
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
        let requests = self.take_ready_syncs()?;
        if requests.is_empty() {
            return Ok(());
        }
        let completed = requests
            .last()
            .expect("nonempty ready sync requests have a final barrier")
            .id;
        let (pending_effects, completions): (Vec<_>, Vec<_>) = requests
            .into_iter()
            .map(|request| (request.effect, request.completion))
            .unzip();
        let mut physical_completed_at = None;
        let result = pending_effects
            .into_iter()
            .map(PendingWriterEffect::activate)
            .collect::<Result<Vec<_>, _>>()
            .and_then(|effects| {
                let result = self.perform_physical_sync(file_opt, &effects);
                if result.is_ok() {
                    physical_completed_at = Some(Instant::now());
                }
                // A terminal receipt must not leave an active child behind
                // for the loop-owned lexical root to wait on.
                drop(effects);
                result
            });
        match &result {
            Ok(sync_elapsed) => {
                self.config.counters.record(|m| {
                    m.record_wal_fsync_ns(
                        u64::try_from(sync_elapsed.as_nanos()).unwrap_or(u64::MAX),
                    );
                    m.record_wal_fsync_count();
                });
                self.config.sync_state.lock().completed_fsyncs = completed;
                self.config.sync_cond.notify_all();
            }
            Err(error) => self.mark_sync_failure(error.replay()),
        }
        for completion in completions.into_iter().flatten() {
            let receipt = result
                .as_ref()
                .map(|elapsed| crate::wal::traits::WalSyncCompletion {
                    prefix: completion.prefix,
                    elapsed: *elapsed,
                    completed_at: physical_completed_at
                        .expect("successful physical sync records its completion instant"),
                })
                .map_err(crate::common::MidgeError::replay);
            let _ = completion.sender.send(receipt);
        }
        result.map(|_| ()).map_err(|_| ())
    }

    fn perform_physical_sync<'a>(
        &'a self,
        file_opt: &mut Option<Box<dyn crate::io::File + 'a>>,
        effects: &[ActiveRequestEffect],
    ) -> crate::common::MidgeResult<Duration> {
        if file_opt.is_none() {
            match self.open_file_handle() {
                Ok(file) => *file_opt = Some(file),
                Err(error) => {
                    tracing::error!(error = ?error, "WAL fsync failed before sync - marking sync as failed");
                    return Err(writer_error(
                        "wal writer could not reopen file handle for fsync",
                        &error,
                    ));
                }
            }
        }
        let sync_result = Self::run_with_effects(effects, || {
            if let Some(file) = file_opt.as_mut() {
                let sync_start = Instant::now();
                crate::failpoints::fail_point!("midge::wal::before_physical_sync");
                file.sync(Durability::Durable)
                    .map(|()| sync_start.elapsed())
            } else {
                Ok(Duration::ZERO)
            }
        });
        sync_result
            .map_err(|error| error.error)?
            .map_err(|error| writer_error("wal writer fsync failed", &error))
    }

    fn take_ready_syncs(&self) -> Result<Vec<PendingSync>, ()> {
        let mut state = self.config.sync_state.lock();
        if state.failure().is_some() {
            return Err(());
        }
        let count = state.requests.front().map_or(0, |first| {
            state
                .requests
                .iter()
                .take_while(|request| {
                    request.writes_through <= state.completed_writes
                        && first.effect.same_authority(&request.effect)
                })
                .count()
        });
        Ok(state.requests.drain(..count).collect())
    }

    fn mark_sync_failure(&self, error: crate::common::MidgeError) {
        let mut s = self.config.sync_state.lock();
        s.sync_failed = true;
        s.last_sync_error = Some(error);
        self.config.sync_cond.notify_all();
        self.config.queue_cond.notify_all();
    }

    fn ack_batch_success(batch: &[QueuedWrite], batch_start_pos: u64) {
        let mut next_pos = batch_start_pos;
        for entry in batch {
            let entry_pos = next_pos;
            next_pos = next_pos.saturating_add(entry.encoded_len as u64);
            match &entry.ack {
                WriteAcknowledgement::Legacy(sender) => {
                    let _ = sender.send(Ok(entry_pos));
                }
                WriteAcknowledgement::Sequenced { prefix, sender } => {
                    let _ = sender.send(Ok(crate::wal::append_completion::WalAppendCompletion {
                        prefix: *prefix,
                        start_offset: entry_pos,
                        last_record_offset: entry_pos.saturating_add(entry.last_record_offset),
                        end_offset: next_pos,
                        elapsed: entry.service_elapsed,
                    }));
                }
                WriteAcknowledgement::Footer { prefix, sender } => {
                    let result = if *prefix
                        == (crate::wal::footer_completion::WalFooterPrefix {
                            operation_id: prefix.operation_id,
                            writer_epoch: prefix.writer_epoch,
                            segment_id: prefix.segment_id,
                            start_offset: entry_pos,
                            end_offset: next_pos,
                        }) {
                        Ok(crate::wal::footer_completion::WalFooterCompletion {
                            prefix: *prefix,
                            elapsed: entry.service_elapsed,
                        })
                    } else {
                        Err(crate::common::MidgeError::Internal(
                            "WAL footer write did not match its admitted prefix".into(),
                        ))
                    };
                    let _ = sender.send(result);
                }
            }
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
            self.config.counters.record(|m| {
                m.record_wal_buffer_pool_overflow(dropped as u64);
            });
        }

        self.config.queue_cond.notify_all();
    }

    fn fail_writes(&self, batch: &[QueuedWrite], failure: &WriteFailure) {
        let mut state = self.config.sync_state.lock();
        state.write_failed = true;
        state.last_write_error = Some(failure.error.replay());
        drop(state);
        // Publish the terminal state before acknowledging any failed append.
        // No further writes from this batch can occur after this proof is sent.
        for entry in batch {
            entry.fail(WalAppendError {
                error: failure.error.replay(),
                unchanged: failure.unchanged,
            });
        }
        self.config.sync_cond.notify_all();
        self.config.queue_cond.notify_all();
    }
}

impl Drop for WriterRunner {
    fn drop(&mut self) {
        // This also runs on panic. Shared writer state may outlive its worker,
        // so every queued child must be released before a successor can enter.
        let mut queue = self.config.queue.lock();
        let mut state = self.config.sync_state.lock();
        let error = state.failure().unwrap_or_else(|| {
            crate::common::MidgeError::Internal(
                "WAL worker stopped before request completion".into(),
            )
        });
        state.write_failed = true;
        state.last_write_error = Some(error.replay());
        state.requests.clear();
        let pending = std::mem::take(&mut *queue);
        drop(state);
        drop(queue);
        for entry in pending {
            entry.fail(WalAppendError::unchanged(error.replay()));
        }
        self.config.sync_cond.notify_all();
        self.config.queue_cond.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::traits::{DirEntry, FsError, Metadata};
    use crate::io::{Fs, FsPath, FsResult, OpenOptions as FsOpenOptions, ReadFs};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::Duration;

    struct PartialWriteFile {
        physical_bytes: Arc<AtomicU64>,
        truncate_succeeds: bool,
        sync_succeeds: bool,
        expire_on_write: Option<Arc<crate::lease::LeaseValidity>>,
        expire_on_truncate: Option<Arc<crate::lease::LeaseValidity>>,
        sync_count: Arc<AtomicU64>,
    }

    impl crate::io::ReadFile for PartialWriteFile {
        fn read_at(&self, _offset: u64, _len: u64) -> FsResult<Bytes> {
            Ok(Bytes::new())
        }

        fn len(&self) -> FsResult<u64> {
            Ok(self
                .physical_bytes
                .load(std::sync::atomic::Ordering::SeqCst))
        }
    }

    impl crate::io::File for PartialWriteFile {
        fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
            self.physical_bytes.store(
                offset + data.len() as u64 / 2,
                std::sync::atomic::Ordering::SeqCst,
            );
            if let Some(validity) = &self.expire_on_write {
                // Represents successor bytes beyond the predecessor append position.
                self.physical_bytes
                    .store(1024, std::sync::atomic::Ordering::SeqCst);
                validity.expire_for_test();
            }
            Err(FsError::NoSpace("after partial write".into()))
        }
        fn truncate(&mut self, len: u64) -> FsResult<()> {
            if !self.truncate_succeeds {
                return Err(FsError::Io("truncate failed".into()));
            }
            self.physical_bytes
                .store(len, std::sync::atomic::Ordering::SeqCst);
            if let Some(validity) = &self.expire_on_truncate {
                validity.expire_for_test();
            }
            Ok(())
        }
        fn append(&mut self, _data: Bytes) -> FsResult<u64> {
            unreachable!("positional writer")
        }

        fn sync(&mut self, _durability: Durability) -> FsResult<()> {
            self.sync_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.sync_succeeds {
                Ok(())
            } else {
                Err(FsError::Io("rollback sync failed".into()))
            }
        }
    }

    #[test]
    fn should_stop_rollback_mutations_when_validity_expires_during_io() {
        for expire_during_write in [true, false] {
            // Arrange
            let validity = Arc::new(crate::lease::LeaseValidity::new());
            validity
                .activate(1, Instant::now() + Duration::from_mins(1))
                .unwrap();
            let check = Arc::clone(&validity);
            let runner = runner_with_sync_state(Arc::new(Mutex::new(SyncState::default())));
            *runner.config.write_authority.write() =
                Some(Arc::new(move |operation: &mut dyn FnMut()| {
                    check
                        .remaining(1)
                        .map(|_| ())
                        .map_err(|error| error.into_validation_error("rollback authority"))?;
                    operation();
                    Ok(())
                }));
            let physical_bytes = Arc::new(AtomicU64::new(0));
            let sync_count = Arc::new(AtomicU64::new(0));
            let mut file: Option<Box<dyn crate::io::File>> = Some(Box::new(PartialWriteFile {
                physical_bytes: Arc::clone(&physical_bytes),
                truncate_succeeds: true,
                sync_succeeds: true,
                expire_on_write: expire_during_write.then(|| Arc::clone(&validity)),
                expire_on_truncate: (!expire_during_write).then(|| Arc::clone(&validity)),
                sync_count: Arc::clone(&sync_count),
            }));
            // Act
            let failure = runner
                .append_once(&mut file, &Bytes::from_static(b"partial frame"))
                .unwrap_err();
            // Assert
            assert!(matches!(
                failure.error,
                crate::common::MidgeError::Fenced(_)
            ));
            assert!(
                !failure.unchanged,
                "rollback lacks a durable unchanged proof"
            );
            assert_eq!(sync_count.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(
                physical_bytes.load(std::sync::atomic::Ordering::SeqCst),
                if expire_during_write { 1024 } else { 0 }
            );
        }
    }

    #[test]
    fn should_prove_unchanged_append_only_after_durable_rollback() {
        for (truncate_succeeds, sync_succeeds) in [(true, true), (false, true), (true, false)] {
            // Arrange
            let physical_bytes = Arc::new(AtomicU64::new(0));
            let runner = runner_with_sync_state(Arc::new(Mutex::new(SyncState::default())));
            let mut file: Option<Box<dyn crate::io::File>> = Some(Box::new(PartialWriteFile {
                physical_bytes: Arc::clone(&physical_bytes),
                truncate_succeeds,
                sync_succeeds,
                expire_on_write: None,
                expire_on_truncate: None,
                sync_count: Arc::new(AtomicU64::new(0)),
            }));

            // Act
            let failure = runner
                .append_once(&mut file, &Bytes::from_static(b"partial frame"))
                .expect_err("injected partial append failure");

            // Assert
            assert_eq!(failure.unchanged, truncate_succeeds && sync_succeeds);
            assert_eq!(
                physical_bytes.load(std::sync::atomic::Ordering::SeqCst) == 0,
                truncate_succeeds
            );
            assert!(failure.error.to_string().contains("partial"));
        }
    }

    struct FailingOpenFs;

    struct NoSpaceSyncFile;

    impl crate::io::ReadFile for NoSpaceSyncFile {
        fn read_at(&self, _offset: u64, _len: u64) -> FsResult<Bytes> {
            Ok(Bytes::new())
        }

        fn len(&self) -> FsResult<u64> {
            Ok(0)
        }
    }

    impl crate::io::File for NoSpaceSyncFile {
        fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
            Ok(())
        }

        fn append(&mut self, _data: Bytes) -> FsResult<u64> {
            Ok(0)
        }

        fn sync(&mut self, _durability: Durability) -> FsResult<()> {
            Err(FsError::NoSpace("No space left on device".to_string()))
        }
    }

    impl ReadFs for FailingOpenFs {
        fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
            self.open(path, FsOpenOptions::READ_ONLY)
                .map(|file| file as Box<dyn crate::io::ReadFile>)
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

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            Err(FsError::Unavailable(format!(
                "list dir failed for {}",
                path.0
            )))
        }
    }

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

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(format!(
                "create dir failed for {}",
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
        {
            let mut state = sync_state.lock();
            if state.pending_fsyncs > state.completed_fsyncs {
                let (effect, _) =
                    PendingWriterEffect::admit(None, None, std::time::Duration::from_secs(1))
                        .unwrap();
                let request = PendingSync {
                    id: state.pending_fsyncs,
                    writes_through: state.completed_writes,
                    effect,
                    completion: None,
                };
                state.requests.push_back(request);
            }
        }
        let fs: Arc<dyn Fs> = Arc::new(FailingOpenFs);
        WriterRunner::new(WriterConfig {
            fs,
            write_authority: Arc::new(parking_lot::RwLock::new(None)),
            path: FsPath::new("wal.log"),
            queue: Arc::new(Mutex::new(Vec::new())),
            queue_cond: Arc::new(Condvar::new()),
            buf_pool: Arc::new(Mutex::new(Vec::new())),
            sync_state,
            sync_cond: Arc::new(Condvar::new()),
            current_pos: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
            counters: crate::telemetry::CounterSink::default(),
            digest: Arc::new(Mutex::new(SegmentDigest::of(&[]))),
        })
    }

    #[test]
    fn should_reject_physical_append_when_validity_expires_after_handle_preparation() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let mut runner = runner_with_sync_state(Arc::new(Mutex::new(SyncState::default())));
        drop(
            fs.open(
                &FsPath::new("wal.log"),
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: false,
                },
            )
            .unwrap(),
        );
        runner.config.fs = fs.clone();
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        validity
            .activate(1, Instant::now() + std::time::Duration::from_mins(1))
            .unwrap();
        let check = Arc::clone(&validity);
        *runner.config.write_authority.write() =
            Some(Arc::new(move |operation: &mut dyn FnMut()| {
                check
                    .remaining(1)
                    .map(|_| ())
                    .map_err(|error| error.into_validation_error("physical WAL authority"))?;
                operation();
                Ok(())
            }));
        let mut file = Some(runner.open_file_handle().unwrap());
        validity.expire_for_test();
        // Act
        let result = runner.append_once(&mut file, &bytes::Bytes::from_static(b"queued"));
        // Assert
        assert!(matches!(
            result,
            Err(WriteFailure {
                error: crate::common::MidgeError::Fenced(_),
                unchanged: true
            })
        ));
        assert_eq!(fs.metadata(&FsPath::new("wal.log")).unwrap().len, 0);
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
            .as_ref()
            .is_some_and(|error| error.to_string().contains("could not reopen file handle")));
    }

    #[test]
    fn should_identify_physical_fsync_no_space_as_wal_writer_failure() {
        // Arrange
        let sync_state = Arc::new(Mutex::new(SyncState {
            pending_fsyncs: 1,
            ..SyncState::default()
        }));
        let runner = runner_with_sync_state(Arc::clone(&sync_state));
        let mut file_opt: Option<Box<dyn crate::io::File>> = Some(Box::new(NoSpaceSyncFile));

        // Act
        let result = runner.complete_pending_fsyncs(&mut file_opt);

        // Assert
        assert!(result.is_err());
        let state = sync_state.lock();
        let error = state
            .last_sync_error
            .as_ref()
            .expect("physical fsync error should be retained");
        assert!(error.to_string().contains("wal writer fsync failed"));
        assert!(
            matches!(error, crate::common::MidgeError::NoSpace(_)),
            "{error:?}"
        );
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
