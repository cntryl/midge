//! Bounded upload and storage-event queues.

use super::{
    Duration, HashMap, Instant, StorageEvent, StorageOutcome, UploadState, UploadStatus, VecDeque,
    MAX_CONCURRENT_PRUNE_WORKERS, MAX_PENDING_PRUNE_REQUESTS, MAX_PENDING_STORAGE_EVENTS,
    MAX_PENDING_STORAGE_EVENT_BYTES, MAX_PENDING_WAL_UPLOADS, MAX_PENDING_WAL_UPLOAD_BYTES,
    STORAGE_CALLBACK_TIMEOUT, WAL_UPLOAD_WORKER_QUEUE_CAPACITY,
};

/// How long a failed WAL upload waits before its next attempt: `base_delay`
/// after the first failure, doubled per further failure, up to `max_delay`.
///
/// A segment that keeps failing therefore cannot spin. The production cap
/// stays short because callers and a shutdown drain wait on this path: a
/// longer one spends their budget waiting instead of retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalUploadRetryPolicy {
    pub(crate) base_delay: Duration,
    pub(crate) max_delay: Duration,
}

impl WalUploadRetryPolicy {
    /// The delay before the attempt that follows the `failures`-th failure.
    pub(crate) fn delay_after(self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(16);
        self.base_delay
            .saturating_mul(1 << doublings)
            .min(self.max_delay)
    }
}

/// WAL uploads that left the queue but whose WAL the writer still owes the
/// cloud: verified, yet not published, because an earlier upload has not
/// landed. They count against the queue's limits, so a segment that keeps
/// failing bounds the writes behind it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HeldUploads {
    pub(crate) entries: usize,
    pub(crate) bytes: u64,
}

impl Default for WalUploadRetryPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
        }
    }
}

impl UploadState {
    /// Record a failed attempt; the next one waits out `retry`'s delay.
    pub(super) fn record_failure(
        &mut self,
        error: String,
        now: Instant,
        retry: WalUploadRetryPolicy,
    ) {
        self.retries = self.retries.saturating_add(1);
        self.status = UploadStatus::Failed {
            error,
            retry_at: now + retry.delay_after(self.retries),
        };
    }

    /// Whether an attempt may start now.
    pub(super) fn is_due(&self, now: Instant) -> bool {
        match &self.status {
            UploadStatus::Pending => true,
            UploadStatus::Failed { retry_at, .. } => now >= *retry_at,
            UploadStatus::InFlight { .. } | UploadStatus::Completed => false,
        }
    }

    /// Whether the upload has left the queue's ownership.
    pub(super) fn is_finished(&self) -> bool {
        match &self.status {
            UploadStatus::Completed => true,
            UploadStatus::Pending | UploadStatus::InFlight { .. } | UploadStatus::Failed { .. } => {
                false
            }
        }
    }

    /// How long until this upload next needs a turn of the queue: `poll`
    /// while it waits to start or to finish, else until its retry is due.
    ///
    /// Never less than `poll`: a due retry the worker channel cannot take
    /// yet would otherwise wake the caller at once, again and again.
    pub(super) fn wake(&self, now: Instant, poll: Duration) -> Duration {
        match &self.status {
            UploadStatus::Failed { retry_at, .. } => {
                retry_at.saturating_duration_since(now).max(poll)
            }
            UploadStatus::Pending | UploadStatus::InFlight { .. } | UploadStatus::Completed => poll,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HybridQueueLimits {
    pub(crate) upload_entries: usize,
    pub(crate) upload_bytes: u64,
    pub(crate) worker_entries: usize,
    pub(crate) event_entries: usize,
    pub(crate) event_bytes: usize,
    pub(crate) prune_workers: usize,
    pub(crate) prune_requests: usize,
    pub(crate) callback_timeout: Duration,
    pub(crate) wal_upload_retry: WalUploadRetryPolicy,
}

impl Default for HybridQueueLimits {
    fn default() -> Self {
        Self {
            upload_entries: MAX_PENDING_WAL_UPLOADS,
            upload_bytes: MAX_PENDING_WAL_UPLOAD_BYTES,
            worker_entries: WAL_UPLOAD_WORKER_QUEUE_CAPACITY,
            event_entries: MAX_PENDING_STORAGE_EVENTS,
            event_bytes: MAX_PENDING_STORAGE_EVENT_BYTES,
            prune_workers: MAX_CONCURRENT_PRUNE_WORKERS,
            prune_requests: MAX_PENDING_PRUNE_REQUESTS,
            callback_timeout: STORAGE_CALLBACK_TIMEOUT,
            wal_upload_retry: WalUploadRetryPolicy::default(),
        }
    }
}

#[derive(Debug)]
pub(super) struct UploadQueue {
    pub(super) entries: VecDeque<UploadState>,
    pub(super) pending_bytes: u64,
    pub(super) max_entries: usize,
    pub(super) max_bytes: u64,
    pub(super) blocked_upload_bytes: Option<u64>,
    pub(super) retry: WalUploadRetryPolicy,
}

impl UploadQueue {
    pub(super) fn new(max_entries: usize, max_bytes: u64, retry: WalUploadRetryPolicy) -> Self {
        Self {
            entries: VecDeque::new(),
            pending_bytes: 0,
            max_entries,
            max_bytes,
            blocked_upload_bytes: None,
            retry,
        }
    }

    pub(super) fn try_push(&mut self, upload: UploadState) -> crate::common::MidgeResult<()> {
        self.ensure_capacity(upload.size_bytes)?;
        self.pending_bytes = self.pending_bytes.saturating_add(upload.size_bytes);
        self.entries.push_back(upload);
        Ok(())
    }

    pub(super) fn ensure_capacity(
        &mut self,
        additional_bytes: u64,
    ) -> crate::common::MidgeResult<()> {
        if self.entries.len() < self.max_entries
            && self.pending_bytes.saturating_add(additional_bytes) <= self.max_bytes
        {
            return Ok(());
        }
        self.blocked_upload_bytes = Some(additional_bytes);
        Err(crate::common::MidgeError::WriteStall(format!(
            "CloudAsync WAL upload queue at capacity: entries={}/{}, bytes={}/{}",
            self.entries.len(),
            self.max_entries,
            self.pending_bytes,
            self.max_bytes
        )))
    }

    pub(super) fn is_stalled(&self, held: HeldUploads) -> bool {
        self.blocked_upload_bytes.is_some()
            || self.entries.len().saturating_add(held.entries) >= self.max_entries
            || self.pending_bytes.saturating_add(held.bytes) >= self.max_bytes
    }

    /// How long until the queue next needs a turn: `poll` while an upload
    /// waits to start or to finish, else until its earliest retry is due.
    /// `None` when nothing is queued.
    pub(super) fn next_turn(&self, now: Instant, poll: Duration) -> Option<Duration> {
        self.entries
            .iter()
            .map(|upload| upload.wake(now, poll))
            .min()
    }

    /// Drop finished uploads. A failed upload stays queued for its retry:
    /// only success, or a failure no retry can fix ([`Self::abandon`]), ends
    /// it.
    pub(super) fn remove_terminal(&mut self) {
        self.retain_uploads(|upload| !upload.is_finished());
    }

    /// End `segment_id`'s upload after a failure no retry can fix.
    pub(super) fn abandon(&mut self, segment_id: u64) {
        self.retain_uploads(|upload| upload.segment_id != segment_id);
    }

    fn retain_uploads(&mut self, keep: impl Fn(&UploadState) -> bool) {
        let mut retained_bytes = 0u64;
        self.entries.retain(|upload| {
            let retain = keep(upload);
            if retain {
                retained_bytes = retained_bytes.saturating_add(upload.size_bytes);
            }
            retain
        });
        self.pending_bytes = retained_bytes;
        if self.blocked_upload_bytes.is_some_and(|additional_bytes| {
            self.entries.len() < self.max_entries
                && self.pending_bytes.saturating_add(additional_bytes) <= self.max_bytes
        }) {
            self.blocked_upload_bytes = None;
        }
    }
}

#[derive(Debug)]
pub(super) struct QueuedStorageEvent {
    pub(super) event: StorageEvent,
    pub(super) externally_delivered: bool,
    pub(super) accounted_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum TerminalEventKey {
    Upload(u64),
    Prune(u64),
}

#[derive(Debug)]
pub(super) struct BoundedEventQueue {
    /// Advisory/transient state changes. These may be coalesced independently
    /// from terminal completions.
    pub(super) entries: VecDeque<QueuedStorageEvent>,
    pub(super) pending_bytes: usize,
    /// One terminal result per admitted operation. A completion must not be
    /// lost merely because the transient queue is saturated.
    pub(super) terminal_entries: HashMap<TerminalEventKey, QueuedStorageEvent>,
    pub(super) terminal_pending_bytes: usize,
    pub(super) max_entries: usize,
    pub(super) max_bytes: usize,
}

impl BoundedEventQueue {
    pub(super) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            pending_bytes: 0,
            terminal_entries: HashMap::new(),
            terminal_pending_bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn terminal_key(event: &StorageEvent) -> Option<TerminalEventKey> {
        match event {
            StorageEvent::CloudAck { segment_id, .. }
            | StorageEvent::CloudFail { segment_id, .. } => {
                Some(TerminalEventKey::Upload(*segment_id))
            }
            StorageEvent::CloudWalPruneComplete { segment_id, .. }
            | StorageEvent::CloudWalPruneAttemptFailed { segment_id, .. } => {
                Some(TerminalEventKey::Prune(*segment_id))
            }
            _ => None,
        }
    }

    pub(super) fn event_bytes(event: &StorageEvent) -> usize {
        let dynamic = match event {
            StorageEvent::WriteComplete { key, result }
            | StorageEvent::DeleteComplete { key, result } => {
                key.len()
                    + match result {
                        StorageOutcome::Ok(()) => 0,
                        StorageOutcome::Err(error) => error.message().len(),
                    }
            }
            StorageEvent::HeadComplete { key, result } => {
                key.len()
                    + match result {
                        StorageOutcome::Ok(metadata) => metadata.etag.len() + 24,
                        StorageOutcome::Err(error) => error.message().len(),
                    }
            }
            StorageEvent::CloudFail { error, .. }
            | StorageEvent::CloudWalPruneAttemptFailed { error, .. } => error.len(),
            StorageEvent::CloudWalPruneComplete { result, .. } => match result {
                StorageOutcome::Ok(()) => 0,
                StorageOutcome::Err(error) => error.message().len(),
            },
            StorageEvent::CloudAck { .. }
            | StorageEvent::BackpressureOn
            | StorageEvent::BackpressureOff => 0,
        };
        std::mem::size_of::<StorageEvent>().saturating_add(dynamic)
    }

    pub(super) fn try_push(
        &mut self,
        event: StorageEvent,
        externally_delivered: bool,
    ) -> Result<(), crate::storage::StorageError> {
        let accounted_bytes = Self::event_bytes(&event);
        if let Some(key) = Self::terminal_key(&event) {
            let replaced = self.terminal_entries.remove(&key);
            if let Some(replaced) = replaced.as_ref() {
                self.terminal_pending_bytes = self
                    .terminal_pending_bytes
                    .saturating_sub(replaced.accounted_bytes);
            }
            if self.terminal_entries.len() >= self.max_entries
                || self.terminal_pending_bytes.saturating_add(accounted_bytes) > self.max_bytes
            {
                if let Some(replaced) = replaced {
                    self.terminal_pending_bytes = self
                        .terminal_pending_bytes
                        .saturating_add(replaced.accounted_bytes);
                    self.terminal_entries.insert(key, replaced);
                }
                return Err(crate::storage::StorageError::io(format!(
                    "terminal storage event queue at capacity: entries={}/{}, bytes={}/{}",
                    self.terminal_entries.len(),
                    self.max_entries,
                    self.terminal_pending_bytes,
                    self.max_bytes
                )));
            }
            self.terminal_pending_bytes =
                self.terminal_pending_bytes.saturating_add(accounted_bytes);
            self.terminal_entries.insert(
                key,
                QueuedStorageEvent {
                    event,
                    externally_delivered,
                    accounted_bytes,
                },
            );
            return Ok(());
        }

        if matches!(
            event,
            StorageEvent::BackpressureOn | StorageEvent::BackpressureOff
        ) {
            self.retain_non_backpressure();
        }

        if self.entries.len() >= self.max_entries
            || self.pending_bytes.saturating_add(accounted_bytes) > self.max_bytes
        {
            return Err(crate::storage::StorageError::io(format!(
                "storage event queue at capacity: entries={}/{}, bytes={}/{}",
                self.entries.len(),
                self.max_entries,
                self.pending_bytes,
                self.max_bytes
            )));
        }
        self.pending_bytes = self.pending_bytes.saturating_add(accounted_bytes);
        self.entries.push_back(QueuedStorageEvent {
            event,
            externally_delivered,
            accounted_bytes,
        });
        Ok(())
    }

    fn retain_non_backpressure(&mut self) {
        self.entries.retain(|queued| {
            !matches!(
                queued.event,
                StorageEvent::BackpressureOn | StorageEvent::BackpressureOff
            )
        });
        self.pending_bytes = self
            .entries
            .iter()
            .map(|queued| queued.accounted_bytes)
            .sum();
    }

    pub(super) fn drain(&mut self) -> Vec<QueuedStorageEvent> {
        self.pending_bytes = 0;
        self.terminal_pending_bytes = 0;
        let mut drained = Vec::with_capacity(
            self.terminal_entries
                .len()
                .saturating_add(self.entries.len()),
        );
        drained.extend(self.terminal_entries.drain().map(|(_, queued)| queued));
        drained.extend(self.entries.drain(..));
        drained
    }

    pub(super) fn pending_prune_completions(&self) -> usize {
        self.terminal_entries
            .keys()
            .filter(|key| matches!(key, TerminalEventKey::Prune(_)))
            .count()
    }
}
