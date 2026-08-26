//! Bounded upload and storage-event queues.

use super::{
    Duration, HashMap, StorageEvent, StorageOutcome, UploadState, UploadStatus, VecDeque,
    MAX_CONCURRENT_PRUNE_WORKERS, MAX_PENDING_PRUNE_REQUESTS, MAX_PENDING_STORAGE_EVENTS,
    MAX_PENDING_STORAGE_EVENT_BYTES, MAX_PENDING_WAL_UPLOADS, MAX_PENDING_WAL_UPLOAD_BYTES,
    STORAGE_CALLBACK_TIMEOUT, WAL_UPLOAD_WORKER_QUEUE_CAPACITY,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct HybridQueueLimits {
    pub(super) upload_entries: usize,
    pub(super) upload_bytes: u64,
    pub(super) worker_entries: usize,
    pub(super) event_entries: usize,
    pub(super) event_bytes: usize,
    pub(super) prune_workers: usize,
    pub(super) prune_requests: usize,
    pub(super) callback_timeout: Duration,
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
}

impl UploadQueue {
    pub(super) fn new(max_entries: usize, max_bytes: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            pending_bytes: 0,
            max_entries,
            max_bytes,
            blocked_upload_bytes: None,
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

    pub(super) fn is_stalled(&self) -> bool {
        self.blocked_upload_bytes.is_some()
            || self.entries.len() >= self.max_entries
            || self.pending_bytes >= self.max_bytes
    }

    pub(super) fn remove_terminal(&mut self) {
        let mut retained_bytes = 0u64;
        self.entries.retain(|upload| {
            let retain = match &upload.status {
                UploadStatus::Completed => false,
                UploadStatus::Failed { .. } => upload.retries < 3,
                UploadStatus::Pending | UploadStatus::InFlight { .. } => true,
            };
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
            StorageEvent::ReadComplete { key, result } => {
                key.len()
                    + match result {
                        StorageOutcome::Ok(data) => data.len(),
                        StorageOutcome::Err(error) => error.len(),
                    }
            }
            StorageEvent::WriteComplete { key, result }
            | StorageEvent::DeleteComplete { key, result } => {
                key.len()
                    + match result {
                        StorageOutcome::Ok(()) => 0,
                        StorageOutcome::Err(error) => error.len(),
                    }
            }
            #[cfg(test)]
            StorageEvent::ListComplete { prefix, result } => {
                prefix.len()
                    + match result {
                        StorageOutcome::Ok(keys) => keys.iter().map(String::len).sum(),
                        StorageOutcome::Err(error) => error.len(),
                    }
            }
            StorageEvent::HeadComplete { key, result } => {
                key.len()
                    + match result {
                        StorageOutcome::Ok(metadata) => metadata.etag.len() + 24,
                        StorageOutcome::Err(error) => error.len(),
                    }
            }
            StorageEvent::CloudFail { error, .. } => error.len(),
            StorageEvent::CloudWalPruneComplete { result, .. } => match result {
                StorageOutcome::Ok(()) => 0,
                StorageOutcome::Err(error) => error.len(),
            },
            StorageEvent::CloudWalPruneAttemptFailed { error, .. } => error.len(),
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
    ) -> Result<(), String> {
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
                return Err(format!(
                    "terminal storage event queue at capacity: entries={}/{}, bytes={}/{}",
                    self.terminal_entries.len(),
                    self.max_entries,
                    self.terminal_pending_bytes,
                    self.max_bytes
                ));
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
            return Err(format!(
                "storage event queue at capacity: entries={}/{}, bytes={}/{}",
                self.entries.len(),
                self.max_entries,
                self.pending_bytes,
                self.max_bytes
            ));
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
