//! Hybrid storage backend - combines local and cloud storage
//!
//! CRITICAL ARCHITECTURE:
//!
//! `HybridStorage` has TWO FORMAT-NEUTRAL ROLES:
//!
//! 1. OBJECT STORAGE:
//!    - `submit_read/write/delete/list` for keyed bytes
//!    - Local + cloud merging/fallback
//!
//! 2. BOUNDED UPLOAD PIPELINE:
//!    - `enqueue_object_upload()` - queue an explicit object key
//!    - `process_uploads()` - initiate cloud uploads
//!    - retain/coalesce terminal acknowledgements without unbounded channels
//!
//! WAL/SST key mapping, physical validation, manifest coverage, and prune policy
//! live in `runtime::hybrid_persistence`; this module never imports those formats.

use super::actor;
use super::policy;
use crate::storage::{
    StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome,
};
use crossbeam::channel as cb;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MAX_PENDING_WAL_UPLOADS: usize = 1_024;
const MAX_PENDING_WAL_UPLOAD_BYTES: u64 = 1024 * 1024 * 1024;
const WAL_UPLOAD_WORKER_QUEUE_CAPACITY: usize = 32;
const MAX_PENDING_STORAGE_EVENTS: usize = MAX_PENDING_WAL_UPLOADS * 2;
pub(crate) const HYBRID_STORAGE_EVENT_CHANNEL_CAPACITY: usize = MAX_PENDING_STORAGE_EVENTS;
const MAX_PENDING_STORAGE_EVENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONCURRENT_PRUNE_WORKERS: usize = 4;
const MAX_PENDING_PRUNE_REQUESTS: usize = 64;
const STORAGE_CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLOUD_ERROR_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy)]
struct HybridQueueLimits {
    upload_entries: usize,
    upload_bytes: u64,
    worker_entries: usize,
    event_entries: usize,
    event_bytes: usize,
    prune_workers: usize,
    prune_requests: usize,
    callback_timeout: Duration,
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
struct UploadQueue {
    entries: VecDeque<UploadState>,
    pending_bytes: u64,
    max_entries: usize,
    max_bytes: u64,
    blocked_upload_bytes: Option<u64>,
}

impl UploadQueue {
    fn new(max_entries: usize, max_bytes: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            pending_bytes: 0,
            max_entries,
            max_bytes,
            blocked_upload_bytes: None,
        }
    }

    fn try_push(&mut self, upload: UploadState) -> crate::common::MidgeResult<()> {
        self.ensure_capacity(upload.size_bytes)?;
        self.pending_bytes = self.pending_bytes.saturating_add(upload.size_bytes);
        self.entries.push_back(upload);
        Ok(())
    }

    fn ensure_capacity(&mut self, additional_bytes: u64) -> crate::common::MidgeResult<()> {
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

    fn is_stalled(&self) -> bool {
        self.blocked_upload_bytes.is_some()
            || self.entries.len() >= self.max_entries
            || self.pending_bytes >= self.max_bytes
    }

    fn remove_terminal(&mut self) {
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
struct QueuedStorageEvent {
    event: StorageEvent,
    externally_delivered: bool,
    accounted_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TerminalEventKey {
    Upload(u64),
    Prune(u64),
}

#[derive(Debug)]
struct BoundedEventQueue {
    /// Advisory/transient state changes. These may be coalesced independently
    /// from terminal completions.
    entries: VecDeque<QueuedStorageEvent>,
    pending_bytes: usize,
    /// One terminal result per admitted operation. A completion must not be
    /// lost merely because the transient queue is saturated.
    terminal_entries: HashMap<TerminalEventKey, QueuedStorageEvent>,
    terminal_pending_bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl BoundedEventQueue {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
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
            StorageEvent::CloudWalPruneComplete { segment_id, .. } => {
                Some(TerminalEventKey::Prune(*segment_id))
            }
            _ => None,
        }
    }

    fn event_bytes(event: &StorageEvent) -> usize {
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
            StorageEvent::CloudAck { .. }
            | StorageEvent::BackpressureOn
            | StorageEvent::BackpressureOff => 0,
        };
        std::mem::size_of::<StorageEvent>().saturating_add(dynamic)
    }

    fn try_push(&mut self, event: StorageEvent, externally_delivered: bool) -> Result<(), String> {
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

    fn drain(&mut self) -> Vec<QueuedStorageEvent> {
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

    fn pending_prune_completions(&self) -> usize {
        self.terminal_entries
            .keys()
            .filter(|key| matches!(key, TerminalEventKey::Prune(_)))
            .count()
    }
}

/// Status of a cloud upload operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadStatus {
    Pending,
    InFlight { started_at: Instant },
    Completed,
    Failed { error: String, retries: u32 },
}

/// State of a pending WAL segment upload
#[derive(Debug, Clone)]
pub struct UploadState {
    pub segment_id: u64,
    pub object_key: String,
    pub local_path: PathBuf,
    pub status: UploadStatus,
    pub max_sequence: u64,
    pub retries: u32,
    size_bytes: u64,
}

/// A stable read plus identity observation for one remote object.
///
/// Format-aware validation belongs to the runtime. Storage only establishes
/// that the bytes it returned still match the provider identity observed by
/// `HEAD`.
#[derive(Clone, Debug)]
pub(crate) struct RemoteObjectProof {
    key: String,
    bytes: Vec<u8>,
    metadata: StorageObjectMetadata,
}

impl RemoteObjectProof {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn metadata(&self) -> &StorageObjectMetadata {
        &self.metadata
    }
}

/// A format-neutral object identity that must still hold immediately before a
/// conditional delete is issued.
#[derive(Clone)]
pub(crate) struct GuardedObjectProof {
    backend: Arc<dyn StorageBackend>,
    key: String,
    expected_bytes: Option<Vec<u8>>,
    metadata: StorageObjectMetadata,
}

impl GuardedObjectProof {
    pub(crate) fn metadata_only(
        backend: Arc<dyn StorageBackend>,
        key: String,
        metadata: StorageObjectMetadata,
    ) -> Self {
        Self {
            backend,
            key,
            expected_bytes: None,
            metadata,
        }
    }

    pub(crate) fn exact(
        backend: Arc<dyn StorageBackend>,
        key: String,
        expected_bytes: Vec<u8>,
        metadata: StorageObjectMetadata,
    ) -> Self {
        Self {
            backend,
            key,
            expected_bytes: Some(expected_bytes),
            metadata,
        }
    }
}

struct PruneWorkerRegistry {
    shutting_down: bool,
    handles: Vec<JoinHandle<()>>,
    max_workers: usize,
    max_requests: usize,
}

impl PruneWorkerRegistry {
    fn new(max_workers: usize, max_requests: usize) -> Self {
        Self {
            shutting_down: false,
            handles: Vec::new(),
            max_workers: max_workers.max(1),
            max_requests: max_requests.max(1),
        }
    }
}

/// Hybrid storage combining local filesystem and cloud backends
///
/// Managed by a Storage Budget Actor to enforce disk constraints, watermarks,
/// and coordination between local caching and cloud durability.
///
/// `CloudAsync` Durability:
/// - Tracks pending WAL segment uploads
/// - Emits `CloudAck` when cloud confirms durability
/// - Handles retries and failure reporting
pub struct HybridStorage {
    /// Local storage backend (usually filesystem)
    local: Arc<dyn StorageBackend>,
    /// Cloud storage backend (S3, GCS, Azure, etc.)
    cloud: Arc<dyn StorageBackend>,
    /// Storage Budget Actor for disk management
    budget_actor: Arc<Mutex<actor::StorageBudgetActor>>,
    /// Pending WAL segment uploads (`CloudAsync` mode)
    upload_queue: Arc<Mutex<UploadQueue>>,
    /// Completed events ready for polling
    event_queue: Arc<Mutex<BoundedEventQueue>>,

    /// Optional external event sink for CloudAck/CloudFail.
    /// When set, upload completions are pushed directly to the runtime event loop
    /// to avoid polling latency.
    external_event_tx: Option<cb::Sender<StorageEvent>>,

    /// Dedicated WAL upload worker sender.
    wal_upload_tx: Option<mpsc::SyncSender<UploadState>>,

    /// Maximum wait for a callback-based storage backend to respond.
    callback_timeout: Duration,

    /// Flag indicating if WAL upload worker thread failed to spawn
    upload_worker_failed: bool,

    /// Background WAL upload worker thread handle
    upload_worker_handle: Option<JoinHandle<()>>,

    /// Remote WAL prune workers are tracked so shutdown can join them before
    /// releasing the lease that fenced their conditional deletes.
    prune_workers: Mutex<PruneWorkerRegistry>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HybridStorageBudgetSnapshot {
    pub max_local_bytes: u64,
    pub total_committed_bytes: u64,
    pub free_bytes: u64,
    pub usage_percent: u32,
    pub pending_evictions: usize,
}

impl HybridStorage {
    /// Create a new hybrid storage with an external event sender.
    ///
    /// When `external_event_tx` is provided, CloudAck/CloudFail will be sent to it
    /// as soon as they occur (in addition to internal bookkeeping), allowing the
    /// runtime to react without polling.
    pub fn new_with_event_sender(
        local: Arc<dyn StorageBackend>,
        cloud: Arc<dyn StorageBackend>,
        external_event_tx: cb::Sender<StorageEvent>,
    ) -> Self {
        Self::with_policy_and_event_sender(
            local,
            cloud,
            policy::StorageBudgetPolicy::default(),
            Some(external_event_tx),
        )
    }

    /// Create a new hybrid storage with a custom storage budget policy
    #[cfg(test)]
    pub fn with_policy(
        local: Arc<dyn StorageBackend>,
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
    ) -> Self {
        Self::with_policy_and_event_sender(local, cloud, policy, None)
    }

    pub fn with_policy_and_event_sender(
        local: Arc<dyn StorageBackend>,
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
        external_event_tx: Option<cb::Sender<StorageEvent>>,
    ) -> Self {
        Self::with_policy_event_sender_and_limits(
            local,
            cloud,
            policy,
            external_event_tx,
            HybridQueueLimits::default(),
        )
    }

    fn with_policy_event_sender_and_limits(
        local: Arc<dyn StorageBackend>,
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
        external_event_tx: Option<cb::Sender<StorageEvent>>,
        limits: HybridQueueLimits,
    ) -> Self {
        let budget_actor = actor::StorageBudgetActor::new(policy);

        let upload_queue = Arc::new(Mutex::new(UploadQueue::new(
            limits.upload_entries,
            limits.upload_bytes,
        )));
        let terminal_capacity = limits
            .upload_entries
            .saturating_add(limits.prune_requests.max(1));
        let terminal_bytes = terminal_capacity.saturating_mul(
            std::mem::size_of::<StorageEvent>().saturating_add(MAX_CLOUD_ERROR_BYTES),
        );
        let event_queue = Arc::new(Mutex::new(BoundedEventQueue::new(
            limits.event_entries.max(terminal_capacity),
            limits.event_bytes.max(terminal_bytes),
        )));
        // Single background worker for WAL uploads.
        // This avoids spawning one OS thread per segment, which is extremely
        // expensive under CloudAsync + synchronous write APIs (e.g. 10k puts).
        let (wal_upload_tx, wal_upload_rx) =
            mpsc::sync_channel::<UploadState>(limits.worker_entries.max(1));
        let (upload_worker_handle, upload_worker_failed) = Self::spawn_wal_upload_worker(
            wal_upload_rx,
            cloud.clone(),
            event_queue.clone(),
            external_event_tx.clone(),
            limits.callback_timeout,
        );

        Self {
            local,
            cloud,
            budget_actor: Arc::new(Mutex::new(budget_actor)),
            upload_queue,
            event_queue,
            external_event_tx,
            wal_upload_tx: Some(wal_upload_tx),
            callback_timeout: limits.callback_timeout,
            upload_worker_failed,
            upload_worker_handle,
            prune_workers: Mutex::new(PruneWorkerRegistry::new(
                limits.prune_workers,
                limits.prune_requests,
            )),
        }
    }

    /// Enqueue a WAL segment for cloud upload (`CloudAsync` mode)
    ///
    /// This is the WAL durability pipeline entry point.
    pub fn ensure_wal_upload_capacity(
        &self,
        additional_bytes: u64,
    ) -> crate::common::MidgeResult<()> {
        let mut queue = self.upload_queue.lock();
        if let Err(error) = queue.ensure_capacity(additional_bytes) {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_write_stall_cloud();
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn ensure_wal_write_admission(&self) -> crate::common::MidgeResult<()> {
        let queue = self.upload_queue.lock();
        if !queue.is_stalled() {
            return Ok(());
        }
        Err(crate::common::MidgeError::WriteStall(format!(
            "CloudAsync WAL upload queue remains at capacity: entries={}/{}, bytes={}/{}",
            queue.entries.len(),
            queue.max_entries,
            queue.pending_bytes,
            queue.max_bytes
        )))
    }

    pub fn is_wal_upload_stalled(&self) -> bool {
        self.upload_queue.lock().is_stalled()
    }

    /// Queue a local file for bounded remote publication under an explicit
    /// object key. The runtime owns the meaning of the request id and frontier.
    pub(crate) fn enqueue_object_upload(
        &self,
        request_id: u64,
        object_key: String,
        local_path: &Path,
        frontier: u64,
    ) -> crate::common::MidgeResult<()> {
        let size_bytes = std::fs::metadata(local_path)
            .map_err(crate::common::MidgeError::Io)?
            .len();
        let upload_state = UploadState {
            segment_id: request_id,
            object_key,
            local_path: local_path.to_path_buf(),
            status: UploadStatus::Pending,
            max_sequence: frontier,
            retries: 0,
            size_bytes,
        };

        let mut queue = self.upload_queue.lock();
        if let Err(error) = queue.try_push(upload_state) {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_write_stall_cloud();
            }
            return Err(error);
        }

        if queue.entries.len() >= queue.max_entries / 2
            || queue.pending_bytes >= queue.max_bytes / 2
        {
            tracing::warn!(
                queue_size = queue.entries.len(),
                queue_bytes = queue.pending_bytes,
                request_id,
                "CloudAsync WAL upload queue growing; cloud uploads may be slow"
            );
        }

        tracing::debug!(
            request_id,
            ?local_path,
            frontier,
            queue_size = queue.entries.len(),
            queue_bytes = queue.pending_bytes,
            "object enqueued for cloud upload"
        );
        Ok(())
    }

    /// Process pending uploads (should be called periodically by runtime)
    ///
    /// **WAL DURABILITY PIPELINE**
    ///
    /// Initiates cloud uploads for pending WAL segments.
    /// This is the ONLY place where WAL segments are uploaded to cloud.
    /// - Reads pending uploads from `upload_queue`
    /// - Initiates cloud upload via cloud backend (not `submit_write`)
    /// - Handles retries on failure (up to 3 attempts)
    /// - Emits CloudAck/CloudFail events to `event_queue`
    ///
    /// Non-blocking - actual uploads happen asynchronously in the dedicated worker thread.
    ///
    /// Returns any drained CloudAck/CloudFail events for the runtime to consume.
    pub fn process_uploads(&self) -> Vec<StorageEvent> {
        // 1) Drain worker completion events first.
        let drained_events = {
            let mut events = self.event_queue.lock();
            events.drain()
        };

        let mut queue = self.upload_queue.lock();

        // 2) Apply drained events to the upload state machine.
        for queued in &drained_events {
            match &queued.event {
                StorageEvent::CloudAck {
                    segment_id,
                    max_sequence: _,
                } => {
                    if let Some(item) = queue
                        .entries
                        .iter_mut()
                        .find(|u| &u.segment_id == segment_id)
                    {
                        item.status = UploadStatus::Completed;
                    }
                }
                StorageEvent::CloudFail { segment_id, error } => {
                    if let Some(item) = queue
                        .entries
                        .iter_mut()
                        .find(|u| &u.segment_id == segment_id)
                    {
                        item.retries = item.retries.saturating_add(1);
                        item.status = UploadStatus::Failed {
                            error: error.clone(),
                            retries: item.retries,
                        };
                    }
                }
                _ => {}
            }
        }

        // 3) Schedule any eligible uploads.
        self.schedule_pending_uploads(&mut queue);

        // 4) Garbage-collect finished items (Completed or Failed after 3 attempts).
        queue.remove_terminal();

        drained_events
            .into_iter()
            .filter(|queued| !queued.externally_delivered)
            .map(|queued| queued.event)
            .collect()
    }

    fn schedule_pending_uploads(&self, queue: &mut UploadQueue) {
        let now = Instant::now();
        for upload in &mut queue.entries {
            let eligible = match upload.status {
                UploadStatus::Pending => true,
                UploadStatus::Failed { .. } => upload.retries < 3,
                UploadStatus::InFlight { .. } | UploadStatus::Completed => false,
            };
            if !eligible {
                continue;
            }

            upload.status = UploadStatus::InFlight { started_at: now };
            if crate::failpoints::is_active("midge::cloud::inject_fail_wal_upload") {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_failed();
                }
                Self::emit_wal_upload_failure(
                    upload,
                    "failpoint: cloud WAL upload failed",
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                );
                continue;
            }

            if self.upload_worker_failed {
                Self::emit_wal_upload_failure(
                    upload,
                    "cloud upload worker failed to start",
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                );
                continue;
            }

            let Some(tx) = &self.wal_upload_tx else {
                Self::emit_wal_upload_failure(
                    upload,
                    "cloud upload worker is shutting down",
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                );
                continue;
            };
            match tx.try_send(upload.clone()) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    upload.status = UploadStatus::Pending;
                    break;
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    tracing::warn!(
                        segment_id = upload.segment_id,
                        "cloud upload worker unavailable"
                    );
                    Self::emit_wal_upload_failure(
                        upload,
                        "cloud upload worker channel disconnected",
                        &self.event_queue,
                        self.external_event_tx.as_ref(),
                    );
                }
            }
        }
    }

    fn duration_micros_to_u64(duration: Duration) -> u64 {
        u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
    }

    fn queue_storage_event(
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
        mut event: StorageEvent,
    ) {
        match &mut event {
            StorageEvent::CloudFail { error, .. }
            | StorageEvent::CloudWalPruneComplete {
                result: StorageOutcome::Err(error),
                ..
            } => Self::truncate_storage_error(error),
            _ => {}
        }
        let externally_delivered =
            external_event_tx.is_some_and(|tx| tx.try_send(event.clone()).is_ok());
        if let Err(error) = event_queue.lock().try_push(event, externally_delivered) {
            tracing::error!(error, "bounded storage event queue rejected completion");
        }
    }

    fn truncate_storage_error(error: &mut String) {
        if error.len() <= MAX_CLOUD_ERROR_BYTES {
            return;
        }
        let mut end = MAX_CLOUD_ERROR_BYTES;
        while !error.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        error.truncate(end);
    }

    fn spawn_wal_upload_worker(
        wal_upload_rx: mpsc::Receiver<UploadState>,
        cloud: Arc<dyn StorageBackend>,
        event_queue: Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<cb::Sender<StorageEvent>>,
        callback_timeout: Duration,
    ) -> (Option<JoinHandle<()>>, bool) {
        let spawn_result = thread::Builder::new()
            .name("midge-wal-uploader".to_string())
            .spawn(move || {
                while let Ok(upload) = wal_upload_rx.recv() {
                    Self::process_wal_upload(
                        &upload,
                        &cloud,
                        &event_queue,
                        external_event_tx.as_ref(),
                        callback_timeout,
                    );
                }
            });

        match spawn_result {
            Ok(handle) => (Some(handle), false),
            Err(error) => {
                tracing::error!("Failed to spawn WAL upload worker: {error}");
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_thread_spawn_failure();
                }
                (None, true)
            }
        }
    }

    fn process_wal_upload(
        upload: &UploadState,
        cloud: &Arc<dyn StorageBackend>,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
        callback_timeout: Duration,
    ) {
        let upload_start = Instant::now();
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_async_wal_upload_started();
        }

        Self::log_wal_upload_start(upload, true);
        let data = match Self::read_wal_file(upload) {
            Ok(data) => data,
            Err(error) => {
                Self::emit_wal_upload_failure(upload, &error, event_queue, external_event_tx);
                return;
            }
        };

        Self::record_wal_bytes(&data);
        if crate::failpoints::is_active("midge::cloud::inject_fail_wal_upload") {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_cloud_async_wal_upload_failed();
            }
            Self::emit_wal_upload_failure(
                upload,
                "failpoint: cloud WAL upload failed",
                event_queue,
                external_event_tx,
            );
            return;
        }

        let expected_data = data.clone();
        let object_key = upload.object_key.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_write_with_headers(
            &upload.object_key,
            data,
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );

        Self::handle_wal_upload_result(
            upload,
            upload_start,
            event_queue,
            external_event_tx,
            &rx,
            callback_timeout,
            |_, _| {
                let proof =
                    Self::stable_object_proof_from_backend(cloud, &object_key, callback_timeout)?;
                if proof.bytes == expected_data {
                    Ok(())
                } else {
                    Err(format!(
                        "remote object '{object_key}' differs from uploaded bytes"
                    ))
                }
            },
        );
    }

    /// Try to reserve space for an upcoming flush and return the operation
    /// token that must settle that exact reservation.
    pub fn reserve_for_flush_with_token(
        &self,
        est_size: u64,
    ) -> Result<actor::StorageReservationToken, actor::ReservationResult> {
        let mut actor = self.budget_actor.lock();
        let result = actor.reserve_for_flush_with_token(est_size);
        drop(actor);

        let reservation_result = match result {
            Ok(_) => actor::ReservationResult::Ok,
            Err(result) => result,
        };
        self.emit_reservation_result(reservation_result);

        result
    }

    fn emit_reservation_result(&self, result: actor::ReservationResult) {
        let event = match result {
            actor::ReservationResult::Ok => StorageEvent::BackpressureOff,
            actor::ReservationResult::WaitForCloudUpload
            | actor::ReservationResult::WaitForCompaction
            | actor::ReservationResult::RejectNoSpace => StorageEvent::BackpressureOn,
        };
        Self::queue_storage_event(&self.event_queue, self.external_event_tx.as_ref(), event);
    }

    fn log_wal_upload_start(upload: &UploadState, with_worker: bool) {
        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
            && upload.segment_id.is_multiple_of(1000)
        {
            if with_worker {
                eprintln!(
                    "[midge] CloudAsync upload start: segment_id={} max_sequence={} path={}",
                    upload.segment_id,
                    upload.max_sequence,
                    upload.local_path.display()
                );
            } else {
                eprintln!(
                    "[midge] CloudAsync inline upload start: segment_id={} max_sequence={} path={}",
                    upload.segment_id,
                    upload.max_sequence,
                    upload.local_path.display()
                );
            }
        }
    }

    fn record_wal_bytes(data: &[u8]) {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_upload(bytes);
        }
    }

    fn read_wal_file(upload: &UploadState) -> Result<Vec<u8>, String> {
        std::fs::read(&upload.local_path)
            .map_err(|error| format!("read {}: {}", upload.local_path.display(), error))
    }

    fn emit_wal_upload_failure(
        upload: &UploadState,
        error: &str,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
    ) {
        let fail = StorageEvent::CloudFail {
            segment_id: upload.segment_id,
            error: error.to_string(),
        };
        Self::queue_storage_event(event_queue, external_event_tx, fail);
    }

    fn log_wal_upload_ack(upload: &UploadState) {
        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
            && upload.segment_id.is_multiple_of(1000)
        {
            eprintln!(
                "[midge] CloudAsync upload ack: segment_id={} max_sequence={}",
                upload.segment_id, upload.max_sequence
            );
        }
    }

    fn emit_wal_upload_ack(
        upload: &UploadState,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
    ) {
        let ack = StorageEvent::CloudAck {
            segment_id: upload.segment_id,
            max_sequence: upload.max_sequence,
        };
        Self::queue_storage_event(event_queue, external_event_tx, ack);
    }

    fn handle_wal_upload_result(
        upload: &UploadState,
        upload_start: Instant,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
        rx: &std::sync::mpsc::Receiver<StorageEvent>,
        callback_timeout: Duration,
        mut verify_remote: impl FnMut(u64, u64) -> Result<(), String>,
    ) {
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::WriteComplete { key, result }) if result.is_ok() => {
                let _ = key;
                if let Err(error) = verify_remote(upload.segment_id, upload.max_sequence) {
                    if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                        telemetry.metrics().record_cloud_async_wal_upload_failed();
                    }
                    Self::emit_wal_upload_failure(
                        upload,
                        &format!("remote WAL readback validation failed: {error}"),
                        event_queue,
                        external_event_tx,
                    );
                    return;
                }

                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_completed(
                        Self::duration_micros_to_u64(upload_start.elapsed()),
                    );
                }
                Self::emit_wal_upload_ack(upload, event_queue, external_event_tx);
                Self::log_wal_upload_ack(upload);
            }
            Ok(StorageEvent::WriteComplete { key, result }) => {
                let _ = key;
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_failed();
                }
                let error = match result {
                    StorageOutcome::Err(e) => e,
                    StorageOutcome::Ok(()) => "Unknown error".to_string(),
                };
                Self::emit_wal_upload_failure(upload, &error, event_queue, external_event_tx);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Self::emit_wal_upload_failure(
                    upload,
                    "cloud WAL upload callback timed out",
                    event_queue,
                    external_event_tx,
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) | Ok(_) => Self::emit_wal_upload_failure(
                upload,
                "cloud WAL upload callback channel closed or returned an unexpected event",
                event_queue,
                external_event_tx,
            ),
        }
    }

    /// Settle the exact flush reservation that published an SST.
    pub fn flush_completed_with_token(
        &self,
        token: actor::StorageReservationToken,
        actual_size: u64,
    ) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.complete_flush_for(token, actual_size);
    }

    /// Release the exact flush reservation whose output did not publish.
    pub fn flush_failed_with_token(&self, token: actor::StorageReservationToken) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.abort_flush_for(token);
    }

    /// Reserve compaction output space and return the token for its terminal
    /// completion or cancellation.
    pub fn compaction_planned_with_token(
        &self,
        input_sizes: &[u64],
    ) -> actor::StorageReservationToken {
        let mut actor = self.budget_actor.lock();
        actor.plan_compaction_with_token(input_sizes)
    }

    /// Settle the exact compaction reservation after manifest publication.
    pub fn compaction_completed_with_token(
        &self,
        token: actor::StorageReservationToken,
        output_sizes: &[u64],
    ) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.complete_compaction_for(token, output_sizes);
    }

    /// Release the exact compaction reservation without deleting its inputs.
    pub fn compaction_aborted_with_token(&self, token: actor::StorageReservationToken) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.abort_compaction_for(token);
    }

    pub fn budget_snapshot(&self) -> HybridStorageBudgetSnapshot {
        let actor = self.budget_actor.lock();
        let disk_state = actor.disk_state();
        let max_local_bytes = actor.max_local_bytes();
        HybridStorageBudgetSnapshot {
            max_local_bytes,
            total_committed_bytes: disk_state.total_committed(),
            free_bytes: disk_state.free_bytes(max_local_bytes),
            usage_percent: disk_state.usage_percent(max_local_bytes),
            pending_evictions: actor.pending_evictions().len(),
        }
    }

    /// Get count of pending uploads (for monitoring)
    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue.lock().entries.len()
    }

    #[cfg(test)]
    fn pending_upload_bytes(&self) -> u64 {
        self.upload_queue.lock().pending_bytes
    }

    fn read_cloud_object_from_backend_blocking(
        cloud: &Arc<dyn StorageBackend>,
        key: &str,
    ) -> Result<Vec<u8>, String> {
        Self::read_object_from_backend_blocking(cloud, key, STORAGE_CALLBACK_TIMEOUT)
    }

    fn read_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_read(key, tx);
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Ok(data),
                ..
            }) => Ok(data),
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(format!("cloud object '{key}' unreadable: {error}")),
            Ok(other) => Err(format!(
                "unexpected cloud read response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("cloud read timed out for '{key}': {error}")),
        }
    }

    fn head_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<StorageObjectMetadata, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_head(key, tx);
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Ok(metadata),
            }) => {
                let _ = returned_key;
                Ok(metadata)
            }
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) => {
                let _ = returned_key;
                Err(format!(
                    "cloud object '{key}' unreadable during cached proof revalidation: {error}"
                ))
            }
            Ok(other) => Err(format!(
                "unexpected cloud HEAD response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("cloud HEAD timed out for '{key}': {error}")),
        }
    }

    fn storage_error_indicates_missing(error: &str) -> bool {
        let error = error.to_ascii_lowercase();
        error.contains("not found")
            || error.contains("notfound")
            || error.contains("no such file")
            || error.contains("does not exist")
            || error.contains("404")
    }

    fn object_exists_in_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
    ) -> Result<bool, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_head(key, tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Ok(_),
            }) => {
                let _ = returned_key;
                Ok(true)
            }
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) if Self::storage_error_indicates_missing(&error) => {
                let _ = returned_key;
                Ok(false)
            }
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) => {
                let _ = returned_key;
                Err(format!("object '{key}' HEAD failed: {error}"))
            }
            Ok(other) => Err(format!(
                "unexpected storage HEAD response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("storage HEAD timed out for '{key}': {error}")),
        }
    }

    fn delete_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
    ) -> Result<bool, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_delete(key, tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(StorageEvent::DeleteComplete {
                key: returned_key,
                result: StorageOutcome::Ok(()),
            }) => {
                let _ = returned_key;
                Ok(true)
            }
            Ok(StorageEvent::DeleteComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) if Self::storage_error_indicates_missing(&error) => {
                let _ = returned_key;
                Ok(false)
            }
            Ok(StorageEvent::DeleteComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) => {
                let _ = returned_key;
                Err(format!("object '{key}' delete failed: {error}"))
            }
            Ok(other) => Err(format!(
                "unexpected storage delete response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("storage delete timed out for '{key}': {error}")),
        }
    }

    fn stable_object_proof_from_backend(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<RemoteObjectProof, String> {
        let before = Self::head_object_from_backend_blocking(backend, key, callback_timeout)?;
        let bytes = Self::read_object_from_backend_blocking(backend, key, callback_timeout)?;
        let after = Self::head_object_from_backend_blocking(backend, key, callback_timeout)?;
        if before != after {
            return Err(format!(
                "object '{key}' identity changed during read: before {before:?}, after {after:?}"
            ));
        }
        if after.size != bytes.len() as u64 {
            return Err(format!(
                "object '{key}' size changed during read: bytes={}, metadata={} ",
                bytes.len(),
                after.size
            ));
        }
        Ok(RemoteObjectProof {
            key: key.to_string(),
            bytes,
            metadata: after,
        })
    }

    /// Read one cloud object together with a stable provider identity. The
    /// runtime may validate the bytes as WAL, SST, or metadata without giving
    /// those formats to the storage layer.
    pub(crate) fn remote_object_proof(&self, key: &str) -> Result<RemoteObjectProof, String> {
        Self::stable_object_proof_from_backend(&self.cloud, key, self.callback_timeout)
    }

    /// Return a stable proof when the remote key exists, or `None` for a
    /// provider-confirmed missing key.
    pub(crate) fn remote_object_proof_optional(
        &self,
        key: &str,
    ) -> Result<Option<RemoteObjectProof>, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_head(key, tx);
        match rx.recv_timeout(self.callback_timeout) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(_),
                ..
            }) => self.remote_object_proof(key).map(Some),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if Self::storage_error_indicates_missing(&error) => Ok(None),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(format!("remote object '{key}' HEAD failed: {error}")),
            Ok(other) => Err(format!(
                "unexpected remote object HEAD response for '{key}': {other:?}"
            )),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err(format!("remote object HEAD timed out for '{key}'"))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(format!("remote object HEAD callback closed for '{key}'"))
            }
        }
    }

    /// Conditionally replace or create a remote object and return a stable
    /// proof of the exact bytes that won the provider CAS.
    pub(crate) fn compare_exchange_remote_object(
        &self,
        key: &str,
        expected: Option<&StorageObjectMetadata>,
        data: Vec<u8>,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
        let headers = if let Some(expected) = expected {
            let etag = expected.etag.trim();
            if etag.is_empty() {
                return Err(crate::common::MidgeError::InvalidArgument(format!(
                    "remote CAS for '{key}' requires a non-empty identity token"
                )));
            }
            vec![("If-Match".to_string(), etag.to_string())]
        } else {
            vec![("If-None-Match".to_string(), "*".to_string())]
        };
        let expected_bytes = data.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_write_with_headers(key, data, headers, tx);
        match rx.recv_timeout(self.callback_timeout) {
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => {}
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => {
                let lower = error.to_ascii_lowercase();
                if lower.contains("precondition")
                    || lower.contains("if-match")
                    || lower.contains("already exists")
                {
                    return Err(crate::common::MidgeError::Busy(format!(
                        "remote CAS conflict for '{key}': {error}"
                    )));
                }
                return Err(crate::common::MidgeError::Internal(format!(
                    "remote CAS failed for '{key}': {error}"
                )));
            }
            Ok(other) => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "unexpected remote CAS response for '{key}': {other:?}"
                )));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(crate::common::MidgeError::Timeout(format!(
                    "remote CAS timed out for '{key}'"
                )));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "remote CAS callback closed for '{key}'"
                )));
            }
        }

        let proof = self
            .remote_object_proof(key)
            .map_err(crate::common::MidgeError::Internal)?;
        if proof.bytes != expected_bytes {
            return Err(crate::common::MidgeError::Corruption(format!(
                "remote CAS for '{key}' read back different bytes"
            )));
        }
        Ok(proof)
    }

    /// Convert a validated cloud read into a metadata-only dependency for a
    /// guarded delete. The delete worker rechecks this identity immediately
    /// before issuing the conditional delete.
    pub(crate) fn remote_identity_guard(&self, proof: &RemoteObjectProof) -> GuardedObjectProof {
        GuardedObjectProof::metadata_only(
            Arc::clone(&self.cloud),
            proof.key.clone(),
            proof.metadata.clone(),
        )
    }

    fn verify_guarded_object_proof(
        proof: &GuardedObjectProof,
        callback_timeout: Duration,
    ) -> Result<(), String> {
        if let Some(expected_bytes) = proof.expected_bytes.as_ref() {
            let actual = Self::stable_object_proof_from_backend(
                &proof.backend,
                &proof.key,
                callback_timeout,
            )?;
            if actual.bytes != *expected_bytes {
                return Err(format!(
                    "guarded object '{}' changed before conditional delete",
                    proof.key
                ));
            }
            if actual.metadata != proof.metadata {
                return Err(format!(
                    "guarded object '{}' identity changed before conditional delete: expected {:?}, actual {:?}",
                    proof.key, proof.metadata, actual.metadata
                ));
            }
            return Ok(());
        }

        let actual =
            Self::head_object_from_backend_blocking(&proof.backend, &proof.key, callback_timeout)?;
        if actual == proof.metadata {
            return Ok(());
        }
        Err(format!(
            "guarded object '{}' identity changed before conditional delete: expected {:?}, actual {actual:?}",
            proof.key, proof.metadata
        ))
    }

    /// Conditionally delete a remote object after revalidating format-neutral
    /// dependency identities. The runtime must first establish all semantic
    /// coverage relationships and supply the resulting proofs.
    pub(crate) fn delete_remote_object_guarded(
        &self,
        request_id: u64,
        target: RemoteObjectProof,
        dependencies: Vec<GuardedObjectProof>,
    ) -> Result<(), String> {
        self.reap_finished_prune_workers();

        let etag = target.metadata.etag.trim().to_string();
        if etag.is_empty() {
            return Err(format!(
                "cannot conditionally delete remote object '{}' without an identity token",
                target.key
            ));
        }

        let cloud = Arc::clone(&self.cloud);
        let target_guard = self.remote_identity_guard(&target);
        let target_key = target.key;
        let event_queue = Arc::clone(&self.event_queue);
        let external_event_tx = self.external_event_tx.clone();
        let callback_timeout = self.callback_timeout;

        let mut workers = self.prune_workers.lock();
        if workers.shutting_down {
            return Err("hybrid storage is shutting down; guarded delete rejected".to_string());
        }
        let pending_completions = self.event_queue.lock().pending_prune_completions();
        if workers.handles.len() >= workers.max_workers {
            return Err(format!(
                "guarded delete workers at capacity: running={}/{}",
                workers.handles.len(),
                workers.max_workers
            ));
        }
        if workers.handles.len().saturating_add(pending_completions) >= workers.max_requests {
            return Err(format!(
                "guarded delete completion queue at capacity: outstanding={}/{}",
                workers.handles.len().saturating_add(pending_completions),
                workers.max_requests
            ));
        }

        let worker = thread::Builder::new()
            .name(format!("midge-object-pruner-{request_id}"))
            .spawn(move || {
                let result = (|| {
                    Self::verify_guarded_object_proof(&target_guard, callback_timeout)?;
                    for dependency in &dependencies {
                        Self::verify_guarded_object_proof(dependency, callback_timeout)?;
                    }

                    let (tx, rx) = std::sync::mpsc::channel();
                    cloud.submit_delete_with_headers(
                        &target_key,
                        vec![("If-Match".into(), etag)],
                        tx,
                    );
                    match rx.recv_timeout(callback_timeout) {
                        Ok(StorageEvent::DeleteComplete { result, .. }) => match result {
                            StorageOutcome::Ok(()) => Ok(()),
                            StorageOutcome::Err(error) => Err(error),
                        },
                        Ok(other) => Err(format!(
                            "unexpected guarded delete response for '{target_key}': {other:?}"
                        )),
                        Err(error) => Err(format!(
                            "guarded delete timed out for '{target_key}': {error}"
                        )),
                    }
                })();

                let result = match result {
                    Ok(()) => StorageOutcome::Ok(()),
                    Err(error) => StorageOutcome::Err(error),
                };
                let event = StorageEvent::CloudWalPruneComplete {
                    segment_id: request_id,
                    result,
                };
                Self::queue_storage_event(&event_queue, external_event_tx.as_ref(), event);
            })
            .map_err(|error| format!("failed to spawn guarded delete worker: {error}"))?;
        workers.handles.push(worker);
        Ok(())
    }

    /// Stop admitting cloud prune work and join every outstanding worker.
    ///
    /// Each worker may issue a conditional remote delete, so shutdown must
    /// wait for it while the current lease/fencing epoch remains valid.
    pub(crate) fn shutdown_background_workers(&self) {
        let handles = {
            let mut workers = self.prune_workers.lock();
            workers.shutting_down = true;
            std::mem::take(&mut workers.handles)
        };

        for worker in handles {
            if let Ok(()) = worker.join() {
                tracing::debug!("cloud WAL prune worker joined");
            } else {
                tracing::warn!("cloud WAL prune worker panicked during join");
            }
        }
    }

    fn reap_finished_prune_workers(&self) {
        let mut workers = self.prune_workers.lock();
        let mut still_running = Vec::new();
        for worker in std::mem::take(&mut workers.handles) {
            if worker.is_finished() {
                if let Ok(()) = worker.join() {
                    tracing::debug!("cloud WAL prune worker completed");
                } else {
                    tracing::warn!("cloud WAL prune worker panicked");
                }
            } else {
                still_running.push(worker);
            }
        }
        workers.handles = still_running;
    }

    /// Publish immutable bytes to the remote backend and local cache. Retries
    /// succeed only when an existing object contains exactly the same bytes.
    pub(crate) fn publish_immutable_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> crate::common::MidgeResult<()> {
        let local_exists = self.ensure_local_immutable_retry_compatible(key, &data)?;
        self.ensure_remote_immutable_published(key, &data)?;
        if !local_exists {
            self.write_local_immutable_cache(key, data)?;
        }
        Ok(())
    }

    fn ensure_local_immutable_retry_compatible(
        &self,
        key: &str,
        data: &[u8],
    ) -> crate::common::MidgeResult<bool> {
        let exists =
            Self::object_exists_in_backend_blocking(&self.local, key).map_err(|error| {
                crate::common::MidgeError::Internal(format!(
                    "local immutable cache preflight failed: {error}"
                ))
            })?;
        if !exists {
            return Ok(false);
        }

        let existing = Self::read_cloud_object_from_backend_blocking(&self.local, key)
            .map_err(crate::common::MidgeError::Internal)?;
        if existing != data {
            return Err(crate::common::MidgeError::Internal(format!(
                "local cache already exists with different bytes for immutable object '{key}'"
            )));
        }
        Ok(true)
    }

    fn ensure_remote_immutable_published(
        &self,
        key: &str,
        data: &[u8],
    ) -> crate::common::MidgeResult<()> {
        let exists = Self::object_exists_in_backend_blocking(&self.cloud, key)
            .map_err(crate::common::MidgeError::Internal)?;
        if exists {
            return Self::ensure_backend_object_matches(&self.cloud, key, data, None);
        }

        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_write_with_headers(
            key,
            data.to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx
            .recv_timeout(self.callback_timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => crate::common::MidgeError::Timeout(
                    "cloud immutable upload callback timed out".to_string(),
                ),
                mpsc::RecvTimeoutError::Disconnected => crate::common::MidgeError::Internal(
                    "cloud immutable upload callback channel closed".to_string(),
                ),
            })?;

        match event {
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            } => Ok(()),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            } => Self::ensure_backend_object_matches(&self.cloud, key, data, Some(&error)),
            other => Err(crate::common::MidgeError::Internal(format!(
                "unexpected cloud immutable upload response: {other:?}"
            ))),
        }
    }

    fn ensure_backend_object_matches(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        expected: &[u8],
        upload_error: Option<&str>,
    ) -> crate::common::MidgeResult<()> {
        let existing =
            Self::read_cloud_object_from_backend_blocking(backend, key).map_err(|read_error| {
                crate::common::MidgeError::Internal(format!(
                    "cloud immutable upload failed{}; readback failed: {read_error}",
                    upload_error.map_or_else(String::new, |error| format!(": {error}"))
                ))
            })?;
        if existing == expected {
            return Ok(());
        }
        Err(crate::common::MidgeError::Internal(format!(
            "cloud immutable upload failed{}: object '{key}' contains different bytes",
            upload_error.map_or_else(String::new, |error| format!(": {error}"))
        )))
    }

    fn write_local_immutable_cache(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> crate::common::MidgeResult<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.local.submit_write_with_headers(
            key,
            data,
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx
            .recv_timeout(self.callback_timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => crate::common::MidgeError::Timeout(
                    "local immutable cache write callback timed out".to_string(),
                ),
                mpsc::RecvTimeoutError::Disconnected => crate::common::MidgeError::Internal(
                    "local immutable cache write callback channel closed".to_string(),
                ),
            })?;
        match event {
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            } => Ok(()),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            } => Err(crate::common::MidgeError::Internal(format!(
                "local immutable cache write failed: {error}"
            ))),
            other => Err(crate::common::MidgeError::Internal(format!(
                "unexpected local immutable cache write response: {other:?}"
            ))),
        }
    }

    /// Delete one immutable key from the remote backend and best-effort local
    /// cache. The caller owns any manifest or lifecycle decision.
    pub(crate) fn delete_immutable_object_blocking(
        &self,
        key: &str,
    ) -> crate::common::MidgeResult<()> {
        match Self::delete_object_from_backend_blocking(&self.cloud, key) {
            Ok(true) => {
                tracing::info!(key, "deleted obsolete remote immutable object");
            }
            Ok(false) => {
                tracing::debug!(key, "remote immutable object already missing");
            }
            Err(error) => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "remote immutable object delete failed: {error}"
                )));
            }
        }

        // This runs inside the tracked GC worker that owns this deletion.
        // Avoid a detached local-cache delete that could outlive the lease.
        match Self::delete_object_from_backend_blocking(&self.local, key) {
            Ok(true) => {
                tracing::debug!(key, "deleted obsolete local immutable cache object");
            }
            Ok(false) => {
                tracing::debug!(key, "local immutable cache object already missing");
            }
            Err(error) => {
                tracing::warn!(
                    key,
                    error,
                    "failed to delete obsolete local immutable cache object"
                );
            }
        }

        Ok(())
    }
}

impl StorageBackend for HybridStorage {
    fn submit_read(&self, key: &str, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - reads SSTs, metadata, etc.
        // Try local first, fall back to cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let key = key.to_string();

        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_read(&key, tx);

        match rx.recv_timeout(self.callback_timeout) {
            Ok(StorageEvent::ReadComplete {
                key: k,
                result: StorageOutcome::Ok(data),
            }) => {
                // Success from local, return immediately
                let _ = callback.send(StorageEvent::ReadComplete {
                    key: k,
                    result: StorageOutcome::Ok(data),
                });
            }
            Ok(StorageEvent::ReadComplete {
                key: k,
                result: StorageOutcome::Err(_),
            }) => {
                // Local miss, try cloud
                let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
                cloud_clone.submit_read(&k, tx_cloud);
                match rx_cloud.recv_timeout(self.callback_timeout) {
                    Ok(event) => {
                        let _ = callback.send(event);
                    }
                    Err(error) => {
                        let _ = callback.send(StorageEvent::ReadComplete {
                            key: k,
                            result: StorageOutcome::Err(format!(
                                "cloud read callback failed or timed out: {error}"
                            )),
                        });
                    }
                }
            }
            _ => {
                let _ = callback.send(StorageEvent::ReadComplete {
                    key: key.clone(),
                    result: StorageOutcome::Err("Hybrid read failed".to_string()),
                });
            }
        }
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - NOT for WAL durability
        // WAL durability uses enqueue_wal_segment() instead

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let data_clone = data.clone();

        // Always write to local first
        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_write(key, data_clone, tx);

        match rx.recv_timeout(self.callback_timeout) {
            Ok(StorageEvent::WriteComplete { result, .. }) => {
                if !key.starts_with("sst/") || matches!(result, StorageOutcome::Err(_)) {
                    let _ = callback.send(StorageEvent::WriteComplete {
                        key: key.to_string(),
                        result,
                    });
                    return;
                }

                let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
                cloud_clone.submit_write_with_headers(
                    key,
                    data,
                    vec![("If-None-Match".into(), "*".into())],
                    tx_cloud,
                );

                let result = match rx_cloud.recv_timeout(self.callback_timeout) {
                    Ok(StorageEvent::WriteComplete {
                        result: StorageOutcome::Ok(()),
                        ..
                    }) => StorageOutcome::Ok(()),
                    Ok(StorageEvent::WriteComplete {
                        result: StorageOutcome::Err(error),
                        ..
                    }) => StorageOutcome::Err(format!(
                        "cloud write failed after local write succeeded: {error}"
                    )),
                    Ok(other) => StorageOutcome::Err(format!(
                        "unexpected cloud write completion event: {other:?}"
                    )),
                    Err(error) => StorageOutcome::Err(format!(
                        "cloud write callback failed or timed out after local write succeeded: {error}"
                    )),
                };

                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result,
                });
            }
            _ => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err("Hybrid write failed".to_string()),
                });
            }
        }
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - deletes SSTs, metadata, etc.
        // Delete from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let key = key.to_string();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_delete(&key, tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_delete(&key, tx_cloud);

        // Wait for both and report result
        let local_result = rx_local.recv_timeout(self.callback_timeout).ok();
        let cloud_result = rx_cloud.recv_timeout(self.callback_timeout).ok();

        let combined_result = match (local_result, cloud_result) {
            (
                Some(StorageEvent::DeleteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                }),
                Some(StorageEvent::DeleteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                }),
            ) => StorageOutcome::Ok(()),
            _ => StorageOutcome::Err("Hybrid delete failed".to_string()),
        };

        let _ = callback.send(StorageEvent::DeleteComplete {
            key,
            result: combined_result,
        });
    }

    #[cfg(test)]
    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - lists SSTs, metadata, etc.
        // Merge results from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let prefix = prefix.to_string();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_list(&prefix, tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_list(&prefix, tx_cloud);

        let mut results = Vec::new();

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(local_items),
            ..
        }) = rx_local.recv_timeout(self.callback_timeout)
        {
            results.extend(local_items);
        }

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(cloud_items),
            ..
        }) = rx_cloud.recv_timeout(self.callback_timeout)
        {
            for item in cloud_items {
                if !results.contains(&item) {
                    results.push(item);
                }
            }
        }

        results.sort();
        results.dedup();

        let _ = callback.send(StorageEvent::ListComplete {
            prefix: prefix.clone(),
            result: StorageOutcome::Ok(results),
        });
    }
}

impl Drop for HybridStorage {
    fn drop(&mut self) {
        self.shutdown_background_workers();

        // Drop sender first so worker recv() unblocks and exits promptly.
        // Waiting for join before dropping sender can deadlock until timeout.
        let _ = self.wal_upload_tx.take();

        // Join the worker before releasing storage ownership. A detached
        // uploader could continue mutating cloud state after the lease is
        // released, violating fencing guarantees.
        if let Some(handle) = self.upload_worker_handle.take() {
            let start = Instant::now();
            match handle.join() {
                Ok(()) => tracing::debug!(
                    elapsed_ms = start.elapsed().as_millis(),
                    "HybridStorage WAL upload worker shutdown cleanly"
                ),
                Err(_) => {
                    tracing::warn!("HybridStorage WAL upload worker panicked during shutdown");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::hybrid_persistence::{
        CloudMetadataPruneGuard, CloudMetadataPruneProof, CloudWalPruneGuard, HybridPersistence,
    };
    use crate::sst::SstFactory;
    use crate::storage::cloud::{CloudStorage, MockCloudBackend};
    use bytes::Bytes;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn should_retain_terminal_upload_completion_when_transient_queue_is_saturated() {
        // Arrange
        let mut queue = BoundedEventQueue::new(1, std::mem::size_of::<StorageEvent>() * 2);
        queue
            .try_push(StorageEvent::BackpressureOn, false)
            .expect("fill transient event capacity");

        // Act
        let result = queue.try_push(
            StorageEvent::CloudAck {
                segment_id: 17,
                max_sequence: 29,
            },
            false,
        );

        // Assert
        assert!(
            result.is_ok(),
            "terminal completion was dropped: {result:?}"
        );
        assert!(queue.drain().iter().any(|queued| {
            matches!(
                queued.event,
                StorageEvent::CloudAck {
                    segment_id: 17,
                    max_sequence: 29
                }
            )
        }));
    }

    #[test]
    fn should_create_remote_object_when_cas_key_is_missing() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let key = "metadata/ddl-manifest.json";
        let data = br#"{"epoch":1}"#.to_vec();
        assert!(storage
            .remote_object_proof_optional(key)
            .expect("check missing CAS key")
            .is_none());

        // Act
        let proof = storage
            .compare_exchange_remote_object(key, None, data.clone())
            .expect("conditionally create remote object");

        // Assert
        assert_eq!(proof.bytes(), data);
        assert_eq!(
            storage
                .remote_object_proof_optional(key)
                .expect("read created CAS key")
                .expect("created CAS key must exist")
                .bytes(),
            proof.bytes()
        );
    }

    #[test]
    fn should_reject_remote_cas_when_identity_is_stale() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let key = "metadata/ddl-manifest.json";
        let original = storage
            .compare_exchange_remote_object(key, None, b"epoch-1".to_vec())
            .expect("create initial remote object");
        storage
            .compare_exchange_remote_object(key, Some(original.metadata()), b"epoch-2".to_vec())
            .expect("advance remote object");

        // Act
        let error = storage
            .compare_exchange_remote_object(key, Some(original.metadata()), b"stale".to_vec())
            .expect_err("stale identity must lose provider CAS");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Busy(_)));
        assert_eq!(
            storage
                .remote_object_proof(key)
                .expect("read winning CAS bytes")
                .bytes(),
            b"epoch-2"
        );
    }

    #[test]
    fn should_reject_guarded_delete_when_worker_capacity_is_exhausted() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create guarded-delete test dir");
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
                .expect("create local backend"),
        );
        let cloud = Arc::new(NeverCompletesBackend::default());
        let limits = HybridQueueLimits {
            prune_workers: 1,
            prune_requests: 1,
            callback_timeout: Duration::from_millis(100),
            ..HybridQueueLimits::default()
        };
        let storage = HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        );
        let target = RemoteObjectProof {
            key: "objects/first".to_string(),
            bytes: vec![1],
            metadata: StorageObjectMetadata {
                size: 1,
                etag: "first-etag".to_string(),
                generation: None,
            },
        };
        storage
            .delete_remote_object_guarded(1, target.clone(), Vec::new())
            .expect("first guarded delete should occupy the worker");

        // Act
        let started = Instant::now();
        let error = storage
            .delete_remote_object_guarded(2, target, Vec::new())
            .expect_err("second guarded delete must be rejected at worker capacity");

        // Assert
        assert!(error.contains("workers at capacity"), "{error}");
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    fn hybrid_with_mock_cloud() -> (Arc<MockCloudBackend>, HybridStorage) {
        let tmp = tempfile::tempdir().expect("create hybrid storage test dir");
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
                .expect("create local backend"),
        );
        let mock_cloud = Arc::new(MockCloudBackend::new());
        let cloud = Arc::new(CloudStorage::new(
            mock_cloud.clone(),
            "hybrid-test".to_string(),
        ));
        let storage = HybridStorage::with_policy(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        (mock_cloud, storage)
    }

    fn valid_sst_bytes(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = factory.create().expect("create SST writer");
        writer
            .add_with_meta(key, Some(value), seq, 0, None)
            .expect("add SST entry");
        writer.finish_bytes().expect("finish SST bytes")
    }

    fn valid_wal_bytes(seq: u64) -> Vec<u8> {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            seq,
            1,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("append WAL frame");
        bytes
    }

    struct AlwaysFailingWriteBackend {
        write_attempts: Arc<AtomicUsize>,
    }

    impl AlwaysFailingWriteBackend {
        fn new(write_attempts: Arc<AtomicUsize>) -> Self {
            Self { write_attempts }
        }
    }

    impl StorageBackend for AlwaysFailingWriteBackend {
        fn submit_read(&self, key: &str, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::ReadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("read unavailable".to_string()),
            });
        }

        fn submit_write(&self, key: &str, _data: Vec<u8>, callback: StorageCallback) {
            self.write_attempts.fetch_add(1, Ordering::SeqCst);
            let _ = callback.send(StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("write unavailable".to_string()),
            });
        }

        fn submit_write_with_headers(
            &self,
            key: &str,
            data: Vec<u8>,
            _headers: Vec<(String, String)>,
            callback: StorageCallback,
        ) {
            self.submit_write(key, data, callback);
        }

        fn submit_delete(&self, key: &str, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Ok(()),
            });
        }

        fn submit_list(&self, prefix: &str, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Ok(Vec::new()),
            });
        }

        fn submit_head(&self, key: &str, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("head unavailable".to_string()),
            });
        }
    }

    #[derive(Default)]
    struct NeverCompletesBackend {
        callbacks: Mutex<Vec<StorageCallback>>,
    }

    impl NeverCompletesBackend {
        fn retain_callback(&self, callback: StorageCallback) {
            self.callbacks.lock().push(callback);
        }
    }

    impl StorageBackend for NeverCompletesBackend {
        fn submit_read(&self, _key: &str, callback: StorageCallback) {
            self.retain_callback(callback);
        }

        fn submit_write(&self, _key: &str, _data: Vec<u8>, callback: StorageCallback) {
            self.retain_callback(callback);
        }

        fn submit_write_with_headers(
            &self,
            _key: &str,
            _data: Vec<u8>,
            _headers: Vec<(String, String)>,
            callback: StorageCallback,
        ) {
            self.retain_callback(callback);
        }

        fn submit_delete(&self, _key: &str, callback: StorageCallback) {
            self.retain_callback(callback);
        }

        fn submit_list(&self, _prefix: &str, callback: StorageCallback) {
            self.retain_callback(callback);
        }

        fn submit_head(&self, _key: &str, callback: StorageCallback) {
            self.retain_callback(callback);
        }
    }

    fn write_cloud_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.cloud.submit_write(key, data, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => {}
            other => panic!("cloud write for '{key}' failed: {other:?}"),
        }
    }

    fn write_local_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.local.submit_write(key, data, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => {}
            other => panic!("local write for '{key}' failed: {other:?}"),
        }
    }

    fn read_local_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.local.submit_read(key, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Ok(data),
                ..
            }) => data,
            other => panic!("local read for '{key}' failed: {other:?}"),
        }
    }

    fn read_cloud_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.cloud.submit_read(key, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Ok(data),
                ..
            }) => data,
            other => panic!("cloud read for '{key}' failed: {other:?}"),
        }
    }

    fn read_hybrid_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.submit_read(key, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Ok(data),
                ..
            }) => data,
            other => panic!("hybrid read for '{key}' failed: {other:?}"),
        }
    }

    fn delete_cloud_object(storage: &HybridStorage, key: &str) {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.cloud.submit_delete(key, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => {}
            other => panic!("cloud delete for '{key}' failed: {other:?}"),
        }
    }

    fn write_cloud_metadata_object(cloud: &CloudStorage, key: &str, data: Vec<u8>) {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(key, data, vec![], tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(crate::storage::cloud::CloudEvent::Put {
                result: crate::storage::cloud::CloudOutcome::Ok(()),
                ..
            }) => {}
            other => panic!("cloud metadata write for '{key}' failed: {other:?}"),
        }
    }

    fn head_cloud_metadata_object(cloud: &CloudStorage, key: &str) -> StorageObjectMetadata {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(crate::storage::cloud::CloudEvent::Head {
                result: crate::storage::cloud::CloudOutcome::Ok(metadata),
                ..
            }) => StorageObjectMetadata {
                size: metadata.size,
                etag: metadata.etag,
                generation: metadata.generation,
            },
            other => panic!("cloud metadata head for '{key}' failed: {other:?}"),
        }
    }

    fn assert_cloud_object_exists(storage: &HybridStorage, key: &str) {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.cloud.submit_head(key, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(_),
                ..
            }) => {}
            other => panic!("expected cloud object '{key}' to exist, got {other:?}"),
        }
    }

    fn assert_cloud_object_missing(storage: &HybridStorage, key: &str) {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.cloud.submit_head(key, tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(_),
                ..
            }) => {}
            other => panic!("expected cloud object '{key}' to be missing, got {other:?}"),
        }
    }

    fn wait_for_wal_prune_result(storage: &HybridStorage, segment_id: u64) -> StorageOutcome<()> {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            for event in storage.process_uploads() {
                if let StorageEvent::CloudWalPruneComplete {
                    segment_id: event_segment,
                    result,
                } = event
                {
                    if event_segment == segment_id {
                        return result;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("timed out waiting for WAL prune result for segment {segment_id}");
    }

    fn manifest_for_ssts(files: &[(&str, u64)]) -> crate::metadata::Manifest {
        let files_with_crc: Vec<_> = files
            .iter()
            .map(|(name, size_bytes)| (*name, *size_bytes, None))
            .collect();
        manifest_for_ssts_with_crc(&files_with_crc)
    }

    fn manifest_for_ssts_with_crc(files: &[(&str, u64, Option<u32>)]) -> crate::metadata::Manifest {
        crate::metadata::Manifest {
            files: files
                .iter()
                .map(
                    |(name, size_bytes, content_crc32c)| crate::metadata::FileMeta {
                        name: (*name).to_string(),
                        level: 0,
                        size_bytes: *size_bytes,
                        content_crc32c: *content_crc32c,
                        cf_id: 0,
                        smallest_key: Some(b"a".to_vec()),
                        largest_key: Some(b"a".to_vec()),
                        smallest_seq: Some(1),
                        largest_seq: Some(1),
                        ..Default::default()
                    },
                )
                .collect(),
            ..Default::default()
        }
    }

    fn manifest_covering_wal(
        sst_name: &str,
        sst_bytes: &[u8],
        sequence: u64,
        content_crc32c: Option<u32>,
    ) -> crate::metadata::Manifest {
        crate::metadata::Manifest {
            files: vec![crate::metadata::FileMeta {
                name: sst_name.to_string(),
                level: 0,
                size_bytes: sst_bytes.len() as u64,
                content_crc32c,
                cf_id: 0,
                smallest_key: Some(b"k".to_vec()),
                largest_key: Some(b"k".to_vec()),
                smallest_seq: Some(sequence),
                largest_seq: Some(sequence),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn should_revalidate_manifest_ssts_on_repeated_validation() {
        // Arrange
        let (mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "cached.sst";
        let bytes = valid_sst_bytes(b"a", b"v1", 1);
        write_cloud_object(&storage, &crate::sst::object_key(sst_name), bytes.clone());
        let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

        mock_cloud.clear_history();
        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("first manifest validation");
        let first_downloads = mock_cloud.get_downloads();
        // Act
        // Assert
        assert!(
            first_downloads
                .iter()
                .any(|key| key.ends_with("sst/cached.sst")),
            "first validation should read the cloud SST, got {first_downloads:?}"
        );

        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("second manifest validation");

        let downloads = mock_cloud.get_downloads();
        assert_eq!(
            downloads
                .iter()
                .filter(|key| key.ends_with("sst/cached.sst"))
                .count(),
            2,
            "authoritative validation must reread the immutable SST: {downloads:?}"
        );
    }

    #[test]
    fn should_revalidate_full_manifest_after_extension() {
        // Arrange
        let (mock_cloud, storage) = hybrid_with_mock_cloud();
        let first_name = "first.sst";
        let second_name = "second.sst";
        let first_bytes = valid_sst_bytes(b"a", b"v1", 1);
        let second_bytes = valid_sst_bytes(b"b", b"v2", 2);
        write_cloud_object(
            &storage,
            &crate::sst::object_key(first_name),
            first_bytes.clone(),
        );
        write_cloud_object(
            &storage,
            &crate::sst::object_key(second_name),
            second_bytes.clone(),
        );

        let first_manifest = manifest_for_ssts(&[(first_name, first_bytes.len() as u64)]);
        storage
            .verify_manifest_cloud_objects(&first_manifest)
            .expect("first manifest validation");

        mock_cloud.clear_history();
        let extended_manifest = crate::metadata::Manifest {
            files: vec![
                crate::metadata::FileMeta {
                    name: first_name.to_string(),
                    level: 0,
                    size_bytes: first_bytes.len() as u64,
                    cf_id: 0,
                    smallest_key: Some(b"a".to_vec()),
                    largest_key: Some(b"a".to_vec()),
                    smallest_seq: Some(1),
                    largest_seq: Some(1),
                    ..Default::default()
                },
                crate::metadata::FileMeta {
                    name: second_name.to_string(),
                    level: 0,
                    size_bytes: second_bytes.len() as u64,
                    cf_id: 0,
                    smallest_key: Some(b"b".to_vec()),
                    largest_key: Some(b"b".to_vec()),
                    smallest_seq: Some(2),
                    largest_seq: Some(2),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        storage
            .verify_manifest_cloud_objects(&extended_manifest)
            .expect("extended manifest validation");
        let downloads = mock_cloud.get_downloads();

        // Act
        // Assert
        assert!(
            downloads.iter().any(|key| key.ends_with("sst/second.sst")),
            "extended validation should read the new SST, got {downloads:?}"
        );
        assert!(
            downloads.iter().any(|key| key.ends_with("sst/first.sst")),
            "authoritative validation should reread the existing SST, got {downloads:?}"
        );
    }

    #[test]
    fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_deleted() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "deleted-after-proof.sst";
        let bytes = valid_sst_bytes(b"a", b"v1", 1);
        let key = crate::sst::object_key(sst_name);
        write_cloud_object(&storage, &key, bytes.clone());
        let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("initial manifest validation");
        delete_cloud_object(&storage, &key);

        let error = storage
            .verify_manifest_cloud_objects(&manifest)
            .expect_err("deleted SST must invalidate cached proof");
        // Act
        // Assert
        assert!(
            error.contains("changed since validation") || error.contains("unreadable"),
            "unexpected stale SST proof error: {error}"
        );
    }

    #[test]
    fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_overwritten() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "overwritten-after-proof.sst";
        let bytes = valid_sst_bytes(b"a", b"v1", 1);
        let key = crate::sst::object_key(sst_name);
        write_cloud_object(&storage, &key, bytes.clone());
        let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("initial manifest validation");
        write_cloud_object(&storage, &key, b"not a valid sst".to_vec());

        let error = storage
            .verify_manifest_cloud_objects(&manifest)
            .expect_err("overwritten SST must invalidate cached proof");
        // Act
        // Assert
        assert!(
            error.contains("changed since validation") || error.contains("size mismatch"),
            "unexpected stale SST proof error: {error}"
        );
    }

    #[test]
    fn should_reject_manifest_sst_when_content_crc_differs() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "wrong-crc.sst";
        let bytes = valid_sst_bytes(b"a", b"v1", 1);
        let key = crate::sst::object_key(sst_name);
        let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
        write_cloud_object(&storage, &key, bytes.clone());
        let manifest =
            manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

        let error = storage
            .verify_manifest_cloud_objects(&manifest)
            .expect_err("manifest SST with mismatched content CRC must not validate");

        // Act
        // Assert
        assert!(
            error.contains("crc") || error.contains("content"),
            "unexpected manifest SST CRC validation error: {error}"
        );
    }

    #[test]
    fn should_not_reuse_size_only_sst_proof_when_manifest_later_requires_crc() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "crc-after-size-proof.sst";
        let bytes = valid_sst_bytes(b"a", b"v1", 1);
        let key = crate::sst::object_key(sst_name);
        let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
        write_cloud_object(&storage, &key, bytes.clone());
        let size_only_manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);
        storage
            .verify_manifest_cloud_objects(&size_only_manifest)
            .expect("initial size-only manifest validation");
        let crc_manifest =
            manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

        let error = storage
            .verify_manifest_cloud_objects(&crc_manifest)
            .expect_err("cached size-only SST proof must not satisfy later CRC requirement");

        // Act
        // Assert
        assert!(
            error.contains("crc") || error.contains("content"),
            "unexpected cached SST CRC validation error: {error}"
        );
    }

    #[test]
    fn should_not_overwrite_existing_remote_sst_during_authoritative_upload() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "collision.sst";
        let key = crate::sst::object_key(sst_name);
        let existing_bytes = valid_sst_bytes(b"a", b"already-committed", 1);
        let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
        write_cloud_object(&storage, &key, existing_bytes.clone());

        let error = storage
            .write_sst_object(sst_name, upload_bytes)
            .expect_err("existing remote SST object must fail authoritative upload");

        // Act
        // Assert
        assert!(
            error.to_string().contains("cloud immutable upload failed")
                || error.to_string().contains("precondition failed"),
            "unexpected SST collision error: {error}"
        );
        assert_eq!(
            read_cloud_object(&storage, &key),
            existing_bytes,
            "authoritative SST upload must not overwrite an existing remote object"
        );
        assert_eq!(
            read_hybrid_object(&storage, &key),
            existing_bytes,
            "failed authoritative SST upload must not leave a conflicting local cache entry"
        );
    }

    #[test]
    fn should_not_create_remote_sst_when_local_cache_key_already_exists() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "local-collision.sst";
        let key = crate::sst::object_key(sst_name);
        let existing_bytes = valid_sst_bytes(b"a", b"local-already-committed", 1);
        let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
        write_local_object(&storage, &key, existing_bytes.clone());

        let error = storage
            .write_sst_object(sst_name, upload_bytes)
            .expect_err("existing local SST cache object must fail authoritative upload");

        // Act
        // Assert
        assert!(
            error.to_string().contains("local cache already exists"),
            "unexpected local SST collision error: {error}"
        );
        assert_eq!(
            read_local_object(&storage, &key),
            existing_bytes,
            "authoritative SST upload must not overwrite an existing local cache object"
        );
        assert_cloud_object_missing(&storage, &key);
    }

    #[test]
    fn should_resume_same_content_sst_publication_after_remote_only_success() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "remote-only-retry.sst";
        let key = crate::sst::object_key(sst_name);
        let bytes = valid_sst_bytes(b"retry", b"value", 7);
        write_cloud_object(&storage, &key, bytes.clone());
        assert_cloud_object_exists(&storage, &key);

        // Act
        storage
            .write_sst_object(sst_name, bytes.clone())
            .expect("same-content retry should finish local cache publication");

        // Assert
        assert_eq!(read_cloud_object(&storage, &key), bytes);
        assert_eq!(read_local_object(&storage, &key), bytes);
    }

    #[test]
    fn should_accept_same_content_sst_retry_when_both_copies_exist() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "fully-published-retry.sst";
        let key = crate::sst::object_key(sst_name);
        let bytes = valid_sst_bytes(b"retry", b"value", 8);
        storage
            .write_sst_object(sst_name, bytes.clone())
            .expect("publish both copies");

        // Act
        storage
            .write_sst_object(sst_name, bytes.clone())
            .expect("same-content retry must be idempotent");

        // Assert
        assert_eq!(read_cloud_object(&storage, &key), bytes);
        assert_eq!(read_local_object(&storage, &key), bytes);
    }

    #[test]
    fn should_revalidate_remote_wal_readback() {
        // Arrange
        let (mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 7;
        let max_sequence = 11;
        write_cloud_object(
            &storage,
            &crate::wal::cloud_segment::object_key(segment_id),
            valid_wal_bytes(max_sequence),
        );

        mock_cloud.clear_history();
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("first remote WAL validation");
        let first_downloads = mock_cloud.get_downloads();
        // Act
        // Assert
        assert!(
            first_downloads
                .iter()
                .any(|key| key.ends_with("wal/00000000000000000007.wal")),
            "first validation should read the cloud WAL, got {first_downloads:?}"
        );

        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("second remote WAL validation");
        let downloads = mock_cloud.get_downloads();
        assert_eq!(
            downloads
                .iter()
                .filter(|key| key.ends_with("wal/00000000000000000007.wal"))
                .count(),
            2,
            "authoritative WAL validation must reread the segment: {downloads:?}"
        );
    }

    #[test]
    fn should_reject_cached_remote_wal_proof_when_cloud_object_is_deleted() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 8;
        let max_sequence = 12;
        let key = crate::wal::cloud_segment::object_key(segment_id);
        write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));

        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        delete_cloud_object(&storage, &key);

        let error = storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect_err("deleted WAL must invalidate cached proof");
        // Act
        // Assert
        assert!(
            error.contains("changed since validation") || error.contains("unreadable"),
            "unexpected stale WAL proof error: {error}"
        );
    }

    #[test]
    fn should_not_prune_remote_wal_when_verified_object_identity_changed() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 11;
        let max_sequence = 21;
        let key = crate::wal::cloud_segment::object_key(segment_id);
        write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
        let stale_proof = storage
            .remote_object_proof(&key)
            .expect("initial remote object proof");

        write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
        storage
            .delete_remote_object_guarded(segment_id, stale_proof, Vec::new())
            .expect("schedule prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        // Act
        // Assert
        assert!(
            result.is_err(),
            "stale WAL proof must make remote prune fail conservatively"
        );
        assert_cloud_object_exists(&storage, &key);
    }

    #[test]
    fn should_not_prune_remote_wal_when_manifest_sst_disappears_after_initial_validation() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 13;
        let max_sequence = 23;
        let wal_key = crate::wal::cloud_segment::object_key(segment_id);
        let sst_name = "missing-after-validation.sst";
        let sst_key = crate::sst::object_key(sst_name);
        let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
        let manifest = manifest_covering_wal(sst_name, &sst_bytes, max_sequence, None);

        write_cloud_object(&storage, &wal_key, valid_wal_bytes(max_sequence));
        write_cloud_object(&storage, &sst_key, sst_bytes);
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("initial manifest SST validation");

        delete_cloud_object(&storage, &sst_key);
        let error = storage
            .prune_cloud_wal_segment(
                segment_id,
                max_sequence,
                CloudWalPruneGuard::new(manifest.clone(), None),
            )
            .expect_err("missing manifest SST must reject prune");

        // Act
        // Assert
        assert!(
            error.contains("unreadable") || error.contains("not found"),
            "unexpected missing manifest SST error: {error}"
        );
        assert_cloud_object_exists(&storage, &wal_key);
    }

    #[test]
    fn should_not_prune_remote_wal_when_manifest_sst_content_crc_differs() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 16;
        let max_sequence = 26;
        let wal_key = crate::wal::cloud_segment::object_key(segment_id);
        let sst_name = "wrong-crc-prune-guard.sst";
        let sst_key = crate::sst::object_key(sst_name);
        let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
        let wrong_crc = crc32c::crc32c(&sst_bytes) ^ 0xffff_ffff;
        let manifest = manifest_covering_wal(sst_name, &sst_bytes, max_sequence, Some(wrong_crc));

        write_cloud_object(&storage, &wal_key, valid_wal_bytes(max_sequence));
        write_cloud_object(&storage, &sst_key, sst_bytes);
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        let error = storage
            .prune_cloud_wal_segment(
                segment_id,
                max_sequence,
                CloudWalPruneGuard::new(manifest, None),
            )
            .expect_err("incorrect manifest CRC must reject prune");

        // Act
        // Assert
        assert!(
            error.contains("crc32c"),
            "unexpected manifest CRC error: {error}"
        );
        assert_cloud_object_exists(&storage, &wal_key);
    }

    #[test]
    fn should_not_prune_remote_wal_when_cloud_metadata_changes_after_initial_validation() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 14;
        let max_sequence = 24;
        let wal_key = crate::wal::cloud_segment::object_key(segment_id);
        let sst_name = "metadata-guard.sst";
        let sst_key = crate::sst::object_key(sst_name);
        let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
        let manifest = manifest_covering_wal(
            sst_name,
            &sst_bytes,
            max_sequence,
            Some(crc32c::crc32c(&sst_bytes)),
        );
        let metadata_backend = Arc::new(MockCloudBackend::new());
        let metadata_cloud = Arc::new(CloudStorage::new(
            metadata_backend,
            "metadata-test".to_string(),
        ));
        let metadata_key = crate::storage::cloud::cloud_metadata_key("manifest.json");
        let metadata_bytes = br#"{"last_persisted_sequence":24}"#.to_vec();

        write_cloud_object(&storage, &wal_key, valid_wal_bytes(max_sequence));
        write_cloud_object(&storage, &sst_key, sst_bytes);
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        write_cloud_metadata_object(&metadata_cloud, &metadata_key, metadata_bytes.clone());
        let metadata_guard = CloudMetadataPruneGuard::new(
            metadata_cloud.clone(),
            vec![CloudMetadataPruneProof {
                key: metadata_key.clone(),
                expected_bytes: metadata_bytes,
                remote: head_cloud_metadata_object(&metadata_cloud, &metadata_key),
            }],
        );

        write_cloud_metadata_object(&metadata_cloud, &metadata_key, b"changed".to_vec());
        storage
            .prune_cloud_wal_segment(
                segment_id,
                max_sequence,
                CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
            )
            .expect("schedule prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        // Act
        // Assert
        assert!(
            result.is_err(),
            "worker-side metadata revalidation must fail conservatively"
        );
        assert_cloud_object_exists(&storage, &wal_key);
    }

    #[test]
    fn should_prune_remote_wal_when_worker_side_guard_remains_valid() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 15;
        let max_sequence = 25;
        let wal_key = crate::wal::cloud_segment::object_key(segment_id);
        let sst_name = "guard-valid.sst";
        let sst_key = crate::sst::object_key(sst_name);
        let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
        let manifest = crate::metadata::Manifest {
            files: vec![crate::metadata::FileMeta {
                name: sst_name.to_string(),
                level: 0,
                size_bytes: sst_bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&sst_bytes)),
                cf_id: 0,
                smallest_key: Some(b"k".to_vec()),
                largest_key: Some(b"k".to_vec()),
                smallest_seq: Some(max_sequence),
                largest_seq: Some(max_sequence),
                ..Default::default()
            }],
            ..Default::default()
        };
        let metadata_backend = Arc::new(MockCloudBackend::new());
        let metadata_cloud = Arc::new(CloudStorage::new(
            metadata_backend,
            "metadata-test".to_string(),
        ));
        let metadata_key = crate::storage::cloud::cloud_metadata_key("manifest.json");
        let metadata_bytes = br#"{"last_persisted_sequence":25}"#.to_vec();

        write_cloud_object(&storage, &wal_key, valid_wal_bytes(max_sequence));
        write_cloud_object(&storage, &sst_key, sst_bytes.clone());
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("initial manifest SST validation");
        write_cloud_metadata_object(&metadata_cloud, &metadata_key, metadata_bytes.clone());
        let metadata_guard = CloudMetadataPruneGuard::new(
            metadata_cloud.clone(),
            vec![CloudMetadataPruneProof {
                key: metadata_key.clone(),
                expected_bytes: metadata_bytes,
                remote: head_cloud_metadata_object(&metadata_cloud, &metadata_key),
            }],
        );

        storage
            .prune_cloud_wal_segment(
                segment_id,
                max_sequence,
                CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
            )
            .expect("schedule prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        // Act
        // Assert
        assert!(
            result.is_ok(),
            "valid worker-side guard should allow conditional remote WAL deletion"
        );
        assert_cloud_object_missing(&storage, &wal_key);
    }

    #[test]
    fn should_reject_remote_wal_segment_with_sequence_beyond_expected_max() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 10;
        let expected_max_sequence = 20;
        write_cloud_object(
            &storage,
            &crate::wal::cloud_segment::object_key(segment_id),
            valid_wal_bytes(expected_max_sequence + 1),
        );

        let error = storage
            .verify_remote_wal_segment(segment_id, expected_max_sequence)
            .expect_err("WAL segment with records beyond expected max must be rejected");
        // Act
        // Assert
        assert!(
            error.contains("exceeds expected"),
            "unexpected WAL max-sequence error: {error}"
        );
    }

    #[test]
    fn should_not_overwrite_existing_remote_wal_during_upload() {
        // Arrange
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let tmp = tempfile::tempdir().expect("create WAL dir");
        let segment_id = 12;
        let upload_max_sequence = 22;
        let key = crate::wal::cloud_segment::object_key(segment_id);
        let existing_bytes = valid_wal_bytes(upload_max_sequence + 100);
        write_cloud_object(&storage, &key, existing_bytes.clone());

        let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
        std::fs::write(&wal_path, valid_wal_bytes(upload_max_sequence)).expect("write local WAL");
        storage
            .enqueue_wal_segment(segment_id, &wal_path, upload_max_sequence)
            .expect("enqueue WAL upload");
        storage.process_uploads();

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut saw_fail = false;
        let mut saw_ack = false;
        while Instant::now() < deadline {
            for event in storage.process_uploads() {
                match event {
                    StorageEvent::CloudFail {
                        segment_id: failed_segment,
                        ..
                    } if failed_segment == segment_id => saw_fail = true,
                    StorageEvent::CloudAck {
                        segment_id: acked_segment,
                        ..
                    } if acked_segment == segment_id => saw_ack = true,
                    _ => {}
                }
            }
            if saw_fail || saw_ack {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Act
        // Assert
        assert!(saw_fail, "existing remote WAL object must fail upload");
        assert!(
            !saw_ack,
            "existing remote WAL object must not produce a CloudAck"
        );
        assert_eq!(
            read_cloud_object(&storage, &key),
            existing_bytes,
            "upload must not overwrite an existing remote WAL object"
        );
    }

    #[test]
    fn should_readback_remote_wal_before_upload_worker_emits_ack() {
        // Arrange
        let (mock_cloud, storage) = hybrid_with_mock_cloud();
        let tmp = tempfile::tempdir().expect("create WAL dir");
        let segment_id = 9;
        let max_sequence = 13;
        let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
        std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");

        mock_cloud.clear_history();
        storage
            .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
            .expect("enqueue WAL upload");
        storage.process_uploads();

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut events = Vec::new();
        while Instant::now() < deadline {
            events.extend(storage.process_uploads());
            if events
                .iter()
                .any(|event| matches!(event, StorageEvent::CloudAck { .. }))
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Act
        // Assert
        assert!(
            events.iter().any(|event| matches!(
                event,
                StorageEvent::CloudAck {
                    segment_id: 9,
                    max_sequence: 13
                }
            )),
            "worker should emit CloudAck after upload and readback, got {events:?}"
        );
        assert!(
            mock_cloud
                .get_downloads()
                .iter()
                .any(|key| key.ends_with("wal/00000000000000000009.wal")),
            "upload worker must read back the remote WAL before ack"
        );
    }

    #[test]
    fn should_stop_retrying_failed_wal_upload_after_retry_budget_exhausted() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create WAL retry test dir");
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
                .expect("create local backend"),
        );
        let write_attempts = Arc::new(AtomicUsize::new(0));
        let cloud = Arc::new(AlwaysFailingWriteBackend::new(Arc::clone(&write_attempts)));
        let storage = HybridStorage::with_policy(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let segment_id = 11;
        let max_sequence = 21;
        let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
        std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");

        storage
            .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
            .expect("enqueue WAL upload");

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut observed_failures = 0usize;
        while Instant::now() < deadline {
            let events = storage.process_uploads();
            observed_failures += events
                .iter()
                .filter(|event| matches!(event, StorageEvent::CloudFail { .. }))
                .count();
            if storage.pending_upload_count() == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Act
        // Assert
        assert_eq!(
            write_attempts.load(Ordering::SeqCst),
            3,
            "permanently failing WAL uploads should stop after the retry budget"
        );
        assert_eq!(
            observed_failures, 3,
            "each failed WAL upload attempt should surface exactly one CloudFail"
        );
        assert_eq!(
            storage.pending_upload_count(),
            0,
            "exhausted WAL uploads must leave the queue so cleanup and shutdown are bounded"
        );
    }

    #[test]
    fn should_reject_wal_upload_when_entry_or_byte_capacity_is_exhausted() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create bounded upload test dir");
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
                .expect("create local backend"),
        );
        let cloud = Arc::new(NeverCompletesBackend::default());
        let first_path = tmp.path().join(crate::wal::segment_file_name(1));
        let second_path = tmp.path().join(crate::wal::segment_file_name(2));
        let wal_bytes = valid_wal_bytes(1);
        std::fs::write(&first_path, &wal_bytes).expect("write first WAL");
        std::fs::write(&second_path, &wal_bytes).expect("write second WAL");
        let limits = HybridQueueLimits {
            upload_entries: 1,
            upload_bytes: wal_bytes.len() as u64,
            callback_timeout: Duration::from_millis(20),
            ..HybridQueueLimits::default()
        };
        let storage = HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        );
        storage
            .enqueue_wal_segment(1, &first_path, 1)
            .expect("first upload must fit");
        assert!(matches!(
            storage.ensure_wal_write_admission(),
            Err(crate::common::MidgeError::WriteStall(_))
        ));

        // Act
        let error = storage
            .enqueue_wal_segment(2, &second_path, 1)
            .expect_err("second upload must be rejected at capacity");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::WriteStall(_)));
        assert!(matches!(
            storage.ensure_wal_write_admission(),
            Err(crate::common::MidgeError::WriteStall(_))
        ));
        assert_eq!(storage.pending_upload_count(), 1);
        assert_eq!(storage.pending_upload_bytes(), wal_bytes.len() as u64);
    }

    #[test]
    fn should_restore_wal_admission_after_upload_capacity_drains() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create admission release test dir");
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
                .expect("create local backend"),
        );
        let mock_cloud = Arc::new(MockCloudBackend::new());
        let cloud = Arc::new(CloudStorage::new(
            mock_cloud,
            "bounded-admission".to_string(),
        ));
        let limits = HybridQueueLimits {
            upload_entries: 1,
            ..HybridQueueLimits::default()
        };
        let storage = HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        );
        let first_path = tmp.path().join(crate::wal::segment_file_name(21));
        let second_path = tmp.path().join(crate::wal::segment_file_name(22));
        std::fs::write(&first_path, valid_wal_bytes(21)).expect("write first WAL");
        std::fs::write(&second_path, valid_wal_bytes(22)).expect("write second WAL");
        storage
            .enqueue_wal_segment(21, &first_path, 21)
            .expect("enqueue first upload");
        let _ = storage
            .enqueue_wal_segment(22, &second_path, 22)
            .expect_err("capacity must reject second upload");
        assert!(storage.ensure_wal_write_admission().is_err());

        // Act
        storage.process_uploads();
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline && storage.ensure_wal_write_admission().is_err() {
            storage.process_uploads();
            std::thread::sleep(Duration::from_millis(5));
        }

        // Assert
        storage
            .ensure_wal_write_admission()
            .expect("completed upload must reopen admission");
        assert_eq!(storage.pending_upload_count(), 0);
    }

    #[test]
    fn should_time_out_missing_cloud_upload_callback_without_hanging_worker() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create callback timeout test dir");
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
                .expect("create local backend"),
        );
        let cloud = Arc::new(NeverCompletesBackend::default());
        let limits = HybridQueueLimits {
            callback_timeout: Duration::from_millis(20),
            ..HybridQueueLimits::default()
        };
        let storage = HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        );
        let wal_path = tmp.path().join(crate::wal::segment_file_name(3));
        std::fs::write(&wal_path, valid_wal_bytes(3)).expect("write WAL");
        storage
            .enqueue_wal_segment(3, &wal_path, 3)
            .expect("enqueue WAL upload");
        storage.process_uploads();

        // Act
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut failure = None;
        while Instant::now() < deadline {
            failure = storage.process_uploads().into_iter().find_map(|event| {
                if let StorageEvent::CloudFail { error, .. } = event {
                    Some(error)
                } else {
                    None
                }
            });
            if failure.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        // Assert
        assert!(
            failure.is_some_and(|error| error.contains("timed out")),
            "missing callback must produce a bounded CloudFail"
        );
    }

    #[test]
    fn should_enforce_internal_storage_event_queue_limits() {
        // Arrange
        let ack = StorageEvent::CloudAck {
            segment_id: 1,
            max_sequence: 1,
        };
        let second_ack = StorageEvent::CloudAck {
            segment_id: 2,
            max_sequence: 2,
        };
        let ack_bytes = BoundedEventQueue::event_bytes(&ack);
        let mut entry_limited = BoundedEventQueue::new(1, ack_bytes * 2);
        let mut byte_limited = BoundedEventQueue::new(2, ack_bytes);

        // Act
        entry_limited
            .try_push(ack.clone(), false)
            .expect("first event fits entry bound");
        let entry_error = entry_limited
            .try_push(second_ack.clone(), false)
            .expect_err("second event exceeds entry bound");
        byte_limited
            .try_push(ack.clone(), false)
            .expect("first event fits byte bound");
        let byte_error = byte_limited
            .try_push(second_ack, false)
            .expect_err("second event exceeds byte bound");

        // Assert
        assert!(entry_error.contains("entries=1/1"));
        assert!(byte_error.contains("bytes="));
        assert_eq!(entry_limited.terminal_entries.len(), 1);
        assert_eq!(byte_limited.terminal_pending_bytes, ack_bytes);
    }
}
