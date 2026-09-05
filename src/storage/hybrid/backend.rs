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

mod object_io;
mod proofs;
mod publication;
mod queues;
mod reservations;
mod uploads;

use proofs::PruneWorkerRegistry;
pub(crate) use proofs::{GuardedObjectProof, RemoteObjectProof};
use queues::{BoundedEventQueue, HybridQueueLimits, UploadQueue};

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

#[derive(Debug, Clone, Copy, Default)]
pub struct HybridStorageBudgetSnapshot {
    pub max_local_bytes: u64,
    pub total_committed_bytes: u64,
    pub free_bytes: u64,
    pub usage_percent: u32,
    pub pending_evictions: usize,
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
    /// Immutable SST storage backend.
    cloud: Arc<dyn StorageBackend>,
    /// Sealed WAL storage backend.
    wal_cloud: Arc<dyn StorageBackend>,
    /// Mutable lease/metadata/DDL storage backend.
    control_cloud: Arc<dyn StorageBackend>,
    /// Storage Budget Actor for disk management
    budget_actor: Arc<Mutex<actor::StorageBudgetActor>>,
    /// Runtime SST files are disposable after remote publication. Avoid a
    /// second full local copy in the raw object backend for these engines.
    ephemeral_sst_cache: std::sync::atomic::AtomicBool,
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

    /// Serializes in-process WAL catalog publication and retirement updates.
    /// Provider compare-exchange remains the cross-process authority boundary.
    wal_catalog_mutation: Mutex<()>,

    /// Remote WAL prune workers are tracked so shutdown can join them before
    /// releasing the lease that fenced their conditional deletes.
    prune_workers: Mutex<PruneWorkerRegistry>,
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

    /// Create hybrid storage with independently managed cloud object classes.
    pub(crate) fn new_with_class_stores_and_event_sender(
        local: Arc<dyn StorageBackend>,
        wal_cloud: Arc<dyn StorageBackend>,
        sst_cloud: Arc<dyn StorageBackend>,
        control_cloud: Arc<dyn StorageBackend>,
        external_event_tx: cb::Sender<StorageEvent>,
        callback_timeout: Duration,
    ) -> Self {
        Self::with_class_stores_policy_event_sender_and_limits(
            local,
            wal_cloud,
            sst_cloud,
            control_cloud,
            policy::StorageBudgetPolicy::default(),
            Some(external_event_tx),
            HybridQueueLimits {
                callback_timeout,
                ..HybridQueueLimits::default()
            },
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

    #[cfg(test)]
    pub(crate) fn with_test_upload_limits(
        local: Arc<dyn StorageBackend>,
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
        upload_entries: usize,
        upload_bytes: u64,
    ) -> Self {
        Self::with_policy_event_sender_and_limits(
            local,
            cloud,
            policy,
            None,
            HybridQueueLimits {
                upload_entries,
                upload_bytes,
                ..HybridQueueLimits::default()
            },
        )
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
        Self::with_class_stores_policy_event_sender_and_limits(
            local,
            Arc::clone(&cloud),
            Arc::clone(&cloud),
            cloud,
            policy,
            external_event_tx,
            limits,
        )
    }

    fn with_class_stores_policy_event_sender_and_limits(
        local: Arc<dyn StorageBackend>,
        wal_cloud: Arc<dyn StorageBackend>,
        cloud: Arc<dyn StorageBackend>,
        control_cloud: Arc<dyn StorageBackend>,
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
            Arc::clone(&wal_cloud),
            event_queue.clone(),
            external_event_tx.clone(),
            limits.callback_timeout,
        );

        Self {
            local,
            cloud,
            wal_cloud,
            control_cloud,
            budget_actor: Arc::new(Mutex::new(budget_actor)),
            ephemeral_sst_cache: std::sync::atomic::AtomicBool::new(false),
            upload_queue,
            event_queue,
            external_event_tx,
            wal_upload_tx: Some(wal_upload_tx),
            callback_timeout: limits.callback_timeout,
            upload_worker_failed,
            upload_worker_handle,
            wal_catalog_mutation: Mutex::new(()),
            prune_workers: Mutex::new(PruneWorkerRegistry::new(
                limits.prune_workers,
                limits.prune_requests,
            )),
        }
    }

    /// Acquire the in-process WAL catalog mutation lock within the caller's budget.
    ///
    /// # Errors
    ///
    /// Returns [`crate::common::MidgeError::Timeout`] when a finite deadline
    /// expires before the current catalog mutation releases the lock.
    pub(crate) fn lock_wal_catalog_mutation_within(
        &self,
        deadline: &crate::common::OperationDeadline,
        operation: &str,
    ) -> crate::common::MidgeResult<parking_lot::MutexGuard<'_, ()>> {
        if !deadline.is_bounded() {
            return Ok(self.wal_catalog_mutation.lock());
        }

        let remaining = deadline.remaining();
        if remaining.is_zero() {
            return Err(crate::common::MidgeError::Timeout(format!(
                "{operation} timed out waiting to mutate the cloud WAL catalog"
            )));
        }
        self.wal_catalog_mutation
            .try_lock_for(remaining)
            .ok_or_else(|| {
                crate::common::MidgeError::Timeout(format!(
                    "{operation} timed out waiting to mutate the cloud WAL catalog"
                ))
            })
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
mod tests;
