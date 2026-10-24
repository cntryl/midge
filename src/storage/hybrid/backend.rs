//! Hybrid storage backend for cloud object publication and bounded uploads.
//!
//! CRITICAL ARCHITECTURE:
//!
//! `HybridStorage` owns the bounded upload pipeline:
//!    - `enqueue_object_upload()` - queue an explicit object key
//!    - `process_uploads()` - initiate cloud uploads
//!    - retain/coalesce terminal acknowledgements without unbounded channels
//!
//! WAL/SST key mapping, physical validation, manifest coverage, and prune policy
//! live in `runtime::hybrid_persistence`; this module never imports those formats.

/// Outcome of a failed conditional remote write.
///
/// `may_have_committed` distinguishes a provider that rejected the write from
/// one that may still apply it. Recovery uses this instead of matching phrases
/// in the error message, where a reworded message silently changed the path.
#[derive(Debug)]
pub(crate) struct RemoteCasFailure {
    pub(crate) may_have_committed: bool,
    pub(crate) error: crate::common::MidgeError,
}

impl RemoteCasFailure {
    /// The provider never admitted the mutation.
    pub(crate) fn not_committed(error: crate::common::MidgeError) -> Self {
        Self {
            may_have_committed: false,
            error,
        }
    }

    /// The mutation may still be applied, so the caller must resolve it
    /// against durable state.
    pub(crate) fn may_have_committed(error: crate::common::MidgeError) -> Self {
        Self {
            may_have_committed: true,
            error,
        }
    }
}

use crate::common::{MidgeError, MidgeResult};

use super::actor;
use super::policy;
use crate::storage::{StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome};
use crossbeam::channel as cb;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

mod control_io;
pub(crate) use control_io::{ControlObject, ControlWriteFailure};
mod file_publication;
pub(crate) use file_publication::ImmutableFileSource;
mod object_io;
mod proofs;
mod publication;
mod queues;
mod reservations;
mod uploads;

use proofs::PruneWorkerRegistry;
pub(crate) use proofs::{GuardedObjectProof, RemoteObjectProof};
#[cfg(test)]
pub(crate) use queues::WalUploadRetryPolicy;
use queues::{BoundedEventQueue, UploadQueue};
pub(crate) use queues::{HeldUploads, HybridQueueLimits};

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
    InFlight {
        started_at: Instant,
    },
    Completed,
    /// The last attempt failed; the next starts at `retry_at`.
    Failed {
        error: String,
        retry_at: Instant,
    },
}

/// State of a pending WAL segment upload
#[derive(Debug, Clone)]
pub struct UploadState {
    pub segment_id: u64,
    pub object_key: String,
    /// The file uploaded, read through the filesystem that wrote it.
    pub(crate) source: Arc<crate::io::FsLocation>,
    pub status: UploadStatus,
    pub max_sequence: u64,
    /// Failed attempts so far.
    pub retries: u32,
    size_bytes: u64,
}

pub use crate::types::HybridStorageBudgetSnapshot;

/// Cloud object stores coordinated with the local disk budget.
///
/// Managed by a Storage Budget Actor to enforce disk constraints, watermarks,
/// and coordination between resident local files and cloud durability.
///
/// `CloudAsync` Durability:
/// - Tracks pending WAL segment uploads
/// - Emits `CloudAck` when cloud confirms durability
/// - Handles retries and failure reporting
struct ObjectStores {
    sst: Arc<dyn StorageBackend>,
    wal: Arc<dyn StorageBackend>,
    control: Arc<dyn StorageBackend>,
}

/// The sender and worker must be closed together while the lease is held.
struct WalUploadWorker {
    tx: Option<mpsc::SyncSender<UploadState>>,
    handles: Vec<JoinHandle<()>>,
}

pub(crate) type StorageWriteAuthority = Arc<dyn Fn() -> MidgeResult<()> + Send + Sync>;

pub struct HybridStorage {
    /// Format-neutral object-store routing, separate from admission and worker ownership.
    stores: ObjectStores,
    /// Operational counters of the engine that owns this storage.
    counters: crate::telemetry::CounterSink,
    /// Storage Budget Actor for disk management
    budget_actor: Arc<Mutex<actor::StorageBudgetActor>>,
    /// Runtime SST files are disposable after remote publication, so the
    /// resident copy is evicted once its object is published.
    ephemeral_sst_cache: std::sync::atomic::AtomicBool,
    /// Pending WAL segment uploads (`CloudAsync` mode)
    upload_queue: Arc<Mutex<UploadQueue>>,
    /// Bounded confirmations waiting for the runtime's publication phase.
    verified_uploads: Arc<Mutex<HashMap<u64, uploads::VerifiedUpload>>>,
    /// Completed events ready for polling
    event_queue: Arc<Mutex<BoundedEventQueue>>,

    /// Optional external event sink for CloudAck/CloudFail.
    /// When set, upload completions are pushed directly to the runtime event loop
    /// to avoid polling latency.
    external_event_tx: Option<cb::Sender<StorageEvent>>,

    /// Dedicated WAL upload worker, joinable before the last storage owner drops.
    wal_upload_worker: Mutex<WalUploadWorker>,

    /// Maximum wait for a callback-based storage backend to respond.
    callback_timeout: Duration,
    immutable_retry: crate::storage::cloud::CloudRetryPolicy,

    /// Flag indicating if WAL upload worker thread failed to spawn
    upload_worker_failed: bool,

    /// Serializes in-process WAL catalog publication and retirement updates.
    /// Provider compare-exchange remains the cross-process authority boundary.
    wal_catalog_mutation: Mutex<()>,
    /// Provider-confirmed copies, consumed before a possibly ambiguous CAS:
    /// the primary's bytes, and only the mirror's CAS identity.
    wal_catalog_cache: Mutex<Option<(ControlObject, StorageObjectMetadata)>>,
    maintenance_memory: std::sync::OnceLock<crate::common::resource_budget::ResourceBudget>,
    write_authority: std::sync::OnceLock<StorageWriteAuthority>,

    /// Remote WAL prune workers are tracked so shutdown can join them before
    /// releasing the lease that fenced their conditional deletes.
    prune_workers: Arc<Mutex<PruneWorkerRegistry>>,
}

impl HybridStorage {
    /// Where this storage records operational events.
    pub(crate) fn counters(&self) -> &crate::telemetry::CounterSink {
        &self.counters
    }

    /// Create a new hybrid storage with an external event sender.
    ///
    /// When `external_event_tx` is provided, CloudAck/CloudFail will be sent to it
    /// as soon as they occur (in addition to internal bookkeeping), allowing the
    /// runtime to react without polling.
    pub fn new_with_event_sender(
        cloud: Arc<dyn StorageBackend>,
        external_event_tx: cb::Sender<StorageEvent>,
    ) -> Self {
        Self::with_policy_and_event_sender(
            cloud,
            policy::StorageBudgetPolicy::default(),
            Some(external_event_tx),
        )
    }

    /// Create hybrid storage with independently managed cloud object classes.
    #[cfg(test)]
    pub(crate) fn new_with_class_stores_and_event_sender(
        wal_cloud: Arc<dyn StorageBackend>,
        sst_cloud: Arc<dyn StorageBackend>,
        control_cloud: Arc<dyn StorageBackend>,
        external_event_tx: cb::Sender<StorageEvent>,
        callback_timeout: Duration,
    ) -> Self {
        Self::new_with_class_stores_upload_policy_and_event_sender(
            wal_cloud,
            sst_cloud,
            control_cloud,
            external_event_tx,
            callback_timeout,
            &crate::storage::cloud::CloudWritePolicy::default(),
        )
    }

    pub(crate) fn new_with_class_stores_upload_policy_and_event_sender(
        wal_cloud: Arc<dyn StorageBackend>,
        sst_cloud: Arc<dyn StorageBackend>,
        control_cloud: Arc<dyn StorageBackend>,
        external_event_tx: cb::Sender<StorageEvent>,
        callback_timeout: Duration,
        policy: &crate::storage::cloud::CloudWritePolicy,
    ) -> Self {
        Self::with_class_stores_policy_event_sender_and_limits(
            wal_cloud,
            sst_cloud,
            control_cloud,
            policy::StorageBudgetPolicy::default(),
            Some(external_event_tx),
            HybridQueueLimits {
                callback_timeout,
                worker_concurrency: policy.wal_upload_concurrency,
                upload_memory_bytes: policy.wal_upload_memory_bytes,
                ..HybridQueueLimits::default()
            },
        )
    }

    /// Create a new hybrid storage with a custom storage budget policy
    #[cfg(test)]
    pub fn with_policy(
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
    ) -> Self {
        Self::with_policy_and_event_sender(cloud, policy, None)
    }

    #[cfg(test)]
    pub(crate) fn with_test_upload_limits(
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
        upload_entries: usize,
        upload_bytes: u64,
    ) -> Self {
        Self::with_policy_event_sender_and_limits(
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
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
        external_event_tx: Option<cb::Sender<StorageEvent>>,
    ) -> Self {
        Self::with_policy_event_sender_and_limits(
            cloud,
            policy,
            external_event_tx,
            HybridQueueLimits::default(),
        )
    }

    pub(crate) fn with_policy_event_sender_and_limits(
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
        external_event_tx: Option<cb::Sender<StorageEvent>>,
        limits: HybridQueueLimits,
    ) -> Self {
        Self::with_class_stores_policy_event_sender_and_limits(
            Arc::clone(&cloud),
            Arc::clone(&cloud),
            cloud,
            policy,
            external_event_tx,
            limits,
        )
    }

    pub(crate) fn with_class_stores_policy_event_sender_and_limits(
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
            limits.wal_upload_retry,
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
        // A fixed pool uploads immutable objects; runtime catalog publication
        // remains a separate single-writer protocol.
        let (wal_upload_tx, wal_upload_rx) =
            mpsc::sync_channel::<UploadState>(limits.worker_entries.max(1));
        let counters = crate::telemetry::CounterSink::default();
        let verified_uploads = Arc::new(Mutex::new(HashMap::new()));
        let (upload_worker_handles, upload_worker_failed) = Self::spawn_wal_upload_worker(
            &counters,
            wal_upload_rx,
            Arc::clone(&wal_cloud),
            event_queue.clone(),
            external_event_tx.clone(),
            uploads::WalUploadWorkerConfig {
                timeout: limits.callback_timeout,
                concurrency: limits.worker_concurrency.clamp(1, 64),
                memory: crate::common::resource_budget::ResourceBudget::new(
                    limits.upload_memory_bytes,
                ),
            },
            Arc::clone(&verified_uploads),
        );

        Self {
            counters,
            stores: ObjectStores {
                sst: cloud,
                wal: wal_cloud,
                control: control_cloud,
            },
            budget_actor: Arc::new(Mutex::new(budget_actor)),
            ephemeral_sst_cache: std::sync::atomic::AtomicBool::new(false),
            upload_queue,
            verified_uploads,
            event_queue,
            external_event_tx,
            wal_upload_worker: Mutex::new(WalUploadWorker {
                tx: Some(wal_upload_tx),
                handles: upload_worker_handles,
            }),
            callback_timeout: limits.callback_timeout,
            immutable_retry: crate::storage::cloud::CloudRetryPolicy::default(),
            upload_worker_failed,
            wal_catalog_mutation: Mutex::new(()),
            wal_catalog_cache: Mutex::new(None),
            maintenance_memory: std::sync::OnceLock::new(),
            write_authority: std::sync::OnceLock::new(),
            prune_workers: Arc::new(Mutex::new(PruneWorkerRegistry::new(
                limits.prune_workers,
                limits.prune_requests,
            ))),
        }
    }

    pub(crate) fn configure_write_authority(
        &self,
        authority: StorageWriteAuthority,
    ) -> MidgeResult<()> {
        self.write_authority.set(authority).map_err(|_| {
            MidgeError::Internal("storage write authority was already configured".into())
        })
    }

    pub(crate) fn check_write_authority(&self) -> MidgeResult<()> {
        self.write_authority
            .get()
            .map_or(Ok(()), |validate| validate())
    }

    /// The caller holds the WAL catalog mutation lock across take and commit.
    pub(crate) fn take_wal_catalog_cache(&self) -> Option<(ControlObject, StorageObjectMetadata)> {
        self.wal_catalog_cache.lock().take()
    }

    pub(crate) fn cache_wal_catalog(&self, primary: ControlObject, mirror: ControlObject) {
        if primary.has_write_receipt() && mirror.has_write_receipt() {
            self.restore_wal_catalog_cache(primary, mirror.into_metadata());
        }
    }

    /// Return a taken cache entry the caller did not replace.
    pub(crate) fn restore_wal_catalog_cache(
        &self,
        primary: ControlObject,
        mirror: StorageObjectMetadata,
    ) {
        *self.wal_catalog_cache.lock() = Some((primary, mirror));
    }

    pub(crate) fn configure_maintenance_memory(
        &self,
        limit: usize,
    ) -> crate::common::resource_budget::ResourceBudget {
        self.maintenance_memory
            .get_or_init(|| crate::common::resource_budget::ResourceBudget::new(limit))
            .clone()
    }

    pub(crate) fn maintenance_memory(
        &self,
    ) -> Option<crate::common::resource_budget::ResourceBudget> {
        self.maintenance_memory.get().cloned()
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
    /// Close upload admission and wait for every accepted WAL upload attempt.
    /// The caller must retain the writer lease until this returns.
    pub(crate) fn shutdown_wal_upload_worker(&self) {
        let mut worker = self.wal_upload_worker.lock();
        worker.tx.take();
        for handle in worker.handles.drain(..) {
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

/// Raw per-class object-store handles for tests that assert object placement.
///
/// These expose the format-neutral routing only; no caller outside tests needs
/// a store handle, so the accessors stay behind `cfg(test)`.
#[cfg(test)]
impl HybridStorage {
    pub(crate) fn sst_store(&self) -> &Arc<dyn StorageBackend> {
        &self.stores.sst
    }
}

impl Drop for HybridStorage {
    fn drop(&mut self) {
        self.shutdown_background_workers();
        self.shutdown_wal_upload_worker();
    }
}

#[cfg(test)]
mod tests;
