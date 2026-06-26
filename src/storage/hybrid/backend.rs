//! Hybrid storage backend - combines local and cloud storage
//!
//! CRITICAL ARCHITECTURE:
//!
//! HybridStorage has TWO SEPARATE ROLES:
//!
//! 1. OBJECT STORAGE (SSTs, metadata):
//!    - submit_read/write/delete/list for SST files
//!    - Local + cloud merging/fallback
//!    - Cloud writes only for sst/ prefix
//!
//! 2. WAL DURABILITY PIPELINE (CloudAsync mode):
//!    - enqueue_wal_segment() - queue WAL for cloud upload
//!    - process_uploads() - initiate cloud uploads
//!    - poll() - retrieve CloudAck/CloudFail events
//!    - NEVER uses submit_write() for WAL
//!
//! WAL Flow:
//!   WalActor → local append barrier → memtable visibility →
//!   enqueue_wal_segment() → process_uploads() → cloud backend →
//!   CloudAck event → WalActor advances cloud durability bookkeeping
//!
//! SST Flow:
//!   Engine → submit_write() → local write + cloud write (if sst/) → done

use super::actor;
use super::policy;
use super::state;
use crate::storage::{
    StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome,
};
use crossbeam::channel as cb;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
    pub local_path: PathBuf,
    pub status: UploadStatus,
    pub max_sequence: u64,
    pub retries: u32,
}

#[derive(Debug, Clone)]
struct VerifiedWalSegment {
    max_sequence: u64,
    data_records: Vec<WalDataCoverageRecord>,
    metadata: StorageObjectMetadata,
}

#[derive(Debug, Clone, Copy)]
struct WalSegmentValidation {
    max_sequence: u64,
}

#[derive(Debug, Clone)]
struct WalSegmentReadback {
    validation: WalSegmentValidation,
    data_records: Vec<WalDataCoverageRecord>,
}

#[derive(Debug, Clone)]
struct WalDataCoverageRecord {
    cf_id: u32,
    op: crate::wal::WalOpKind,
    key: Vec<u8>,
    range_end: Option<Vec<u8>>,
    seq: u64,
}

#[derive(Debug, Clone)]
struct VerifiedCloudObject {
    metadata: StorageObjectMetadata,
    content_crc32c: Option<u32>,
    summary: Option<crate::sst::fs::SstFileSummary>,
}

#[derive(Clone)]
pub(crate) struct CloudMetadataPruneGuard {
    cloud: Arc<crate::storage::cloud::CloudStorage>,
    objects: Vec<CloudMetadataPruneProof>,
}

impl CloudMetadataPruneGuard {
    pub(crate) fn new(
        cloud: Arc<crate::storage::cloud::CloudStorage>,
        objects: Vec<CloudMetadataPruneProof>,
    ) -> Self {
        Self { cloud, objects }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CloudMetadataPruneProof {
    pub(crate) key: String,
    pub(crate) expected_bytes: Vec<u8>,
    pub(crate) remote: StorageObjectMetadata,
}

#[derive(Clone, Default)]
pub(crate) struct CloudWalPruneGuard {
    manifest: crate::metadata::Manifest,
    metadata: Option<CloudMetadataPruneGuard>,
}

impl CloudWalPruneGuard {
    pub(crate) fn new(
        manifest: crate::metadata::Manifest,
        metadata: Option<CloudMetadataPruneGuard>,
    ) -> Self {
        Self { manifest, metadata }
    }
}

/// Hybrid storage combining local filesystem and cloud backends
///
/// Managed by a Storage Budget Actor to enforce disk constraints, watermarks,
/// and coordination between local caching and cloud durability.
///
/// CloudAsync Durability:
/// - Tracks pending WAL segment uploads
/// - Emits CloudAck when cloud confirms durability
/// - Handles retries and failure reporting
pub struct HybridStorage {
    /// Local storage backend (usually filesystem)
    local: Arc<dyn StorageBackend>,
    /// Cloud storage backend (S3, GCS, Azure, etc.)
    cloud: Arc<dyn StorageBackend>,
    /// Storage Budget Actor for disk management
    budget_actor: Arc<Mutex<actor::StorageBudgetActor>>,
    /// Pending WAL segment uploads (CloudAsync mode)
    upload_queue: Arc<Mutex<VecDeque<UploadState>>>,
    /// Completed events ready for polling
    event_queue: Arc<Mutex<VecDeque<StorageEvent>>>,

    /// WAL segments whose remote object was read back and decoded successfully.
    verified_wal_segments: Arc<Mutex<HashMap<u64, VerifiedWalSegment>>>,

    /// Immutable SST objects whose remote object was read back and opened successfully.
    verified_sst_objects: Arc<Mutex<HashMap<String, VerifiedCloudObject>>>,

    /// Optional external event sink for CloudAck/CloudFail.
    /// When set, upload completions are pushed directly to the runtime event loop
    /// to avoid polling latency.
    external_event_tx: Option<cb::Sender<StorageEvent>>,

    /// Dedicated WAL upload worker sender.
    wal_upload_tx: Option<mpsc::Sender<UploadState>>,

    /// Flag indicating if WAL upload worker thread failed to spawn
    upload_worker_failed: bool,

    /// Background WAL upload worker thread handle
    upload_worker_handle: Option<JoinHandle<()>>,
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
        let budget_actor = actor::StorageBudgetActor::new(policy);

        let upload_queue: Arc<Mutex<VecDeque<UploadState>>> = Arc::new(Mutex::new(VecDeque::new()));
        let event_queue: Arc<Mutex<VecDeque<StorageEvent>>> = Arc::new(Mutex::new(VecDeque::new()));
        let verified_wal_segments: Arc<Mutex<HashMap<u64, VerifiedWalSegment>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let verified_sst_objects: Arc<Mutex<HashMap<String, VerifiedCloudObject>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Single background worker for WAL uploads.
        // This avoids spawning one OS thread per segment, which is extremely
        // expensive under CloudAsync + synchronous write APIs (e.g. 10k puts).
        let (wal_upload_tx, wal_upload_rx) = mpsc::channel::<UploadState>();
        let mut upload_worker_failed = false;
        let upload_worker_handle = {
            let cloud = Arc::clone(&cloud);
            let event_queue = Arc::clone(&event_queue);
            let external_event_tx = external_event_tx.clone();
            let verified_wal_segments = Arc::clone(&verified_wal_segments);

            let spawn_result = thread::Builder::new()
                .name("midge-wal-uploader".to_string())
                .spawn(move || {
                    while let Ok(upload) = wal_upload_rx.recv() {
                        let upload_start = Instant::now();
                        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                            telemetry.metrics().record_cloud_async_wal_upload_started();
                        }

                        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
                            && upload.segment_id % 1000 == 0
                        {
                            eprintln!(
                                "[midge] CloudAsync upload start: segment_id={} max_sequence={} path={:?}",
                                upload.segment_id, upload.max_sequence, upload.local_path
                            );
                        }

                        let data = match std::fs::read(&upload.local_path) {
                            Ok(data) => data,
                            Err(e) => {
                                let error = format!("read {:?}: {}", upload.local_path, e);
                                let mut events = event_queue.lock();
                                events.push_back(StorageEvent::CloudFail {
                                    segment_id: upload.segment_id,
                                    error: error.clone(),
                                });
                                continue;
                            }
                        };

                        let bytes = data.len() as u64;
                        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                            telemetry.metrics().record_cloud_upload(bytes);
                        }

                        let forced_failure =
                            fail::eval("midge::cloud::inject_fail_wal_upload", |_| true)
                                .unwrap_or(false);
                        if forced_failure {
                            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                telemetry.metrics().record_cloud_async_wal_upload_failed();
                            }
                            let mut events = event_queue.lock();
                            let fail = StorageEvent::CloudFail {
                                segment_id: upload.segment_id,
                                error: "failpoint: cloud WAL upload failed".to_string(),
                            };
                            events.push_back(fail.clone());
                            if let Some(tx) = &external_event_tx {
                                let _ = tx.send(fail);
                            }
                            continue;
                        }

                        let (tx, rx) = std::sync::mpsc::channel();
                        let cloud_key = crate::wal::cloud_segment_object_key(upload.segment_id);
                        cloud.submit_write_with_headers(
                            cloud_key,
                            data,
                            vec![("If-None-Match".into(), "*".into())],
                            tx,
                        );

                        match rx.recv() {
                            Ok(StorageEvent::WriteComplete { result, .. }) if result.is_ok() => {
                                if let Err(error) = Self::verify_remote_wal_segment_with_backend(
                                    &cloud,
                                    &verified_wal_segments,
                                    upload.segment_id,
                                    upload.max_sequence,
                                ) {
                                    if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                        telemetry.metrics().record_cloud_async_wal_upload_failed();
                                    }
                                    let fail = StorageEvent::CloudFail {
                                        segment_id: upload.segment_id,
                                        error: format!(
                                            "remote WAL readback validation failed: {error}"
                                        ),
                                    };
                                    let mut events = event_queue.lock();
                                    events.push_back(fail.clone());
                                    if let Some(tx) = &external_event_tx {
                                        let _ = tx.send(fail);
                                    }
                                    continue;
                                }

                                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                    telemetry
                                        .metrics()
                                        .record_cloud_async_wal_upload_completed(upload_start.elapsed().as_micros() as u64);
                                }
                                let mut events = event_queue.lock();
                                let ack = StorageEvent::CloudAck {
                                    segment_id: upload.segment_id,
                                    max_sequence: upload.max_sequence,
                                };
                                events.push_back(ack.clone());
                                if let Some(tx) = &external_event_tx {
                                    let _ = tx.send(ack);
                                }

                                if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
                                    && upload.segment_id % 1000 == 0
                                {
                                    eprintln!(
                                        "[midge] CloudAsync upload ack: segment_id={} max_sequence={}",
                                        upload.segment_id, upload.max_sequence
                                    );
                                }
                            }
                            Ok(StorageEvent::WriteComplete { result, .. }) => {
                                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                    telemetry.metrics().record_cloud_async_wal_upload_failed();
                                }
                                let error = match result {
                                    StorageOutcome::Err(e) => e,
                                    _ => "Unknown error".to_string(),
                                };
                                let mut events = event_queue.lock();
                                let fail = StorageEvent::CloudFail {
                                    segment_id: upload.segment_id,
                                    error: error.clone(),
                                };
                                events.push_back(fail.clone());
                                if let Some(tx) = &external_event_tx {
                                    let _ = tx.send(fail);
                                }
                            }
                            _ => {
                                let error = "Channel error".to_string();
                                let mut events = event_queue.lock();
                                let fail = StorageEvent::CloudFail {
                                    segment_id: upload.segment_id,
                                    error: error.clone(),
                                };
                                events.push_back(fail.clone());
                                if let Some(tx) = &external_event_tx {
                                    let _ = tx.send(fail);
                                }
                            }
                        }
                    }
                });

            match spawn_result {
                Ok(handle) => Some(handle),
                Err(e) => {
                    tracing::error!("Failed to spawn WAL upload worker: {}", e);
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_thread_spawn_failure();
                    }
                    upload_worker_failed = true;
                    None
                }
            }
        };

        Self {
            local,
            cloud,
            budget_actor: Arc::new(Mutex::new(budget_actor)),
            upload_queue,
            event_queue,
            verified_wal_segments,
            verified_sst_objects,
            external_event_tx,
            wal_upload_tx: Some(wal_upload_tx),
            upload_worker_failed,
            upload_worker_handle,
        }
    }

    /// Enqueue a WAL segment for cloud upload (CloudAsync mode)
    ///
    /// This is the WAL durability pipeline entry point.
    pub fn enqueue_wal_segment(&self, segment_id: u64, local_path: PathBuf, max_sequence: u64) {
        let upload_state = UploadState {
            segment_id,
            local_path: local_path.clone(),
            status: UploadStatus::Pending,
            max_sequence,
            retries: 0,
        };

        let mut queue = self.upload_queue.lock();

        // CRITICAL: Phase 3.2 - HybridStorage unbounded queue backpressure
        // Prevent runaway queue growth when cloud uploads can't keep up with WAL generation.
        // If queue exceeds 1000 segments (~1TB of local WAL), log critical warning.
        // This indicates WAL generation rate >> cloud upload throughput.
        if queue.len() >= 1000 {
            tracing::error!(
                queue_size = queue.len(),
                segment_id,
                "CloudAsync WAL upload queue exceeded critical threshold (1000 segments); \
                 WAL generation rate may exceed cloud upload capacity. \
                 This may indicate misconfigured cloud credentials, network issues, or insufficient cloud throughput."
            );
        } else if queue.len() >= 100 {
            // Warn at 100 segments to give operators early signal
            tracing::warn!(
                queue_size = queue.len(),
                segment_id,
                "CloudAsync WAL upload queue growing; cloud uploads may be slow"
            );
        }

        queue.push_back(upload_state);

        tracing::debug!(
            segment_id,
            ?local_path,
            max_sequence,
            queue_size = queue.len(),
            "WAL segment enqueued for cloud upload"
        );
    }

    /// Process pending uploads (should be called periodically by runtime)
    ///
    /// **WAL DURABILITY PIPELINE**
    ///
    /// Initiates cloud uploads for pending WAL segments.
    /// This is the ONLY place where WAL segments are uploaded to cloud.
    /// - Reads pending uploads from upload_queue
    /// - Initiates cloud upload via cloud backend (not submit_write)
    /// - Handles retries on failure (up to 3 attempts)
    /// - Emits CloudAck/CloudFail events to event_queue
    ///
    /// Non-blocking - actual uploads happen asynchronously in the dedicated worker thread.
    ///
    /// Returns any drained CloudAck/CloudFail events for the runtime to consume.
    pub fn process_uploads(&self) -> Vec<StorageEvent> {
        // 1) Drain worker completion events first.
        let drained_events = {
            let mut events = self.event_queue.lock();
            events.drain(..).collect::<Vec<_>>()
        };

        let mut queue = self.upload_queue.lock();

        // 2) Apply drained events to the upload state machine.
        for event in &drained_events {
            match event {
                StorageEvent::CloudAck {
                    segment_id,
                    max_sequence: _,
                } => {
                    if let Some(item) = queue.iter_mut().find(|u| &u.segment_id == segment_id) {
                        item.status = UploadStatus::Completed;
                    }
                }
                StorageEvent::CloudFail { segment_id, error } => {
                    if let Some(item) = queue.iter_mut().find(|u| &u.segment_id == segment_id) {
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

        // If we have an external event channel, CloudAck/CloudFail were already pushed.
        // We still must schedule uploads here; we only suppress returning events to
        // avoid double-application in the runtime.
        let suppress_return_events = self.external_event_tx.is_some();

        // 3) Schedule any eligible uploads.
        let now = Instant::now();
        for upload in queue.iter_mut() {
            let eligible = match upload.status {
                UploadStatus::Pending => true,
                UploadStatus::Failed { .. } => upload.retries < 3,
                UploadStatus::InFlight { .. } | UploadStatus::Completed => false,
            };
            if !eligible {
                continue;
            }

            upload.status = UploadStatus::InFlight { started_at: now };

            let forced_failure =
                fail::eval("midge::cloud::inject_fail_wal_upload", |_| true).unwrap_or(false);
            if forced_failure {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_failed();
                }

                let fail = StorageEvent::CloudFail {
                    segment_id: upload.segment_id,
                    error: "failpoint: cloud WAL upload failed".to_string(),
                };
                {
                    let mut events = self.event_queue.lock();
                    events.push_back(fail.clone());
                }
                if let Some(tx) = &self.external_event_tx {
                    let _ = tx.send(fail);
                }
                continue;
            }

            // Send to the dedicated worker; avoid per-upload thread spawn.
            // If worker failed to spawn, perform inline upload as fallback.
            if self.upload_worker_failed {
                self.process_upload_inline(upload.clone());
            } else if self
                .wal_upload_tx
                .as_ref()
                .is_none_or(|tx| tx.send(upload.clone()).is_err())
            {
                // Worker thread died unexpectedly - fall back to inline mode
                tracing::warn!(
                    segment_id = upload.segment_id,
                    "WAL upload worker unavailable, falling back to inline upload"
                );
                self.process_upload_inline(upload.clone());
            }
        }

        // 4) Garbage-collect finished items (Completed or Failed after 3 attempts).
        queue.retain(|u| match &u.status {
            UploadStatus::Completed => false,
            UploadStatus::Failed { .. } => u.retries < 3,
            _ => true,
        });

        if suppress_return_events {
            Vec::new()
        } else {
            drained_events
        }
    }

    /// Try to reserve space for an upcoming flush.
    pub fn reserve_for_flush(&self, est_size: u64) -> actor::ReservationResult {
        let mut actor = self.budget_actor.lock();
        actor
            .handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size })
            .unwrap_or(actor::ReservationResult::Ok)
    }

    /// Process a single WAL upload inline (fallback when worker thread unavailable)
    ///
    /// This is a fallback path used when:
    /// - The background upload worker failed to spawn
    /// - The worker thread died unexpectedly
    ///
    /// Performs the upload synchronously in the caller's context (typically runtime thread).
    /// This may add latency but prevents deadlock when resources are constrained.
    fn process_upload_inline(&self, upload: UploadState) {
        let upload_start = Instant::now();
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_async_wal_upload_started();
        }

        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
            && upload.segment_id.is_multiple_of(1000)
        {
            eprintln!(
                "[midge] CloudAsync inline upload start: segment_id={} max_sequence={} path={:?}",
                upload.segment_id, upload.max_sequence, upload.local_path
            );
        }

        // Read file
        let data = match std::fs::read(&upload.local_path) {
            Ok(data) => data,
            Err(e) => {
                let error = format!("read {:?}: {}", upload.local_path, e);
                let mut events = self.event_queue.lock();
                events.push_back(StorageEvent::CloudFail {
                    segment_id: upload.segment_id,
                    error: error.clone(),
                });
                if let Some(tx) = &self.external_event_tx {
                    let _ = tx.send(StorageEvent::CloudFail {
                        segment_id: upload.segment_id,
                        error,
                    });
                }
                return;
            }
        };

        let bytes = data.len() as u64;
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_upload(bytes);
        }

        let forced_failure =
            fail::eval("midge::cloud::inject_fail_wal_upload", |_| true).unwrap_or(false);
        if forced_failure {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_cloud_async_wal_upload_failed();
            }
            let mut events = self.event_queue.lock();
            events.push_back(StorageEvent::CloudFail {
                segment_id: upload.segment_id,
                error: "failpoint: cloud WAL upload failed".to_string(),
            });
            if let Some(tx) = &self.external_event_tx {
                let _ = tx.send(StorageEvent::CloudFail {
                    segment_id: upload.segment_id,
                    error: "failpoint: cloud WAL upload failed".to_string(),
                });
            }
            return;
        }

        // Submit to cloud backend
        let (tx, rx) = std::sync::mpsc::channel();
        let cloud_key = crate::wal::cloud_segment_object_key(upload.segment_id);
        self.cloud.submit_write_with_headers(
            cloud_key,
            data,
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );

        // Wait for completion
        match rx.recv() {
            Ok(StorageEvent::WriteComplete { result, .. }) if result.is_ok() => {
                if let Err(error) =
                    self.verify_remote_wal_segment(upload.segment_id, upload.max_sequence)
                {
                    if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                        telemetry.metrics().record_cloud_async_wal_upload_failed();
                    }
                    let fail = StorageEvent::CloudFail {
                        segment_id: upload.segment_id,
                        error: format!("remote WAL readback validation failed: {error}"),
                    };
                    let mut events = self.event_queue.lock();
                    events.push_back(fail.clone());
                    if let Some(tx) = &self.external_event_tx {
                        let _ = tx.send(fail);
                    }
                    return;
                }

                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_completed(
                        upload_start.elapsed().as_micros() as u64,
                    );
                }
                let mut events = self.event_queue.lock();
                let ack = StorageEvent::CloudAck {
                    segment_id: upload.segment_id,
                    max_sequence: upload.max_sequence,
                };
                events.push_back(ack.clone());
                if let Some(tx) = &self.external_event_tx {
                    let _ = tx.send(ack);
                }

                if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
                    && upload.segment_id.is_multiple_of(1000)
                {
                    eprintln!(
                        "[midge] CloudAsync inline upload ack: segment_id={} max_sequence={}",
                        upload.segment_id, upload.max_sequence
                    );
                }
            }
            Ok(StorageEvent::WriteComplete { result, .. }) => {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_failed();
                }
                let error = match result {
                    StorageOutcome::Err(e) => e,
                    _ => "Unknown error".to_string(),
                };
                let mut events = self.event_queue.lock();
                let fail = StorageEvent::CloudFail {
                    segment_id: upload.segment_id,
                    error: error.clone(),
                };
                events.push_back(fail.clone());
                if let Some(tx) = &self.external_event_tx {
                    let _ = tx.send(fail);
                }
            }
            _ => {
                let error = "Channel error".to_string();
                let mut events = self.event_queue.lock();
                let fail = StorageEvent::CloudFail {
                    segment_id: upload.segment_id,
                    error: error.clone(),
                };
                events.push_back(fail.clone());
                if let Some(tx) = &self.external_event_tx {
                    let _ = tx.send(fail);
                }
            }
        }
    }

    /// Signal that a flush completed with actual size
    pub fn flush_completed(&self, actual_size: u64) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.handle_event(actor::StorageBudgetEvent::FlushCompleted { actual_size });
    }

    /// Signal that a cloud upload completed
    pub fn cloud_upload_completed(&self, sst_id: u64, actual_size: u64) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.handle_event(actor::StorageBudgetEvent::CloudUploadCompleted {
            sst_id,
            actual_size,
        });
    }

    /// Signal that compaction is starting
    pub fn compaction_planned(&self, input_sizes: Vec<u64>) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.handle_event(actor::StorageBudgetEvent::CompactionPlanned { input_sizes });
    }

    /// Signal that compaction completed
    pub fn compaction_completed(&self, output_sizes: Vec<u64>) {
        let mut actor = self.budget_actor.lock();
        let _ = actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted { output_sizes });
    }

    /// Get current disk state snapshot
    pub fn disk_state(&self) -> state::DiskState {
        let actor = self.budget_actor.lock();
        actor.disk_state()
    }

    /// Get mutable access to the budget actor for testing and monitoring
    pub fn budget_actor(
        &self,
    ) -> Result<parking_lot::MutexGuard<'_, actor::StorageBudgetActor>, String> {
        Ok(self.budget_actor.lock())
    }

    /// Get count of pending uploads (for monitoring)
    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue.lock().len()
    }

    fn read_cloud_object_from_backend_blocking(
        cloud: &Arc<dyn StorageBackend>,
        key: &str,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_read(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
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

    fn read_cloud_object_blocking(&self, key: &str) -> Result<Vec<u8>, String> {
        Self::read_cloud_object_from_backend_blocking(&self.cloud, key)
    }

    fn head_cloud_object_from_backend_blocking(
        cloud: &Arc<dyn StorageBackend>,
        key: &str,
    ) -> Result<StorageObjectMetadata, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(metadata),
                ..
            }) => Ok(metadata),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(format!(
                "cloud object '{key}' unreadable during cached proof revalidation: {error}"
            )),
            Ok(other) => Err(format!(
                "unexpected cloud HEAD response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("cloud HEAD timed out for '{key}': {error}")),
        }
    }

    fn head_cloud_object_blocking(&self, key: &str) -> Result<StorageObjectMetadata, String> {
        Self::head_cloud_object_from_backend_blocking(&self.cloud, key)
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
        backend.submit_head(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(_),
                ..
            }) => Ok(true),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if Self::storage_error_indicates_missing(&error) => Ok(false),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(format!("object '{key}' HEAD failed: {error}")),
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
        backend.submit_delete(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => Ok(true),
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if Self::storage_error_indicates_missing(&error) => Ok(false),
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(format!("object '{key}' delete failed: {error}")),
            Ok(other) => Err(format!(
                "unexpected storage delete response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("storage delete timed out for '{key}': {error}")),
        }
    }

    fn verify_cloud_object_proof(
        cloud: &Arc<dyn StorageBackend>,
        key: &str,
        expected: &StorageObjectMetadata,
    ) -> Result<(), String> {
        let actual = Self::head_cloud_object_from_backend_blocking(cloud, key)?;
        if &actual == expected {
            return Ok(());
        }

        Err(format!(
            "cloud object '{key}' changed since validation: expected {expected:?}, actual {actual:?}"
        ))
    }

    fn validate_wal_segment_bytes(
        key: &str,
        data: &[u8],
        expected_max_sequence: u64,
    ) -> Result<WalSegmentReadback, String> {
        if data.is_empty() {
            return Err(format!("cloud WAL segment '{key}' is empty"));
        }

        let mut pos = 0usize;
        let mut records = 0usize;
        let mut observed_max_sequence = 0u64;
        let mut data_records = Vec::new();
        while pos < data.len() {
            let header_end = pos
                .checked_add(crate::wal::frame::WAL_FRAME_HEADER_LEN)
                .ok_or_else(|| format!("cloud WAL segment '{key}' frame offset overflow"))?;
            if header_end > data.len() {
                return Err(format!(
                    "cloud WAL segment '{key}' has incomplete frame header at offset {pos}"
                ));
            }

            let (payload_len, expected_crc) =
                crate::wal::frame::decode_frame_header(&data[pos..header_end])
                    .map_err(|error| format!("cloud WAL segment '{key}' frame header: {error}"))?;
            let payload_end = header_end
                .checked_add(payload_len)
                .ok_or_else(|| format!("cloud WAL segment '{key}' payload offset overflow"))?;
            if payload_end > data.len() {
                return Err(format!(
                    "cloud WAL segment '{key}' has incomplete record at offset {pos}"
                ));
            }

            let payload = &data[header_end..payload_end];
            crate::wal::frame::verify_frame_crc(payload, expected_crc)
                .map_err(|error| format!("cloud WAL segment '{key}' frame CRC: {error}"))?;
            let record = crate::wal::encoding::decode(payload)
                .map_err(|error| format!("cloud WAL segment '{key}' record decode: {error}"))?;
            observed_max_sequence = observed_max_sequence.max(record.seq);
            if !matches!(
                record.op,
                crate::wal::WalOpKind::TxnBegin | crate::wal::WalOpKind::TxnCommit
            ) {
                data_records.push(WalDataCoverageRecord {
                    cf_id: record.cf_id,
                    op: record.op,
                    key: record.key.to_vec(),
                    range_end: record.range_end.map(|range_end| range_end.to_vec()),
                    seq: record.seq,
                });
            }
            records += 1;
            pos = payload_end;
        }

        if records == 0 {
            return Err(format!("cloud WAL segment '{key}' contains no records"));
        }
        if observed_max_sequence < expected_max_sequence {
            return Err(format!(
                "cloud WAL segment '{key}' max sequence {observed_max_sequence} is below expected {expected_max_sequence}"
            ));
        }
        if observed_max_sequence > expected_max_sequence {
            return Err(format!(
                "cloud WAL segment '{key}' max sequence {observed_max_sequence} exceeds expected {expected_max_sequence}"
            ));
        }

        Ok(WalSegmentReadback {
            validation: WalSegmentValidation {
                max_sequence: observed_max_sequence,
            },
            data_records,
        })
    }

    fn is_verified_wal_segment(
        verified_wal_segments: &Arc<Mutex<HashMap<u64, VerifiedWalSegment>>>,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> bool {
        verified_wal_segments
            .lock()
            .get(&segment_id)
            .is_some_and(|proof| proof.max_sequence == expected_max_sequence)
    }

    pub fn is_remote_wal_segment_verified(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> bool {
        Self::is_verified_wal_segment(
            &self.verified_wal_segments,
            segment_id,
            expected_max_sequence,
        )
    }

    pub fn verify_cached_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String> {
        let key = crate::wal::cloud_segment_object_key(segment_id);
        let Some(proof) = self.verified_wal_segments.lock().get(&segment_id).cloned() else {
            return Err(format!(
                "cloud WAL segment {segment_id} has no prior readback proof for max sequence {expected_max_sequence}"
            ));
        };
        if proof.max_sequence != expected_max_sequence {
            return Err(format!(
                "cached cloud WAL segment '{key}' max sequence {} does not match expected {expected_max_sequence}",
                proof.max_sequence
            ));
        }
        Self::verify_cloud_object_proof(&self.cloud, &key, &proof.metadata)
    }

    pub(crate) fn cached_remote_wal_segment_covered_by_manifest(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        manifest: &crate::metadata::Manifest,
    ) -> Result<bool, String> {
        let proof = self.cached_remote_wal_segment_proof(segment_id, expected_max_sequence)?;
        Ok(Self::wal_data_records_covered_by_manifest(
            &proof.data_records,
            manifest,
        ))
    }

    fn cached_remote_wal_segment_proof(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<VerifiedWalSegment, String> {
        let key = crate::wal::cloud_segment_object_key(segment_id);
        let Some(proof) = self.verified_wal_segments.lock().get(&segment_id).cloned() else {
            return Err(format!(
                "cloud WAL segment {segment_id} has no prior readback proof for max sequence {expected_max_sequence}"
            ));
        };
        if proof.max_sequence != expected_max_sequence {
            return Err(format!(
                "cached cloud WAL segment '{key}' max sequence {} does not match expected {expected_max_sequence}",
                proof.max_sequence
            ));
        }
        Ok(proof)
    }

    fn wal_data_records_covered_by_manifest(
        data_records: &[WalDataCoverageRecord],
        manifest: &crate::metadata::Manifest,
    ) -> bool {
        data_records
            .iter()
            .all(|record| Self::manifest_covers_wal_data_record(manifest, record))
    }

    fn manifest_covers_wal_data_record(
        manifest: &crate::metadata::Manifest,
        record: &WalDataCoverageRecord,
    ) -> bool {
        manifest
            .files
            .iter()
            .any(|file| Self::manifest_file_covers_wal_data_record(file, record))
    }

    fn manifest_file_covers_wal_data_record(
        file: &crate::metadata::FileMeta,
        record: &WalDataCoverageRecord,
    ) -> bool {
        if file.cf_id != record.cf_id {
            return false;
        }

        let (Some(smallest_seq), Some(largest_seq)) = (file.smallest_seq, file.largest_seq) else {
            return false;
        };
        if record.seq < smallest_seq || record.seq > largest_seq {
            return false;
        }

        let (Some(smallest_key), Some(largest_key)) =
            (file.smallest_key.as_ref(), file.largest_key.as_ref())
        else {
            return false;
        };

        match record.op {
            crate::wal::WalOpKind::Put
            | crate::wal::WalOpKind::Insert
            | crate::wal::WalOpKind::Delete => {
                smallest_key.as_slice() <= record.key.as_slice()
                    && record.key.as_slice() <= largest_key.as_slice()
            }
            crate::wal::WalOpKind::DeleteRange => {
                record.range_end.as_ref().is_some_and(|range_end| {
                    smallest_key.as_slice() <= record.key.as_slice()
                        && range_end.as_slice() <= largest_key.as_slice()
                })
            }
            crate::wal::WalOpKind::TxnBegin | crate::wal::WalOpKind::TxnCommit => true,
        }
    }

    fn verify_remote_wal_segment_with_backend(
        cloud: &Arc<dyn StorageBackend>,
        verified_wal_segments: &Arc<Mutex<HashMap<u64, VerifiedWalSegment>>>,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String> {
        let key = crate::wal::cloud_segment_object_key(segment_id);
        if let Some(proof) = verified_wal_segments.lock().get(&segment_id).cloned() {
            if proof.max_sequence != expected_max_sequence {
                return Err(format!(
                    "cached cloud WAL segment '{key}' max sequence {} does not match expected {expected_max_sequence}",
                    proof.max_sequence
                ));
            }
            return Self::verify_cloud_object_proof(cloud, &key, &proof.metadata);
        }

        let data = Self::read_cloud_object_from_backend_blocking(cloud, &key)?;
        let readback = Self::validate_wal_segment_bytes(&key, &data, expected_max_sequence)?;
        let metadata = Self::head_cloud_object_from_backend_blocking(cloud, &key)?;
        if metadata.size != data.len() as u64 {
            return Err(format!(
                "cloud WAL segment '{key}' size changed during validation: read={}, head={}",
                data.len(),
                metadata.size
            ));
        }
        verified_wal_segments.lock().insert(
            segment_id,
            VerifiedWalSegment {
                max_sequence: readback.validation.max_sequence,
                data_records: readback.data_records,
                metadata,
            },
        );
        Ok(())
    }

    pub fn verify_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String> {
        Self::verify_remote_wal_segment_with_backend(
            &self.cloud,
            &self.verified_wal_segments,
            segment_id,
            expected_max_sequence,
        )
    }

    fn validate_sst_object_bytes(
        sst_name: &str,
        expected_size_bytes: u64,
        expected_content_crc32c: Option<u32>,
        expected_file: Option<&crate::metadata::FileMeta>,
        data: &[u8],
    ) -> Result<(u32, crate::sst::fs::SstFileSummary), String> {
        if expected_size_bytes > 0 && data.len() as u64 != expected_size_bytes {
            return Err(format!(
                "cloud SST '{sst_name}' size mismatch: manifest={expected_size_bytes}, object={}",
                data.len()
            ));
        }

        let actual_content_crc32c = crc32c::crc32c(data);
        if let Some(expected_content_crc32c) = expected_content_crc32c {
            if actual_content_crc32c != expected_content_crc32c {
                return Err(format!(
                    "cloud SST '{sst_name}' content crc32c {actual_content_crc32c:08x} does not match manifest {expected_content_crc32c:08x}"
                ));
            }
        }

        let mut temp = tempfile::Builder::new()
            .prefix("midge-cloud-sst-verify-")
            .suffix(".sst")
            .tempfile()
            .map_err(|error| format!("create temp SST verifier for '{sst_name}': {error}"))?;
        temp.write_all(data)
            .map_err(|error| format!("write temp SST verifier for '{sst_name}': {error}"))?;
        temp.flush()
            .map_err(|error| format!("flush temp SST verifier for '{sst_name}': {error}"))?;

        let reader = crate::sst::fs::SstFileIo::open_with_real_fs(temp.path())
            .map_err(|error| format!("cloud SST '{sst_name}' failed validation: {error}"))?;
        let summary = reader
            .summary()
            .map_err(|error| format!("cloud SST '{sst_name}' summary validation: {error}"))?;
        if let Some(expected_file) = expected_file {
            Self::verify_sst_summary_matches_manifest(sst_name, &summary, expected_file)?;
        }

        Ok((actual_content_crc32c, summary))
    }

    fn verify_sst_summary_matches_manifest(
        sst_name: &str,
        summary: &crate::sst::fs::SstFileSummary,
        file: &crate::metadata::FileMeta,
    ) -> Result<(), String> {
        if file.size_bytes > 0 && summary.size_bytes != file.size_bytes {
            return Err(format!(
                "cloud SST '{sst_name}' physical size {} does not match manifest {}",
                summary.size_bytes, file.size_bytes
            ));
        }
        if let Some(smallest_key) = file.smallest_key.as_ref() {
            if summary.smallest_key.as_slice() != smallest_key.as_slice() {
                return Err(format!(
                    "cloud SST '{sst_name}' smallest key does not match manifest"
                ));
            }
        }
        if let Some(largest_key) = file.largest_key.as_ref() {
            if summary.largest_key.as_slice() != largest_key.as_slice() {
                return Err(format!(
                    "cloud SST '{sst_name}' largest key does not match manifest"
                ));
            }
        }
        if let Some(smallest_seq) = file.smallest_seq {
            if summary.smallest_seq != smallest_seq {
                return Err(format!(
                    "cloud SST '{sst_name}' smallest sequence {} does not match manifest {smallest_seq}",
                    summary.smallest_seq
                ));
            }
        }
        if let Some(largest_seq) = file.largest_seq {
            if summary.largest_seq != largest_seq {
                return Err(format!(
                    "cloud SST '{sst_name}' largest sequence {} does not match manifest {largest_seq}",
                    summary.largest_seq
                ));
            }
        }
        Ok(())
    }

    pub fn verify_manifest_cloud_objects(
        &self,
        manifest: &crate::metadata::Manifest,
    ) -> Result<(), String> {
        Self::verify_manifest_cloud_objects_with_backend(
            &self.cloud,
            &self.verified_sst_objects,
            manifest,
        )
    }

    fn verify_manifest_cloud_objects_with_backend(
        cloud: &Arc<dyn StorageBackend>,
        verified_sst_objects: &Arc<Mutex<HashMap<String, VerifiedCloudObject>>>,
        manifest: &crate::metadata::Manifest,
    ) -> Result<(), String> {
        for file in &manifest.files {
            let key = crate::sst::object_key(&file.name);
            Self::verify_sst_cloud_object_with_backend(
                cloud,
                verified_sst_objects,
                &key,
                &file.name,
                file.size_bytes,
                file.content_crc32c,
                Some(file),
            )?;
        }

        for sst_name in &manifest.ssts {
            if manifest.files.iter().any(|file| file.name == *sst_name) {
                continue;
            }
            let key = crate::sst::object_key(sst_name);
            Self::verify_sst_cloud_object_with_backend(
                cloud,
                verified_sst_objects,
                &key,
                sst_name,
                0,
                None,
                None,
            )?;
        }

        Ok(())
    }

    fn verify_sst_cloud_object(
        &self,
        key: &str,
        sst_name: &str,
        expected_size_bytes: u64,
        expected_content_crc32c: Option<u32>,
        expected_file: Option<&crate::metadata::FileMeta>,
    ) -> Result<(), String> {
        Self::verify_sst_cloud_object_with_backend(
            &self.cloud,
            &self.verified_sst_objects,
            key,
            sst_name,
            expected_size_bytes,
            expected_content_crc32c,
            expected_file,
        )
    }

    fn verify_sst_cloud_object_with_backend(
        cloud: &Arc<dyn StorageBackend>,
        verified_sst_objects: &Arc<Mutex<HashMap<String, VerifiedCloudObject>>>,
        key: &str,
        sst_name: &str,
        expected_size_bytes: u64,
        expected_content_crc32c: Option<u32>,
        expected_file: Option<&crate::metadata::FileMeta>,
    ) -> Result<(), String> {
        if let Some(proof) = verified_sst_objects.lock().get(key).cloned() {
            if expected_size_bytes > 0 && proof.metadata.size != expected_size_bytes {
                return Err(format!(
                    "cached cloud SST '{sst_name}' size {} does not match manifest {expected_size_bytes}",
                    proof.metadata.size
                ));
            }
            Self::verify_cloud_object_proof(cloud, key, &proof.metadata)?;
            if let Some(expected_content_crc32c) = expected_content_crc32c {
                if proof.content_crc32c != Some(expected_content_crc32c) {
                    let actual = proof
                        .content_crc32c
                        .map(|crc| format!("{crc:08x}"))
                        .unwrap_or_else(|| "unknown".to_string());
                    return Err(format!(
                        "cached cloud SST '{sst_name}' content crc32c {actual} does not match manifest {expected_content_crc32c:08x}"
                    ));
                }
            }
            if let Some(expected_file) = expected_file {
                let Some(summary) = proof.summary.as_ref() else {
                    return Err(format!(
                        "cached cloud SST '{sst_name}' has no physical summary proof"
                    ));
                };
                Self::verify_sst_summary_matches_manifest(sst_name, summary, expected_file)?;
            }
            return Ok(());
        }

        let data = Self::read_cloud_object_from_backend_blocking(cloud, key)?;
        let (content_crc32c, summary) = Self::validate_sst_object_bytes(
            sst_name,
            expected_size_bytes,
            expected_content_crc32c,
            expected_file,
            &data,
        )?;
        let metadata = Self::head_cloud_object_from_backend_blocking(cloud, key)?;
        if metadata.size != data.len() as u64 {
            return Err(format!(
                "cloud SST '{sst_name}' size changed during validation: read={}, head={}",
                data.len(),
                metadata.size
            ));
        }
        verified_sst_objects.lock().insert(
            key.to_string(),
            VerifiedCloudObject {
                metadata,
                content_crc32c: Some(content_crc32c),
                summary: Some(summary),
            },
        );
        Ok(())
    }

    pub(crate) fn prune_cloud_wal_segment(
        &self,
        segment_id: u64,
        guard: CloudWalPruneGuard,
    ) -> Result<(), String> {
        let proof = self
            .verified_wal_segments
            .lock()
            .get(&segment_id)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "cannot prune cloud WAL segment {segment_id} without a cached readback proof"
                )
            })?;
        let etag = proof.metadata.etag.trim().to_string();
        if etag.is_empty() {
            return Err(format!(
                "cannot prune cloud WAL segment {segment_id} without a conditional delete token"
            ));
        }

        let cloud = Arc::clone(&self.cloud);
        let event_queue = Arc::clone(&self.event_queue);
        let external_event_tx = self.external_event_tx.clone();
        let verified_wal_segments = Arc::clone(&self.verified_wal_segments);
        let verified_sst_objects = Arc::clone(&self.verified_sst_objects);
        let expected_max_sequence = proof.max_sequence;

        thread::Builder::new()
            .name(format!("midge-wal-pruner-{segment_id}"))
            .spawn(move || {
                let key = crate::wal::cloud_segment_object_key(segment_id);
                let result = match Self::revalidate_cloud_wal_prune_guard(
                    &cloud,
                    &verified_wal_segments,
                    &verified_sst_objects,
                    segment_id,
                    expected_max_sequence,
                    &guard,
                ) {
                    Ok(()) => {
                        let (tx, rx) = std::sync::mpsc::channel();
                        cloud.submit_delete_with_headers(
                            key.clone(),
                            vec![("If-Match".into(), etag)],
                            tx,
                        );

                        match rx.recv_timeout(Duration::from_secs(30)) {
                            Ok(StorageEvent::DeleteComplete { result, .. }) => result,
                            Ok(other) => StorageOutcome::Err(format!(
                                "unexpected cloud WAL prune response for '{key}': {other:?}"
                            )),
                            Err(error) => StorageOutcome::Err(format!(
                                "cloud WAL prune timed out for '{key}': {error}"
                            )),
                        }
                    }
                    Err(error) => StorageOutcome::Err(error),
                };

                let event = StorageEvent::CloudWalPruneComplete { segment_id, result };

                {
                    let mut events = event_queue.lock();
                    events.push_back(event.clone());
                }

                if let Some(tx) = external_event_tx {
                    let _ = tx.send(event);
                }
            })
            .map(|_| ())
            .map_err(|error| format!("failed to spawn cloud WAL prune worker: {error}"))
    }

    fn revalidate_cloud_wal_prune_guard(
        cloud: &Arc<dyn StorageBackend>,
        verified_wal_segments: &Arc<Mutex<HashMap<u64, VerifiedWalSegment>>>,
        verified_sst_objects: &Arc<Mutex<HashMap<String, VerifiedCloudObject>>>,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: &CloudWalPruneGuard,
    ) -> Result<(), String> {
        Self::verify_remote_wal_segment_with_backend(
            cloud,
            verified_wal_segments,
            segment_id,
            expected_max_sequence,
        )?;
        let proof = verified_wal_segments
            .lock()
            .get(&segment_id)
            .cloned()
            .ok_or_else(|| {
                format!("cloud WAL segment {segment_id} has no readback proof after validation")
            })?;
        if !Self::wal_data_records_covered_by_manifest(&proof.data_records, &guard.manifest) {
            return Err(format!(
                "cloud WAL segment {segment_id} contains records not covered by the committed manifest"
            ));
        }
        Self::verify_manifest_cloud_objects_with_backend(
            cloud,
            verified_sst_objects,
            &guard.manifest,
        )?;
        Self::verify_cloud_metadata_prune_guard(guard.metadata.as_ref())
    }

    fn verify_cloud_metadata_prune_guard(
        guard: Option<&CloudMetadataPruneGuard>,
    ) -> Result<(), String> {
        let Some(guard) = guard else {
            return Ok(());
        };

        for proof in &guard.objects {
            let (tx, rx) = std::sync::mpsc::channel();
            guard.cloud.submit_get(proof.key.clone(), tx);
            match rx.recv_timeout(Duration::from_secs(30)) {
                Ok(crate::storage::cloud::CloudEvent::GetComplete {
                    result: crate::storage::cloud::CloudOutcome::Ok(data),
                    ..
                }) => {
                    if data != proof.expected_bytes {
                        return Err(format!(
                            "cloud metadata '{}' changed before WAL prune",
                            proof.key
                        ));
                    }
                    let (head_tx, head_rx) = std::sync::mpsc::channel();
                    guard.cloud.submit_head(proof.key.clone(), head_tx);
                    match head_rx.recv_timeout(Duration::from_secs(30)) {
                        Ok(crate::storage::cloud::CloudEvent::HeadComplete {
                            result: crate::storage::cloud::CloudOutcome::Ok(actual),
                            ..
                        }) => {
                            let actual = StorageObjectMetadata {
                                size: actual.size,
                                etag: actual.etag,
                                generation: actual.generation,
                            };
                            if actual != proof.remote {
                                return Err(format!(
                                    "cloud metadata '{}' identity changed before WAL prune: expected {:?}, actual {:?}",
                                    proof.key, proof.remote, actual
                                ));
                            }
                        }
                        Ok(crate::storage::cloud::CloudEvent::HeadComplete {
                            result: crate::storage::cloud::CloudOutcome::Err(error),
                            ..
                        }) => {
                            return Err(format!(
                                "cloud metadata '{}' is unreadable before WAL prune: {error}",
                                proof.key
                            ));
                        }
                        Ok(other) => {
                            return Err(format!(
                                "unexpected cloud metadata HEAD response for '{}': {other:?}",
                                proof.key
                            ));
                        }
                        Err(error) => {
                            return Err(format!(
                                "cloud metadata HEAD timed out for '{}': {error}",
                                proof.key
                            ));
                        }
                    }
                }
                Ok(crate::storage::cloud::CloudEvent::GetComplete {
                    result: crate::storage::cloud::CloudOutcome::Err(error),
                    ..
                }) => {
                    return Err(format!(
                        "cloud metadata '{}' is unreadable before WAL prune: {error}",
                        proof.key
                    ));
                }
                Ok(other) => {
                    return Err(format!(
                        "unexpected cloud metadata read response for '{}': {other:?}",
                        proof.key
                    ));
                }
                Err(error) => {
                    return Err(format!(
                        "cloud metadata read timed out for '{}': {error}",
                        proof.key
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn write_sst_object(
        &self,
        sst_name: &str,
        data: Vec<u8>,
    ) -> crate::common::MidgeResult<()> {
        let key = crate::sst::object_key(sst_name);
        let expected_size_bytes = data.len() as u64;
        let expected_content_crc32c = Some(crc32c::crc32c(&data));

        if Self::object_exists_in_backend_blocking(&self.local, &key).map_err(|error| {
            crate::common::MidgeError::Internal(format!(
                "local SST cache preflight failed: {error}"
            ))
        })? {
            return Err(crate::common::MidgeError::Internal(format!(
                "local SST cache already exists for immutable SST object '{key}'"
            )));
        }

        let forced_failure =
            fail::eval("midge::cloud::inject_fail_sst_upload", |_| true).unwrap_or(false);
        if forced_failure {
            return Err(crate::common::MidgeError::Internal(
                "failpoint: cloud SST upload failed".to_string(),
            ));
        }

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        self.cloud.submit_write_with_headers(
            key.clone(),
            data.clone(),
            vec![("If-None-Match".into(), "*".into())],
            tx_cloud,
        );

        let cloud_result = rx_cloud.recv().map_err(|_| {
            crate::common::MidgeError::Internal(
                "cloud SST upload callback channel closed".to_string(),
            )
        })?;

        match cloud_result {
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            } => self
                .verify_sst_cloud_object(
                    &key,
                    sst_name,
                    expected_size_bytes,
                    expected_content_crc32c,
                    None,
                )
                .map_err(crate::common::MidgeError::Internal)?,
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            } => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "cloud SST upload failed: {error}"
                )));
            }
            other => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "unexpected cloud SST upload response: {other:?}"
                )));
            }
        }

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        self.local.submit_write_with_headers(
            key.clone(),
            data,
            vec![("If-None-Match".into(), "*".into())],
            tx_local,
        );

        let local_result = rx_local.recv().map_err(|_| {
            crate::common::MidgeError::Internal(
                "local SST cache write callback channel closed".to_string(),
            )
        })?;

        match local_result {
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            } => {}
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            } => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "local SST cache write failed: {error}"
                )));
            }
            other => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "unexpected local SST cache write response: {other:?}"
                )));
            }
        }
        Ok(())
    }

    pub fn delete_sst_object_blocking(&self, sst_name: &str) -> crate::common::MidgeResult<()> {
        let key = crate::sst::object_key(sst_name);

        match Self::delete_object_from_backend_blocking(&self.cloud, &key) {
            Ok(true) => {
                tracing::info!(sst_name, key, "Deleted obsolete cloud SST object");
            }
            Ok(false) => {
                tracing::debug!(sst_name, key, "Obsolete cloud SST object already missing");
            }
            Err(error) => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "cloud SST delete failed: {error}"
                )));
            }
        }

        self.verified_sst_objects.lock().remove(&key);

        if let Err(error) = Self::delete_object_from_backend_blocking(&self.local, &key) {
            tracing::warn!(
                sst_name,
                key,
                error,
                "Failed to delete obsolete SST from local hybrid cache"
            );
        }

        Ok(())
    }
}

impl StorageBackend for HybridStorage {
    fn submit_read(&self, key: String, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - reads SSTs, metadata, etc.
        // Try local first, fall back to cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let key_clone = key.clone();

        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_read(key_clone.clone(), tx);

        match rx.recv() {
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
                cloud_clone.submit_read(k, tx_cloud);
                if let Ok(event) = rx_cloud.recv() {
                    let _ = callback.send(event);
                }
            }
            _ => {
                let _ = callback.send(StorageEvent::ReadComplete {
                    key,
                    result: StorageOutcome::Err("Hybrid read failed".to_string()),
                });
            }
        }
    }

    fn submit_write(&self, key: String, data: Vec<u8>, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - NOT for WAL durability
        // WAL durability uses enqueue_wal_segment() instead

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let key_clone = key.clone();
        let data_clone = data.clone();

        // Always write to local first
        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_write(key_clone, data_clone, tx);

        match rx.recv() {
            Ok(StorageEvent::WriteComplete { ref result, .. }) => {
                // Send result back to caller immediately (local write complete)
                let event = StorageEvent::WriteComplete {
                    key: key.clone(),
                    result: result.clone(),
                };
                let _ = callback.send(event);

                // Schedule cloud write ONLY for SST files (not WAL)
                // WAL cloud uploads happen via enqueue_wal_segment() + process_uploads()
                if key.starts_with("sst/") && result.is_ok() {
                    let (tx_cloud, _) = std::sync::mpsc::channel();
                    cloud_clone.submit_write_with_headers(
                        key,
                        data,
                        vec![("If-None-Match".into(), "*".into())],
                        tx_cloud,
                    );
                }
            }
            _ => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key,
                    result: StorageOutcome::Err("Hybrid write failed".to_string()),
                });
            }
        }
    }

    fn submit_delete(&self, key: String, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - deletes SSTs, metadata, etc.
        // Delete from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let key_clone = key.clone();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_delete(key_clone.clone(), tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_delete(key_clone, tx_cloud);

        // Wait for both and report result
        let local_result = rx_local.recv().ok();
        let cloud_result = rx_cloud.recv().ok();

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

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - lists SSTs, metadata, etc.
        // Merge results from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let prefix_clone = prefix.clone();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_list(prefix_clone.clone(), tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_list(prefix_clone, tx_cloud);

        let mut results = Vec::new();

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(local_items),
            ..
        }) = rx_local.recv()
        {
            results.extend(local_items);
        }

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(cloud_items),
            ..
        }) = rx_cloud.recv()
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
            prefix,
            result: StorageOutcome::Ok(results),
        });
    }
}

impl Drop for HybridStorage {
    fn drop(&mut self) {
        // Drop sender first so worker recv() unblocks and exits promptly.
        // Waiting for join before dropping sender can deadlock until timeout.
        let _ = self.wal_upload_tx.take();

        // Wait for the worker thread to complete with a timeout
        if let Some(handle) = self.upload_worker_handle.take() {
            let start = Instant::now();
            let timeout = Duration::from_secs(30);

            // Spawn a thread to join with timeout
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                let result = handle.join();
                let _ = tx.send(result);
            });

            // Wait for completion or timeout
            match rx.recv_timeout(timeout) {
                Ok(Ok(())) => {
                    tracing::debug!(
                        elapsed_ms = start.elapsed().as_millis(),
                        "HybridStorage WAL upload worker shutdown cleanly"
                    );
                }
                Ok(Err(_)) => {
                    tracing::warn!("HybridStorage WAL upload worker panicked during shutdown");
                }
                Err(_timeout) => {
                    tracing::error!(
                        "HybridStorage WAL upload worker did not shutdown within 30s timeout; thread detached"
                    );
                    // Thread will be detached and continue running
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SstFactory;
    use crate::storage::cloud::{CloudStorage, MockCloudBackend};
    use bytes::Bytes;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};

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
        fn submit_read(&self, key: String, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::ReadComplete {
                key,
                result: StorageOutcome::Err("read unavailable".to_string()),
            });
        }

        fn submit_write(&self, key: String, _data: Vec<u8>, callback: StorageCallback) {
            self.write_attempts.fetch_add(1, Ordering::SeqCst);
            let _ = callback.send(StorageEvent::WriteComplete {
                key,
                result: StorageOutcome::Err("write unavailable".to_string()),
            });
        }

        fn submit_write_with_headers(
            &self,
            key: String,
            data: Vec<u8>,
            _headers: Vec<(String, String)>,
            callback: StorageCallback,
        ) {
            self.submit_write(key, data, callback);
        }

        fn submit_delete(&self, key: String, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::DeleteComplete {
                key,
                result: StorageOutcome::Ok(()),
            });
        }

        fn submit_list(&self, prefix: String, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::ListComplete {
                prefix,
                result: StorageOutcome::Ok(Vec::new()),
            });
        }

        fn submit_head(&self, key: String, callback: StorageCallback) {
            let _ = callback.send(StorageEvent::HeadComplete {
                key,
                result: StorageOutcome::Err("head unavailable".to_string()),
            });
        }
    }

    fn write_cloud_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
        let (tx, rx) = std::sync::mpsc::channel();
        storage.cloud.submit_write(key.to_string(), data, tx);
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
        storage.local.submit_write(key.to_string(), data, tx);
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
        storage.local.submit_read(key.to_string(), tx);
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
        storage.cloud.submit_read(key.to_string(), tx);
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
        storage.submit_read(key.to_string(), tx);
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
        storage.cloud.submit_delete(key.to_string(), tx);
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
        cloud.submit_put(key.to_string(), data, vec![], tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(crate::storage::cloud::CloudEvent::PutComplete {
                result: crate::storage::cloud::CloudOutcome::Ok(()),
                ..
            }) => {}
            other => panic!("cloud metadata write for '{key}' failed: {other:?}"),
        }
    }

    fn head_cloud_metadata_object(cloud: &CloudStorage, key: &str) -> StorageObjectMetadata {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(crate::storage::cloud::CloudEvent::HeadComplete {
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
        storage.cloud.submit_head(key.to_string(), tx);
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
        storage.cloud.submit_head(key.to_string(), tx);
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

    #[test]
    fn should_not_reread_verified_manifest_ssts_on_repeated_validation() {
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
        assert!(
            first_downloads
                .iter()
                .any(|key| key.ends_with("sst/cached.sst")),
            "first validation should read the cloud SST, got {first_downloads:?}"
        );

        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("second manifest validation");

        assert_eq!(
            mock_cloud.get_downloads(),
            first_downloads,
            "verified immutable SSTs should not be reread on unchanged manifest validation"
        );
    }

    #[test]
    fn should_only_validate_new_manifest_ssts_after_manifest_extends() {
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

        assert!(
            downloads.iter().any(|key| key.ends_with("sst/second.sst")),
            "extended validation should read the new SST, got {downloads:?}"
        );
        assert!(
            downloads.iter().all(|key| !key.ends_with("sst/first.sst")),
            "extended validation should not reread already verified SSTs, got {downloads:?}"
        );
    }

    #[test]
    fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_deleted() {
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
        assert!(
            error.contains("changed since validation") || error.contains("unreadable"),
            "unexpected stale SST proof error: {error}"
        );
    }

    #[test]
    fn should_validate_legacy_manifest_ssts_before_wal_cleanup() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let mut manifest = crate::metadata::Manifest::default();
        manifest.ssts.push("legacy-missing.sst".to_string());

        let error = storage
            .verify_manifest_cloud_objects(&manifest)
            .expect_err("legacy manifest SST references must be validated");
        assert!(
            error.contains("legacy-missing.sst") || error.contains("sst/legacy-missing.sst"),
            "unexpected legacy SST validation error: {error}"
        );
    }

    #[test]
    fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_overwritten() {
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
        assert!(
            error.contains("changed since validation") || error.contains("size mismatch"),
            "unexpected stale SST proof error: {error}"
        );
    }

    #[test]
    fn should_reject_manifest_sst_when_content_crc_differs() {
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

        assert!(
            error.contains("crc") || error.contains("content"),
            "unexpected manifest SST CRC validation error: {error}"
        );
    }

    #[test]
    fn should_not_reuse_size_only_sst_proof_when_manifest_later_requires_crc() {
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

        assert!(
            error.contains("crc") || error.contains("content"),
            "unexpected cached SST CRC validation error: {error}"
        );
    }

    #[test]
    fn should_not_overwrite_existing_remote_sst_during_authoritative_upload() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "collision.sst";
        let key = crate::sst::object_key(sst_name);
        let existing_bytes = valid_sst_bytes(b"a", b"already-committed", 1);
        let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
        write_cloud_object(&storage, &key, existing_bytes.clone());

        let error = storage
            .write_sst_object(sst_name, upload_bytes)
            .expect_err("existing remote SST object must fail authoritative upload");

        assert!(
            error.to_string().contains("cloud SST upload failed")
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
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let sst_name = "local-collision.sst";
        let key = crate::sst::object_key(sst_name);
        let existing_bytes = valid_sst_bytes(b"a", b"local-already-committed", 1);
        let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
        write_local_object(&storage, &key, existing_bytes.clone());

        let error = storage
            .write_sst_object(sst_name, upload_bytes)
            .expect_err("existing local SST cache object must fail authoritative upload");

        assert!(
            error.to_string().contains("local SST cache already exists"),
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
    fn should_cache_remote_wal_readback_validation() {
        let (mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 7;
        let max_sequence = 11;
        write_cloud_object(
            &storage,
            &crate::wal::cloud_segment_object_key(segment_id),
            valid_wal_bytes(max_sequence),
        );

        mock_cloud.clear_history();
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("first remote WAL validation");
        let first_downloads = mock_cloud.get_downloads();
        assert!(
            first_downloads
                .iter()
                .any(|key| key.ends_with("wal/00000000000000000007.wal")),
            "first validation should read the cloud WAL, got {first_downloads:?}"
        );

        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("second remote WAL validation");
        assert_eq!(
            mock_cloud.get_downloads(),
            first_downloads,
            "verified immutable WAL segments should not be reread"
        );
    }

    #[test]
    fn should_reject_cached_remote_wal_proof_when_cloud_object_is_deleted() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 8;
        let max_sequence = 12;
        let key = crate::wal::cloud_segment_object_key(segment_id);
        write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));

        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        delete_cloud_object(&storage, &key);

        let error = storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect_err("deleted WAL must invalidate cached proof");
        assert!(
            error.contains("changed since validation") || error.contains("unreadable"),
            "unexpected stale WAL proof error: {error}"
        );
    }

    #[test]
    fn should_not_prune_remote_wal_when_verified_object_identity_changed() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 11;
        let max_sequence = 21;
        let key = crate::wal::cloud_segment_object_key(segment_id);
        write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");

        write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
        storage
            .prune_cloud_wal_segment(segment_id, CloudWalPruneGuard::default())
            .expect("schedule prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        assert!(
            result.is_err(),
            "stale WAL proof must make remote prune fail conservatively"
        );
        assert_cloud_object_exists(&storage, &key);
    }

    #[test]
    fn should_not_prune_remote_wal_when_manifest_sst_disappears_after_initial_validation() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 13;
        let max_sequence = 23;
        let wal_key = crate::wal::cloud_segment_object_key(segment_id);
        let sst_name = "missing-after-validation.sst";
        let sst_key = crate::sst::object_key(sst_name);
        let sst_bytes = valid_sst_bytes(b"a", b"v1", 1);
        let manifest = manifest_for_ssts(&[(sst_name, sst_bytes.len() as u64)]);

        write_cloud_object(&storage, &wal_key, valid_wal_bytes(max_sequence));
        write_cloud_object(&storage, &sst_key, sst_bytes);
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        storage
            .verify_manifest_cloud_objects(&manifest)
            .expect("initial manifest SST validation");

        delete_cloud_object(&storage, &sst_key);
        storage
            .prune_cloud_wal_segment(segment_id, CloudWalPruneGuard::new(manifest.clone(), None))
            .expect("schedule prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        assert!(
            result.is_err(),
            "worker-side manifest revalidation must fail conservatively"
        );
        assert_cloud_object_exists(&storage, &wal_key);
    }

    #[test]
    fn should_not_prune_remote_wal_when_manifest_sst_content_crc_differs() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 16;
        let max_sequence = 26;
        let wal_key = crate::wal::cloud_segment_object_key(segment_id);
        let sst_name = "wrong-crc-prune-guard.sst";
        let sst_key = crate::sst::object_key(sst_name);
        let sst_bytes = valid_sst_bytes(b"a", b"v1", 1);
        let wrong_crc = crc32c::crc32c(&sst_bytes) ^ 0xffff_ffff;
        let manifest =
            manifest_for_ssts_with_crc(&[(sst_name, sst_bytes.len() as u64, Some(wrong_crc))]);

        write_cloud_object(&storage, &wal_key, valid_wal_bytes(max_sequence));
        write_cloud_object(&storage, &sst_key, sst_bytes);
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("initial remote WAL validation");
        storage
            .prune_cloud_wal_segment(segment_id, CloudWalPruneGuard::new(manifest, None))
            .expect("schedule guarded prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        assert!(
            result.is_err(),
            "worker-side manifest CRC revalidation must fail conservatively"
        );
        assert_cloud_object_exists(&storage, &wal_key);
    }

    #[test]
    fn should_not_prune_remote_wal_when_cloud_metadata_changes_after_initial_validation() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 14;
        let max_sequence = 24;
        let wal_key = crate::wal::cloud_segment_object_key(segment_id);
        let metadata_backend = Arc::new(MockCloudBackend::new());
        let metadata_cloud = Arc::new(CloudStorage::new(
            metadata_backend,
            "metadata-test".to_string(),
        ));
        let metadata_key = crate::storage::cloud::cloud_metadata_key("manifest.json");
        let metadata_bytes = br#"{"last_persisted_sequence":24}"#.to_vec();

        write_cloud_object(&storage, &wal_key, valid_wal_bytes(max_sequence));
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
                CloudWalPruneGuard::new(crate::metadata::Manifest::default(), Some(metadata_guard)),
            )
            .expect("schedule prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        assert!(
            result.is_err(),
            "worker-side metadata revalidation must fail conservatively"
        );
        assert_cloud_object_exists(&storage, &wal_key);
    }

    #[test]
    fn should_prune_remote_wal_when_worker_side_guard_remains_valid() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 15;
        let max_sequence = 25;
        let wal_key = crate::wal::cloud_segment_object_key(segment_id);
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
                CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
            )
            .expect("schedule prune");

        let result = wait_for_wal_prune_result(&storage, segment_id);
        assert!(
            result.is_ok(),
            "valid worker-side guard should allow conditional remote WAL deletion"
        );
        assert_cloud_object_missing(&storage, &wal_key);
    }

    #[test]
    fn should_reject_remote_wal_segment_with_sequence_beyond_expected_max() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let segment_id = 10;
        let expected_max_sequence = 20;
        write_cloud_object(
            &storage,
            &crate::wal::cloud_segment_object_key(segment_id),
            valid_wal_bytes(expected_max_sequence + 1),
        );

        let error = storage
            .verify_remote_wal_segment(segment_id, expected_max_sequence)
            .expect_err("WAL segment with records beyond expected max must be rejected");
        assert!(
            error.contains("exceeds expected"),
            "unexpected WAL max-sequence error: {error}"
        );
    }

    #[test]
    fn should_not_overwrite_existing_remote_wal_during_upload() {
        let (_mock_cloud, storage) = hybrid_with_mock_cloud();
        let tmp = tempfile::tempdir().expect("create WAL dir");
        let segment_id = 12;
        let upload_max_sequence = 22;
        let key = crate::wal::cloud_segment_object_key(segment_id);
        let existing_bytes = valid_wal_bytes(upload_max_sequence + 100);
        write_cloud_object(&storage, &key, existing_bytes.clone());

        let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
        std::fs::write(&wal_path, valid_wal_bytes(upload_max_sequence)).expect("write local WAL");
        storage.enqueue_wal_segment(segment_id, wal_path, upload_max_sequence);
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
        assert!(
            !storage.is_remote_wal_segment_verified(segment_id, upload_max_sequence),
            "failed conditional upload must not cache a proof for the attempted segment"
        );
    }

    #[test]
    fn should_readback_remote_wal_before_upload_worker_emits_ack() {
        let (mock_cloud, storage) = hybrid_with_mock_cloud();
        let tmp = tempfile::tempdir().expect("create WAL dir");
        let segment_id = 9;
        let max_sequence = 13;
        let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
        std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");

        mock_cloud.clear_history();
        storage.enqueue_wal_segment(segment_id, wal_path, max_sequence);
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

        storage.enqueue_wal_segment(segment_id, wal_path, max_sequence);

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
}
