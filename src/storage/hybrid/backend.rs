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
//! 2. WAL DURABILITY PIPELINE (CloudFirst mode):
//!    - enqueue_wal_segment() - queue WAL for cloud upload
//!    - process_uploads() - initiate cloud uploads
//!    - poll() - retrieve CloudAck/CloudFail events
//!    - NEVER uses submit_write() for WAL
//!
//! WAL Flow:
//!   WalActor → local write → enqueue_wal_segment() → process_uploads() →
//!   cloud backend → CloudAck event → WalActor applies to memtable
//!
//! SST Flow:
//!   Engine → submit_write() → local write + cloud write (if sst/) → done

use super::actor;
use super::policy;
use super::state;
use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use crossbeam::channel as cb;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

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
}

/// Hybrid storage combining local filesystem and cloud backends
///
/// Managed by a Storage Budget Actor to enforce disk constraints, watermarks,
/// and coordination between local caching and cloud durability.
///
/// CloudFirst Durability:
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
    /// Pending WAL segment uploads (CloudFirst mode)
    upload_queue: Arc<Mutex<VecDeque<UploadState>>>,
    /// Completed events ready for polling
    event_queue: Arc<Mutex<VecDeque<StorageEvent>>>,

    /// Optional external event sink for CloudAck/CloudFail.
    /// When set, upload completions are pushed directly to the runtime event loop
    /// to avoid polling latency.
    external_event_tx: Option<cb::Sender<StorageEvent>>,

    /// Dedicated WAL upload worker sender.
    wal_upload_tx: mpsc::Sender<UploadState>,
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

        // Single background worker for WAL uploads.
        // This avoids spawning one OS thread per segment, which is extremely
        // expensive under CloudFirst + synchronous write APIs (e.g. 10k puts).
        let (wal_upload_tx, wal_upload_rx) = mpsc::channel::<UploadState>();
        {
            let cloud = Arc::clone(&cloud);
            let event_queue = Arc::clone(&event_queue);
            let external_event_tx = external_event_tx.clone();

            std::thread::Builder::new()
                .name("midge-wal-uploader".to_string())
                .spawn(move || {
                    while let Ok(upload) = wal_upload_rx.recv() {
                        let upload_start = Instant::now();
                        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                            telemetry.metrics().record_cloudfirst_wal_upload_started();
                        }

                        if std::env::var_os("MIDGE_TRACE_CLOUDFIRST").is_some()
                            && upload.segment_id % 1000 == 0
                        {
                            eprintln!(
                                "[midge] CloudFirst upload start: segment_id={} max_sequence={} path={:?}",
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

                        let (tx, rx) = std::sync::mpsc::channel();
                        let cloud_key = format!("wal/{}.wal", upload.segment_id);
                        cloud.submit_write(cloud_key, data, tx);

                        match rx.recv() {
                            Ok(StorageEvent::WriteComplete { result, .. }) if result.is_ok() => {
                                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                    telemetry
                                        .metrics()
                                        .record_cloudfirst_wal_upload_completed(upload_start.elapsed().as_micros() as u64);
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

                                if std::env::var_os("MIDGE_TRACE_CLOUDFIRST").is_some()
                                    && upload.segment_id % 1000 == 0
                                {
                                    eprintln!(
                                        "[midge] CloudFirst upload ack: segment_id={} max_sequence={}",
                                        upload.segment_id, upload.max_sequence
                                    );
                                }
                            }
                            Ok(StorageEvent::WriteComplete { result, .. }) => {
                                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                    telemetry.metrics().record_cloudfirst_wal_upload_failed();
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
                })
                .expect("failed to spawn WAL upload worker");
        }

        Self {
            local,
            cloud,
            budget_actor: Arc::new(Mutex::new(budget_actor)),
            upload_queue,
            event_queue,
            external_event_tx,
            wal_upload_tx,
        }
    }

    /// Enqueue a WAL segment for cloud upload (CloudFirst mode)
    ///
    /// This is the WAL durability pipeline entry point.
    pub fn enqueue_wal_segment(&self, segment_id: u64, local_path: PathBuf, max_sequence: u64) {
        let upload_state = UploadState {
            segment_id,
            local_path: local_path.clone(),
            status: UploadStatus::Pending,
            max_sequence,
        };

        let mut queue = self.upload_queue.lock();
        queue.push_back(upload_state);

        tracing::debug!(
            segment_id,
            ?local_path,
            max_sequence,
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
                        let prev_retries = match item.status {
                            UploadStatus::Failed { retries, .. } => retries,
                            _ => 0,
                        };
                        item.status = UploadStatus::Failed {
                            error: error.clone(),
                            retries: prev_retries.saturating_add(1),
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
                UploadStatus::Failed { retries, .. } => retries < 3,
                UploadStatus::InFlight { .. } | UploadStatus::Completed => false,
            };
            if !eligible {
                continue;
            }

            upload.status = UploadStatus::InFlight { started_at: now };

            // Send to the dedicated worker; avoid per-upload thread spawn.
            let _ = self.wal_upload_tx.send(upload.clone());
        }

        // 4) Garbage-collect finished items (Completed or Failed after 3 retries).
        queue.retain(|u| match &u.status {
            UploadStatus::Completed => false,
            UploadStatus::Failed { retries, .. } => *retries < 3,
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
                    cloud_clone.submit_write(key, data, tx_cloud);
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
